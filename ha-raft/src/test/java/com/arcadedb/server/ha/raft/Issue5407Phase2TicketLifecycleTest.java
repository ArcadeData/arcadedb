/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.server.ha.raft;

import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.engine.TransactionManager;
import com.arcadedb.exception.TransactionCommittedRemotelyException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Issue #5407: which exit of {@code replicateAndCommitLocally} releases the phase-2 ticket is the
 * load-bearing part of the fix, so it is pinned here rather than only end-to-end in the crash ITs.
 * <p>
 * The ticket protects the Raft replay window for an entry this node has committed but not yet
 * applied locally. Releasing it too eagerly is exactly the #5407 bug: a snapshot checkpoint taken
 * afterwards (Ratis takes one on shutdown) buries the entry below the replay position and the write
 * is lost for good. An earlier iteration of this fix released the ticket in a blanket
 * {@code finally}, which looked correct but let ordinary exception unwinding drop the guard before
 * the shutdown snapshot - and both crash ITs still failed. Hence: retain by default, release only
 * where the local pages are provably settled.
 */
class Issue5407Phase2TicketLifecycleTest {

  // Only ever used as the DatabaseContext key (proxied is a mock, nothing touches the filesystem).
  // Made unique per test so parallel runs cannot share a context, and so it carries no POSIX-only path.
  private final String dbPath = "issue5407-phase2-ticket-" + UUID.randomUUID();

  private LocalDatabase         proxied;
  private RaftHAServer          raftServer;
  private TransactionContext    tx;
  private ArcadeStateMachine    stateMachine;
  private TransactionManager    txManager;
  private RaftTransactionBroker broker;

  @BeforeEach
  void setUp() {
    txManager = mock(TransactionManager.class);
    proxied = mock(LocalDatabase.class);
    when(proxied.getDatabasePath()).thenReturn(dbPath);
    when(proxied.getName()).thenReturn("issue5407");
    when(proxied.getTransactionManager()).thenReturn(txManager);
    when(proxied.getSchema()).thenReturn(mock(Schema.class, RETURNS_DEEP_STUBS));
    when(proxied.executeInReadLock(any())).thenAnswer(inv -> ((Callable<?>) inv.getArgument(0)).call());

    broker = mock(RaftTransactionBroker.class);
    raftServer = mock(RaftHAServer.class, RETURNS_DEEP_STUBS);
    when(raftServer.isLeader()).thenReturn(true);
    when(raftServer.getTransactionBroker()).thenReturn(broker);

    tx = mock(TransactionContext.class);
    stateMachine = new ArcadeStateMachine();
  }

  @AfterEach
  void tearDown() {
    RaftReplicatedDatabase.TEST_POST_REPLICATION_HOOK = null;
    DatabaseContext.INSTANCE.removeContext(dbPath);
  }

  /** The whole point: phase 2 wrote the pages, so the entry is durable here and may be checkpointed. */
  @Test
  void successfulPhase2ReleasesTheTicket() {
    final RaftReplicatedDatabase database = newDatabase();
    final long ticket = stateMachine.beginLocalPhase2();
    assertThat(stateMachine.pendingLocalPhase2Count()).isEqualTo(1);

    database.replicateAndCommitLocally(payload(), true, stateMachine, ticket);

    assertThat(stateMachine.pendingLocalPhase2Count())
        .as("a completed phase 2 must stop pinning the snapshot checkpoint")
        .isZero();
  }

  /**
   * The #5407 window itself: replication succeeded, then something threw before phase 2 could run.
   * This node now holds a committed entry it never applied, so the guard MUST survive the unwind.
   */
  @Test
  void faultBetweenReplicationAndPhase2RetainsTheTicket() {
    final RaftReplicatedDatabase database = newDatabase();
    final long ticket = stateMachine.beginLocalPhase2();

    RaftReplicatedDatabase.TEST_POST_REPLICATION_HOOK = dbName -> {
      throw new IllegalStateException("simulated crash between Raft commit and phase 2");
    };

    assertThatThrownBy(() -> database.replicateAndCommitLocally(payload(), true, stateMachine, ticket))
        .isInstanceOf(IllegalStateException.class);

    assertThat(stateMachine.pendingLocalPhase2Count())
        .as("phase 2 never ran, so the entry must stay replayable - releasing here is the #5407 bug")
        .isEqualTo(1);
  }

  /** Phase 2 threw and reconciliation could not settle the pages either: the entry stays unapplied. */
  @Test
  void unreconciledPhase2FailureRetainsTheTicket() {
    final RaftReplicatedDatabase database = newDatabase();
    final long ticket = stateMachine.beginLocalPhase2();

    doThrow(new IllegalStateException("simulated phase 2 failure")).when(tx).commit2ndPhase(any());
    // Reconciliation replays the payload WAL through the transaction manager; make that fail too.
    doThrow(new IllegalStateException("simulated reconcile failure"))
        .when(txManager).applyChanges(any(), any(), anyBoolean());

    assertThatThrownBy(() -> database.replicateAndCommitLocally(payload(), true, stateMachine, ticket))
        .isInstanceOf(TransactionCommittedRemotelyException.class);

    assertThat(stateMachine.pendingLocalPhase2Count())
        .as("with the pages never settled the entry must remain inside the replay window")
        .isEqualTo(1);
  }

  /** Replication failed outright: no entry exists, so there is nothing to protect. */
  @Test
  void replicationFailureReleasesTheTicket() {
    final RaftReplicatedDatabase database = newDatabase();
    final long ticket = stateMachine.beginLocalPhase2();

    doThrow(new IllegalStateException("simulated replication failure"))
        .when(broker).replicateTransaction(anyString(), any(), any());

    assertThatThrownBy(() -> database.replicateAndCommitLocally(payload(), true, stateMachine, ticket))
        .isInstanceOf(TransactionException.class);

    assertThat(stateMachine.pendingLocalPhase2Count())
        .as("a transaction that never reached the log needs no replay protection")
        .isZero();
  }

  /**
   * ALL-quorum recovery: MAJORITY committed the entry, so this path applies phase 2 locally. When
   * that succeeds the pages are on disk and the ticket goes.
   */
  @Test
  void majorityCommittedRecoveryReleasesTheTicketWhenPagesSettle() {
    final RaftReplicatedDatabase database = newDatabase();
    final long ticket = stateMachine.beginLocalPhase2();

    doThrow(new MajorityCommittedAllFailedException("simulated ALL-quorum watch failure"))
        .when(broker).replicateTransaction(anyString(), any(), any());

    assertThatThrownBy(() -> database.replicateAndCommitLocally(payload(), true, stateMachine, ticket))
        .isInstanceOf(MajorityCommittedAllFailedException.class);

    assertThat(stateMachine.pendingLocalPhase2Count())
        .as("ALL-quorum recovery wrote the pages, so the checkpoint may cover the entry")
        .isZero();
  }

  /** ...but if that recovery apply fails and reconciliation cannot settle the pages either, it stays. */
  @Test
  void majorityCommittedRecoveryRetainsTheTicketWhenPagesNeverSettle() {
    final RaftReplicatedDatabase database = newDatabase();
    final long ticket = stateMachine.beginLocalPhase2();

    doThrow(new MajorityCommittedAllFailedException("simulated ALL-quorum watch failure"))
        .when(broker).replicateTransaction(anyString(), any(), any());
    doThrow(new IllegalStateException("simulated recovery apply failure")).when(tx).commit2ndPhase(any());
    doThrow(new IllegalStateException("simulated reconcile failure"))
        .when(txManager).applyChanges(any(), any(), anyBoolean());

    assertThatThrownBy(() -> database.replicateAndCommitLocally(payload(), true, stateMachine, ticket))
        .isInstanceOf(MajorityCommittedAllFailedException.class);

    assertThat(stateMachine.pendingLocalPhase2Count())
        .as("the entry is committed cluster-wide but absent here, so it must stay replayable")
        .isEqualTo(1);
  }

  /** A replica runs no phase 2 - the state machine applies the entry normally - so it holds nothing. */
  @Test
  void replicaCommitReleasesTheTicket() {
    final RaftReplicatedDatabase database = newDatabase();
    final long ticket = stateMachine.beginLocalPhase2();

    assertThatCode(() -> database.replicateAndCommitLocally(payload(), false, stateMachine, ticket))
        .doesNotThrowAnyException();

    assertThat(stateMachine.pendingLocalPhase2Count())
        .as("no leader-side phase 2 is pending on a replica")
        .isZero();
  }

  private RaftReplicatedDatabase newDatabase() {
    final RaftReplicatedDatabase database = new RaftReplicatedDatabase(null, proxied, raftServer);
    DatabaseContext.INSTANCE.init(proxied, tx);
    return database;
  }

  /** 24 zero bytes deserialize to an empty WAL transaction (txId=0, 0 pages). */
  private RaftReplicatedDatabase.ReplicationPayload payload() {
    return new RaftReplicatedDatabase.ReplicationPayload(tx, null, new byte[24], Map.of());
  }
}
