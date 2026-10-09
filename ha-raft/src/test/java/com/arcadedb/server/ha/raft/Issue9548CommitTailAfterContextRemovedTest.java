/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
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
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DatabaseIsClosedException;
import com.arcadedb.exception.TransactionCommittedRemotelyException;
import com.arcadedb.schema.Schema;
import com.arcadedb.utility.SubclassMocks;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Issue #9548, the committing thread's half: a close of the database on another thread (the engine's JVM shutdown hook
 * in the report) removes every thread's transaction context while the committer waits for the quorum or for the apply
 * thread. The commit tail then required that context to pop it and threw "Transaction context not found on current
 * thread", which replaced the outcome the caller is owed. Every tail that runs after the entry left this node must
 * conclude without it: completed when the pages were published, {@link TransactionCommittedRemotelyException} when they
 * were not.
 * <p>
 * The close is simulated on the committing thread itself, from inside the broker call, by removing its context: that is
 * exactly what {@code DatabaseContext.removeAllContexts} does to it from the closing thread.
 */
class Issue9548CommitTailAfterContextRemovedTest {
  private static final String DB_NAME   = "issue9548";
  private static final long   WAL_TX_ID = 9548L;

  private final String dbPath = "issue9548-commit-tail-" + UUID.randomUUID();

  private LocalDatabase                             proxied;
  private FakeRaftHAServer                          raftServer;
  private TransactionContext                        tx;
  private ArcadeStateMachine                        stateMachine;
  private FakeRaftTransactionBroker                 broker;
  private RaftReplicatedDatabase                    database;
  private RaftReplicatedDatabase.ReplicationPayload payload;

  @BeforeEach
  void setUp() {
    proxied = SubclassMocks.mock(LocalDatabase.class);
    when(proxied.getDatabasePath()).thenReturn(dbPath);
    when(proxied.getName()).thenReturn(DB_NAME);
    when(proxied.getTransactionManager()).thenReturn(SubclassMocks.mock(TransactionManager.class));
    when(proxied.getSchema()).thenReturn(mock(Schema.class, RETURNS_DEEP_STUBS));
    when(proxied.executeInReadLock(any())).thenAnswer(inv -> ((Callable<?>) inv.getArgument(0)).call());

    broker = new FakeRaftTransactionBroker();
    raftServer = FakeRaftHAServer.detached();
    raftServer.leader(true);
    raftServer.transactionBroker(broker);
    raftServer.quorumTimeout(1_000L);
    raftServer.txPreparedAtCapable(false);
    stateMachine = new ArcadeStateMachine();
    raftServer.stateMachine(stateMachine);

    tx = SubclassMocks.mock(TransactionContext.class);
    DatabaseContext.INSTANCE.init(proxied, tx);
    database = new RaftReplicatedDatabase(null, proxied, raftServer);
    payload = new RaftReplicatedDatabase.ReplicationPayload(tx, null, walTransactionBytes(WAL_TX_ID), Map.of());
  }

  @AfterEach
  void tearDown() {
    DatabaseContext.INSTANCE.removeContext(dbPath);
  }

  /**
   * The report's exact sequence: the apply thread claimed the entry and failed to publish it (the database was closing),
   * the quorum wait timed out, and the committing thread - its context gone - must still tell the caller that the
   * transaction is committed cluster-wide and must not be retried.
   */
  @Test
  void aTimedOutCommitWhosePublicationFailedReportsCommittedRemotelyWithoutAContext() {
    final DatabaseIsClosedException closed = new DatabaseIsClosedException(DB_NAME);
    broker.on("replicateTransaction", args -> {
      databaseClosedOnAnotherThread();
      claim().failed(closed, false);
      throw new ReplicationDispatchedTimeoutException("dispatched, outcome unknown");
    });

    assertThatThrownBy(() -> database.replicateAndCommitLocally(payload, true, stateMachine))
        .isInstanceOf(TransactionCommittedRemotelyException.class)
        .hasMessageContaining("committed cluster-wide")
        .hasCause(closed);

    verify(tx).setRemotelyCommitted(true);
    verify(tx).concludeFailedPhase2(closed);
    verify(proxied, never()).rollback();
  }

  /** Acknowledged, failed publication: same answer as above through the acknowledged exit. */
  @Test
  void anAcknowledgedCommitWhosePublicationFailedReportsCommittedRemotelyWithoutAContext() {
    final ConcurrentModificationException cause = new ConcurrentModificationException("simulated phase-2 failure");
    broker.on("replicateTransaction", args -> {
      databaseClosedOnAnotherThread();
      claim().failed(cause, true);
      return 7L;
    });

    assertThatThrownBy(() -> database.replicateAndCommitLocally(payload, true, stateMachine))
        .isInstanceOf(TransactionCommittedRemotelyException.class)
        .hasCause(cause);

    verify(tx).concludeFailedPhase2(cause);
  }

  /** Published by the apply thread: the commit completes, the missing context is not an error. */
  @Test
  void aPublishedCommitCompletesWithoutAContext() {
    broker.on("replicateTransaction", args -> {
      databaseClosedOnAnotherThread();
      claim().published();
      return 7L;
    });

    database.replicateAndCommitLocally(payload, true, stateMachine);

    verify(tx).completeCommit();
    verify(proxied, never()).rollback();
  }

  /** Unclaimed with a live state machine: the commit waits for the apply and releases, without a context. */
  @Test
  void anUnclaimedCommitALiveStateMachineAppliesIsReleasedWithoutAContext() {
    broker.on("replicateTransaction", args -> {
      databaseClosedOnAnotherThread();
      return 7L;
    });

    database.replicateAndCommitLocally(payload, true, stateMachine);

    verify(tx).concludeCommitWithoutPublishing();
    verify(tx, never()).commit2ndPhase(any());
  }

  /** Unclaimed during a shutdown: the committing thread publishes the pages itself, without a context. */
  @Test
  void anUnclaimedCommitPublishedByTheCommittingThreadDuringAShutdownCompletesWithoutAContext() {
    raftServer.shutdownRequested(true);
    broker.on("replicateTransaction", args -> {
      databaseClosedOnAnotherThread();
      return 7L;
    });

    database.replicateAndCommitLocally(payload, true, stateMachine);

    verify(tx).commit2ndPhase(any());
    verify(tx).setRemotelyCommitted(true);
  }

  /** A replica waits for its own apply and releases, without a context. */
  @Test
  void aReplicaCommitIsReleasedWithoutAContext() {
    broker.on("replicateTransaction", args -> {
      databaseClosedOnAnotherThread();
      return 7L;
    });

    database.replicateAndCommitLocally(payload, false, null);

    verify(tx).concludeCommitWithoutPublishing();
  }

  /** What {@code DatabaseContext.removeAllContexts} does to this thread when another thread closes the database. */
  private void databaseClosedOnAnotherThread() {
    DatabaseContext.INSTANCE.removeContext(dbPath);
    assertThat(DatabaseContext.INSTANCE.getContextIfExists(dbPath)).isNull();
  }

  private LocalCommit claim() {
    final LocalCommit claimed = stateMachine.claimLocalCommit(DB_NAME, WAL_TX_ID, payload.walData());
    assertThat(claimed).as("the transaction must be registered before the entry is dispatched").isNotNull();
    return claimed;
  }

  /** The wire layout {@code ArcadeStateMachine.deserializeWalTransaction} reads: id, timestamp, no pages. */
  private static byte[] walTransactionBytes(final long txId) {
    final ByteBuffer buf = ByteBuffer.allocate(2 * Long.BYTES + 2 * Integer.BYTES);
    buf.putLong(txId);
    buf.putLong(System.currentTimeMillis());
    buf.putInt(0);
    buf.putInt(0);
    return buf.array();
  }
}
