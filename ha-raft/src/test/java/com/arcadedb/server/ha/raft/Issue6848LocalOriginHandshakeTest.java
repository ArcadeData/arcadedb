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
import com.arcadedb.schema.Schema;
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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Issue #6848: the #4790 abandoned mark and the state machine's origin-skip are two threads deciding
 * the same question - who writes a locally-originated entry's pages - and before this fix they never
 * spoke to each other. The mark was published only after the entry had already been handed to Ratis,
 * so a fast commit (or simply a JVM slow enough on the committing thread's side) let
 * {@code applyTxEntry} run first, find nothing and origin-skip an entry whose phase 2 never ran. The
 * leader then stayed one transaction behind its followers for the rest of its uptime.
 * <p>
 * Both sides now claim the same slot, so exactly one of them wins and the loser takes over the work.
 * These tests pin both orderings and the cleanup that keeps the slot map bounded.
 *
 * @see Issue5410AbandonedPhase2TicketTest for the ticket-carrying half of the same marker
 * @see Issue6848AbandonedMarkRaceIT for the same race driven through a real three-node cluster
 */
class Issue6848LocalOriginHandshakeTest {

  private static final String DB_NAME = "issue6848";

  // Only ever used as the DatabaseContext key (proxied is a mock, nothing touches the filesystem).
  private final String dbPath = "issue6848-local-origin-handshake-" + UUID.randomUUID();

  private LocalDatabase         proxied;
  private RaftHAServer          raftServer;
  private TransactionContext    tx;
  private ArcadeStateMachine    stateMachine;
  private RaftTransactionBroker broker;

  @BeforeEach
  void setUp() {
    proxied = mock(LocalDatabase.class);
    when(proxied.getDatabasePath()).thenReturn(dbPath);
    when(proxied.getName()).thenReturn(DB_NAME);
    when(proxied.getTransactionManager()).thenReturn(mock(TransactionManager.class));
    when(proxied.getSchema()).thenReturn(mock(Schema.class, RETURNS_DEEP_STUBS));
    when(proxied.executeInReadLock(any())).thenAnswer(inv -> ((Callable<?>) inv.getArgument(0)).call());

    broker = mock(RaftTransactionBroker.class);
    raftServer = mock(RaftHAServer.class, RETURNS_DEEP_STUBS);
    when(raftServer.isLeader()).thenReturn(true);
    when(raftServer.getTransactionBroker()).thenReturn(broker);
    when(raftServer.getStateMachine()).thenAnswer(inv -> stateMachine);

    tx = mock(TransactionContext.class);
    stateMachine = new ArcadeStateMachine();
  }

  @AfterEach
  void tearDown() {
    DatabaseContext.INSTANCE.removeContext(dbPath);
  }

  /**
   * The correlation key is read from the leading 8 bytes instead of deserializing the whole WAL,
   * because it is read on the leader's hot commit path for an entry that is normally origin-skipped.
   * The two readings must agree, or the committing thread and the apply thread would claim different
   * slots and the handshake would silently stop working.
   */
  @Test
  void peekingTheTransactionIdAgreesWithFullDeserialization() {
    final byte[] walData = walTransactionBytes(4242L);

    assertThat(ArcadeStateMachine.peekWalTransactionId(walData))
        .as("the cheap read must name the same transaction as the full deserialization")
        .isEqualTo(ArcadeStateMachine.deserializeWalTransaction(walData).txId);
  }

  /** A payload too short to hold an id is corruption, not transaction 0. */
  @Test
  void peekingATruncatedPayloadIsRejected() {
    assertThatThrownBy(() -> ArcadeStateMachine.peekWalTransactionId(new byte[Long.BYTES - 1]))
        .isInstanceOf(ReplicationException.class);
    assertThatThrownBy(() -> ArcadeStateMachine.peekWalTransactionId(null))
        .isInstanceOf(ReplicationException.class);
  }

  /**
   * Ordering A (the pre-#6848 happy accident): the committing thread marks first, so the apply that
   * follows finds the mark, applies the entry and gets the ticket to release.
   */
  @Test
  void whenTheMarkIsPublishedFirstTheApplyClaimsTheTicket() {
    final long ticket = stateMachine.beginLocalPhase2();

    assertThat(stateMachine.markLocalTransactionAbandoned(DB_NAME, 7L, ticket))
        .as("an unclaimed slot means the state machine's apply will write the pages")
        .isTrue();

    assertThat(stateMachine.claimLocalOriginatedEntry(DB_NAME, 7L))
        .as("the apply must pick up the ticket the abandoning commit left behind")
        .isEqualTo(ticket);
    assertThat(stateMachine.claimLocalOriginatedEntry(DB_NAME, 7L))
        .as("a replay of the same entry must origin-skip instead of applying twice")
        .isEqualTo(ArcadeStateMachine.NO_ABANDONED_MARK);
  }

  /**
   * Ordering B (the #6848 bug): the apply thread gets there first and origin-skips. The commit that
   * abandons afterwards must be told it now owns the apply - before the fix it was told nothing and
   * the write was lost on this node.
   */
  @Test
  void whenTheApplyOriginSkipsFirstTheAbandoningCommitIsToldItOwnsTheApply() {
    final long ticket = stateMachine.beginLocalPhase2();

    assertThat(stateMachine.claimLocalOriginatedEntry(DB_NAME, 9L))
        .as("with no mark yet the apply must origin-skip")
        .isEqualTo(ArcadeStateMachine.NO_ABANDONED_MARK);

    assertThat(stateMachine.markLocalTransactionAbandoned(DB_NAME, 9L, ticket))
        .as("the apply already happened without writing the pages, so the committer must apply them")
        .isFalse();

    assertThat(stateMachine.consumeAbandonedLocalTransaction(DB_NAME, 9L))
        .as("a slot both sides are done with must not linger and pin a ticket forever")
        .isEqualTo(ArcadeStateMachine.NO_ABANDONED_MARK);
  }

  /** The cleanup on the settled paths removes the origin-skip slot and nothing else. */
  @Test
  void forgettingAnEntryDropsTheOriginSkipSlotButNeverAnAbandonedMark() {
    final long ticket = stateMachine.beginLocalPhase2();

    stateMachine.claimLocalOriginatedEntry(DB_NAME, 3L);
    stateMachine.forgetLocalOriginatedEntry(DB_NAME, 3L);
    assertThat(stateMachine.markLocalTransactionAbandoned(DB_NAME, 3L, ticket))
        .as("the origin-skip slot must be gone, so a later mark claims a free slot")
        .isTrue();

    stateMachine.forgetLocalOriginatedEntry(DB_NAME, 3L);
    assertThat(stateMachine.consumeAbandonedLocalTransaction(DB_NAME, 3L))
        .as("forgetting must never drop the abandoned mark: the apply still has to release its ticket")
        .isEqualTo(ticket);
  }

  /**
   * The whole fix, end to end on the committing thread: the entry was origin-skipped before the
   * dispatched timeout surfaced, so {@code replicateAndCommitLocally} must apply phase 2 itself and
   * release the ticket. Before the fix it rolled back, left the pages unwritten and kept the ticket
   * (and the snapshot checkpoint) pinned for the rest of the node's uptime.
   */
  @Test
  void aDispatchedTimeoutAfterAnOriginSkipAppliesPhase2Locally() {
    final RaftReplicatedDatabase database = newDatabase();
    final long ticket = stateMachine.beginLocalPhase2();

    // The Raft apply thread got to the entry first (txId 0: see payload()).
    stateMachine.claimLocalOriginatedEntry(DB_NAME, 0L);
    dispatchedTimeoutOnReplicate();

    assertThatThrownBy(() -> database.replicateAndCommitLocally(payload(), true, stateMachine, ticket))
        .as("the caller still learns the replication outcome was indeterminate")
        .isInstanceOf(ReplicationDispatchedTimeoutException.class);

    verify(tx, times(1)).commit2ndPhase(any());
    verify(tx).setRemotelyCommitted(true);
    assertThat(stateMachine.pendingLocalPhase2Count())
        .as("the pages are written here, so nothing may keep pinning the snapshot checkpoint")
        .isZero();
  }

  /**
   * The unchanged path, restated so the fix cannot quietly turn every dispatched timeout into a local
   * apply: with no origin-skip claim the mark stands, the transaction rolls back and the ticket stays
   * held until the entry really applies (the #5407 contract).
   */
  @Test
  void aDispatchedTimeoutWithoutAnOriginSkipStillRollsBackAndRetainsTheTicket() {
    final RaftReplicatedDatabase database = newDatabase();
    final long ticket = stateMachine.beginLocalPhase2();

    dispatchedTimeoutOnReplicate();

    assertThatThrownBy(() -> database.replicateAndCommitLocally(payload(), true, stateMachine, ticket))
        .isInstanceOf(ReplicationDispatchedTimeoutException.class);

    assertThat(stateMachine.consumeAbandonedLocalTransaction(DB_NAME, 0L))
        .as("the mark must stand so the eventual apply releases the ticket")
        .isEqualTo(ticket);
    assertThat(stateMachine.pendingLocalPhase2Count())
        .as("the entry may still commit while unapplied here, so it must stay replayable")
        .isEqualTo(1);
  }

  private void dispatchedTimeoutOnReplicate() {
    doThrow(new ReplicationDispatchedTimeoutException("simulated dispatched-then-timed-out replication"))
        .when(broker).replicateTransaction(anyString(), any(), any());
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

  /** An empty WAL transaction (no pages) carrying {@code txId}, in the replicated wire layout. */
  private static byte[] walTransactionBytes(final long txId) {
    final ByteBuffer buf = ByteBuffer.allocate(24);
    buf.putLong(txId);   // txId
    buf.putLong(1L);     // timestamp
    buf.putInt(0);       // page count
    buf.putInt(0);       // segment size
    return buf.array();
  }
}
