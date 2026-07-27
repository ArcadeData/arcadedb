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

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
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
import static org.mockito.Mockito.when;

/**
 * Issue #5410: the #4790 dispatched-timeout branch retains its phase-2 ticket on purpose - the entry
 * may still reach quorum, and the retained ticket is what keeps it replayable if this node dies
 * before applying it. But when the abandoned entry later DOES commit and
 * {@code ArcadeStateMachine.applyTxEntry} applies it, its pages become durable and nothing released
 * the ticket, so the snapshot checkpoint - and with it Raft log purging - stayed pinned until the
 * process restarted.
 * <p>
 * The fix records the ticket alongside the abandoned marker so the two can be reconciled, and
 * releases it on the branch that actually applies the transaction. The release must never move to
 * the origin-skip branch: that would reintroduce #5407.
 *
 * @see Issue5407Phase2TicketLifecycleTest for the surrounding ticket-lifecycle contract
 */
class Issue5410AbandonedPhase2TicketTest {

  private static final String DB_NAME = "issue5410";

  // Only ever used as the DatabaseContext key (proxied is a mock, nothing touches the filesystem).
  private final String dbPath = "issue5410-abandoned-ticket-" + UUID.randomUUID();

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
   * The pre-existing #5407 contract, restated here because the fix must not weaken it: an
   * indeterminate replication outcome keeps the ticket, because the entry may yet commit while this
   * node never applied it.
   */
  @Test
  void dispatchedTimeoutStillRetainsTheTicket() {
    final RaftReplicatedDatabase database = newDatabase();
    final long ticket = stateMachine.beginLocalPhase2();

    dispatchedTimeoutOnReplicate();

    assertThatThrownBy(() -> database.replicateAndCommitLocally(payload(), true, stateMachine, ticket))
        .isInstanceOf(ReplicationDispatchedTimeoutException.class);

    assertThat(stateMachine.pendingLocalPhase2Count())
        .as("the entry may still commit while unapplied here, so it must stay replayable")
        .isEqualTo(1);
  }

  /**
   * The #5410 fix: the abandoned marker must carry the ticket so the later apply can release it.
   * Before the fix the marker recorded only an insertion timestamp and the correlation was
   * impossible.
   */
  @Test
  void abandonedMarkerCarriesThePhase2Ticket() {
    final RaftReplicatedDatabase database = newDatabase();
    final long ticket = stateMachine.beginLocalPhase2();

    dispatchedTimeoutOnReplicate();

    assertThatThrownBy(() -> database.replicateAndCommitLocally(payload(), true, stateMachine, ticket))
        .isInstanceOf(ReplicationDispatchedTimeoutException.class);

    // The 24-zero-byte payload deserializes to an empty WAL transaction with txId 0.
    assertThat(stateMachine.consumeAbandonedLocalTransaction(DB_NAME, 0L))
        .as("the marker must remember which ticket to release once the entry applies")
        .isEqualTo(ticket);
  }

  /**
   * Consuming the marker and releasing the recorded ticket is what unpins log compaction. This is
   * the sequence {@code applyTxEntry} performs once the abandoned entry reaches quorum and its
   * pages are written.
   */
  @Test
  void releasingTheRecordedTicketUnpinsTheCheckpoint() {
    final RaftReplicatedDatabase database = newDatabase();
    final long ticket = stateMachine.beginLocalPhase2();

    dispatchedTimeoutOnReplicate();

    assertThatThrownBy(() -> database.replicateAndCommitLocally(payload(), true, stateMachine, ticket))
        .isInstanceOf(ReplicationDispatchedTimeoutException.class);
    assertThat(stateMachine.pendingLocalPhase2Count()).isEqualTo(1);

    stateMachine.endLocalPhase2(stateMachine.consumeAbandonedLocalTransaction(DB_NAME, 0L));

    assertThat(stateMachine.pendingLocalPhase2Count())
        .as("once the abandoned entry's pages are written the checkpoint must be free to advance")
        .isZero();
  }

  /**
   * The origin-skip branch: an entry with no abandoned marker was already applied by phase 2, and
   * releasing anything on its behalf here would reintroduce #5407. The lookup must report "nothing
   * to release" rather than a live ticket.
   */
  @Test
  void anEntryWithoutAnAbandonedMarkerReleasesNothing() {
    stateMachine.beginLocalPhase2();

    assertThat(stateMachine.consumeAbandonedLocalTransaction(DB_NAME, 42L))
        .as("no marker means the origin-skip branch, which must never release a ticket")
        .isEqualTo(ArcadeStateMachine.NO_ABANDONED_MARK);
    assertThat(stateMachine.pendingLocalPhase2Count())
        .as("the in-flight phase 2 must be untouched")
        .isEqualTo(1);
  }

  /** The marker is consumed exactly once, so a replay of the same entry correctly skips again. */
  @Test
  void theMarkerIsConsumedOnlyOnce() {
    final long ticket = stateMachine.beginLocalPhase2();
    stateMachine.markLocalTransactionAbandoned(DB_NAME, 7L, ticket);

    assertThat(stateMachine.consumeAbandonedLocalTransaction(DB_NAME, 7L)).isEqualTo(ticket);
    assertThat(stateMachine.consumeAbandonedLocalTransaction(DB_NAME, 7L))
        .as("a replayed entry must not release a ticket a second time")
        .isEqualTo(ArcadeStateMachine.NO_ABANDONED_MARK);
  }

  /**
   * A marker dropped by TTL pruning must NOT release its ticket. Pruning is not proof the entry never
   * committed: if it commits afterwards it is origin-skipped (so it never applies here), and the entry
   * must stay inside the replay window. The docs lean on this invariant, so it is pinned here.
   */
  @Test
  void aTtlPrunedMarkerKeepsItsTicketHeld() throws Exception {
    final long staleTicket = stateMachine.beginLocalPhase2();
    stateMachine.markLocalTransactionAbandoned(DB_NAME, 11L, staleTicket);

    backdateAbandonedMark(DB_NAME, 11L);

    // Marking any other transaction runs the prune pass that evicts the backdated entry.
    final long freshTicket = stateMachine.beginLocalPhase2();
    stateMachine.markLocalTransactionAbandoned(DB_NAME, 12L, freshTicket);

    assertThat(stateMachine.consumeAbandonedLocalTransaction(DB_NAME, 11L))
        .as("the stale marker must have been pruned")
        .isEqualTo(ArcadeStateMachine.NO_ABANDONED_MARK);
    assertThat(stateMachine.pendingLocalPhase2Count())
        .as("pruning a marker must not release its ticket - the entry may still commit unapplied")
        .isEqualTo(2);
  }

  /**
   * Rewrites one abandoned mark's insertion time to just beyond {@code ABANDONED_TX_TTL_MS} so the
   * next mark prunes it, without making the test wait out the real 10-minute TTL.
   */
  @SuppressWarnings("unchecked")
  private void backdateAbandonedMark(final String databaseName, final long walTxId) throws Exception {
    final Field marksField = ArcadeStateMachine.class.getDeclaredField("abandonedLocalTransactions");
    marksField.setAccessible(true);
    final Map<String, Object> marks = (Map<String, Object>) marksField.get(stateMachine);

    final Field ttlField = ArcadeStateMachine.class.getDeclaredField("ABANDONED_TX_TTL_MS");
    ttlField.setAccessible(true);
    final long ttlMs = (long) ttlField.get(null);

    final String key = databaseName + "/" + walTxId;
    final Object mark = marks.get(key);
    final Constructor<?> ctor = mark.getClass().getDeclaredConstructor(long.class, long.class);
    ctor.setAccessible(true);
    final Field ticketField = mark.getClass().getDeclaredField("phase2Ticket");
    ticketField.setAccessible(true);
    marks.put(key, ctor.newInstance(ticketField.get(mark), System.currentTimeMillis() - ttlMs - 1_000L));
  }

  /**
   * Releasing the sentinel must be a no-op: {@code applyTxEntry} calls the release path for every
   * applied entry, and the overwhelming majority carry no ticket at all (replica applies, replay).
   */
  @Test
  void releasingTheNoTicketSentinelIsANoOp() {
    final long ticket = stateMachine.beginLocalPhase2();

    stateMachine.endLocalPhase2(ArcadeStateMachine.NO_PHASE2_TICKET);

    assertThat(stateMachine.pendingLocalPhase2Count())
        .as("the sentinel must never evict a live ticket")
        .isEqualTo(1);
    stateMachine.endLocalPhase2(ticket);
    assertThat(stateMachine.pendingLocalPhase2Count()).isZero();
  }

  /**
   * The stats the Micrometer gauges read: with nothing in flight they must report the idle
   * placeholders rather than {@code Long.MAX_VALUE} leaking out of the internal scan.
   */
  @Test
  void pendingPhase2StatsReportIdlePlaceholdersWhenNothingIsInFlight() {
    assertThat(stateMachine.pendingLocalPhase2Count()).isZero();
    assertThat(stateMachine.oldestPendingLocalPhase2HeldMs())
        .as("no held ticket means no age to report")
        .isZero();
    assertThat(stateMachine.lowestPendingLocalPhase2ReplayFloor())
        .as("no held ticket means no floor is pinning the checkpoint")
        .isEqualTo(-1L);
  }

  /** With a ticket in flight the gauges must expose the pinned floor and a non-negative age. */
  @Test
  void pendingPhase2StatsExposeTheHeldFloor() {
    final long ticket = stateMachine.beginLocalPhase2();

    assertThat(stateMachine.pendingLocalPhase2Count()).isEqualTo(1);
    assertThat(stateMachine.oldestPendingLocalPhase2HeldMs()).isGreaterThanOrEqualTo(0L);
    assertThat(stateMachine.lowestPendingLocalPhase2ReplayFloor())
        .as("a fresh state machine has applied nothing, so the floor is -1")
        .isEqualTo(-1L);

    stateMachine.endLocalPhase2(ticket);
    assertThat(stateMachine.oldestPendingLocalPhase2HeldMs()).isZero();
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
}
