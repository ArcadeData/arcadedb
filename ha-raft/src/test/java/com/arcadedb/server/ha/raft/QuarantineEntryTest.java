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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.TransactionManager;
import com.arcadedb.exception.WALVersionGapException;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.FakeArcadeDBServer;
import com.arcadedb.utility.FileUtils;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.arcadedb.utility.SubclassMocks.mock;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.when;

/**
 * How a database enters quarantine and how a dropped database leaves it.
 */
class QuarantineEntryTest {

  /**
   * Issue #7495: a committed TX entry whose WAL payload is too short to hold its transaction id has to be
   * quarantined like any other unreadable committed entry, not reported as a bare replication error.
   * <p>
   * Since issue #6965 {@code applyTxEntry} reads the WAL transaction id before anything else, to match the entry
   * against a transaction this node originated. That read reported a truncated payload as a plain
   * {@link ReplicationException}, and {@code applyWithRetry} rethrows a {@code ReplicationException} unchanged -
   * it is the resync signal the WAL-gap path raises - so the failure went straight past
   * {@code handleUnexpectedApplyError}. No database was marked diverged, no targeted snapshot resync was
   * triggered, and the failed future is one Ratis swallows while advancing its own applied index: the corrupt
   * entry was skipped on this node and nothing said so.
   * <p>
   * The fix classifies it as what it is - a {@link RaftLogEntryDecodeException}, the type issue #7138 introduced
   * for a committed entry of a KNOWN type this version cannot read - so it reaches the per-database quarantine
   * (issue #4797) and the leader resends the database as a snapshot.
   * <p>
   * The harness uses a {@code null} server, so no cluster is stood up; the quarantine bookkeeping and the
   * exception classification are what these tests read.
   */
  @Nested
  class TruncatedWalEntryQuarantine {

    private static TransactionContext txEntry(final ArcadeStateMachine sm, final String databaseName,
        final byte[] walData, final long index) {
      final ByteString payload = RaftLogEntryCodec.encodeTxEntry(databaseName, walData, Collections.emptyMap());
      final LogEntryProto logEntry = LogEntryProto.newBuilder()
          .setTerm(1L)
          .setIndex(index)
          .setStateMachineLogEntry(StateMachineLogEntryProto.newBuilder().setLogData(payload).build())
          .build();
      return TransactionContext.newBuilder().setStateMachine(sm).setLogEntry(logEntry).build();
    }

    // Regression for #7495: a TX entry with a truncated WAL payload is quarantined
    @Test
    void aTruncatedWalPayloadIsClassifiedAsADecodeFailureAndQuarantinesItsDatabase() {
      final ArcadeStateMachine sm = new ArcadeStateMachine();

      // Shorter than the 8 bytes the transaction id occupies: the id cannot be read at all.
      final CompletableFuture<Message> future = sm.applyTransaction(txEntry(sm, "db-A", new byte[3], 5L));

      assertThat(future.isCompletedExceptionally()).isTrue();
      assertThatThrownBy(future::join)
          .hasCauseInstanceOf(ReplicationException.class)
          .hasMessageContaining("Apply error on database 'db-A'")
          .hasMessageContaining("per-database snapshot resync in progress");

      // The classification is the fix: without it the failure is a bare ReplicationException that
      // applyWithRetry rethrows unchanged, so nothing below this line would hold.
      final Throwable failure = catchThrowable(future::join);
      assertThat(failure.getCause().getCause())
          .as("the quarantine wraps the decode failure that caused it")
          .isInstanceOf(RaftLogEntryDecodeException.class)
          .hasMessageContaining("Cannot read the WAL transaction id");
      assertThat(failure.getCause().getCause().getCause())
          .hasMessageContaining("Corrupted WAL transaction entry");

      assertThat(sm.isDatabaseDiverged("db-A")).as("quarantined for a targeted resync").isTrue();
      assertThat(sm.isDatabaseDiverged("db-B")).isFalse();
      assertThat(sm.isHaltedAfterCriticalError()).as("one bad entry must not halt the whole node").isFalse();
    }

    // Regression for #7495: a TX entry with a truncated WAL payload is quarantined
    @Test
    void anEmptyWalPayloadIsTreatedTheSameWay() {
      final ArcadeStateMachine sm = new ArcadeStateMachine();

      final CompletableFuture<Message> future = sm.applyTransaction(txEntry(sm, "db-A", new byte[0], 5L));

      assertThatThrownBy(future::join)
          .hasCauseInstanceOf(ReplicationException.class)
          .hasMessageContaining("per-database snapshot resync in progress");
      assertThat(sm.isDatabaseDiverged("db-A")).isTrue();
      assertThat(sm.isHaltedAfterCriticalError()).isFalse();
    }

    // Regression for #7495: a TX entry with a truncated WAL payload is quarantined
    /**
     * The entry type the decode failure reports matters: {@code handleUnexpectedApplyError} quarantines only when
     * the failure names a database, and a TX entry always does.
     */
    @Test
    void theDecodeFailureNamesTheEntryTypeAndTheDatabase() {
      final ArcadeStateMachine sm = new ArcadeStateMachine();

      final CompletableFuture<Message> future = sm.applyTransaction(txEntry(sm, "db-A", new byte[1], 9L));

      final Throwable decode = catchThrowable(future::join).getCause().getCause();
      assertThat(decode).isInstanceOf(RaftLogEntryDecodeException.class);
      assertThat(((RaftLogEntryDecodeException) decode).getType()).isEqualTo(RaftLogEntryType.TX_ENTRY);
      assertThat(((RaftLogEntryDecodeException) decode).getDatabaseName()).isEqualTo("db-A");
    }

    // Regression for #7495: a TX entry with a truncated WAL payload is quarantined
    /**
     * The same reclassification one step further in: a payload long enough to carry the transaction id but corrupt
     * beyond it fails in {@code deserializeWalTransaction}, which rejects a misaligned page count with the same
     * {@link ReplicationException} (issue #4420) and had the same consequence on this path - rethrown unchanged,
     * never quarantined.
     */
    @Test
    void aCorruptWalPayloadPastTheTransactionIdIsAlsoQuarantined() {
      // databaseFor() is the seam the apply resolves a database through, and the decode is the next line after it.
      // Returning null gets past it without a server, so the decode is what fails rather than the lookup.
      final ArcadeStateMachine sm = new ArcadeStateMachine() {
        @Override
        DatabaseInternal databaseFor(final String databaseName) {
          return null;
        }
      };

      final ByteBuffer buf = ByteBuffer.allocate(2 * Long.BYTES + 2 * Integer.BYTES);
      buf.putLong(1L); // txId - readable, so this gets past the transaction-id read
      buf.putLong(0L); // timestamp
      buf.putInt(-7);  // pageCount: the corruption of issue #4420
      buf.putInt(0);   // segmentSize

      final CompletableFuture<Message> future = sm.applyTransaction(txEntry(sm, "db-A", buf.array(), 11L));

      assertThatThrownBy(future::join)
          .hasCauseInstanceOf(ReplicationException.class)
          .hasMessageContaining("Apply error on database 'db-A'")
          .hasMessageContaining("per-database snapshot resync in progress");

      final Throwable decode = catchThrowable(future::join).getCause().getCause();
      assertThat(decode).isInstanceOf(RaftLogEntryDecodeException.class)
          .hasMessageContaining("Cannot decode the WAL payload of");
      assertThat(decode.getCause()).hasMessageContaining("invalid page count");

      assertThat(sm.isDatabaseDiverged("db-A")).isTrue();
      assertThat(sm.isHaltedAfterCriticalError()).isFalse();
    }

    // Regression for #7495: a TX entry with a truncated WAL payload is quarantined
    /**
     * The narrow window between the two decoders: 8 bytes is enough for the transaction id, but the WAL header the
     * decoder reads is 24, so a payload in between runs out of buffer before {@code deserializeWalTransaction}
     * reaches any of its explicit checks and raises a {@code BufferUnderflowException} instead of a
     * {@link ReplicationException}. It is the same condition - a committed entry this node cannot read - and it
     * must be reported as the same thing, rather than the diagnosis depending on how far in the corruption starts.
     */
    @Test
    void aPayloadTooShortForTheWalHeaderIsClassifiedLikeAnyOtherUndecodableOne() {
      final ArcadeStateMachine sm = new ArcadeStateMachine() {
        @Override
        DatabaseInternal databaseFor(final String databaseName) {
          return null;
        }
      };

      // 10 bytes: past the 8-byte transaction id, short of the 24-byte header.
      final CompletableFuture<Message> future = sm.applyTransaction(txEntry(sm, "db-A", new byte[10], 13L));

      assertThatThrownBy(future::join)
          .hasCauseInstanceOf(ReplicationException.class)
          .hasMessageContaining("Apply error on database 'db-A'");

      final Throwable decode = catchThrowable(future::join).getCause().getCause();
      assertThat(decode).isInstanceOf(RaftLogEntryDecodeException.class)
          .hasMessageContaining("Cannot decode the WAL payload of");
      assertThat(decode.getCause()).isInstanceOf(BufferUnderflowException.class);

      assertThat(sm.isDatabaseDiverged("db-A")).isTrue();
      assertThat(sm.isHaltedAfterCriticalError()).isFalse();
    }

    // Regression for #7495: a TX entry with a truncated WAL payload is quarantined
    /**
     * The bounded-escalation budget must be charged ONCE per failure.
     * <p>
     * {@code handleUnexpectedApplyError} rethrows the original error once the budget is exhausted, so a node that
     * can never resync halts rather than degrading silently. The original here is a
     * {@link RaftLogEntryDecodeException}, and {@code applyTransaction} catches that type separately as the handler
     * for an unreadable ENVELOPE - a different failure, decoded before {@code applyWithRetry} is ever called.
     * Without the guard in {@code applyWithRetry} the escalated exception reaches that catch and is handled a
     * second time, charging one failure twice against the budget that just tripped.
     */
    @Test
    void theEscalationBudgetIsChargedOncePerUndecodableEntry() {
      final ArcadeStateMachine sm = new ArcadeStateMachine();

      // The budget is `incrementAndGet() > 100`, so the first 100 quarantine and the 101st escalates.
      for (int i = 1; i <= 100; i++) {
        final int index = i;
        assertThatThrownBy(() -> sm.applyTransaction(txEntry(sm, "db-A", new byte[0], index)).join())
            .as("failure #%d is recoverable", i)
            .hasCauseInstanceOf(ReplicationException.class);
      }
      assertThat(sm.divergedSwallowedErrorCount()).isEqualTo(100);
      assertThat(sm.isHaltedAfterCriticalError()).isFalse();

      final CompletableFuture<Message> escalated = sm.applyTransaction(txEntry(sm, "db-A", new byte[0], 101L));

      assertThat(escalated.isCompletedExceptionally()).isTrue();
      assertThat(sm.isHaltedAfterCriticalError()).as("a node that can never resync halts loudly").isTrue();
      assertThat(sm.divergedSwallowedErrorCount())
          .as("one failure, one swallow charged - not two")
          .isEqualTo(101);
    }

    // Regression for #7495: a TX entry with a truncated WAL payload is quarantined
    /**
     * The coverage {@code ArcadeStateMachinePerDatabaseHaltTest} was written for and quietly stopped having: an
     * apply error that is NOT a decode failure. A payload long enough to carry the transaction id gets past the
     * read that this issue is about and fails where that test's javadoc says it does - resolving the database
     * against a {@code null} server - which is the representative "unexpected error" issue #4797 scoped to one
     * database.
     */
    @Test
    void anUnexpectedApplyErrorPastTheWalReadStillQuarantinesOneDatabaseOnly() {
      final ArcadeStateMachine sm = new ArcadeStateMachine();

      // 8 bytes: the transaction id reads back as 0 and the apply proceeds to resolve the database.
      final CompletableFuture<Message> future = sm.applyTransaction(txEntry(sm, "db-A", new byte[Long.BYTES], 5L));

      assertThatThrownBy(future::join)
          .hasCauseInstanceOf(ReplicationException.class)
          .hasMessageContaining("Apply error on database 'db-A'");
      assertThatThrownBy(future::join).getRootCause().isInstanceOf(NullPointerException.class);

      assertThat(sm.isDatabaseDiverged("db-A")).isTrue();
      assertThat(sm.isDatabaseDiverged("db-B")).isFalse();
      assertThat(sm.isHaltedAfterCriticalError()).isFalse();

      // A healthy database keeps being applied on its own merits rather than being refused node-wide.
      final CompletableFuture<Message> dbB = sm.applyTransaction(txEntry(sm, "db-B", new byte[Long.BYTES], 6L));
      assertThatThrownBy(dbB::join).hasMessageNotContaining("halted after critical error");
      assertThat(sm.isHaltedAfterCriticalError()).isFalse();
    }
  }

  /**
   * Issue #7641 (review of PR #7649): a DROP entry has to retire the quarantine bookkeeping of the database it
   * removes - the per-database read floor of issue #6760 and the diverged marker beside it.
   * <p>
   * Both exist to hold readers back from a database a snapshot install could not bring up to date, until a targeted
   * resync restores it, and {@code clearDivergedDatabase} - which runs on that resync - was the only thing that ever
   * cleared them. A database that is dropped WHILE quarantined has no resync left to wait for, so nothing would ever
   * clear its floor and it outlived the database for the node's lifetime. Two consequences, both real once
   * {@code dropInReplicas} started waiting for the local apply:
   * <ul>
   *   <li>{@code RaftHAServer.getTrustedAppliedIndex(dbName)} clamps to the floor, so that wait could never be
   *   satisfied - {@code drop database} would report failure through its whole quorum timeout even though the
   *   directory was gone, and every later drop of a database recreated under that name would do the same;</li>
   *   <li>a database recreated under the name would inherit the dead floor and have its LINEARIZABLE reads pinned
   *   behind it.</li>
   * </ul>
   */
  @Nested
  class DroppedDatabaseClearsQuarantine {

    private static final String DB_NAME = "quarantined7641";
    private static final long   FLOOR   = 17L;

    // Regression for #7641: a DROP entry retires the quarantine bookkeeping of the dropped database
    /**
     * Drives the already-absent branch of {@code applyDropDatabaseEntry}: it is the one that needs no live database,
     * and it retires the same bookkeeping the branch that actually deletes does. The floor is seeded the way
     * {@code settleDivergedStateAfterInstall} publishes it.
     */
    @Test
    void droppingAQuarantinedDatabaseRetiresItsReadFloorAndDivergedMarker() throws Exception {
      final ArcadeStateMachine sm = new ArcadeStateMachine();
      sm.setServer(serverWithout(DB_NAME));

      seedDatabaseAppliedFloor(sm, DB_NAME, FLOOR);
      sm.markStateDiverged(DB_NAME);

      assertThat(sm.getDatabaseAppliedFloor(DB_NAME)).as("precondition: the database is quarantined").isEqualTo(FLOOR);
      assertThat(sm.isDatabaseDiverged(DB_NAME)).isTrue();

      sm.applyDropDatabaseEntry(RaftLogEntryCodec.decode(RaftLogEntryCodec.encodeDropDatabaseEntry(DB_NAME)), 1L);

      // -1 is "this database is not known to be behind", which is the only honest answer once it does not exist
      assertThat(sm.getDatabaseAppliedFloor(DB_NAME)).as("a dropped database has no read floor left to honour")
          .isEqualTo(-1L);
      assertThat(sm.isDatabaseDiverged(DB_NAME)).as("nor a resync obligation").isFalse();
    }

    /** A database that was NOT quarantined when it was dropped must be unaffected - there is nothing to retire. */
    // Regression for #7641: a DROP entry retires the quarantine bookkeeping of the dropped database
    @Test
    void droppingAHealthyDatabaseTouchesNoQuarantineState() throws Exception {
      final ArcadeStateMachine sm = new ArcadeStateMachine();
      sm.setServer(serverWithout(DB_NAME));

      // A DIFFERENT database is the quarantined one: dropping this one must leave its floor standing
      seedDatabaseAppliedFloor(sm, "other7641", FLOOR);
      sm.markStateDiverged("other7641");

      sm.applyDropDatabaseEntry(RaftLogEntryCodec.decode(RaftLogEntryCodec.encodeDropDatabaseEntry(DB_NAME)), 1L);

      assertThat(sm.getDatabaseAppliedFloor("other7641")).as("an unrelated database keeps its floor").isEqualTo(FLOOR);
      assertThat(sm.isDatabaseDiverged("other7641")).isTrue();
    }

    /** A server that holds no databases at all, which is all the already-absent drop branch needs to look at. */
    private static ArcadeDBServer serverWithout(final String databaseName) {
      final FakeArcadeDBServer server = FakeArcadeDBServer.create((String) null, new ContextConfiguration());
      // evictBootstrapBaseline resolves the baseline file through the server's configuration
      return server;
    }

    @SuppressWarnings("unchecked")
    private static void seedDatabaseAppliedFloor(final ArcadeStateMachine sm, final String dbName, final long floor)
        throws Exception {
      final Field f = ArcadeStateMachine.class.getDeclaredField("staleDatabaseAppliedFloors");
      f.setAccessible(true);
      ((Map<String, Long>) f.get(sm)).put(dbName, floor);
    }
  }

  /**
   * Regression tests for issue #11250: a node whose own crash recovery stopped short of replaying every transaction (a WAL
   * gap after a power loss) still rejoined the cluster, won an election and answered queries from what the power loss left.
   * <p>
   * The invariant: <b>a database whose local recovery did not complete is quarantined, so the node does not serve it, leads
   * no longer with it, and re-installs it from the cluster.</b> A sole voter has no copy to re-install from, so it is alerted
   * about instead, as for #9308.
   */
  @Nested
  class IncompleteRecoveryQuarantine {

    private static final String DB_NAME = "db11250";

    private final AtomicBoolean      soleVoter = new AtomicBoolean(false);
    private       FakeRaftHAServer   raft;
    private       ArcadeStateMachine sm;

    @BeforeEach
    void setUp() {
      raft = FakeRaftHAServer.detached();
      raft.leader(true);
      raft.on("isSoleVoter", args -> soleVoter.get());
      sm = new ArcadeStateMachine();
      sm.setRaftHAServer(raft);
    }

    // Regression for #11250: a node whose crash recovery stopped short quarantines the database
    @Test
    void anIncompleteRecoveryQuarantinesTheDatabaseAndHandsLeadershipOff() {
      assertThat(sm.quarantineAfterIncompleteRecovery(DB_NAME)).isTrue();

      assertThat(sm.isDatabaseDiverged(DB_NAME)).isTrue();
      assertThat(sm.quarantineCause(DB_NAME)).isEqualTo(DivergenceCause.LOCAL_RECOVERY_INCOMPLETE);
      assertThat(sm.isResyncInProgress()).as("the node must leave the ready set").isTrue();
      assertThat(raft.calls("handOffLeadershipToResync")).singleElement()
          .satisfies(args -> assertThat((String) args.get(0)).contains("'" + DB_NAME + "'"));
    }

    // Regression for #11250: a node whose crash recovery stopped short quarantines the database
    @Test
    void aSecondCallDoesNotQuarantineAgain() {
      assertThat(sm.quarantineAfterIncompleteRecovery(DB_NAME)).isTrue();
      assertThat(sm.quarantineAfterIncompleteRecovery(DB_NAME)).isFalse();
      assertThat(raft.calls("handOffLeadershipToResync")).hasSize(1);
    }

    // Regression for #11250: a node whose crash recovery stopped short quarantines the database
    @Test
    void aSoleVoterIsNotQuarantinedBecauseNothingCouldLiftIt() {
      soleVoter.set(true);

      assertThat(sm.quarantineAfterIncompleteRecovery(DB_NAME)).isFalse();

      assertThat(sm.isDatabaseDiverged(DB_NAME)).isFalse();
      assertThat(sm.isResyncInProgress()).isFalse();
      assertThat(raft.calls("handOffLeadershipToResync")).isEmpty();
    }

    /** #11258: a unique index lost for a missing header page is quarantined like an incomplete recovery. */
    // Regression for #11250: a node whose crash recovery stopped short quarantines the database
    @Test
    void anUnavailableUniqueIndexQuarantinesTheDatabase() {
      assertThat(sm.quarantineAfterUnavailableUniqueIndex(DB_NAME, List.of("idx"))).isTrue();

      assertThat(sm.isDatabaseDiverged(DB_NAME)).isTrue();
      assertThat(sm.quarantineCause(DB_NAME)).isEqualTo(DivergenceCause.UNAVAILABLE_UNIQUE_INDEX);
      assertThat(raft.calls("handOffLeadershipToResync")).hasSize(1);
    }

    // Regression for #11250: a node whose crash recovery stopped short quarantines the database
    @Test
    void aSoleVoterIsNotQuarantinedForAnUnavailableUniqueIndex() {
      soleVoter.set(true);

      assertThat(sm.quarantineAfterUnavailableUniqueIndex(DB_NAME, List.of("idx"))).isFalse();
      assertThat(sm.isDatabaseDiverged(DB_NAME)).isFalse();
    }

    /** #11258: a dictionary a power loss emptied is quarantined like an incomplete recovery. */
    // Regression for #11250: a node whose crash recovery stopped short quarantines the database
    @Test
    void anEmptiedDictionaryQuarantinesTheDatabase() {
      assertThat(sm.quarantineAfterEmptiedDictionary(DB_NAME)).isTrue();

      assertThat(sm.isDatabaseDiverged(DB_NAME)).isTrue();
      assertThat(sm.quarantineCause(DB_NAME)).isEqualTo(DivergenceCause.EMPTY_DICTIONARY);
      assertThat(sm.isResyncInProgress()).isTrue();
      assertThat(raft.calls("handOffLeadershipToResync")).hasSize(1);
    }

    // Regression for #11250: a node whose crash recovery stopped short quarantines the database
    @Test
    void aSoleVoterIsNotQuarantinedForAnEmptiedDictionary() {
      soleVoter.set(true);

      assertThat(sm.quarantineAfterEmptiedDictionary(DB_NAME)).isFalse();
      assertThat(sm.isDatabaseDiverged(DB_NAME)).isFalse();
    }

    // Regression for #11250: a node whose crash recovery stopped short quarantines the database
    @Test
    void theScanIsANoOpWithoutAServer() {
      assertThat(sm.quarantineDatabasesWithIncompleteRecovery()).isEmpty();
    }
  }

  /**
   * Regression test for issue #8483: a leader that quarantined one of its own databases kept leadership, and nothing
   * could heal it. The resync a quarantine triggers is refused on the leader role (it cannot pull from itself), and
   * since #8468 the leader also refuses to serve the quarantined database to its followers, so until an operator moved
   * the leadership by hand the database could be installed nowhere.
   * <p>
   * A leader now hands leadership to a healthy peer: at once from the apply paths that raise the quarantine, and from
   * the health tick for a quarantine or read floor nothing raised in this term (restored from disk, left by an
   * incomplete install).
   */
  @Nested
  class QuarantinedLeaderHandsOffLeadership {

    // ---------------------------------------------------------------------------------------------
    // The apply paths that raise a quarantine hand leadership off at once
    // ---------------------------------------------------------------------------------------------

    // Regression for #8483: a leader that quarantined a database hands off leadership
    @Test
    void anApplyErrorThatQuarantinesADatabaseOnTheLeaderHandsLeadershipOff() {
      final FakeRaftHAServer raft = raftMock(true);
      final ArcadeStateMachine sm = new ArcadeStateMachine();
      sm.setRaftHAServer(raft);

      // An entry this node cannot decode: handleUnexpectedApplyError quarantines db-A (see Issue7495 test).
      final CompletableFuture<Message> future = sm.applyTransaction(txEntry(sm, "db-A", new byte[0], 5L));

      assertThatThrownBy(future::join).hasMessageContaining("per-database snapshot resync in progress");
      assertThat(sm.isDatabaseDiverged("db-A")).isTrue();
      assertThat(raft.calls("handOffLeadershipToResync")).singleElement().satisfies(args -> assertThat((String) args.get(0)).contains("'db-A'"));
    }

    // Regression for #8483: a leader that quarantined a database hands off leadership
    @Test
    void aSecondErrorOnTheSameQuarantineDoesNotAskAgain() {
      final FakeRaftHAServer raft = raftMock(true);
      final ArcadeStateMachine sm = new ArcadeStateMachine();
      sm.setRaftHAServer(raft);

      assertThatThrownBy(() -> sm.applyTransaction(txEntry(sm, "db-A", new byte[0], 5L)).join());
      assertThatThrownBy(() -> sm.applyTransaction(txEntry(sm, "db-A", new byte[0], 6L)).join());

      assertThat(raft.calls("handOffLeadershipToResync")).hasSize(1);
    }

    // Regression for #8483: a leader that quarantined a database hands off leadership
    @Test
    void anApplyErrorOnAFollowerDoesNotTouchTheLeadership() {
      final FakeRaftHAServer raft = raftMock(false);
      final ArcadeStateMachine sm = new ArcadeStateMachine();
      sm.setRaftHAServer(raft);

      assertThatThrownBy(() -> sm.applyTransaction(txEntry(sm, "db-A", new byte[0], 5L)).join());

      assertThat(sm.isDatabaseDiverged("db-A")).isTrue();
      assertThat(raft.calls("handOffLeadershipToResync")).isEmpty();
    }

    // Regression for #8483: a leader that quarantined a database hands off leadership
    @Test
    void aWalVersionGapOnTheLeaderHandsLeadershipOff() {
      final FakeRaftHAServer raft = raftMock(true);
      final TransactionManager txManager = mock(TransactionManager.class);
      when(txManager.applyChanges(any(), anyMap(), any(), anyBoolean())).thenThrow(new WALVersionGapException("gap"));
      final DatabaseInternal db = mock(DatabaseInternal.class);
      when(db.getTransactionManager()).thenReturn(txManager);
      final ArcadeStateMachine sm = new ArcadeStateMachine() {
        @Override
        DatabaseInternal databaseFor(final String databaseName) {
          return db;
        }
      };
      sm.setRaftHAServer(raft);

      final CompletableFuture<Message> future = sm.applyTransaction(txEntry(sm, "db-A", emptyWalTransaction(), 5L));

      assertThatThrownBy(future::join);
      assertThat(sm.quarantineCause("db-A")).isEqualTo(DivergenceCause.WAL_VERSION_GAP);
      assertThat(raft.calls("handOffLeadershipToResync")).singleElement().satisfies(args -> assertThat((String) args.get(0)).contains("'db-A'"));
    }

    // ---------------------------------------------------------------------------------------------
    // The health tick is the backstop for a quarantine nothing raised in this term
    // ---------------------------------------------------------------------------------------------

    // Regression for #8483: a leader that quarantined a database hands off leadership
    @Test
    void theHealthTickHandsLeadershipOffForAQuarantineTheLeaderAlreadyHeld(@TempDir final Path tempDir) throws Exception {
      final FakeRaftHAServer raft = raftMock(true);
      final ArcadeStateMachine sm = newStateMachine(tempDir);
      sm.setRaftHAServer(raft);
      try {
        // What a quarantine restored from disk looks like: recorded, with no apply path raising it in this JVM.
        sm.markStateDiverged("db-A", DivergenceCause.UNDECODABLE_LOG_ENTRY);
        assertThat(raft.calls("handOffLeadershipToResync")).isEmpty();

        sm.retryUnfilledSnapshotGap();

        assertThat(raft.calls("handOffLeadershipToResync")).singleElement().satisfies(args -> assertThat((String) args.get(0)).contains("db-A"));
      } finally {
        sm.close();
      }
    }

    // Regression for #8483: a leader that quarantined a database hands off leadership
    /**
     * A node-wide read floor is the same state for a leader: reinitialize() publishes it when the snapshot marker runs
     * ahead of what was applied, and only a resync from another node clears it.
     */
    @Test
    void theHealthTickHandsLeadershipOffForAReadFloorTheLeaderHolds(@TempDir final Path tempDir) throws Exception {
      final FakeRaftHAServer raft = raftMock(true);
      final ArcadeStateMachine sm = newStateMachine(tempDir);
      sm.setRaftHAServer(raft);
      try {
        final Field f = ArcadeStateMachine.class.getDeclaredField("staleSnapshotAppliedFloor");
        f.setAccessible(true);
        ((AtomicLong) f.get(sm)).set(42L);

        sm.retryUnfilledSnapshotGap();

        assertThat(raft.calls("handOffLeadershipToResync")).singleElement().satisfies(args -> assertThat((String) args.get(0)).contains("read floor at 42"));
      } finally {
        sm.close();
      }
    }

    // Regression for #8483: a leader that quarantined a database hands off leadership
    @Test
    void theHealthTickLeavesAHealthyLeaderAlone(@TempDir final Path tempDir) throws Exception {
      final FakeRaftHAServer raft = raftMock(true);
      final ArcadeStateMachine sm = newStateMachine(tempDir);
      sm.setRaftHAServer(raft);
      try {
        sm.retryUnfilledSnapshotGap();

        assertThat(raft.calls("handOffLeadershipToResync")).isEmpty();
      } finally {
        sm.close();
      }
    }

    // Regression for #8483: a leader that quarantined a database hands off leadership
    @Test
    void theHealthTickNeverHandsOffFromAFollower(@TempDir final Path tempDir) throws Exception {
      final FakeRaftHAServer raft = raftMock(false);
      final ArcadeStateMachine sm = newStateMachine(tempDir);
      sm.setRaftHAServer(raft);
      try {
        sm.markStateDiverged("db-A", DivergenceCause.APPLY_ERROR);
        sm.retryUnfilledSnapshotGap();

        assertThat(raft.calls("handOffLeadershipToResync")).isEmpty();
      } finally {
        sm.close();
      }
    }

    // ---------------------------------------------------------------------------------------------
    // RaftHAServer's gates
    // ---------------------------------------------------------------------------------------------

    // Regression for #8483: a leader that quarantined a database hands off leadership
    @Test
    void aHandoffIsAdmittedOncePerCooldown() {
      final AtomicLong last = new AtomicLong();
      final long now = 1_000_000L;

      assertThat(PeerChannelRecovery.admitQuarantineHandoff(last, now)).isTrue();
      assertThat(PeerChannelRecovery.admitQuarantineHandoff(last, now + 1)).isFalse();
      assertThat(PeerChannelRecovery.admitQuarantineHandoff(last, now + 9 * 60_000L)).isFalse();
      assertThat(PeerChannelRecovery.admitQuarantineHandoff(last, now + 10 * 60_000L)).isTrue();
      assertThat(last.get()).isEqualTo(now + 10 * 60_000L);
    }

    // Regression for #8483: a leader that quarantined a database hands off leadership
    /**
     * The window is taken only when a transfer is about to be attempted (code review on PR #8531): a handoff dropped
     * because no other peer is configured yet, or because this node no longer leads, must not suppress the one that
     * becomes possible when a peer joins moments later.
     */
    @Test
    void aHandoffWithNoPeerOrNoLeadershipLeavesTheWindowIntact() {
      final AtomicLong lastHandoff = new AtomicLong();
      final AtomicLong lastNoPeerReport = new AtomicLong();
      final long t0 = 1_000_000L;

      assertThat(PeerChannelRecovery.decideQuarantineHandoff(false, true, lastHandoff, lastNoPeerReport, t0))
          .isEqualTo(RaftHAServer.QuarantineHandoff.NOT_LEADER);
      assertThat(PeerChannelRecovery.decideQuarantineHandoff(true, false, lastHandoff, lastNoPeerReport, t0))
          .isEqualTo(RaftHAServer.QuarantineHandoff.NO_PEER_REPORT);
      assertThat(lastHandoff.get()).as("neither drop may take the handoff window").isZero();

      // A peer joins 30 s later: the handoff goes ahead at once.
      assertThat(PeerChannelRecovery.decideQuarantineHandoff(true, true, lastHandoff, lastNoPeerReport, t0 + 30_000L))
          .isEqualTo(RaftHAServer.QuarantineHandoff.TRANSFER);
      // ...and the next tick is inside the window.
      assertThat(PeerChannelRecovery.decideQuarantineHandoff(true, true, lastHandoff, lastNoPeerReport, t0 + 33_000L))
          .isEqualTo(RaftHAServer.QuarantineHandoff.COOLDOWN);
    }

    /** A peer-less leader reports the operator action once per window, not on every health tick. */
    // Regression for #8483: a leader that quarantined a database hands off leadership
    @Test
    void theNoPeerReportIsThrottled() {
      final AtomicLong lastHandoff = new AtomicLong();
      final AtomicLong lastNoPeerReport = new AtomicLong();
      final long t0 = 1_000_000L;

      assertThat(PeerChannelRecovery.decideQuarantineHandoff(true, false, lastHandoff, lastNoPeerReport, t0))
          .isEqualTo(RaftHAServer.QuarantineHandoff.NO_PEER_REPORT);
      assertThat(PeerChannelRecovery.decideQuarantineHandoff(true, false, lastHandoff, lastNoPeerReport, t0 + 3_000L))
          .isEqualTo(RaftHAServer.QuarantineHandoff.NO_PEER);
      assertThat(PeerChannelRecovery.decideQuarantineHandoff(true, false, lastHandoff, lastNoPeerReport, t0 + 10 * 60_000L))
          .isEqualTo(RaftHAServer.QuarantineHandoff.NO_PEER_REPORT);
    }

    // Regression for #8483: a leader that quarantined a database hands off leadership
    @Test
    void aSingleNodeClusterHasNoPeerToHandLeadershipTo() {
      final RaftPeerId self = RaftPeerId.valueOf("self");

      assertThat(PeerChannelRecovery.hasHandoffTarget(List.of(peer("self", 0)), self, null)).isFalse();
      assertThat(PeerChannelRecovery.hasHandoffTarget(List.of(), self, null)).isFalse();
      assertThat(PeerChannelRecovery.hasHandoffTarget(List.of(peer("self", 0), peer("other", 0)), self, null)).isTrue();
    }

    // Regression for #8483: a leader that quarantined a database hands off leadership
    /**
     * A peer that is present but not eligible is no target either (code review on PR #8531): handing off to a lagging
     * follower falls back to a Ratis step-down that re-elects this node, an election for nothing.
     */
    @Test
    void aLaggingPeerIsNoHandoffTarget() {
      final RaftPeerId self = RaftPeerId.valueOf("self");
      final ClusterMonitor monitor = new ClusterMonitor(10L);
      monitor.updateLeaderCommitIndex(10_000L);
      monitor.updateReplicaMatchIndex("other", 0L, 0L); // lag 10000 > threshold 10

      assertThat(PeerChannelRecovery.hasHandoffTarget(List.of(peer("self", 0), peer("other", 0)), self, monitor)).isFalse();

      monitor.updateReplicaMatchIndex("other", 10_000L, 0L); // caught up
      assertThat(PeerChannelRecovery.hasHandoffTarget(List.of(peer("self", 0), peer("other", 0)), self, monitor)).isTrue();
    }

    // ---------------------------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------------------------

    private static RaftPeer peer(final String id, final int priority) {
      return RaftPeer.newBuilder().setId(RaftPeerId.valueOf(id)).setAddress(id + ":2434").setPriority(priority).build();
    }

    private static FakeRaftHAServer raftMock(final boolean leader) {
      final FakeRaftHAServer raft = FakeRaftHAServer.detached();
      raft.leader(leader);
      final RaftPeerId leaderId = RaftPeerId.valueOf(leader ? "self" : "peer-b");
      raft.leaderId(leaderId);
      raft.localPeerId(RaftPeerId.valueOf("self"));
      raft.peerHttpAddress(leaderId, "peer-b:2480");
      return raft;
    }

    private static ArcadeStateMachine newStateMachine(final Path tempDir) {
      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, tempDir.resolve("databases").toString());
      final ArcadeStateMachine sm = new ArcadeStateMachine();
      sm.setServer(new ArcadeDBServer(config));
      return sm;
    }

    /** A well-formed WAL payload of a transaction with no pages, so the apply reaches applyChanges. */
    private static byte[] emptyWalTransaction() {
      final ByteBuffer buf = ByteBuffer.allocate(2 * Long.BYTES + 2 * Integer.BYTES);
      buf.putLong(1L); // txId
      buf.putLong(0L); // timestamp
      buf.putInt(0);   // pageCount
      buf.putInt(0);   // segmentSize
      return buf.array();
    }

    private static TransactionContext txEntry(final ArcadeStateMachine sm, final String databaseName,
        final byte[] walData, final long index) {
      final ByteString payload = RaftLogEntryCodec.encodeTxEntry(databaseName, walData, Collections.emptyMap());
      final LogEntryProto logEntry = LogEntryProto.newBuilder()
          .setTerm(1L)
          .setIndex(index)
          .setStateMachineLogEntry(StateMachineLogEntryProto.newBuilder().setLogData(payload).build())
          .build();
      return TransactionContext.newBuilder().setStateMachine(sm).setLogEntry(logEntry).build();
    }
  }

  /**
   * Regression test for issue #11531: a leader-driven snapshot install that gives up on a database must not register its
   * snapshot marker before the quarantine and the read floor of that database are on disk.
   * <p>
   * The install registered the marker first and only then wrote {@code .raft/applied-index} with the final quarantine
   * ({@code SNAPSHOT_INSTALL_INCOMPLETE}) and read floors. A failed write was logged and ignored, the install reported
   * success, and Ratis purged the log up to the marker. A restart before a later write succeeded brought the given-up
   * database back with no quarantine and no floor, and nothing left in the log to replay into it.
   * <p>
   * What must hold instead: the final state is written before the marker, and an install that cannot make the quarantine
   * durable fails, so Ratis neither records the boundary nor purges the log and retries the install.
   */
  @Nested
  class SnapshotInstallQuarantineWrite {

    private static final String LEADER_PEER_ID    = "peer-b_2434";
    private static final String LOCAL_HTTP        = "localhost:2480";
    private static final long   FIRST_LOG_INDEX   = 5_000L;
    private static final long   SNAPSHOT_INDEX    = FIRST_LOG_INDEX - 1;
    private static final long   PERSISTED_APPLIED = 100L;
    private static final String STALE_DB          = "db-stale";
    private static final String HEALTHY_DB        = "db-ok";

    // Regression for #11531: the snapshot marker is registered only after quarantine and read floor are on disk
    /**
     * The core regression: the quarantine cannot be written, so the install fails and leaves no marker behind. Before the
     * fix the future completed with the snapshot index and the marker was registered.
     */
    @Test
    void anInstallThatCannotPersistItsQuarantineFailsBeforeTheMarker(@TempDir final Path tempDir) throws Exception {
      final ArcadeStateMachine sm = newStateMachine(tempDir, Set.of(STALE_DB, HEALTHY_DB));
      replaceReconciler(sm, new StubReconciler(Set.of(STALE_DB)));
      sm.setRaftHAServer(followerRaft());
      try {
        sm.writePersistedAppliedIndex(PERSISTED_APPLIED, STALE_DB);
        sm.writePersistedAppliedIndex(PERSISTED_APPLIED, HEALTHY_DB);
        blockAppliedIndexFile(tempDir);

        assertThatThrownBy(() -> sm.notifyInstallSnapshotFromLeader(leaderRoleInfo(),
            TermIndex.valueOf(9L, FIRST_LOG_INDEX)).get())
            .as("an install whose quarantine is in memory only must not be reported to Ratis as installed")
            .isInstanceOf(ExecutionException.class)
            .hasStackTraceContaining(STALE_DB);

        final SnapshotInfo latest = sm.getLatestSnapshot();
        assertThat(latest == null ? -1L : latest.getIndex())
            .as("and no marker may be registered: Ratis would purge the log up to it")
            .isNotEqualTo(SNAPSHOT_INDEX);

        assertThat(sm.isDatabaseDiverged(STALE_DB))
            .as("the quarantine still holds in memory: the failure is fail-closed")
            .isTrue();
        assertThat(sm.getDatabaseAppliedFloor(STALE_DB)).isEqualTo(PERSISTED_APPLIED);
      } finally {
        sm.close();
      }
    }

    // Regression for #11531: the snapshot marker is registered only after quarantine and read floor are on disk
    /**
     * Once the disk recovers, the retried install registers the marker, and by then the quarantine and the floor of the
     * given-up database are already on disk, where a restart reads them back.
     */
    @Test
    void theRetriedInstallRegistersTheMarkerWithTheQuarantineAlreadyOnDisk(@TempDir final Path tempDir) throws Exception {
      final ArcadeStateMachine sm = newStateMachine(tempDir, Set.of(STALE_DB, HEALTHY_DB));
      replaceReconciler(sm, new StubReconciler(Set.of(STALE_DB)));
      sm.setRaftHAServer(followerRaft());
      try {
        sm.writePersistedAppliedIndex(PERSISTED_APPLIED, STALE_DB);
        sm.writePersistedAppliedIndex(PERSISTED_APPLIED, HEALTHY_DB);
        final Path appliedIndex = blockAppliedIndexFile(tempDir);

        assertThatThrownBy(() -> sm.notifyInstallSnapshotFromLeader(leaderRoleInfo(),
            TermIndex.valueOf(9L, FIRST_LOG_INDEX)).get()).isInstanceOf(ExecutionException.class);

        FileUtils.deleteRecursively(appliedIndex.toFile());

        final TermIndex installed = sm.notifyInstallSnapshotFromLeader(leaderRoleInfo(),
            TermIndex.valueOf(9L, FIRST_LOG_INDEX)).get();
        assertThat(installed.getIndex()).isEqualTo(SNAPSHOT_INDEX);
        assertThat(sm.getLatestSnapshot().getIndex()).isEqualTo(SNAPSHOT_INDEX);

        final JSONObject persisted = new JSONObject(Files.readString(appliedIndex));
        assertThat(persisted.getJSONObject("quarantine", new JSONObject()).getString(STALE_DB, null))
            .as("the given-up database is quarantined on disk")
            .isEqualTo("SNAPSHOT_INSTALL_INCOMPLETE");
        assertThat(persisted.getJSONObject("floors", new JSONObject()).getLong(STALE_DB, -1L))
            .as("with the floor of the position it genuinely reached")
            .isEqualTo(PERSISTED_APPLIED);
        assertThat(persisted.getJSONObject("db").getLong(HEALTHY_DB, -1L))
            .as("and the refreshed database is recorded at the snapshot index")
            .isEqualTo(SNAPSHOT_INDEX);
        assertThat(persisted.getJSONObject("db").getLong(STALE_DB, -1L)).isEqualTo(PERSISTED_APPLIED);
      } finally {
        sm.close();
      }
    }

    // Regression for #11531: the snapshot marker is registered only after quarantine and read floor are on disk
    /**
     * Control: an install that gave up on nothing publishes no quarantine, so a failed write of its positions does not fail
     * it. The marker it registers is ahead of the persisted position, which {@code reinitialize()} detects on the next
     * restart as a stale-snapshot gap and resyncs (issue #6111): failing it here would only loop Ratis on a full disk.
     */
    @Test
    void aCleanInstallStillCompletesWhenOnlyItsPositionsCannotBeWritten(@TempDir final Path tempDir) throws Exception {
      final ArcadeStateMachine sm = newStateMachine(tempDir, Set.of(STALE_DB, HEALTHY_DB));
      replaceReconciler(sm, new StubReconciler(Set.of()));
      sm.setRaftHAServer(followerRaft());
      try {
        sm.writePersistedAppliedIndex(PERSISTED_APPLIED, STALE_DB);
        sm.writePersistedAppliedIndex(PERSISTED_APPLIED, HEALTHY_DB);
        blockAppliedIndexFile(tempDir);

        final TermIndex installed = sm.notifyInstallSnapshotFromLeader(leaderRoleInfo(),
            TermIndex.valueOf(9L, FIRST_LOG_INDEX)).get();
        assertThat(installed.getIndex()).isEqualTo(SNAPSHOT_INDEX);
        assertThat(sm.getLatestSnapshot().getIndex()).isEqualTo(SNAPSHOT_INDEX);
        assertThat(sm.isResyncInProgress()).isFalse();
      } finally {
        sm.close();
      }
    }

    /** A non-empty directory where the applied-index file goes: the atomic replace of it fails on every write. */
    private static Path blockAppliedIndexFile(final Path tempDir) throws IOException {
      final Path appliedIndex = tempDir.resolve("databases").resolve(".raft").resolve("applied-index");
      Files.deleteIfExists(appliedIndex);
      Files.writeString(Files.createDirectories(appliedIndex).resolve("blocker"), "x");
      return appliedIndex;
    }

    private static class StubReconciler extends DatabaseReconciler {
      private final Set<String> givenUp;

      StubReconciler(final Set<String> givenUp) {
        this.givenUp = givenUp;
      }

      @Override
      ReconcileFromLeaderResult reconcileDatabasesFromLeader(final String leaderPeerId, final String leaderHttpAddr,
          final String leaderHttpsAddr, final String clusterToken, final long installedBoundaryIndex) {
        return new ReconcileFromLeaderResult(givenUp, null);
      }
    }

    private static FakeRaftHAServer followerRaft() {
      final FakeRaftHAServer raft = FakeRaftHAServer.detached();
      raft.leader(false);
      raft.leaderId(RaftPeerId.valueOf(LEADER_PEER_ID));
      raft.peerHttpAddress(RaftPeerId.valueOf(LEADER_PEER_ID), "peer-b:2480");
      raft.localHttpAddress(LOCAL_HTTP);
      return raft;
    }

    private static RaftProtos.RoleInfoProto leaderRoleInfo() {
      return RaftProtos.RoleInfoProto.newBuilder()
          .setFollowerInfo(RaftProtos.FollowerInfoProto.newBuilder()
              .setLeaderInfo(RaftProtos.ServerRpcProto.newBuilder()
                  .setId(RaftProtos.RaftPeerProto.newBuilder()
                      .setId(ByteString.copyFromUtf8(LEADER_PEER_ID)))))
          .build();
    }

    private static ArcadeStateMachine newStateMachine(final Path tempDir, final Set<String> databaseNames)
        throws IOException {
      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, tempDir.resolve("databases").toString());
      config.setValue(GlobalConfiguration.HA_AUTO_ACQUIRE_DATABASES, false);
      config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRIES, 0);

      final FakeArcadeDBServer server = FakeArcadeDBServer.create();
      server.returns("getConfiguration", config);
      server.returns("getDatabaseNames", databaseNames);

      final ArcadeStateMachine sm = new ArcadeStateMachine();
      sm.setServer(server);
      sm.initialize(stubRaftServer(), RaftGroupId.valueOf(UUID.randomUUID()),
          RaftStorage.newBuilder()
              .setDirectory(tempDir.resolve("raft").toFile())
              .setOption(RaftStorage.StartupOption.FORMAT)
              .build());
      return sm;
    }

    private static RaftServer stubRaftServer() {
      return (RaftServer) Proxy.newProxyInstance(
          QuarantineEntryTest.SnapshotInstallQuarantineWrite.class.getClassLoader(),
          new Class<?>[] { RaftServer.class },
          (proxy, method, args) -> {
            if ("getId".equals(method.getName()))
              return RaftPeerId.valueOf("test-peer");
            if ("close".equals(method.getName()) || "start".equals(method.getName()))
              return null;
            throw new UnsupportedOperationException("Stub: " + method.getName());
          });
    }

    private static void replaceReconciler(final ArcadeStateMachine sm, final DatabaseReconciler reconciler)
        throws Exception {
      final Field f = ArcadeStateMachine.class.getDeclaredField("reconciler");
      f.setAccessible(true);
      f.set(sm, reconciler);
    }
  }
}
