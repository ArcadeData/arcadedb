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
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.ProtocolContext;
import com.arcadedb.database.RID;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.FakeArcadeDBServer;
import com.arcadedb.server.ServerControlPlane;
import com.arcadedb.utility.FileUtils;
import com.sun.net.httpserver.HttpServer;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.storage.RaftStorage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Quarantine persistence across restarts, refusal of clients and snapshot requests, and the all-voters-quarantined case.
 */
class QuarantinePersistenceAndRefusalTest {

  /**
   * Regression tests for issue #7735.
   * <p>
   * A quarantine ({@code divergedDatabases}) deliberately does NOT advance {@code lastAppliedIndex} for the entry
   * that tripped it - {@code applyTransaction} returns a failed future before the {@code getAndSet} - while every
   * LATER committed entry does advance it. The only thing standing between that and a permanently short replica
   * was an in-memory {@code ConcurrentHashMap} that nothing persisted and {@code reinitialize()} did not
   * reconstruct, so a restart produced a node that was {@code RUNNING}, answered 200 on {@code /api/v1/ready},
   * reported {@code alerts: []} and was missing a committed mutation. {@code takeSnapshot()} made that permanent
   * rather than self-correcting: it had no divergence guard, so a quarantined node still checkpointed past the
   * skipped index and authorised Ratis to purge the log through it.
   * <p>
   * The invariant these tests pin: <b>a database quarantined from the committed Raft log stays quarantined across
   * a restart, and the Raft log is never checkpointed past the entry the quarantine skipped.</b>
   */
  @Nested
  class QuarantineSurvivesRestart {

    private static final String DB_A = "orders";
    private static final String DB_B = "invoices";

    // ---------------------------------------------------------------------------------------------
    // One test per quarantine entry point: each must survive the restart under its own cause
    // ---------------------------------------------------------------------------------------------

    // Regression for #7735: a quarantine survives a restart
    /**
     * {@code handleUnexpectedApplyError} on an entry this node cannot decode: the trigger issue #7495 added and
     * the one the report was filed against, because it is the trigger most likely to get a pod restarted.
     */
    @Test
    void anUndecodableEntryQuarantineIsStillRecordedAfterARestart(@TempDir final Path tempDir) throws Exception {
      quarantineSurvives(tempDir, DivergenceCause.UNDECODABLE_LOG_ENTRY);
    }

    /** {@code applyReplicatedTransaction}'s WAL version gap (issues #4740, #4797). */
    // Regression for #7735: a quarantine survives a restart
    @Test
    void aWalVersionGapQuarantineIsStillRecordedAfterARestart(@TempDir final Path tempDir) throws Exception {
      quarantineSurvives(tempDir, DivergenceCause.WAL_VERSION_GAP);
    }

    /** {@code handleUnexpectedApplyError}'s general apply failure (issue #4797). */
    // Regression for #7735: a quarantine survives a restart
    @Test
    void anApplyErrorQuarantineIsStillRecordedAfterARestart(@TempDir final Path tempDir) throws Exception {
      quarantineSurvives(tempDir, DivergenceCause.APPLY_ERROR);
    }

    /** {@code settleDivergedStateAfterInstall}'s incomplete install (issue #6760). */
    // Regression for #7735: a quarantine survives a restart
    @Test
    void anIncompleteSnapshotInstallQuarantineIsStillRecordedAfterARestart(@TempDir final Path tempDir)
        throws Exception {
      quarantineSurvives(tempDir, DivergenceCause.SNAPSHOT_INSTALL_INCOMPLETE);
    }

    // Regression for #7735: a quarantine survives a restart
    /**
     * The whole point of persisting it: the restarted node must refuse readiness and say so in the status
     * document, instead of coming back {@code alerts: []} on a database that is missing a committed entry.
     */
    @Test
    void theRestartedNodeIsUnreadyAndSaysWhyRatherThanReportingNoAlerts(@TempDir final Path tempDir)
        throws Exception {
      final ArcadeStateMachine before = newStateMachine(tempDir);
      try {
        before.markStateDiverged(DB_A, DivergenceCause.UNDECODABLE_LOG_ENTRY);
        assertThat(before.isResyncInProgress()).isTrue();
      } finally {
        before.close();
      }

      final ArcadeStateMachine after = newStateMachine(tempDir);
      try {
        assertThat(after.isResyncInProgress())
            .as("readiness must still answer 503 after the restart, not 200 on a short database")
            .isTrue();

        final ArcadeStateMachine.LocalResyncState state = after.getLocalResyncState();
        assertThat(state.inProgress()).isTrue();
        assertThat(state.divergedDatabases()).containsExactly(DB_A);
        assertThat(state.divergenceCauses()).containsEntry(DB_A, DivergenceCause.UNDECODABLE_LOG_ENTRY);
      } finally {
        after.close();
      }
    }

    // ---------------------------------------------------------------------------------------------
    // takeSnapshot(): the log must stay replayable past the entry the quarantine skipped
    // ---------------------------------------------------------------------------------------------

    // Regression for #7735: a quarantine survives a restart
    /**
     * Before the fix this returned the applied index, which covers entries that came AFTER one this node never
     * applied, and Ratis was free to purge the log through it. That is what turned a replayable skip into a
     * permanent one.
     */
    @Test
    void takeSnapshotIsRefusedWhileADatabaseIsQuarantined(@TempDir final Path tempDir) throws Exception {
      final RaftStorage raftStorage = newFormattedStorage(tempDir);
      final ArcadeStateMachine sm = new ArcadeStateMachine();
      try {
        sm.initialize(stubRaftServer(), RaftGroupId.valueOf(UUID.randomUUID()), raftStorage);
        setLastApplied(sm, 7L, 4242L);

        sm.markStateDiverged(DB_A, DivergenceCause.UNDECODABLE_LOG_ENTRY);

        assertThat(sm.takeSnapshot())
            .as("a checkpoint here would authorise Ratis to purge the entry the quarantine skipped")
            .isEqualTo(RaftLog.INVALID_LOG_INDEX);
        assertThat(sm.getStateMachineStorage().getLatestSnapshot())
            .as("and no marker may be left behind either")
            .isNull();
      } finally {
        sm.close();
        raftStorage.close();
      }
    }

    /** The refusal is not a one-way door: once the resync has cleared the quarantine, checkpointing resumes. */
    // Regression for #7735: a quarantine survives a restart
    @Test
    void takeSnapshotResumesOnceTheResyncClearedTheQuarantine(@TempDir final Path tempDir) throws Exception {
      final RaftStorage raftStorage = newFormattedStorage(tempDir);
      final ArcadeStateMachine sm = new ArcadeStateMachine();
      try {
        sm.initialize(stubRaftServer(), RaftGroupId.valueOf(UUID.randomUUID()), raftStorage);
        setLastApplied(sm, 7L, 4242L);

        sm.markStateDiverged(DB_A, DivergenceCause.WAL_VERSION_GAP);
        assertThat(sm.takeSnapshot()).isEqualTo(RaftLog.INVALID_LOG_INDEX);

        sm.clearDivergedDatabase(DB_A);

        assertThat(sm.takeSnapshot()).isEqualTo(4242L);
        assertThat(sm.getStateMachineStorage().getLatestSnapshot()).isNotNull();
      } finally {
        sm.close();
        raftStorage.close();
      }
    }

    // ---------------------------------------------------------------------------------------------
    // The inverse bug: a healed database must not come back quarantined
    // ---------------------------------------------------------------------------------------------

    /** A targeted resync clears one database; the other one's quarantine must survive, and only that one. */
    // Regression for #7735: a quarantine survives a restart
    @Test
    void aTargetedResyncDropsOnlyItsOwnDatabaseFromTheDurableQuarantine(@TempDir final Path tempDir)
        throws Exception {
      final ArcadeStateMachine before = newStateMachine(tempDir);
      try {
        before.markStateDiverged(DB_A, DivergenceCause.WAL_VERSION_GAP);
        before.markStateDiverged(DB_B, DivergenceCause.APPLY_ERROR);
        before.clearDivergedDatabase(DB_A);
      } finally {
        before.close();
      }

      final ArcadeStateMachine after = newStateMachine(tempDir);
      try {
        assertThat(after.isDatabaseDiverged(DB_A)).as("the resync healed it, so it must not come back").isFalse();
        assertThat(after.isDatabaseDiverged(DB_B)).isTrue();
        assertThat(after.getLocalResyncState().divergedDatabases()).containsExactly(DB_B);
      } finally {
        after.close();
      }
    }

    /** A full resync reinstalls every database, so nothing may be quarantined after the restart. */
    // Regression for #7735: a quarantine survives a restart
    @Test
    void aFullResyncLeavesNoQuarantineBehindForTheNextStart(@TempDir final Path tempDir) throws Exception {
      final ArcadeStateMachine before = newStateMachine(tempDir);
      try {
        before.markStateDiverged(DB_A, DivergenceCause.WAL_VERSION_GAP);
        before.markStateDiverged(DB_B, DivergenceCause.UNDECODABLE_LOG_ENTRY);
        before.clearDivergedState();
      } finally {
        before.close();
      }

      final ArcadeStateMachine after = newStateMachine(tempDir);
      try {
        assertThat(after.getLocalResyncState().divergedDatabases()).isEmpty();
        assertThat(after.isResyncInProgress()).as("a healed node must be ready again").isFalse();
      } finally {
        after.close();
      }
    }

    // ---------------------------------------------------------------------------------------------
    // The file it shares with the applied index
    // ---------------------------------------------------------------------------------------------

    // Regression for #7735: a quarantine survives a restart
    /**
     * The quarantine and the applied position it qualifies travel in the SAME atomic write, so a crash cannot
     * leave a file that says "applied up to N" without saying "and this database was skipped on the way".
     */
    @Test
    void theQuarantineAndTheAppliedPositionAreRestoredFromOneFile(@TempDir final Path tempDir) throws Exception {
      final ArcadeStateMachine before = newStateMachine(tempDir);
      try {
        before.writePersistedAppliedIndex(4242L, DB_A);
        before.markStateDiverged(DB_A, DivergenceCause.UNDECODABLE_LOG_ENTRY);
      } finally {
        before.close();
      }

      final JSONObject persisted = new JSONObject(Files.readString(appliedIndexFile(tempDir)));
      assertThat(persisted.getLong("global", -1)).isEqualTo(4242L);
      assertThat(persisted.getJSONObject("quarantine").getString(DB_A, null))
          .isEqualTo(DivergenceCause.UNDECODABLE_LOG_ENTRY.name());

      final ArcadeStateMachine after = newStateMachine(tempDir);
      try {
        assertThat(after.readPersistedAppliedIndex()).isEqualTo(4242L);
        assertThat(after.readPersistedAppliedIndex(DB_A)).isEqualTo(4242L);
        assertThat(after.isDatabaseDiverged(DB_A)).isTrue();
      } finally {
        after.close();
      }
    }

    /** A healthy node writes the file it always wrote: the key only appears when something is quarantined. */
    // Regression for #7735: a quarantine survives a restart
    @Test
    void aHealthyNodeWritesNoQuarantineKeyAtAll(@TempDir final Path tempDir) throws Exception {
      final ArcadeStateMachine sm = newStateMachine(tempDir);
      try {
        sm.writePersistedAppliedIndex(11L, DB_A);

        final JSONObject persisted = new JSONObject(Files.readString(appliedIndexFile(tempDir)));
        assertThat(persisted.has("quarantine")).isFalse();
      } finally {
        sm.close();
      }
    }

    /** A file written by a build that predates this change carries no quarantine, and reads as none. */
    // Regression for #7735: a quarantine survives a restart
    @Test
    void aFileWrittenBeforeThisFixRestoresNoQuarantine(@TempDir final Path tempDir) throws Exception {
      writeAppliedIndexFile(tempDir, "{\"global\":99,\"db\":{\"" + DB_A + "\":99}}");

      final ArcadeStateMachine sm = newStateMachine(tempDir);
      try {
        assertThat(sm.readPersistedAppliedIndex()).isEqualTo(99L);
        assertThat(sm.isDatabaseDiverged(DB_A)).isFalse();
        assertThat(sm.isResyncInProgress()).isFalse();
      } finally {
        sm.close();
      }
    }

    // Regression for #7735: a quarantine survives a restart
    /**
     * A cause name this build does not know - written by a newer node - degrades to {@code APPLY_ERROR} rather
     * than dropping the entry. The cause only changes what the alert SAYS; dropping it would reinstate exactly
     * the silent divergence the persistence exists to prevent.
     */
    @Test
    void anUnknownCauseKeepsTheQuarantineRatherThanDroppingIt(@TempDir final Path tempDir) throws Exception {
      writeAppliedIndexFile(tempDir,
          "{\"global\":99,\"db\":{},\"quarantine\":{\"" + DB_A + "\":\"A_CAUSE_FROM_THE_FUTURE\"}}");

      final ArcadeStateMachine sm = newStateMachine(tempDir);
      try {
        assertThat(sm.isDatabaseDiverged(DB_A)).as("an unreadable cause must never silence the quarantine").isTrue();
        assertThat(sm.getLocalResyncState().divergenceCauses())
            .containsEntry(DB_A, DivergenceCause.APPLY_ERROR);
      } finally {
        sm.close();
      }
    }

    // Regression for #7735: a quarantine survives a restart
    /**
     * A restored quarantine keeps the cause it was recorded with, and a later mark on the same database does not
     * overwrite it - the same first-cause-wins rule {@code markStateDiverged} has had since issue #7741, extended
     * across the restart. The restored cause IS the first one: it describes the failure that quarantined the
     * database, while every mark after it comes from an entry that hit the same wall on a database already
     * waiting for a resync.
     */
    @Test
    void aRestoredQuarantineKeepsTheCauseItWasRecordedWith(@TempDir final Path tempDir) throws Exception {
      writeAppliedIndexFile(tempDir,
          "{\"global\":99,\"db\":{},\"quarantine\":{\"" + DB_A + "\":\"WAL_VERSION_GAP\"}}");

      final ArcadeStateMachine sm = newStateMachine(tempDir);
      try {
        sm.markStateDiverged(DB_A, DivergenceCause.UNDECODABLE_LOG_ENTRY);

        assertThat(sm.getLocalResyncState().divergenceCauses())
            .as("the first cause is the one that describes what went wrong")
            .containsEntry(DB_A, DivergenceCause.WAL_VERSION_GAP);
        assertThat(sm.isDatabaseDiverged(DB_A)).isTrue();
      } finally {
        sm.close();
      }
    }

    // ---------------------------------------------------------------------------------------------
    // The batch writer the snapshot-install path uses
    // ---------------------------------------------------------------------------------------------

    // Regression for #7735: a quarantine survives a restart
    /**
     * A snapshot install learns about every database it gave up on at once, so it quarantines them in one write
     * rather than rewriting the applied-index file once per database (code review on PR #8146). The batch has to
     * be as durable as the single-database writer.
     */
    @Test
    void aBatchQuarantineIsWrittenOnceAndRestoresEveryDatabase(@TempDir final Path tempDir) throws Exception {
      final ArcadeStateMachine before = newStateMachine(tempDir);
      try {
        before.settleDivergedStateAfterInstall(Set.of(DB_A, DB_B), 100L);
      } finally {
        before.close();
      }

      final JSONObject quarantine = new JSONObject(Files.readString(appliedIndexFile(tempDir)))
          .getJSONObject("quarantine");
      assertThat(quarantine.keySet()).as("one file, both databases").containsExactlyInAnyOrder(DB_A, DB_B);

      final ArcadeStateMachine after = newStateMachine(tempDir);
      try {
        assertThat(after.getLocalResyncState().divergedDatabases()).containsExactlyInAnyOrder(DB_A, DB_B);
        assertThat(after.getLocalResyncState().divergenceCauses())
            .containsEntry(DB_A, DivergenceCause.SNAPSHOT_INSTALL_INCOMPLETE)
            .containsEntry(DB_B, DivergenceCause.SNAPSHOT_INSTALL_INCOMPLETE);
      } finally {
        after.close();
      }
    }

    // ---------------------------------------------------------------------------------------------
    // Self-heal: a restored quarantine has no in-JVM resync behind it, so the tick must re-drive one
    // ---------------------------------------------------------------------------------------------

    // Regression for #7735: a quarantine survives a restart
    /**
     * {@code triggerDatabaseResync} runs once, at the mark, in the JVM that raised the quarantine. A quarantine
     * restored from disk has no such attempt behind it, so without the HealthMonitor tick the persisted mark
     * would hold the node out of the ready set for good. The tick used to return immediately unless a read floor
     * was outstanding, which a quarantine on its own never publishes.
     */
    @Test
    void theHealthTickRetriesAResyncForAQuarantineWithNoReadFloor(@TempDir final Path tempDir) throws Exception {
      final ArcadeStateMachine sm = newStateMachine(tempDir);
      sm.setRaftHAServer(followerRaftHAServerMock());
      try {
        assertThat(readLastRetryMs(sm)).as("nothing outstanding yet").isZero();

        sm.markStateDiverged(DB_A, DivergenceCause.UNDECODABLE_LOG_ENTRY);
        sm.retryUnfilledSnapshotGap();

        assertThat(readLastRetryMs(sm))
            .as("the tick must take its throttle slot and drive a resync for the quarantined database")
            .isNotZero();
      } finally {
        sm.close();
      }
    }

    /** ...and a node with nothing quarantined and no floor still does nothing, as it always did. */
    // Regression for #7735: a quarantine survives a restart
    @Test
    void theHealthTickStillDoesNothingOnAHealthyNode(@TempDir final Path tempDir) throws Exception {
      final ArcadeStateMachine sm = newStateMachine(tempDir);
      sm.setRaftHAServer(followerRaftHAServerMock());
      try {
        sm.retryUnfilledSnapshotGap();

        assertThat(readLastRetryMs(sm)).isZero();
      } finally {
        sm.close();
      }
    }

    // ---------------------------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------------------------

    /** Marks {@code DB_A} under {@code cause}, restarts, and asserts the mark and its cause both came back. */
    private static void quarantineSurvives(final Path tempDir, final DivergenceCause cause) throws Exception {
      final ArcadeStateMachine before = newStateMachine(tempDir);
      try {
        before.markStateDiverged(DB_A, cause);
        assertThat(before.isDatabaseDiverged(DB_A)).isTrue();
      } finally {
        before.close();
      }

      final ArcadeStateMachine after = newStateMachine(tempDir);
      try {
        assertThat(after.isDatabaseDiverged(DB_A))
            .as("the quarantine was in-memory only, so the restart used to forget it")
            .isTrue();
        assertThat(after.getLocalResyncState().divergenceCauses())
            .as("and the operator must still be told WHY, not just that something is wrong")
            .containsEntry(DB_A, cause);
      } finally {
        after.close();
      }
    }

    /**
     * A real (unstarted) {@link ArcadeDBServer} rooted at {@code tempDir}, so the state machine resolves
     * {@code .raft/applied-index} under it. Constructing a second one over the same directory is the restart.
     */
    private static ArcadeStateMachine newStateMachine(final Path tempDir) {
      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, tempDir.resolve("databases").toString());
      final ArcadeStateMachine sm = new ArcadeStateMachine();
      sm.setServer(new ArcadeDBServer(config));
      return sm;
    }

    private static Path appliedIndexFile(final Path tempDir) {
      return tempDir.resolve("databases").resolve(".raft").resolve("applied-index");
    }

    private static void writeAppliedIndexFile(final Path tempDir, final String content) throws IOException {
      final Path file = appliedIndexFile(tempDir);
      Files.createDirectories(file.getParent());
      Files.writeString(file, content);
    }

    private static RaftHAServer followerRaftHAServerMock() {
      final RaftPeerId leader = RaftPeerId.valueOf("peer-b_2434");
      final FakeRaftHAServer mockRaft = FakeRaftHAServer.detached();
      mockRaft.leader(false);
      mockRaft.leaderId(leader);
      mockRaft.peerHttpAddress(leader, "peer-b:2480");
      return mockRaft;
    }

    private static long readLastRetryMs(final ArcadeStateMachine sm) throws Exception {
      final Field f = ArcadeStateMachine.class.getDeclaredField("lastStaleSnapshotRetryMs");
      f.setAccessible(true);
      return ((AtomicLong) f.get(sm)).get();
    }

    private static RaftStorage newFormattedStorage(final Path dir) throws IOException {
      return RaftStorage.newBuilder()
          .setDirectory(dir.resolve("raft-storage").toFile())
          .setOption(RaftStorage.StartupOption.FORMAT)
          .build();
    }

    /**
     * Advances the applied position the way {@code applyTransaction} does: the private {@code lastAppliedIndex}
     * counter {@code takeSnapshot()} reads, plus the BaseStateMachine term/index that supplies the marker term.
     */
    private static void setLastApplied(final ArcadeStateMachine sm, final long term, final long index)
        throws Exception {
      final Field f = ArcadeStateMachine.class.getDeclaredField("lastAppliedIndex");
      f.setAccessible(true);
      ((AtomicLong) f.get(sm)).set(index);

      final Method m = findMethod(sm.getClass(), "updateLastAppliedTermIndex", long.class, long.class);
      m.setAccessible(true);
      m.invoke(sm, term, index);
    }

    private static Method findMethod(final Class<?> type, final String name, final Class<?>... params)
        throws NoSuchMethodException {
      for (Class<?> c = type; c != null; c = c.getSuperclass()) {
        try {
          return c.getDeclaredMethod(name, params);
        } catch (final NoSuchMethodException ignored) {
          // walk up to the superclass
        }
      }
      throw new NoSuchMethodException(name);
    }

    /**
     * Minimal {@link RaftServer} stub: {@code BaseStateMachine.initialize()} only needs a non-null
     * {@code getId()}; nothing else is reached on this path.
     */
    private static RaftServer stubRaftServer() {
      return (RaftServer) Proxy.newProxyInstance(
          QuarantinePersistenceAndRefusalTest.QuarantineSurvivesRestart.class.getClassLoader(),
          new Class<?>[] { RaftServer.class },
          (proxy, method, args) -> {
            if ("getId".equals(method.getName()))
              return RaftPeerId.valueOf("test-peer");
            if ("close".equals(method.getName()) || "start".equals(method.getName()))
              return null;
            throw new UnsupportedOperationException("Stub: " + method.getName());
          });
    }
  }

  /**
   * Regression tests for issue #9553: when every voter has quarantined the same database there is no copy left to resync
   * from, and the cluster used to say so only through one SEVERE line and one stalled resync loop per node.
   * <p>
   * Each node now advertises its quarantined databases on the capability poll every node already runs against every peer,
   * so any node can tell that every voter holds the database quarantined. That raises a dedicated critical alert naming
   * the way out, opens the accept-diverged override (#9449) on those nodes - the way out the alert names - and stops a
   * follower from retrying a resync from a leader that holds the same database quarantined and cannot serve it.
   */
  @Nested
  class AllVotersQuarantined {

    private static final String DB    = "chaos";
    private static final String OTHER = "other";

    private static final RaftPeerId SELF = RaftPeerId.valueOf("peer-a_2434");
    private static final RaftPeerId B    = RaftPeerId.valueOf("peer-b_2435");
    private static final RaftPeerId C    = RaftPeerId.valueOf("peer-c_2436");

    @TempDir
    Path root;

    private final List<HttpServer> stubs = new ArrayList<>();

    // -- the wire: a node says what it holds quarantined --------------------------------------------------------------

    // Regression for #9553: every voter quarantined the same database
    @Test
    void aNodeAdvertisesItsQuarantinedDatabasesSorted() {
      final JSONObject json = PostCapabilitiesHandler.advertisement(B.toString(), Set.of(), false, Set.of(OTHER, DB));

      assertThat(json.getJSONArray(PostCapabilitiesHandler.QUARANTINED).toList()).containsExactly(DB, OTHER);
    }

    // Regression for #9553: every voter quarantined the same database
    @Test
    void aHealthyNodesDocumentCarriesNoQuarantineField() {
      assertThat(PostCapabilitiesHandler.advertisement(B.toString(), Set.of(), false, Set.of())
          .has(PostCapabilitiesHandler.QUARANTINED)).isFalse();
      assertThat(PostCapabilitiesHandler.advertisement(B.toString(), Set.of(), true)
          .has(PostCapabilitiesHandler.QUARANTINED)).isFalse();
    }

    // Regression for #9553: every voter quarantined the same database
    @Test
    void theQueryReadsTheQuarantinesAndAPeerThatPredatesThemReadsAsNone() throws Exception {
      final String url = "http://peer-b:2480/api/v1/cluster/capabilities";

      assertThat(PeerCapabilityQuery.parse(B.toString(),
          PostCapabilitiesHandler.advertisement(B.toString(), Set.of(), false, Set.of(DB)).toString(), url).quarantined())
          .containsExactly(DB);
      assertThat(PeerCapabilityQuery.parse(B.toString(),
          "{\"peerId\":\"" + B + "\",\"version\":\"26.10.1\",\"capabilities\":[]}", url).quarantined())
          .as("a build that predates the field").isEmpty();
    }

    // -- what each node remembers about its peers ---------------------------------------------------------------------

    // Regression for #9553: every voter quarantined the same database
    @Test
    void theRegistryRemembersWhatEachPeerHoldsQuarantined() {
      final PeerCapabilityRegistry registry = new PeerCapabilityRegistry();
      final long generation = registry.generation();
      registry.record(generation, B.toString(), Set.of(), "26.10.2", false, Set.of(DB));
      registry.record(generation, C.toString(), Set.of(), "26.10.2", false, Set.of());

      assertThat(registry.reportsQuarantined(B.toString(), DB)).isTrue();
      assertThat(registry.reportsQuarantined(B.toString(), OTHER)).isFalse();
      assertThat(registry.reportsQuarantined(C.toString(), DB)).isFalse();
      assertThat(registry.reportsQuarantined("never-asked", DB)).as("unknown is not quarantined").isFalse();

      registry.record(generation, B.toString(), Set.of(), "26.10.2", false, Set.of());
      assertThat(registry.reportsQuarantined(B.toString(), DB)).as("the resync landed, the next poll says so").isFalse();
    }

    // Regression for #9553: every voter quarantined the same database
    @Test
    void aStaleAnswerIsNotBelieved() {
      final AtomicLong clock = new AtomicLong(1_000L);
      final PeerCapabilityRegistry registry = new PeerCapabilityRegistry(5_000L);
      registry.setClock(clock::get);
      registry.record(registry.generation(), B.toString(), Set.of(), "26.10.2", false, Set.of(DB));

      clock.addAndGet(5_001L);
      assertThat(registry.reportsQuarantined(B.toString(), DB)).isFalse();
    }

    // -- the decision: every voter said so ----------------------------------------------------------------------------

    // Regression for #9553: every voter quarantined the same database
    @Test
    void aDatabaseEveryVoterHoldsQuarantinedIsReported() {
      final PeerCapabilityRegistry registry = registryWhere(Set.of(DB, OTHER), Set.of(DB));

      assertThat(VoterQuarantineCheck.quarantinedOnEveryVoter(Set.of(DB, OTHER), voters(), SELF, registry))
          .as("only the database every voter reports").containsExactly(DB);
    }

    // Regression for #9553: every voter quarantined the same database
    @Test
    void oneVoterWithAUsableCopyIsEnough() {
      assertThat(VoterQuarantineCheck.quarantinedOnEveryVoter(Set.of(DB), voters(), SELF, registryWhere(Set.of(DB), Set.of())))
          .isEmpty();
    }

    // Regression for #9553: every voter quarantined the same database
    @Test
    void aVoterThatDidNotAnswerKeepsTheDatabaseOut() {
      final PeerCapabilityRegistry registry = new PeerCapabilityRegistry();
      registry.record(registry.generation(), B.toString(), Set.of(), "26.10.2", false, Set.of(DB));
      // C never answered: it may hold the only good copy, and "unknown" must not read as "quarantined"

      assertThat(VoterQuarantineCheck.quarantinedOnEveryVoter(Set.of(DB), voters(), SELF, registry)).isEmpty();
    }

    // Regression for #9553: every voter quarantined the same database
    @Test
    void aVoterOnABuildThatDoesNotReportQuarantinesKeepsTheDatabaseOut() {
      final PeerCapabilityRegistry registry = new PeerCapabilityRegistry();
      registry.record(registry.generation(), B.toString(), Set.of(), "26.10.2", false, Set.of(DB));
      registry.record(registry.generation(), C.toString(), Set.of(), "26.10.1", false);

      assertThat(VoterQuarantineCheck.quarantinedOnEveryVoter(Set.of(DB), voters(), SELF, registry)).isEmpty();
    }

    // Regression for #9553: every voter quarantined the same database
    @Test
    void aSoleVoterAndAHealthyNodeAreNeverReported() {
      final PeerCapabilityRegistry registry = registryWhere(Set.of(DB), Set.of(DB));

      assertThat(VoterQuarantineCheck.quarantinedOnEveryVoter(Set.of(DB), List.of(peer(SELF)), SELF, registry))
          .as("a sole voter has its own alert and override (#9308, #9449)").isEmpty();
      assertThat(VoterQuarantineCheck.quarantinedOnEveryVoter(Set.of(), voters(), SELF, registry))
          .as("this node holds no quarantine, so it holds a usable copy").isEmpty();
    }

    // -- the alert ----------------------------------------------------------------------------------------------------

    // Regression for #9553: every voter quarantined the same database
    @Test
    void theAlertIsCriticalAndNamesTheWayOut() {
      final JSONArray alerts = new JSONArray();

      ClusterAlerts.addNoHealthyCopyAlert(List.of(DB), null, alerts);

      assertThat(alerts.length()).isEqualTo(1);
      final JSONObject alert = alerts.getJSONObject(0);
      assertThat(alert.getString("id")).isEqualTo("no-healthy-copy-on-any-voter");
      assertThat(alert.getString("severity")).isEqualTo(ClusterAlerts.SEVERITY_CRITICAL);
      assertThat(alert.getString("message")).contains(DB);
      assertThat(alert.getString("recommendation")).contains(PostAcceptDivergedHandler.ROUTE).contains("backup")
          .contains("/api/v1/cluster/leader");
      assertThat(alert.getJSONObject("details").getJSONArray("databases").toList()).containsExactly(DB);
    }

    // Regression for #9553: every voter quarantined the same database
    @Test
    void theAlertIsScopedToTheDatabasesTheCallerMaySee() {
      final JSONArray hidden = new JSONArray();
      ClusterAlerts.addNoHealthyCopyAlert(List.of(DB), Set.of(OTHER), hidden);
      assertThat(hidden.length()).as("a caller authorized on none of them has nothing to read").isZero();

      final JSONArray scoped = new JSONArray();
      ClusterAlerts.addNoHealthyCopyAlert(List.of(DB, OTHER), Set.of(OTHER), scoped);
      assertThat(scoped.getJSONObject(0).getJSONObject("details").getJSONArray("databases").toList())
          .containsExactly(OTHER);
      assertThat(scoped.getJSONObject(0).getString("message")).doesNotContain(DB);
    }

    // Regression for #9553: every voter quarantined the same database
    @Test
    void noAlertWhenNothingIsQuarantinedEverywhere() {
      final JSONArray alerts = new JSONArray();
      ClusterAlerts.addNoHealthyCopyAlert(List.of(), null, alerts);
      assertThat(alerts.length()).isZero();
    }

    /** The caller: the status document's alert scan raises it from a real state machine and the peers' answers. */
    // Regression for #9553: every voter quarantined the same database
    @Test
    void theClusterScanRaisesTheAlertWhenEveryVoterReportsTheQuarantine() throws Exception {
      final ArcadeDBServer server = newServer();
      final ArcadeStateMachine sm = newStateMachine(server);
      try {
        sm.markStateDiverged(DB, DivergenceCause.APPLY_ERROR);
        final FakeRaftHAServer raft = clusterOf(sm, registryWhere(Set.of(DB), Set.of(DB)));
        sm.setRaftHAServer(raft);

        assertThat(alertIds(ClusterAlerts.scan(server, sm))).contains("no-healthy-copy-on-any-voter")
            .as("each node's own resync state is still reported").contains("local-resync-in-progress");

        raft.peerCapabilityRegistry(registryWhere(Set.of(DB), Set.of()));
        assertThat(alertIds(ClusterAlerts.scan(server, sm))).as("C serves a copy again")
            .doesNotContain("no-healthy-copy-on-any-voter");
      } finally {
        sm.close();
      }
    }

    // -- the way out: accept one copy ---------------------------------------------------------------------------------

    // Regression for #9553: every voter quarantined the same database
    @Test
    void theOverrideIsOpenWhenEveryVoterHoldsTheDatabaseQuarantined() throws Exception {
      final ArcadeDBServer server = newServer();
      final ArcadeStateMachine sm = newStateMachine(server);
      try {
        sm.markStateDiverged(DB, DivergenceCause.APPLY_ERROR);
        final FakeRaftHAServer raft = clusterOf(sm, registryWhere(Set.of(DB), Set.of(DB)));
        answers(raft, B, false);
        answers(raft, C, false);

        final JSONObject result = pluginOf(raft, server).acceptDivergedDatabase(DB, "user 'root'");

        assertThat(result.getString("database")).isEqualTo(DB);
        assertThat(sm.isDatabaseDiverged(DB)).isFalse();
        assertThat(sm.isResyncInProgress()).isFalse();
      } finally {
        sm.close();
      }
    }

    // Regression for #9553: every voter quarantined the same database
    @Test
    void theOverrideStaysClosedWhileOneVoterHoldsAUsableCopy() throws Exception {
      final ArcadeDBServer server = newServer();
      final ArcadeStateMachine sm = newStateMachine(server);
      try {
        sm.markStateDiverged(DB, DivergenceCause.APPLY_ERROR);
        final FakeRaftHAServer raft = clusterOf(sm, registryWhere(Set.of(DB), Set.of()));

        assertThatThrownBy(() -> pluginOf(raft, server).acceptDivergedDatabase(DB, "user 'root'"))
            .isInstanceOf(ServerControlPlane.OperationNotAvailableException.class)
            .hasMessageContaining("not every voter");
        assertThat(sm.isDatabaseDiverged(DB)).isTrue();
      } finally {
        sm.close();
      }
    }

    // Regression for #9553: every voter quarantined the same database
    /**
     * Review on PR #9567: the registry can be one poll behind, so a voter that has just accepted its own copy still reads
     * as quarantined there. The override asks every voter now, and a voter that serves the database keeps it closed - so
     * two nodes cannot each accept a different copy.
     */
    @Test
    void theOverrideAsksEveryVoterNowAndStaysClosedWhenOneServesACopy() throws Exception {
      final ArcadeDBServer server = newServer();
      final ArcadeStateMachine sm = newStateMachine(server);
      try {
        sm.markStateDiverged(DB, DivergenceCause.APPLY_ERROR);
        final FakeRaftHAServer raft = clusterOf(sm, registryWhere(Set.of(DB), Set.of(DB)));
        answers(raft, B, false);
        answers(raft, C, true); // C accepted its copy since its last capability answer

        assertThatThrownBy(() -> pluginOf(raft, server).acceptDivergedDatabase(DB, "user 'root'"))
            .isInstanceOf(ServerControlPlane.OperationNotAvailableException.class)
            .hasMessageContaining("voter '" + C + "' serves a copy");
        assertThat(sm.isDatabaseDiverged(DB)).as("nothing lifted").isTrue();
      } finally {
        sm.close();
      }
    }

    /** A voter that cannot be asked is not a "no": the override stays closed. */
    // Regression for #9553: every voter quarantined the same database
    @Test
    void theOverrideStaysClosedWhenAVoterCannotBeAsked() throws Exception {
      final ArcadeDBServer server = newServer();
      final ArcadeStateMachine sm = newStateMachine(server);
      try {
        sm.markStateDiverged(DB, DivergenceCause.APPLY_ERROR);
        final FakeRaftHAServer raft = clusterOf(sm, registryWhere(Set.of(DB), Set.of(DB)));
        answers(raft, B, false);
        // C has no address this node may dial

        assertThatThrownBy(() -> pluginOf(raft, server).acceptDivergedDatabase(DB, "user 'root'"))
            .isInstanceOf(ServerControlPlane.OperationNotAvailableException.class)
            .hasMessageContaining("voter '" + C + "' could not be asked");
        assertThat(sm.isDatabaseDiverged(DB)).isTrue();
      } finally {
        sm.close();
      }
    }

    // -- no resync from a leader that cannot serve it -----------------------------------------------------------------

    // Regression for #9553: every voter quarantined the same database
    @Test
    void theDatabasesTheLeaderHoldsQuarantinedAreLeftOutOfTheResync() {
      final PeerCapabilityRegistry registry = new PeerCapabilityRegistry();
      registry.record(registry.generation(), B.toString(), Set.of(), "26.10.2", false, Set.of(DB));

      assertThat(DatabaseQuarantine.resyncableFromLeader(Set.of(DB, OTHER), B.toString(), registry))
          .containsExactly(OTHER);
      assertThat(DatabaseQuarantine.resyncableFromLeader(Set.of(DB), C.toString(), registry))
          .as("a leader that did not answer is not assumed quarantined").containsExactly(DB);
      assertThat(DatabaseQuarantine.resyncableFromLeader(Set.of(DB), null, registry)).containsExactly(DB);
    }

    /** The caller: the health tick does not even take its throttle slot for a resync the leader can only refuse. */
    // Regression for #9553: every voter quarantined the same database
    @Test
    void theHealthTickDoesNotRetryAResyncFromALeaderThatHoldsTheSameQuarantine() throws Exception {
      final ArcadeStateMachine sm = newStateMachine();
      try {
        final PeerCapabilityRegistry registry = new PeerCapabilityRegistry();
        registry.record(registry.generation(), B.toString(), Set.of(), "26.10.2", false, Set.of(DB));
        sm.setRaftHAServer(followerOf(B, registry));
        sm.markStateDiverged(DB, DivergenceCause.APPLY_ERROR);

        sm.retryUnfilledSnapshotGap();
        assertThat(readLastRetryMs(sm)).as("nothing a resync from this leader could restore").isZero();

        registry.record(registry.generation(), B.toString(), Set.of(), "26.10.2", false, Set.of());
        sm.retryUnfilledSnapshotGap();
        assertThat(readLastRetryMs(sm)).as("the leader serves it again: the very next tick retries").isNotZero();
      } finally {
        sm.close();
      }
    }

    // -- helpers ------------------------------------------------------------------------------------------------------

    /** A registry in which B answered {@code quarantinedOnB} and C answered {@code quarantinedOnC}. */
    private static PeerCapabilityRegistry registryWhere(final Set<String> quarantinedOnB, final Set<String> quarantinedOnC) {
      final PeerCapabilityRegistry registry = new PeerCapabilityRegistry();
      registry.record(registry.generation(), B.toString(), Set.of(), "26.10.2", false, quarantinedOnB);
      registry.record(registry.generation(), C.toString(), Set.of(), "26.10.2", false, quarantinedOnC);
      return registry;
    }

    private FakeRaftHAServer clusterOf(final ArcadeStateMachine sm, final PeerCapabilityRegistry registry) {
      final FakeRaftHAServer raft = FakeRaftHAServer.detached();
      raft.localPeerId(SELF);
      raft.localHttpAddress("local-host:2480");
      raft.clusterToken(null);
      raft.livePeers(voters());
      raft.stateMachine(sm);
      raft.peerCapabilityRegistry(registry);
      return raft;
    }

    /**
     * Starts a stand-in for {@code peer}'s bootstrap-state endpoint answering the {@code copyOf} question with
     * {@code serves}, and points {@code raft} at it.
     */
    private void answers(final FakeRaftHAServer raft, final RaftPeerId peer, final boolean serves) throws IOException {
      final HttpServer stub = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
      stub.createContext("/", exchange -> {
        final byte[] body = new JSONObject().put("peerId", peer.toString()).put(UnverifiedClosedCopyCheck.SERVES, serves)
            .toString().getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, body.length);
        try (final OutputStream out = exchange.getResponseBody()) {
          out.write(body);
        }
      });
      stub.start();
      stubs.add(stub);
      raft.peerHttpAddress(peer, "localhost:" + stub.getAddress().getPort());
    }

    @AfterEach
    void stopStubs() {
      for (final HttpServer stub : stubs)
        stub.stop(0);
      stubs.clear();
    }

    /** The plugin the HTTP route and the gRPC RPC both end at, wired to the fake cluster. */
    private static RaftHAPlugin pluginOf(final FakeRaftHAServer raft, final ArcadeDBServer server) {
      final RaftHAPlugin plugin = raft.plugin();
      plugin.configure(server, server.getConfiguration());
      return plugin;
    }

    private static FakeRaftHAServer followerOf(final RaftPeerId leader, final PeerCapabilityRegistry registry) {
      final FakeRaftHAServer raft = FakeRaftHAServer.detached();
      raft.leader(false);
      raft.localPeerId(SELF);
      raft.leaderId(leader);
      raft.peerHttpAddress(leader, "peer-b:2480");
      raft.peerCapabilityRegistry(registry);
      return raft;
    }

    private ArcadeStateMachine newStateMachine() {
      return newStateMachine(newServer());
    }

    private static ArcadeStateMachine newStateMachine(final ArcadeDBServer server) {
      final ArcadeStateMachine sm = new ArcadeStateMachine();
      sm.setServer(server);
      return sm;
    }

    private ArcadeDBServer newServer() {
      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, root.resolve("databases").toString());
      return new ArcadeDBServer(config);
    }

    private static List<Object> alertIds(final JSONArray alerts) {
      final JSONArray ids = new JSONArray();
      for (int i = 0; i < alerts.length(); i++)
        ids.put(alerts.getJSONObject(i).getString("id"));
      return ids.toList();
    }

    private static long readLastRetryMs(final ArcadeStateMachine sm) throws Exception {
      final Field f = ArcadeStateMachine.class.getDeclaredField("lastStaleSnapshotRetryMs");
      f.setAccessible(true);
      return ((AtomicLong) f.get(sm)).get();
    }

    private static List<RaftPeer> voters() {
      return List.of(peer(SELF), peer(B), peer(C));
    }

    private static RaftPeer peer(final RaftPeerId id) {
      return RaftPeer.newBuilder().setId(id).setAddress("localhost:0").build();
    }
  }

  /**
   * Regression tests for issue #11281: a database quarantined because its own copy is torn (a crash recovery that did not
   * replay every transaction, #11250; a unique index with no header page, #11258) kept answering client reads from the
   * torn files while its resync was pending. The Jepsen bank workload read {@code {nil 1165}} instead of five accounts
   * totalling 5000 from such a node.
   * <p>
   * {@code RaftReplicatedDatabase} now refuses a client request on such a database with a {@link NeedRetryException},
   * like it does while the directory is being replaced, and still lets the engine's own threads through.
   */
  @Nested
  class QuarantinedDatabaseRefusesClients {

    private static final String DB_DIR  = "./target/databases";
    private static final String DB_NAME = "test-11281-quarantine-gate";
    private static final String DB_PATH = DB_DIR + "/" + DB_NAME;

    private LocalDatabase                        localDb;
    private RaftReplicatedDatabase               replicated;
    private final AtomicReference<DivergenceCause> cause = new AtomicReference<>();
    private RID                                  seedRid;

    @BeforeEach
    void setUp() {
      FileUtils.deleteRecursively(new File(DB_PATH));
      FileUtils.deleteRecursively(new File(DB_DIR + "/.raft"));
      localDb = (LocalDatabase) new DatabaseFactory(DB_PATH).create();
      localDb.getSchema().createDocumentType("Seed");
      localDb.transaction(() -> seedRid = localDb.newDocument("Seed").set("k", 1).save().getIdentity());

      final FakeArcadeDBServer server = FakeArcadeDBServer.create();
      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, DB_DIR);
      server.returns("getConfiguration", config);
      final FakeArcadeStateMachine stateMachine = new FakeArcadeStateMachine() {
        @Override
        DivergenceCause quarantineCause(final String dbName) {
          return DB_NAME.equals(dbName) ? cause.get() : null;
        }
      };
      final FakeRaftHAServer raft = FakeRaftHAServer.detached();
      raft.leader(true);
      raft.stateMachine(stateMachine);
      replicated = new RaftReplicatedDatabase(server, localDb, raft);
      localDb.setAutoTransaction(true);
    }

    @AfterEach
    void tearDown() {
      ProtocolContext.clear();
      if (localDb != null && localDb.isOpen()) {
        if (localDb.isTransactionActive())
          localDb.rollbackAllNested();
        localDb.close();
      }
      FileUtils.deleteRecursively(new File(DB_PATH));
      FileUtils.deleteRecursively(new File(DB_DIR + "/.raft"));
    }

    private Map<String, Executable> clientEntryPoints() {
      final Map<String, Executable> calls = new LinkedHashMap<>();
      calls.put("query(sql)", () -> close(replicated.query("sql", "select from Seed")));
      calls.put("query(sql, varargs)", () -> close(replicated.query("sql", "select from Seed where k = ?", 1)));
      calls.put("query(sql, map)", () -> close(replicated.query("sql", "select from Seed where k = :k", Map.of("k", 1))));
      calls.put("command(sql)", () -> close(replicated.command("sql", "select from Seed")));
      calls.put("begin()", () -> replicated.begin());
      calls.put("lookupByRID", () -> replicated.lookupByRID(seedRid, true));
      calls.put("existsRecord", () -> replicated.existsRecord(seedRid));
      calls.put("countType", () -> replicated.countType("Seed", true));
      calls.put("iterateType", () -> replicated.iterateType("Seed", true));
      calls.put("scanType", () -> replicated.scanType("Seed", true, doc -> true));
      calls.put("async()", () -> replicated.async());
      return calls;
    }

    private static void close(final ResultSet resultSet) {
      while (resultSet.hasNext())
        resultSet.next();
      resultSet.close();
    }

    private void assertEveryClientEntryPointIsRefused(final String protocol) {
      ProtocolContext.set(protocol);
      try {
        clientEntryPoints().forEach((name, call) -> assertThatThrownBy(call::execute)
            .as("a %s client's %s on a quarantined database", protocol, name)
            .isInstanceOf(NeedRetryException.class)
            .hasMessageContaining(DB_NAME)
            .hasMessageContaining("quarantined"));
      } finally {
        ProtocolContext.clear();
      }
    }

    private void assertEveryClientEntryPointIsServed(final String protocol) {
      ProtocolContext.set(protocol);
      try {
        clientEntryPoints().forEach((name, call) -> {
          assertThatNoException().as("%s's %s", protocol, name).isThrownBy(call::execute);
          if (localDb.isTransactionActive())
            localDb.rollback();
        });
      } finally {
        ProtocolContext.clear();
      }
    }

    // Regression for #11281: a quarantined database refuses client reads and writes
    @Test
    void aClientIsServedWhenNothingIsQuarantined() {
      assertEveryClientEntryPointIsServed("http");
    }

    // Regression for #11281: a quarantined database refuses client reads and writes
    @Test
    void everyClientEntryPointIsRefusedAfterAnIncompleteCrashRecovery() {
      cause.set(DivergenceCause.LOCAL_RECOVERY_INCOMPLETE);
      for (final String protocol : new String[] { "http", "bolt", "postgres", "grpc", "mongo", "redis", "ws" })
        assertEveryClientEntryPointIsRefused(protocol);
    }

    // Regression for #11281: a quarantined database refuses client reads and writes
    @Test
    void everyClientEntryPointIsRefusedWhenAUniqueIndexCouldNotBeLoaded() {
      cause.set(DivergenceCause.UNAVAILABLE_UNIQUE_INDEX);
      assertEveryClientEntryPointIsRefused("http");
    }

    // Regression for #11281: a quarantined database refuses client reads and writes
    @Test
    void everyClientEntryPointIsRefusedWhenTheDictionaryWasEmptied() {
      cause.set(DivergenceCause.EMPTY_DICTIONARY);
      assertEveryClientEntryPointIsRefused("http");
    }

    // Regression for #11281: a quarantined database refuses client reads and writes
    @Test
    void theServiceResumesOnceTheQuarantineLifts() {
      cause.set(DivergenceCause.LOCAL_RECOVERY_INCOMPLETE);
      assertEveryClientEntryPointIsRefused("http");
      cause.set(null);
      assertEveryClientEntryPointIsServed("http");
    }

    /** The apply thread, the resync and the health monitor read through the same wrapper: refusing them is a divergence. */
    // Regression for #11281: a quarantined database refuses client reads and writes
    @Test
    void theEngineIsNeverRefused() {
      cause.set(DivergenceCause.LOCAL_RECOVERY_INCOMPLETE);
      assertThat(ProtocolContext.get()).isEqualTo(ProtocolContext.INTERNAL);
      assertEveryClientEntryPointIsServed(ProtocolContext.INTERNAL);
    }

    /** A replication-gap quarantine keeps its documented behaviour: the copy is merely behind, not torn. */
    // Regression for #11281: a quarantined database refuses client reads and writes
    @Test
    void aDatabaseQuarantinedForAReplicationGapStillServes() {
      cause.set(DivergenceCause.WAL_VERSION_GAP);
      assertEveryClientEntryPointIsServed("http");
    }

    /** The census: the gate names exactly the causes that say the local files are torn, and every cause is decided. */
    // Regression for #11281: a quarantined database refuses client reads and writes
    @Test
    void everyCauseIsClassified() {
      assertThat(DivergenceCause.values()).as("the scan still reaches DivergenceCause").isNotEmpty();
      for (final DivergenceCause c : DivergenceCause.values())
        assertThat(c.isLocalCopyUntrustworthy()).as(c.name()).isEqualTo(
            c == DivergenceCause.LOCAL_RECOVERY_INCOMPLETE || c == DivergenceCause.UNAVAILABLE_UNIQUE_INDEX
                || c == DivergenceCause.EMPTY_DICTIONARY);
    }
  }

  /**
   * Unit tests for issue #8468: the quarantine a snapshot-serving node consults before it serves a database.
   * {@code QuarantinedSnapshotSourceIT} drives the refusal through the HTTP handler on a real cluster.
   */
  @Nested
  class QuarantinedSnapshotSource {

    private static final String DB = "db-a";

    // Regression for #8468: a quarantined node refuses to serve the database as a snapshot source
    @Test
    void aHealthyDatabaseHasNoQuarantineCause(@TempDir final Path tempDir) throws Exception {
      final ArcadeStateMachine sm = newStateMachine(tempDir);
      try {
        assertThat(sm.quarantineCause(DB)).isNull();
        assertThat(sm.quarantineCause(null)).isNull();
      } finally {
        sm.close();
      }
    }

    // Regression for #8468: a quarantined node refuses to serve the database as a snapshot source
    @Test
    void aQuarantinedDatabaseReportsItsCauseUntilTheResyncClearsIt(@TempDir final Path tempDir) throws Exception {
      final ArcadeStateMachine sm = newStateMachine(tempDir);
      try {
        sm.markStateDiverged(DB, DivergenceCause.WAL_VERSION_GAP);
        assertThat(sm.quarantineCause(DB)).isEqualTo(DivergenceCause.WAL_VERSION_GAP);
        assertThat(sm.quarantineCause("db-b")).as("a quarantine is per database").isNull();

        sm.clearDivergedDatabase(DB);
        assertThat(sm.quarantineCause(DB)).isNull();
      } finally {
        sm.close();
      }
    }

    // Regression for #8468: a quarantined node refuses to serve the database as a snapshot source
    @Test
    void aDatabaseAnInstallGaveUpOnIsReportedAsQuarantined(@TempDir final Path tempDir) throws Exception {
      // Its copy is behind the snapshot index the node's applied index already reached, the same overstatement.
      final ArcadeStateMachine sm = newStateMachine(tempDir);
      try {
        sm.settleDivergedStateAfterInstall(Set.of(DB), 100L);
        assertThat(sm.quarantineCause(DB)).isEqualTo(DivergenceCause.SNAPSHOT_INSTALL_INCOMPLETE);
      } finally {
        sm.close();
      }
    }

    // Regression for #8468: a quarantined node refuses to serve the database as a snapshot source
    @Test
    void aQuarantineRestoredFromDiskIsSeenByTheFirstCaller(@TempDir final Path tempDir) throws Exception {
      final ArcadeStateMachine before = newStateMachine(tempDir);
      try {
        before.markStateDiverged(DB, DivergenceCause.APPLY_ERROR);
      } finally {
        before.close();
      }
      final ArcadeStateMachine after = newStateMachine(tempDir);
      try {
        assertThat(after.quarantineCause(DB)).as("a restart must not serve the copy it quarantined")
            .isEqualTo(DivergenceCause.APPLY_ERROR);
      } finally {
        after.close();
      }
    }

    // Regression for #8468: a quarantined node refuses to serve the database as a snapshot source
    @Test
    void aNodeWithoutRaftHasNothingToRefuse() {
      assertThat(SnapshotHttpHandler.servedDatabaseQuarantine(null, DB)).isNull();
    }

    private static ArcadeStateMachine newStateMachine(final Path tempDir) {
      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, tempDir.resolve("databases").toString());
      final ArcadeStateMachine sm = new ArcadeStateMachine();
      sm.setServer(new ArcadeDBServer(config));
      return sm;
    }
  }
}
