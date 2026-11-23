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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerDatabase;
import com.arcadedb.server.StaticBaseServerTest;
import com.arcadedb.server.TestServerHelper;
import com.arcadedb.server.ha.raft.ArcadeStateMachine.LocalResyncState;
import com.sun.net.httpserver.HttpServer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.util.LifeCycle;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Local resync status and recovery state: resync coverage, divergence reformat and crash-loop record.
 */
class ResyncRecoveryTest {

  /**
   * Regression tests for issue #7136.
   * <p>
   * {@code GET /api/v1/cluster} published nothing about the local node's own resync state, so on the very node
   * readiness was holding out of the Kubernetes Service the endpoint an operator polls answered {@code 200} with
   * {@code raftState: "RUNNING"} and {@code alerts: []}. None of the four components of
   * {@link ArcadeStateMachine#isResyncInProgress()} - the queued snapshot download, the running one, the WAL
   * version-gap quarantine (#4797/#4740) and the stale-snapshot read floors (#6111/#6760) - reached the document.
   * <p>
   * The invariant these tests pin: <b>anything that makes readiness return 503 must appear in the cluster status
   * document</b>. {@link ArcadeStateMachine#getLocalResyncState()} is that document's source, and readiness reads
   * the same value, so the two cannot drift.
   */
  @Nested
  class LocalResyncStatus {

    private static final String DB_A = "db-a";
    private static final String DB_B = "db-b";

    // ---------------------------------------------------------------------------------------------
    // The report is the whole of isResyncInProgress(), component by component
    // ---------------------------------------------------------------------------------------------

    // Regression for #7136: the cluster endpoint publishes the local node resync state
    @Test
    void aQuietNodeReportsNothingInFlight(@TempDir final Path tempDir) throws Exception {
      final ArcadeStateMachine sm = newStateMachine(tempDir);
      try {
        final LocalResyncState state = sm.getLocalResyncState();

        assertThat(state.inProgress()).isFalse();
        assertThat(sm.isResyncInProgress()).isFalse();
        assertThat(state.snapshotDownloadQueued()).isFalse();
        assertThat(state.snapshotDownloadInProgress()).isFalse();
        assertThat(state.divergedDatabases()).isEmpty();
        assertThat(state.snapshotAppliedFloor()).isEqualTo(-1L);
        assertThat(state.databaseAppliedFloors()).isEmpty();
      } finally {
        sm.close();
      }
    }

    // Regression for #7136: the cluster endpoint publishes the local node resync state
    @Test
    void aQueuedSnapshotDownloadIsBothReportedAndUnready(@TempDir final Path tempDir) throws Exception {
      final ArcadeStateMachine sm = newStateMachine(tempDir);
      try {
        flag(sm, "needsSnapshotDownload", true);

        assertThat(sm.getLocalResyncState().snapshotDownloadQueued()).isTrue();
        assertThat(sm.getLocalResyncState().inProgress()).isTrue();
        assertThat(sm.isResyncInProgress()).as("readiness and the status document must agree").isTrue();
      } finally {
        sm.close();
      }
    }

    // Regression for #7136: the cluster endpoint publishes the local node resync state
    @Test
    void aRunningSnapshotDownloadIsBothReportedAndUnready(@TempDir final Path tempDir) throws Exception {
      final ArcadeStateMachine sm = newStateMachine(tempDir);
      try {
        flag(sm, "snapshotDownloadInProgress", true);

        assertThat(sm.getLocalResyncState().snapshotDownloadInProgress()).isTrue();
        assertThat(sm.getLocalResyncState().inProgress()).isTrue();
        assertThat(sm.isResyncInProgress()).isTrue();
      } finally {
        sm.close();
      }
    }

    /** The WAL version-gap quarantine (#4797/#4740): the condition the issue was reported against. */
    // Regression for #7136: the cluster endpoint publishes the local node resync state
    @Test
    void aWalGapQuarantineNamesTheDatabase(@TempDir final Path tempDir) throws Exception {
      final ArcadeStateMachine sm = newStateMachine(tempDir);
      try {
        sm.markStateDiverged(DB_B);
        sm.markStateDiverged(DB_A);

        final LocalResyncState state = sm.getLocalResyncState();
        assertThat(state.divergedDatabases()).as("sorted for a stable poll payload").containsExactly(DB_A, DB_B);
        assertThat(state.inProgress()).isTrue();
        assertThat(sm.isResyncInProgress()).isTrue();
      } finally {
        sm.close();
      }
    }

    /** The node-wide stale-snapshot read floor (#6111): set with neither download flag raised. */
    // Regression for #7136: the cluster endpoint publishes the local node resync state
    @Test
    void anOutstandingReadFloorIsBothReportedAndUnready(@TempDir final Path tempDir) throws Exception {
      final ArcadeStateMachine sm = newStateMachine(tempDir);
      try {
        floor(sm, 100L);

        assertThat(sm.getLocalResyncState().snapshotAppliedFloor()).isEqualTo(100L);
        assertThat(sm.getLocalResyncState().inProgress()).isTrue();
        assertThat(sm.isResyncInProgress()).isTrue();
      } finally {
        sm.close();
      }
    }

    /** The per-database read floor (#6760). */
    // Regression for #7136: the cluster endpoint publishes the local node resync state
    @Test
    void aPerDatabaseReadFloorIsBothReportedAndUnready(@TempDir final Path tempDir) throws Exception {
      final ArcadeStateMachine sm = newStateMachine(tempDir);
      try {
        databaseFloor(sm, DB_A, 42L);

        final LocalResyncState state = sm.getLocalResyncState();
        assertThat(state.databaseAppliedFloors()).containsExactly(Map.entry(DB_A, 42L));
        assertThat(state.inProgress()).isTrue();
        assertThat(sm.isResyncInProgress()).isTrue();
      } finally {
        sm.close();
      }
    }

    // Regression for #7136: the cluster endpoint publishes the local node resync state
    /**
     * The report is a snapshot, not a live view: a later change to the state machine must not mutate a document
     * already handed to a caller (two concurrent polls would otherwise see each other's state mid-render).
     */
    @Test
    void theReportIsAnImmutableSnapshot(@TempDir final Path tempDir) throws Exception {
      final ArcadeStateMachine sm = newStateMachine(tempDir);
      try {
        sm.markStateDiverged(DB_A);
        final LocalResyncState state = sm.getLocalResyncState();

        sm.markStateDiverged(DB_B);

        assertThat(state.divergedDatabases()).containsExactly(DB_A);
        assertThat(sm.getLocalResyncState().divergedDatabases()).containsExactly(DB_A, DB_B);
      } finally {
        sm.close();
      }
    }

    // ---------------------------------------------------------------------------------------------
    // The alert a monitoring rule keyed on alerts[] can fire on
    // ---------------------------------------------------------------------------------------------

    // Regression for #7136: the cluster endpoint publishes the local node resync state
    @Test
    void noAlertWhileNothingIsResyncing() {
      final JSONArray alerts = new JSONArray();
      ClusterAlerts.addLocalResyncAlert(clean(), null, alerts);
      ClusterAlerts.addLocalResyncAlert(null, null, alerts);

      assertThat(alerts.length()).isZero();
    }

    // Regression for #7136: the cluster endpoint publishes the local node resync state
    @Test
    void aRunningDownloadRaisesAWarningNamingWhatIsInFlight() {
      final JSONArray alerts = new JSONArray();
      ClusterAlerts.addLocalResyncAlert(
          new LocalResyncState(false, true, -1, Map.of(), Map.of()), null, alerts);

      assertThat(alerts.length()).isEqualTo(1);
      final JSONObject alert = alerts.getJSONObject(0);
      assertThat(alert.getString("id")).isEqualTo("local-resync-in-progress");
      assertThat(alert.getString("severity")).isEqualTo(ClusterAlerts.SEVERITY_WARNING);
      final JSONObject details = alert.getJSONObject("details");
      assertThat(details.getBoolean("snapshotDownloadInProgress")).isTrue();
      assertThat(details.getBoolean("snapshotDownloadQueued")).isFalse();
    }

    // Regression for #7136: the cluster endpoint publishes the local node resync state
    @Test
    void aQuarantinedDatabaseEscalatesToCriticalAndIsNamed() {
      final JSONArray alerts = new JSONArray();
      ClusterAlerts.addLocalResyncAlert(
          new LocalResyncState(false, false, -1, Map.of(), Map.of(DB_A, DivergenceCause.WAL_VERSION_GAP)), null, alerts);

      assertThat(alerts.length()).isEqualTo(1);
      final JSONObject alert = alerts.getJSONObject(0);
      assertThat(alert.getString("severity")).isEqualTo(ClusterAlerts.SEVERITY_CRITICAL);
      assertThat(alert.getJSONObject("details").getJSONArray("divergedDatabases").toList()).containsExactly(DB_A);
    }

    // Regression for #7136: the cluster endpoint publishes the local node resync state
    @Test
    void anOutstandingReadFloorEscalatesToCritical() {
      final JSONArray alerts = new JSONArray();
      ClusterAlerts.addLocalResyncAlert(
          new LocalResyncState(false, false, 100L, Map.of(), Map.of()), null, alerts);

      assertThat(alerts.getJSONObject(0).getString("severity")).isEqualTo(ClusterAlerts.SEVERITY_CRITICAL);
      assertThat(alerts.getJSONObject(0).getJSONObject("details").getLong("snapshotAppliedFloor")).isEqualTo(100L);
    }

    // Regression for #7136: the cluster endpoint publishes the local node resync state
    /**
     * The alert names databases, so a caller scoped to one database must not learn another one's name - the same
     * rule the other database-scoped alerts follow. The node-level condition itself is NOT hidden: the alert still
     * fires, because whether this node serves traffic is not a per-tenant fact.
     */
    @Test
    void databaseNamesAreScopedToTheCallerButTheAlertStillFires() {
      final JSONArray alerts = new JSONArray();
      ClusterAlerts.addLocalResyncAlert(
          new LocalResyncState(false, false, -1, Map.of(DB_B, 7L),
              Map.of(DB_A, DivergenceCause.WAL_VERSION_GAP, DB_B, DivergenceCause.APPLY_ERROR)), Set.of(DB_A), alerts);

      assertThat(alerts.length()).as("a resyncing node is node-level news, not per-tenant news").isEqualTo(1);
      final JSONObject details = alerts.getJSONObject(0).getJSONObject("details");
      assertThat(details.getJSONArray("divergedDatabases").toList()).containsExactly(DB_A);
      assertThat(details.getJSONObject("databaseAppliedFloors").keySet()).isEmpty();
    }

    // ---------------------------------------------------------------------------------------------
    // The localResync object the status document publishes
    // ---------------------------------------------------------------------------------------------

    // Regression for #7136: the cluster endpoint publishes the local node resync state
    @Test
    void theStatusDocumentCarriesEveryComponentOnAQuietNode() {
      final JSONObject json = GetClusterHandler.buildLocalResync(clean(), null);

      assertThat(json.getBoolean("inProgress")).isFalse();
      assertThat(json.getBoolean("snapshotDownloadQueued")).isFalse();
      assertThat(json.getBoolean("snapshotDownloadInProgress")).isFalse();
      assertThat(json.getJSONArray("divergedDatabases").toList()).isEmpty();
      assertThat(json.getLong("snapshotAppliedFloor")).isEqualTo(-1L);
      assertThat(json.getJSONObject("databaseAppliedFloors").keySet()).isEmpty();
    }

    /** The reported scenario: a follower quarantined on a WAL version gap must no longer read as green. */
    // Regression for #7136: the cluster endpoint publishes the local node resync state
    @Test
    void aQuarantinedFollowerIsVisibleInTheStatusDocument() {
      final JSONObject json = GetClusterHandler.buildLocalResync(
          new LocalResyncState(true, false, -1, Map.of(DB_A, 42L), Map.of(DB_A, DivergenceCause.WAL_VERSION_GAP)), null);

      assertThat(json.getBoolean("inProgress")).isTrue();
      assertThat(json.getBoolean("snapshotDownloadQueued")).isTrue();
      assertThat(json.getJSONArray("divergedDatabases").toList()).containsExactly(DB_A);
      assertThat(json.getJSONObject("databaseAppliedFloors").getLong(DB_A)).isEqualTo(42L);
    }

    // Regression for #7136: the cluster endpoint publishes the local node resync state
    /**
     * {@code inProgress} is the node's answer and survives scoping; only the database names are reduced. A
     * tenant that may see nothing still learns the node is not serving.
     */
    @Test
    void theStatusDocumentScopesDatabaseNamesButNotTheNodeLevelAnswer() {
      final JSONObject json = GetClusterHandler.buildLocalResync(
          new LocalResyncState(false, false, -1, Map.of(DB_B, 7L),
              Map.of(DB_A, DivergenceCause.WAL_VERSION_GAP, DB_B, DivergenceCause.WAL_VERSION_GAP)), Set.of(DB_A));

      assertThat(json.getBoolean("inProgress")).isTrue();
      assertThat(json.getJSONArray("divergedDatabases").toList()).containsExactly(DB_A);
      assertThat(json.getJSONObject("databaseAppliedFloors").keySet()).isEmpty();
    }

    // ---------------------------------------------------------------------------------------------
    // The status-exporter leftover from #7040: a removed peer must stop reading as "not yet converged"
    // ---------------------------------------------------------------------------------------------

    // Regression for #7136: the cluster endpoint publishes the local node resync state
    @Test
    void aDeclaredPeerThatHasNeverJoinedStillCountsAsPending() {
      // Bootstrap window (issue #5304): arcadedb-3 is declared, has never been committed, and the table must
      // keep saying the membership is not converged yet.
      final int expected = RaftClusterStatusExporter.expectedMemberCount(
          List.of("arcadedb-1", "arcadedb-2", "arcadedb-3"), List.of("arcadedb-1", "arcadedb-2"),
          Set.of("arcadedb-1", "arcadedb-2"), true, 2);

      assertThat(expected).isEqualTo(3);
    }

    // Regression for #7136: the cluster endpoint publishes the local node resync state
    @Test
    void aPeerRemovedFromTheConfigurationStopsCountingAsPending() {
      // arcadedb-3 was a committed member and then left (DELETE /api/v1/cluster/peer/{id}). Convergence is
      // complete: the note must clear rather than blame a peer that is never coming back.
      final int expected = RaftClusterStatusExporter.expectedMemberCount(
          List.of("arcadedb-1", "arcadedb-2", "arcadedb-3"), List.of("arcadedb-1", "arcadedb-2"),
          Set.of("arcadedb-1", "arcadedb-2", "arcadedb-3"), true, 2);

      assertThat(expected).isEqualTo(2);
    }

    // Regression for #7136: the cluster endpoint publishes the local node resync state
    @Test
    void aRuntimeJoinNotInTheDeclaredListDoesNotInflateTheCount() {
      // A peer added with POST /api/v1/cluster/peer is a committed member the server list never declared.
      final int expected = RaftClusterStatusExporter.expectedMemberCount(
          List.of("arcadedb-1"), List.of("arcadedb-1", "arcadedb-2"), Set.of("arcadedb-1", "arcadedb-2"), true, 2);

      assertThat(expected).isEqualTo(2);
    }

    // Regression for #7136: the cluster endpoint publishes the local node resync state
    @Test
    void aTickThatMeasuredNoMembershipMakesNoConvergenceClaim() {
      // The division was unreadable this tick (issue #5271): getCommittedPeersOrNull() answered nothing, so the
      // rows were rendered from the separate fallback read and none of the three inputs describes them. Scanning
      // them anyway answers 0 (every declared peer has been committed before, so none is pending), and the
      // declared-list denominator answers 3 against 2 rendered rows - reporting the removed peer as pending
      // again, for one tick, which is the bug this change exists to remove. The row count is the only honest
      // answer: the note stays silent until a tick actually measures something.
      final int expected = RaftClusterStatusExporter.expectedMemberCount(
          List.of("arcadedb-1", "arcadedb-2", "arcadedb-3"), List.<String>of(),
          Set.of("arcadedb-1", "arcadedb-2", "arcadedb-3"), false, 2);

      assertThat(expected).as("an unmeasured tick claims neither convergence nor divergence").isEqualTo(2);
    }

    // ---------------------------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------------------------

    private static LocalResyncState clean() {
      return new LocalResyncState(false, false, -1, Map.of(), Map.of());
    }

    private static ArcadeStateMachine newStateMachine(final Path tempDir) {
      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, tempDir.resolve("databases").toString());
      final ArcadeStateMachine sm = new ArcadeStateMachine();
      sm.setServer(new ArcadeDBServer(config));
      return sm;
    }

    private static void flag(final ArcadeStateMachine sm, final String fieldName, final boolean value)
        throws Exception {
      final Field f = ArcadeStateMachine.class.getDeclaredField(fieldName);
      f.setAccessible(true);
      ((AtomicBoolean) f.get(sm)).set(value);
    }

    private static void floor(final ArcadeStateMachine sm, final long value) throws Exception {
      final Field f = ArcadeStateMachine.class.getDeclaredField("staleSnapshotAppliedFloor");
      f.setAccessible(true);
      ((AtomicLong) f.get(sm)).set(value);
    }

    @SuppressWarnings("unchecked")
    private static void databaseFloor(final ArcadeStateMachine sm, final String dbName, final long value)
        throws Exception {
      final Field f = ArcadeStateMachine.class.getDeclaredField("staleDatabaseAppliedFloors");
      f.setAccessible(true);
      ((ConcurrentHashMap<String, Long>) f.get(sm)).put(dbName, value);
    }
  }

  /**
   * Regression tests for issue #8464: a snapshot resync reinstalled only the databases REGISTERED on the node, so a
   * database closed there with {@code close database} - deregistered, its directory left on disk, and reopened by the
   * next request that names it - kept its pre-resync files while the resync recorded the whole gap as applied.
   * <p>
   * The fixture is the one {@link ArcadeStateMachinePendingBootstrapReplacementTest} uses: a real {@link ArcadeDBServer} (HA off)
   * holding the databases, a real {@link ArcadeStateMachine}, a mocked follower-side {@link RaftHAServer}, and a local
   * HTTP server standing in for the leader's snapshot endpoint. The leader's copy carries a different record count, so
   * an install that landed is observable on the reopened database.
   */
  @Nested
  @Timeout(120)
  class ResyncCoversClosedDatabase {

    private static final String     DB_NAME        = "db8464";
    private static final String     OTHER_DB       = "db8464other";
    private static final String     PASSWORD       = "DefaultPasswordForTests";
    private static final int        LIVE_COUNT     = 12;
    private static final int        SNAPSHOT_COUNT = 27;
    private static final long       FLOOR          = 5L;
    private static final RaftPeerId LOCAL          = RaftPeerId.valueOf("local");
    private static final RaftPeerId LEADER         = RaftPeerId.valueOf("leader");
    private static final TermIndex  MARKER         = TermIndex.valueOf(3L, 40L);

    @TempDir
    Path root;

    private ArcadeDBServer             server;
    private ArcadeStateMachine         sm;
    private HttpServer                 leader;
    private String                     leaderAddress;
    private final Map<String, AtomicInteger> downloads = new ConcurrentHashMap<>();

    @BeforeEach
    void setUp() throws IOException {
      server = startServer();
      createLocalDatabase(DB_NAME);

      leader = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
      leader.start();
      leaderAddress = "localhost:" + leader.getAddress().getPort();

      final FakeRaftHAServer raft = FakeRaftHAServer.detached();
      raft.leader(false);
      raft.localPeerId(LOCAL);
      raft.localHttpAddress("local-host:2480");
      raft.clusterToken(null);
      raft.leaderId(LEADER);
      raft.peerHttpAddress(LEADER, leaderAddress);

      sm = new ArcadeStateMachine();
      sm.setServer(server);
      sm.setRaftHAServer(raft);
    }

    @AfterEach
    void tearDown() throws IOException {
      if (sm != null)
        sm.close();
      if (leader != null)
        leader.stop(0);
      if (server != null) {
        for (final String name : List.of(DB_NAME, OTHER_DB))
          try {
            if (server.existsDatabase(name))
              ((DatabaseInternal) server.getDatabaseForOperator(name)).getEmbedded().drop();
          } catch (final Exception ignore) {
            // best-effort cleanup; the @TempDir is removed regardless
          }
        server.stop();
      }
    }

    // ------------------------------------------------------------------------------------------------------------
    // The full resync (leader change, watchdog, stale-snapshot retry)
    // ------------------------------------------------------------------------------------------------------------

    /** The issue as reported: the full resync walked the registry, skipped the closed database and cleared the floor. */
    // Regression for #8464: a snapshot resync also covers a database closed on the node
    @Test
    void aFullResyncReinstallsADatabaseClosedOnThisNode() throws Exception {
      leaderServes(DB_NAME);
      closeLocally(DB_NAME);
      setStaleSnapshotAppliedFloor(FLOOR);

      sm.triggerSnapshotDownload();

      assertThat(downloadsOf(DB_NAME)).as("the closed database was pulled from the leader").isEqualTo(1);
      assertThat(liveCount(DB_NAME)).as("and what this node serves for it is the leader's copy").isEqualTo(SNAPSHOT_COUNT);
      assertThat(sm.getStaleSnapshotAppliedFloor()).as("the gap is closed for real, so the floor resolves").isEqualTo(-1L);
      assertThat(sm.isDatabaseDiverged(DB_NAME)).isFalse();
      assertThat(sm.isResyncInProgress()).isFalse();
    }

    // Regression for #8464: a snapshot resync also covers a database closed on the node
    /**
     * A closed database the leader cannot serve - closed there too, the ordinary maintenance close - must neither be
     * laundered into "applied" nor hold the resync of every other database hostage. It is quarantined with its own read
     * floor and the others are reinstalled.
     */
    @Test
    void aClosedDatabaseTheLeaderCannotServeIsQuarantinedWithoutFailingTheOthers() throws Exception {
      createLocalDatabase(OTHER_DB);
      leaderServes(OTHER_DB);
      // A failure other than 404: a 404 says the leader does not hold it, which is not quarantined (issue #8559)
      leaderFails(DB_NAME);
      closeLocally(DB_NAME);
      sm.writePersistedAppliedIndex(FLOOR, DB_NAME);
      setStaleSnapshotAppliedFloor(FLOOR);

      sm.triggerSnapshotDownload();

      assertThat(liveCount(OTHER_DB)).as("the registered database was reinstalled regardless").isEqualTo(SNAPSHOT_COUNT);
      assertThat(sm.isDatabaseDiverged(DB_NAME)).as("the closed copy nobody refreshed stays quarantined").isTrue();
      assertThat(sm.getDatabaseAppliedFloor(DB_NAME)).as("with a read floor at what it genuinely applied").isEqualTo(FLOOR);
      assertThat(sm.readPersistedAppliedIndex(DB_NAME)).as("and it is not recorded at the marker index").isEqualTo(FLOOR);
      assertThat(sm.isDatabaseDiverged(OTHER_DB)).isFalse();
      assertThat(sm.isResyncInProgress()).as("the node stays out of the ready set while it holds that copy").isTrue();
      assertThat(liveCount(DB_NAME)).as("nothing replaced the closed copy").isEqualTo(LIVE_COUNT);
    }

    // Regression for #8464: a snapshot resync also covers a database closed on the node
    /**
     * The quarantine above is what protects the closed copy once the node-wide floor is gone, so it must reach disk
     * before the floor is dropped: one that lives in memory alone is lost on a restart, and the directory would then be
     * reopened ready and unclamped. When it cannot be written the resync fails and keeps the floor.
     */
    @Test
    void aQuarantineThatCannotBePersistedKeepsTheNodeWideFloor() throws Exception {
      createLocalDatabase(OTHER_DB);
      leaderServes(OTHER_DB);
      leaderFails(DB_NAME);
      closeLocally(DB_NAME);
      sm.writePersistedAppliedIndex(FLOOR, DB_NAME);
      setStaleSnapshotAppliedFloor(FLOOR);

      // A non-empty directory where the applied-index file goes: the atomic replace of it fails on every write
      final Path appliedIndex = root.resolve("databases").resolve(".raft").resolve("applied-index");
      assertThat(Files.isRegularFile(appliedIndex)).as("the fixture targets the real file").isTrue();
      Files.delete(appliedIndex);
      Files.writeString(Files.createDirectories(appliedIndex).resolve("blocker"), "x");

      sm.triggerSnapshotDownload();

      assertThat(sm.getStaleSnapshotAppliedFloor()).as("the node-wide floor stands").isEqualTo(FLOOR);
      assertThat(sm.isDatabaseDiverged(DB_NAME)).as("and the in-memory quarantine too").isTrue();
      assertThat(sm.isResyncInProgress()).isTrue();
    }

    // ------------------------------------------------------------------------------------------------------------
    // The targeted resync of a quarantined database
    // ------------------------------------------------------------------------------------------------------------

    // Regression for #8464: a snapshot resync also covers a database closed on the node
    /**
     * The targeted resync reinstalled only {@code if (server.existsDatabase(dbName))}, so a quarantined database that
     * was then closed was never repaired and its quarantine never lifted.
     */
    @Test
    void aTargetedResyncReinstallsAQuarantinedDatabaseClosedOnThisNode() throws Exception {
      leaderServes(DB_NAME);
      sm.markStateDiverged(DB_NAME);
      closeLocally(DB_NAME);

      // Only a quarantine is outstanding, so the health tick drives the targeted resync of it.
      sm.retryUnfilledSnapshotGap();
      sm.awaitLifecycleTasksForTesting(60_000);

      assertThat(downloadsOf(DB_NAME)).isEqualTo(1);
      assertThat(liveCount(DB_NAME)).isEqualTo(SNAPSHOT_COUNT);
      assertThat(sm.isDatabaseDiverged(DB_NAME)).as("the quarantine lifts once the copy is replaced").isFalse();
    }

    // ------------------------------------------------------------------------------------------------------------
    // The Ratis-initiated install, on the legacy refresh-existing path
    // ------------------------------------------------------------------------------------------------------------

    /** With auto-acquire off (or the leader's list unavailable) the install refreshed only the registered databases. */
    // Regression for #8464: a snapshot resync also covers a database closed on the node
    @Test
    void theLegacyRefreshInstallsADatabaseClosedOnThisNode() throws Exception {
      server.getConfiguration().setValue(GlobalConfiguration.HA_AUTO_ACQUIRE_DATABASES, false);
      leaderServes(DB_NAME);
      closeLocally(DB_NAME);

      final DatabaseReconciler reconciler = new DatabaseReconciler() {
        @Override
        LeaderDatabaseQuery.BootstrapState fetchSnapshotMarker(final String leaderPeerId, final String leaderHttpAddr, final String leaderHttpsAddr,
            final String clusterToken) {
          return new LeaderDatabaseQuery.BootstrapState(List.of(), TermIndex.valueOf(3L, 40L));
        }
      };
      reconciler.setServer(server);

      reconciler.reconcileDatabasesFromLeader("leader", leaderAddress, null, null, -1L);

      assertThat(downloadsOf(DB_NAME)).isEqualTo(1);
      assertThat(liveCount(DB_NAME)).isEqualTo(SNAPSHOT_COUNT);
    }

    // Regression for #8464: a snapshot resync also covers a database closed on the node
    /**
     * Issue #8558: the legacy refresh added the closed databases to a bare loop, so one the leader cannot serve - closed
     * there too, answered 404 on every retry - threw out of the reconcile, the install never registered its marker and the
     * leader re-notified it forever. A 404 is now the leader-missing verdict (issue #8559); any other failure is reported
     * as not installed, and the registered databases are refreshed either way.
     */
    @Test
    void theLegacyRefreshReportsAClosedDatabaseTheLeaderCannotServeInsteadOfFailingTheInstall() throws Exception {
      server.getConfiguration().setValue(GlobalConfiguration.HA_AUTO_ACQUIRE_DATABASES, false);
      createLocalDatabase(OTHER_DB);
      leaderServes(OTHER_DB);
      // A failure other than 404: a 404 says the leader does not hold it, which is not quarantined (issue #8559)
      leaderFails(DB_NAME);
      closeLocally(DB_NAME);

      final DatabaseReconciler.ReconcileFromLeaderResult result = markerOnlyReconciler().reconcileDatabasesFromLeader(
          "leader", leaderAddress, null, null, -1L);

      assertThat(result.notInstalled()).as("the closed copy nobody refreshed is reported, not thrown").containsExactly(DB_NAME);
      assertThat(result.leaderSnapshotTermIndex()).isEqualTo(MARKER);
      assertThat(liveCount(OTHER_DB)).as("the registered database was refreshed regardless").isEqualTo(SNAPSHOT_COUNT);
      assertThat(liveCount(DB_NAME)).as("nothing replaced the closed copy").isEqualTo(LIVE_COUNT);
    }

    // Regression for #8464: a snapshot resync also covers a database closed on the node
    /**
     * The same through the default configuration's route to the legacy refresh: auto-acquire on, the leader's full
     * listing failing once. The not-installed set then reaches the state machine, which quarantines the closed copy at
     * its own applied position and records the refreshed one at the snapshot index.
     */
    @Test
    void theDegradedAutoAcquireRefreshQuarantinesAClosedDatabaseTheLeaderCannotServe() throws Exception {
      createLocalDatabase(OTHER_DB);
      leaderServes(OTHER_DB);
      leaderFails(DB_NAME);
      closeLocally(DB_NAME);
      sm.writePersistedAppliedIndex(FLOOR, DB_NAME);

      final DatabaseReconciler reconciler = new DatabaseReconciler() {
        @Override
        LeaderDatabaseQuery.BootstrapState fetchBootstrapState(final String leaderPeerId, final String leaderHttpAddr, final String leaderHttpsAddr,
            final String clusterToken) throws IOException {
          throw new IOException("listing timed out");
        }

        @Override
        LeaderDatabaseQuery.BootstrapState fetchSnapshotMarker(final String leaderPeerId, final String leaderHttpAddr, final String leaderHttpsAddr,
            final String clusterToken) {
          return new LeaderDatabaseQuery.BootstrapState(List.of(), MARKER);
        }
      };
      reconciler.setServer(server);

      final DatabaseReconciler.ReconcileFromLeaderResult result = reconciler.reconcileDatabasesFromLeader("leader", leaderAddress,
          null, null, -1L);
      assertThat(result.notInstalled()).containsExactly(DB_NAME);
      assertThat(liveCount(OTHER_DB)).isEqualTo(SNAPSHOT_COUNT);

      sm.completeSnapshotInstall(MARKER.getIndex(), result.notInstalled());

      assertThat(sm.isDatabaseDiverged(DB_NAME)).as("the closed copy stays quarantined").isTrue();
      assertThat(sm.getDatabaseAppliedFloor(DB_NAME)).as("with a read floor at what it genuinely applied").isEqualTo(FLOOR);
      assertThat(sm.readPersistedAppliedIndex(DB_NAME)).as("and it is not recorded at the snapshot index").isEqualTo(FLOOR);
      assertThat(sm.readPersistedAppliedIndex(OTHER_DB)).isEqualTo(MARKER.getIndex());
      assertThat(sm.isDatabaseDiverged(OTHER_DB)).isFalse();
      assertThat(sm.isResyncInProgress()).as("the node stays out of the ready set while it holds that copy").isTrue();
    }

    // Regression for #8464: a snapshot resync also covers a database closed on the node
    /**
     * Only the closed ones are isolated: a REGISTERED database the leader failed to serve still fails the install. The
     * failure is a 503, which says nothing about whether the leader holds it; a 404 is the leader-missing verdict (issue
     * #8588), pinned by the next test.
     */
    @Test
    void theLegacyRefreshStillFailsTheInstallForARegisteredDatabaseTheLeaderCannotServe() {
      server.getConfiguration().setValue(GlobalConfiguration.HA_AUTO_ACQUIRE_DATABASES, false);
      // A 404 would now be the leader not holding it, which keeps the copy as LEADER_MISSING (issue #8588); a 503 says nothing
      // about whether it holds it, so the install still has to be re-driven
      leaderFails(DB_NAME);

      assertThatThrownBy(() -> markerOnlyReconciler().reconcileDatabasesFromLeader("leader", leaderAddress, null, null, -1L))
          .as("Ratis must re-drive the install for a database this node serves")
          .isInstanceOf(IOException.class);
      assertThat(liveCount(DB_NAME)).isEqualTo(LIVE_COUNT);
    }

    // Regression for #8464: a snapshot resync also covers a database closed on the node
    /**
     * Issue #8588: a REGISTERED database the leader answers 404 for is not a failed refresh - the same leader answers the
     * same on every retry, so failing the install for it made Ratis re-drive it forever. It is reported
     * {@code LEADER_MISSING} in the result's {@code leaderMissing} set (what keeps the install from recording it at the
     * snapshot index), and kept and served unchanged.
     */
    @Test
    void theLegacyRefreshReportsARegisteredDatabaseTheLeaderDoesNotHold() throws Exception {
      server.getConfiguration().setValue(GlobalConfiguration.HA_AUTO_ACQUIRE_DATABASES, false);
      // No context at all: the leader answers 404 for DB_NAME, which is registered here

      final DatabaseReconciler reconciler = markerOnlyReconciler();
      final DatabaseReconciler.ReconcileFromLeaderResult result = reconciler.reconcileDatabasesFromLeader("leader",
          leaderAddress, null, null, -1L);

      assertThat(result.leaderMissing()).as("reported instead of thrown").containsExactly(DB_NAME);
      assertThat(result.notInstalled()).isEmpty();
      assertThat(reconciler.getAcquireStatus(DB_NAME).state()).isEqualTo(DatabaseReconciler.AcquireState.LEADER_MISSING);
      assertThat(server.existsDatabase(DB_NAME)).as("still registered and served").isTrue();
      assertThat(liveCount(DB_NAME)).as("nothing replaced the copy").isEqualTo(LIVE_COUNT);
    }

    // Regression for #8464: a snapshot resync also covers a database closed on the node
    /**
     * On the auto-acquire path a closed database used to be invisible: neither refreshed nor reported. The leader not
     * holding it is now surfaced as {@code LEADER_MISSING}, and nothing is downloaded or dropped for it.
     */
    @Test
    void theAutoAcquireReconcileReportsAClosedDatabaseTheLeaderDoesNotHold() throws Exception {
      closeLocally(DB_NAME);

      final DatabaseReconciler reconciler = new DatabaseReconciler() {
        @Override
        LeaderDatabaseQuery.BootstrapState fetchBootstrapState(final String leaderPeerId, final String leaderHttpAddr, final String leaderHttpsAddr,
            final String clusterToken) {
          return new LeaderDatabaseQuery.BootstrapState(List.of(), TermIndex.valueOf(3L, 40L));
        }
      };
      reconciler.setServer(server);

      reconciler.reconcileDatabasesFromLeader("leader", leaderAddress, null, null, -1L);

      assertThat(reconciler.getAcquireStatus(DB_NAME)).isNotNull();
      assertThat(reconciler.getAcquireStatus(DB_NAME).state()).isEqualTo(DatabaseReconciler.AcquireState.LEADER_MISSING);
      assertThat(downloadsOf(DB_NAME)).isZero();
      assertThat(Files.isDirectory(root.resolve("databases").resolve(DB_NAME))).as("the local copy is kept").isTrue();
    }

    // ------------------------------------------------------------------------------------------------------------
    // What counts as a closed database
    // ------------------------------------------------------------------------------------------------------------

    // Regression for #8464: a snapshot resync also covers a database closed on the node
    /**
     * Only a directory the server would reopen under that name counts: not a registered one, not a reserved one (the
     * Raft control directory, the staging directories), not one whose name the server refuses to open, and not an
     * empty one, which holds nothing to serve.
     */
    @Test
    void onlyANonEmptyUnregisteredUserDirectoryIsAClosedDatabase() throws Exception {
      closeLocally(DB_NAME);
      final Path databases = root.resolve("databases");
      Files.createDirectories(databases.resolve("emptyDir"));
      Files.writeString(Files.createDirectories(databases.resolve(".dropped-x")).resolve("f"), "x");
      Files.writeString(databases.resolve("aPlainFile"), "x");
      // A name getDatabase refuses to open ('..' is rejected by checkDatabaseNameIsValid), so nothing can serve it
      Files.writeString(Files.createDirectories(databases.resolve("bad..name")).resolve("f"), "x");
      createLocalDatabase(OTHER_DB); // registered: the registry already covers it

      assertThat(SnapshotInstaller.closedDatabaseNames(server)).containsExactly(DB_NAME);
    }

    // ------------------------------------------------------------------------------------------------------------

    /** What {@code close database} does (ServerControlPlane.closeDatabase): close, deregister, leave the files. */
    private void closeLocally(final String name) {
      final ServerDatabase database = server.getDatabaseForOperator(name);
      database.getEmbedded().close();
      server.removeDatabase(name);
      assertThat(server.existsDatabase(name)).isFalse();
      assertThat(Files.isDirectory(root.resolve("databases").resolve(name))).as("the files stay on disk").isTrue();
    }

    private void createLocalDatabase(final String name) {
      final ServerDatabase live = server.getOrCreateDatabase(name);
      live.transaction(() -> {
        live.getSchema().createVertexType("Node");
        for (int i = 0; i < LIVE_COUNT; i++)
          live.newVertex("Node").set("v", i).save();
      });
    }

    /** The real reconciler with the leader's snapshot marker answered locally; the downloads still go to the stub leader. */
    private DatabaseReconciler markerOnlyReconciler() {
      final DatabaseReconciler reconciler = new DatabaseReconciler() {
        @Override
        LeaderDatabaseQuery.BootstrapState fetchSnapshotMarker(final String leaderPeerId, final String leaderHttpAddr, final String leaderHttpsAddr,
            final String clusterToken) {
          return new LeaderDatabaseQuery.BootstrapState(List.of(), MARKER);
        }
      };
      reconciler.setServer(server);
      return reconciler;
    }

    /** Reopens the database the way the next request would, and counts what it serves. */
    private long liveCount(final String name) {
      return server.getDatabaseForOperator(name).countType("Node", true);
    }

    private int downloadsOf(final String name) {
      final AtomicInteger count = downloads.get(name);
      return count == null ? 0 : count.get();
    }

    private void setStaleSnapshotAppliedFloor(final long floor) throws Exception {
      final Field f = ArcadeStateMachine.class.getDeclaredField("staleSnapshotAppliedFloor");
      f.setAccessible(true);
      ((AtomicLong) f.get(sm)).set(floor);
    }

    /** Makes the leader fail to serve {@code name} with a 503, a failure that says nothing about whether it holds it. */
    private void leaderFails(final String name) {
      leader.createContext("/api/v1/ha/snapshot/" + name, exchange -> {
        exchange.sendResponseHeaders(503, -1);
        exchange.close();
      });
    }

    /** Makes the leader serve a real database with a DIFFERENT record count under {@code name}. */
    private void leaderServes(final String name) throws IOException {
      final Path source = root.resolve("leader-copy").resolve(name);
      try (final Database snap = new DatabaseFactory(source.toString()).create()) {
        snap.transaction(() -> {
          snap.getSchema().createVertexType("Node");
          for (int i = 0; i < SNAPSHOT_COUNT; i++)
            snap.newVertex("Node").set("v", i).save();
        });
      }
      final byte[] zip = zipDirectory(source);
      final AtomicInteger counter = downloads.computeIfAbsent(name, k -> new AtomicInteger());
      leader.createContext("/api/v1/ha/snapshot/" + name, exchange -> {
        counter.incrementAndGet();
        exchange.sendResponseHeaders(200, zip.length);
        exchange.getResponseBody().write(zip);
        exchange.close();
      });
    }

    private static byte[] zipDirectory(final Path dir) throws IOException {
      final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      try (final ZipOutputStream zip = new ZipOutputStream(bytes); final Stream<Path> files = Files.walk(dir)) {
        for (final Path file : files.filter(Files::isRegularFile).toList()) {
          zip.putNextEntry(new ZipEntry(dir.relativize(file).toString().replace('\\', '/')));
          zip.write(Files.readAllBytes(file));
          zip.closeEntry();
        }
      }
      return bytes.toByteArray();
    }

    private ArcadeDBServer startServer() throws IOException {
      final Path databasesDir = root.resolve("databases");
      Files.createDirectories(databasesDir);

      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.SERVER_NAME, "ArcadeDB_8464");
      config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, databasesDir.toString());
      config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, root.toString());
      config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, PASSWORD);
      config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_HOST, "localhost");
      // A free port rather than the fixed 2480: anything else listening there would take this server's requests.
      config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_PORT, String.valueOf(StaticBaseServerTest.allocateFreePorts(1)[0]));
      config.setValue(GlobalConfiguration.HA_ENABLED, false);
      // Fail a download on its first attempt: the exponential backoff would only make the test slow.
      config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRIES, 0);
      config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRY_BASE_MS, 0L);
      config.setValue(GlobalConfiguration.NETWORK_USE_SSL, false);

      final ArcadeDBServer started = new ArcadeDBServer(config);
      started.start();
      return started;
    }
  }

  /**
   * Issue #8901: the stale-term divergence reformat must not fire on a follower whose Ratis division was restarted in
   * place and has not taken a single replicated entry since, under the same leader term. That is the dead replication
   * path of #8898 - the leader's append stream still bound to the closed server instance - not a divergence, and the
   * reformat turned a node that had only been paused into a voter with an empty log.
   */
  @Nested
  class DeadReplicationPathNoReformat {

    private static final long DURATION_MS = 5_000L;
    private static final long INTERVAL_MS = 1_000L;

    static final class FakeTarget implements HealthMonitor.HealthTarget {
      final    AtomicInteger divergenceRecover = new AtomicInteger();
      volatile boolean       stuckDiverged     = true;
      volatile boolean       pathUnproven      = true;

      @Override
      public LifeCycle.State getRaftLifeCycleState() {
        return LifeCycle.State.RUNNING;
      }

      @Override
      public boolean isShutdownRequested() {
        return false;
      }

      @Override
      public void restartRatisIfNeeded() {
      }

      @Override
      public boolean isFollowerStuckDiverged() {
        return stuckDiverged;
      }

      @Override
      public void recoverFromDivergence() {
        divergenceRecover.incrementAndGet();
      }

      @Override
      public boolean isReplicationPathUnprovenSinceRestart() {
        return pathUnproven;
      }
    }

    private static HealthMonitor monitor(final FakeTarget target, final AtomicLong clock, final boolean recovery) {
      final HealthMonitor monitor = new HealthMonitor(target, INTERVAL_MS, 0L, DURATION_MS, recovery, 3, 0, DURATION_MS);
      monitor.setClock(clock::get);
      return monitor;
    }

    private static void tickFor(final HealthMonitor monitor, final AtomicLong clock, final long durationMs) {
      for (long elapsed = 0; elapsed < durationMs; elapsed += INTERVAL_MS) {
        monitor.tick();
        clock.addAndGet(INTERVAL_MS);
      }
    }

    // Regression for #8901: no divergence reformat on a follower with a dead replication path
    @Test
    void noReformatWhileNoEntryReachedTheRestartedDivision() {
      final FakeTarget target = new FakeTarget();
      final AtomicLong clock = new AtomicLong(0L);
      final HealthMonitor monitor = monitor(target, clock, true);

      tickFor(monitor, clock, DURATION_MS * 10);

      assertThat(target.divergenceRecover.get())
          .as("a restarted division that has taken no entry under the same term is a dead path, not a divergence")
          .isZero();
      assertThat(monitor.isFollowerStuckDivergedConfirmed())
          .as("the stuck signature is still reported to the operator: only the destructive action is held back")
          .isTrue();
    }

    // Regression for #8901: no divergence reformat on a follower with a dead replication path
    @Test
    void theWindowStartsOverOnceTheReplicationPathIsProven() {
      final FakeTarget target = new FakeTarget();
      final AtomicLong clock = new AtomicLong(0L);
      final HealthMonitor monitor = monitor(target, clock, true);

      tickFor(monitor, clock, DURATION_MS * 3);
      assertThat(target.divergenceRecover.get()).isZero();

      // An entry landed (or the term moved): from here on a persisting signature is a divergence again, but the
      // whole window has to elapse first. A leader whose first append after a reconnect is rejected and corrected
      // must not be reformatted on the very tick the path came back.
      target.pathUnproven = false;
      tickFor(monitor, clock, DURATION_MS - INTERVAL_MS);
      assertThat(target.divergenceRecover.get())
          .as("the time spent on a dead path does not count toward the divergence window")
          .isZero();

      tickFor(monitor, clock, 2 * INTERVAL_MS);
      assertThat(target.divergenceRecover.get())
          .as("a divergence that persists for the full window once appends reach the division is still reformatted")
          .isEqualTo(1);
    }

    // Regression for #8901: no divergence reformat on a follower with a dead replication path
    @Test
    void aDivisionNeverRestartedInPlaceKeepsTheReformat() {
      final FakeTarget target = new FakeTarget();
      target.pathUnproven = false;
      final AtomicLong clock = new AtomicLong(0L);
      final HealthMonitor monitor = monitor(target, clock, true);

      tickFor(monitor, clock, DURATION_MS + 2 * INTERVAL_MS);

      assertThat(target.divergenceRecover.get())
          .as("issue #4741: a divergence persisted across a process restart must still be reformatted")
          .isEqualTo(1);
    }

    // Regression for #8901: no divergence reformat on a follower with a dead replication path
    @Test
    void theDeadPathIsReportedWithRecoveryDisabled() {
      final FakeTarget target = new FakeTarget();
      final AtomicLong clock = new AtomicLong(0L);
      final HealthMonitor monitor = monitor(target, clock, false);

      tickFor(monitor, clock, DURATION_MS * 3);

      assertThat(target.divergenceRecover.get()).isZero();
      assertThat(monitor.isFollowerStuckDivergedConfirmed()).isTrue();
    }

    // Regression for #8901: no divergence reformat on a follower with a dead replication path
    @Test
    void pathUnprovenOnlyAfterAnInPlaceRestartWithNoEntryAndTheSameTerm() {
      final TermIndex last = TermIndex.valueOf(27, 1_564_566);
      final RaftHAServer.InPlaceRestartBaseline baseline = new RaftHAServer.InPlaceRestartBaseline(last, 28, true);

      assertThat(RaftHAServer.replicationPathUnproven(null, last, 28, true))
          .as("no in-place restart in this process: no old server instance can hold the leader's stream")
          .isFalse();
      assertThat(RaftHAServer.replicationPathUnproven(baseline, last, 28, true))
          .as("the #8898 shape: same last entry, same term as right after the restart")
          .isTrue();
      assertThat(RaftHAServer.replicationPathUnproven(baseline, TermIndex.valueOf(28, 1_564_567), 28, true))
          .as("an entry was appended since the restart: the leader's appends reach this division")
          .isFalse();
      assertThat(RaftHAServer.replicationPathUnproven(baseline, TermIndex.valueOf(26, 1_564_566), 28, true))
          .as("a truncate-and-append back to the same index is still an append that reached the division")
          .isFalse();
      assertThat(RaftHAServer.replicationPathUnproven(baseline, last, 29, true))
          .as("a newer term means a new leader, whose appenders dial the running server")
          .isFalse();
      assertThat(RaftHAServer.replicationPathUnproven(new RaftHAServer.InPlaceRestartBaseline(null, 28, true), null, 28, true))
          .as("a reformatted, still empty log that took nothing is unproven too")
          .isTrue();
      assertThat(RaftHAServer.replicationPathUnproven(baseline, last, 29, false))
          .as("a term bumped by a candidate's vote request, with no leader at it, proves nothing")
          .isTrue();
      assertThat(RaftHAServer.replicationPathUnproven(baseline, last, -1, true))
          .as("an unreadable term is no proof: hold the destructive action")
          .isTrue();
    }

    // Regression for #8901: no divergence reformat on a follower with a dead replication path
    @Test
    void aPendingBaselineIsResolvedFirstAndOnlyALaterMoveProvesThePath() {
      final AtomicReference<RaftHAServer.InPlaceRestartBaseline> ref = new AtomicReference<>(RatisInPlaceRestart.PENDING_BASELINE);
      final AtomicReference<RaftHAServer.InPlaceRestartBaseline> position = new AtomicReference<>();

      assertThat(RaftHAServer.replicationPathUnproven(ref, position::get))
          .as("still unreadable: nothing to compare against, hold").isTrue();
      assertThat(ref.get()).isSameAs(RatisInPlaceRestart.PENDING_BASELINE);

      position.set(new RaftHAServer.InPlaceRestartBaseline(TermIndex.valueOf(27, 10), 28, true));
      assertThat(RaftHAServer.replicationPathUnproven(ref, position::get))
          .as("the first readable position becomes the baseline, it cannot prove anything itself").isTrue();
      assertThat(ref.get()).isEqualTo(position.get());

      assertThat(RaftHAServer.replicationPathUnproven(ref, position::get)).isTrue();

      position.set(new RaftHAServer.InPlaceRestartBaseline(TermIndex.valueOf(28, 11), 28, true));
      assertThat(RaftHAServer.replicationPathUnproven(ref, position::get)).isFalse();
      assertThat(ref.get()).as("a proven path drops the baseline: the answer stays false until the next restart").isNull();

      position.set(null);
      assertThat(RaftHAServer.replicationPathUnproven(ref, position::get))
          .as("with no baseline an unreadable division is no reason to hold anything").isFalse();
    }

    // Regression for #8901: no divergence reformat on a follower with a dead replication path
    @Test
    void anUnreadableDivisionHoldsWithoutTouchingTheBaseline() {
      final RaftHAServer.InPlaceRestartBaseline baseline = new RaftHAServer.InPlaceRestartBaseline(TermIndex.valueOf(27, 10), 28, true);
      final AtomicReference<RaftHAServer.InPlaceRestartBaseline> ref = new AtomicReference<>(baseline);

      assertThat(RaftHAServer.replicationPathUnproven(ref, () -> null)).isTrue();
      assertThat(ref.get()).isSameAs(baseline);
    }

    // Regression for #8901: no divergence reformat on a follower with a dead replication path
    @Test
    void aNewerRestartBaselineIsNeverOverwrittenByAStaleEvaluation() {
      final RaftHAServer.InPlaceRestartBaseline first = new RaftHAServer.InPlaceRestartBaseline(TermIndex.valueOf(27, 10), 28, true);
      final RaftHAServer.InPlaceRestartBaseline second = new RaftHAServer.InPlaceRestartBaseline(TermIndex.valueOf(28, 20), 28, true);
      final AtomicReference<RaftHAServer.InPlaceRestartBaseline> ref = new AtomicReference<>(first);

      // The evaluation read the first baseline, then a second in-place restart published its own before the evaluation
      // got round to dropping the first: the drop must not erase the second.
      final boolean unproven = RaftHAServer.replicationPathUnproven(ref, () -> {
        ref.set(second);
        return new RaftHAServer.InPlaceRestartBaseline(TermIndex.valueOf(28, 11), 28, true);
      });

      assertThat(unproven).isFalse();
      assertThat(ref.get()).isSameAs(second);
    }

    // Regression for #8901: no divergence reformat on a follower with a dead replication path
    @Test
    void theDeadPathWarningRepeatsWhileTheHoldLasts() {
      final FakeTarget target = new FakeTarget();
      final AtomicLong clock = new AtomicLong(0L);
      final HealthMonitor monitor = monitor(target, clock, true);

      tickFor(monitor, clock, DURATION_MS * 3);
      assertThat(monitor.getDeadPathReports()).as("logged once, not on every tick").isEqualTo(1);

      tickFor(monitor, clock, HealthMonitor.DEAD_PATH_REPORT_INTERVAL_MS);
      assertThat(monitor.getDeadPathReports()).as("repeated while the hold lasts, for an operator who missed it").isEqualTo(2);

      target.pathUnproven = false;
      monitor.tick();
      target.pathUnproven = true;
      clock.addAndGet(INTERVAL_MS);
      monitor.tick();
      assertThat(monitor.getDeadPathReports()).as("a new hold after the path was proven logs at once").isEqualTo(3);
    }
  }

  /**
   * Regression tests for issue #8380. The crash-loop escalation record of issue #7736 lived inside the Raft storage
   * directory, which the divergence reformat ({@code RaftHAServer.restartRatis(true)}, reached automatically from
   * {@link HealthMonitor}'s stuck-follower check) deletes wholesale. Nothing told the monitor, so its
   * {@code crashLoopEscalationPersisted} flag stayed true over a record that was gone: the next escalation in that
   * lifetime skipped the write, and the next process start walked the whole restart/reformat/snapshot-download ladder
   * again - the per-lifetime cost #7736 was filed to remove.
   * <p>
   * The record now lives beside the storage directory, and every escalation writes it and re-derives the flag from the
   * disk instead of trusting the cached one.
   */
  @Nested
  class CrashLoopRecordSurvivesDivergenceReformat {

    private static final int  THRESHOLD         = 3;
    private static final long RECOVERY_DURATION = 5_000L;

    /** The lifecycle of a Raft division, including the stuck-at-stale-term signature that triggers a reformat. */
    private static final class Lifecycle {
      volatile LifeCycle.State state     = LifeCycle.State.CLOSED;
      volatile boolean         stuck     = false;
      final    AtomicInteger   restarts  = new AtomicInteger();
      final    AtomicInteger   reformats = new AtomicInteger();
    }

    /**
     * A health target whose escalation record is kept on disk by a real {@link RaftHAServer}, and whose divergence
     * reformat does what {@code restartRatis(true)} does to the disk: delete the Raft storage directory wholesale.
     */
    private static HealthMonitor.HealthTarget onDisk(final Lifecycle lifecycle, final RaftHAServer store, final File storageDir) {
      return new HealthMonitor.HealthTarget() {
        @Override
        public LifeCycle.State getRaftLifeCycleState() {
          return lifecycle.state;
        }

        @Override
        public boolean isShutdownRequested() {
          return false;
        }

        @Override
        public void restartRatisIfNeeded() {
          lifecycle.restarts.incrementAndGet();
        }

        @Override
        public boolean isFollowerStuckDiverged() {
          return lifecycle.stuck;
        }

        @Override
        public void recoverFromDivergence() {
          lifecycle.reformats.incrementAndGet();
          deleteRecursively(storageDir);
          assertThat(storageDir).as("the reformat deleted the Raft storage directory").doesNotExist();
        }

        @Override
        public boolean hasPersistedCrashLoopEscalation() {
          return store.hasPersistedCrashLoopEscalation();
        }

        @Override
        public boolean persistCrashLoopEscalation(final String reason) {
          return store.persistCrashLoopEscalation(reason);
        }

        @Override
        public void clearPersistedCrashLoopEscalation() {
          store.clearPersistedCrashLoopEscalation();
        }
      };
    }

    private static RaftHAServer detachedServer(final File raftStorageRoot) {
      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.HA_SERVER_LIST, "localhost:2434:2480");
      config.setValue(GlobalConfiguration.HA_RAFT_STORAGE_DIRECTORY, raftStorageRoot.getAbsolutePath());
      final ContextConfiguration configuration = new ContextConfiguration();
      configuration.setValue(GlobalConfiguration.SERVER_NAME, "localhost");
      final ArcadeDBServer server = TestServerHelper.unstartedServer(raftStorageRoot.toPath(), configuration);
      return new RaftHAServer(server, config);
    }

    private static HealthMonitor newLifetime(final HealthMonitor.HealthTarget target, final AtomicLong clock) {
      final HealthMonitor monitor = new HealthMonitor(target, 1000, 0L, RECOVERY_DURATION, true, 2, THRESHOLD);
      monitor.setClock(clock::get);
      return monitor;
    }

    private static void ticks(final HealthMonitor monitor, final AtomicLong clock, final int count) {
      for (int i = 0; i < count; i++) {
        clock.addAndGet(1_000);
        monitor.tick();
      }
    }

    private static void deleteRecursively(final File dir) {
      if (!dir.exists())
        return;
      try (final Stream<Path> paths = Files.walk(dir.toPath())) {
        paths.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
      } catch (final IOException e) {
        throw new IllegalStateException(e);
      }
    }

    // Regression for #8380: the crash-loop record survives the divergence reformat
    @Test
    void theRecordIsNotInsideTheRaftStorageDirectory(@TempDir final Path root) {
      final File storageDir = new File(root.toFile(), "raft-storage-peer");
      final File record = RaftStorageFiles.crashLoopEscalationMarkerFile(storageDir);

      assertThat(record.getParentFile().getAbsoluteFile()).isEqualTo(root.toFile().getAbsoluteFile());
      assertThat(record.toPath().startsWith(storageDir.toPath())).as("a reformat deletes everything under the storage dir")
          .isFalse();
      assertThat(record.getName()).isEqualTo("raft-storage-peer." + RaftStorageFiles.CRASH_LOOP_ESCALATION_MARKER);
    }

    // Regression for #8380: the crash-loop record survives the divergence reformat
    @Test
    void aRecordSurvivesTheDeletionOfTheRaftStorageDirectory(@TempDir final Path root) {
      final RaftHAServer server = detachedServer(root.toFile());
      assertThat(server.persistCrashLoopEscalation("Ratis crash-loop persists after 11 restarts")).isTrue();
      // The storage exists with a Raft group in it, as on a running node.
      final File storageDir = storageDirOrCreate(root.toFile());

      deleteRecursively(storageDir); // what restartRatis(true) does before it FORMATs

      assertThat(storageDir).doesNotExist();
      assertThat(server.hasPersistedCrashLoopEscalation()).as("the reformat must not take the record with it").isTrue();
      assertThat(detachedServer(root.toFile()).hasPersistedCrashLoopEscalation()).as("and the next process start reads it")
          .isTrue();
    }

    // Regression for #8380: the crash-loop record survives the divergence reformat
    @Test
    void theIssueRepro_aDivergenceReformatInAnInheritedLifetimeDoesNotLoseTheEscalation(@TempDir final Path root) {
      final Lifecycle lifecycle = new Lifecycle();
      final AtomicLong clock = new AtomicLong();

      // 1. The first lifetime crash-loops, takes its one crash-loop reformat, crash-loops again and escalates.
      final HealthMonitor first = newLifetime(onDisk(lifecycle, detachedServer(root.toFile()), new File(root.toFile(), "none")),
          clock);
      ticks(first, clock, 2 * (THRESHOLD + 1));
      assertThat(first.isCrashLoopEscalated()).isTrue();
      assertThat(first.isCrashLoopRestartPending()).as("recorded: asks for its one process restart").isTrue();
      assertThat(lifecycle.reformats.get()).isEqualTo(1);

      // 2. The restarted process inherits the record. Its Raft storage exists with a group in it.
      final RaftHAServer secondStore = detachedServer(root.toFile());
      final File storageDir = storageDirOrCreate(root.toFile());
      final HealthMonitor second = newLifetime(onDisk(lifecycle, secondStore, storageDir), clock);
      assertThat(second.isCrashLoopEscalated()).isTrue();
      assertThat(second.isCrashLoopRestartPending()).isFalse();

      // 3. Its division comes up; 4. then it meets the stuck-at-stale-term signature long enough for the reformat.
      lifecycle.state = LifeCycle.State.RUNNING;
      lifecycle.stuck = true;
      ticks(second, clock, (int) (RECOVERY_DURATION / 1_000) + 2);
      assertThat(lifecycle.reformats.get()).as("the stuck-follower check reformatted the Raft storage").isEqualTo(2);
      assertThat(secondStore.hasPersistedCrashLoopEscalation())
          .as("the reformat deleted the storage directory, not the escalation record next to it").isTrue();

      // 5. The crash loop returns: the node re-escalates with the record on disk and asks for no restart.
      lifecycle.stuck = false;
      lifecycle.state = LifeCycle.State.CLOSED;
      ticks(second, clock, 2 * (THRESHOLD + 1));
      assertThat(second.isCrashLoopEscalated()).isTrue();
      assertThat(second.isCrashLoopRestartPending()).isFalse();
      assertThat(lifecycle.reformats.get()).as("no crash-loop reformat either: it is still spent").isEqualTo(2);

      // 6. The next process start inherits the escalation instead of re-arming the whole ladder.
      final int restartsBefore = lifecycle.restarts.get();
      final HealthMonitor third = newLifetime(onDisk(lifecycle, detachedServer(root.toFile()), storageDir), clock);
      assertThat(third.isCrashLoopEscalated()).as("inherited, not re-armed").isTrue();
      ticks(third, clock, 50);
      assertThat(lifecycle.restarts.get()).as("no restart ladder in the third lifetime").isEqualTo(restartsBefore);
      assertThat(lifecycle.reformats.get()).as("no reformat and snapshot download in the third lifetime").isEqualTo(2);
    }

    // Regression for #8380: the crash-loop record survives the divergence reformat
    @Test
    void anEscalationRewritesARecordThatWasDeletedUnderTheMonitor() {
      // The monitor's flag is a cache of a file something else can delete (an operator, or a reformat on a layout
      // that keeps the record inside the storage). A stale-true flag must not skip the write of the next escalation.
      final HealthMonitorTickTest.CrashLoopEscalationSurvivesRestart.RecordingTarget target =
          new HealthMonitorTickTest.CrashLoopEscalationSurvivesRestart.RecordingTarget();
      final HealthMonitor first = new HealthMonitor(target, 1000, 0L, RECOVERY_DURATION, true, 2, THRESHOLD);
      for (int i = 0; i < 2 * (THRESHOLD + 1); i++)
        first.tick();
      assertThat(target.record.get()).isNotNull();

      final AtomicLong clock = new AtomicLong();
      final HealthMonitor second = newLifetime(target, clock);
      target.state.set(LifeCycle.State.RUNNING);
      ticks(second, clock, 1);
      assertThat(second.isCrashLoopEscalated()).isFalse();

      target.record.set(null); // gone from the disk; the monitor was not told

      target.state.set(LifeCycle.State.CLOSED);
      ticks(second, clock, 2 * (THRESHOLD + 1));
      assertThat(second.isCrashLoopEscalated()).isTrue();
      assertThat(target.record.get()).as("the escalation is on disk again for the next process start")
          .contains("Ratis crash-loop persists");
      assertThat(second.isCrashLoopRestartPending()).as("still the inherited incident: no second restart").isFalse();
    }

    // Regression for #8380: the crash-loop record survives the divergence reformat
    @Test
    void aFailedRewriteOfARecordStillOnDiskKeepsItRecorded() {
      // Re-deriving the flag from the disk must not lower it when a rewrite fails but the earlier record is intact:
      // lowering it would re-arm the reformat for a node whose next start will inherit the escalation anyway.
      final HealthMonitorTickTest.CrashLoopEscalationSurvivesRestart.RecordingTarget target =
          new HealthMonitorTickTest.CrashLoopEscalationSurvivesRestart.RecordingTarget();
      target.record.set("left by the previous lifetime");
      target.recordWritable = false;
      final AtomicLong clock = new AtomicLong();
      final HealthMonitor monitor = newLifetime(target, clock);
      target.state.set(LifeCycle.State.RUNNING);
      ticks(monitor, clock, 1);

      target.state.set(LifeCycle.State.CLOSED);
      ticks(monitor, clock, 2 * (THRESHOLD + 1));
      assertThat(monitor.isCrashLoopEscalated()).isTrue();
      assertThat(target.reformats.get()).as("the inherited reformat stays spent").isZero();
      assertThat(target.record.get()).isEqualTo("left by the previous lifetime");

      // A long healthy spell afterwards still clears it through the ordinary ten-minute window.
      target.state.set(LifeCycle.State.RUNNING);
      ticks(monitor, clock, 1);
      clock.addAndGet(HealthMonitor.CRASH_LOOP_RECORD_RESET_MS);
      monitor.tick();
      assertThat(target.record.get()).isNull();
    }

    // Regression for #8380: the crash-loop record survives the divergence reformat
    @Test
    void aNonPersistentStorageWipeDiscardsTheRecordWithTheStorage(@TempDir final Path root) {
      // With arcadedb.ha.raftPersistStorage=false every start formats a fresh log; before #8380 the record went with
      // the directory, and moving it beside the directory must not make it outlive the storage it describes.
      final RaftHAServer server = detachedServer(root.toFile());
      assertThat(server.persistCrashLoopEscalation("Ratis crash-loop persists after 11 restarts")).isTrue();
      final File storageDir = storageDirOrCreate(root.toFile());

      RaftStorageFiles.discardNonPersistentRaftStorage(storageDir);

      assertThat(storageDir).doesNotExist();
      assertThat(RaftStorageFiles.crashLoopEscalationMarkerFile(storageDir)).doesNotExist();
      assertThat(server.hasPersistedCrashLoopEscalation()).isFalse();
      RaftStorageFiles.discardNonPersistentRaftStorage(storageDir); // idempotent on a missing storage
    }

    /**
     * The peer's Raft storage directory as {@link RaftHAServer} resolves it for {@link #detachedServer}, created with a
     * Raft group inside as on a running node. Resolved independently of where the record is kept, so the tests below
     * fail on the behaviour - the record lost with the storage - rather than on a path.
     */
    private static File storageDirOrCreate(final File root) {
      final File storageDir = RaftStorageFiles.resolveRaftStorageDir(root.getAbsolutePath(), null, root.getAbsolutePath(),
          "localhost_2434");
      if (!storageDir.exists())
        assertThat(new File(storageDir, "group-0").mkdirs()).isTrue();
      return storageDir;
    }
  }
}
