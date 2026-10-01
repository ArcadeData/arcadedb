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
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerDatabase;
import com.arcadedb.server.StaticBaseServerTest;
import com.sun.net.httpserver.HttpServer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.FileInfo;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Regression tests for issue #8559: a snapshot resync quarantined a database closed on this node whenever the leader
 * could not serve it, and a quarantine holds the WHOLE node out of the ready set. When the leader does not hold the
 * database at all it answers 404 on every retry, so the quarantine could never lift: a directory the node was not even
 * serving became a permanent readiness outage.
 * <p>
 * The leader's 404 is now classified as the verdict it is - the cluster's leader does not hold that database, the
 * auto-acquire reconcile's {@code LEADER_MISSING} - on every path that installs a closed database: the full resync,
 * the targeted resync of a quarantine, and the legacy refresh of a Ratis-initiated install. The copy is kept and
 * reported; nothing is quarantined for it. Every other failure still quarantines.
 * <p>
 * Issue #8588 extends the same verdict to a REGISTERED database the leader does not hold - one the cluster dropped while
 * this node was down, whose drop entry was compacted away before it caught up, which {@code loadDatabases} registers
 * again at boot. It used to fail the whole full resync, the whole legacy install and every targeted resync of its
 * quarantine, each retried against the same 404 for good, while the auto-acquire reconcile - the default path - reported
 * the very same input {@code LEADER_MISSING} and completed. Every path now reports it the way the auto-acquire one does.
 * <p>
 * The fixture is the one {@link Issue8464ResyncCoversClosedDatabaseTest} uses: a real {@link ArcadeDBServer} (HA off),
 * a real {@link ArcadeStateMachine}, a mocked follower-side {@link RaftHAServer}, and a local HTTP server standing in
 * for the leader's snapshot endpoint, which answers 404 for every database it has no context for - exactly what
 * {@link SnapshotHttpHandler} answers for a database it does not hold registered.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
@Timeout(120)
class Issue8559ClosedDatabaseTheLeaderDoesNotHoldTest {

  private static final String     DB_NAME        = "db8559";
  private static final String     OTHER_DB       = "db8559other";
  private static final String     PASSWORD       = "DefaultPasswordForTests";
  private static final int        LIVE_COUNT     = 12;
  private static final int        SNAPSHOT_COUNT = 27;
  private static final long       FLOOR          = 5L;
  private static final long       MARKER         = 40L;
  private static final RaftPeerId LOCAL          = RaftPeerId.valueOf("local");
  private static final RaftPeerId LEADER         = RaftPeerId.valueOf("leader");

  @TempDir
  Path root;

  private ArcadeDBServer     server;
  private ArcadeStateMachine sm;
  private HttpServer         leader;
  private String             leaderAddress;

  @BeforeEach
  void setUp() throws IOException {
    server = startServer();
    createLocalDatabase(DB_NAME);

    leader = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    leader.start();
    leaderAddress = "localhost:" + leader.getAddress().getPort();

    final RaftHAServer raft = mock(RaftHAServer.class);
    when(raft.isLeader()).thenReturn(false);
    when(raft.getLocalPeerId()).thenReturn(LOCAL);
    when(raft.getLocalHttpAddress()).thenReturn("local-host:2480");
    when(raft.getClusterToken()).thenReturn(null);
    when(raft.getLeaderId()).thenReturn(LEADER);
    when(raft.getUnambiguousPeerHttpAddress(LEADER)).thenReturn(leaderAddress);

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
            ((DatabaseInternal) server.getDatabase(name)).getEmbedded().drop();
        } catch (final Exception ignore) {
          // best-effort cleanup; the @TempDir is removed regardless
        }
      server.stop();
    }
  }

  // ------------------------------------------------------------------------------------------------------------
  // The download itself
  // ------------------------------------------------------------------------------------------------------------

  /** The leader's 404 is the only failure typed as "the leader does not hold it"; a 503 is an ordinary failure. */
  @Test
  void onlyALeader404IsTypedAsTheLeaderNotHoldingTheDatabase() throws Exception {
    leaderAnswers(DB_NAME, 503, 404); // a failed attempt first: only the LAST attempt decides the type
    leaderAnswers(OTHER_DB, 503);
    final Path staging = Files.createDirectories(root.resolve("staging"));

    assertThatThrownBy(() -> SnapshotInstaller.downloadWithRetry(DB_NAME, staging, leaderAddress, null, 1, 0L))
        .isInstanceOf(LeaderDoesNotHoldDatabaseException.class);
    assertThatThrownBy(() -> SnapshotInstaller.downloadWithRetry(OTHER_DB, staging, leaderAddress, null, 1, 0L))
        .isInstanceOf(IOException.class)
        .isNotInstanceOf(LeaderDoesNotHoldDatabaseException.class);
  }

  // ------------------------------------------------------------------------------------------------------------
  // The full resync
  // ------------------------------------------------------------------------------------------------------------

  /**
   * The issue as reported: the leader does not hold the closed database, so it is reported LEADER_MISSING and kept,
   * the other databases are reinstalled, and the node comes back ready instead of quarantined for good.
   */
  @Test
  void aFullResyncDoesNotQuarantineAClosedDatabaseTheLeaderDoesNotHold() throws Exception {
    createLocalDatabase(OTHER_DB);
    leaderServes(OTHER_DB); // no context for DB_NAME: the leader answers 404 for it
    closeLocally(DB_NAME);
    sm.writePersistedAppliedIndex(FLOOR, DB_NAME);
    setStaleSnapshotAppliedFloor(FLOOR);

    sm.triggerSnapshotDownload();

    assertThat(liveCount(OTHER_DB)).as("the registered database was reinstalled").isEqualTo(SNAPSHOT_COUNT);
    assertThat(sm.isDatabaseDiverged(DB_NAME)).as("a copy the leader does not hold is not quarantined").isFalse();
    assertThat(sm.getDatabaseAppliedFloor(DB_NAME)).as("nor clamped").isEqualTo(-1L);
    assertThat(sm.getStaleSnapshotAppliedFloor()).as("the node-wide floor resolves").isEqualTo(-1L);
    assertThat(sm.isResyncInProgress()).as("so the node is ready again").isFalse();
    assertThat(leaderMissing(DB_NAME)).as("it is reported the way the auto-acquire reconcile reports it").isTrue();
    assertThat(sm.readPersistedAppliedIndex(DB_NAME)).as("and not laundered into applied").isEqualTo(FLOOR);
    assertThat(liveCount(DB_NAME)).as("the local copy is kept untouched").isEqualTo(LIVE_COUNT);
  }

  /**
   * Nothing replaced the closed copy, so a bootstrap-divergence mark on it - the copy the cluster's first-formation
   * baseline decided against - is kept, while the one on a database the resync did reinstall is cleared.
   */
  @Test
  void aFullResyncKeepsTheBootstrapMarkOfAClosedDatabaseTheLeaderDoesNotHold() throws Exception {
    createLocalDatabase(OTHER_DB);
    leaderServes(OTHER_DB);
    sm.markBootstrapUnreconciled(DB_NAME);
    sm.markBootstrapUnreconciled(OTHER_DB);
    closeLocally(DB_NAME);
    setStaleSnapshotAppliedFloor(FLOOR);

    sm.triggerSnapshotDownload();

    assertThat(sm.getBootstrapUnreconciledDatabases()).containsExactly(DB_NAME);
    assertThat(sm.isDatabaseDiverged(DB_NAME)).isFalse();
  }

  /** Any other failure is still an install that did not happen, and still quarantines. */
  @Test
  void aFullResyncStillQuarantinesAClosedDatabaseTheLeaderFailedToServe() throws Exception {
    createLocalDatabase(OTHER_DB);
    leaderServes(OTHER_DB);
    leaderAnswers(DB_NAME, 503);
    closeLocally(DB_NAME);
    sm.writePersistedAppliedIndex(FLOOR, DB_NAME);
    setStaleSnapshotAppliedFloor(FLOOR);

    sm.triggerSnapshotDownload();

    assertThat(sm.isDatabaseDiverged(DB_NAME)).isTrue();
    assertThat(sm.getDatabaseAppliedFloor(DB_NAME)).isEqualTo(FLOOR);
    assertThat(sm.isResyncInProgress()).isTrue();
    assertThat(leaderMissing(DB_NAME)).isFalse();
  }

  /** A closed database the leader DOES hold is still reinstalled, and a stale LEADER_MISSING verdict on it is lifted. */
  @Test
  void aFullResyncThatInstallsAClosedDatabaseLiftsAStaleLeaderMissingVerdict() throws Exception {
    leaderServes(DB_NAME);
    closeLocally(DB_NAME);
    sm.getReconciler().markLeaderMissing(DB_NAME);
    setStaleSnapshotAppliedFloor(FLOOR);

    sm.triggerSnapshotDownload();

    assertThat(liveCount(DB_NAME)).isEqualTo(SNAPSHOT_COUNT);
    assertThat(leaderMissing(DB_NAME)).isFalse();
    assertThat(sm.isResyncInProgress()).isFalse();
  }

  /**
   * Issue #8588: a REGISTERED database the leader does not hold failed the WHOLE full resync - the node-wide floor
   * stayed, the node stayed out of the ready set, and the health tick retried against the same 404 for good. It is now
   * reported LEADER_MISSING and kept, as the auto-acquire reconcile reports it, and the resync completes.
   */
  @Test
  void aFullResyncReportsARegisteredDatabaseTheLeaderDoesNotHoldAndCompletes() throws Exception {
    createLocalDatabase(OTHER_DB);
    leaderServes(OTHER_DB); // no context for DB_NAME: the leader answers 404 for it
    sm.writePersistedAppliedIndex(FLOOR, DB_NAME);
    setStaleSnapshotAppliedFloor(FLOOR);
    registerSnapshotMarker(MARKER);

    sm.triggerSnapshotDownload();

    assertThat(liveCount(OTHER_DB)).as("the other registered database was reinstalled").isEqualTo(SNAPSHOT_COUNT);
    assertThat(sm.readPersistedAppliedIndex(OTHER_DB)).as("the reinstalled one is at the marker").isEqualTo(MARKER);
    assertThat(sm.readPersistedAppliedIndex(DB_NAME)).as("the leader-missing one is not laundered into applied")
        .isEqualTo(FLOOR);
    assertThat(sm.getStaleSnapshotAppliedFloor()).as("the node-wide floor resolves").isEqualTo(-1L);
    assertThat(sm.isResyncInProgress()).as("so the node is ready again").isFalse();
    assertThat(sm.isDatabaseDiverged(DB_NAME)).as("not quarantined").isFalse();
    assertThat(leaderMissing(DB_NAME)).as("reported the way the auto-acquire reconcile reports it").isTrue();
    assertThat(server.existsDatabase(DB_NAME)).as("still registered").isTrue();
    assertThat(liveCount(DB_NAME)).as("the local copy is kept untouched").isEqualTo(LIVE_COUNT);
  }

  /** Nothing replaced the registered copy either, so its bootstrap-divergence mark is kept (issue #8588). */
  @Test
  void aFullResyncKeepsTheBootstrapMarkOfARegisteredDatabaseTheLeaderDoesNotHold() throws Exception {
    createLocalDatabase(OTHER_DB);
    leaderServes(OTHER_DB);
    sm.markBootstrapUnreconciled(DB_NAME);
    sm.markBootstrapUnreconciled(OTHER_DB);
    setStaleSnapshotAppliedFloor(FLOOR);

    sm.triggerSnapshotDownload();

    assertThat(sm.getBootstrapUnreconciledDatabases()).containsExactly(DB_NAME);
  }

  /** Any other failure of a registered database still fails the full resync closed, floor kept (issue #8588). */
  @Test
  void aFullResyncStillFailsForARegisteredDatabaseTheLeaderFailedToServe() throws Exception {
    createLocalDatabase(OTHER_DB);
    leaderServes(OTHER_DB);
    leaderAnswers(DB_NAME, 503);
    setStaleSnapshotAppliedFloor(FLOOR);

    sm.triggerSnapshotDownload();

    assertThat(sm.getStaleSnapshotAppliedFloor()).isEqualTo(FLOOR);
    assertThat(sm.isResyncInProgress()).isTrue();
    assertThat(leaderMissing(DB_NAME)).isFalse();
  }

  // ------------------------------------------------------------------------------------------------------------
  // The targeted resync of a quarantine
  // ------------------------------------------------------------------------------------------------------------

  /**
   * A quarantine already on a closed database the leader does not hold - raised before this fix and restored from disk,
   * which is what the issue's restarted node carries - is lifted by the health tick's targeted resync instead of being
   * retried against the same 404 forever.
   */
  @Test
  void theTargetedResyncLiftsTheQuarantineOfAClosedDatabaseTheLeaderDoesNotHold() throws Exception {
    closeLocally(DB_NAME);
    sm.settleDivergedStateAfterInstall(Set.of(DB_NAME), 40L);
    assertThat(sm.isResyncInProgress()).as("the fixture starts quarantined").isTrue();

    sm.retryUnfilledSnapshotGap();
    sm.awaitLifecycleTasksForTesting(60_000);

    assertThat(sm.isDatabaseDiverged(DB_NAME)).isFalse();
    assertThat(sm.getDatabaseAppliedFloor(DB_NAME)).isEqualTo(-1L);
    assertThat(sm.isResyncInProgress()).as("the node is ready again").isFalse();
    assertThat(leaderMissing(DB_NAME)).isTrue();
    assertThat(liveCount(DB_NAME)).as("the local copy is kept").isEqualTo(LIVE_COUNT);
  }

  /**
   * Issue #8588: a quarantined REGISTERED database the leader does not hold was retried against the same 404 on every
   * health tick, holding the node out of the ready set for good. It is now lifted and reported LEADER_MISSING - what
   * the leader-driven auto-acquire install already did with the same database - and the copy is kept and served.
   */
  @Test
  void theTargetedResyncLiftsTheQuarantineOfARegisteredDatabaseTheLeaderDoesNotHold() throws Exception {
    sm.markStateDiverged(DB_NAME);
    assertThat(sm.isResyncInProgress()).as("the fixture starts quarantined").isTrue();

    sm.retryUnfilledSnapshotGap();
    sm.awaitLifecycleTasksForTesting(60_000);

    assertThat(sm.isDatabaseDiverged(DB_NAME)).isFalse();
    assertThat(sm.getDatabaseAppliedFloor(DB_NAME)).isEqualTo(-1L);
    assertThat(sm.isResyncInProgress()).as("the node is ready again").isFalse();
    assertThat(leaderMissing(DB_NAME)).isTrue();
    assertThat(server.existsDatabase(DB_NAME)).as("still registered").isTrue();
    assertThat(liveCount(DB_NAME)).as("the local copy is kept").isEqualTo(LIVE_COUNT);
  }

  /** Any other failure of a registered database's targeted resync still keeps its quarantine (issue #8588). */
  @Test
  void theTargetedResyncStillKeepsTheQuarantineOfARegisteredDatabaseTheLeaderFailedToServe() throws Exception {
    leaderAnswers(DB_NAME, 503);
    sm.markStateDiverged(DB_NAME);

    sm.retryUnfilledSnapshotGap();
    sm.awaitLifecycleTasksForTesting(60_000);

    assertThat(sm.isDatabaseDiverged(DB_NAME)).isTrue();
    assertThat(sm.isResyncInProgress()).isTrue();
    assertThat(leaderMissing(DB_NAME)).isFalse();
  }

  /**
   * Issue #8067 made the targeted resync install a quarantined database with no copy on this node. When the leader does
   * not hold it either, the quarantine is lifted and the database reported LEADER_MISSING, as for a copy that is
   * present, but nothing is marked: there is no closed copy here to keep from being reopened.
   */
  @Test
  void theTargetedResyncLiftsTheQuarantineOfAMissingDatabaseTheLeaderDoesNotHold() throws Exception {
    sm.markStateDiverged(OTHER_DB);
    assertThat(sm.isResyncInProgress()).as("the fixture starts quarantined").isTrue();

    sm.retryUnfilledSnapshotGap();
    sm.awaitLifecycleTasksForTesting(60_000);

    assertThat(sm.isDatabaseDiverged(OTHER_DB)).isFalse();
    assertThat(sm.isResyncInProgress()).as("the node is ready again").isFalse();
    assertThat(leaderMissing(OTHER_DB)).isTrue();
    assertThat(server.existsDatabase(OTHER_DB)).isFalse();
    // The failed install may leave its empty staging directory behind, which is not a copy (issue #8045)
    final Path dbDir = root.resolve("databases").resolve(OTHER_DB);
    if (Files.isDirectory(dbDir))
      try (final Stream<Path> files = Files.list(dbDir)) {
        assertThat(files.toList()).as("no copy, and no unverified-closed-copy mark, is left behind").isEmpty();
      }
  }

  // ------------------------------------------------------------------------------------------------------------
  // The Ratis-initiated install, on the legacy refresh-existing path
  // ------------------------------------------------------------------------------------------------------------

  /**
   * With auto-acquire off the install refreshed closed databases too (#8464), and a 404 for one failed the whole install,
   * which Ratis then re-triggered for good. It is now reported LEADER_MISSING and the install completes.
   */
  @Test
  void theLegacyRefreshReportsAClosedDatabaseTheLeaderDoesNotHoldAndCompletes() throws Exception {
    server.getConfiguration().setValue(GlobalConfiguration.HA_AUTO_ACQUIRE_DATABASES, false);
    createLocalDatabase(OTHER_DB);
    leaderServes(OTHER_DB);
    closeLocally(DB_NAME);
    final DatabaseReconciler reconciler = legacyReconciler();

    reconciler.reconcileDatabasesFromLeader("leader", leaderAddress, null, null, -1L);

    assertThat(liveCount(OTHER_DB)).isEqualTo(SNAPSHOT_COUNT);
    assertThat(reconciler.getAcquireStatus(DB_NAME)).isNotNull();
    assertThat(reconciler.getAcquireStatus(DB_NAME).state()).isEqualTo(DatabaseReconciler.AcquireState.LEADER_MISSING);
    assertThat(Files.isDirectory(root.resolve("databases").resolve(DB_NAME))).as("the local copy is kept").isTrue();
  }

  /**
   * Issue #8588: a REGISTERED database the leader does not hold failed the legacy install, which Ratis then re-triggered
   * for good. It is now reported LEADER_MISSING, as the auto-acquire reconcile reports it, and the install completes.
   */
  @Test
  void theLegacyRefreshReportsARegisteredDatabaseTheLeaderDoesNotHoldAndCompletes() throws Exception {
    server.getConfiguration().setValue(GlobalConfiguration.HA_AUTO_ACQUIRE_DATABASES, false);
    createLocalDatabase(OTHER_DB);
    leaderServes(OTHER_DB);
    final DatabaseReconciler reconciler = legacyReconciler();

    final DatabaseReconciler.ReconcileFromLeaderResult result =
        reconciler.reconcileDatabasesFromLeader("leader", leaderAddress, null, null, -1L);

    assertThat(liveCount(OTHER_DB)).isEqualTo(SNAPSHOT_COUNT);
    assertThat(reconciler.getAcquireStatus(DB_NAME)).isNotNull();
    assertThat(reconciler.getAcquireStatus(DB_NAME).state()).isEqualTo(DatabaseReconciler.AcquireState.LEADER_MISSING);
    assertThat(result.notInstalled()).as("not quarantined by the install").isEmpty();
    assertThat(result.leaderMissing()).as("but handed back as not refreshed").containsExactly(DB_NAME);
    assertThat(server.existsDatabase(DB_NAME)).as("still registered").isTrue();
    assertThat(liveCount(DB_NAME)).as("the local copy is kept").isEqualTo(LIVE_COUNT);
  }

  /**
   * The auto-acquire reconcile hands its LEADER_MISSING databases back too (issue #8588), so the leader-driven install
   * does not record a copy it never refreshed as being at the snapshot index.
   */
  @Test
  void theAutoAcquireReconcileHandsBackARegisteredDatabaseTheLeaderDoesNotHold() throws Exception {
    createLocalDatabase(OTHER_DB);
    leaderServes(OTHER_DB);
    final DatabaseReconciler reconciler = new DatabaseReconciler() {
      @Override
      LeaderDatabaseQuery.BootstrapState fetchBootstrapState(final String leaderPeerId, final String leaderHttpAddr, final String leaderHttpsAddr,
          final String clusterToken) {
        return new LeaderDatabaseQuery.BootstrapState(List.of(new LeaderDatabaseQuery.DatabaseInfo(OTHER_DB, 1L)),
            TermIndex.valueOf(3L, MARKER));
      }
    };
    reconciler.setServer(server);

    final DatabaseReconciler.ReconcileFromLeaderResult result =
        reconciler.reconcileDatabasesFromLeader("leader", leaderAddress, null, null, -1L);

    assertThat(result.notInstalled()).isEmpty();
    assertThat(result.leaderMissing()).containsExactly(DB_NAME);
    assertThat(reconciler.getAcquireStatus(DB_NAME).state()).isEqualTo(DatabaseReconciler.AcquireState.LEADER_MISSING);
  }

  /**
   * The leader-driven install records the snapshot index for every database it refreshed, but a LEADER_MISSING one keeps
   * its own position - and, unlike a database the install gave up on, is neither quarantined nor clamped (issue #8588).
   */
  @Test
  void theInstallDoesNotRecordALeaderMissingDatabaseAtTheSnapshotIndex() {
    createLocalDatabase(OTHER_DB);
    sm.writePersistedAppliedIndex(FLOOR, DB_NAME);
    sm.markStateDiverged(DB_NAME);

    sm.completeSnapshotInstall(MARKER, Set.of(), Set.of(DB_NAME));

    assertThat(sm.readPersistedAppliedIndex(OTHER_DB)).isEqualTo(MARKER);
    assertThat(sm.readPersistedAppliedIndex(DB_NAME)).as("not laundered into applied").isEqualTo(FLOOR);
    assertThat(sm.isDatabaseDiverged(DB_NAME)).as("not quarantined").isFalse();
    assertThat(sm.getDatabaseAppliedFloor(DB_NAME)).as("nor clamped").isEqualTo(-1L);
  }

  /** Any other failure of a registered database still fails the legacy install, so Ratis retries it (issue #8588). */
  @Test
  void theLegacyRefreshStillFailsForARegisteredDatabaseTheLeaderFailedToServe() {
    server.getConfiguration().setValue(GlobalConfiguration.HA_AUTO_ACQUIRE_DATABASES, false);
    leaderAnswers(DB_NAME, 503);
    final DatabaseReconciler reconciler = legacyReconciler();

    assertThatThrownBy(() -> reconciler.reconcileDatabasesFromLeader("leader", leaderAddress, null, null, -1L))
        .isInstanceOf(IOException.class)
        .isNotInstanceOf(LeaderDoesNotHoldDatabaseException.class);
  }

  // ------------------------------------------------------------------------------------------------------------

  private DatabaseReconciler legacyReconciler() {
    final DatabaseReconciler reconciler = new DatabaseReconciler() {
      @Override
      LeaderDatabaseQuery.BootstrapState fetchSnapshotMarker(final String leaderPeerId, final String leaderHttpAddr, final String leaderHttpsAddr,
          final String clusterToken) {
        return new LeaderDatabaseQuery.BootstrapState(List.of(), TermIndex.valueOf(3L, 40L));
      }
    };
    reconciler.setServer(server);
    return reconciler;
  }

  private boolean leaderMissing(final String name) {
    final DatabaseReconciler.AcquireStatus status = sm.getReconciler().getAcquireStatus(name);
    return status != null && status.state() == DatabaseReconciler.AcquireState.LEADER_MISSING;
  }

  /** What {@code close database} does (ServerControlPlane.closeDatabase): close, deregister, leave the files. */
  private void closeLocally(final String name) {
    final ServerDatabase database = server.getDatabase(name);
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

  /** Reopens the database the way the next request would, and counts what it serves. */
  private long liveCount(final String name) {
    return server.getDatabase(name).countType("Node", true);
  }

  /** Publishes a latest snapshot at {@code index}, the marker a full resync records every reinstalled database at. */
  private void registerSnapshotMarker(final long index) {
    ((SimpleStateMachineStorage) sm.getStateMachineStorage()).updateLatestSnapshot(
        new SingleFileSnapshotInfo(new FileInfo(root.resolve("snapshot-marker"), null), 3L, index));
  }

  private void setStaleSnapshotAppliedFloor(final long floor) throws Exception {
    final Field f = ArcadeStateMachine.class.getDeclaredField("staleSnapshotAppliedFloor");
    f.setAccessible(true);
    ((AtomicLong) f.get(sm)).set(floor);
  }

  /**
   * Makes the leader answer {@code statuses} with no body for {@code name}, one per request in order, the last one
   * repeated.
   */
  private void leaderAnswers(final String name, final int... statuses) {
    final AtomicInteger attempt = new AtomicInteger();
    leader.createContext("/api/v1/ha/snapshot/" + name, exchange -> {
      final int status = statuses[Math.min(attempt.getAndIncrement(), statuses.length - 1)];
      exchange.sendResponseHeaders(status, -1);
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
    leader.createContext("/api/v1/ha/snapshot/" + name, exchange -> {
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
    config.setValue(GlobalConfiguration.SERVER_NAME, "ArcadeDB_8559");
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, databasesDir.toString());
    config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, root.toString());
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, PASSWORD);
    config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_HOST, "localhost");
    config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_PORT,
        String.valueOf(StaticBaseServerTest.allocateFreePorts(1)[0]));
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
