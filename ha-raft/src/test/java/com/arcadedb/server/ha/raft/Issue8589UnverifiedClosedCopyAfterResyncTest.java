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
import com.arcadedb.exception.DatabaseNotAvailableException;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.HAServerPlugin;
import com.arcadedb.server.ServerDatabase;
import com.arcadedb.server.StaticBaseServerTest;
import com.sun.net.httpserver.HttpServer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.protocol.TermIndex;
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
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Regression tests for issue #8589: after issue #8559 a database CLOSED on a follower that the leader answers 404 for is
 * kept, neither reinstalled nor quarantined. The leader may have closed a database the cluster still has - an ordinary
 * maintenance close - and the follower's copy may be behind the committed log, yet the next request on the follower
 * that named it reopened it through {@code ArcadeDBServer.getDatabase} and served it unclamped.
 * <p>
 * Every path that reaches that verdict - the full resync, the targeted resync of a quarantine, the legacy refresh and
 * the auto-acquire reconcile of a Ratis-initiated install - now marks the copy unverified on disk, and a follower
 * refuses to reopen it. The node stays ready. An install that later succeeds replaces the copy and the mark with it.
 * <p>
 * Same fixture as {@link Issue8559ClosedDatabaseTheLeaderDoesNotHoldTest}, plus an {@link HAServerPlugin} registered on
 * the server that answers "not the leader", which is what makes it a follower to {@code getDatabase}.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
@Timeout(120)
class Issue8589UnverifiedClosedCopyAfterResyncTest {

  private static final String     DB_NAME        = "db8589";
  private static final String     OTHER_DB       = "db8589other";
  private static final String     PASSWORD       = "DefaultPasswordForTests";
  private static final int        LIVE_COUNT     = 12;
  private static final int        SNAPSHOT_COUNT = 27;
  private static final long       FLOOR          = 5L;
  private static final RaftPeerId LOCAL          = RaftPeerId.valueOf("local");
  private static final RaftPeerId LEADER         = RaftPeerId.valueOf("leader");

  @TempDir
  Path root;

  private ArcadeDBServer     server;
  private ArcadeStateMachine sm;
  private HttpServer         leader;
  private String             leaderAddress;
  private HAServerPlugin     role;

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

    role = mock(HAServerPlugin.class);
    when(role.isLeader()).thenReturn(false);
    server.setHA(role);
  }

  @AfterEach
  void tearDown() throws IOException {
    if (sm != null)
      sm.close();
    if (leader != null)
      leader.stop(0);
    if (server != null) {
      server.setHA(null);
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

  /** The issue as reported, on the full resync: the copy is kept, the node is ready, and the follower will not serve it. */
  @Test
  void aFullResyncMarksAClosedCopyTheLeaderDoesNotHoldAndTheFollowerRefusesToReopenIt() throws Exception {
    createLocalDatabase(OTHER_DB);
    leaderServes(OTHER_DB); // no context for DB_NAME: the leader answers 404 for it
    closeLocally(DB_NAME);
    setStaleSnapshotAppliedFloor(FLOOR);

    sm.triggerSnapshotDownload();

    assertThat(sm.isResyncInProgress()).as("the node is ready: this is not a quarantine").isFalse();
    assertThat(sm.isDatabaseDiverged(DB_NAME)).isFalse();
    assertThat(leaderMissing(DB_NAME)).isTrue();
    assertThat(Files.exists(marker(DB_NAME))).as("the copy is marked unverified").isTrue();
    assertRefusedOnThisFollower(DB_NAME);
    assertThat(liveCount(OTHER_DB)).as("every other database is served").isEqualTo(SNAPSHOT_COUNT);
  }

  /**
   * A replicated entry for a marked copy resolves its database through the same reopen point, and is refused there
   * rather than applied onto a copy that may be behind: the refusal is what routes the entry to the per-database
   * quarantine and targeted resync of {@code handleUnexpectedApplyError}.
   */
  @Test
  void theApplyPathResolvesAMarkedCopyThroughTheSameRefusal() throws Exception {
    closeLocally(DB_NAME);
    setStaleSnapshotAppliedFloor(FLOOR);
    sm.triggerSnapshotDownload();
    assertThat(Files.exists(marker(DB_NAME))).isTrue();

    assertThatThrownBy(() -> sm.databaseFor(DB_NAME)).isInstanceOf(DatabaseNotAvailableException.class);
    assertThat(server.existsDatabase(DB_NAME)).isFalse();
  }

  /** The targeted resync that lifts a quarantine on the same verdict marks the copy too. */
  @Test
  void theTargetedResyncMarksTheClosedCopyWhoseQuarantineItLifts() throws Exception {
    closeLocally(DB_NAME);
    sm.settleDivergedStateAfterInstall(Set.of(DB_NAME), 40L);

    sm.retryUnfilledSnapshotGap();
    sm.awaitLifecycleTasksForTesting(60_000);

    assertThat(sm.isResyncInProgress()).isFalse();
    assertThat(Files.exists(marker(DB_NAME))).isTrue();
    assertRefusedOnThisFollower(DB_NAME);
  }

  /** The legacy refresh of a Ratis-initiated install marks it. */
  @Test
  void theLegacyRefreshMarksAClosedCopyTheLeaderDoesNotHold() throws Exception {
    server.getConfiguration().setValue(GlobalConfiguration.HA_AUTO_ACQUIRE_DATABASES, false);
    closeLocally(DB_NAME);

    legacyReconciler().reconcileDatabasesFromLeader(leaderAddress, null, null, -1L);

    assertThat(Files.exists(marker(DB_NAME))).isTrue();
    assertRefusedOnThisFollower(DB_NAME);
  }

  /**
   * The auto-acquire reconcile reports the same input LEADER_MISSING from the leader's database list, without a 404:
   * a closed copy is marked, a registered one - which this node serves - is not.
   */
  @Test
  void theAutoAcquireReconcileMarksAClosedCopyTheLeaderDoesNotListButNotARegisteredOne() throws Exception {
    createLocalDatabase(OTHER_DB);
    closeLocally(DB_NAME);
    final DatabaseReconciler reconciler = new DatabaseReconciler() {
      @Override
      LeaderDatabaseQuery.BootstrapState fetchBootstrapState(final String leaderHttpAddr, final String leaderHttpsAddr,
          final String clusterToken) {
        return new LeaderDatabaseQuery.BootstrapState(List.of(), TermIndex.valueOf(3L, 40L));
      }
    };
    reconciler.setServer(server);

    reconciler.reconcileDatabasesFromLeader(leaderAddress, null, null, -1L);

    assertThat(reconciler.getAcquireStatus(DB_NAME).state()).isEqualTo(DatabaseReconciler.AcquireState.LEADER_MISSING);
    assertThat(Files.exists(marker(DB_NAME))).isTrue();
    assertRefusedOnThisFollower(DB_NAME);
    assertThat(Files.exists(marker(OTHER_DB))).as("a registered database is not marked").isFalse();
    assertThat(liveCount(OTHER_DB)).isEqualTo(LIVE_COUNT);
  }

  /**
   * The issue's "also covers" case: a leader whose 404 was transient - its copy still loading - holds the database by
   * the next resync, which reinstalls the copy and takes the mark away with it, so the follower serves it again.
   */
  @Test
  void aLaterResyncThatInstallsTheCopyClearsTheMark() throws Exception {
    closeLocally(DB_NAME);
    setStaleSnapshotAppliedFloor(FLOOR);
    sm.triggerSnapshotDownload();
    assertThat(Files.exists(marker(DB_NAME))).as("the fixture starts marked").isTrue();

    leaderServes(DB_NAME);
    setStaleSnapshotAppliedFloor(FLOOR);
    sm.triggerSnapshotDownload();

    assertThat(Files.exists(marker(DB_NAME))).as("the install replaced the copy and its mark").isFalse();
    assertThat(leaderMissing(DB_NAME)).isFalse();
    assertThat(liveCount(DB_NAME)).as("the follower serves the leader's copy").isEqualTo(SNAPSHOT_COUNT);
  }

  /** Any other failure still quarantines, as before, and marks nothing: the quarantine is what protects the copy. */
  @Test
  void anyOtherFailureStillQuarantinesAndMarksNothing() throws Exception {
    createLocalDatabase(OTHER_DB);
    leaderServes(OTHER_DB);
    leaderAnswers(DB_NAME, 503);
    closeLocally(DB_NAME);
    setStaleSnapshotAppliedFloor(FLOOR);

    sm.triggerSnapshotDownload();

    assertThat(sm.isDatabaseDiverged(DB_NAME)).isTrue();
    assertThat(Files.exists(marker(DB_NAME))).isFalse();
  }

  /**
   * Without the mark the copy would stay reopenable and unverified, so a mark that cannot be written falls back to the
   * quarantine, which fails closed.
   */
  @Test
  void aMarkThatCannotBeWrittenFallsBackToTheQuarantine() throws Exception {
    final Path dir = root.resolve("databases").resolve(DB_NAME);
    assumeTrue(Files.getFileStore(dir).supportsFileAttributeView("posix"), "needs POSIX permissions");
    closeLocally(DB_NAME);
    final Set<PosixFilePermission> original = Files.getPosixFilePermissions(dir);
    Files.setPosixFilePermissions(dir, PosixFilePermissions.fromString("r-xr-xr-x"));
    try {
      assumeTrue(!Files.isWritable(dir), "the process can write a read-only directory (running as root?)");
      setStaleSnapshotAppliedFloor(FLOOR);

      sm.triggerSnapshotDownload();

      assertThat(Files.exists(marker(DB_NAME))).isFalse();
      assertThat(sm.isDatabaseDiverged(DB_NAME)).as("quarantined instead").isTrue();
      assertThat(sm.isResyncInProgress()).isTrue();
    } finally {
      Files.setPosixFilePermissions(dir, original);
    }
  }

  /** The same fallback on the targeted resync: a quarantine it cannot replace with a mark is left standing. */
  @Test
  void aMarkThatCannotBeWrittenLeavesTheTargetedResyncsQuarantineStanding() throws Exception {
    final Path dir = root.resolve("databases").resolve(DB_NAME);
    assumeTrue(Files.getFileStore(dir).supportsFileAttributeView("posix"), "needs POSIX permissions");
    closeLocally(DB_NAME);
    sm.settleDivergedStateAfterInstall(Set.of(DB_NAME), 40L);
    final long floor = sm.getDatabaseAppliedFloor(DB_NAME);
    final Set<PosixFilePermission> original = Files.getPosixFilePermissions(dir);
    Files.setPosixFilePermissions(dir, PosixFilePermissions.fromString("r-xr-xr-x"));
    try {
      assumeTrue(!Files.isWritable(dir), "the process can write a read-only directory (running as root?)");

      sm.retryUnfilledSnapshotGap();
      sm.awaitLifecycleTasksForTesting(60_000);

      assertThat(Files.exists(marker(DB_NAME))).isFalse();
      assertThat(sm.isDatabaseDiverged(DB_NAME)).as("still quarantined").isTrue();
      assertThat(sm.getDatabaseAppliedFloor(DB_NAME)).as("with its floor").isEqualTo(floor);
      assertThat(sm.isResyncInProgress()).isTrue();
      assertThat(leaderMissing(DB_NAME)).as("and not reported as kept").isFalse();
    } finally {
      Files.setPosixFilePermissions(dir, original);
    }
  }

  // ------------------------------------------------------------------------------------------------------------

  private void assertRefusedOnThisFollower(final String name) {
    assertThatThrownBy(() -> server.getDatabase(name))
        .isInstanceOf(DatabaseNotAvailableException.class)
        .hasMessageContaining("could not verify");
    assertThat(server.existsDatabase(name)).isFalse();
  }

  private Path marker(final String name) {
    return root.resolve("databases").resolve(name).resolve(ArcadeDBServer.UNVERIFIED_CLOSED_COPY_FILE);
  }

  private DatabaseReconciler legacyReconciler() {
    final DatabaseReconciler reconciler = new DatabaseReconciler() {
      @Override
      LeaderDatabaseQuery.BootstrapState fetchSnapshotMarker(final String leaderHttpAddr, final String leaderHttpsAddr,
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
    config.setValue(GlobalConfiguration.SERVER_NAME, "ArcadeDB_8589");
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
