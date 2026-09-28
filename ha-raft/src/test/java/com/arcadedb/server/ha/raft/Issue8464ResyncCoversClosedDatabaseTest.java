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
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Regression tests for issue #8464: a snapshot resync reinstalled only the databases REGISTERED on the node, so a
 * database closed there with {@code close database} - deregistered, its directory left on disk, and reopened by the
 * next request that names it - kept its pre-resync files while the resync recorded the whole gap as applied.
 * <p>
 * The fixture is the one {@link Issue8367BootstrapReplacementRearmTest} uses: a real {@link ArcadeDBServer} (HA off)
 * holding the databases, a real {@link ArcadeStateMachine}, a mocked follower-side {@link RaftHAServer}, and a local
 * HTTP server standing in for the leader's snapshot endpoint. The leader's copy carries a different record count, so
 * an install that landed is observable on the reopened database.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
@Timeout(120)
class Issue8464ResyncCoversClosedDatabaseTest {

  private static final String     DB_NAME        = "db8464";
  private static final String     OTHER_DB       = "db8464other";
  private static final String     PASSWORD       = "DefaultPasswordForTests";
  private static final int        LIVE_COUNT     = 12;
  private static final int        SNAPSHOT_COUNT = 27;
  private static final long       FLOOR          = 5L;
  private static final RaftPeerId LOCAL          = RaftPeerId.valueOf("local");
  private static final RaftPeerId LEADER         = RaftPeerId.valueOf("leader");

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
  // The full resync (leader change, watchdog, stale-snapshot retry)
  // ------------------------------------------------------------------------------------------------------------

  /** The issue as reported: the full resync walked the registry, skipped the closed database and cleared the floor. */
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
  @Test
  void theLegacyRefreshInstallsADatabaseClosedOnThisNode() throws Exception {
    server.getConfiguration().setValue(GlobalConfiguration.HA_AUTO_ACQUIRE_DATABASES, false);
    leaderServes(DB_NAME);
    closeLocally(DB_NAME);

    final DatabaseReconciler reconciler = new DatabaseReconciler() {
      @Override
      LeaderDatabaseQuery.BootstrapState fetchSnapshotMarker(final String leaderHttpAddr, final String leaderHttpsAddr,
          final String clusterToken) {
        return new LeaderDatabaseQuery.BootstrapState(List.of(), TermIndex.valueOf(3L, 40L));
      }
    };
    reconciler.setServer(server);

    reconciler.reconcileDatabasesFromLeader(leaderAddress, null, null, -1L);

    assertThat(downloadsOf(DB_NAME)).isEqualTo(1);
    assertThat(liveCount(DB_NAME)).isEqualTo(SNAPSHOT_COUNT);
  }

  /**
   * On the auto-acquire path a closed database used to be invisible: neither refreshed nor reported. The leader not
   * holding it is now surfaced as {@code LEADER_MISSING}, and nothing is downloaded or dropped for it.
   */
  @Test
  void theAutoAcquireReconcileReportsAClosedDatabaseTheLeaderDoesNotHold() throws Exception {
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

    assertThat(reconciler.getAcquireStatus(DB_NAME)).isNotNull();
    assertThat(reconciler.getAcquireStatus(DB_NAME).state()).isEqualTo(DatabaseReconciler.AcquireState.LEADER_MISSING);
    assertThat(downloadsOf(DB_NAME)).isZero();
    assertThat(Files.isDirectory(root.resolve("databases").resolve(DB_NAME))).as("the local copy is kept").isTrue();
  }

  // ------------------------------------------------------------------------------------------------------------
  // What counts as a closed database
  // ------------------------------------------------------------------------------------------------------------

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
    config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_PORT, String.valueOf(freePort()));
    config.setValue(GlobalConfiguration.HA_ENABLED, false);
    // Fail a download on its first attempt: the exponential backoff would only make the test slow.
    config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRIES, 0);
    config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRY_BASE_MS, 0L);
    config.setValue(GlobalConfiguration.NETWORK_USE_SSL, false);

    final ArcadeDBServer started = new ArcadeDBServer(config);
    started.start();
    return started;
  }

  private static int freePort() throws IOException {
    try (final ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }
}
