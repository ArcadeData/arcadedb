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
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.HAServerPlugin;
import com.arcadedb.server.ServerDatabase;
import com.arcadedb.server.StaticBaseServerTest;
import com.arcadedb.server.TestServerHelper;
import com.arcadedb.server.UnstartedHttpServers;
import com.arcadedb.server.http.handler.ExecutionResponse;
import com.arcadedb.server.security.ServerSecurityUser;
import com.sun.net.httpserver.HttpServer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.util.LifeCycle;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
 * Unverified closed copies created by a resync: handling after the resync and re-verification.
 */
class UnverifiedClosedCopyResyncTest {

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
   * Same fixture as {@link ClosedDatabaseSnapshotResyncTest}, plus an {@link HAServerPlugin} registered on
   * the server that answers "not the leader", which is what makes it a follower to {@code getDatabase}.
   */
  @Nested
  @Timeout(120)
  class UnverifiedClosedCopyAfterResync {

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
              ((DatabaseInternal) server.getDatabaseForOperator(name)).getEmbedded().drop();
          } catch (final Exception ignore) {
            // best-effort cleanup; the @TempDir is removed regardless
          }
        server.stop();
      }
    }

    /** The issue as reported, on the full resync: the copy is kept, the node is ready, and the follower will not serve it. */
    // Regression for #8589: a closed database the leader answers 404 for is kept as unverified
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

    // Regression for #8589: a closed database the leader answers 404 for is kept as unverified
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
    // Regression for #8589: a closed database the leader answers 404 for is kept as unverified
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
    // Regression for #8589: a closed database the leader answers 404 for is kept as unverified
    @Test
    void theLegacyRefreshMarksAClosedCopyTheLeaderDoesNotHold() throws Exception {
      server.getConfiguration().setValue(GlobalConfiguration.HA_AUTO_ACQUIRE_DATABASES, false);
      closeLocally(DB_NAME);

      legacyReconciler().reconcileDatabasesFromLeader("leader", leaderAddress, null, null, -1L);

      assertThat(Files.exists(marker(DB_NAME))).isTrue();
      assertRefusedOnThisFollower(DB_NAME);
    }

    // Regression for #8589: a closed database the leader answers 404 for is kept as unverified
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
        LeaderDatabaseQuery.BootstrapState fetchBootstrapState(final String leaderPeerId, final String leaderHttpAddr, final String leaderHttpsAddr,
            final String clusterToken) {
          return new LeaderDatabaseQuery.BootstrapState(List.of(), TermIndex.valueOf(3L, 40L));
        }
      };
      reconciler.setServer(server);

      reconciler.reconcileDatabasesFromLeader("leader", leaderAddress, null, null, -1L);

      assertThat(reconciler.getAcquireStatus(DB_NAME).state()).isEqualTo(DatabaseReconciler.AcquireState.LEADER_MISSING);
      assertThat(Files.exists(marker(DB_NAME))).isTrue();
      assertRefusedOnThisFollower(DB_NAME);
      assertThat(Files.exists(marker(OTHER_DB))).as("a registered database is not marked").isFalse();
      assertThat(liveCount(OTHER_DB)).isEqualTo(LIVE_COUNT);
    }

    // Regression for #8589: a closed database the leader answers 404 for is kept as unverified
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
    // Regression for #8589: a closed database the leader answers 404 for is kept as unverified
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

    // Regression for #8589: a closed database the leader answers 404 for is kept as unverified
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
    // Regression for #8589: a closed database the leader answers 404 for is kept as unverified
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
      assertThatThrownBy(() -> server.getDatabaseForOperator(name))
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

    /** Reopens the database the way the next request would, and counts what it serves. */
    private long liveCount(final String name) {
      return server.getDatabaseForOperator(name).countType("Node", true);
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

  /**
   * Regression tests for issue #8606: a closed copy a resync marked unverified, because the leader answered 404 for it
   * (issue #8589), was re-verified only when something happened to try an install again - a later full resync, a
   * Ratis-initiated install, or a replicated entry for that database. Nothing retried on its own, so once the leader held
   * the database again, a database that is only read stayed refused on the follower indefinitely.
   * <p>
   * The {@link HealthMonitor} tick now re-verifies every marked copy against the leader, with backoff: it asks the leader
   * whether its snapshot endpoint would serve the database, and reinstalls the copy when it does. A refused request and a new leader
   * restart the backoff.
   * <p>
   * Same fixture as {@link UnverifiedClosedCopyResyncTest.UnverifiedClosedCopyAfterResync}, with the fake leader also answering the
   * {@code copyOf} form of the bootstrap-state RPC.
   */
  @Nested
  @Timeout(120)
  class UnverifiedClosedCopyReverify {

    @RegisterExtension
    static final UnstartedHttpServers HTTP_SERVERS = new UnstartedHttpServers();

    private static final String     DB_NAME        = "db8606";
    private static final String     PASSWORD       = "DefaultPasswordForTests";
    private static final int        LIVE_COUNT     = 12;
    private static final int        SNAPSHOT_COUNT = 27;
    private static final long       FLOOR          = 5L;
    private static final RaftPeerId LOCAL          = RaftPeerId.valueOf("local");
    private static final RaftPeerId LEADER         = RaftPeerId.valueOf("leader");
    private static final RaftPeerId NEW_LEADER     = RaftPeerId.valueOf("new-leader");

    @TempDir
    Path root;

    private       ArcadeDBServer     server;
    private       ArcadeStateMachine sm;
    private       FakeRaftHAServer       raft;
    private       HttpServer         leader;
    private       String             leaderAddress;
    // What the fake leader answers about each database: absent = it would not serve it.
    private final Set<String>        leaderHolds       = ConcurrentHashMap.newKeySet();
    private final AtomicInteger      probes            = new AtomicInteger();
    private final AtomicInteger      snapshotRequests  = new AtomicInteger();
    private       boolean            answerOlderFormat = false;
    // Runs inside the fake leader's probe handler, while the round waits for the answer: null = nothing.
    private volatile Runnable          duringProbe;

    @BeforeEach
    void setUp() throws IOException {
      server = startServer();
      final ServerDatabase live = server.getOrCreateDatabase(DB_NAME);
      live.transaction(() -> {
        live.getSchema().createVertexType("Node");
        for (int i = 0; i < LIVE_COUNT; i++)
          live.newVertex("Node").set("v", i).save();
      });

      leader = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
      leader.createContext(BootstrapElection.BOOTSTRAP_STATE_ROUTE, exchange -> {
        probes.incrementAndGet();
        final Runnable hook = duringProbe;
        if (hook != null)
          hook.run();
        final String name = new JSONObject(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8))
            .getString(UnverifiedClosedCopyCheck.COPY_OF, null);
        // Shaped as PostBootstrapStateHandler answers, signed by whichever peer the mock currently names leader.
        final JSONObject answer = new JSONObject().put("peerId", raft.getLeaderId().toString())
            .put(UnverifiedClosedCopyCheck.COPY, new UnverifiedClosedCopyCheck.CopyState(true, 50L).toJSON(name));
        if (!answerOlderFormat)
          answer.put(UnverifiedClosedCopyCheck.SERVES, leaderHolds.contains(name));
        final byte[] body = answer.toString().getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(200, body.length);
        exchange.getResponseBody().write(body);
        exchange.close();
      });
      leader.start();
      leaderAddress = "localhost:" + leader.getAddress().getPort();

      raft = FakeRaftHAServer.detached();
      raft.leader(false);
      raft.localPeerId(LOCAL);
      raft.localHttpAddress("local-host:2480");
      raft.clusterToken(null);
      raft.leaderId(LEADER);
      raft.peerHttpAddress(LEADER, leaderAddress);
      raft.peerHttpAddress(NEW_LEADER, leaderAddress);

      sm = new ArcadeStateMachine();
      sm.setServer(server);
      sm.setRaftHAServer(raft);

      final HAServerPlugin role = mock(HAServerPlugin.class);
      when(role.isLeader()).thenReturn(false);
      server.setHA(role);

      markUnverifiedThroughAResync();
    }

    @AfterEach
    void tearDown() throws IOException {
      if (sm != null)
        sm.close();
      if (leader != null)
        leader.stop(0);
      if (server != null) {
        server.setHA(null);
        try {
          if (server.existsDatabase(DB_NAME))
            ((DatabaseInternal) server.getDatabaseForOperator(DB_NAME)).getEmbedded().drop();
        } catch (final Exception ignore) {
          // best-effort cleanup; the @TempDir is removed regardless
        }
        server.stop();
      }
    }

    /** The issue as reported: the leader holds the database again, and the health tick reinstalls the copy on its own. */
    // Regression for #8606: an unverified closed copy is re-verified without waiting for another install
    @Test
    void theHealthTickReinstallsAMarkedCopyOnceTheLeaderHoldsTheDatabaseAgain() throws Exception {
      leaderServesSnapshot();
      leaderHolds.add(DB_NAME);

      reverifyRound();

      assertThat(Files.exists(marker())).as("the install replaced the copy and its mark").isFalse();
      assertThat(leaderMissing()).isFalse();
      assertThat(server.getDatabaseForOperator(DB_NAME).countType("Node", true)).as("the follower serves the leader's copy")
          .isEqualTo(SNAPSHOT_COUNT);
      assertThat(failures()).isZero();
    }

    // Regression for #8606: an unverified closed copy is re-verified without waiting for another install
    /**
     * While the leader still does not hold the database the copy stays refused, and the leader is only asked - nothing is
     * downloaded, so a 404 is not retried with the download's own backoff on every round.
     */
    @Test
    void whileTheLeaderStillDoesNotHoldItTheCopyStaysRefusedAndNothingIsDownloaded() throws Exception {
      leaderServesSnapshot(); // reachable, but the leader says it would not serve it

      reverifyRound();

      assertThat(probes.get()).isEqualTo(1);
      assertThat(snapshotRequests.get()).as("no download was attempted").isZero();
      assertThat(Files.exists(marker())).isTrue();
      assertRefusedOnThisFollower();
      assertThat(failures()).isEqualTo(1);
    }

    /** A failed round backs off: the next tick does not ask again before the interval has passed. */
    // Regression for #8606: an unverified closed copy is re-verified without waiting for another install
    @Test
    void aRoundThatLeavesTheCopyMarkedBacksOff() throws Exception {
      reverifyRound();
      assertThat(probes.get()).isEqualTo(1);
      assertThat(failures()).isEqualTo(1);

      // Pinned rather than left to the clock: with the round just claimed and the ladder at its top, the window is
      // UNVERIFIED_COPY_REVERIFY_MAX_INTERVAL_MS long, so no JVM stall between the two calls can open it (#6260).
      setFailures(16);
      lastReverify().set(System.currentTimeMillis());
      sm.reverifyUnverifiedClosedCopies();
      sm.awaitLifecycleTasksForTesting(60_000);

      assertThat(probes.get()).as("the next tick is inside the backoff window").isEqualTo(1);
      assertThat(UnverifiedCopyReverifier.unverifiedClosedCopyReverifyIntervalMs(0))
          .isEqualTo(ArcadeStateMachine.UNVERIFIED_COPY_REVERIFY_INTERVAL_MS);
      assertThat(UnverifiedCopyReverifier.unverifiedClosedCopyReverifyIntervalMs(1))
          .isEqualTo(2 * ArcadeStateMachine.UNVERIFIED_COPY_REVERIFY_INTERVAL_MS);
      assertThat(UnverifiedCopyReverifier.unverifiedClosedCopyReverifyIntervalMs(Integer.MAX_VALUE))
          .isEqualTo(ArcadeStateMachine.UNVERIFIED_COPY_REVERIFY_MAX_INTERVAL_MS);
    }

    // Regression for #8606: an unverified closed copy is re-verified without waiting for another install
    /**
     * The leader closes the database between its answer and the download: the install gets the 404, the copy keeps its
     * mark and stays refused, and the round counts as one that left it in place.
     */
    @Test
    void aLeaderThatClosesTheDatabaseAfterAnsweringLeavesTheCopyMarked() throws Exception {
      leaderHolds.add(DB_NAME); // says it serves it, but has no snapshot route for it: the download is answered 404

      reverifyRound();

      assertThat(probes.get()).isEqualTo(1);
      assertThat(Files.exists(marker())).isTrue();
      assertRefusedOnThisFollower();
      assertThat(failures()).isEqualTo(1);
    }

    // Regression for #8606: an unverified closed copy is re-verified without waiting for another install
    /**
     * A tick while a round is still waiting on the leader does not queue another round behind it: the backoff is updated
     * only when a round ends, so queued rounds would run back to back once the slow one finished.
     */
    @Test
    void aTickWhileARoundIsStillRunningDoesNotQueueAnother() throws Exception {
      final CountDownLatch probing = new CountDownLatch(1);
      final CountDownLatch release = new CountDownLatch(1);
      duringProbe = () -> {
        probing.countDown();
        try {
          release.await(30, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      };
      lastReverify().set(0L);
      sm.reverifyUnverifiedClosedCopies();
      assertThat(probing.await(30, TimeUnit.SECONDS)).as("the first round is waiting on the leader").isTrue();

      lastReverify().set(0L); // the throttle alone would let this tick through
      sm.reverifyUnverifiedClosedCopies();
      release.countDown();
      sm.awaitLifecycleTasksForTesting(60_000);

      assertThat(probes.get()).as("one round, not two").isEqualTo(1);
      duringProbe = null;
      reverifyRound();
      assertThat(probes.get()).as("the guard is released once the round ends").isEqualTo(2);
    }

    // Regression for #8606: an unverified closed copy is re-verified without waiting for another install
    /**
     * A round that finds another download holding the single flight learned nothing about the leader: the copy stays
     * marked, but the backoff does not climb.
     */
    @Test
    void aRoundThatFindsAnotherDownloadRunningDoesNotClimbTheBackoff() throws Exception {
      reverifyRound(); // the first tick records the leader, which restarts the ladder: done before the count is pinned
      leaderServesSnapshot();
      leaderHolds.add(DB_NAME);
      final Field f = ArcadeStateMachine.class.getDeclaredField("snapshotDownloadInProgress");
      f.setAccessible(true);
      final AtomicBoolean downloading = (AtomicBoolean) f.get(sm);
      duringProbe = () -> downloading.set(true); // a download starts between the tick and the install
      setFailures(2);
      try {
        reverifyRound();
      } finally {
        downloading.set(false);
        duringProbe = null;
      }

      assertThat(snapshotRequests.get()).isZero();
      assertThat(Files.exists(marker())).isTrue();
      assertThat(failures()).isEqualTo(2);
    }

    /** A round that throws is counted as a failed one and releases the in-flight guard, rather than vanishing. */
    // Regression for #8606: an unverified closed copy is re-verified without waiting for another install
    @Test
    void aRoundThatThrowsIsCountedAndReleasesTheGuard() throws Exception {
      // The tick reads the leader once, the round once more: the round's read throws.
      final AtomicInteger leaderReads = new AtomicInteger();
      raft.on("getLeaderId", args -> {
        if (leaderReads.incrementAndGet() == 2)
          throw new IllegalStateException("boom");
        return LEADER;
      });

      reverifyRound();

      assertThat(failures()).isEqualTo(1);
      assertThat(probes.get()).isZero();
      reverifyRound();
      assertThat(probes.get()).as("the guard was released, so the next round runs").isEqualTo(1);
    }

    /** A new leader may hold what the previous one did not: it is asked at the next tick, whatever the backoff. */
    // Regression for #8606: an unverified closed copy is re-verified without waiting for another install
    @Test
    void aNewLeaderIsAskedAtTheNextTick() throws Exception {
      reverifyRound();
      assertThat(probes.get()).isEqualTo(1);

      leaderServesSnapshot();
      leaderHolds.add(DB_NAME);
      raft.leaderId(NEW_LEADER);
      sm.reverifyUnverifiedClosedCopies();
      sm.awaitLifecycleTasksForTesting(60_000);

      assertThat(probes.get()).isEqualTo(2);
      assertThat(Files.exists(marker())).isFalse();
    }

    // Regression for #8606: an unverified closed copy is re-verified without waiting for another install
    /**
     * A request this follower refused restarts the ladder, through the HA plugin the server tells: the wait before the
     * next round drops back to the shortest one.
     */
    @Test
    void aRefusedRequestRestartsTheBackoffThroughThePlugin() throws Exception {
      reverifyRound();
      reverifyRound();
      assertThat(failures()).isEqualTo(2);

      final RaftHAPlugin plugin = new RaftHAPlugin();
      final Field field = RaftHAPlugin.class.getDeclaredField("raftHAServer");
      field.setAccessible(true);
      field.set(plugin, raft);
      raft.stateMachine(sm);
      server.setHA(plugin);
      try {
        assertRefusedOnThisFollower();
      } finally {
        final HAServerPlugin role = mock(HAServerPlugin.class);
        when(role.isLeader()).thenReturn(false);
        server.setHA(role);
      }

      assertThat(failures()).isZero();
    }

    /** The leader reopens a marked copy on demand after asking its peers (issue #8605): its tick does nothing. */
    // Regression for #8606: an unverified closed copy is re-verified without waiting for another install
    @Test
    void theLeaderDoesNotReverify() throws Exception {
      raft.leader(true);

      reverifyRound();

      assertThat(probes.get()).isZero();
      assertThat(Files.exists(marker())).isTrue();
    }

    /** A leader that predates the field cannot say it serves the database, so no install is attempted on its answer. */
    // Regression for #8606: an unverified closed copy is re-verified without waiting for another install
    @Test
    void anAnswerWithoutTheServesFieldDoesNotInstall() throws Exception {
      leaderServesSnapshot();
      leaderHolds.add(DB_NAME);
      answerOlderFormat = true;

      reverifyRound();

      assertThat(probes.get()).isEqualTo(1);
      assertThat(snapshotRequests.get()).isZero();
      assertThat(Files.exists(marker())).isTrue();
    }

    /** The leader's side of the probe: what its snapshot route serves - registered there, and not quarantined (#8468). */
    // Regression for #8606: an unverified closed copy is re-verified without waiting for another install
    @Test
    void theLeadersHandlerReportsWhetherItServesTheDatabase() throws Exception {
      raft.stateMachine(sm);
      final ServerDatabase other = server.getOrCreateDatabase("db8606other");
      try {
        assertThat(handlerAnswer("db8606other").getBoolean(UnverifiedClosedCopyCheck.SERVES, false)).isTrue();
        final JSONObject closedHere = handlerAnswer(DB_NAME);
        assertThat(closedHere.getBoolean(UnverifiedClosedCopyCheck.SERVES, true)).as("closed here").isFalse();
        assertThat(closedHere.getJSONObject(UnverifiedClosedCopyCheck.COPY).getBoolean("present", false)).isTrue();

        sm.settleDivergedStateAfterInstall(Set.of("db8606other"), 40L);
        assertThat(handlerAnswer("db8606other").getBoolean(UnverifiedClosedCopyCheck.SERVES, true))
            .as("registered but quarantined: the snapshot route refuses it").isFalse();
      } finally {
        other.getEmbedded().drop();
        server.removeDatabase("db8606other");
      }
    }

    /** The follower's side: the answer counts only when the leader wrote it, and a missing member is a "no". */
    // Regression for #8606: an unverified closed copy is re-verified without waiting for another install
    @Test
    void theAnswerIsReadOnlyFromTheLeader() throws Exception {
      final String signed = new JSONObject().put("peerId", "leader").put(UnverifiedClosedCopyCheck.SERVES, true).toString();
      assertThat(UnverifiedClosedCopyCheck.parseServes(200, signed, "leader", "http://x")).isTrue();
      assertThat(UnverifiedClosedCopyCheck.parseServes(200, new JSONObject().put("peerId", "leader").toString(),
          "leader", "http://x")).as("an older server's answer").isFalse();
      assertThatThrownBy(() -> UnverifiedClosedCopyCheck.parseServes(200, signed, "other", "http://x"))
          .isInstanceOf(LeaderDatabaseQuery.WrongPeerAnsweredException.class);
      assertThatThrownBy(() -> UnverifiedClosedCopyCheck.parseServes(403, "{}", "leader", "http://x"))
          .isInstanceOf(IOException.class);
    }

    /** The tick drives the hook before its lifecycle branch, so a CLOSED Ratis division does not stop it. */
    // Regression for #8606: an unverified closed copy is re-verified without waiting for another install
    @Test
    void everyHealthTickDrivesTheReverification() {
      final AtomicInteger calls = new AtomicInteger();
      final HealthMonitor.HealthTarget target = new HealthMonitor.HealthTarget() {
        @Override
        public LifeCycle.State getRaftLifeCycleState() {
          return LifeCycle.State.CLOSED;
        }

        @Override
        public boolean isShutdownRequested() {
          return false;
        }

        @Override
        public void restartRatisIfNeeded() {
        }

        @Override
        public void reverifyUnverifiedClosedCopies() {
          calls.incrementAndGet();
        }
      };
      final HealthMonitor monitor = new HealthMonitor(target, 0);

      monitor.tick();
      monitor.tick();

      assertThat(calls.get()).isEqualTo(2);
    }

    // ------------------------------------------------------------------------------------------------------------

    /** The #8589 path that writes the mark: a full resync that finds the leader not holding a closed copy. */
    private void markUnverifiedThroughAResync() {
      final ServerDatabase database = server.getDatabaseForOperator(DB_NAME);
      database.getEmbedded().close();
      server.removeDatabase(DB_NAME);
      try {
        final Field f = ArcadeStateMachine.class.getDeclaredField("staleSnapshotAppliedFloor");
        f.setAccessible(true);
        ((AtomicLong) f.get(sm)).set(FLOOR);
      } catch (final ReflectiveOperationException e) {
        throw new IllegalStateException(e);
      }
      sm.triggerSnapshotDownload(); // no snapshot context yet: the leader answers 404
      assertThat(Files.exists(marker())).as("the fixture starts marked").isTrue();
      assertThat(leaderMissing()).isTrue();
    }

    private JSONObject handlerAnswer(final String name) throws Exception {
      // Fully qualified: the JDK's HttpServer, imported above, is the fake leader.
      final com.arcadedb.server.http.HttpServer httpServer = HTTP_SERVERS.of(server);
      final RaftHAPlugin plugin = new RaftHAPlugin();
      plugin.setRaftHAServer(raft);
      final ServerSecurityUser root = TestServerHelper.securityUser("root");
      final ExecutionResponse response = new PostBootstrapStateHandler(httpServer, plugin).execute(null, root,
          new JSONObject().put(UnverifiedClosedCopyCheck.COPY_OF, name));
      assertThat(response.getCode()).isEqualTo(200);
      return new JSONObject(response.getResponse());
    }

    /** Runs one round now, whatever the throttle says, and waits for it. */
    private void reverifyRound() throws Exception {
      lastReverify().set(0L);
      sm.reverifyUnverifiedClosedCopies();
      sm.awaitLifecycleTasksForTesting(60_000);
    }

    private AtomicLong lastReverify() throws Exception {
      final Field f = ArcadeStateMachine.class.getDeclaredField("lastUnverifiedCopyReverifyMs");
      f.setAccessible(true);
      return (AtomicLong) f.get(sm);
    }

    private AtomicInteger failureCounter() throws Exception {
      final Field f = ArcadeStateMachine.class.getDeclaredField("unverifiedCopyReverifyFailures");
      f.setAccessible(true);
      return (AtomicInteger) f.get(sm);
    }

    private int failures() throws Exception {
      return failureCounter().get();
    }

    private void setFailures(final int value) throws Exception {
      failureCounter().set(value);
    }

    private void assertRefusedOnThisFollower() {
      assertThatThrownBy(() -> server.getDatabaseForOperator(DB_NAME))
          .isInstanceOf(DatabaseNotAvailableException.class)
          .hasMessageContaining("could not verify");
      assertThat(server.existsDatabase(DB_NAME)).isFalse();
    }

    private boolean leaderMissing() {
      final DatabaseReconciler.AcquireStatus status = sm.getReconciler().getAcquireStatus(DB_NAME);
      return status != null && status.state() == DatabaseReconciler.AcquireState.LEADER_MISSING;
    }

    private Path marker() {
      return root.resolve("databases").resolve(DB_NAME).resolve(ArcadeDBServer.UNVERIFIED_CLOSED_COPY_FILE);
    }

    /** Makes the leader serve a real database with a DIFFERENT record count, counting the requests. */
    private void leaderServesSnapshot() throws IOException {
      final Path source = root.resolve("leader-copy").resolve(DB_NAME);
      try (final Database snap = new DatabaseFactory(source.toString()).create()) {
        snap.transaction(() -> {
          snap.getSchema().createVertexType("Node");
          for (int i = 0; i < SNAPSHOT_COUNT; i++)
            snap.newVertex("Node").set("v", i).save();
        });
      }
      final byte[] zip = zipDirectory(source);
      leader.createContext("/api/v1/ha/snapshot/" + DB_NAME, exchange -> {
        snapshotRequests.incrementAndGet();
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
      config.setValue(GlobalConfiguration.SERVER_NAME, "ArcadeDB_8606");
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
}
