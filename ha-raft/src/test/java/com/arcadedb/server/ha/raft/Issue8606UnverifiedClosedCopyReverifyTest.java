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
import org.apache.ratis.util.LifeCycle;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
 * Same fixture as {@link Issue8589UnverifiedClosedCopyAfterResyncTest}, with the fake leader also answering the
 * {@code copyOf} form of the bootstrap-state RPC.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
@Timeout(120)
class Issue8606UnverifiedClosedCopyReverifyTest {
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
  private       RaftHAServer       raft;
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

    raft = mock(RaftHAServer.class);
    when(raft.isLeader()).thenReturn(false);
    when(raft.getLocalPeerId()).thenReturn(LOCAL);
    when(raft.getLocalHttpAddress()).thenReturn("local-host:2480");
    when(raft.getClusterToken()).thenReturn(null);
    when(raft.getLeaderId()).thenReturn(LEADER);
    when(raft.getUnambiguousPeerHttpAddress(LEADER)).thenReturn(leaderAddress);
    when(raft.getUnambiguousPeerHttpAddress(NEW_LEADER)).thenReturn(leaderAddress);

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
          ((DatabaseInternal) server.getDatabase(DB_NAME)).getEmbedded().drop();
      } catch (final Exception ignore) {
        // best-effort cleanup; the @TempDir is removed regardless
      }
      server.stop();
    }
  }

  /** The issue as reported: the leader holds the database again, and the health tick reinstalls the copy on its own. */
  @Test
  void theHealthTickReinstallsAMarkedCopyOnceTheLeaderHoldsTheDatabaseAgain() throws Exception {
    leaderServesSnapshot();
    leaderHolds.add(DB_NAME);

    reverifyRound();

    assertThat(Files.exists(marker())).as("the install replaced the copy and its mark").isFalse();
    assertThat(leaderMissing()).isFalse();
    assertThat(server.getDatabase(DB_NAME).countType("Node", true)).as("the follower serves the leader's copy")
        .isEqualTo(SNAPSHOT_COUNT);
    assertThat(failures()).isZero();
  }

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
    assertThat(ArcadeStateMachine.unverifiedClosedCopyReverifyIntervalMs(0))
        .isEqualTo(ArcadeStateMachine.UNVERIFIED_COPY_REVERIFY_INTERVAL_MS);
    assertThat(ArcadeStateMachine.unverifiedClosedCopyReverifyIntervalMs(1))
        .isEqualTo(2 * ArcadeStateMachine.UNVERIFIED_COPY_REVERIFY_INTERVAL_MS);
    assertThat(ArcadeStateMachine.unverifiedClosedCopyReverifyIntervalMs(Integer.MAX_VALUE))
        .isEqualTo(ArcadeStateMachine.UNVERIFIED_COPY_REVERIFY_MAX_INTERVAL_MS);
  }

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
  @Test
  void aRoundThatThrowsIsCountedAndReleasesTheGuard() throws Exception {
    // The tick reads the leader once, the round once more: the round's read throws.
    when(raft.getLeaderId()).thenReturn(LEADER).thenThrow(new IllegalStateException("boom")).thenReturn(LEADER);

    reverifyRound();

    assertThat(failures()).isEqualTo(1);
    assertThat(probes.get()).isZero();
    reverifyRound();
    assertThat(probes.get()).as("the guard was released, so the next round runs").isEqualTo(1);
  }

  /** A new leader may hold what the previous one did not: it is asked at the next tick, whatever the backoff. */
  @Test
  void aNewLeaderIsAskedAtTheNextTick() throws Exception {
    reverifyRound();
    assertThat(probes.get()).isEqualTo(1);

    leaderServesSnapshot();
    leaderHolds.add(DB_NAME);
    when(raft.getLeaderId()).thenReturn(NEW_LEADER);
    sm.reverifyUnverifiedClosedCopies();
    sm.awaitLifecycleTasksForTesting(60_000);

    assertThat(probes.get()).isEqualTo(2);
    assertThat(Files.exists(marker())).isFalse();
  }

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
    when(raft.getStateMachine()).thenReturn(sm);
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
  @Test
  void theLeaderDoesNotReverify() throws Exception {
    when(raft.isLeader()).thenReturn(true);

    reverifyRound();

    assertThat(probes.get()).isZero();
    assertThat(Files.exists(marker())).isTrue();
  }

  /** A leader that predates the field cannot say it serves the database, so no install is attempted on its answer. */
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
  @Test
  void theLeadersHandlerReportsWhetherItServesTheDatabase() throws Exception {
    when(raft.getStateMachine()).thenReturn(sm);
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
    final ServerDatabase database = server.getDatabase(DB_NAME);
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
    assertThatThrownBy(() -> server.getDatabase(DB_NAME))
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
