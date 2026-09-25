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
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Regression tests for issue #8367: a first-formation bootstrap replacement whose retry ALSO fails used to release
 * the readiness holder and the #8363 request-path refusal, while the copy the committed baseline rejected was still
 * on disk and still serving - and nothing ever tried the replacement again.
 * <p>
 * The fixture is the leader-absent-then-available sequence the issue asks for, driven below the multi-node level: a
 * real {@link ArcadeDBServer} (HA off) holding the database, a real {@link ArcadeStateMachine} applying the bootstrap
 * entry, a mocked {@link RaftHAServer} whose leader is unknown until the test says otherwise, and a local HTTP server
 * standing in for the leader's snapshot endpoint. Every assertion reads the real state machine and the real
 * database: the install that lands is observable as a record-count change.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
@Timeout(120)
class Issue8367BootstrapReplacementRearmTest {

  private static final String     DB_NAME        = "db8367";
  private static final String     PASSWORD       = "DefaultPasswordForTests";
  private static final int        LIVE_COUNT     = 12;
  private static final int        SNAPSHOT_COUNT = 27;
  private static final RaftPeerId LOCAL          = RaftPeerId.valueOf("local");
  private static final RaftPeerId LEADER         = RaftPeerId.valueOf("leader");

  @TempDir
  Path root;

  private ArcadeDBServer                server;
  private ArcadeStateMachine            sm;
  private RaftHAServer                  raft;
  private HttpServer                    leader;
  private final AtomicReference<String> leaderAddress     = new AtomicReference<>();
  private final AtomicInteger           snapshotDownloads = new AtomicInteger();

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

    raft = mock(RaftHAServer.class);
    when(raft.isLeader()).thenReturn(false);
    when(raft.getLocalPeerId()).thenReturn(LOCAL);
    when(raft.getLocalHttpAddress()).thenReturn("local-host:2480");
    when(raft.getClusterToken()).thenReturn(null);
    // The leader is unknown until reachableLeader() publishes one: the ordinary state at a first formation, where the
    // bootstrap entry is applied while the election on this peer may still be settling.
    when(raft.getLeaderId()).thenAnswer(invocation -> leaderAddress.get() == null ? null : LEADER);
    when(raft.getUnambiguousPeerHttpAddress(LEADER)).thenAnswer(invocation -> leaderAddress.get());

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
      try {
        if (server.existsDatabase(DB_NAME))
          ((DatabaseInternal) server.getDatabase(DB_NAME)).getEmbedded().drop();
      } catch (final Exception ignore) {
        // best-effort cleanup; the @TempDir is removed regardless
      }
      server.stop();
    }
  }

  // ------------------------------------------------------------------------------------------------------------
  // The defect
  // ------------------------------------------------------------------------------------------------------------

  /**
   * The issue as reported. The first download fails and its one scheduled retry fails too - no leader is reachable
   * for either - and at that point the node used to go back into the pool with the #8363 gate open, serving the copy
   * the committed baseline had decided against.
   */
  @Test
  void aReplacementWhoseRetryAlsoFailsKeepsHoldingReadinessAndTheRequestGate() throws Exception {
    applyMismatchedBaseline();

    assertThat(sm.getPendingBootstrapReplacements())
        .as("the replacement the baseline ordered is still pending after both attempts failed")
        .containsExactly(DB_NAME);
    assertThat(sm.isBootstrapInstallInFlight(DB_NAME))
        .as("the #8363 request gate keeps refusing clients on the rejected copy").isTrue();
    assertThat(sm.getBootstrapInstallsInFlight())
        .as("the database stays published in the bootstrap-install-in-progress alert").containsExactly(DB_NAME);
    assertThat(sm.bootstrapWindowReason())
        .as("the node stays out of the Service").isNotNull().contains("replacing 1 database(s) on this node");
    assertThat(liveCount()).as("nothing replaced the copy").isEqualTo(LIVE_COUNT);
    assertThat(sm.getBootstrapUnreconciledDatabases())
        .as("an ordinary 'no leader yet' failure must not raise the #6124 CRITICAL divergence alert").isEmpty();
  }

  /**
   * The sharper half: something has to try again. The health tick is that something, and it replaces the copy the
   * moment a leader is reachable, which releases the node back into the Service.
   */
  @Test
  void thePeriodicCheckReplacesTheCopyOnceALeaderIsReachable() throws Exception {
    applyMismatchedBaseline();

    // Still no leader: the tick stands down without spending its throttle slot, so the first tick after a leader
    // appears is the one that installs.
    sm.retryPendingBootstrapReplacements();
    sm.awaitLifecycleTasksForTesting(60_000);
    assertThat(sm.getPendingBootstrapReplacements()).containsExactly(DB_NAME);
    assertThat(snapshotDownloads.get()).isZero();

    reachableLeader();
    sm.retryPendingBootstrapReplacements();
    sm.awaitLifecycleTasksForTesting(60_000);

    assertThat(snapshotDownloads.get()).as("the tick pulled the leader's snapshot").isEqualTo(1);
    assertThat(liveCount()).as("the rejected copy was replaced by the leader's").isEqualTo(SNAPSHOT_COUNT);
    assertThat(sm.getPendingBootstrapReplacements()).isEmpty();
    assertThat(sm.isBootstrapInstallInFlight(DB_NAME)).as("clients are served again").isFalse();
    assertThat(sm.getBootstrapInstallsInFlight()).isEmpty();
    assertThat(sm.bootstrapWindowReason()).as("and the node rejoins the Service by itself").isNull();

    // Nothing left to do: another tick is free and downloads nothing.
    sm.retryPendingBootstrapReplacements();
    sm.awaitLifecycleTasksForTesting(60_000);
    assertThat(snapshotDownloads.get()).isEqualTo(1);
  }

  /** A retry that fails again keeps the replacement pending rather than consuming it. */
  @Test
  void aTickWhoseDownloadFailsKeepsTheReplacementPending() throws Exception {
    applyMismatchedBaseline();

    // A leader is named, but its snapshot endpoint answers 503: the download fails on the tick too.
    leader.createContext("/api/v1/ha/snapshot/" + DB_NAME, exchange -> {
      snapshotDownloads.incrementAndGet();
      exchange.sendResponseHeaders(503, -1);
      exchange.close();
    });
    leader.start();
    leaderAddress.set("localhost:" + leader.getAddress().getPort());

    sm.retryPendingBootstrapReplacements();
    sm.awaitLifecycleTasksForTesting(60_000);

    assertThat(snapshotDownloads.get()).as("the tick did attempt the download").isPositive();
    assertThat(sm.getPendingBootstrapReplacements()).containsExactly(DB_NAME);
    assertThat(sm.isBootstrapInstallInFlight(DB_NAME)).isTrue();
    assertThat(sm.getBootstrapInstallsInFlight())
        .as("the tick's own install holder was released; only the pending one remains").containsExactly(DB_NAME);
    assertThat(liveCount()).isEqualTo(LIVE_COUNT);
  }

  // ------------------------------------------------------------------------------------------------------------
  // Every other path that replaces the copy settles the replacement too
  // ------------------------------------------------------------------------------------------------------------

  /** The operator remedy every log line names: POST /api/v1/cluster/resync/{database}. */
  @Test
  void theOperatorResyncSettlesThePendingReplacement() throws Exception {
    applyMismatchedBaseline();
    reachableLeader();

    sm.resyncDatabaseFromLeader(DB_NAME);

    assertThat(liveCount()).isEqualTo(SNAPSHOT_COUNT);
    assertThat(sm.getPendingBootstrapReplacements()).isEmpty();
    assertThat(sm.isBootstrapInstallInFlight(DB_NAME)).isFalse();
    assertThat(sm.bootstrapWindowReason()).isNull();
  }

  /** The full resync a leader change or the watchdog drives reinstalls every database, this one included. */
  @Test
  void aFullResyncSettlesThePendingReplacement() throws Exception {
    applyMismatchedBaseline();
    reachableLeader();

    sm.triggerSnapshotDownload();

    assertThat(liveCount()).isEqualTo(SNAPSHOT_COUNT);
    assertThat(sm.getPendingBootstrapReplacements()).isEmpty();
    assertThat(sm.isBootstrapInstallInFlight(DB_NAME)).isFalse();
    assertThat(sm.bootstrapWindowReason()).isNull();
  }

  /** A forced reinstall entry (the leader's {@code forceSnapshot} install) replaces the copy just the same. */
  @Test
  void aForcedInstallEntrySettlesThePendingReplacement() throws Exception {
    applyMismatchedBaseline();
    reachableLeader();

    sm.applyInstallDatabaseEntry(RaftLogEntryCodec.decode(RaftLogEntryCodec.encodeInstallDatabaseEntry(DB_NAME, true)), 9L);

    assertThat(liveCount()).isEqualTo(SNAPSHOT_COUNT);
    assertThat(sm.getPendingBootstrapReplacements()).isEmpty();
    assertThat(sm.isBootstrapInstallInFlight(DB_NAME)).isFalse();
  }

  /** A dropped database has no copy left to replace, and must not refuse clients on a namesake created later. */
  @Test
  void droppingTheDatabaseSettlesThePendingReplacement() throws Exception {
    applyMismatchedBaseline();

    sm.applyDropDatabaseEntry(RaftLogEntryCodec.decode(RaftLogEntryCodec.encodeDropDatabaseEntry(DB_NAME)));

    assertThat(sm.getPendingBootstrapReplacements()).isEmpty();
    assertThat(sm.isBootstrapInstallInFlight(DB_NAME)).isFalse();
    assertThat(sm.getBootstrapInstallsInFlight()).isEmpty();
  }

  // ------------------------------------------------------------------------------------------------------------
  // Boundaries of the re-arm
  // ------------------------------------------------------------------------------------------------------------

  /**
   * A node cannot install from itself. On the leader the tick installs nothing - and keeps holding, because the copy
   * is still the one the baseline rejected.
   */
  @Test
  void onTheLeaderTheTickInstallsNothingAndKeepsHolding() throws Exception {
    applyMismatchedBaseline();
    reachableLeader();
    when(raft.isLeader()).thenReturn(true);

    sm.retryPendingBootstrapReplacements();
    sm.awaitLifecycleTasksForTesting(60_000);

    assertThat(snapshotDownloads.get()).isZero();
    assertThat(sm.getPendingBootstrapReplacements()).containsExactly(DB_NAME);
    assertThat(sm.isBootstrapInstallInFlight(DB_NAME)).isTrue();
    assertThat(liveCount()).isEqualTo(LIVE_COUNT);
  }

  /**
   * The constraint the issue names first: the #6124 "local is fresher, refuse to overwrite" branch protects an
   * operator copy on purpose, so it is never armed and no tick ever downloads over it.
   */
  @Test
  void theLocalIsFresherBranchIsNeverArmed() throws Exception {
    reachableLeader();
    // A baseline BELOW the local transaction id: the refusal branch, which installs nothing.
    assertThatNoException().isThrownBy(() -> sm.applyBootstrapFingerprintEntry(baseline(0L), 7L));
    sm.awaitLifecycleTasksForTesting(60_000);

    assertThat(sm.getBootstrapUnreconciledDatabases()).containsExactly(DB_NAME);
    assertThat(sm.getPendingBootstrapReplacements()).isEmpty();
    assertThat(sm.isBootstrapInstallInFlight(DB_NAME)).isFalse();

    sm.retryPendingBootstrapReplacements();
    sm.awaitLifecycleTasksForTesting(60_000);
    assertThat(snapshotDownloads.get()).as("the operator's fresher copy is never downloaded over").isZero();
    assertThat(liveCount()).isEqualTo(LIVE_COUNT);
  }

  // ------------------------------------------------------------------------------------------------------------

  /**
   * Applies a bootstrap baseline this node's copy does not match, with no leader reachable, and waits for the one
   * scheduled retry to have run: both attempts have failed when this returns.
   */
  private void applyMismatchedBaseline() throws Exception {
    // lastTxId above the local one, so the "local is fresher" refusal does not fire.
    assertThatNoException().isThrownBy(() -> sm.applyBootstrapFingerprintEntry(baseline(Long.MAX_VALUE), 7L));
    sm.awaitLifecycleTasksForTesting(60_000);
    assertThat(snapshotDownloads.get()).as("no leader was reachable for either attempt").isZero();
  }

  private static RaftLogEntryCodec.DecodedEntry baseline(final long lastTxId) {
    final ByteString encoded = RaftLogEntryCodec.encodeBootstrapFingerprintEntry(DB_NAME, "0".repeat(64), lastTxId);
    return RaftLogEntryCodec.decode(encoded);
  }

  private long liveCount() {
    return server.getDatabase(DB_NAME).countType("Node", true);
  }

  /** Publishes a leader whose snapshot endpoint serves a real database with a DIFFERENT record count. */
  private void reachableLeader() throws IOException {
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
      snapshotDownloads.incrementAndGet();
      exchange.sendResponseHeaders(200, zip.length);
      exchange.getResponseBody().write(zip);
      exchange.close();
    });
    leader.start();
    leaderAddress.set("localhost:" + leader.getAddress().getPort());
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
    config.setValue(GlobalConfiguration.SERVER_NAME, "ArcadeDB_8367");
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
