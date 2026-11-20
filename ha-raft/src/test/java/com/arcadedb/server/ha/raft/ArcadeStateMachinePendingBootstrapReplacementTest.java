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
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerDatabase;
import com.arcadedb.server.StaticBaseServerTest;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

/**
 * Tests of the pending first-formation bootstrap replacement of the {@link ArcadeStateMachine}: it keeps holding
 * readiness while the replacement is retried, and survives a restart of the node.
 * <p>
 * The fixture is a real {@link ArcadeDBServer} (HA off) holding the database, a real state machine applying the bootstrap
 * entry, a stubbed {@link RaftHAServer} whose leader is unknown until the test says otherwise, and a local HTTP server
 * standing in for the leader snapshot endpoint.
 */
@Timeout(120)
class ArcadeStateMachinePendingBootstrapReplacementTest {

  private static final String     DB_NAME        = "pending-replacement-db";
  private static final String     PASSWORD       = "DefaultPasswordForTests";
  private static final int        LIVE_COUNT     = 12;
  private static final int        SNAPSHOT_COUNT = 27;
  private static final RaftPeerId LOCAL          = RaftPeerId.valueOf("local");
  private static final RaftPeerId LEADER         = RaftPeerId.valueOf("leader");

  @TempDir
  Path root;
  private ArcadeDBServer                server;
  private ArcadeStateMachine            sm;
  private FakeRaftHAServer                  raft;
  private HttpServer                    leader;
  private final AtomicReference<String> leaderAddress     = new AtomicReference<>();
  private final AtomicInteger           snapshotDownloads = new AtomicInteger();

  /**
   * Applies a bootstrap baseline this node's copy does not match, with no leader reachable, and waits for the one
   * scheduled retry to have run: both attempts have failed when this returns.
   */

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

    raft = FakeRaftHAServer.detached();
    raft.leader(false);
    raft.localPeerId(LOCAL);
    raft.localHttpAddress("local-host:2480");
    raft.clusterToken(null);
    // The leader is unknown until reachableLeader() publishes one: the ordinary state at a first formation, where the
    // bootstrap entry is applied while the election on this peer may still be settling.
    raft.on("getLeaderId", args -> leaderAddress.get() == null ? null : LEADER);
    raft.on("getUnambiguousPeerHttpAddress", args -> Objects.equals(args[0], LEADER) ? leaderAddress.get() : null);

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
          ((DatabaseInternal) server.getDatabaseForOperator(DB_NAME)).getEmbedded().drop();
      } catch (final Exception ignore) {
        // best-effort cleanup; the @TempDir is removed regardless
      }
      server.stop();
    }
  }

  // Regression for #8367: a replacement whose retry also fails keeps holding readiness and is retried
  // ------------------------------------------------------------------------------------------------------------
  // The defect
  // ------------------------------------------------------------------------------------------------------------

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
   * <p>
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

  // Regression for #8367: a replacement whose retry also fails keeps holding readiness and is retried
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

  // Regression for #8367: a replacement whose retry also fails keeps holding readiness and is retried
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

  // Regression for #8367: a replacement whose retry also fails keeps holding readiness and is retried
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

  // Regression for #8367: a replacement whose retry also fails keeps holding readiness and is retried
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

  // Regression for #8367: a replacement whose retry also fails keeps holding readiness and is retried
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

  // Regression for #8367: a replacement whose retry also fails keeps holding readiness and is retried
  /** A dropped database has no copy left to replace, and must not refuse clients on a namesake created later. */
  @Test
  void droppingTheDatabaseSettlesThePendingReplacement() throws Exception {
    applyMismatchedBaseline();

    sm.applyDropDatabaseEntry(RaftLogEntryCodec.decode(RaftLogEntryCodec.encodeDropDatabaseEntry(DB_NAME)));

    assertThat(sm.getPendingBootstrapReplacements()).isEmpty();
    assertThat(sm.isBootstrapInstallInFlight(DB_NAME)).isFalse();
    assertThat(sm.getBootstrapInstallsInFlight()).isEmpty();
  }

  // Regression for #8367: a replacement whose retry also fails keeps holding readiness and is retried
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
    raft.leader(true);

    sm.retryPendingBootstrapReplacements();
    sm.awaitLifecycleTasksForTesting(60_000);

    assertThat(snapshotDownloads.get()).isZero();
    assertThat(sm.getPendingBootstrapReplacements()).containsExactly(DB_NAME);
    assertThat(sm.isBootstrapInstallInFlight(DB_NAME)).isTrue();
    assertThat(liveCount()).isEqualTo(LIVE_COUNT);
  }

  // Regression for #8367: a replacement whose retry also fails keeps holding readiness and is retried
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
    return server.getDatabaseForOperator(DB_NAME).countType("Node", true);
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
    config.setValue(GlobalConfiguration.SERVER_NAME, "ArcadeDB_pending_replacement");
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

  // Regression for #8411: a pending replacement is persisted and survives a restart
  /**
   * Regression tests for issue #8411: a first-formation bootstrap replacement left pending by issue #8367 lived in
   * memory only, so a restart before the leader's copy landed brought the node back with the rejected copy on disk, in
   * the Service, and with nothing retrying the replacement - the replay of the baseline entry is skipped for a registered
   * database whose applied index covers it, and is not replayed at all once a Ratis snapshot is past it.
   * <p>
   * Same fixture as the tests above. A restart is a brand-new {@link ArcadeStateMachine}
   * on the same server directory: it applies nothing, exactly like a node whose baseline entry is skipped or compacted.
   * <p>
   * The issue as reported: the restarted node must come back holding, not serving the rejected copy.
   */
  @Test
  void aPendingReplacementSurvivesARestart() throws Exception {
    applyMismatchedBaseline();
    assertThat(sm.getPendingBootstrapReplacements()).containsExactly(DB_NAME);

    restart();

    assertThat(sm.getPendingBootstrapReplacements())
        .as("the replacement the committed baseline ordered is still pending after the restart").containsExactly(DB_NAME);
    assertThat(sm.isBootstrapInstallInFlight(DB_NAME))
        .as("the #8363 request gate refuses clients on the rejected copy from the first request on").isTrue();
    assertThat(sm.getBootstrapInstallsInFlight()).containsExactly(DB_NAME);
    assertThat(sm.bootstrapWindowReason())
        .as("the node stays out of the Service").isNotNull().contains("replacing 1 database(s) on this node");
    assertThat(sm.getBootstrapUnreconciledDatabases())
        .as("restoring it must not raise the #6124 CRITICAL divergence alert").isEmpty();
    assertThat(liveCount()).isEqualTo(LIVE_COUNT);
  }

  // Regression for #8411: a pending replacement is persisted and survives a restart
  /** The request gate alone, read first after the restart: nothing else may be needed to load the pending state. */
  @Test
  void theRequestGateHoldsWithoutAnyOtherReadAfterTheRestart() throws Exception {
    applyMismatchedBaseline();

    restart();

    assertThat(sm.isBootstrapInstallInFlight(DB_NAME)).isTrue();
  }

  // Regression for #8411: a pending replacement is persisted and survives a restart
  /** Something still has to try again after the restart: the health tick replaces the copy and clears the flag. */
  @Test
  void theHealthTickReplacesTheCopyAfterTheRestartAndClearsTheFlag() throws Exception {
    applyMismatchedBaseline();
    restart();

    reachableLeader();
    sm.retryPendingBootstrapReplacements();
    sm.awaitLifecycleTasksForTesting(60_000);

    assertThat(snapshotDownloads.get()).as("the tick pulled the leader's snapshot").isEqualTo(1);
    assertThat(liveCount()).isEqualTo(SNAPSHOT_COUNT);
    assertThat(sm.getPendingBootstrapReplacements()).isEmpty();
    assertThat(sm.isBootstrapInstallInFlight(DB_NAME)).isFalse();
    assertThat(sm.getBootstrapInstallsInFlight())
        .as("the restored holder was released exactly once").isEmpty();
    assertThat(sm.bootstrapWindowReason()).isNull();
    assertThat(persistedEntry().has("pendingReplacement")).as("the flag is cleared on disk").isFalse();

    restart();
    assertThat(sm.getPendingBootstrapReplacements()).as("and a later restart finds nothing pending").isEmpty();
    assertThat(sm.isBootstrapInstallInFlight(DB_NAME)).isFalse();
  }

  // Regression for #8411: a pending replacement is persisted and survives a restart
  /** The operator remedy, run as the first thing after the restart, settles the durable replacement too. */
  @Test
  void theOperatorResyncAfterTheRestartSettlesThePersistedReplacement() throws Exception {
    applyMismatchedBaseline();
    restart();
    reachableLeader();

    sm.resyncDatabaseFromLeader(DB_NAME);

    assertThat(liveCount()).isEqualTo(SNAPSHOT_COUNT);
    assertThat(sm.getPendingBootstrapReplacements()).isEmpty();
    assertThat(sm.getBootstrapInstallsInFlight()).isEmpty();
    restart();
    assertThat(sm.getPendingBootstrapReplacements()).isEmpty();
  }

  // Regression for #8411: a pending replacement is persisted and survives a restart
  /** A path that replaces the copy in the same session clears the persisted flag, not just the in-memory one. */
  @Test
  void aReplacementSettledBeforeTheRestartIsNotRestored() throws Exception {
    applyMismatchedBaseline();
    assertThat(persistedEntry().getBoolean("pendingReplacement", false)).as("armed durably").isTrue();
    reachableLeader();

    sm.triggerSnapshotDownload();
    assertThat(sm.getPendingBootstrapReplacements()).isEmpty();
    assertThat(persistedEntry().has("pendingReplacement")).isFalse();

    restart();
    assertThat(sm.getPendingBootstrapReplacements()).isEmpty();
    assertThat(sm.isBootstrapInstallInFlight(DB_NAME)).isFalse();
  }

  // Regression for #8411: a pending replacement is persisted and survives a restart
  /** A DROP of the database removes its entry, flag included, so a namesake created later is not held after a restart. */
  @Test
  void droppingTheDatabaseClearsThePersistedReplacement() throws Exception {
    applyMismatchedBaseline();

    sm.applyDropDatabaseEntry(RaftLogEntryCodec.decode(RaftLogEntryCodec.encodeDropDatabaseEntry(DB_NAME)));

    restart();
    assertThat(sm.getPendingBootstrapReplacements()).isEmpty();
    assertThat(sm.isBootstrapInstallInFlight(DB_NAME)).isFalse();
  }

  // Regression for #8411: a pending replacement is persisted and survives a restart
  /** A replay of the same baseline after the restart must not stack a second holder on the restored one. */
  @Test
  void aReplayAfterTheRestartDoesNotStackASecondHolder() throws Exception {
    applyMismatchedBaseline();
    restart();

    // The per-database replay-skip does not fire here (the test convenience persists no applied index), so the
    // mismatch arm runs again and fails again: its holder must be released, not added to the restored one.
    applyMismatchedBaseline();
    assertThat(sm.getPendingBootstrapReplacements()).containsExactly(DB_NAME);

    reachableLeader();
    sm.resyncDatabaseFromLeader(DB_NAME);

    assertThat(sm.getBootstrapInstallsInFlight()).as("no holder outlives the replacement").isEmpty();
    assertThat(sm.bootstrapWindowReason()).isNull();
  }

  // Regression for #8411: a pending replacement is persisted and survives a restart
  /** Upgrade path: a file written before #8411 carries no flag and reads as nothing pending. */
  @Test
  void aFileWithoutTheFlagReadsAsNothingPending() throws Exception {
    final Path raftDir = root.resolve("databases").resolve(".raft");
    Files.createDirectories(raftDir);
    Files.writeString(raftDir.resolve("bootstrap-baselines"),
        new JSONObject().put(DB_NAME, new JSONObject().put("fingerprint", "0".repeat(64)).put("lastTxId", 5L)).toString());

    restart();

    assertThat(sm.getBootstrapBaseline(DB_NAME)).isNotNull();
    assertThat(sm.getPendingBootstrapReplacements()).isEmpty();
    assertThat(sm.isBootstrapInstallInFlight(DB_NAME)).isFalse();
  }

  // ------------------------------------------------------------------------------------------------------------

  private ArcadeStateMachine newStateMachine() {
    final ArcadeStateMachine machine = new ArcadeStateMachine();
    machine.setServer(server);
    machine.setRaftHAServer(raft);
    return machine;
  }

  /** A process restart as far as the state machine is concerned: a new instance, no in-memory state. */
  private void restart() throws IOException {
    sm.close();
    sm = newStateMachine();
  }

  private JSONObject persistedEntry() throws IOException {
    final String content = Files.readString(root.resolve("databases").resolve(".raft").resolve("bootstrap-baselines"));
    return new JSONObject(content.trim()).getJSONObject(DB_NAME);
  }
}
