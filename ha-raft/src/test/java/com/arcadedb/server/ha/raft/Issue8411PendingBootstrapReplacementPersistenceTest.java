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
import com.arcadedb.serializer.json.JSONObject;
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
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

/**
 * Regression tests for issue #8411: a first-formation bootstrap replacement left pending by issue #8367 lived in
 * memory only, so a restart before the leader's copy landed brought the node back with the rejected copy on disk, in
 * the Service, and with nothing retrying the replacement - the replay of the baseline entry is skipped for a registered
 * database whose applied index covers it, and is not replayed at all once a Ratis snapshot is past it.
 * <p>
 * Same fixture as {@link Issue8367BootstrapReplacementRearmTest}. A restart is a brand-new {@link ArcadeStateMachine}
 * on the same server directory: it applies nothing, exactly like a node whose baseline entry is skipped or compacted.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
@Timeout(120)
class Issue8411PendingBootstrapReplacementPersistenceTest {

  private static final String     DB_NAME        = "db8411";
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
    raft.on("getLeaderId", args -> leaderAddress.get() == null ? null : LEADER);
    raft.on("getUnambiguousPeerHttpAddress", args -> Objects.equals(args[0], LEADER) ? leaderAddress.get() : null);

    sm = newStateMachine();
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

  /** The issue as reported: the restarted node must come back holding, not serving the rejected copy. */
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

  /** The request gate alone, read first after the restart: nothing else may be needed to load the pending state. */
  @Test
  void theRequestGateHoldsWithoutAnyOtherReadAfterTheRestart() throws Exception {
    applyMismatchedBaseline();

    restart();

    assertThat(sm.isBootstrapInstallInFlight(DB_NAME)).isTrue();
  }

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

  /** A DROP of the database removes its entry, flag included, so a namesake created later is not held after a restart. */
  @Test
  void droppingTheDatabaseClearsThePersistedReplacement() throws Exception {
    applyMismatchedBaseline();

    sm.applyDropDatabaseEntry(RaftLogEntryCodec.decode(RaftLogEntryCodec.encodeDropDatabaseEntry(DB_NAME)));

    restart();
    assertThat(sm.getPendingBootstrapReplacements()).isEmpty();
    assertThat(sm.isBootstrapInstallInFlight(DB_NAME)).isFalse();
  }

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
    config.setValue(GlobalConfiguration.SERVER_NAME, "ArcadeDB_8411");
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
