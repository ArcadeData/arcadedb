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
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.SubclassMocks;
import com.sun.net.httpserver.HttpServer;
import org.apache.ratis.protocol.RaftPeerId;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Regression tests for issue #8067: {@code applyInstallDatabaseEntry}'s leader skip returned, at TRACE, BEFORE the
 * forceSnapshot replay guard that detects "this entry was applied in a previous session but the database is not here
 * now" (issue #7221). So a leader that had lost a database a previous session installed got no install, no durable
 * mark and no alert - the same construction #7901 fixed in {@code installFromLeaderForBootstrap}.
 * <p>
 * The leader now asserts its premise: when the replayed entry was applied before and the database has no copy on
 * this node at all, the apply fails into the per-database quarantine, which is durable, raised as an alert, hands the
 * leadership off, and - once this node is a follower - reinstalls the database from the new leader.
 * <p>
 * Fixture as in {@link Issue8464ResyncCoversClosedDatabaseTest}: a real {@link ArcadeDBServer} (HA off), a real
 * {@link ArcadeStateMachine}, a mocked {@link RaftHAServer} whose role the test flips, and a local HTTP server standing
 * in for the leader's snapshot endpoint.
 */
@Timeout(120)
class Issue8067LeaderForceSnapshotReplayMissingDatabaseTest {

  private static final String     DB_NAME        = "db8067";
  private static final String     PASSWORD       = "DefaultPasswordForTests";
  private static final int        LIVE_COUNT     = 12;
  private static final int        SNAPSHOT_COUNT = 27;
  private static final long       ENTRY_INDEX    = 42L;
  private static final RaftPeerId LOCAL          = RaftPeerId.valueOf("local");
  private static final RaftPeerId OTHER          = RaftPeerId.valueOf("other");

  @TempDir
  Path root;

  private ArcadeDBServer      server;
  private ArcadeStateMachine  sm;
  private RaftHAServer        raft;
  private HttpServer          otherPeer;
  private String              otherPeerAddress;
  private final AtomicBoolean leading   = new AtomicBoolean(true);
  private final AtomicInteger downloads = new AtomicInteger();

  @BeforeEach
  void setUp() throws IOException {
    server = startServer();
    createLocalDatabase();

    otherPeer = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    otherPeer.start();
    otherPeerAddress = "localhost:" + otherPeer.getAddress().getPort();

    raft = SubclassMocks.mock(RaftHAServer.class);
    when(raft.isLeader()).thenAnswer(inv -> leading.get());
    when(raft.getLocalPeerId()).thenReturn(LOCAL);
    when(raft.getLocalHttpAddress()).thenReturn("local-host:2480");
    when(raft.getClusterToken()).thenReturn(null);
    // Whoever leads, the snapshot source is the other peer: while this node leads, resolveSnapshotSource refuses on
    // the role before it ever looks at an address.
    when(raft.getLeaderId()).thenReturn(OTHER);
    when(raft.getUnambiguousPeerHttpAddress(OTHER)).thenReturn(otherPeerAddress);

    sm = newStateMachine();
  }

  @AfterEach
  void tearDown() throws IOException {
    if (sm != null)
      sm.close();
    if (otherPeer != null)
      otherPeer.stop(0);
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

  /**
   * The issue as reported: a previous session applied this forceSnapshot entry, the database directory was removed,
   * and the node came back as the leader. The replayed entry used to return at TRACE; it must now reach the
   * per-database quarantine, which is the apply path's durable mark and alert, and hand the leadership off.
   */
  @Test
  void aLeaderMissingADatabaseTheReplayedEntryInstalledQuarantinesIt() throws Exception {
    sm.writePersistedAppliedIndex(ENTRY_INDEX + 8, DB_NAME);
    wipeTheLocalCopy();

    assertThatThrownBy(this::applyTheReplayedEntry)
        .as("the leader cannot reinstall from itself, so the entry must fail rather than be skipped silently")
        .isInstanceOf(ReplicationException.class);

    assertThat(sm.isDatabaseDiverged(DB_NAME)).as("the missing database is quarantined").isTrue();
    assertThat(sm.quarantineCause(DB_NAME)).isEqualTo(DivergenceCause.APPLY_ERROR);
    verify(raft).handOffLeadershipToResync(contains("'" + DB_NAME + "'"));
    sm.awaitLifecycleTasksForTesting(60_000);
    assertThat(downloads.get()).as("nothing is pulled while this node still leads").isZero();

    // Durable: the next session comes back with the mark, so the alert does not vanish on a restart.
    sm.close();
    sm = newStateMachine();
    assertThat(sm.isDatabaseDiverged(DB_NAME)).as("the quarantine survives a restart").isTrue();
  }

  /**
   * And the install the leader could not do happens once leadership has moved: the health tick's targeted resync of a
   * quarantined database used to skip one with no local copy at all, which would have left this quarantine - and the
   * node out of the ready set - standing until a restart replayed the entry as a follower.
   */
  @Test
  void onceLeadershipMovesTheHealthTickInstallsTheMissingDatabase() throws Exception {
    sm.writePersistedAppliedIndex(ENTRY_INDEX + 8, DB_NAME);
    wipeTheLocalCopy();
    assertThatThrownBy(this::applyTheReplayedEntry).isInstanceOf(ReplicationException.class);
    sm.awaitLifecycleTasksForTesting(60_000);

    leading.set(false);
    otherPeerServesTheDatabase();

    sm.retryUnfilledSnapshotGap();
    sm.awaitLifecycleTasksForTesting(60_000);

    assertThat(downloads.get()).as("the missing database was pulled from the new leader").isEqualTo(1);
    assertThat(server.getDatabase(DB_NAME).countType("Node", true)).isEqualTo(SNAPSHOT_COUNT);
    assertThat(sm.isDatabaseDiverged(DB_NAME)).as("the quarantine lifts once the copy is installed").isFalse();
  }

  /**
   * Issue #8940: a sole voter has no peer to hand the leadership to or to install from, so the quarantine would never
   * lift, and it would keep the node unready and the log un-checkpointed. It must alert and carry on instead.
   */
  @Test
  void aSoleVoterMissingTheDatabaseIsNotQuarantined() {
    when(raft.isSoleVoter()).thenReturn(true);
    sm.writePersistedAppliedIndex(ENTRY_INDEX + 8, DB_NAME);
    wipeTheLocalCopy();

    assertThatCode(this::applyTheReplayedEntry).doesNotThrowAnyException();

    assertThat(sm.isDatabaseDiverged(DB_NAME)).isFalse();
    assertThat(sm.isResyncInProgress()).isFalse();
    verify(raft, never()).handOffLeadershipToResync(anyString());
  }

  /** The counter-case: a leader that holds the database takes no action, exactly as before. */
  @Test
  void aLeaderThatHoldsTheDatabaseStillSkips() {
    sm.writePersistedAppliedIndex(ENTRY_INDEX + 8, DB_NAME);

    assertThatCode(this::applyTheReplayedEntry).doesNotThrowAnyException();

    assertThat(sm.isDatabaseDiverged(DB_NAME)).isFalse();
    verify(raft, never()).handOffLeadershipToResync(anyString());
  }

  /**
   * A leader whose copy is merely CLOSED - deregistered, its files on disk - still holds the authoritative copy every
   * peer installs from. Quarantining it would hand off leadership and pull a download over perfectly good files; the
   * same distinction #7901 draws with {@code isDatabasePresentLocally}.
   */
  @Test
  void aLeaderWhoseCopyIsMerelyClosedStillSkips() {
    sm.writePersistedAppliedIndex(ENTRY_INDEX + 8, DB_NAME);
    server.getDatabase(DB_NAME).getEmbedded().close();
    server.removeDatabase(DB_NAME);
    assertThat(Files.isDirectory(root.resolve("databases").resolve(DB_NAME))).isTrue();

    assertThatCode(this::applyTheReplayedEntry).doesNotThrowAnyException();

    assertThat(sm.isDatabaseDiverged(DB_NAME)).isFalse();
    verify(raft, never()).handOffLeadershipToResync(anyString());
  }

  /**
   * No evidence a previous session applied the entry: the per-database index is evicted by a DROP entry, which follows
   * this one in the log and removes the database anyway. Replaying the older install on the leader must stay a no-op,
   * not quarantine a database the cluster has since dropped.
   */
  @Test
  void aLeaderWithNoRecordOfTheEntryStillSkips() {
    wipeTheLocalCopy();
    assertThat(sm.readPersistedAppliedIndex(DB_NAME)).isLessThan(ENTRY_INDEX);

    assertThatCode(this::applyTheReplayedEntry).doesNotThrowAnyException();

    assertThat(sm.isDatabaseDiverged(DB_NAME)).isFalse();
    verify(raft, never()).handOffLeadershipToResync(anyString());
  }

  // ------------------------------------------------------------------------------------------------------------

  /** Through applyWithRetry, the wrapper applyTransaction runs the dispatch in, so the failure routing is the real one. */
  private void applyTheReplayedEntry() {
    final RaftLogEntryCodec.DecodedEntry entry = RaftLogEntryCodec.decode(
        RaftLogEntryCodec.encodeInstallDatabaseEntry(DB_NAME, true));
    sm.applyWithRetry(ENTRY_INDEX, DB_NAME, () -> sm.applyInstallDatabaseEntry(entry, ENTRY_INDEX));
  }

  private ArcadeStateMachine newStateMachine() {
    final ArcadeStateMachine machine = new ArcadeStateMachine();
    machine.setServer(server);
    machine.setRaftHAServer(raft);
    return machine;
  }

  /** The operator's wipe: the node restarts with the database directory gone, so nothing registers it. */
  private void wipeTheLocalCopy() {
    final ServerDatabase database = server.getDatabase(DB_NAME);
    database.getEmbedded().close();
    server.removeDatabase(DB_NAME);
    FileUtils.deleteRecursively(root.resolve("databases").resolve(DB_NAME).toFile());
    assertThat(server.existsDatabase(DB_NAME)).isFalse();
    assertThat(Files.exists(root.resolve("databases").resolve(DB_NAME))).isFalse();
  }

  private void createLocalDatabase() {
    final ServerDatabase live = server.getOrCreateDatabase(DB_NAME);
    live.transaction(() -> {
      live.getSchema().createVertexType("Node");
      for (int i = 0; i < LIVE_COUNT; i++)
        live.newVertex("Node").set("v", i).save();
    });
  }

  /** Makes the other peer serve a real database with a DIFFERENT record count, so a landed install is observable. */
  private void otherPeerServesTheDatabase() throws IOException {
    final Path source = root.resolve("leader-copy").resolve(DB_NAME);
    try (final Database snap = new DatabaseFactory(source.toString()).create()) {
      snap.transaction(() -> {
        snap.getSchema().createVertexType("Node");
        for (int i = 0; i < SNAPSHOT_COUNT; i++)
          snap.newVertex("Node").set("v", i).save();
      });
    }
    final byte[] zip = zipDirectory(source);
    otherPeer.createContext("/api/v1/ha/snapshot/" + DB_NAME, exchange -> {
      downloads.incrementAndGet();
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
    config.setValue(GlobalConfiguration.SERVER_NAME, "ArcadeDB_8067");
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
