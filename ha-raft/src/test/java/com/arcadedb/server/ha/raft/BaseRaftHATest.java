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
import com.arcadedb.exception.DatabaseIsClosedException;
import com.arcadedb.log.LogManager;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.HAServerPlugin;
import com.arcadedb.server.ServerPlugin;
import com.arcadedb.utility.FileUtils;

import org.apache.ratis.protocol.RaftPeerId;

import java.io.File;
import java.util.Map;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.logging.Level;

/**
 * Base class for Raft HA integration tests.
 * Configures servers to use the Raft HA implementation instead of the legacy HAServer.
 * Overrides lifecycle methods that depend on legacy HAServer APIs.
 */
public abstract class BaseRaftHATest extends BaseGraphServerTest {

  private static final int  BASE_RAFT_PORT          = 2434;
  // 120s: two independent CI runs of this PR (runs 31578870619 and 31587763511) each showed
  // Issue5410AbandonedTicketReleaseIT stall for the ENTIRE poll budget - once at 30s, once at 60s -
  // immediately after RaftMigrationCompactionRaceIT (a 72k-record, 24-compaction-round, 12-writer-
  // thread IT) ran in the same shared, reused JVM fork (failsafe reuseForks=true/forkCount=1 for the
  // whole module). Both stalls were confirmed real via the test's own embedded log timestamps, not a
  // log-flush artifact. Not reproducible locally in isolation, nor under an emulated 4-CPU/3.9GB-heap
  // constraint matching the CI runner - only in the full-suite adjacency. The suspected cause was
  // ArcadeStateMachine.notifyInstallSnapshotFromLeader running its download on the JDK common
  // ForkJoinPool, which is also shared with JDK GC/reference handler internals - exactly the
  // "long-running [GC-heavy] work starves engine work" failure shape this looks like. That caller has
  // its own executor since issue #6202, so half of the suspicion is gone; the timeout stays until a
  // CI run shows it can come down, and the other half of the fix - isolating heavy ITs into their own
  // fork - is still a follow-up. The bump costs nothing when nothing stalls: withResyncRetry(),
  // awaitValue() and awaitCountOn() all return as soon as the condition is met.
  private static final long RESYNC_RETRY_TIMEOUT_MS = 120_000;

  /**
   * Returns the peer ID for a given server index in the test cluster.
   * Matches the host_raftPort format used by {@link RaftHAServer#parsePeerList}.
   */
  protected String peerIdForIndex(final int index) {
    return "localhost_" + (BASE_RAFT_PORT + index);
  }

  /**
   * Returns true if Raft storage directories should be preserved across server stop/start
   * within a single test. Override to true in tests that call {@link #restartServer(int)}.
   * Default is false to match existing test behaviour.
   */
  protected boolean persistentRaftStorage() {
    return false;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    if (persistentRaftStorage())
      config.setValue(GlobalConfiguration.HA_RAFT_PERSIST_STORAGE, true);

    // Disable the health monitor in tests by default. During teardown, servers stop
    // one by one, causing remaining nodes' Ratis to enter CLOSED state. The health
    // monitor would repeatedly restart Ratis (creating new threads each time) until
    // the JVM runs out of native threads. Tests that explicitly need the health
    // monitor (e.g. RaftHealthMonitorRecoveryIT) can override and re-enable it.
    config.setValue(GlobalConfiguration.HA_HEALTH_CHECK_INTERVAL, 0L);

    // Each in-process server needs a unique Raft port. Extract the server index
    // from the server name (e.g., "ArcadeDB_1" → index 1) to offset the base port.
    final String serverName = config.getValueAsString(GlobalConfiguration.SERVER_NAME);
    final int index = Integer.parseInt(serverName.substring(serverName.lastIndexOf('_') + 1));
    config.setValue(GlobalConfiguration.HA_RAFT_PORT, BASE_RAFT_PORT + index);
  }

  /**
   * Extends the base cleanup to also remove Raft storage directories.
   * This ensures that stale Raft state from a previous test run (e.g. after a crash
   * or forced JVM kill) does not prevent the server from starting up correctly.
   * Within the same test run, {@link #restartServer(int)} preserves the Raft storage
   * because {@link GlobalConfiguration#HA_RAFT_PERSIST_STORAGE} is set to true.
   * <p>
   * NOTE: this method reads {@link GlobalConfiguration#HA_RAFT_STORAGE_DIRECTORY} from
   * the global static config, not from the per-server {@link com.arcadedb.ContextConfiguration}.
   * Subclasses that set {@code HA_RAFT_STORAGE_DIRECTORY} via {@code onServerConfiguration()}
   * must override this method to clean up the custom directory, as shown in
   * {@link RaftStorageDirectoryIT}.
   * <p>
   * Since issue #5272 the default Raft storage lives under the database directory
   * ({@code <databaseDirectory>/.raft-storage}), which {@code super.deleteDatabaseFolders()} already
   * removes; the explicit deletion below additionally cleans the legacy under-root-path location so a
   * stale directory left by an old build does not block startup.
   */
  @Override
  protected void deleteDatabaseFolders() {
    super.deleteDatabaseFolders();
    final String rootPath = GlobalConfiguration.SERVER_ROOT_PATH.getValueAsString();
    if (rootPath == null)
      return;
    for (int i = 0; i < getServerCount(); i++)
      FileUtils.deleteRecursively(new File(rootPath, "raft-storage-" + peerIdForIndex(i)));
  }

  @Override
  protected String getServerAddresses() {
    // For Raft HA, the server list uses host:raftPort:httpPort so that follower nodes can
    // forward write commands to the leader via HTTP when needed.
    // The HTTP port here is a best-effort hint used before servers start; after startup
    // startServers() patches httpAddresses with the actual bound ports.
    final StringBuilder sb = new StringBuilder();
    for (int i = 0; i < getServerCount(); i++) {
      if (i > 0)
        sb.append(",");
      sb.append("localhost:").append(BASE_RAFT_PORT + i).append(":").append(2480 + i);
    }
    return sb.toString();
  }

  @Override
  protected void startServers() {
    super.startServers();
    // Patch every server's httpAddresses map with the ports the HTTP server actually bound to.
    // This corrects stale values from getServerAddresses() when dynamic port assignment shifted
    // any server away from its expected port (e.g. port already taken by another process).
    for (int i = 0; i < getServerCount(); i++) {
      final RaftHAPlugin plugin = getRaftPlugin(i);
      if (plugin == null || plugin.getRaftHAServer() == null)
        continue;
      final Map<RaftPeerId, String> httpAddresses = plugin.getRaftHAServer().getHttpAddresses();
      for (int j = 0; j < getServerCount(); j++) {
        final RaftPeerId peerId = RaftPeerId.valueOf(peerIdForIndex(j));
        httpAddresses.put(peerId, "localhost:" + getServer(j).getHttpServer().getPort());
      }
    }
  }

  @Override
  protected int getServerCount() {
    return 2;
  }

  @Override
  protected HAServerPlugin.SERVER_ROLE getServerRole(final int serverIndex) {
    // With Raft, leader election is automatic; all nodes start as ANY
    return HAServerPlugin.SERVER_ROLE.ANY;
  }

  @Override
  protected void waitForReplicationIsCompleted(final int serverNumber) {
    // Find the leader's last applied index, retrying briefly in case we hit a leaderless window
    // during an election transition
    long leaderLastIndex = -1;
    for (int attempt = 0; attempt < 30 && leaderLastIndex <= 0; attempt++) {
      for (int i = 0; i < getServerCount(); i++) {
        final RaftHAPlugin plugin = getRaftPlugin(i);
        if (plugin != null && plugin.isLeader()) {
          final var termIndex = plugin.getRaftHAServer().getStateMachine().getLastAppliedTermIndex();
          if (termIndex != null)
            leaderLastIndex = termIndex.getIndex();
          break;
        }
      }
      if (leaderLastIndex <= 0) {
        try {
          Thread.sleep(100);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        }
      }
    }

    if (leaderLastIndex <= 0)
      return;

    // Wait for this server's state machine to catch up to the leader's last applied index
    final RaftHAPlugin plugin = getRaftPlugin(serverNumber);
    if (plugin == null)
      return;

    final long targetIndex = leaderLastIndex;
    final long deadline = System.currentTimeMillis() + 30_000;
    while (System.currentTimeMillis() < deadline) {
      final var termIndex = plugin.getRaftHAServer().getStateMachine().getLastAppliedTermIndex();
      if (termIndex != null && termIndex.getIndex() >= targetIndex)
        return;
      try {
        Thread.sleep(100);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }
    LogManager.instance()
        .log(this, Level.WARNING, "Timeout waiting for server %d to replicate to index %d", serverNumber, targetIndex);
  }

  @Override
  protected void waitAllReplicasAreConnected() {
    // Wait for a Raft leader to be elected
    final long deadline = System.currentTimeMillis() + 30_000;
    while (System.currentTimeMillis() < deadline) {
      for (int i = 0; i < getServerCount(); i++) {
        final RaftHAPlugin plugin = getRaftPlugin(i);
        if (plugin != null && plugin.isLeader()) {
          LogManager.instance().log(this, Level.INFO, "Raft leader elected on server %d", i);
          serversSynchronized = true;
          return;
        }
      }
      try {
        Thread.sleep(500);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }
    LogManager.instance().log(this, Level.WARNING, "Timeout waiting for Raft leader election");
    // Set true to unblock test setup; individual tests will fail if no leader is actually present.
    serversSynchronized = true;
  }

  /**
   * Returns the RaftHAPlugin from the specified server, or null if not available.
   */
  protected RaftHAPlugin getRaftPlugin(final int serverIndex) {
    if (getServer(serverIndex) == null || !getServer(serverIndex).isStarted())
      return null;
    for (final ServerPlugin plugin : getServer(serverIndex).getPlugins()) {
      if (plugin instanceof RaftHAPlugin raftPlugin)
        return raftPlugin;
    }
    return null;
  }

  @Override
  protected void checkDatabasesAreIdentical() {
    // Ensure all Raft replicas have caught up before comparing pages.
    // The base endTest() calls this directly without a Raft-aware wait.
    for (int i = 0; i < getServerCount(); i++) {
      if (getServer(i) != null && getServer(i).isStarted())
        waitForReplicationIsCompleted(i);
    }
    super.checkDatabasesAreIdentical();
  }

  /**
   * Waits for every running server in the cluster to apply entries up to the current
   * leader's last-applied index. Use this after a write before reading from all servers
   * to avoid timing windows where Raft followers haven't applied the entries yet.
   */
  protected void waitForAllServers() {
    for (int i = 0; i < getServerCount(); i++) {
      if (getServer(i) != null && getServer(i).isStarted())
        waitForReplicationIsCompleted(i);
    }
  }

  /**
   * Waits for replication to propagate across the cluster, then verifies
   * that all server databases are identical.
   */
  protected void assertClusterConsistency() {
    for (int i = 0; i < getServerCount(); i++) {
      if (getServer(i) != null && getServer(i).isStarted())
        waitForReplicationIsCompleted(i);
    }
    checkDatabasesAreIdentical();
  }

  /**
   * Returns the index of the current Raft leader, or -1 if no leader is elected.
   */
  protected int findLeaderIndex() {
    for (int i = 0; i < getServerCount(); i++) {
      final RaftHAPlugin plugin = getRaftPlugin(i);
      if (plugin != null && plugin.isLeader())
        return i;
    }
    return -1;
  }

  /**
   * Stops server {@code serverIndex} then immediately restarts it using the same
   * {@link com.arcadedb.server.ArcadeDBServer} instance and configuration. Waits for replication to
   * catch up before returning.
   * <p>
   * Only valid when {@link #persistentRaftStorage()} returns true; otherwise Raft
   * storage is deleted on restart and the peer cannot rejoin the same group.
   */
  protected void restartServer(final int serverIndex) {
    if (getServer(serverIndex).isStarted()) {
      LogManager.instance().log(this, Level.INFO, "TEST: Stopping server %d for restart", serverIndex);
      getServer(serverIndex).stop();
    }

    // Brief pause to allow the OS to release the gRPC port
    try {
      Thread.sleep(2_000);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      return;
    }

    LogManager.instance().log(this, Level.INFO, "TEST: Starting server %d again", serverIndex);
    getServer(serverIndex).start();

    // Wait for the restarted peer to catch up to the current leader's last applied index
    waitForReplicationIsCompleted(serverIndex);
    LogManager.instance().log(this, Level.INFO, "TEST: Server %d restarted and caught up", serverIndex);
  }

  /**
   * Runs {@code operation} against a freshly resolved handle for server {@code serverIndex}'s database, retrying
   * with another freshly resolved handle while {@code operation} throws {@link DatabaseIsClosedException} - the
   * shape a snapshot-reinstall resync leaves when it closes and reinstalls the database out from under a handle
   * resolved (or held) before the resync ran (issue #5977).
   * <p>
   * Three independent HA ITs hit this once each: two reinvented this exact resolve-and-retry shape ad hoc
   * ({@code Issue5492TruncateBatchNotReplicatedIT}, {@code Issue5655CypherCommitsOnInnerDatabaseIT}) and a third
   * had no defense at all ({@code Issue5492SchemaWalNotShippedIT}, {@code RaftReadConsistencyBookmarkIT}). This is
   * the shared version, so a new HA IT that queries a follower shortly after inducing a resync gets it for free
   * instead of needing to know the trap exists.
   * <p>
   * {@code operation} must resolve nothing itself but the {@code Database} handed to it - it must not close over
   * a handle obtained outside this method, or the retry cannot help it. A simpler "wait for any in-flight resync
   * to finish, then resolve once" helper is not enough: the resync can start in the gap between that wait check
   * and the use that follows it, so retrying on the actual failure is the only shape that is not itself racy.
   */
  protected <T> T withResyncRetry(final int serverIndex, final Function<Database, T> operation) {
    final long deadline = System.currentTimeMillis() + RESYNC_RETRY_TIMEOUT_MS;
    while (true) {
      final Database db = getServerDatabase(serverIndex, getDatabaseName());
      try {
        return operation.apply(db);
      } catch (final DatabaseIsClosedException e) {
        if (System.currentTimeMillis() >= deadline)
          throw e;
        LogManager.instance().log(this, Level.INFO,
            "TEST: database '%s' on server %d closed mid-operation (snapshot-reinstall resync); retrying with a fresh handle",
            getDatabaseName(), serverIndex);
        try {
          Thread.sleep(250);
        } catch (final InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(ie);
        }
      }
    }
  }

  /**
   * Counts through a freshly resolved database handle. {@code count(id)} rather than {@code count(*)}: the
   * latter reads a cached per-bucket counter, the wrong tool when the question is whether the pages themselves
   * arrived. A single attempt - no retry of its own - so a {@link DatabaseIsClosedException} propagates to the
   * caller, which is what {@link #awaitCountOn} relies on to treat a resync window as "try again shortly"
   * rather than a hard failure.
   */
  protected long countOn(final int serverIndex, final String typeName) {
    final Database db = getServerDatabase(serverIndex, getDatabaseName());
    return ((Number) db.command("sql", "SELECT count(id) AS cnt FROM " + typeName).next().getProperty("cnt")).longValue();
  }

  /**
   * Polls {@code supplier} until it returns {@code expected} or the deadline passes, then returns whatever was
   * last read successfully.
   * <p>
   * On timeout it deliberately returns that last good reading rather than a sentinel, so the assertion reports
   * the state the follower is actually stuck in - {@code but was: 0L}, the write never arrived - instead of a
   * {@code -1L} that says only "the helper gave up" and hides which of the two happened. {@code -1} survives to
   * the assertion only when every single attempt threw, which is itself the distinct diagnosis: the follower
   * never became queryable at all.
   */
  protected long awaitValue(final long expected, final LongSupplier supplier) throws InterruptedException {
    final long deadline = System.currentTimeMillis() + RESYNC_RETRY_TIMEOUT_MS;
    long lastRead = -1;
    while (System.currentTimeMillis() < deadline) {
      try {
        lastRead = supplier.getAsLong();
        if (lastRead == expected)
          return lastRead;
      } catch (final RuntimeException e) {
        // Mid-resync the database is closed and being reinstalled; keep polling until the deadline. The
        // previous good reading is kept: it describes the follower better than the fact that one poll hit a
        // resync window.
      }
      Thread.sleep(250);
    }
    return lastRead;
  }

  protected long awaitCountOn(final int serverIndex, final String typeName, final long expected) throws InterruptedException {
    return awaitValue(expected, () -> countOn(serverIndex, typeName));
  }
}
