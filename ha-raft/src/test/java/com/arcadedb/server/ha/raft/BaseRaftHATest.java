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
import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.function.IntPredicate;
import java.util.function.LongSupplier;
import java.util.logging.Level;

/**
 * Base class for Raft HA integration tests.
 * Configures servers to use the Raft HA implementation instead of the legacy HAServer.
 * Overrides lifecycle methods that depend on legacy HAServer APIs.
 */
public abstract class BaseRaftHATest extends BaseGraphServerTest {

  private static final int  BASE_RAFT_PORT          = 2434;
  // 15s, down from 30s and originally from 120s, and set from a measurement rather than from a suspicion
  // (issues #6267 and #6343).
  //
  // The 120s was a reaction to two CI runs (31578870619, 31587763511) in which
  // Issue5410AbandonedTicketReleaseIT stalled for the ENTIRE poll budget - once at 30s, once at 60s -
  // immediately after RaftMigrationCompactionRaceIT (a 72k-record, 24-compaction-round, 12-writer-thread IT)
  // ran in the same shared, reused JVM fork (failsafe reuseForks=true/forkCount=1 for the whole module). The
  // stalls were real, not a log-flush artifact, and never reproduced in isolation. The suspected cause was
  // ArcadeStateMachine.notifyInstallSnapshotFromLeader running its download on the JDK common ForkJoinPool,
  // shared with JDK GC/reference-handler internals - the "long-running work starves engine work" shape. That
  // caller has had its own executor since issue #6202, and #6221/#6226 added the instrument below rather than
  // guessing: a budget nothing exhausts leaves no trace of how much of it was needed.
  //
  // The measurement, over nine full ha-integration-tests runs since #6226 merged (31968696717, 31969178061,
  // 31969810563, 31972218219, 31975575222, 31977924355, 31980155942, 31980224898 and the merge run itself),
  // 235 tests each: NOT ONE wait exceeded the 10s report threshold. The corroboration is the per-class
  // elapsed time - all ten classes that call these helpers ran in every one of those runs, and the slowest
  // of them took 53s WALL CLOCK for the whole class, cluster startup and teardown included, so no single
  // wait inside it can have approached even half the old budget.
  //
  // 30s was what the rest of this class already treated as "long enough for the cluster to do anything it is
  // going to do": waitForReplicationIsCompleted, waitAllReplicasAreConnected and LEADER_ELECTION_TIMEOUT_MS
  // all use it, and matching them was the right first cut for a budget that had never been measured at all.
  //
  // The second measurement, asked for by issue #6343, is FORTY full ha-integration-tests runs on main between
  // 2026-08-18 and 2026-08-26 - every one of them after #6267's merge, 238 tests each, all 123 IT classes in
  // every run - and again NOT ONE wait reached the report threshold, now 5s rather than 10s. Sampled every
  // fifth run of the 200 the API still had logs for, so the sample spans the whole window rather than one
  // quiet stretch of it. The corroboration is sharper than last time: Issue5410AbandonedTicketReleaseIT, the
  // class whose stalls bought the original 120s, now takes 13.2-17.0s WALL CLOCK for the ENTIRE class over
  // those 40 runs - cluster startup, three-node replication and teardown included - so the wait that once
  // consumed 60s of budget cannot now be consuming even a fifth of 15s.
  //
  // 15s keeps the rule the 30s cut used, applied at the resolution the instrument now has: three times the
  // largest wait the evidence can prove any run needed (< 5s). It is no longer tied to its siblings above,
  // and should not be - those three wait for an election or for a whole cluster to converge, this one waits
  // out a single snapshot-reinstall window, and the measurement says that window is a different size. A
  // genuine hang now costs 15s instead of 30s, and 105s less than it did before any of this was measured.
  //
  // Cutting further is deliberately NOT the plan: the ratchet stops here. Below ~10s the budget stops being
  // three times a measured wait and starts being the wait itself, which converts a busy runner into a red
  // lane - the trade the instrument exists to avoid. From here the instrument's job is to catch a regression
  // rather than to propose the next cut.
  //
  // Which means reading its lines correctly, and 2.5s is low enough that they will appear: a local run of the
  // eleven helper-calling IT classes reported a wait satisfied at 3013 ms, a perfectly ordinary one that the
  // old 5s threshold simply could not see. A line is EVIDENCE ABOUT THAT WAIT, not an alarm - what would be a
  // finding is the number in it climbing toward the budget, or a GAVE UP appearing at all. That is the whole
  // reason the elapsed and the budget are both printed: the line is only worth as much as the comparison
  // between them.
  private static final long RESYNC_RETRY_TIMEOUT_MS = 15_000;
  /**
   * Above this, a wait is worth a line in the log: it is evidence about the budget above, not noise. What
   * matters is the ratio between the two, because that is the fraction of the budget a wait can consume while
   * still saying nothing - the blindness that let the 120s stand unmeasured for years - and it is not an
   * absolute: leaving this at 5s under a 15s budget would let a wait burn a THIRD of it in silence.
   *
   * <p>The history, stated accurately because it is cited as evidence: 10s of 120s (issue #6221) was a
   * twelfth; 5s of 30s (issue #6267) was a sixth, so that cut halved the resolution even as it improved the
   * budget; 2.5s of 15s (issue #6343) is a sixth again. So the rule is not "it has always been a sixth" - it
   * has been a sixth since #6267, and this cut holds it there rather than letting it slip to a third. A sixth
   * is the floor the equality in {@code SlowWaitInstrumentTest} now pins, and the twelfth is worth knowing
   * only as the reminder that a budget can be so large that even a generous threshold sees nothing.
   *
   * @see #slowWaitReport(String, long, boolean)
   */
  private static final long SLOW_WAIT_REPORT_MS     = 2_500;
  /**
   * How long {@link #findLeaderIndex()} waits for an election before answering "no leader". An election in these
   * in-process clusters settles in a second or two; the budget is for the loaded CI runner where the question
   * arrives while one is still in flight.
   */
  private static final long LEADER_ELECTION_TIMEOUT_MS = 30_000;

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

  /**
   * Ensures the replicas about to be compared have caught up, then compares them. The base {@code endTest()}
   * calls the comparison directly, without a Raft-aware wait.
   * <p>
   * The wait covers exactly {@link #getServerToCheck()} - the servers the comparison will look at - rather than
   * every configured one. The two sets differ only for a test that takes a server out of the Raft group
   * ({@code DynamicMembershipTest}): a peer that is no longer a member never applies another entry, so waiting
   * for it to reach the leader's last-applied index can only burn the full 30 s budget per evicted server and
   * then log a timeout, and comparing it can only report the divergence the eviction was asking for. Both are
   * charged to {@code endTest}, which is the wrong place to read about a peer some earlier line removed on
   * purpose (issue #6267).
   */
  @Override
  protected void checkDatabasesAreIdentical() {
    for (final int i : getServerToCheck())
      if (getServer(i) != null && getServer(i).isStarted())
        waitForReplicationIsCompleted(i);
    super.checkDatabasesAreIdentical();
  }

  /**
   * The subset of server indexes {@code keep} accepts, in index order, as the {@code int[]}
   * {@link #getServerToCheck()} is declared to return. The one place that turns a per-server predicate into that
   * array, so an override does not have to hand-roll the two-pass count-then-fill each time.
   */
  protected int[] serversMatching(final IntPredicate keep) {
    final int count = getServerCount();
    final int[] buffer = new int[count];
    int found = 0;
    for (int i = 0; i < count; i++)
      if (keep.test(i))
        buffer[found++] = i;
    return found == count ? buffer : Arrays.copyOf(buffer, found);
  }

  /**
   * The servers that are currently running. The default set for a test that deliberately stops one: a stopped
   * server has no database to compare, and nothing to wait for.
   */
  protected int[] startedServers() {
    return serversMatching(i -> getServer(i) != null && getServer(i).isStarted());
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
   * Returns the index of the current Raft leader, waiting up to {@link #LEADER_ELECTION_TIMEOUT_MS} for one, or
   * -1 if none is elected within it.
   * <p>
   * It used to sample once and return -1 the instant it was called during a leaderless window - between the
   * cluster starting and the first election, or across a term change. Nearly every caller then asserts
   * {@code isGreaterThanOrEqualTo(0)} with "a Raft leader must be elected", which on a loaded machine fails in a
   * different test method on every run: not a leaderless cluster, a question asked half a second early. Seven
   * call sites had already wrapped it in an {@code await().until(() -> findLeaderIndex() >= 0)} of their own,
   * one test at a time; waiting here is that workaround made general.
   * <p>
   * No caller asserts a negative result - none of the ~100 tests that use it expects a leaderless cluster - so
   * waiting changes no test's meaning, only how long the answer takes when the answer is "not yet". A cluster
   * that genuinely has no leader still costs the full budget once, at the end of which the caller fails exactly
   * as it did before.
   */
  protected int findLeaderIndex() {
    final long deadline = System.currentTimeMillis() + LEADER_ELECTION_TIMEOUT_MS;
    while (true) {
      for (int i = 0; i < getServerCount(); i++) {
        final RaftHAPlugin plugin = getRaftPlugin(i);
        if (plugin != null && plugin.isLeader())
          return i;
      }
      if (System.currentTimeMillis() >= deadline)
        return -1;
      try {
        Thread.sleep(100);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        return -1;
      }
    }
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
    final long start = System.currentTimeMillis();
    final long deadline = start + RESYNC_RETRY_TIMEOUT_MS;
    while (true) {
      final Database db = getServerDatabase(serverIndex, getDatabaseName());
      try {
        final T result = operation.apply(db);
        reportSlowWait("withResyncRetry on server " + serverIndex, start, true);
        return result;
      } catch (final DatabaseIsClosedException e) {
        if (System.currentTimeMillis() >= deadline) {
          reportSlowWait("withResyncRetry on server " + serverIndex, start, false);
          throw e;
        }
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
    final long start = System.currentTimeMillis();
    final long deadline = start + RESYNC_RETRY_TIMEOUT_MS;
    long lastRead = -1;
    while (System.currentTimeMillis() < deadline) {
      try {
        lastRead = supplier.getAsLong();
        if (lastRead == expected) {
          reportSlowWait("awaitValue(" + expected + ")", start, true);
          return lastRead;
        }
      } catch (final RuntimeException e) {
        // Mid-resync the database is closed and being reinstalled; keep polling until the deadline. The
        // previous good reading is kept: it describes the follower better than the fact that one poll hit a
        // resync window.
      }
      Thread.sleep(250);
    }
    reportSlowWait("awaitValue(" + expected + ")", start, false);
    return lastRead;
  }

  protected long awaitCountOn(final int serverIndex, final String typeName, final long expected) throws InterruptedException {
    return awaitValue(expected, () -> countOn(serverIndex, typeName));
  }

  /**
   * The one fixed token every slow-wait report carries, so that collecting this instrument's evidence out of a
   * CI log is a grep for a literal string rather than for a sentence somebody may since have reworded.
   * <p>
   * It is here because the absence of it cost issue #6343 a false reading. That issue asked, in as many words,
   * for "the {@code SLOW WAIT} lines" from the runs on main - and there were none, in forty full runs. The
   * conclusion that no wait had been slow happened to be the true one, but the grep could not have said
   * otherwise: no line this instrument has ever emitted contained the string {@code SLOW WAIT}. A report
   * nobody can search for by name is a report that reads as silence, and silence is exactly the answer the
   * evidence-gathering step is trying to distinguish a finding from. The marker makes the two distinguishable
   * again, and {@code SlowWaitInstrumentTest} is what keeps them that way.
   */
  static final String SLOW_WAIT_MARKER = "TEST SLOW WAIT:";

  /**
   * Builds the line {@link #reportSlowWait} logs, or returns {@code null} when the wait was not slow enough to
   * be worth one. Split out from the logging so the threshold decision and the wording of the report can be
   * asserted by a plain unit test without standing a three-node cluster up to provoke a slow wait - which is
   * what a test would otherwise have to do, and could not do reliably.
   * <p>
   * A budget that is never exhausted leaves no evidence of how much of it was needed, which is exactly why the
   * old 120 s stood on a suspicion rather than on a measurement (issue #6221): a wait that satisfies in 300 ms
   * and one that satisfies at 95 s are indistinguishable in a green run. Nine runs of silence took it to 30 s
   * (issue #6267) and forty more took it to 15 s (issue #6343). From here the instrument's job changes from
   * proposing cuts to catching regressions: see {@link #RESYNC_RETRY_TIMEOUT_MS}.
   * <p>
   * Logged, not asserted: a slow wait is not a failure, and a test that failed the moment a CI runner was busy
   * would be a worse trade than the timeout it was meant to justify.
   */
  static String slowWaitReport(final String what, final long elapsedMs, final boolean satisfied) {
    if (elapsedMs < SLOW_WAIT_REPORT_MS)
      return null;
    // The issue number, and nothing else editorial: what a line means belongs in the javadoc on
    // RESYNC_RETRY_TIMEOUT_MS, which is where somebody reading one will end up anyway, and not repeated in every
    // occurrence of it in a CI log. Everything here is load-bearing for a grep or for the reader of a single
    // line - the marker, which wait, how long, and out of how much.
    return "%s %s %s after %d ms of the %d ms budget (issue #6343)".formatted(
        SLOW_WAIT_MARKER, what, satisfied ? "satisfied" : "GAVE UP", elapsedMs, RESYNC_RETRY_TIMEOUT_MS);
  }

  private void reportSlowWait(final String what, final long startMs, final boolean satisfied) {
    logSlowWait(this, what, System.currentTimeMillis() - startMs, satisfied);
  }

  /**
   * Emits the report, if there is one to emit. Takes the requester rather than reading {@code this} so that
   * {@code SlowWaitInstrumentTest} can drive the real logging path - the one that decides whether the marker
   * survives into a CI log - without standing up the three-node cluster it would otherwise take to provoke a
   * slow wait.
   */
  static void logSlowWait(final Object requester, final String what, final long elapsedMs, final boolean satisfied) {
    final String report = slowWaitReport(what, elapsedMs, satisfied);
    if (report != null)
      // "%s" with the report as the argument, not the report as the format string: `what` is caller-supplied
      // and a stray % in it would turn the instrument into a formatting error instead of a report.
      LogManager.instance().log(requester, Level.WARNING, "%s", report);
  }
}
