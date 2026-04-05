/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
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
import com.arcadedb.log.LogManager;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerPlugin;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.TestServerHelper;
import com.arcadedb.utility.CodeUtils;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

/**
 * Benchmark measuring insert performance across different Raft HA configurations.
 * Compares: single server (no HA), 3-server Raft cluster, and 5-server Raft cluster.
 * <p>
 * Run with: {@code mvn test -pl ha-raft -Dtest=RaftHAInsertBenchmark -Dtag=benchmark}
 */
@Tag("benchmark")
public class RaftHAInsertBenchmark {

  private static final int    BASE_RAFT_PORT = 2434;
  private static final int    BASE_HTTP_PORT = 2480;
  private static final String DB_NAME        = "benchdb";
  private static final String VERTEX_TYPE    = "Sensor";
  private static final int    WARMUP_COUNT   = 500;
  private static final int    MEASURE_COUNT  = 5000;
  private static final int    ASYNC_COUNT    = 100_000;
  private static final int    TX_BATCH_SIZE  = 100;
  private static final int    ASYNC_THREADS  = 8;
  private static final String ROOT_PASSWORD  = BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS;

  @Test
  void runBenchmark() throws Exception {
    final StringBuilder report = new StringBuilder();
    report.append("\n");
    report.append("=".repeat(90)).append("\n");
    report.append("  ArcadeDB Raft HA Insert Benchmark\n");
    report.append("=".repeat(90)).append("\n");
    report.append(String.format("  Sync:  %,d records (batch %d/tx)  |  Async: %,d records (%d threads)%n",
        MEASURE_COUNT, TX_BATCH_SIZE, ASYNC_COUNT, ASYNC_THREADS));
    report.append("=".repeat(90)).append("\n\n");

    // Sync scenarios
    report.append(runSingleServerBaseline());
    report.append(runClusterEmbeddedOnLeader(3));
    report.append(runClusterEmbeddedOnLeader(5));
    report.append(runClusterViaFollower(3));
    report.append(runClusterConcurrentDistributed(3));
    report.append(runClusterConcurrentDistributed(5));

    // Async scenarios
    report.append(runSingleServerAsync());
    report.append(runClusterAsyncOnLeader(3));
    report.append(runClusterAsyncOnLeader(5));

    report.append("=".repeat(90)).append("\n");
    LogManager.instance().log(this, Level.WARNING, report.toString());
  }

  // ---------------------------------------------------------------------------
  //  Schema initialization (common for all scenarios)
  // ---------------------------------------------------------------------------

  private void initSchema(final Database db) {
    // Use multiple buckets so async threads distribute records across slots
    db.command("SQL", "CREATE VERTEX TYPE " + VERTEX_TYPE + " IF NOT EXISTS BUCKETS " + ASYNC_THREADS);
    db.command("SQL", "CREATE PROPERTY " + VERTEX_TYPE + ".sensorId IF NOT EXISTS LONG");
    db.command("SQL", "CREATE PROPERTY " + VERTEX_TYPE + ".value IF NOT EXISTS DOUBLE");
    db.command("SQL", "CREATE PROPERTY " + VERTEX_TYPE + ".timestamp IF NOT EXISTS LONG");
  }

  // ---------------------------------------------------------------------------
  //  Scenario: Single server, no HA
  // ---------------------------------------------------------------------------

  private String runSingleServerBaseline() throws Exception {
    cleanUp(1);
    createDatabase(0);

    final ArcadeDBServer server = new ArcadeDBServer(serverConfig(0, false, 1));
    server.start();

    try {
      final Database db = server.getDatabase(DB_NAME);
      initSchema(db);

      insertEmbedded(db, WARMUP_COUNT, 0);
      final long[] latencies = insertEmbedded(db, MEASURE_COUNT, WARMUP_COUNT);
      return formatResult("1 server (no HA) - embedded", latencies, 1);
    } finally {
      server.stop();
    }
  }

  // ---------------------------------------------------------------------------
  //  Scenario: Raft cluster, embedded writes on leader
  // ---------------------------------------------------------------------------

  private String runClusterEmbeddedOnLeader(final int serverCount) throws Exception {
    final ArcadeDBServer[] servers = startCluster(serverCount);
    try {
      final ArcadeDBServer leader = findLeader(servers);
      final Database db = leader.getDatabase(DB_NAME);
      initSchema(db);
      Thread.sleep(2000);

      insertEmbedded(db, WARMUP_COUNT, 0);
      final long[] latencies = insertEmbedded(db, MEASURE_COUNT, WARMUP_COUNT);
      return formatResult(serverCount + " servers (Raft HA) - embedded on leader", latencies, 1);
    } finally {
      stopCluster(servers);
    }
  }

  // ---------------------------------------------------------------------------
  //  Scenario: Raft cluster, RemoteDatabase writes to a follower (HTTP forwarding)
  // ---------------------------------------------------------------------------

  private String runClusterViaFollower(final int serverCount) throws Exception {
    final ArcadeDBServer[] servers = startCluster(serverCount);
    try {
      final ArcadeDBServer leader = findLeader(servers);
      initSchema(leader.getDatabase(DB_NAME));
      Thread.sleep(2000);

      int followerPort = -1;
      for (final ArcadeDBServer s : servers) {
        final RaftHAPlugin plugin = getRaftPlugin(s);
        if (plugin != null && !plugin.isLeader()) {
          followerPort = s.getHttpServer().getPort();
          break;
        }
      }

      try (final RemoteDatabase db = new RemoteDatabase("127.0.0.1", followerPort, DB_NAME, "root", ROOT_PASSWORD)) {
        insertRemote(db, WARMUP_COUNT, 0);
        final long[] latencies = insertRemote(db, MEASURE_COUNT, WARMUP_COUNT);
        return formatResult(serverCount + " servers (Raft HA) - remote via follower proxy", latencies, 1);
      }
    } finally {
      stopCluster(servers);
    }
  }

  // ---------------------------------------------------------------------------
  //  Scenario: Raft cluster, concurrent writes from all servers via RemoteDatabase
  // ---------------------------------------------------------------------------

  private String runClusterConcurrentDistributed(final int serverCount) throws Exception {
    final ArcadeDBServer[] servers = startCluster(serverCount);
    try {
      final ArcadeDBServer leader = findLeader(servers);
      initSchema(leader.getDatabase(DB_NAME));
      Thread.sleep(2000);

      try (final RemoteDatabase db = new RemoteDatabase("127.0.0.1", leader.getHttpServer().getPort(),
          DB_NAME, "root", ROOT_PASSWORD)) {
        insertRemote(db, WARMUP_COUNT, 0);
      }

      final int perThread = MEASURE_COUNT / serverCount;
      final List<long[]> allLatencies = new ArrayList<>();
      final CountDownLatch startLatch = new CountDownLatch(1);
      final CountDownLatch doneLatch = new CountDownLatch(serverCount);
      final AtomicLong offset = new AtomicLong(WARMUP_COUNT + MEASURE_COUNT);

      for (int i = 0; i < serverCount; i++) {
        final int port = servers[i].getHttpServer().getPort();
        final long base = offset.getAndAdd(perThread);
        new Thread(() -> {
          try (final RemoteDatabase db = new RemoteDatabase("127.0.0.1", port, DB_NAME, "root", ROOT_PASSWORD)) {
            startLatch.await();
            final long[] latencies = insertRemote(db, perThread, base);
            synchronized (allLatencies) { allLatencies.add(latencies); }
          } catch (final Exception e) {
            LogManager.instance().log(this, Level.WARNING, "Concurrent write error: %s", e.getMessage());
          } finally { doneLatch.countDown(); }
        }, "bench-writer-" + i).start();
      }

      final long concurrentStart = System.nanoTime();
      startLatch.countDown();
      doneLatch.await();
      final long concurrentElapsed = System.nanoTime() - concurrentStart;

      int totalOps = 0;
      for (final long[] l : allLatencies) totalOps += l.length;
      final long[] merged = new long[totalOps];
      int pos = 0;
      for (final long[] l : allLatencies) { System.arraycopy(l, 0, merged, pos, l.length); pos += l.length; }

      return formatResult(serverCount + " servers (Raft HA) - concurrent (" + serverCount + " threads)",
          merged, serverCount, concurrentElapsed);
    } finally {
      stopCluster(servers);
    }
  }

  // ---------------------------------------------------------------------------
  //  Scenario: Single server, async inserts (no HA)
  // ---------------------------------------------------------------------------

  private String runSingleServerAsync() throws Exception {
    cleanUp(1);
    createDatabase(0);

    final ArcadeDBServer server = new ArcadeDBServer(serverConfig(0, false, 1));
    server.start();

    try {
      final Database db = server.getDatabase(DB_NAME);
      initSchema(db);

      insertAsync(db, WARMUP_COUNT, 0);

      final long start = System.nanoTime();
      insertAsync(db, ASYNC_COUNT, WARMUP_COUNT);
      final long elapsed = System.nanoTime() - start;
      return formatAsyncResult("1 server (no HA) - async", ASYNC_COUNT, elapsed);
    } finally {
      server.stop();
    }
  }

  // ---------------------------------------------------------------------------
  //  Scenario: Raft cluster, async inserts on leader
  // ---------------------------------------------------------------------------

  private String runClusterAsyncOnLeader(final int serverCount) throws Exception {
    final ArcadeDBServer[] servers = startCluster(serverCount);
    try {
      final ArcadeDBServer leader = findLeader(servers);
      final Database db = leader.getDatabase(DB_NAME);
      initSchema(db);
      Thread.sleep(2000);

      insertAsync(db, WARMUP_COUNT, 0);

      final long start = System.nanoTime();
      insertAsync(db, ASYNC_COUNT, WARMUP_COUNT);
      final long elapsed = System.nanoTime() - start;
      return formatAsyncResult(serverCount + " servers (Raft HA) - async on leader", ASYNC_COUNT, elapsed);
    } finally {
      stopCluster(servers);
    }
  }

  // ---------------------------------------------------------------------------
  //  Insert methods
  // ---------------------------------------------------------------------------

  private long[] insertEmbedded(final Database db, final int count, final long startId) {
    final long[] latencies = new long[count / TX_BATCH_SIZE];
    int latIdx = 0;

    for (int batch = 0; batch < count; batch += TX_BATCH_SIZE) {
      final int bStart = batch;
      final int bEnd = Math.min(batch + TX_BATCH_SIZE, count);
      final long txStart = System.nanoTime();

      db.transaction(() -> {
        for (int i = bStart; i < bEnd; i++)
          db.newVertex(VERTEX_TYPE)
              .set("sensorId", startId + i)
              .set("value", Math.random() * 1000)
              .set("timestamp", System.currentTimeMillis())
              .save();
      });

      if (latIdx < latencies.length)
        latencies[latIdx++] = System.nanoTime() - txStart;
    }
    return Arrays.copyOf(latencies, latIdx);
  }

  private long[] insertRemote(final RemoteDatabase db, final int count, final long startId) {
    final long[] latencies = new long[count];
    for (int i = 0; i < count; i++) {
      final long start = System.nanoTime();
      db.command("SQL", "INSERT INTO " + VERTEX_TYPE + " SET sensorId = ?, value = ?, timestamp = ?",
          startId + i, Math.random() * 1000, System.currentTimeMillis());
      latencies[i] = System.nanoTime() - start;
    }
    return latencies;
  }

  /**
   * Inserts records using database.async() with multiple buckets to distribute across threads.
   */
  private void insertAsync(final Database db, final int count, final long startId) {
    GlobalConfiguration.ASYNC_OPERATIONS_QUEUE_SIZE.setValue(16384);

    final var async = db.async();
    async.setParallelLevel(ASYNC_THREADS);
    async.setCommitEvery(5_000);
    async.setBackPressure(80);

    final AtomicLong errors = new AtomicLong(0);
    async.onError(e -> errors.incrementAndGet());

    for (int i = 0; i < count; i++)
      async.createRecord(
          db.newVertex(VERTEX_TYPE)
              .set("sensorId", startId + i)
              .set("value", Math.random() * 1000)
              .set("timestamp", System.currentTimeMillis()),
          null);

    async.waitCompletion(120_000);

    if (errors.get() > 0)
      LogManager.instance().log(this, Level.WARNING, "Async insert: %d errors out of %d", errors.get(), count);
  }

  // ---------------------------------------------------------------------------
  //  Raft cluster lifecycle
  // ---------------------------------------------------------------------------

  private ArcadeDBServer[] startCluster(final int serverCount) throws Exception {
    cleanUp(serverCount);
    createDatabase(0);

    for (int i = 1; i < serverCount; i++) {
      final File src = new File("./target/databases0/" + DB_NAME);
      final File dst = new File("./target/databases" + i + "/" + DB_NAME);
      if (!dst.exists()) {
        dst.getParentFile().mkdirs();
        FileUtils.copyDirectory(src, dst);
      }
    }

    final ArcadeDBServer[] servers = new ArcadeDBServer[serverCount];
    for (int i = 0; i < serverCount; i++) {
      servers[i] = new ArcadeDBServer(serverConfig(i, true, serverCount));
      servers[i].start();
    }

    final long deadline = System.currentTimeMillis() + 30_000;
    while (System.currentTimeMillis() < deadline) {
      for (final ArcadeDBServer s : servers) {
        final RaftHAPlugin plugin = getRaftPlugin(s);
        if (plugin != null && plugin.isLeader())
          return servers;
      }
      Thread.sleep(500);
    }
    throw new RuntimeException("Raft leader election timed out");
  }

  private void stopCluster(final ArcadeDBServer[] servers) {
    for (final ArcadeDBServer s : servers)
      if (s != null && s.isStarted())
        s.stop();
    GlobalConfiguration.resetAll();
  }

  private ArcadeDBServer findLeader(final ArcadeDBServer[] servers) {
    for (final ArcadeDBServer s : servers) {
      final RaftHAPlugin plugin = getRaftPlugin(s);
      if (plugin != null && plugin.isLeader())
        return s;
    }
    throw new RuntimeException("No Raft leader found");
  }

  private RaftHAPlugin getRaftPlugin(final ArcadeDBServer server) {
    for (final ServerPlugin plugin : server.getPlugins())
      if (plugin instanceof RaftHAPlugin raftPlugin)
        return raftPlugin;
    return null;
  }

  private ContextConfiguration serverConfig(final int index, final boolean haEnabled, final int serverCount) {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_NAME, "ArcadeDB_" + index);
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, "./target/databases" + index);
    config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, "./target");
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, ROOT_PASSWORD);
    config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_HOST, "localhost");
    config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_PORT, String.valueOf(BASE_HTTP_PORT + index));
    config.setValue(GlobalConfiguration.HA_ENABLED, haEnabled);

    if (haEnabled) {
      config.setValue(GlobalConfiguration.HA_IMPLEMENTATION, "raft");
      config.setValue(GlobalConfiguration.HA_CLUSTER_NAME, "bench-cluster");
      config.setValue(GlobalConfiguration.HA_RAFT_PORT, BASE_RAFT_PORT + index);
      config.setValue(GlobalConfiguration.HA_QUORUM, "MAJORITY");

      // Three-part format: host:raftPort:httpPort (enables follower HTTP forwarding)
      final StringBuilder serverList = new StringBuilder();
      for (int i = 0; i < serverCount; i++) {
        if (i > 0) serverList.append(",");
        serverList.append("localhost:").append(BASE_RAFT_PORT + i).append(":").append(BASE_HTTP_PORT + i);
      }
      config.setValue(GlobalConfiguration.HA_SERVER_LIST, serverList.toString());
    }
    return config;
  }

  private void createDatabase(final int index) {
    GlobalConfiguration.SERVER_DATABASE_DIRECTORY.setValue("./target/databases" + index);
    try (final Database db = new DatabaseFactory("./target/databases" + index + "/" + DB_NAME).create()) {
      // Empty database, schema created later
    }
  }

  private void cleanUp(final int serverCount) {
    TestServerHelper.checkActiveDatabases();
    GlobalConfiguration.resetAll();
    // Allow the OS time to release ports from the previous scenario before binding new ones
    CodeUtils.sleep(1_000);
    for (int i = 0; i < serverCount; i++) {
      FileUtils.deleteRecursively(new File("./target/databases" + i));
      FileUtils.deleteRecursively(new File("./target/raft-storage-peer-" + i));
    }
    new File("./target/config/server-users.jsonl").delete();
  }

  // ---------------------------------------------------------------------------
  //  Reporting
  // ---------------------------------------------------------------------------

  private String formatResult(final String scenario, final long[] latenciesNs, final int threads) {
    return formatResult(scenario, latenciesNs, threads, -1);
  }

  private String formatResult(final String scenario, final long[] latenciesNs, final int threads,
      final long overallElapsedNs) {
    Arrays.sort(latenciesNs);
    final int n = latenciesNs.length;
    if (n == 0) return scenario + ": NO DATA\n\n";

    final long totalNs = Arrays.stream(latenciesNs).sum();
    final double avgUs = (totalNs / (double) n) / 1_000.0;
    final double minUs = latenciesNs[0] / 1_000.0;
    final double maxUs = latenciesNs[n - 1] / 1_000.0;
    final double p99Us = latenciesNs[(int) (n * 0.99)] / 1_000.0;
    final double p95Us = latenciesNs[(int) (n * 0.95)] / 1_000.0;
    final double medianUs = latenciesNs[n / 2] / 1_000.0;
    final double elapsedSec = overallElapsedNs > 0 ? overallElapsedNs / 1_000_000_000.0 : totalNs / 1_000_000_000.0;
    final double opsPerSec = n / elapsedSec;

    final StringBuilder sb = new StringBuilder();
    sb.append(String.format("  %-55s%n", scenario));
    sb.append(String.format("  %-55s%n", "-".repeat(55)));
    sb.append(String.format("    Ops:        %,d operations (%d thread%s)%n", n, threads, threads > 1 ? "s" : ""));
    sb.append(String.format("    Throughput: %,.0f ops/sec%n", opsPerSec));
    sb.append(String.format("    Avg:        %,.0f us  |  Median: %,.0f us%n", avgUs, medianUs));
    sb.append(String.format("    Min:        %,.0f us  |  P95:    %,.0f us%n", minUs, p95Us));
    sb.append(String.format("    P99:        %,.0f us  |  Max:    %,.0f us%n", p99Us, maxUs));
    sb.append("\n");
    return sb.toString();
  }

  private String formatAsyncResult(final String scenario, final int count, final long elapsedNs) {
    final double elapsedSec = elapsedNs / 1_000_000_000.0;
    final double opsPerSec = count / elapsedSec;

    final StringBuilder sb = new StringBuilder();
    sb.append(String.format("  %-55s%n", scenario));
    sb.append(String.format("  %-55s%n", "-".repeat(55)));
    sb.append(String.format("    Ops:        %,d records (%d async threads, commitEvery=5000)%n", count, ASYNC_THREADS));
    sb.append(String.format("    Throughput: %,.0f inserts/sec%n", opsPerSec));
    sb.append(String.format("    Elapsed:    %.1f seconds%n", elapsedSec));
    sb.append("\n");
    return sb.toString();
  }
}
