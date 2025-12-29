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
package com.arcadedb.containers.performance;

import com.arcadedb.test.support.ContainersTestTemplate;
import com.arcadedb.test.support.DatabaseWrapper;
import com.arcadedb.test.support.ServerWrapper;
import eu.rekawek.toxiproxy.Proxy;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Benchmark for measuring replication throughput with various quorum settings.
 * Tests transaction throughput (tx/sec) under different HA configurations.
 */
class ReplicationThroughputBenchmarkIT extends ContainersTestTemplate {

  private static final int WARMUP_TRANSACTIONS = 100;
  private static final int BENCHMARK_TRANSACTIONS = 1000;
  private static final int PHOTOS_PER_USER = 10;

  @Test
  @DisplayName("Benchmark: Replication throughput with MAJORITY quorum")
  void benchmarkReplicationThroughputMajorityQuorum() throws IOException {
    runThroughputBenchmark("majority", "Majority Quorum");
  }

  @Test
  @DisplayName("Benchmark: Replication throughput with ALL quorum")
  void benchmarkReplicationThroughputAllQuorum() throws IOException {
    runThroughputBenchmark("all", "All Quorum");
  }

  @Test
  @DisplayName("Benchmark: Replication throughput with NONE quorum")
  void benchmarkReplicationThroughputNoneQuorum() throws IOException {
    runThroughputBenchmark("none", "None Quorum");
  }

  private void runThroughputBenchmark(String quorum, String description) throws IOException {
    logger.info("=== Starting Replication Throughput Benchmark: {} ===", description);

    // Create proxies for 3-node cluster
    final Proxy arcade1Proxy = toxiproxyClient.createProxy("arcade1Proxy", "0.0.0.0:8666", "arcade1:2424");
    final Proxy arcade2Proxy = toxiproxyClient.createProxy("arcade2Proxy", "0.0.0.0:8667", "arcade2:2424");
    final Proxy arcade3Proxy = toxiproxyClient.createProxy("arcade3Proxy", "0.0.0.0:8668", "arcade3:2424");

    // Create 3-node HA cluster with specified quorum
    GenericContainer<?> arcade1 = createArcadeContainer("arcade1", "{arcade2}proxy:8667,{arcade3}proxy:8668", quorum, "any",
        network);
    GenericContainer<?> arcade2 = createArcadeContainer("arcade2", "{arcade1}proxy:8666,{arcade3}proxy:8668", quorum, "any",
        network);
    GenericContainer<?> arcade3 = createArcadeContainer("arcade3", "{arcade1}proxy:8666,{arcade2}proxy:8667", quorum, "any",
        network);

    logger.info("Starting cluster");
    List<ServerWrapper> servers = startContainers();

    DatabaseWrapper db1 = new DatabaseWrapper(servers.get(0), idSupplier);
    DatabaseWrapper db2 = new DatabaseWrapper(servers.get(1), idSupplier);
    DatabaseWrapper db3 = new DatabaseWrapper(servers.get(2), idSupplier);

    try {
      logger.info("Creating database and schema");
      db1.createDatabase();
      db1.createSchema();

      // Warmup phase
      logger.info("Warmup phase: {} transactions", WARMUP_TRANSACTIONS);
      for (int i = 0; i < WARMUP_TRANSACTIONS; i++) {
        db1.addUserAndPhotos(1, PHOTOS_PER_USER);
      }

      logger.info("Warmup complete. Starting benchmark...");

      // Benchmark phase
      final long startTime = System.nanoTime();
      final AtomicInteger completedTx = new AtomicInteger(0);
      final AtomicInteger failedTx = new AtomicInteger(0);

      for (int i = 0; i < BENCHMARK_TRANSACTIONS; i++) {
        try {
          db1.addUserAndPhotos(1, PHOTOS_PER_USER);
          completedTx.incrementAndGet();
        } catch (Exception e) {
          failedTx.incrementAndGet();
          logger.warn("Transaction failed: {}", e.getMessage());
        }

        // Print progress every 100 transactions
        if ((i + 1) % 100 == 0) {
          logger.info("Progress: {}/{} transactions", i + 1, BENCHMARK_TRANSACTIONS);
        }
      }

      final long endTime = System.nanoTime();
      final long durationMs = TimeUnit.NANOSECONDS.toMillis(endTime - startTime);
      final double durationSec = durationMs / 1000.0;
      final double txPerSec = completedTx.get() / durationSec;

      // Wait for replication to complete
      logger.info("Waiting for replication to complete...");
      TimeUnit.SECONDS.sleep(5);

      // Verify replication
      final long db1Count = db1.countUsers();
      final long db2Count = db2.countUsers();
      final long db3Count = db3.countUsers();

      // Print results
      logger.info("=== Benchmark Results: {} ===", description);
      logger.info("Quorum: {}", quorum);
      logger.info("Total transactions: {}", BENCHMARK_TRANSACTIONS);
      logger.info("Completed transactions: {}", completedTx.get());
      logger.info("Failed transactions: {}", failedTx.get());
      logger.info("Duration: {} ms ({} sec)", durationMs, String.format("%.2f", durationSec));
      logger.info("Throughput: {}", String.format("%.2f tx/sec", txPerSec));
      logger.info("Average latency: {} ms/tx", String.format("%.2f", durationMs / (double) completedTx.get()));
      logger.info("Replication verification: db1={}, db2={}, db3={}", db1Count, db2Count, db3Count);
      logger.info("===========================================");

    } catch (InterruptedException e) {
      logger.error("Benchmark interrupted", e);
      Thread.currentThread().interrupt();
    } finally {
      db1.close();
      db2.close();
      db3.close();
    }
  }

  // Note: To compare throughput across all quorum settings, run the individual
  // benchmark tests separately and compare their results.
  // Running multiple cluster lifecycles in a single test method is not supported
  // due to test framework lifecycle constraints.
}
