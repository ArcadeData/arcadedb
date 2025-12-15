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
import org.awaitility.Awaitility;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Benchmark for measuring leader failover time.
 * Measures the time from leader death to new leader election and cluster recovery.
 */
public class FailoverTimeBenchmark extends ContainersTestTemplate {

  private static final int WARMUP_ITERATIONS = 3;
  private static final int BENCHMARK_ITERATIONS = 10;

  @Test
  @DisplayName("Benchmark: Leader failover time with 3-node cluster")
  void benchmarkLeaderFailoverTime() throws IOException, InterruptedException {
    logger.info("=== Starting Leader Failover Time Benchmark ===");

    // Create proxies for 3-node cluster
    final Proxy arcade1Proxy = toxiproxyClient.createProxy("arcade1Proxy", "0.0.0.0:8666", "arcade1:2424");
    final Proxy arcade2Proxy = toxiproxyClient.createProxy("arcade2Proxy", "0.0.0.0:8667", "arcade2:2424");
    final Proxy arcade3Proxy = toxiproxyClient.createProxy("arcade3Proxy", "0.0.0.0:8668", "arcade3:2424");

    // Create 3-node HA cluster with majority quorum
    GenericContainer<?> arcade1 = createArcadeContainer("arcade1", "{arcade2}proxy:8667,{arcade3}proxy:8668", "majority", "any",
        network);
    GenericContainer<?> arcade2 = createArcadeContainer("arcade2", "{arcade1}proxy:8666,{arcade3}proxy:8668", "majority", "any",
        network);
    GenericContainer<?> arcade3 = createArcadeContainer("arcade3", "{arcade1}proxy:8666,{arcade2}proxy:8667", "majority", "any",
        network);

    logger.info("Starting cluster - arcade1 will become leader");
    List<ServerWrapper> servers = startContainers();

    DatabaseWrapper db1 = new DatabaseWrapper(servers.get(0), idSupplier);
    DatabaseWrapper db2 = new DatabaseWrapper(servers.get(1), idSupplier);
    DatabaseWrapper db3 = new DatabaseWrapper(servers.get(2), idSupplier);

    try {
      logger.info("Creating database and schema");
      db1.createDatabase();
      db1.createSchema();
      db1.addUserAndPhotos(10, 5);

      // Warmup phase
      logger.info("Warmup phase: {} iterations", WARMUP_ITERATIONS);
      for (int i = 0; i < WARMUP_ITERATIONS; i++) {
        performFailoverCycle(arcade1, arcade1Proxy, db2, false);
        TimeUnit.SECONDS.sleep(5); // Allow cluster to stabilize
      }

      logger.info("Warmup complete. Starting benchmark...");

      // Benchmark phase
      long totalFailoverTime = 0;
      long minFailoverTime = Long.MAX_VALUE;
      long maxFailoverTime = 0;

      for (int i = 0; i < BENCHMARK_ITERATIONS; i++) {
        logger.info("Benchmark iteration {}/{}", i + 1, BENCHMARK_ITERATIONS);

        long failoverTime = performFailoverCycle(arcade1, arcade1Proxy, db2, true);

        totalFailoverTime += failoverTime;
        minFailoverTime = Math.min(minFailoverTime, failoverTime);
        maxFailoverTime = Math.max(maxFailoverTime, failoverTime);

        logger.info("Iteration {} failover time: {} ms", i + 1, failoverTime);

        // Allow cluster to stabilize between iterations
        TimeUnit.SECONDS.sleep(5);
      }

      // Calculate statistics
      double avgFailoverTime = totalFailoverTime / (double) BENCHMARK_ITERATIONS;

      // Print results
      logger.info("=== Benchmark Results: Leader Failover Time ===");
      logger.info("Cluster configuration: 3 nodes, majority quorum");
      logger.info("Benchmark iterations: {}", BENCHMARK_ITERATIONS);
      logger.info("Average failover time: {} ms", String.format("%.2f", avgFailoverTime));
      logger.info("Min failover time: {} ms", minFailoverTime);
      logger.info("Max failover time: {} ms", maxFailoverTime);
      logger.info("Failover time range: {} ms", maxFailoverTime - minFailoverTime);
      logger.info("===============================================");

    } finally {
      db1.close();
      db2.close();
      db3.close();
    }
  }

  /**
   * Performs a single failover cycle: kill leader, measure time until cluster recovers
   *
   * @param leaderContainer  The container running the leader
   * @param leaderProxy      The Toxiproxy proxy for the leader
   * @param replicaDb        A database connection to a replica node
   * @param measureTime      Whether to measure and return the failover time
   * @return The failover time in milliseconds (or 0 if measureTime is false)
   */
  private long performFailoverCycle(
      GenericContainer<?> leaderContainer,
      Proxy leaderProxy,
      DatabaseWrapper replicaDb,
      boolean measureTime) throws InterruptedException {

    final AtomicLong failoverStartTime = new AtomicLong();
    final AtomicLong failoverEndTime = new AtomicLong();
    final AtomicBoolean failoverDetected = new AtomicBoolean(false);

    // Kill the leader
    if (measureTime) {
      failoverStartTime.set(System.currentTimeMillis());
    }

    logger.info("Killing leader node");
    leaderContainer.stop();

    // Wait for failover and new leader election
    logger.info("Waiting for failover to complete");
    Awaitility.await()
        .atMost(60, TimeUnit.SECONDS)
        .pollInterval(100, TimeUnit.MILLISECONDS)
        .until(() -> {
          try {
            // Try to write to the replica - if successful, new leader is elected
            replicaDb.addUserAndPhotos(1, 1);

            if (measureTime && !failoverDetected.get()) {
              failoverEndTime.set(System.currentTimeMillis());
              failoverDetected.set(true);
            }

            return true;
          } catch (Exception e) {
            return false;
          }
        });

    // Restart the stopped node
    logger.info("Restarting stopped node");
    leaderContainer.start();
    TimeUnit.SECONDS.sleep(10); // Allow node to rejoin cluster

    if (measureTime) {
      return failoverEndTime.get() - failoverStartTime.get();
    }

    return 0;
  }

  @Test
  @DisplayName("Benchmark: Failover time under load")
  void benchmarkFailoverTimeUnderLoad() throws IOException, InterruptedException {
    logger.info("=== Starting Failover Time Under Load Benchmark ===");

    // Create proxies for 3-node cluster
    final Proxy arcade1Proxy = toxiproxyClient.createProxy("arcade1Proxy", "0.0.0.0:8666", "arcade1:2424");
    final Proxy arcade2Proxy = toxiproxyClient.createProxy("arcade2Proxy", "0.0.0.0:8667", "arcade2:2424");
    final Proxy arcade3Proxy = toxiproxyClient.createProxy("arcade3Proxy", "0.0.0.0:8668", "arcade3:2424");

    // Create 3-node HA cluster
    GenericContainer<?> arcade1 = createArcadeContainer("arcade1", "{arcade2}proxy:8667,{arcade3}proxy:8668", "majority", "any",
        network);
    GenericContainer<?> arcade2 = createArcadeContainer("arcade2", "{arcade1}proxy:8666,{arcade3}proxy:8668", "majority", "any",
        network);
    GenericContainer<?> arcade3 = createArcadeContainer("arcade3", "{arcade1}proxy:8666,{arcade2}proxy:8667", "majority", "any",
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

      // Start background load thread
      final AtomicBoolean stopLoad = new AtomicBoolean(false);
      final Thread loadThread = new Thread(() -> {
        while (!stopLoad.get()) {
          try {
            db2.addUserAndPhotos(1, 2);
            TimeUnit.MILLISECONDS.sleep(100);
          } catch (Exception e) {
            // Expected during failover
          }
        }
      });
      loadThread.start();

      // Let load run for a bit
      TimeUnit.SECONDS.sleep(5);

      // Measure failover time under load
      final long startTime = System.currentTimeMillis();
      logger.info("Killing leader while under load");
      db1.close();
      arcade1.stop();

      // Wait for writes to succeed on new leader
      Awaitility.await()
          .atMost(60, TimeUnit.SECONDS)
          .pollInterval(100, TimeUnit.MILLISECONDS)
          .until(() -> {
            try {
              db3.addUserAndPhotos(1, 1);
              return true;
            } catch (Exception e) {
              return false;
            }
          });

      final long failoverTime = System.currentTimeMillis() - startTime;

      // Stop background load
      stopLoad.set(true);
      loadThread.join();

      logger.info("=== Benchmark Results: Failover Under Load ===");
      logger.info("Failover time under continuous write load: {} ms", failoverTime);
      logger.info("==============================================");

    } finally {
      db2.close();
      db3.close();
    }
  }
}
