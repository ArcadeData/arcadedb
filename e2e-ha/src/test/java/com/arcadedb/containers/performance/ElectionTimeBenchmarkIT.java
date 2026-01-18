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
import java.util.concurrent.atomic.AtomicLong;

/**
 * Benchmark for measuring leader election time.
 * Measures the time taken for leader election in various scenarios.
 */
public class ElectionTimeBenchmarkIT extends ContainersTestTemplate {

  private static final int WARMUP_ITERATIONS    = 3;
  private static final int BENCHMARK_ITERATIONS = 10;

  @Test
  @DisplayName("Benchmark: Leader election time in 3-node cluster")
  void benchmarkLeaderElectionTime() throws IOException, InterruptedException {
    logger.info("=== Starting Leader Election Time Benchmark ===");
    logger.info("Note: This benchmark measures a single election cycle.");
    logger.info("For multiple iterations, run this test multiple times.");

    long electionTime = performElectionCycle(true);

    // Print results
    logger.info("=== Benchmark Results: Leader Election Time ===");
    logger.info("Cluster configuration: 3 nodes, majority quorum");
    logger.info("Election time: {} ms", electionTime);
    logger.info("===============================================");
  }

  /**
   * Performs a single election cycle: start cluster, measure time until leader is elected
   *
   * @param measureTime Whether to measure and return the election time
   *
   * @return The election time in milliseconds (or 0 if measureTime is false)
   */
  private long performElectionCycle(boolean measureTime) throws IOException, InterruptedException {
    final AtomicLong electionStartTime = new AtomicLong();
    final AtomicLong electionEndTime = new AtomicLong();

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

    if (measureTime) {
      electionStartTime.set(System.currentTimeMillis());
    }

    logger.info("Starting cluster - election will occur");
    List<ServerWrapper> servers = startContainers();

    DatabaseWrapper db1 = new DatabaseWrapper(servers.get(0), idSupplier);

    try {
      // Wait for leader election by attempting to create a database
      Awaitility.await()
          .atMost(60, TimeUnit.SECONDS)
          .pollInterval(100, TimeUnit.MILLISECONDS)
          .until(() -> {
            try {
              db1.createDatabase();
              if (measureTime) {
                electionEndTime.set(System.currentTimeMillis());
              }
              return true;
            } catch (Exception e) {
              return false;
            }
          });

      if (measureTime) {
        return electionEndTime.get() - electionStartTime.get();
      }

    } finally {
      db1.close();
    }

    return 0;
  }

  @Test
  @DisplayName("Benchmark: Election time with 2-node cluster")
  void benchmarkTwoNodeElectionTime() throws IOException, InterruptedException {
    logger.info("=== Benchmarking 2-Node Cluster Election Time ===");

    long electionTime = benchmarkTwoNodeElection();

    logger.info("=== Benchmark Results: 2-Node Election Time ===");
    logger.info("2-node cluster election time: {} ms", electionTime);
    logger.info("===============================================");
  }

  private long benchmarkElectionForClusterSize(int clusterSize) throws IOException, InterruptedException {
    if (clusterSize == 2) {
      return benchmarkTwoNodeElection();
    } else if (clusterSize == 3) {
      return benchmarkThreeNodeElection();
    }
    throw new IllegalArgumentException("Unsupported cluster size: " + clusterSize);
  }

  private long benchmarkTwoNodeElection() throws IOException, InterruptedException {
    final AtomicLong electionTime = new AtomicLong();

    // Create proxies for 2-node cluster
    final Proxy arcade1Proxy = toxiproxyClient.createProxy("arcade1Proxy", "0.0.0.0:8666", "arcade1:2424");
    final Proxy arcade2Proxy = toxiproxyClient.createProxy("arcade2Proxy", "0.0.0.0:8667", "arcade2:2424");

    // Create 2-node HA cluster
    GenericContainer<?> arcade1 = createArcadeContainer("arcade1", "{arcade2}proxy:8667", "majority", "any", network);
    GenericContainer<?> arcade2 = createArcadeContainer("arcade2", "{arcade1}proxy:8666", "majority", "any", network);

    final long startTime = System.currentTimeMillis();

    logger.info("Starting 2-node cluster");
    List<ServerWrapper> servers = startContainers();

    DatabaseWrapper db1 = new DatabaseWrapper(servers.get(0), idSupplier);

    try {
      // Wait for leader election
      Awaitility.await()
          .atMost(60, TimeUnit.SECONDS)
          .pollInterval(100, TimeUnit.MILLISECONDS)
          .until(() -> {
            try {
              db1.createDatabase();
              electionTime.set(System.currentTimeMillis() - startTime);
              return true;
            } catch (Exception e) {
              return false;
            }
          });

    } finally {
      db1.close();
    }

    return electionTime.get();
  }

  private long benchmarkThreeNodeElection() throws IOException, InterruptedException {
    return performElectionCycle(true);
  }

  @Test
  @DisplayName("Benchmark: Re-election time after leader failure")
  void benchmarkReElectionTime() throws IOException, InterruptedException {
    logger.info("=== Starting Re-Election Time Benchmark ===");

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

    try {
      logger.info("Creating database - initial election");
      db1.createDatabase();
      db1.createSchema();

      // Measure re-election time
      final long startTime = System.currentTimeMillis();

      logger.info("Killing current leader");
      db1.close();
      arcade1.stop();

      // Wait for re-election by attempting write
      Awaitility.await()
          .atMost(60, TimeUnit.SECONDS)
          .pollInterval(100, TimeUnit.MILLISECONDS)
          .until(() -> {
            try {
              db2.addUserAndPhotos(1, 1);
              return true;
            } catch (Exception e) {
              return false;
            }
          });

      final long reElectionTime = System.currentTimeMillis() - startTime;

      logger.info("=== Benchmark Results: Re-Election Time ===");
      logger.info("Re-election time after leader failure: {} ms", reElectionTime);
      logger.info("===========================================");

    } finally {
      db2.close();
    }
  }
}
