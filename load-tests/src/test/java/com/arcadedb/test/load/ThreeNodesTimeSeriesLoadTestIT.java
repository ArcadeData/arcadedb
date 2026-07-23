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
package com.arcadedb.test.load;

import com.arcadedb.remote.RemoteException;
import com.arcadedb.test.support.ContainersTestTemplate;
import com.arcadedb.test.support.ServerWrapper;
import com.arcadedb.test.support.TimeSeriesDatabaseWrapper;
import com.arcadedb.test.support.TimeSeriesDatabaseWrapper.Protocol;
import io.micrometer.core.instrument.Metrics;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class ThreeNodesTimeSeriesLoadTestIT extends ContainersTestTemplate {

  private static final String SERVER_LIST       = "arcadedb-0:2434:2480,arcadedb-1:2434:2480,arcadedb-2:2434:2480";
  private static final int    NUM_THREADS       = 3;
  private static final int    POINTS_PER_THREAD = 10000;

  @AfterEach
  @Override
  public void tearDown() {
    // Skip compareAllDatabases(): with non-persistent containers, database files are not on the
    // host after stop. The test body already verifies convergence via Awaitility.
    super.tearDown();
  }

  @ParameterizedTest(name = "Three-node Raft HA TS load test with {0} ingestion")
  @EnumSource(TimeSeriesDatabaseWrapper.Protocol.class)
  @DisplayName("Three-node Raft HA: time series replication consistency")
  void threeNodeTimeSeriesReplication(final TimeSeriesDatabaseWrapper.Protocol protocol) throws Exception {
    createArcadeContainer("arcadedb-0", SERVER_LIST, "majority", network);
    createArcadeContainer("arcadedb-1", SERVER_LIST, "majority", network);
    createArcadeContainer("arcadedb-2", SERVER_LIST, "majority", network);

    logger.info("Starting all containers");
    final List<ServerWrapper> servers = startCluster();

    // Ingest wrapper (parameter protocol) targets node 0 and writes auto-forward to the leader;
    // reader/asserter wrappers always use HTTP, one per node (mirrors ThreeNodesLoadTestIT).
    // try-with-resources so all four connections close even if an assertion below fails.
    try (final TimeSeriesDatabaseWrapper ingest = new TimeSeriesDatabaseWrapper(servers.getFirst(), protocol);
        final TimeSeriesDatabaseWrapper r0 = new TimeSeriesDatabaseWrapper(servers.getFirst(), Protocol.SQL_HTTP);
        final TimeSeriesDatabaseWrapper r1 = new TimeSeriesDatabaseWrapper(servers.get(1), Protocol.SQL_HTTP);
        final TimeSeriesDatabaseWrapper r2 = new TimeSeriesDatabaseWrapper(servers.get(2), Protocol.SQL_HTTP)) {

      logger.info("Creating database and schema");
      ingest.createDatabase();
      ingest.createSchema();

      logger.info("Checking schema replicated to all nodes");
      r0.checkSchema();
      r1.checkSchema();
      r2.checkSchema();

      final int bulkPoints = NUM_THREADS * POINTS_PER_THREAD;
      final long expectedTotal = bulkPoints + 4L;

      final LocalDateTime startedAt = LocalDateTime.now();
      logger.info("Starting TS HA load test on protocol {}", protocol.name());
      logger.info("Ingesting {} bulk points using {} threads (+4 verification points)", bulkPoints, NUM_THREADS);
      logger.info("Starting at {}", DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(startedAt));

      final ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
      for (int t = 0; t < NUM_THREADS; t++) {
        final int threadIndex = t;
        executor.submit(() -> {
          final TimeSeriesDatabaseWrapper w = new TimeSeriesDatabaseWrapper(servers.getFirst(), protocol);
          try {
            final long base = 1_000_000_000L + threadIndex * 100_000_000L;
            w.ingestSeries("sensor-" + threadIndex, "region-" + (threadIndex % 3), base, POINTS_PER_THREAD);
          } finally {
            w.close();
          }
        });
      }
      executor.shutdown();

      while (!executor.isTerminated()) {
        try {
          logger.info("Points n0/n1/n2: {} / {} / {}", r0.countPoints(), r1.countPoints(), r2.countPoints());
        } catch (final Exception e) {
          logger.error(e.getMessage(), e);
        }
        try {
          TimeUnit.SECONDS.sleep(5);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }

      // Deterministic verification series, ingested after the bulk load completes.
      ingest.ingestVerificationSeries();

      final LocalDateTime finishedAt = LocalDateTime.now();
      logger.info("Finishing at {}", DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(finishedAt));
      logger.info("Total time: {} minutes", Duration.between(startedAt, finishedAt).toMinutes());

      // Wait for all three nodes to converge to the expected total.
      Awaitility.await()
          .atMost(2, TimeUnit.MINUTES)
          .pollInterval(5, TimeUnit.SECONDS)
          .until(() -> {
            try {
              final long c0 = r0.countPoints();
              final long c1 = r1.countPoints();
              final long c2 = r2.countPoints();
              logger.info("Convergence check: {} / {} / {} (expected {})", c0, c1, c2, expectedTotal);
              return c0 == expectedTotal &&
                  c1 == expectedTotal &&
                  c2 == expectedTotal;
            } catch (final RemoteException e) {
              logger.debug("Convergence check transient failure: {}", e.getMessage());
              return false;
            } catch (final RuntimeException e) {
              logger.warn("Convergence check threw unexpected exception, will keep polling", e);
              return false;
            }
          });

      Metrics.globalRegistry.getMeters().forEach(meter -> logger.info("Meter: {} - {}", meter.getId().getName(), meter.measure()));

      // Assert exact count + query correctness on each node. The exact total (mutable + sealed) is
      // verified via assertThatPointCountIs. Aggregation (a tag-filtered query, WHERE sensor_id =
      // 'sensor-0') reflects only the asynchronously-sealed-and-replicated subset, so its exact count is
      // NOT deterministic; only the temperature extremes 15.0/34.0 are (the cycle 15.0..34.0 settles
      // once a full cycle is sealed). The /latest endpoint reads the mutable buffer, so latest(verify)
      // is deterministic once the verify points have replicated (guaranteed by the convergence wait).
      for (final TimeSeriesDatabaseWrapper r : List.of(r0, r1, r2)) {
        r.assertThatPointCountIs(expectedTotal);

        r.assertAggregateExtremes("sensor-0", 15.0, 34.0);

        assertThat(r.latestTimestamp("sensor_id:" + TimeSeriesDatabaseWrapper.VERIFY_SENSOR)).isEqualTo(3000L);
      }
    }
  }
}
