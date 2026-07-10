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

import com.arcadedb.test.support.ContainersTestTemplate;
import com.arcadedb.test.support.ServerWrapper;
import com.arcadedb.test.support.TimeSeriesDatabaseWrapper;

import io.micrometer.core.instrument.Metrics;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class SingleServerTimeSeriesLoadTestIT extends ContainersTestTemplate {

  private static final int NUM_THREADS       = 5;
  private static final int POINTS_PER_THREAD = 10000;

  @DisplayName("Single server time series load test")
  @ParameterizedTest(name = "TS load test with {0} ingestion")
  @EnumSource(TimeSeriesDatabaseWrapper.Protocol.class)
  void singleServerTimeSeriesLoadTest(final TimeSeriesDatabaseWrapper.Protocol protocol) throws Exception {
    createArcadeContainer("arcade", network);
    final ServerWrapper server = startContainers().getFirst();

    // try-with-resources so the admin connection is closed even if an assertion below fails.
    try (final TimeSeriesDatabaseWrapper admin = new TimeSeriesDatabaseWrapper(server, protocol)) {
      admin.createDatabase();
      admin.createSchema();

      final int bulkPoints = NUM_THREADS * POINTS_PER_THREAD;
      final long expectedTotal = bulkPoints + 4L; // + verification series

      final LocalDateTime startedAt = LocalDateTime.now();
      logger.info("Starting TS load test on protocol {}", protocol.name());
      logger.info("Ingesting {} bulk points using {} threads (+4 verification points)", bulkPoints, NUM_THREADS);
      logger.info("Starting at {}", DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(startedAt));

      final ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
      for (int t = 0; t < NUM_THREADS; t++) {
        final int threadIndex = t;
        executor.submit(() -> {
          try (final TimeSeriesDatabaseWrapper w = new TimeSeriesDatabaseWrapper(server, protocol)) {
            final long base = 1_000_000_000L + threadIndex * 100_000_000L;
            w.ingestSeries("sensor-" + threadIndex, "region-" + (threadIndex % 3), base, POINTS_PER_THREAD);
          }
        });
      }
      executor.shutdown();

      while (!executor.isTerminated()) {
        try {
          logger.info("Current points: {}", admin.countPoints());
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
      admin.ingestVerificationSeries();

      final LocalDateTime finishedAt = LocalDateTime.now();
      logger.info("Finishing at {}", DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(finishedAt));
      logger.info("Total time: {} minutes", Duration.between(startedAt, finishedAt).toMinutes());

      Metrics.globalRegistry.getMeters().forEach(meter -> logger.info("Meter: {} - {}", meter.getId().getName(), meter.measure()));

      // Exact total count.
      admin.assertThatPointCountIs(expectedTotal);

      // Aggregation correctness over a sealed bulk sensor (sensor-0). This is also a tag-filtered query
      // (WHERE sensor_id = 'sensor-0'). temperature = 15.0 + (i % 20) cycles 15.0..34.0 every 20 points,
      // so the time-bucket min/max settle at 15.0/34.0. The aggregation (and any tag-filtered query)
      // reflects only the asynchronously-sealed subset, so the exact aggregated count/avg are not
      // deterministic; extremes are asserted here and the exact total via assertThatPointCountIs.
      admin.assertAggregateExtremes("sensor-0", 15.0, 34.0);

      // Latest value (tag-filtered) over the verification series. The /latest endpoint reads the
      // mutable buffer too, so it sees the just-written points immediately.
      assertThat(admin.latestTimestamp("sensor_id:" + TimeSeriesDatabaseWrapper.VERIFY_SENSOR)).isEqualTo(3000L);
    }
  }
}
