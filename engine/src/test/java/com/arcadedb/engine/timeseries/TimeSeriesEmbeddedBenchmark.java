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
package com.arcadedb.engine.timeseries;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

/**
 * Benchmark for TimeSeries ingestion using the embedded (LocalDatabase) API.
 * Uses the async API for parallel ingestion and logs metrics every second.
 * <p>
 * Run with: mvn test -pl engine -Dtest="com.arcadedb.engine.timeseries.TimeSeriesEmbeddedBenchmark#run"
 * Or as a standalone main() method.
 *
 * @Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
@Tag("benchmark")
public class TimeSeriesEmbeddedBenchmark {

  private static final String DB_PATH        = "target/databases/ts-benchmark-embedded";
  private static final int    TOTAL_POINTS   = Integer.getInteger("benchmark.totalPoints", 50_000_000);
  private static final int    BATCH_SIZE     = Integer.getInteger("benchmark.batchSize", 20_000);
  private static final int    PARALLEL_LEVEL = Integer.getInteger("benchmark.parallelLevel", 4);
  private static final int    NUM_SENSORS    = Integer.getInteger("benchmark.numSensors", 100);

  public static void main(final String[] args) throws Exception {
    new TimeSeriesEmbeddedBenchmark().run();
  }

  @Test
  public void run() throws Exception {
    // Clean up
    FileUtils.deleteRecursively(new File(DB_PATH));

    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    final Database database = factory.create();

    try {
      // Create TimeSeries type with enough shards to match the parallel level (avoids MVCC conflicts)
      database.command("sql",
          "CREATE TIMESERIES TYPE SensorData TIMESTAMP ts TAGS (sensor_id STRING) FIELDS (temperature DOUBLE, " +
              "humidity DOUBLE) SHARDS " + PARALLEL_LEVEL);

      System.out.println("=== ArcadeDB TimeSeries Embedded Benchmark ===");
      System.out.printf("Total points: %,d | Batch size: %,d | Parallel level: %d | Sensors: %d%n",
          TOTAL_POINTS, BATCH_SIZE, PARALLEL_LEVEL, NUM_SENSORS);
      System.out.println("----------------------------------------------");

      // Configure async
      database.async().setParallelLevel(PARALLEL_LEVEL);
      // Each task already writes BATCH_SIZE samples, so commit every few tasks (not every BATCH_SIZE tasks)
      database.async().setCommitEvery(5);
      database.async().setBackPressure(50);
      database.setReadYourWrites(false);

      final AtomicLong totalInserted = new AtomicLong(0);
      final AtomicLong errors = new AtomicLong(0);
      final long startTime = System.nanoTime();

      database.async().onError(exception -> {
        errors.incrementAndGet();
        LogManager.instance().log(TimeSeriesEmbeddedBenchmark.class, Level.SEVERE,
            "Async error: %s", exception, exception.getMessage());
      });

      // Start metrics reporter thread
      final Thread metricsThread = new Thread(() -> {
        long lastCount = 0;
        long lastTime = System.nanoTime();
        while (!Thread.currentThread().isInterrupted()) {
          try {
            Thread.sleep(1000);
          } catch (final InterruptedException e) {
            break;
          }
          final long now = System.nanoTime();
          final long currentCount = totalInserted.get();
          final long deltaCount = currentCount - lastCount;
          final double deltaSec = (now - lastTime) / 1_000_000_000.0;
          final double instantRate = deltaCount / deltaSec;
          final double elapsedSec = (now - startTime) / 1_000_000_000.0;
          final double avgRate = currentCount / elapsedSec;
          final double progress = (currentCount * 100.0) / TOTAL_POINTS;

          System.out.printf("[%6.1fs] Inserted: %,12d (%5.1f%%) | Instant: %,12.0f pts/s | Avg: %,12.0f pts/s | " +
                  "Errors: %d%n",
              elapsedSec, currentCount, progress, instantRate, avgRate, errors.get());

          lastCount = currentCount;
          lastTime = now;
        }
      }, "metrics-reporter");
      metricsThread.setDaemon(true);
      metricsThread.start();

      // Insert data points using async appendSamples API (handles shard routing and transactions automatically)
      final long baseTimestamp = System.currentTimeMillis() - (long) TOTAL_POINTS * 100;
      final int batchCount = TOTAL_POINTS / BATCH_SIZE;

      for (int batch = 0; batch < batchCount; batch++) {
        final long batchStart = baseTimestamp + (long) batch * BATCH_SIZE * 100;
        final long[] timestamps = new long[BATCH_SIZE];
        final Object[] sensorIds = new Object[BATCH_SIZE];
        final Object[] temperatures = new Object[BATCH_SIZE];
        final Object[] humidities = new Object[BATCH_SIZE];

        for (int i = 0; i < BATCH_SIZE; i++) {
          timestamps[i] = batchStart + i * 100L;
          sensorIds[i] = "sensor_" + (i % NUM_SENSORS);
          temperatures[i] = 20.0 + (Math.random() * 15.0);
          humidities[i] = 40.0 + (Math.random() * 40.0);
        }

        database.async().appendSamples("SensorData", timestamps, sensorIds, temperatures, humidities);
        totalInserted.addAndGet(BATCH_SIZE);
      }

      // Wait for all async operations to complete
      database.async().waitCompletion();
      final long endTime = System.nanoTime();

      // Stop metrics thread
      metricsThread.interrupt();
      metricsThread.join(2000);

      // Print final results
      final double totalSec = (endTime - startTime) / 1_000_000_000.0;
      final long finalCount = totalInserted.get();
      final double finalRate = finalCount / totalSec;

      System.out.println("==============================================");
      System.out.println("              FINAL RESULTS");
      System.out.println("==============================================");
      System.out.printf("Total points inserted: %,d%n", finalCount);
      System.out.printf("Total time:            %.2f seconds%n", totalSec);
      System.out.printf("Average throughput:    %,.0f points/second%n", finalRate);
      System.out.printf("Errors:                %d%n", errors.get());
      System.out.printf("Parallel level:        %d%n", PARALLEL_LEVEL);

      // Compact mutable data into sealed columnar storage
      System.out.println("\n--- Compaction ---");
      final long compactStart = System.nanoTime();
      ((LocalTimeSeriesType) database.getSchema().getType("SensorData")).getEngine().compactAll();
      final long compactTime = (System.nanoTime() - compactStart) / 1_000_000;
      System.out.printf("Compaction time:       %,d ms%n", compactTime);

      // Query performance test
      System.out.println("\n--- Query Performance ---");

      // Count query
      long queryStart = System.nanoTime();
      database.query("sql", "SELECT count(*) AS cnt FROM SensorData").close();
      long queryTime = (System.nanoTime() - queryStart) / 1_000_000;
      System.out.printf("COUNT(*):              %d ms%n", queryTime);

      // Range scan
      queryStart = System.nanoTime();
      final long midTs = baseTimestamp + (long) (TOTAL_POINTS / 2) * 100;
      database.query("sql", "SELECT FROM SensorData WHERE ts BETWEEN ? AND ?",
          midTs, midTs + 3_600_000L).close();
      queryTime = (System.nanoTime() - queryStart) / 1_000_000;
      System.out.printf("1h range scan:         %d ms%n", queryTime);

      // Aggregation with time bucket (full scan — may require large heap for big datasets)
      try {
        queryStart = System.nanoTime();
        database.query("sql",
            "SELECT ts.timeBucket('1h', ts) AS hour, avg(temperature) AS avg_temp, max(temperature) AS max_temp " +
                "FROM SensorData GROUP BY hour").close();
        queryTime = (System.nanoTime() - queryStart) / 1_000_000;
        System.out.printf("Hourly aggregation:    %d ms%n", queryTime);
      } catch (final Exception e) {
        System.out.printf("Hourly aggregation:    SKIPPED (%s)%n", e.getMessage());
      }

      System.out.println("==============================================");

    } finally {
      database.close();
      factory.close();
      FileUtils.deleteRecursively(new File(DB_PATH));
    }
  }
}
