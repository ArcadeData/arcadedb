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
package com.arcadedb.server;

import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Benchmark for TimeSeries ingestion via the HTTP endpoint (InfluxDB Line Protocol).
 * Starts an ArcadeDB server in-process and measures throughput.
 *
 * Run with: mvn test -pl server -Dtest="com.arcadedb.server.TimeSeriesHttpBenchmark#run"
 */
class TimeSeriesHttpBenchmark extends BaseGraphServerTest {

  private static final int TOTAL_POINTS    = Integer.getInteger("benchmark.totalPoints", 1_000_000);
  private static final int BATCH_SIZE      = Integer.getInteger("benchmark.batchSize", 5_000);
  private static final int HTTP_THREADS    = Integer.getInteger("benchmark.httpThreads", 8);
  private static final int NUM_SENSORS     = Integer.getInteger("benchmark.numSensors", 100);

  @Test
  void run() throws Exception {
    testEachServer((serverIndex) -> {
      // Create TimeSeries type
      command(serverIndex,
          "CREATE TIMESERIES TYPE SensorData TIMESTAMP ts TAGS (sensor_id STRING) FIELDS (temperature DOUBLE, humidity DOUBLE)");

      System.out.println("=== ArcadeDB TimeSeries HTTP Benchmark ===");
      System.out.printf("Total points: %,d | Batch size: %,d | HTTP threads: %d | Sensors: %d%n",
          TOTAL_POINTS, BATCH_SIZE, HTTP_THREADS, NUM_SENSORS);
      System.out.println("-------------------------------------------");

      final AtomicLong totalInserted = new AtomicLong(0);
      final AtomicLong totalRequests = new AtomicLong(0);
      final AtomicLong totalErrors = new AtomicLong(0);
      final AtomicLong totalLatencyNs = new AtomicLong(0);
      final long startTime = System.nanoTime();

      // Start metrics reporter
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
          final long reqs = totalRequests.get();
          final long deltaCount = currentCount - lastCount;
          final double deltaSec = (now - lastTime) / 1_000_000_000.0;
          final double instantRate = deltaCount / deltaSec;
          final double elapsedSec = (now - startTime) / 1_000_000_000.0;
          final double avgRate = currentCount / Math.max(elapsedSec, 0.001);
          final double progress = (currentCount * 100.0) / TOTAL_POINTS;
          final double avgLatencyMs = reqs > 0 ? (totalLatencyNs.get() / reqs) / 1_000_000.0 : 0;

          System.out.printf(
              "[%6.1fs] Inserted: %,12d (%5.1f%%) | Instant: %,12.0f pts/s | Avg: %,12.0f pts/s | Reqs: %,d | AvgLatency: %.1f ms | Errors: %d%n",
              elapsedSec, currentCount, progress, instantRate, avgRate, reqs, avgLatencyMs, totalErrors.get());

          lastCount = currentCount;
          lastTime = now;
        }
      }, "metrics-reporter");
      metricsThread.setDaemon(true);
      metricsThread.start();

      // Prepare batches and send via HTTP threads
      final int batchCount = TOTAL_POINTS / BATCH_SIZE;
      final ExecutorService executor = Executors.newFixedThreadPool(HTTP_THREADS);
      final CountDownLatch latch = new CountDownLatch(batchCount);
      final long baseTimestamp = System.currentTimeMillis() - (long) TOTAL_POINTS * 100;

      for (int batch = 0; batch < batchCount; batch++) {
        final int batchIdx = batch;
        executor.submit(() -> {
          try {
            // Build line protocol batch
            final StringBuilder sb = new StringBuilder(BATCH_SIZE * 80);
            final long batchStart = baseTimestamp + (long) batchIdx * BATCH_SIZE * 100;
            for (int i = 0; i < BATCH_SIZE; i++) {
              final long ts = batchStart + i * 100L;
              final String sensorId = "sensor_" + (i % NUM_SENSORS);
              final double temperature = 20.0 + (Math.random() * 15.0);
              final double humidity = 40.0 + (Math.random() * 40.0);
              sb.append("SensorData,sensor_id=").append(sensorId)
                  .append(" temperature=").append(temperature)
                  .append(",humidity=").append(humidity)
                  .append(' ').append(ts)
                  .append('\n');
            }

            final long reqStart = System.nanoTime();
            final int statusCode = postLineProtocol(serverIndex, sb.toString(), "ms");
            final long reqDuration = System.nanoTime() - reqStart;

            totalLatencyNs.addAndGet(reqDuration);
            totalRequests.incrementAndGet();

            if (statusCode == 204 || statusCode == 200)
              totalInserted.addAndGet(BATCH_SIZE);
            else
              totalErrors.incrementAndGet();

          } catch (final Exception e) {
            totalErrors.incrementAndGet();
          } finally {
            latch.countDown();
          }
        });
      }

      latch.await();
      executor.shutdown();
      final long endTime = System.nanoTime();

      // Stop metrics thread
      metricsThread.interrupt();
      metricsThread.join(2000);

      // Final results
      final double totalSec = (endTime - startTime) / 1_000_000_000.0;
      final long finalCount = totalInserted.get();
      final double finalRate = finalCount / totalSec;
      final long reqs = totalRequests.get();
      final double avgLatencyMs = reqs > 0 ? (totalLatencyNs.get() / reqs) / 1_000_000.0 : 0;

      System.out.println("===========================================");
      System.out.println("              FINAL RESULTS");
      System.out.println("===========================================");
      System.out.printf("Total points inserted: %,d%n", finalCount);
      System.out.printf("Total time:            %.2f seconds%n", totalSec);
      System.out.printf("Average throughput:    %,.0f points/second%n", finalRate);
      System.out.printf("Total HTTP requests:   %,d%n", reqs);
      System.out.printf("Avg request latency:   %.1f ms%n", avgLatencyMs);
      System.out.printf("Avg req throughput:    %,.0f req/second%n", reqs / totalSec);
      System.out.printf("HTTP threads:          %d%n", HTTP_THREADS);
      System.out.printf("Batch size:            %,d points/request%n", BATCH_SIZE);
      System.out.printf("Errors:                %d%n", totalErrors.get());
      System.out.println("===========================================");
    });
  }

  private int postLineProtocol(final int serverIndex, final String body, final String precision) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(
        "http://127.0.0.1:248" + serverIndex + "/api/v1/ts/graph/write?precision=" + precision)
        .toURL()
        .openConnection();

    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    connection.setRequestProperty("Content-Type", "text/plain");
    connection.setDoOutput(true);
    connection.setConnectTimeout(30_000);
    connection.setReadTimeout(60_000);

    try (final OutputStream os = connection.getOutputStream()) {
      os.write(body.getBytes(StandardCharsets.UTF_8));
      os.flush();
    }

    return connection.getResponseCode();
  }

  @Override
  protected boolean isCreateDatabases() {
    return true;
  }
}
