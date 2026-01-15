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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.async.AsyncResultsetCallback;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONObject;

import org.junit.jupiter.api.Test;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.logging.*;

import static org.assertj.core.api.Assertions.*;

/**
 * Test for GitHub Issue #3122: Async commands should run in parallel.
 * https://github.com/ArcadeData/arcadedb/issues/3122
 */
class Issue3122AsyncParallelCommandsIT extends BaseGraphServerTest {

  private static final String DATABASE_NAME  = "Issue3122AsyncParallelCommands";
  private static final int    SLEEP_DURATION = 2000; // 2 seconds per sleep command

  @Override
  protected String getDatabaseName() {
    return DATABASE_NAME;
  }

  /**
   * Test that two async commands sent via HTTP run in parallel, not sequentially.
   * If they run in parallel, total time should be ~SLEEP_DURATION.
   * If they run sequentially, total time would be ~2*SLEEP_DURATION.
   */
  @Test
  void testHttpAsyncCommandsRunInParallel() throws Exception {
    testEachServer((serverIndex) -> {
      // Verify the server has at least 2 async worker threads by default (fix for issue #3122)
      final Database database = getServer(0).getDatabase(getDatabaseName());
      final int parallelLevel = database.async().getParallelLevel();
      LogManager.instance().log(this, Level.INFO, "Async parallel level: %d", parallelLevel);
      assertThat(parallelLevel)
          .as("Server should have at least 2 async worker threads by default")
          .isGreaterThanOrEqualTo(2);

      final long startTime = System.currentTimeMillis();

      // Send two async SLEEP commands concurrently
      final ExecutorService executor = Executors.newFixedThreadPool(2);
      final List<Future<Integer>> futures = new ArrayList<>();

      for (int i = 0; i < 2; i++) {
        final int cmdNum = i + 1;
        futures.add(executor.submit(() -> {
          final HttpURLConnection connection = (HttpURLConnection) new URL(
              "http://localhost:248" + serverIndex + "/api/v1/command/" + DATABASE_NAME).openConnection();

          connection.setRequestMethod("POST");
          connection.setRequestProperty("Authorization",
              "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));

          // Use sqlscript with SLEEP command as per the issue example
          formatPayload(connection, new JSONObject()
              .put("language", "sqlscript")
              .put("command", "SLEEP " + SLEEP_DURATION + "; CONSOLE.log 'Command " + cmdNum + " completed'")
              .put("awaitResponse", false));
          connection.connect();

          try {
            final int responseCode = connection.getResponseCode();
            LogManager.instance().log(this, Level.INFO, "Async command %d sent, response code: %d", cmdNum, responseCode);
            return responseCode;
          } finally {
            connection.disconnect();
          }
        }));
      }

      // Wait for both HTTP requests to return (they should return immediately with 202)
      for (Future<Integer> future : futures) {
        final int responseCode = future.get(5, TimeUnit.SECONDS);
        assertThat(responseCode).isEqualTo(202);
      }

      executor.shutdown();

      // Now wait for the async commands to actually complete
      database.async().waitCompletion(SLEEP_DURATION * 3);

      final long endTime = System.currentTimeMillis();
      final long totalTime = endTime - startTime;

      LogManager.instance().log(this, Level.INFO,
          "Total time for 2 async SLEEP commands (%d ms each): %d ms", SLEEP_DURATION, totalTime);

      // If running in parallel, total time should be close to SLEEP_DURATION
      // If running sequentially, total time would be close to 2*SLEEP_DURATION
      // We use 1.5x as the threshold - parallel should be well under this
      final long maxExpectedParallelTime = (long) (SLEEP_DURATION * 1.5);

      assertThat(totalTime)
          .as("Async commands should run in parallel. Total time: %d ms, expected max: %d ms", totalTime, maxExpectedParallelTime)
          .isLessThan(maxExpectedParallelTime);
    });
  }

  /**
   * Test that two async commands via the database.async() API run in parallel.
   */
  @Test
  void testDatabaseAsyncCommandsRunInParallel() throws Exception {
    final Database database = getServer(0).getDatabase(getDatabaseName());

    // Ensure we have at least 2 async worker threads
    final int originalParallelLevel = database.async().getParallelLevel();
    if (originalParallelLevel < 2) {
      database.async().setParallelLevel(2);
    }

    try {
      final long startTime = System.currentTimeMillis();
      final AtomicInteger completedCount = new AtomicInteger(0);
      final CountDownLatch latch = new CountDownLatch(2);

      // Send two async SLEEP commands
      for (int i = 0; i < 2; i++) {
        final int cmdNum = i + 1;
        database.async().command("sqlscript",
            "SLEEP " + SLEEP_DURATION + "; CONSOLE.log 'Database async command " + cmdNum + " completed'",
            new AsyncResultsetCallback() {
              @Override
              public void onComplete(final ResultSet resultset) {
                completedCount.incrementAndGet();
                latch.countDown();
                LogManager.instance().log(this, Level.INFO, "Database async command %d completed", cmdNum);
              }

              @Override
              public void onError(final Exception exception) {
                latch.countDown();
                LogManager.instance().log(this, Level.SEVERE, "Database async command %d failed", exception, cmdNum);
              }
            });
      }

      // Wait for both commands to complete
      final boolean completed = latch.await(SLEEP_DURATION * 3, TimeUnit.MILLISECONDS);
      assertThat(completed).as("Both async commands should complete within timeout").isTrue();
      assertThat(completedCount.get()).isEqualTo(2);

      final long endTime = System.currentTimeMillis();
      final long totalTime = endTime - startTime;

      LogManager.instance().log(this, Level.INFO,
          "Total time for 2 database.async() SLEEP commands (%d ms each): %d ms", SLEEP_DURATION, totalTime);

      // If running in parallel, total time should be close to SLEEP_DURATION
      final long maxExpectedParallelTime = (long) (SLEEP_DURATION * 1.5);

      assertThat(totalTime)
          .as("Database async commands should run in parallel. Total time: %d ms, expected max: %d ms", totalTime,
              maxExpectedParallelTime)
          .isLessThan(maxExpectedParallelTime);

    } finally {
      database.async().setParallelLevel(originalParallelLevel);
    }
  }

  /**
   * Test that simple SQL SLEEP commands (not sqlscript) also run in parallel.
   */
  @Test
  void testSimpleSqlAsyncCommandsRunInParallel() throws Exception {
    final Database database = getServer(0).getDatabase(getDatabaseName());

    // Ensure we have at least 2 async worker threads
    final int originalParallelLevel = database.async().getParallelLevel();
    if (originalParallelLevel < 2) {
      database.async().setParallelLevel(2);
    }

    try {
      final long startTime = System.currentTimeMillis();
      final AtomicInteger completedCount = new AtomicInteger(0);
      final CountDownLatch latch = new CountDownLatch(2);

      // Send two async SLEEP commands using plain SQL
      for (int i = 0; i < 2; i++) {
        final int cmdNum = i + 1;
        database.async().command("sql",
            "SLEEP " + SLEEP_DURATION,
            new AsyncResultsetCallback() {
              @Override
              public void onComplete(final ResultSet resultset) {
                completedCount.incrementAndGet();
                latch.countDown();
                LogManager.instance().log(this, Level.INFO, "SQL async command %d completed", cmdNum);
              }

              @Override
              public void onError(final Exception exception) {
                latch.countDown();
                LogManager.instance().log(this, Level.SEVERE, "SQL async command %d failed", exception, cmdNum);
              }
            });
      }

      // Wait for both commands to complete
      final boolean completed = latch.await(SLEEP_DURATION * 3, TimeUnit.MILLISECONDS);
      assertThat(completed).as("Both SQL async commands should complete within timeout").isTrue();
      assertThat(completedCount.get()).isEqualTo(2);

      final long endTime = System.currentTimeMillis();
      final long totalTime = endTime - startTime;

      LogManager.instance().log(this, Level.INFO,
          "Total time for 2 SQL async SLEEP commands (%d ms each): %d ms", SLEEP_DURATION, totalTime);

      // If running in parallel, total time should be close to SLEEP_DURATION
      final long maxExpectedParallelTime = (long) (SLEEP_DURATION * 1.5);

      assertThat(totalTime)
          .as("SQL async commands should run in parallel. Total time: %d ms, expected max: %d ms", totalTime,
              maxExpectedParallelTime)
          .isLessThan(maxExpectedParallelTime);

    } finally {
      database.async().setParallelLevel(originalParallelLevel);
    }
  }

  @Override
  protected void populateDatabase() {
    // No data population needed for this test
  }
}
