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

import com.arcadedb.database.Database;
import com.arcadedb.database.async.AsyncResultsetCallback;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONObject;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.function.IntFunction;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for GitHub Issue #3122: Async commands should run in parallel.
 * https://github.com/ArcadeData/arcadedb/issues/3122
 * <p>
 * None of the assertions below compares elapsed time against a fixed constant. Doing so made the class a
 * CI flake (issue #5630): a run that takes 7 s alone on a developer machine took 26.8 s on a loaded shared
 * runner, nine times over a 3 s budget, while running perfectly in parallel the whole time. What separates
 * parallel from sequential execution is not how long the work took but how the two commands were spaced,
 * so that is what is asserted:
 * <ul>
 *   <li>with a completion callback, the two commands must finish <em>less than one SLEEP apart</em> - run
 *       sequentially the second cannot even start until the first is done, so its completion is at least
 *       {@code SLEEP_DURATION} later no matter how loaded the machine is;</li>
 *   <li>without one (the HTTP route), a single-command baseline is measured back to back with the
 *       concurrent pair, and the pair must cost well under twice the baseline. What makes the ratio hold
 *       where an absolute budget does not is that the dominant term in both measurements is the same
 *       server-side SLEEP - a wall-clock wait, so largely load-independent - leaving the parallel ratio
 *       near 1.0 and the sequential one near 2.0 whatever the runner is doing.</li>
 * </ul>
 * <p>
 * Every method here waits out at least one real 2 s SLEEP, and the HTTP one waits out two (baseline then
 * pair), so the class is tagged slow per the repository convention for multi-second regression tests.
 */
@Tag("slow")
class Issue3122AsyncParallelCommandsIT extends BaseGraphServerTest {

  private static final String DATABASE_NAME  = "Issue3122AsyncParallelCommands";
  private static final int    SLEEP_DURATION = 2000; // 2 seconds per sleep command

  /**
   * Liveness guards only, never performance assertions: they exist so a wedged async executor fails the
   * test instead of hanging it. Deliberately far above any plausible loaded-CI duration.
   */
  private static final long COMPLETION_TIMEOUT_MS      = 120_000;
  private static final long HTTP_RESPONSE_TIMEOUT_SECS = 60;

  /**
   * A pair of commands executed sequentially costs about twice the single-command baseline; executed in
   * parallel it costs about the baseline. The threshold sits midway, far from both outcomes.
   * <p>
   * Do not raise it to buy margin against a stray pause. Writing {@code o} for the fixed per-request
   * overhead, the sequential ratio is {@code (2*SLEEP + o) / (SLEEP + o)}, which <em>falls</em> toward 1.0
   * as {@code o} grows: it is 2.0 only when the overhead is negligible, and already 1.5 once the overhead
   * reaches a whole SLEEP. Every increase of this constant therefore buys false-failure margin by giving
   * up false-pass margin, and a regression guard that silently passes is the worse of the two. The
   * baseline-validity assertion below is the cheaper way to protect the same property.
   */
  private static final double SEQUENTIAL_COST_FRACTION = 1.5;

  @Override
  protected String getDatabaseName() {
    return DATABASE_NAME;
  }

  /**
   * Test that two async commands sent via HTTP run in parallel, not sequentially.
   */
  @Test
  void httpAsyncCommandsRunInParallel() throws Exception {
    testEachServer(serverIndex -> {
      // Verify the server has at least 2 async worker threads by default (fix for issue #3122)
      final Database database = getServer(0).getDatabase(getDatabaseName());
      final int parallelLevel = database.async().getParallelLevel();
      LogManager.instance().log(this, Level.INFO, "Async parallel level: %d", parallelLevel);
      assertThat(parallelLevel)
          .as("Server should have at least 2 async worker threads by default")
          .isGreaterThanOrEqualTo(2);

      // Baseline first, on the same server and under the same load as the pair it is compared against.
      final long singleMs = timeHttpAsyncSleepCommands(serverIndex, database, 1);
      final long pairMs = timeHttpAsyncSleepCommands(serverIndex, database, 2);

      LogManager.instance().log(this, Level.INFO,
          "One async SLEEP command (%d ms each): %d ms; two concurrently: %d ms", SLEEP_DURATION, singleMs, pairMs);

      // The threshold below is only meaningful if the baseline actually waited out its SLEEP. Were
      // waitCompletion to return before the command was picked up, singleMs would collapse to the HTTP
      // round trip and the comparison would silently stop testing anything.
      // This is safe to assert rather than merely hope for: on the awaitResponse=false path
      // PostCommandHandler calls executeCommandAsync - which enqueues via database.async().command() -
      // before it builds the 202, so by the time the future below has observed the response the command
      // is already on the queue that waitCompletion drains.
      assertThat(singleMs)
          .as("The single-command baseline must have waited out its SLEEP, otherwise the ratio below is "
              + "measured against nothing (baseline: %d ms, SLEEP: %d ms)", singleMs, SLEEP_DURATION)
          .isGreaterThanOrEqualTo(SLEEP_DURATION);

      assertThat(pairMs)
          .as("Two concurrent async commands must cost far less than two sequential ones "
              + "(one command: %d ms, two commands: %d ms)", singleMs, pairMs)
          .isLessThan((long) (singleMs * SEQUENTIAL_COST_FRACTION));
    });
  }

  /**
   * Test that two async commands via the database.async() API run in parallel.
   */
  @Test
  void databaseAsyncCommandsRunInParallel() throws Exception {
    final Database database = getServer(0).getDatabase(getDatabaseName());

    // Ensure we have at least 2 async worker threads
    final int originalParallelLevel = database.async().getParallelLevel();
    if (originalParallelLevel < 2)
      database.async().setParallelLevel(2);

    try {
      assertCommandsOverlapped("database.async()", awaitAsyncSleepCompletions(database, "sqlscript",
          cmdNum -> "SLEEP " + SLEEP_DURATION + "; CONSOLE.log 'Database async command " + cmdNum + " completed'"));
    } finally {
      database.async().setParallelLevel(originalParallelLevel);
    }
  }

  /**
   * Test that simple SQL SLEEP commands (not sqlscript) also run in parallel.
   */
  @Test
  void simpleSqlAsyncCommandsRunInParallel() throws Exception {
    final Database database = getServer(0).getDatabase(getDatabaseName());

    // Ensure we have at least 2 async worker threads
    final int originalParallelLevel = database.async().getParallelLevel();
    if (originalParallelLevel < 2)
      database.async().setParallelLevel(2);

    try {
      assertCommandsOverlapped("SQL", awaitAsyncSleepCompletions(database, "sql",
          cmdNum -> "SLEEP " + SLEEP_DURATION));
    } finally {
      database.async().setParallelLevel(originalParallelLevel);
    }
  }

  /**
   * Submits two async SLEEP commands and returns the instant at which each one reported completion.
   */
  private long[] awaitAsyncSleepCompletions(final Database database, final String language,
      final IntFunction<String> commandForIndex) throws InterruptedException {
    final int commands = 2;
    final AtomicLongArray completedAt = new AtomicLongArray(commands);
    final AtomicInteger completedCount = new AtomicInteger();
    final CountDownLatch latch = new CountDownLatch(commands);

    for (int i = 0; i < commands; i++) {
      final int cmdNum = i + 1;
      final int slot = i;
      database.async().command(language, commandForIndex.apply(cmdNum), new AsyncResultsetCallback() {
        @Override
        public void onComplete(final ResultSet resultset) {
          // Written before countDown() so the await() below happens-after every timestamp.
          completedAt.set(slot, System.currentTimeMillis());
          completedCount.incrementAndGet();
          latch.countDown();
          LogManager.instance().log(this, Level.INFO, "Async %s command %d completed", language, cmdNum);
        }

        @Override
        public void onError(final Exception exception) {
          latch.countDown();
          LogManager.instance().log(this, Level.SEVERE, "Async %s command %d failed", exception, language, cmdNum);
        }
      });
    }

    assertThat(latch.await(COMPLETION_TIMEOUT_MS, TimeUnit.MILLISECONDS))
        .as("Both async commands should complete within %d ms", COMPLETION_TIMEOUT_MS).isTrue();
    assertThat(completedCount.get()).isEqualTo(commands);

    return new long[] { completedAt.get(0), completedAt.get(1) };
  }

  /**
   * Asserts the two commands were in flight at the same time. Sequential execution puts at least one whole
   * SLEEP between the two completions, because the second command cannot start before the first has
   * finished; parallel execution puts them within scheduling noise of each other. The gap is independent
   * of how loaded the machine is, which a total-elapsed-time budget is not.
   */
  private void assertCommandsOverlapped(final String label, final long[] completions) {
    final long completionGap = Math.abs(completions[1] - completions[0]);

    LogManager.instance().log(this, Level.INFO,
        "Completion gap for 2 %s SLEEP commands (%d ms each): %d ms", label, SLEEP_DURATION, completionGap);

    assertThat(completionGap)
        .as("%s async commands must run in parallel: run sequentially their completions would be at least "
            + "one SLEEP (%d ms) apart, they were %d ms apart", label, SLEEP_DURATION, completionGap)
        .isLessThan((long) SLEEP_DURATION);
  }

  /**
   * Fires {@code count} async SLEEP commands over HTTP and returns the wall-clock time until all of them
   * have finished executing on the server.
   */
  private long timeHttpAsyncSleepCommands(final int serverIndex, final Database database, final int count)
      throws Exception {
    final ExecutorService executor = Executors.newFixedThreadPool(count);
    try {
      final long startTime = System.currentTimeMillis();
      final List<Future<Integer>> futures = new ArrayList<>();

      for (int i = 0; i < count; i++) {
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

      // The HTTP requests themselves return immediately with 202; the work continues on the async executor.
      for (final Future<Integer> future : futures)
        assertThat(future.get(HTTP_RESPONSE_TIMEOUT_SECS, TimeUnit.SECONDS)).isEqualTo(202);

      assertThat(database.async().waitCompletion(COMPLETION_TIMEOUT_MS))
          .as("The %d async command(s) must finish within %d ms", count, COMPLETION_TIMEOUT_MS).isTrue();

      return System.currentTimeMillis() - startTime;
    } finally {
      executor.shutdown();
    }
  }

  @Override
  protected void populateDatabase() {
    // No data population needed for this test
  }
}
