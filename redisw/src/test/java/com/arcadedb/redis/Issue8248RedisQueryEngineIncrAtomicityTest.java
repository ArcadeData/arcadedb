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
package com.arcadedb.redis;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.redis.query.RedisQueryEngine;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #8248: the fix for #7776 made INCR/DECR atomic on the RESP wire path
 * ({@code RedisNetworkExecutor}) only. The {@code redis} query engine ({@link RedisQueryEngine}), reached through
 * {@code Database.command("redis", ...)} and {@code POST /api/v1/command/{db}} with {@code "language": "redis"}, kept
 * reading the counter with {@code getGlobalVariable} and writing it back with {@code setGlobalVariable}. The engine
 * instance is cached per database and shared by every request thread, so two concurrent callers read the same value,
 * both wrote their own successor, and each was answered a plausible count that never happened.
 * <p>
 * Every concurrent test drives the real engine through a real entry point and asserts the exact total: a
 * get-then-set loses a large share of the increments at this contention, so a regression cannot pass.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
public class Issue8248RedisQueryEngineIncrAtomicityTest extends BaseRedisServerTest {

  private static final int THREADS    = 8;
  private static final int PER_THREAD = 500;

  @Test
  void concurrentIncrThroughDatabaseCommandIsNotLost() throws Exception {
    final Database database = getServerDatabase(0, getDatabaseName());
    final long total = runConcurrently(THREADS, PER_THREAD, () -> close(database.command("redis", "INCR issue8248incr")));

    assertThat(((Number) database.command("redis", "GET issue8248incr").next().getProperty("value")).longValue())
        .as("%d threads x %d INCR through Database.command(\"redis\", ...) must add up exactly", THREADS, PER_THREAD)
        .isEqualTo(total);
  }

  @Test
  void concurrentIncrByThroughDatabaseCommandIsNotLost() throws Exception {
    final Database database = getServerDatabase(0, getDatabaseName());
    final long total = runConcurrently(THREADS, PER_THREAD, () -> close(database.command("redis", "INCRBY issue8248incrby 3")));

    assertThat(((Number) database.command("redis", "GET issue8248incrby").next().getProperty("value")).longValue())
        .isEqualTo(total * 3);
  }

  @Test
  void concurrentDecrThroughDatabaseCommandIsNotLost() throws Exception {
    final Database database = getServerDatabase(0, getDatabaseName());
    final long total = runConcurrently(THREADS, PER_THREAD, () -> close(database.command("redis", "DECR issue8248decr")));

    assertThat(((Number) database.command("redis", "GET issue8248decr").next().getProperty("value")).longValue())
        .as("%d threads x %d DECR through Database.command(\"redis\", ...) must add up exactly", THREADS, PER_THREAD)
        .isEqualTo(-total);
  }

  @Test
  void concurrentDecrByThroughDatabaseCommandIsNotLost() throws Exception {
    final Database database = getServerDatabase(0, getDatabaseName());
    final long total = runConcurrently(THREADS, PER_THREAD, () -> close(database.command("redis", "DECRBY issue8248decrby 2")));

    assertThat(((Number) database.command("redis", "GET issue8248decrby").next().getProperty("value")).longValue())
        .isEqualTo(-total * 2);
  }

  @Test
  void concurrentIncrByFloatThroughDatabaseCommandIsNotLost() throws Exception {
    final Database database = getServerDatabase(0, getDatabaseName());
    // 0.5 is exactly representable, so the sum of any interleaving is exact and the assertion needs no tolerance.
    final long total = runConcurrently(THREADS, PER_THREAD,
        () -> close(database.command("redis", "INCRBYFLOAT issue8248float 0.5")));

    assertThat(((Number) database.command("redis", "GET issue8248float").next().getProperty("value")).doubleValue())
        .isEqualTo(total * 0.5);
  }

  @Test
  void concurrentIncrThroughHttpCommandEndpointIsNotLost() throws Exception {
    // The surface the issue names: POST /api/v1/command/{db} with "language": "redis", every request on its own
    // server thread against the one cached engine instance.
    final int threads = 4;
    final int perThread = 100;
    final long total = runConcurrently(threads, perThread, () -> httpCommand("INCR issue8248http"));

    assertThat(((Number) getServerDatabase(0, getDatabaseName()).command("redis", "GET issue8248http").next().getProperty("value")).longValue())
        .as("%d threads x %d INCR through POST /api/v1/command must add up exactly", threads, perThread)
        .isEqualTo(total);
  }

  @Test
  void eachCallerIsAnsweredTheValueItsOwnIncrementProduced() throws Exception {
    // A get-then-set answers two interleaved callers the same count. With the read, the addition and the write as
    // one operation, every reply is distinct and together they are exactly 1..N.
    final Database database = getServerDatabase(0, getDatabaseName());
    final int threads = THREADS;
    final int perThread = 250;
    final boolean[] seen = new boolean[threads * perThread + 1];
    final AtomicReference<String> duplicate = new AtomicReference<>();

    runConcurrently(threads, perThread, () -> {
      try (final ResultSet rs = database.command("redis", "INCR issue8248replies")) {
        final int reply = ((Number) rs.next().getProperty("value")).intValue();
        synchronized (seen) {
          if (reply < 1 || reply >= seen.length || seen[reply])
            duplicate.compareAndSet(null, "reply " + reply + " was out of range or given twice");
          else
            seen[reply] = true;
        }
      }
    });

    assertThat(duplicate.get()).isNull();
  }

  @Test
  void aNonNumericValueIsRefusedAndLeftUnchanged() {
    // The type check now runs INSIDE the atomic update; throwing out of it must leave the key exactly as it was.
    final Database database = getServerDatabase(0, getDatabaseName());
    close(database.command("redis", "SET issue8248text not-a-number"));

    assertThatThrownBy(() -> database.command("redis", "INCR issue8248text")).hasMessageContaining("is not a number");
    assertThatThrownBy(() -> database.command("redis", "DECRBY issue8248text 2")).hasMessageContaining("is not a number");
    assertThat(database.command("redis", "GET issue8248text").next().<Object>getProperty("value")).isEqualTo("not-a-number");
  }

  @Test
  void theArithmeticIsUnchanged() {
    final Database database = getServerDatabase(0, getDatabaseName());

    assertThat(value(database, "INCR issue8248mixed")).isEqualTo(1L);
    assertThat(value(database, "INCRBY issue8248mixed 10")).isEqualTo(11L);
    assertThat(value(database, "DECR issue8248mixed")).isEqualTo(10L);
    assertThat(value(database, "DECRBY issue8248mixed 4")).isEqualTo(6L);

    // A numeric string left by SET is promoted, as before.
    close(database.command("redis", "SET issue8248string 41"));
    assertThat(value(database, "INCR issue8248string")).isEqualTo(42L);
    close(database.command("redis", "SET issue8248string2 41"));
    assertThat(value(database, "DECR issue8248string2")).isEqualTo(40L);

    assertThat(((Number) database.command("redis", "INCRBYFLOAT issue8248mixed 0.5").next().getProperty("value")).doubleValue())
        .isEqualTo(6.5);
  }

  private static long value(final Database database, final String command) {
    try (final ResultSet rs = database.command("redis", command)) {
      return ((Number) rs.next().getProperty("value")).longValue();
    }
  }

  private static void close(final ResultSet rs) {
    rs.close();
  }

  private interface Call {
    void run() throws Exception;
  }

  /**
   * Runs {@code call} {@code perThread} times on each of {@code threads} threads released together, fails on the first
   * error any of them hit, and returns the number of calls made.
   */
  private static long runConcurrently(final int threads, final int perThread, final Call call) throws Exception {
    final CountDownLatch start = new CountDownLatch(1);
    final CountDownLatch done = new CountDownLatch(threads);
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final List<Thread> workers = new ArrayList<>();

    for (int t = 0; t < threads; t++) {
      final Thread thread = new Thread(() -> {
        try {
          start.await();
          for (int i = 0; i < perThread; i++)
            call.run();
        } catch (final Throwable e) {
          failure.compareAndSet(null, e);
        } finally {
          done.countDown();
        }
      }, "issue8248-" + t);
      workers.add(thread);
      thread.start();
    }

    start.countDown();
    assertThat(done.await(120, TimeUnit.SECONDS)).as("the worker threads must all finish").isTrue();
    for (final Thread thread : workers)
      thread.join();

    assertThat(failure.get()).isNull();
    return (long) threads * perThread;
  }

  private void httpCommand(final String command) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) URI.create(
        "http://127.0.0.1:" + getServerHttpPort() + "/api/v1/command/" + getDatabaseName()).toURL().openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));
    connection.setRequestProperty("Content-Type", "application/json");
    connection.setDoOutput(true);

    final JSONObject request = new JSONObject();
    request.put("language", "redis");
    request.put("command", command);
    try (final OutputStream os = connection.getOutputStream()) {
      os.write(request.toString().getBytes(StandardCharsets.UTF_8));
    }

    final int responseCode = connection.getResponseCode();
    try {
      if (responseCode != 200)
        throw new IllegalStateException("HTTP " + responseCode + ": " + new String(connection.getErrorStream().readAllBytes(), StandardCharsets.UTF_8));
      connection.getInputStream().readAllBytes();
    } finally {
      connection.disconnect();
    }
  }

  @Override
  protected void populateDatabase() {
  }

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("Redis Protocol:com.arcadedb.redis.RedisProtocolPlugin");
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }
}
