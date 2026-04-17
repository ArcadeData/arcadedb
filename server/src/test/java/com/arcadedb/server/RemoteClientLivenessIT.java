/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.remote.RemoteHttpComponent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Single-server liveness test for {@link RemoteDatabase} that reproduces the workload shape of
 * {@code SingleLocalhostServerSimpleLoadTestIT} (User+Photo schema with Lucene full-text and
 * geospatial indexes, sqlscript+LOCK+COMMIT RETRY, TypeIdSupplier-style mixed read-write for
 * friendships/likes) and asserts that no single HTTP call exceeds the client's own per-request
 * timeout. Motivated by a reproducer where {@code HttpClient.send()} parked indefinitely past
 * its {@code .timeout()} directive on apache-ratis.
 * <p>
 * This test is orthogonal to HA: it drives a single standalone server so any liveness failure
 * observed here is squarely a client-side bug.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("IntegrationTest")
class RemoteClientLivenessIT extends BaseGraphServerTest {

  // Scale comparable to the ha-redesign variant of SingleLocalhostServerSimpleLoadTestIT:
  // 1 thread, 500 users, 10 photos each, 1000 friendships, 1000 likes. Parallelized across a few
  // threads to also stress HTTP/2 multiplexing on the shared server's connections.
  private static final int  LOADER_THREADS        = 3;
  private static final int  USERS_PER_THREAD      = 200;
  private static final int  PHOTOS_PER_USER       = 5;
  private static final int  FRIENDSHIPS           = 500;
  private static final int  LIKES                 = 500;
  private static final int  CLIENT_REQ_TIMEOUT_MS = 30_000;
  // If any HTTP call exceeds twice the client's own per-request timeout, the client is not
  // honoring its own .timeout() directive and we have reproduced the apache-ratis hang.
  private static final long LIVENESS_BUDGET_MS    = 2 * CLIENT_REQ_TIMEOUT_MS;
  private static final long TEST_DEADLINE_MS      = 10 * 60_000;

  private static final String[] WORDS = {
      "lorem", "ipsum", "dolor", "sit", "amet", "consectetur", "adipiscing", "elit", "sed", "do",
      "eiusmod", "tempor", "incididunt", "ut", "labore", "et", "dolore", "magna", "aliqua"
  };

  private final AtomicInteger livenessViolations = new AtomicInteger(0);
  private final AtomicLong    totalLatencyNanos  = new AtomicLong(0);
  private final AtomicLong    maxLatencyNanos    = new AtomicLong(0);
  private final AtomicInteger completedCalls     = new AtomicInteger(0);

  @Override
  protected int getServerCount() {
    return 1;
  }

  @Override
  protected boolean isCreateDatabases() {
    return true;
  }

  @Override
  protected void populateDatabase() {
    // Identical schema to DatabaseWrapper.createSchema() from load-tests: Lucene FULL_TEXT on
    // description, GEOSPATIAL on location, LIST OF STRING tags with BY ITEM index, materialized
    // view. This is the server-side work that made the original hang reproduce.
    final Database database = getDatabases()[0];
    database.command("sqlscript", """
        CREATE VERTEX TYPE User;
        CREATE PROPERTY User.id INTEGER;
        CREATE INDEX ON User (id) UNIQUE;

        CREATE VERTEX TYPE Photo;
        CREATE PROPERTY Photo.id INTEGER;
        CREATE PROPERTY Photo.description STRING;
        CREATE PROPERTY Photo.tags LIST OF STRING;
        CREATE PROPERTY Photo.location STRING;

        CREATE INDEX ON Photo (id) UNIQUE;
        CREATE INDEX ON Photo (tags BY ITEM) NOTUNIQUE;
        CREATE INDEX ON Photo (description) FULL_TEXT METADATA {
          "analyzer": "org.apache.lucene.analysis.en.EnglishAnalyzer"
          };
        CREATE INDEX ON Photo (location) GEOSPATIAL;

        CREATE EDGE TYPE HasUploaded;
        CREATE EDGE TYPE FriendOf;
        CREATE EDGE TYPE Likes;

        CREATE MATERIALIZED VIEW UserStats AS
          SELECT id AS userId,
            out('HasUploaded').in('Likes').size() AS totalLikes,
            out('FriendOf').size() AS totalFriendships
          FROM User
          REFRESH INCREMENTAL;
        """);
  }

  @Test
  @DisplayName("remote client honors its per-request timeout under full-schema workload")
  void perCallTimeoutIsHonored() throws Exception {
    final AtomicInteger userIdGen = new AtomicInteger(0);
    final AtomicInteger photoIdGen = new AtomicInteger(1_000_000);

    final ExecutorService callGuard = Executors.newCachedThreadPool();
    try {
      // Phase 1: users and their photos. Each thread gets its own RemoteDatabase (matches
      // DatabaseWrapper ownership in the original test).
      runPhase("users+photos", LOADER_THREADS, (idx) ->
          addUsersAndPhotos(callGuard, userIdGen, photoIdGen));

      // Phase 2: friendships and likes in parallel, via TypeIdSupplier-style SELECT-then-WRITE.
      // This is where the original hang manifested - in a SELECT following many writes.
      runPhase("friendships+likes", 2, (idx) -> {
        if (idx == 0) createFriendships(callGuard);
        else createLikes(callGuard);
      });
    } finally {
      callGuard.shutdownNow();
    }

    final long totalCalls = completedCalls.get() + livenessViolations.get();
    final double avgMs = completedCalls.get() == 0 ? 0
        : totalLatencyNanos.get() / 1_000_000.0 / completedCalls.get();
    final String summary = String.format(
        "LIVENESS-STATS: completedCalls=%d issued=%d  avgLatency=%.1fms  maxLatency=%dms  livenessViolations=%d",
        completedCalls.get(), totalCalls, avgMs,
        maxLatencyNanos.get() / 1_000_000L, livenessViolations.get());
    System.out.println(summary);
    LogManager.instance().log(this, Level.INFO, summary);

    assertThat(livenessViolations.get())
        .as("number of HTTP calls that exceeded the client's own %dms per-request timeout",
            CLIENT_REQ_TIMEOUT_MS)
        .isZero();
  }

  // ---------------------------------------------------------------------------------------------
  // Phase bodies - ported from DatabaseWrapper in load-tests.
  // ---------------------------------------------------------------------------------------------

  private void addUsersAndPhotos(final ExecutorService callGuard,
      final AtomicInteger userIdGen, final AtomicInteger photoIdGen) {
    final Random rnd = new Random();
    try (final RemoteDatabase db = openRemote()) {
      for (int u = 0; u < USERS_PER_THREAD; u++) {
        final int userId = userIdGen.getAndIncrement();
        boundedCommand(db, callGuard, """
            BEGIN;
            LOCK TYPE User;
            CREATE VERTEX User SET id = ?;
            COMMIT RETRY 30;
            """, userId);

        for (int p = 0; p < PHOTOS_PER_USER; p++) {
          final int photoId = photoIdGen.getAndIncrement();
          final String tag1 = "tag" + (p % PHOTOS_PER_USER);
          final String tag2 = "tag" + (p % PHOTOS_PER_USER + 1);
          final String description = longDescription(rnd);
          final String location = randomPoint(rnd);
          boundedCommand(db, callGuard, """
              BEGIN;
              LOCK TYPE User, Photo, HasUploaded;
              LET user = SELECT FROM User WHERE id = ?;
              LET photo = CREATE VERTEX Photo SET id = ?, description = ?, tags = [?, ?], location = ?;
              CREATE EDGE HasUploaded FROM $user TO $photo;
              COMMIT RETRY 30;
              """, userId, photoId, description, tag1, tag2, location);
        }
      }
    }
  }

  private void createFriendships(final ExecutorService callGuard) {
    try (final RemoteDatabase db = openRemote()) {
      final List<Integer> userIds = loadAllIds(db, callGuard, "User");
      final Random rnd = new Random(42);
      int created = 0;
      while (created < FRIENDSHIPS) {
        final int a = userIds.get(rnd.nextInt(userIds.size()));
        final int b = userIds.get(rnd.nextInt(userIds.size()));
        if (a == b) continue;
        boundedCommand(db, callGuard, """
            BEGIN;
            LOCK TYPE User, FriendOf;
            CREATE EDGE FriendOf FROM (SELECT FROM User WHERE id = ?) TO (SELECT FROM User WHERE id = ?);
            COMMIT RETRY 30;
            """, a, b);
        created++;
      }
    }
  }

  private void createLikes(final ExecutorService callGuard) {
    try (final RemoteDatabase db = openRemote()) {
      final List<Integer> userIds = loadAllIds(db, callGuard, "User");
      final List<Integer> photoIds = loadAllIds(db, callGuard, "Photo");
      final Random rnd = new Random(7);
      for (int i = 0; i < LIKES; i++) {
        final int u = userIds.get(rnd.nextInt(userIds.size()));
        final int p = photoIds.get(rnd.nextInt(photoIds.size()));
        boundedCommand(db, callGuard, """
            BEGIN;
            LOCK TYPE User, Photo, Likes;
            CREATE EDGE Likes FROM (SELECT FROM User WHERE id = ?) TO (SELECT FROM Photo WHERE id = ?);
            COMMIT RETRY 30;
            """, u, p);
      }
    }
  }

  /**
   * Pulls every id of the given type once up-front in batches of 100. Mirrors the read-pick-write
   * traffic pattern of TypeIdSupplier but in a finite way - we don't need an infinite id stream
   * to stress the client, and we want to ensure any hang we observe is a real liveness issue, not
   * a test-code infinite loop looking for ids past the end of the collection.
   */
  private List<Integer> loadAllIds(final RemoteDatabase db, final ExecutorService callGuard,
      final String type) {
    final List<Integer> all = new ArrayList<>();
    int skip = 0;
    while (true) {
      final ResultSet rs = boundedQuery(db, callGuard,
          "SELECT id FROM " + type + " ORDER BY id SKIP ? LIMIT ?", skip, 100);
      int batch = 0;
      while (rs.hasNext()) {
        all.add(rs.next().getProperty("id"));
        batch++;
      }
      if (batch == 0) break;
      skip += batch;
    }
    return all;
  }

  // ---------------------------------------------------------------------------------------------
  // Bounded-call helpers: each remote invocation is wrapped so it cannot park past the liveness
  // budget. This is what surfaces the apache-ratis hang as a concrete test failure rather than a
  // hung JVM.
  // ---------------------------------------------------------------------------------------------

  private void boundedCommand(final RemoteDatabase db, final ExecutorService callGuard,
      final String script, final Object... params) {
    final String language = script.contains(";") ? "sqlscript" : "sql";
    final long start = System.nanoTime();
    final Future<?> f = callGuard.submit((Callable<Void>) () -> {
      db.command(language, script, params);
      return null;
    });
    completeOrRecord(f, start, "command");
  }

  private ResultSet boundedQuery(final RemoteDatabase db, final ExecutorService callGuard,
      final String sql, final Object... params) {
    final long start = System.nanoTime();
    final Future<ResultSet> f = callGuard.submit(() -> db.query("sql", sql, params));
    return (ResultSet) completeOrRecord(f, start, "query");
  }

  private Object completeOrRecord(final Future<?> f, final long startNanos, final String kind) {
    try {
      final Object v = f.get(LIVENESS_BUDGET_MS, TimeUnit.MILLISECONDS);
      final long elapsed = System.nanoTime() - startNanos;
      totalLatencyNanos.addAndGet(elapsed);
      maxLatencyNanos.updateAndGet(prev -> Math.max(prev, elapsed));
      completedCalls.incrementAndGet();
      return v;
    } catch (final TimeoutException e) {
      livenessViolations.incrementAndGet();
      f.cancel(true);
      throw new LivenessViolationException(
          kind + " exceeded " + LIVENESS_BUDGET_MS + "ms (client timeout was "
              + CLIENT_REQ_TIMEOUT_MS + "ms)");
    } catch (final ExecutionException e) {
      throw new RuntimeException(e.getCause());
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Phase runner - propagates task exceptions (otherwise AssertionError / LivenessViolation get
  // swallowed by the Future and the phase appears to have completed cleanly).
  // ---------------------------------------------------------------------------------------------

  private void runPhase(final String name, final int parallelism,
      final java.util.function.IntConsumer body) throws InterruptedException {
    final ExecutorService exec = Executors.newFixedThreadPool(parallelism);
    final List<Future<?>> futures = new ArrayList<>(parallelism);
    for (int t = 0; t < parallelism; t++) {
      final int idx = t;
      futures.add(exec.submit(() -> body.accept(idx)));
    }
    exec.shutdown();
    final boolean finished = exec.awaitTermination(TEST_DEADLINE_MS, TimeUnit.MILLISECONDS);
    if (!finished) {
      final String dump = dumpAllThreads();
      exec.shutdownNow();
      throw new AssertionError(
          "Phase '" + name + "' did not finish within " + TEST_DEADLINE_MS + "ms. "
              + "completedCalls=" + completedCalls.get()
              + " livenessViolations=" + livenessViolations.get()
              + "\n--- THREAD DUMP ---\n" + dump);
    }
    for (final Future<?> f : futures) {
      try {
        f.get();
      } catch (final ExecutionException e) {
        final Throwable cause = e.getCause() != null ? e.getCause() : e;
        if (cause instanceof AssertionError ae) throw ae;
        if (cause instanceof RuntimeException re) throw re;
        throw new RuntimeException(cause);
      }
    }
    LogManager.instance().log(this, Level.INFO,
        "PHASE '%s' finished. completedCalls=%d livenessViolations=%d",
        name, completedCalls.get(), livenessViolations.get());
  }

  // ---------------------------------------------------------------------------------------------
  // Small utilities
  // ---------------------------------------------------------------------------------------------

  private RemoteDatabase openRemote() {
    final RemoteDatabase db = new RemoteDatabase("localhost", 2480, getDatabaseName(),
        "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
    db.setConnectionStrategy(RemoteHttpComponent.CONNECTION_STRATEGY.FIXED);
    db.setTimeout(CLIENT_REQ_TIMEOUT_MS);
    return db;
  }

  private static String longDescription(final Random rnd) {
    final StringBuilder sb = new StringBuilder(800);
    for (int i = 0; i < 100; i++) {
      if (i > 0) sb.append(' ');
      sb.append(WORDS[rnd.nextInt(WORDS.length)]);
    }
    return sb.toString();
  }

  private static String randomPoint(final Random rnd) {
    final double lon = Math.round((-180.0 + rnd.nextDouble() * 360.0) * 1e6) / 1e6;
    final double lat = Math.round((-90.0 + rnd.nextDouble() * 180.0) * 1e6) / 1e6;
    return "POINT (" + lon + " " + lat + ")";
  }

  private static String dumpAllThreads() {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (final PrintStream ps = new PrintStream(baos)) {
      final ThreadInfo[] infos = ManagementFactory.getThreadMXBean().dumpAllThreads(true, true);
      for (final ThreadInfo ti : infos)
        ps.println(ti);
    }
    return baos.toString();
  }

  private static class LivenessViolationException extends RuntimeException {
    LivenessViolationException(final String m) { super(m); }
  }
}
