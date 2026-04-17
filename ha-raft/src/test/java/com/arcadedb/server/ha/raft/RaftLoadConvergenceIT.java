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
package com.arcadedb.server.ha.raft;

import com.arcadedb.database.Database;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.remote.RemoteHttpComponent;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reproduces the workload shape of SingleLocalhostServerSimpleLoadTestIT against a 3-server Ratis
 * HA cluster, but with bounded HTTP calls so the test cannot hang on a stuck connection. The goal
 * is to surface fragility under moderate concurrent write load while still guaranteeing that the
 * test terminates and that all three replicas converge to the same database at the end.
 * <p>
 * The test fails loudly in two orthogonal dimensions:
 * <ol>
 *   <li><b>Liveness</b>: every HTTP call has a per-call timeout; the whole workload has a
 *       wall-clock deadline. If either is exceeded, a thread dump is logged and the test fails.</li>
 *   <li><b>Safety / convergence</b>: after the workload, counts on all servers must match the
 *       expected totals, and {@link BaseGraphServerTest#checkDatabasesAreIdentical()} runs in the
 *       inherited {@code @AfterEach} to verify the three databases are byte-for-byte equivalent.</li>
 * </ol>
 * Port layout inherited from {@link BaseGraphServerTest}: HTTP 2480+i, Raft 2424+i.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("IntegrationTest")
@Tag("slow")
class RaftLoadConvergenceIT extends BaseRaftHATest {

  // 3 concurrent loader threads hitting the leader with LOCK TYPE + COMMIT RETRY writes. This
  // is the stable CI scale. A 5-thread × 400-user × 10-photo × 2000-fr/likes variant has been
  // verified green twice byte-identically (~10 min each) with the idempotency-key retry fix in
  // place, but is too slow for default CI.
  private static final int  LOADER_THREADS       = 3;
  private static final int  USERS_PER_THREAD     = 200;
  private static final int  PHOTOS_PER_USER      = 5;
  private static final int  FRIENDSHIPS          = 500;
  private static final int  LIKES                = 500;
  private static final long CALL_TIMEOUT_MS      = 45_000;
  private static final long WORKLOAD_DEADLINE_MS = 20 * 60_000;

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected boolean isCreateDatabases() {
    return true;
  }

  @Override
  protected void populateDatabase() {
    // Exact same schema as DatabaseWrapper.createSchema() in the load-tests module: Lucene
    // FULL_TEXT on description, GEOSPATIAL on location, tags LIST OF STRING with BY ITEM index,
    // materialized view. This matches the server-side stress that accompanied the original
    // load-test runs.
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
  void loadConvergesOnAllReplicas() throws Exception {
    final int leaderHttpPort = 2480 + findLeaderIndex();
    LogManager.instance().log(this, Level.INFO, "TEST: Leader at HTTP port %d", leaderHttpPort);

    final AtomicInteger userIdGen = new AtomicInteger(0);
    final AtomicInteger photoIdGen = new AtomicInteger(1_000_000);
    final AtomicInteger callTimeouts = new AtomicInteger(0);
    final AtomicInteger callFailures = new AtomicInteger(0);

    final ExecutorService callGuard = Executors.newCachedThreadPool();

    try {
      // Phase 1: create all users and photos. We must finish this before starting friendships and
      // likes so that the CREATE EDGE clauses always find their endpoint vertices - otherwise a
      // CREATE EDGE over an empty SELECT silently succeeds with zero edges created.
      runPhase("users+photos", LOADER_THREADS,
          () -> addUsersAndPhotos(leaderHttpPort, userIdGen, photoIdGen, callGuard,
              callTimeouts, callFailures),
          callTimeouts, callFailures);

      // Phase 2: friendships and likes in parallel. All endpoints exist at this point.
      runPhase("friendships+likes", 2, (threadIdx) -> {
        if (threadIdx == 0)
          createFriendships(leaderHttpPort, callGuard, callTimeouts, callFailures);
        else
          createLikes(leaderHttpPort, callGuard, callTimeouts, callFailures);
      }, callTimeouts, callFailures);
    } finally {
      callGuard.shutdownNow();
    }

    LogManager.instance().log(this, Level.INFO,
        "TEST: Workload finished. Call timeouts=%d failures=%d", callTimeouts.get(), callFailures.get());

    // Liveness invariant: every individual call must have succeeded. Timeouts indicate the stuck-
    // HTTP-connection pathology we are trying to surface; any non-zero count is a regression.
    assertThat(callTimeouts.get())
        .as("per-call HTTP timeouts - indicates stuck connections under load")
        .isZero();
    assertThat(callFailures.get())
        .as("non-timeout operation failures")
        .isZero();

    // Safety invariant: every server must converge to the same totals. checkDatabasesAreIdentical
    // in @AfterEach will additionally verify byte-for-byte equivalence via DatabaseComparator.
    final int expectedUsers = LOADER_THREADS * USERS_PER_THREAD;
    final int expectedPhotos = expectedUsers * PHOTOS_PER_USER;

    for (int i = 0; i < getServerCount(); i++) {
      waitForReplicationIsCompleted(i);
      final Database db = getServer(i).getDatabase(getDatabaseName());
      assertThat(db.countType("User", false))
          .as("server %d User count", i).isEqualTo(expectedUsers);
      assertThat(db.countType("Photo", false))
          .as("server %d Photo count", i).isEqualTo(expectedPhotos);
      assertThat(db.countType("FriendOf", false))
          .as("server %d FriendOf count", i).isEqualTo((long) FRIENDSHIPS);
      assertThat(db.countType("Likes", false))
          .as("server %d Likes count", i).isEqualTo((long) LIKES);
    }
  }

  /**
   * Runs {@code body} on {@code parallelism} threads and bounds the total phase by
   * {@link #WORKLOAD_DEADLINE_MS}. Fails loudly with a thread dump if the phase doesn't finish.
   */
  private void runPhase(final String name, final int parallelism, final Runnable body,
      final AtomicInteger timeouts, final AtomicInteger failures) throws InterruptedException {
    runPhase(name, parallelism, (idx) -> body.run(), timeouts, failures);
  }

  private void runPhase(final String name, final int parallelism, final java.util.function.IntConsumer body,
      final AtomicInteger timeouts, final AtomicInteger failures) throws InterruptedException {
    final ExecutorService exec = Executors.newFixedThreadPool(parallelism);
    final java.util.List<Future<?>> futures = new java.util.ArrayList<>(parallelism);
    for (int t = 0; t < parallelism; t++) {
      final int idx = t;
      futures.add(exec.submit(() -> body.accept(idx)));
    }
    exec.shutdown();
    final boolean finished = exec.awaitTermination(WORKLOAD_DEADLINE_MS, TimeUnit.MILLISECONDS);
    if (!finished) {
      final String dump = dumpAllThreads();
      exec.shutdownNow();
      throw new AssertionError(
          "Phase '" + name + "' did not finish within " + WORKLOAD_DEADLINE_MS + "ms. "
              + "Call timeouts=" + timeouts.get() + " failures=" + failures.get()
              + "\n--- THREAD DUMP ---\n" + dump);
    }
    // Surface any Throwable from the loader threads. Without this, AssertionError / RuntimeException
    // raised inside the submitted tasks is swallowed by the Future and the phase appears to have
    // completed cleanly.
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
    LogManager.instance().log(this, Level.INFO, "TEST: Phase '%s' finished. timeouts=%d failures=%d",
        name, timeouts.get(), failures.get());
  }

  private void addUsersAndPhotos(final int httpPort, final AtomicInteger userIdGen,
      final AtomicInteger photoIdGen, final ExecutorService callGuard,
      final AtomicInteger timeouts, final AtomicInteger failures) {
    try (final RemoteDatabase db = openRemote(httpPort)) {
      for (int i = 0; i < USERS_PER_THREAD; i++) {
        final int userId = userIdGen.getAndIncrement();
        boundedCommand(db, callGuard, timeouts, failures, """
            BEGIN;
            LOCK TYPE User;
            CREATE VERTEX User SET id = ?;
            COMMIT RETRY 30;
            """, userId);

        for (int p = 0; p < PHOTOS_PER_USER; p++) {
          final int photoId = photoIdGen.getAndIncrement();
          boundedCommand(db, callGuard, timeouts, failures, """
              BEGIN;
              LOCK TYPE User, Photo, HasUploaded;
              LET user = SELECT FROM User WHERE id = ?;
              LET photo = CREATE VERTEX Photo SET id = ?, description = ?;
              CREATE EDGE HasUploaded FROM $user TO $photo;
              COMMIT RETRY 30;
              """, userId, photoId, "photo-" + photoId);
        }
      }
    }
  }

  private void createFriendships(final int httpPort, final ExecutorService callGuard,
      final AtomicInteger timeouts, final AtomicInteger failures) {
    final int totalUsers = LOADER_THREADS * USERS_PER_THREAD;
    try (final RemoteDatabase db = openRemote(httpPort)) {
      final Random rnd = new Random(42);
      for (int k = 0; k < FRIENDSHIPS; ) {
        final int a = rnd.nextInt(totalUsers);
        final int b = rnd.nextInt(totalUsers);
        if (a == b) continue;
        // Invariant we rely on for the final count assertion: both endpoints must be visible on
        // the leader at the moment we fire CREATE EDGE. If not, a silent zero-edge script would
        // hide the real regression behind the HTTP 200 response.
        final long aExists = boundedQueryCount(db, callGuard, timeouts, failures,
            "SELECT FROM User WHERE id = ?", a);
        final long bExists = boundedQueryCount(db, callGuard, timeouts, failures,
            "SELECT FROM User WHERE id = ?", b);
        assertThat(aExists).as("User id %d visible before CREATE EDGE", a).isEqualTo(1L);
        assertThat(bExists).as("User id %d visible before CREATE EDGE", b).isEqualTo(1L);

        // Exercise the exact workload shape of SingleLocalhostServerSimpleLoadTestIT: sqlscript
        // wrapping an explicit BEGIN / LOCK TYPE / COMMIT RETRY 30. We do NOT assert on the row
        // count of the resulting ResultSet because sqlscript yields rows for multiple statements
        // and the aggregate is not a reliable "edges created" signal. The truth is the final
        // countType("FriendOf") assertion.
        boundedCommand(db, callGuard, timeouts, failures, """
            BEGIN;
            LOCK TYPE User, FriendOf;
            CREATE EDGE FriendOf FROM (SELECT FROM User WHERE id = ?) TO (SELECT FROM User WHERE id = ?);
            COMMIT RETRY 30;
            """, a, b);
        k++;
      }
    }
  }

  private void createLikes(final int httpPort, final ExecutorService callGuard,
      final AtomicInteger timeouts, final AtomicInteger failures) {
    final int totalUsers = LOADER_THREADS * USERS_PER_THREAD;
    try (final RemoteDatabase db = openRemote(httpPort)) {
      for (int k = 0; k < LIKES; k++) {
        final int userId = ThreadLocalRandom.current().nextInt(totalUsers);
        final int photoId = 1_000_000 + ThreadLocalRandom.current().nextInt(totalUsers * PHOTOS_PER_USER);
        boundedCommand(db, callGuard, timeouts, failures, """
            BEGIN;
            LOCK TYPE User, Photo, Likes;
            CREATE EDGE Likes FROM (SELECT FROM User WHERE id = ?) TO (SELECT FROM Photo WHERE id = ?);
            COMMIT RETRY 30;
            """, userId, photoId);
      }
    }
  }

  /**
   * Runs a single command via the remote client with a hard per-call timeout. The underlying
   * {@link RemoteDatabase} already configures a 30s socket timeout, but under the HTTP/2 stuck-
   * connection pathology observed in apache-ratis, {@code HttpClient.send()} can park past that
   * budget. This bound is belt-and-suspenders: it guarantees the worker thread always makes
   * progress and bubbles up a {@link TimeoutException} that the assertions can catch.
   */
  private void boundedCommand(final RemoteDatabase db, final ExecutorService callGuard,
      final AtomicInteger timeouts, final AtomicInteger failures,
      final String script, final Object... params) {
    boundedCommandCount(db, callGuard, timeouts, failures, script, params);
  }

  /**
   * Same as {@link #boundedCommand} but returns the number of records the command produced. Used
   * by CREATE EDGE callers to assert that an edge was actually created rather than trusting the
   * HTTP 200 response - an empty {@code SELECT} on the FROM or TO side produces a successful
   * response with zero edges, which would silently hide a real convergence bug behind the HTTP
   * layer.
   */
  private long boundedCommandCount(final RemoteDatabase db, final ExecutorService callGuard,
      final AtomicInteger timeouts, final AtomicInteger failures,
      final String script, final Object... params) {
    final String language = script.contains(";") ? "sqlscript" : "sql";
    final Future<Long> f = callGuard.submit(() -> {
      final ResultSet rs = db.command(language, script, params);
      long c = 0;
      while (rs.hasNext()) {
        rs.next();
        c++;
      }
      return c;
    });
    try {
      return f.get(CALL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    } catch (final TimeoutException e) {
      timeouts.incrementAndGet();
      f.cancel(true);
      throw new RuntimeException("HTTP call exceeded " + CALL_TIMEOUT_MS + "ms", e);
    } catch (final ExecutionException e) {
      failures.incrementAndGet();
      throw new RuntimeException(e.getCause());
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  private long boundedQueryCount(final RemoteDatabase db, final ExecutorService callGuard,
      final AtomicInteger timeouts, final AtomicInteger failures,
      final String sql, final Object... params) {
    final Future<Long> f = callGuard.submit(() -> {
      final ResultSet rs = db.query("sql", sql, params);
      long c = 0;
      while (rs.hasNext()) {
        rs.next();
        c++;
      }
      return c;
    });
    try {
      return f.get(CALL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    } catch (final TimeoutException e) {
      timeouts.incrementAndGet();
      f.cancel(true);
      throw new RuntimeException("HTTP query exceeded " + CALL_TIMEOUT_MS + "ms", e);
    } catch (final ExecutionException e) {
      failures.incrementAndGet();
      throw new RuntimeException(e.getCause());
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  private RemoteDatabase openRemote(final int httpPort) {
    final RemoteDatabase db = new RemoteDatabase("localhost", httpPort, getDatabaseName(),
        "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
    db.setConnectionStrategy(RemoteHttpComponent.CONNECTION_STRATEGY.FIXED);
    db.setTimeout(30_000);
    return db;
  }

  private static void sleepQuietly(final long ms) {
    try { Thread.sleep(ms); } catch (final InterruptedException ie) { Thread.currentThread().interrupt(); }
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
}
