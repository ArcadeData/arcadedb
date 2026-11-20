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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.exception.DatabaseIsReopeningException;
import com.arcadedb.exception.DatabaseNotAvailableException;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.query.RunningQuery;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.lang.reflect.Field;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * How the server opens, reuses and closes its databases: the narrow "not available" error of a lookup that may not load,
 * a stopped server that opens nothing, a closed active instance that is never reused, and the idle / max-open close.
 */
class ServerDatabaseOpenCloseTest {
  // Regression: #6778 getDatabase with allowLoad=false and no open handle throws DatabaseNotAvailableException
  /**
   * Regression test for issue #6778: {@link ArcadeDBServer#getDatabase(String, boolean, boolean)} with
   * {@code allowLoad=false} and no open handle for the name must throw the narrow
   * {@link DatabaseNotAvailableException} - not the generic {@code DatabaseOperationException} it used to
   * throw - so {@code AbstractServerHttpHandler} can answer an accurate 404 instead of falling through to a
   * generic 500 (pinned separately in {@code HttpErrorStatusMappingTest.ErrorStatusParity}).
   */
  @Nested
  class NotAvailableWithoutLoad extends StaticBaseServerTest {
    private ArcadeDBServer server;

    @BeforeEach
    public void beginTest() {
      super.beginTest();
    }

    @AfterEach
    public void endTest() {
      if (server != null && server.isStarted())
        server.stop();
      super.endTest();
    }

    @Test
    void allowLoadFalseWithNoOpenHandleThrowsDatabaseNotAvailableException() {
      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, "./target");
      config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, DEFAULT_PASSWORD_FOR_TESTS);
      config.setValue(GlobalConfiguration.SERVER_HTTP_IO_THREADS, 2);
      config.setValue(GlobalConfiguration.TYPE_DEFAULT_BUCKETS, 2);

      server = new ArcadeDBServer(config);
      server.start();

      assertThatThrownBy(() -> server.getDatabase("issue6778-never-opened", false, false))
          .isInstanceOf(DatabaseNotAvailableException.class)
          .hasMessageContaining("issue6778-never-opened");
    }
  }

  // Regression: #10707 a stopped server opens no database for a thread that outlived its services
  /**
   * Issue #10707: a stopped server opens no database. A thread that outlived the services of the server - a security reload sweep
   * that was running when they stopped, a request still in flight - used to reach {@link ArcadeDBServer#getDatabaseForOperator(String)} after
   * the server had closed its databases, and reopened the directory: registered behind a stopped server, holding its file lock, so
   * the next server on that directory failed to start with {@code OverlappingFileLockException}.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class StoppedServerOpensNothing extends BaseGraphServerTest {
    @Test
    void aStoppedServerRefusesToOpenOrCreateADatabase() throws Exception {
      final ArcadeDBServer server = getServer(0);
      final String path = server.getDatabaseForOperator(getDatabaseName()).getDatabasePath();
      server.stop();

      // from another thread, as the security watcher or a late request would
      final CompletableFuture<Object> lateOpen = CompletableFuture.supplyAsync(() -> server.getDatabaseForOperator(getDatabaseName()));
      assertThatThrownBy(() -> lateOpen.get(30, TimeUnit.SECONDS)).hasCauseInstanceOf(DatabaseNotAvailableException.class)
          .hasMessageContaining("stopped");
      assertThatThrownBy(() -> server.getDatabaseForClient(getDatabaseName())).isInstanceOf(DatabaseNotAvailableException.class);
      assertThatThrownBy(() -> server.createDatabase("stopped10707", ComponentFile.MODE.READ_WRITE))
          .isInstanceOf(DatabaseNotAvailableException.class);

      assertThat(server.existsDatabase(getDatabaseName())).as("nothing registered behind the stopped server").isFalse();
      assertThat(DatabaseFactory.getActiveDatabaseInstance(path)).as("nothing open").isNull();

      // the directory is free: what the next server on it does first
      try (final Database reopened = new DatabaseFactory(path).open()) {
        assertThat(reopened.isOpen()).isTrue();
      }
    }

    @Test
    void aServerStartedAgainOpensItsDatabases() {
      final ArcadeDBServer server = getServer(0);
      server.stop();
      server.start();
      assertThat(server.getDatabaseForOperator(getDatabaseName()).isOpen()).as("a new lifecycle lifts the refusal").isTrue();
    }
  }

  // Regression: #8316 the slow path does not reuse a closed instance left in the active-instance map
  /**
   * Issue #8316: the slow path of {@link ArcadeDBServer#getDatabaseForOperator(String)} reused any instance registered in
   * {@link DatabaseFactory}'s active-instance map for the same path without asking whether it was still open. A close that
   * failed half way left its CLOSED instance there, and the lookup - including the HA snapshot installer's reopen after a
   * successful swap - registered that dead instance instead of opening the installed files. The engine now unregisters a
   * closed instance even when its close throws; this pins the server's own guard for the window in which one is still
   * registered, staged by putting a closed instance back in the map.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ClosedActiveInstanceNotReused {
    private static final String DB = "issue8316";

    @TempDir
    private Path serverDir;

    private ArcadeDBServer server;
    private Path           dbPath;
    private Database       stale;

    @BeforeEach
    void setUp() {
      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, serverDir.toString());
      server = new ArcadeDBServer(config);
      dbPath = serverDir.resolve(DB).toAbsolutePath().normalize();
    }

    @AfterEach
    void tearDown() throws Exception {
      if (stale != null)
        activeInstances().remove(dbPath, stale);
      final Database live = DatabaseFactory.getActiveDatabaseInstance(dbPath.toString());
      if (live != null && live.isOpen())
        live.drop();
    }

    @Test
    void anOpenActiveInstanceIsStillReused() {
      final Database opened = new DatabaseFactory(dbPath.toString()).create();

      assertThat(server.getDatabaseForOperator(DB).getWrappedDatabaseInstance())
          .as("a plugin that opened the database at startup shares its instance with the server")
          .isSameAs(opened);
    }

    @Test
    void aClosedActiveInstanceIsNeverRegistered() throws Exception {
      final Database closed = new DatabaseFactory(dbPath.toString()).create();
      closed.close();
      // What a close that marked the instance closed and then failed used to leave behind.
      activeInstances().put(dbPath, closed);
      stale = closed;

      assertThatThrownBy(() -> server.getDatabaseForOperator(DB))
          .as("the lookup refuses the path as still in use rather than serving a closed database")
          .isInstanceOf(DatabaseOperationException.class);
      assertThat(server.getDatabaseNames()).doesNotContain(DB);

      // Once the stale entry is gone, the same lookup opens the files.
      activeInstances().remove(dbPath, closed);
      stale = null;
      final ServerDatabase reopened = server.getDatabaseForOperator(DB);
      assertThat(reopened.isOpen()).isTrue();
      assertThat(reopened.getWrappedDatabaseInstance()).isNotSameAs(closed);
    }

    @SuppressWarnings("unchecked")
    private static Map<Path, Database> activeInstances() throws Exception {
      final Field field = DatabaseFactory.class.getDeclaredField("ACTIVE_INSTANCES");
      field.setAccessible(true);
      return (Map<Path, Database>) field.get(null);
    }
  }

  // Regression: #11068 an idle database, or one beyond maxOpenDatabases, is closed and reopened by the next request
  /**
   * Issue #11068: a database nobody uses is closed by the server ({@code arcadedb.server.databaseIdleTimeout}), or beyond the
   * cap on open databases ({@code arcadedb.server.maxOpenDatabases}), and the next request reopens it. A database with a
   * transaction open, a statement running or an HTTP session is never closed, however quiet. The checks are driven directly:
   * the scheduled one waits an hour here, so nothing closes behind a test's back.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class IdleClose extends BaseGraphServerTest {
    private static final String SECOND = "idle11068second";

    private final HttpClient http = HttpClient.newHttpClient();

    @Override
    protected void onServerConfiguration(final ContextConfiguration config) {
      config.setValue(GlobalConfiguration.SERVER_DATABASE_IDLE_CHECK_INTERVAL, TimeUnit.HOURS.toMillis(1));
      // The warm-up runs statements on an open database in the background: activity no test asked for
      config.setValue(GlobalConfiguration.SERVER_QUERY_WARM_UP, false);
    }

    @AfterEach
    void reopenAndDropTheSecondDatabase() {
      final ArcadeDBServer server = getServer(0);
      if (server == null || !server.isStarted())
        return;
      final ContextConfiguration configuration = server.getConfiguration();
      configuration.setValue(GlobalConfiguration.SERVER_DATABASE_IDLE_TIMEOUT, 0L);
      configuration.setValue(GlobalConfiguration.SERVER_MAX_OPEN_DATABASES, 0);
      configuration.setValue(GlobalConfiguration.SERVER_DATABASE_IDLE_EXCLUDE, "");
      configuration.setValue(GlobalConfiguration.SERVER_DATABASE_IDLE_MAX_CLOSES_PER_CHECK, 10);
      // The base class drops what it registered: a database left closed for idleness is opened again first
      server.getDatabaseForOperator(getDatabaseName());
      if (server.isDatabaseAvailable(SECOND))
        new ServerControlPlane(server).dropDatabase(SECOND);
    }

    @Test
    void anIdleDatabaseIsClosedAndTheNextHttpRequestReopensIt() throws Exception {
      final ArcadeDBServer server = getServer(0);
      final DatabaseIdleCloser closer = server.getDatabaseIdleCloser();
      idleTimeout(1);

      assertThat(closeIdle()).isEqualTo(1);
      assertThat(server.existsDatabase(getDatabaseName())).as("deregistered").isFalse();
      assertThat(closer.getIdleCloses()).isEqualTo(1);

      // Not refused as one a 'close database' closed: listed and reported as existing while it is closed
      assertThat(server.getAvailableDatabaseNames()).contains(getDatabaseName());
      assertThat(server.isDatabaseAvailable(getDatabaseName())).isTrue();
      assertThat(get("/api/v1/databases").getJSONArray("result").toList()).contains(getDatabaseName());
      assertThat(get("/api/v1/exists/" + getDatabaseName()).getBoolean("result")).isTrue();

      // The next request opens it again, where a 'close database' would have answered it with a refusal
      final JSONObject answer = command("SELECT count(*) AS n FROM V1");
      assertThat(answer.getJSONArray("result").getJSONObject(0).getLong("n")).isGreaterThan(0);
      assertThat(server.existsDatabase(getDatabaseName())).isTrue();
      assertThat(closer.getReopens()).isEqualTo(1);
    }

    @Test
    void aDatabaseUsedSinceTheLastCheckStaysOpen() throws Exception {
      idleTimeout(TimeUnit.HOURS.toMillis(1));
      getServer(0).getDatabaseIdleCloser().check();
      command("SELECT FROM V1 LIMIT 1");

      // A timeout an hour long, and the database was just used: nothing to close
      assertThat(getServer(0).getDatabaseIdleCloser().check()).isZero();
      assertThat(getServer(0).existsDatabase(getDatabaseName())).isTrue();
    }

    @Test
    void anActivityBetweenTwoChecksResetsTheIdleClock() throws Exception {
      idleTimeout(1);
      final DatabaseIdleCloser closer = getServer(0).getDatabaseIdleCloser();
      closer.check();
      Thread.sleep(20);
      // Used after the first sample: the second check sees the count move and keeps it open
      command("SELECT FROM V1 LIMIT 1");
      assertThat(closer.check()).isZero();
      assertThat(getServer(0).existsDatabase(getDatabaseName())).isTrue();
    }

    @Test
    void aTransactionOpenOnAnotherThreadKeepsTheDatabaseOpen() throws Exception {
      idleTimeout(1);
      final Database database = getServer(0).getDatabaseForOperator(getDatabaseName());
      final CountDownLatch begun = new CountDownLatch(1);
      final CountDownLatch release = new CountDownLatch(1);
      final Thread holder = new Thread(() -> {
        database.begin();
        begun.countDown();
        try {
          release.await(30, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        } finally {
          database.rollback();
        }
      });
      holder.start();
      try {
        assertThat(begun.await(30, TimeUnit.SECONDS)).isTrue();
        assertThat(closeIdle()).as("a quiet transaction is still a transaction").isZero();
        assertThat(getServer(0).existsDatabase(getDatabaseName())).isTrue();
      } finally {
        release.countDown();
        holder.join(30_000);
      }
      assertThat(closeIdle()).isEqualTo(1);
    }

    @Test
    void anHttpSessionKeepsTheDatabaseOpen() throws Exception {
      idleTimeout(1);
      final HttpResponse<String> begun = post("/api/v1/begin/" + getDatabaseName(), "", null);
      assertThat(begun.statusCode()).isEqualTo(204);
      final String session = begun.headers().firstValue("arcadedb-session-id").orElseThrow();

      // Its transaction lives in the session between two requests, on no thread
      assertThat(closeIdle()).isZero();
      assertThat(getServer(0).existsDatabase(getDatabaseName())).isTrue();

      assertThat(post("/api/v1/rollback/" + getDatabaseName(), "", session).statusCode()).isEqualTo(204);
      assertThat(closeIdle()).isEqualTo(1);
    }

    @Test
    void aRunningStatementKeepsTheDatabaseOpen() throws Exception {
      idleTimeout(1);
      final RunningQuery running = getServer(0).getRunningQueries()
          .register(getDatabaseName(), "root", "test", null, null, RunningQuery.Client.NONE);
      try {
        assertThat(closeIdle()).isZero();
      } finally {
        running.close();
      }
      assertThat(closeIdle()).isEqualTo(1);
    }

    @Test
    void anExcludedDatabaseIsNeverClosed() throws Exception {
      idleTimeout(1);
      getServer(0).getConfiguration().setValue(GlobalConfiguration.SERVER_DATABASE_IDLE_EXCLUDE, " other , " + getDatabaseName());
      assertThat(closeIdle()).isZero();
      assertThat(getServer(0).existsDatabase(getDatabaseName())).isTrue();
    }

    @Test
    void aHandleResolvedBeforeTheCloseIsToldToRetry() throws Exception {
      idleTimeout(1);
      final Database stale = getServer(0).getDatabaseForClient(getDatabaseName());
      assertThat(closeIdle()).isEqualTo(1);

      // Retryable, not the permanent "closed": the retry resolves the name again and finds the database reopened
      assertThatThrownBy(() -> stale.query("sql", "SELECT FROM V1 LIMIT 1").close()).isInstanceOf(
          DatabaseIsReopeningException.class);
      try (final var rs = getServer(0).getDatabaseForClient(getDatabaseName()).query("sql", "SELECT FROM V1 LIMIT 1")) {
        assertThat(rs.hasNext()).isTrue();
      }
    }

    @Test
    void theCapClosesTheLeastRecentlyUsedDatabase() throws Exception {
      final ArcadeDBServer server = getServer(0);
      server.createDatabase(SECOND, ComponentFile.MODE.READ_WRITE);
      final DatabaseIdleCloser closer = server.getDatabaseIdleCloser();

      // Both sampled under a cap they fit in, then only the main one used: the second is the least recently used
      server.getConfiguration().setValue(GlobalConfiguration.SERVER_MAX_OPEN_DATABASES, 10);
      assertThat(closer.check()).isZero();
      Thread.sleep(20);
      command("SELECT FROM V1 LIMIT 1");
      server.getConfiguration().setValue(GlobalConfiguration.SERVER_MAX_OPEN_DATABASES, 1);
      assertThat(closer.check()).isEqualTo(1);
      assertThat(closer.getCapCloses()).isEqualTo(1);
      assertThat(server.existsDatabase(SECOND)).isFalse();
      assertThat(server.existsDatabase(getDatabaseName())).isTrue();
      assertThat(server.getAvailableDatabaseNames()).contains(SECOND, getDatabaseName());

      // Under the cap again: nothing more to close
      assertThat(closer.check()).isZero();
    }

    @Test
    void oneCheckClosesAtMostItsBudget() throws Exception {
      final ArcadeDBServer server = getServer(0);
      server.createDatabase(SECOND, ComponentFile.MODE.READ_WRITE);
      idleTimeout(1);
      server.getConfiguration().setValue(GlobalConfiguration.SERVER_DATABASE_IDLE_MAX_CLOSES_PER_CHECK, 1);
      final DatabaseIdleCloser closer = server.getDatabaseIdleCloser();

      closer.check();
      Thread.sleep(20);
      assertThat(closer.check()).isEqualTo(1);
      assertThat(closer.check()).isEqualTo(1);
      assertThat(server.getDatabaseNames()).doesNotContain(SECOND, getDatabaseName());
    }

    @Test
    void concurrentChecksCloseTheDatabaseOnce() throws Exception {
      idleTimeout(1);
      final DatabaseIdleCloser closer = getServer(0).getDatabaseIdleCloser();
      closer.check();
      Thread.sleep(20);

      // An embedder's check racing another one: they take turns on the samples, and the database is closed and counted once
      final int threads = 8;
      final CountDownLatch start = new CountDownLatch(1);
      final AtomicInteger closed = new AtomicInteger();
      final Thread[] checkers = new Thread[threads];
      for (int i = 0; i < threads; i++) {
        checkers[i] = new Thread(() -> {
          try {
            start.await();
            closed.addAndGet(closer.check());
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        });
        checkers[i].start();
      }
      start.countDown();
      for (final Thread checker : checkers)
        checker.join(30_000);

      assertThat(closed.get()).isEqualTo(1);
      assertThat(closer.getIdleCloses()).isEqualTo(1);
    }

    @Test
    void aClosedDatabaseRemovedFromDiskIsForgotten() throws Exception {
      final ArcadeDBServer server = getServer(0);
      server.createDatabase(SECOND, ComponentFile.MODE.READ_WRITE);
      idleTimeout(1);
      getServer(0).getConfiguration().setValue(GlobalConfiguration.SERVER_DATABASE_IDLE_EXCLUDE, getDatabaseName());
      final DatabaseIdleCloser closer = server.getDatabaseIdleCloser();
      assertThat(closeIdle()).isEqualTo(1);
      assertThat(closer.getClosedAwaitingReopen()).isEqualTo(1);

      // Deleted behind the server's back while closed: it will never be reopened, so it is not remembered for ever
      FileUtils.deleteRecursively(
          new File(server.getConfiguration().getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY), SECOND));
      final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
      while (server.isDatabaseAvailable(SECOND) && System.nanoTime() < deadline)
        Thread.sleep(100);
      assertThat(server.isDatabaseAvailable(SECOND)).isFalse();

      closer.check();
      assertThat(closer.getClosedAwaitingReopen()).isZero();
    }

    @Test
    void aDatabaseClosedByCommandIsNotReopenedNorListed() throws Exception {
      // The existing contract of 'close database' is untouched: it stays closed until 'open database'
      final ArcadeDBServer server = getServer(0);
      server.createDatabase(SECOND, ComponentFile.MODE.READ_WRITE);
      new ServerControlPlane(server).closeDatabase(SECOND);
      assertThat(server.getAvailableDatabaseNames()).doesNotContain(SECOND);
      assertThat(server.isDatabaseAvailable(SECOND)).isFalse();
      new ServerControlPlane(server).openDatabase(SECOND);
    }

    private void idleTimeout(final long millis) {
      getServer(0).getConfiguration().setValue(GlobalConfiguration.SERVER_DATABASE_IDLE_TIMEOUT, millis);
    }

    /**
     * Checks a little apart, until one closes something: the first samples the database, a later one finds it unused past a 1 ms
     * timeout. Several, so that background work landing between two of them (it moves the activity count) delays the close instead
     * of failing the test; a database that is pinned stays open through all of them.
     */
    private int closeIdle() throws InterruptedException {
      final DatabaseIdleCloser closer = getServer(0).getDatabaseIdleCloser();
      int closed = closer.check();
      for (int attempt = 0; attempt < 10 && closed == 0; attempt++) {
        Thread.sleep(20);
        closed += closer.check();
      }
      return closed;
    }

    private JSONObject command(final String sql) throws Exception {
      final HttpResponse<String> response = post("/api/v1/command/" + getDatabaseName(),
          new JSONObject().put("language", "sql").put("command", sql).toString(), null);
      assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
      return new JSONObject(response.body());
    }

    private JSONObject get(final String path) throws Exception {
      final HttpResponse<String> response = http.send(HttpRequest.newBuilder(URI.create(getServerHttpUrl(0, path)))
          .header("Authorization", authorization()).GET().build(), HttpResponse.BodyHandlers.ofString());
      assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
      return new JSONObject(response.body());
    }

    private HttpResponse<String> post(final String path, final String body, final String session) throws Exception {
      final HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(getServerHttpUrl(0, path)))
          .header("Authorization", authorization()).header("Content-Type", "application/json")
          .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8));
      if (session != null)
        request.header("arcadedb-session-id", session);
      return http.send(request.build(), HttpResponse.BodyHandlers.ofString());
    }

    private static String authorization() {
      return "Basic " + Base64.getEncoder()
          .encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8));
    }
  }
}
