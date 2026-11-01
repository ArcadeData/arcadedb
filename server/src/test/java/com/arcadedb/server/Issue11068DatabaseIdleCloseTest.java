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
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.exception.DatabaseIsReopeningException;
import com.arcadedb.query.RunningQuery;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11068: a database nobody uses is closed by the server ({@code arcadedb.server.databaseIdleTimeout}), or beyond the
 * cap on open databases ({@code arcadedb.server.maxOpenDatabases}), and the next request reopens it. A database with a
 * transaction open, a statement running or an HTTP session is never closed, however quiet. The checks are driven directly:
 * the scheduled one waits an hour here, so nothing closes behind a test's back.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue11068DatabaseIdleCloseTest extends BaseGraphServerTest {
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
