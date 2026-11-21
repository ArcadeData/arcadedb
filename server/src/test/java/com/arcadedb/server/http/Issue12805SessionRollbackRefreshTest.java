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
package com.arcadedb.server.http;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.security.ServerSecurityUser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Issue #12805: a TIMESERIES sample written in an HTTP session that is then rolled back stays stored (#7410), and the refresh of the
 * continuous aggregates over the type is owed whichever way the transaction ends (#10687). The refresh was handed to the worker
 * thread that ran the {@code INSERT}, and a rollback on any other worker dropped it as if it were the sweep of a dead thread's
 * transaction. Each request of a session runs on whichever worker is free, so the {@code /rollback} dropped it as often as not, and
 * the aggregate disagreed with its defining query until a later write happened to refresh it.
 */
class Issue12805SessionRollbackRefreshTest extends BaseGraphServerTest {
  private static final String TYPE       = "X12805";
  private static final String AGGREGATE  = "agg12805";
  private static final String DEFINITION =
      "SELECT h, ts.timeBucket('1m', ts) AS b, count(*) AS c, sum(v) AS s FROM " + TYPE + " GROUP BY h, b";
  private static final int    ROUNDS     = 4;

  private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

  /** The one bucket of the aggregate: every sample of the tests has the same tag and falls in the same minute. */
  private record Totals(long count, double sum) {
  }

  @FunctionalInterface
  private interface Work {
    void run() throws Exception;
  }

  @BeforeEach
  void createSourceAndAggregate() {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.command("sql", "CREATE TIMESERIES TYPE " + TYPE + " TIMESTAMP ts TAGS (h STRING) FIELDS (v DOUBLE) SHARDS 1");
    database.command("sql", insertSample(1000, 1.0));
    database.command("sql", "CREATE CONTINUOUS AGGREGATE " + AGGREGATE + " AS " + DEFINITION);
    assertThat(aggregateTotals()).isEqualTo(new Totals(1, 1.0));
  }

  /**
   * The issue's repro over HTTP: begin, insert and roll back, round after round, with the aggregate checked after each one. Requests
   * sent one after the other are served by the worker released last, so a statement of another session is kept running on the
   * {@code INSERT}'s worker while the {@code /rollback} is sent, which makes it run on another one, as it does on a busy server.
   */
  @Test
  void sessionRollbacksOverHttpKeepTheAggregateEqualToItsDefiningQuery() throws Exception {
    final ServerSecurityUser user = rootUser();
    for (int i = 0; i < ROUNDS; i++) {
      final String sessionId = begin();
      final String busyId = begin();

      final HttpResponse<String> inserted = send("/command/" + getDatabaseName(), sessionId, sqlCommand(insertSample(2000 + i, 10.0)));
      assertThat(inserted.statusCode()).as(inserted.body()).isEqualTo(200);

      final CompletableFuture<HttpResponse<String>> sleeping = http.sendAsync(
          request("/command/" + getDatabaseName(), busyId, sqlCommand("SLEEP 400")), HttpResponse.BodyHandlers.ofString());
      final HttpSession busy = sessions().getSessionById(user, busyId);
      await().atMost(10, TimeUnit.SECONDS).until(busy::isBusy);

      final HttpResponse<String> rolledBack = send("/rollback/" + getDatabaseName(), sessionId, null);
      assertThat(rolledBack.statusCode()).as(rolledBack.body()).isEqualTo(204);

      assertThat(sleeping.get(30, TimeUnit.SECONDS).statusCode()).isEqualTo(200);
      assertThat(send("/rollback/" + getDatabaseName(), busyId, null).statusCode()).isEqualTo(204);

      // the sample is not part of the transaction and stays (#7410), and so the aggregate counts it
      final Totals expected = new Totals(i + 2, 1.0 + 10.0 * (i + 1));
      assertThat(definingQueryTotals()).as("round %d: the defining query", i).isEqualTo(expected);
      assertThat(aggregateTotals()).as("round %d: the aggregate after the session's rollback", i).isEqualTo(expected);
    }
  }

  /**
   * The same requests run on two worker threads of their own, so that the {@code /rollback} lands on another worker than the
   * {@code INSERT} every time rather than by chance. Each request binds the session's transaction to its worker and runs under the
   * session, as {@code DatabaseAbstractHandler} does.
   */
  @Test
  void aRollbackOnAnotherWorkerThanTheInsertRefreshesTheAggregate() throws Exception {
    final ServerSecurityUser user = rootUser();
    final HttpSession session = beginSession(user);

    onWorker("issue12805-insert", () -> request(session, user, false, () -> database().command("sql", insertSample(2000, 10.0))));
    assertThat(aggregateTotals()).as("the refresh waits for the end of the transaction").isEqualTo(new Totals(1, 1.0));

    onWorker("issue12805-rollback", () -> request(session, user, true, () -> {
      database().rollback();
      return null;
    }));

    assertThat(definingQueryTotals()).isEqualTo(new Totals(2, 11.0));
    assertThat(aggregateTotals()).as("the rollback on another worker runs the refresh the insert owes").isEqualTo(new Totals(2, 11.0));
  }

  /** A command that fails rolls the session's transaction back on the worker it runs on, which owes the refresh just the same. */
  @Test
  void aFailedCommandOnAnotherWorkerThanTheInsertRefreshesTheAggregate() throws Exception {
    final ServerSecurityUser user = rootUser();
    final HttpSession session = beginSession(user);

    onWorker("issue12805-insert", () -> request(session, user, false, () -> database().command("sql", insertSample(2000, 10.0))));

    onWorker("issue12805-failure", () -> assertThatThrownBy(() -> request(session, user, false, () -> {
      throw new IllegalStateException("the command failed");
    })).isInstanceOf(IllegalStateException.class));

    assertThat(definingQueryTotals()).isEqualTo(new Totals(2, 11.0));
    assertThat(aggregateTotals()).as("the rollback of the failed command runs the refresh the insert owes")
        .isEqualTo(new Totals(2, 11.0));

    onWorker("issue12805-end", () -> request(session, user, true, () -> {
      if (database().isTransactionActive())
        database().rollback();
      return null;
    }));
    sessions().removeSession(session.id);
  }

  /** What {@code POST /begin} does, on a worker thread of its own. */
  private HttpSession beginSession(final ServerSecurityUser user) throws Exception {
    final AtomicReference<HttpSession> session = new AtomicReference<>();
    onWorker("issue12805-begin", () -> {
      final DatabaseInternal database = database();
      DatabaseContext.INSTANCE.init(database);
      try {
        database.begin();
        final TransactionContext tx = database.getTransaction();
        tx.setClientManaged(true);
        session.set(sessions().createSession(user, tx));
        tx.setRequester(session.get().id);
      } finally {
        DatabaseContext.INSTANCE.removeContext(database.getDatabasePath());
      }
    });
    return session.get();
  }

  /** One request of the session: its transaction bound to the current worker, the work run under the session. */
  private void request(final HttpSession session, final ServerSecurityUser user, final boolean endsSession, final Callable<?> work)
      throws Exception {
    final DatabaseInternal database = database();
    DatabaseContext.INSTANCE.init(database, session.transaction);
    try {
      session.execute(user, work, true, endsSession, true);
    } finally {
      DatabaseContext.INSTANCE.removeContext(database.getDatabasePath());
    }
  }

  private static void onWorker(final String name, final Work work) throws Exception {
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final Thread worker = new Thread(() -> {
      try {
        work.run();
      } catch (final Throwable e) {
        failure.set(e);
      }
    }, name);
    worker.start();
    worker.join();
    if (failure.get() instanceof Exception e)
      throw e;
    if (failure.get() instanceof Error e)
      throw e;
  }

  private Totals aggregateTotals() {
    return totals("SELECT c, s FROM " + AGGREGATE);
  }

  private Totals definingQueryTotals() {
    return totals(DEFINITION);
  }

  private Totals totals(final String query) {
    try (final ResultSet rs = database().query("sql", query)) {
      assertThat(rs.hasNext()).as(query).isTrue();
      final Result row = rs.next();
      assertThat(rs.hasNext()).as("one bucket: %s", query).isFalse();
      return new Totals(((Number) row.getProperty("c")).longValue(), ((Number) row.getProperty("s")).doubleValue());
    }
  }

  private static String insertSample(final long ts, final double value) {
    return "INSERT INTO " + TYPE + " SET ts = " + ts + ", h = 'a', v = " + value;
  }

  private DatabaseInternal database() {
    return (DatabaseInternal) getServerDatabase(0, getDatabaseName());
  }

  private HttpSessionManager sessions() {
    return getServer(0).getHttpServer().getSessionManager();
  }

  private ServerSecurityUser rootUser() {
    return getServer(0).getSecurity().getUser("root");
  }

  private String begin() throws Exception {
    final HttpResponse<String> begun = send("/begin/" + getDatabaseName(), null, "");
    assertThat(begun.statusCode()).as(begun.body()).isEqualTo(204);
    final String sessionId = begun.headers().firstValue(HttpSessionManager.ARCADEDB_SESSION_ID).orElse(null);
    assertThat(sessionId).isNotBlank();
    return sessionId;
  }

  private HttpResponse<String> send(final String path, final String sessionId, final String body) throws Exception {
    return http.send(request(path, sessionId, body), HttpResponse.BodyHandlers.ofString());
  }

  private HttpRequest request(final String path, final String sessionId, final String body) {
    final HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(getServerHttpUrl(0, "/api/v1" + path)))
        .timeout(Duration.ofSeconds(30))
        .header("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));
    if (sessionId != null)
      builder.header(HttpSessionManager.ARCADEDB_SESSION_ID, sessionId);
    if (body != null && !body.isEmpty())
      builder.header("Content-Type", "application/json");
    builder.POST(body != null ? HttpRequest.BodyPublishers.ofString(body) : HttpRequest.BodyPublishers.noBody());
    return builder.build();
  }

  private static String sqlCommand(final String command) {
    return new JSONObject().put("language", "sql").put("command", command).toString();
  }
}
