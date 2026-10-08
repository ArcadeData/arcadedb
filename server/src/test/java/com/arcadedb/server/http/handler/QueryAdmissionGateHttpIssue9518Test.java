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
package com.arcadedb.server.http.handler;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.exception.QueryAdmissionException;
import com.arcadedb.exception.QueryHeapBudgetExceededException;
import com.arcadedb.query.sql.executor.QueryAdmissionGate;
import com.arcadedb.query.sql.executor.QueryHeapBudget;
import com.arcadedb.query.sql.executor.QueryHeapTracker;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import io.undertow.server.HttpServerExchange;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Issue #9518 over HTTP: with the query admission gate enabled, a query that arrives when every slot is taken waits for
 * one instead of running at once, on all three query endpoints; one that waits too long is answered 503, the status a
 * client's retry policy keys on; and every request gives its slot back, whether it succeeded or failed.
 * <p>
 * The test takes the only slot itself, through the JVM-wide gate the handlers use, so what a request does while it is
 * taken does not depend on timing.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class QueryAdmissionGateHttpIssue9518Test extends BaseGraphServerTest {
  private static final HttpClient HTTP  = HttpClient.newHttpClient();
  private static final String     QUERY = "SELECT 1 AS one";
  private static final String     SORT  = "SELECT count(*) AS c FROM (SELECT FROM Doc9518 ORDER BY name)";
  private static final int        ROWS  = 20_000;
  private static final long       MB    = 1024 * 1024;

  private final QueryAdmissionGate gate = QueryAdmissionGate.getInstance();

  @AfterEach
  void resetGate() {
    GlobalConfiguration.QUERY_MAX_CONCURRENT.reset();
    GlobalConfiguration.QUERY_QUEUE_TIMEOUT.reset();
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.reset();
  }

  @Test
  void aQueryWaitsForAFreeSlotOnEveryQueryEndpointInsteadOfBeingRefused() throws Exception {
    GlobalConfiguration.QUERY_MAX_CONCURRENT.setValue(1);
    GlobalConfiguration.QUERY_QUEUE_TIMEOUT.setValue(60_000L);

    for (final String endpoint : new String[] { "POST /query", "POST /command", "GET /query" }) {
      final int queuedBefore = gate.getQueued();
      final CompletableFuture<HttpResponse<String>> response;
      final QueryAdmissionGate.Ticket held = gate.admit();
      try {
        response = HTTP.sendAsync(request(endpoint, QUERY), HttpResponse.BodyHandlers.ofString());
        await().atMost(Duration.ofSeconds(30)).until(() -> gate.getQueued() == queuedBefore + 1 || response.isDone());
        assertThat(response).as(endpoint + " must wait while the only slot is taken").isNotDone();
      } finally {
        held.close();
      }

      final HttpResponse<String> answer = response.get(30, TimeUnit.SECONDS);
      assertThat(answer.statusCode()).as(endpoint + ": " + answer.body()).isEqualTo(200);
      assertThat(new JSONObject(answer.body()).getJSONArray("result").getJSONObject(0).getInt("one")).isEqualTo(1);
      await().atMost(Duration.ofSeconds(30)).until(() -> gate.getRunning() == 0);
    }
  }

  @Test
  void aQueryThatWaitsTooLongIsAnswered503AndNothingOfItRuns() throws Exception {
    GlobalConfiguration.QUERY_MAX_CONCURRENT.setValue(1);
    GlobalConfiguration.QUERY_QUEUE_TIMEOUT.setValue(100L);
    getServerDatabase(0, getDatabaseName()).getSchema().getOrCreateDocumentType("Admission9518");

    try (final QueryAdmissionGate.Ticket ignored = gate.admit()) {
      for (final String endpoint : new String[] { "POST /query", "GET /query" }) {
        final HttpResponse<String> answer = HTTP.send(request(endpoint, QUERY), HttpResponse.BodyHandlers.ofString());
        assertThat(answer.statusCode()).as(endpoint + ": " + answer.body()).isEqualTo(503);
        assertThat(new JSONObject(answer.body()).getString("exception")).isEqualTo(QueryAdmissionException.class.getName());
      }

      // A REFUSED WRITE WAS NEVER EXECUTED
      final HttpResponse<String> write = HTTP.send(request("POST /command", "INSERT INTO Admission9518 SET id = 1"),
          HttpResponse.BodyHandlers.ofString());
      assertThat(write.statusCode()).as(write.body()).isEqualTo(503);
    }
    assertThat(getServerDatabase(0, getDatabaseName()).countType("Admission9518", false)).isZero();
  }

  /**
   * The case the gate exists for (#9404): the running queries hold nearly all the heap budget. Without the gate a heavy
   * query starts at once and is refused by the budget with a 503; with it, the same query waits until the heap is given
   * back and then succeeds, although a slot was free all along.
   */
  @Test
  void aHeavyQueryWaitsForTheHeapInsteadOfBeingRefusedByTheBudget() throws Exception {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.getSchema().createVertexType("Doc9518");
    database.transaction(() -> {
      for (int i = 0; i < ROWS; i++)
        database.newVertex("Doc9518").set("id", i, "name", "name-%06d-%s".formatted(i, "x".repeat(80))).save();
    });

    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(QueryHeapBudget.getReservedBytes() / MB + 16);
    GlobalConfiguration.QUERY_QUEUE_TIMEOUT.setValue(60_000L);

    // THE OTHER QUERIES HOLD ALL THE BUDGET BUT 512KB
    final QueryHeapTracker others = new QueryHeapTracker();
    others.charge(QueryHeapTracker.UNRESERVED_BYTES + QueryHeapBudget.getLimitBytes() - QueryHeapBudget.getReservedBytes() - MB / 2,
        "other queries");
    boolean othersClosed = false;
    try {
      // GATE DISABLED: THE QUERY STARTS AT ONCE AND THE BUDGET REFUSES IT
      final HttpResponse<String> refused = HTTP.send(request("POST /command", SORT), HttpResponse.BodyHandlers.ofString());
      assertThat(refused.statusCode()).as(refused.body()).isEqualTo(503);
      assertThat(new JSONObject(refused.body()).getString("exception")).isEqualTo(QueryHeapBudgetExceededException.class.getName());

      // GATE ENABLED, WITH SLOTS TO SPARE: ONE QUERY RUNNING (THE TEST'S OWN TICKET) AND THE HEAP ABOVE THE WATERMARK
      GlobalConfiguration.QUERY_MAX_CONCURRENT.setValue(4);
      try (final QueryAdmissionGate.Ticket ignored = gate.admit()) {
        final long deferralsBefore = gate.getHeapDeferrals();
        final CompletableFuture<HttpResponse<String>> response = HTTP.sendAsync(request("POST /command", SORT),
            HttpResponse.BodyHandlers.ofString());
        await().atMost(Duration.ofSeconds(30)).until(() -> gate.getHeapDeferrals() > deferralsBefore || response.isDone());
        assertThat(response).as("the query waits for the heap").isNotDone();

        // THE OTHER QUERIES GIVE THE HEAP BACK: THE WAITING ONE STARTS, WHILE THE TEST STILL HOLDS ITS SLOT
        others.close();
        othersClosed = true;
        final HttpResponse<String> answer = response.get(60, TimeUnit.SECONDS);
        assertThat(answer.statusCode()).as(answer.body()).isEqualTo(200);
        assertThat(new JSONObject(answer.body()).getJSONArray("result").getJSONObject(0).getLong("c")).isEqualTo(ROWS);
      }
    } finally {
      if (!othersClosed)
        others.close();
      GlobalConfiguration.QUERY_MAX_HEAP_RAM.reset();
    }
  }

  @Test
  void aFailingQueryGivesItsSlotBack() throws Exception {
    GlobalConfiguration.QUERY_MAX_CONCURRENT.setValue(1);
    // NO WAITING: A SLOT LEFT TAKEN BY THE FAILED QUERY WOULD MAKE THE NEXT ONE A 503
    GlobalConfiguration.QUERY_QUEUE_TIMEOUT.setValue(0L);

    for (final String endpoint : new String[] { "POST /query", "POST /command", "GET /query" }) {
      final HttpResponse<String> failed = HTTP.send(request(endpoint, "SELECT FROM NotExistentType9518"),
          HttpResponse.BodyHandlers.ofString());
      assertThat(failed.statusCode()).as(endpoint + ": " + failed.body()).isGreaterThanOrEqualTo(400).isNotEqualTo(503);

      final HttpResponse<String> next = HTTP.send(request(endpoint, QUERY), HttpResponse.BodyHandlers.ofString());
      assertThat(next.statusCode()).as(endpoint + ": " + next.body()).isEqualTo(200);
    }
    assertThat(gate.getRunning()).isZero();
  }

  @Test
  void aStreamedQueryGivesItsSlotBackOnceTheRowsAreWritten() throws Exception {
    GlobalConfiguration.QUERY_MAX_CONCURRENT.setValue(1);
    GlobalConfiguration.QUERY_QUEUE_TIMEOUT.setValue(0L);

    for (int i = 0; i < 3; i++) {
      final HttpRequest streamed = HttpRequest.newBuilder(URI.create(getServerHttpUrl(0, "/api/v1/query/" + getDatabaseName())))
          .header("Content-Type", "application/json").header("Accept", "application/x-ndjson").header("Authorization", basicAuth())
          .POST(HttpRequest.BodyPublishers.ofString(new JSONObject().put("language", "sql").put("command", QUERY).toString()))
          .build();
      final HttpResponse<String> answer = HTTP.send(streamed, HttpResponse.BodyHandlers.ofString());
      assertThat(answer.statusCode()).as(answer.body()).isEqualTo(200);
      // THE ROWS ARE WRITTEN BEFORE THE HANDLER RETURNS AND CLOSES THE TICKET
      await().atMost(Duration.ofSeconds(30)).until(() -> gate.getRunning() == 0);
    }
  }

  @Test
  void everyDatabaseRequestLeavesTheIoThreadWhileTheGateIsEnabled() {
    final GetQueryHandler query = new GetQueryHandler(null);
    final HttpServerExchange exchange = new HttpServerExchange(null);
    assertThat(query.mustExecuteOnWorkerThread(exchange)).as("a session-less buffered GET is still short enough").isFalse();

    GlobalConfiguration.QUERY_MAX_CONCURRENT.setValue(0);
    assertThat(query.mayWaitForAdmission()).as("gate disabled: the IO thread answers, as before").isFalse();

    GlobalConfiguration.QUERY_MAX_CONCURRENT.setValue(4);
    assertThat(query.mayWaitForAdmission()).as("gate enabled: the query may wait, never on an IO thread").isTrue();
    assertThat(new PostVectorSearchHandler(null).mayWaitForAdmission()).isTrue();
    assertThat(new GetPromQLQueryHandler(null).mayWaitForAdmission()).isTrue();

    // NEVER HELD BEHIND THE QUERIES: SESSION MANAGEMENT AND THE HEALTH PROBE
    assertThat(new PostBeginHandler(null).mayWaitForAdmission()).isFalse();
    assertThat(new PostRollbackHandler(null).mayWaitForAdmission()).isFalse();
    assertThat(new GetGrafanaHealthHandler(null).mayWaitForAdmission()).isFalse();
  }

  /** A session's rollback, which releases what its queries hold, is never queued behind them. */
  @Test
  void aSessionIsRolledBackWhileEverySlotIsTaken() throws Exception {
    GlobalConfiguration.QUERY_MAX_CONCURRENT.setValue(1);
    GlobalConfiguration.QUERY_QUEUE_TIMEOUT.setValue(0L);

    final HttpResponse<String> begin = HTTP.send(HttpRequest.newBuilder(URI.create(getServerHttpUrl(0, "/api/v1/begin/" + getDatabaseName())))
        .header("Authorization", basicAuth()).POST(HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.ofString());
    assertThat(begin.statusCode()).as(begin.body()).isEqualTo(204);
    final String sessionId = begin.headers().firstValue("arcadedb-session-id").orElseThrow();

    try (final QueryAdmissionGate.Ticket ignored = gate.admit()) {
      final HttpResponse<String> query = HTTP.send(HttpRequest.newBuilder(request("POST /query", QUERY), (n, v) -> true)
          .header("arcadedb-session-id", sessionId).build(), HttpResponse.BodyHandlers.ofString());
      assertThat(query.statusCode()).as("the session's query waits for a slot like any other: " + query.body()).isEqualTo(503);

      final HttpResponse<String> rollback = HTTP.send(
          HttpRequest.newBuilder(URI.create(getServerHttpUrl(0, "/api/v1/rollback/" + getDatabaseName())))
              .header("Authorization", basicAuth()).header("arcadedb-session-id", sessionId)
              .POST(HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.ofString());
      assertThat(rollback.statusCode()).as(rollback.body()).isEqualTo(204);
    }
  }

  /** A bulk load is work like a command: it waits for the gate too, and gives its slot back when it is done. */
  @Test
  void aBatchLoadGoesThroughTheGate() throws Exception {
    getServerDatabase(0, getDatabaseName()).getSchema().getOrCreateVertexType("Batch9518");
    GlobalConfiguration.QUERY_MAX_CONCURRENT.setValue(1);
    GlobalConfiguration.QUERY_QUEUE_TIMEOUT.setValue(0L);

    final HttpRequest load = HttpRequest.newBuilder(URI.create(getServerHttpUrl(0, "/api/v1/batch/" + getDatabaseName())))
        .header("Authorization", basicAuth()).header("Content-Type", "application/x-ndjson")
        .POST(HttpRequest.BodyPublishers.ofString("{\"@type\":\"vertex\",\"@class\":\"Batch9518\",\"name\":\"a\"}\n")).build();

    try (final QueryAdmissionGate.Ticket ignored = gate.admit()) {
      final HttpResponse<String> refused = HTTP.send(load, HttpResponse.BodyHandlers.ofString());
      assertThat(refused.statusCode()).as(refused.body()).isEqualTo(503);
    }
    assertThat(getServerDatabase(0, getDatabaseName()).countType("Batch9518", false)).isZero();

    for (int i = 0; i < 2; i++) {
      final HttpResponse<String> loaded = HTTP.send(load, HttpResponse.BodyHandlers.ofString());
      assertThat(loaded.statusCode()).as(loaded.body()).isEqualTo(200);
    }
    assertThat(getServerDatabase(0, getDatabaseName()).countType("Batch9518", false)).isEqualTo(2);
    assertThat(gate.getRunning()).isZero();
  }

  private HttpRequest request(final String endpoint, final String command) {
    final HttpRequest.Builder builder;
    if (endpoint.startsWith("GET")) {
      final String path = "/api/v1/query/" + getDatabaseName() + "/sql/" + URLEncoder.encode(command, StandardCharsets.UTF_8).replace("+", "%20");
      builder = HttpRequest.newBuilder(URI.create(getServerHttpUrl(0, path))).GET();
    } else {
      final String path = "/api/v1" + endpoint.substring("POST ".length()) + "/" + getDatabaseName();
      builder = HttpRequest.newBuilder(URI.create(getServerHttpUrl(0, path))).header("Content-Type", "application/json")
          .POST(HttpRequest.BodyPublishers.ofString(new JSONObject().put("language", "sql").put("command", command).toString()));
    }
    return builder.header("Authorization", basicAuth()).build();
  }

  private static String basicAuth() {
    return "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8));
  }
}
