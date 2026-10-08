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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.Identifiable;
import com.arcadedb.event.BeforeRecordUpdateListener;
import com.arcadedb.function.sql.SQLFunctionAbstract;
import com.arcadedb.query.sql.SQLQueryEngine;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8899 (consolidating #8880 and #8875): the in-band {@code error} line of a streamed NDJSON response, written
 * once the 200 is already on the wire.
 * <ul>
 * <li>#8880 - a failure the classifier answers with a {@code Retry-After} header on the buffered encoding must carry
 *     the same back-off as a {@code retryAfter} member, on both the query and the batch stream;</li>
 * <li>#8875 - the query stream's {@code message} carried the raw exception text in every server mode, while the
 *     buffered encoding of the same failure conceals the free-form text in production mode.</li>
 * </ul>
 * Every query test fails on the SECOND row, so the first row is already flushed and the failure can only go in band.
 */
public class Issue8899StreamedErrorLineIT extends BaseGraphServerTest {
  private static final String TYPE_NAME   = "Stream8899";
  private static final String VERTEX_TYPE = "V8899";
  private static final String EDGE_TYPE   = "E8899";
  private static final String NDJSON      = "application/x-ndjson";
  private static final String FUNCTION    = "fail8899";
  private static final String SECRET      = "/var/lib/arcadedb/secret-internal-path";

  // Shared by every test of the class: sound only because the methods of one class run sequentially on one server
  private static volatile Supplier<RuntimeException> failure;
  private static final    AtomicInteger              calls = new AtomicInteger();

  private volatile boolean batchArmed;
  private          String  previousMode;

  /** Lets the first row through, then throws whatever the test installed - unwrapped, as an engine failure is. */
  private static final class FailOnSecondRow extends SQLFunctionAbstract {
    FailOnSecondRow() {
      super(FUNCTION);
    }

    @Override
    public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult,
        final Object[] params, final CommandContext context) {
      final int call = calls.incrementAndGet();
      if (call > 1)
        throw failure.get();
      return call;
    }

    @Override
    public String getSyntax() {
      return FUNCTION + "()";
    }
  }

  @Override
  protected void populateDatabase() {
    super.populateDatabase();
    final Database db = getDatabase(0);
    db.transaction(() -> {
      db.command("sql", "CREATE DOCUMENT TYPE " + TYPE_NAME + " BUCKETS 1");
      for (int i = 0; i < 5; i++)
        db.newDocument(TYPE_NAME).set("idx", i).save();
    });
    db.getSchema().createVertexType(VERTEX_TYPE);
    db.getSchema().createEdgeType(EDGE_TYPE);
  }

  @BeforeEach
  void registerFunction() {
    calls.set(0);
    previousMode = getServer(0).getConfiguration().getValueAsString(GlobalConfiguration.SERVER_MODE);
    sqlEngine().getFunctionFactory().register(new FailOnSecondRow());
  }

  @AfterEach
  void cleanUp() {
    sqlEngine().getFunctionFactory().unregister(FUNCTION);
    failure = null;
    batchArmed = false;
    getServer(0).getConfiguration().setValue(GlobalConfiguration.SERVER_MODE, previousMode);
  }

  // ---------------------------------------------------------------------------------------------------------------
  // #8880: retryAfter on the query stream
  // ---------------------------------------------------------------------------------------------------------------

  @Test
  void aRetryLaterFailureCarriesItsBackOffOnTheQueryStream() throws Exception {
    failure = () -> new RetryLaterException("snapshot install in progress", 7);
    final JSONObject error = streamedQueryError();
    assertThat(error.getInt("status")).as("error line: %s", error).isEqualTo(503);
    assertThat(error.getString("exception")).isEqualTo(RetryLaterException.class.getName());
    assertThat(error.getLong("retryAfter")).as("error line: %s", error).isEqualTo(7L);
  }

  @Test
  void anInFlightRefusalCarriesItsBackOffOnTheQueryStream() throws Exception {
    failure = () -> new RequestStillInFlightException("an identical request is still executing", 4);
    final JSONObject error = streamedQueryError();
    assertThat(error.getInt("status")).as("error line: %s", error).isEqualTo(409);
    assertThat(error.getLong("retryAfter")).as("error line: %s", error).isEqualTo(4L);
  }

  @Test
  void aFailureWithNoBackOffLeavesTheMemberOut() throws Exception {
    failure = () -> new IllegalStateException("engine fault");
    final JSONObject error = streamedQueryError();
    assertThat(error.getInt("status")).as("error line: %s", error).isEqualTo(500);
    assertThat(error.has("retryAfter")).as("error line: %s", error).isFalse();
  }

  // ---------------------------------------------------------------------------------------------------------------
  // #8875: message concealment on the query stream
  // ---------------------------------------------------------------------------------------------------------------

  @Test
  void productionModeConcealsTheRawMessageOnTheQueryStream() throws Exception {
    failure = () -> new IllegalStateException("cannot open " + SECRET);
    getServer(0).getConfiguration().setValue(GlobalConfiguration.SERVER_MODE, "production");
    final JSONObject error = streamedQueryError();
    assertThat(error.getInt("status")).as("error line: %s", error).isEqualTo(500);
    assertThat(error.toString()).as("no member may leak the raw text").doesNotContain(SECRET);
    // The classified label, the same value the buffered body puts in 'error'
    assertThat(error.getString("message")).isNotBlank().isEqualTo(error.getString("error"));
    assertThat(error.has("detail")).isFalse();
    // The bounded wire contract members stay
    assertThat(error.getString("exception")).isEqualTo(IllegalStateException.class.getName());
  }

  @Test
  void productionModeStillCarriesTheBackOff() throws Exception {
    failure = () -> new RetryLaterException("leader at " + SECRET + " is installing a snapshot", 3);
    getServer(0).getConfiguration().setValue(GlobalConfiguration.SERVER_MODE, "production");
    final JSONObject error = streamedQueryError();
    assertThat(error.getInt("status")).as("error line: %s", error).isEqualTo(503);
    assertThat(error.getLong("retryAfter")).isEqualTo(3L);
    assertThat(error.getString("message")).doesNotContain(SECRET);
  }

  /** Development mode is unchanged for every consumer that reads 'message', and gains the cause chain in 'detail'. */
  @Test
  void developmentModeKeepsTheRawMessage() throws Exception {
    failure = () -> new IllegalStateException("cannot open " + SECRET);
    getServer(0).getConfiguration().setValue(GlobalConfiguration.SERVER_MODE, "development");
    final JSONObject error = streamedQueryError();
    assertThat(error.getString("message")).isEqualTo("cannot open " + SECRET);
    assertThat(error.getString("detail")).contains(SECRET);
  }

  // ---------------------------------------------------------------------------------------------------------------
  // #8880: retryAfter on the batch stream
  // ---------------------------------------------------------------------------------------------------------------

  @Test
  void aRetryLaterFailureCarriesItsBackOffOnTheBatchStream() throws Exception {
    // Thrown while GraphBatch.close() finalizes the vertices: after the progress lines, so the 200 is on the wire
    getServer(0).getDatabase(getDatabaseName()).getSchema().getType(VERTEX_TYPE).getEvents()
        .registerListener((BeforeRecordUpdateListener) record -> {
          if (batchArmed)
            throw new RetryLaterException("snapshot install in progress", 9);
          return true;
        });
    batchArmed = true;
    final List<JSONObject> events = postBatch();

    assertThat(events.getFirst().has("progress")).as("the failure must come after the stream started: %s", events)
        .isTrue();
    final JSONObject last = events.getLast();
    assertThat(last.has("error")).as("terminal line: %s", last).isTrue();
    final JSONObject error = last.getJSONObject("error");
    assertThat(error.getInt("status")).as("error line: %s", error).isEqualTo(503);
    assertThat(error.getLong("retryAfter")).as("error line: %s", error).isEqualTo(9L);
  }

  // ---------------------------------------------------------------------------------------------------------------

  private SQLQueryEngine sqlEngine() {
    return (SQLQueryEngine) getServerDatabase(0, getDatabaseName()).getQueryEngine("sql");
  }

  private JSONObject streamedQueryError() throws Exception {
    final JSONObject payload = new JSONObject().put("language", "sql")
        .put("command", "SELECT idx, " + FUNCTION + "() AS f FROM " + TYPE_NAME);
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(getServerHttpUrl(0, "/api/v1/query/" + getDatabaseName())))
        .timeout(Duration.ofSeconds(60))
        .header("Authorization", authorization())
        .header("Content-Type", "application/json")
        .header("Accept", NDJSON)
        .POST(HttpRequest.BodyPublishers.ofString(payload.toString()))
        .build();
    final List<JSONObject> events = readAllEvents(send(request));
    assertThat(events.getFirst().has("record")).as("the failure must come after a row was already sent").isTrue();
    assertThat(events.stream().anyMatch(e -> e.has("stats"))).isFalse();
    assertThat(events.getLast().has("error")).as("terminal line: %s", events.getLast()).isTrue();
    return events.getLast().getJSONObject("error");
  }

  private List<JSONObject> postBatch() throws Exception {
    final StringBuilder payload = new StringBuilder();
    for (int i = 0; i < 3; i++)
      payload.append("{\"@type\":\"vertex\",\"@class\":\"").append(VERTEX_TYPE).append("\",\"@id\":\"v").append(i)
          .append("\",\"id\":").append(i).append("}\n");
    // The edge makes GraphBatch.close() rewrite the vertices' head chunk pointers, which is where the listener throws
    payload.append("{\"@type\":\"edge\",\"@class\":\"").append(EDGE_TYPE).append("\",\"@from\":\"v0\",\"@to\":\"v1\"}\n");
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(getServerHttpUrl(0, "/api/v1/batch/" + getDatabaseName())))
        .timeout(Duration.ofSeconds(60))
        .header("Authorization", authorization())
        .header("Content-Type", NDJSON)
        .header("Accept", NDJSON)
        .POST(HttpRequest.BodyPublishers.ofString(payload.toString()))
        .build();
    return readAllEvents(send(request));
  }

  private static String authorization() {
    return "Basic " + Base64.getEncoder()
        .encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8));
  }

  private static HttpResponse<InputStream> send(final HttpRequest request) throws Exception {
    return HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(30)).build()
        .send(request, HttpResponse.BodyHandlers.ofInputStream());
  }

  private static List<JSONObject> readAllEvents(final HttpResponse<InputStream> response) throws Exception {
    assertThat(response.statusCode()).isEqualTo(200);
    final List<JSONObject> events = new ArrayList<>();
    try (final BufferedReader reader = new BufferedReader(new InputStreamReader(response.body(), StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null)
        if (!line.isBlank())
          events.add(new JSONObject(line));
    }
    return events;
  }
}
