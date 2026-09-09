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
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.http.handler.AbstractQueryHandler;
import io.micrometer.core.instrument.Metrics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7306: the HTTP query and command endpoints materialized the whole result set into one JSON object before
 * the first byte reached the client, so a large result was fully resident in the server heap regardless of how
 * much of it the client meant to consume. gRPC had {@code StreamQuery}; HTTP had no counterpart.
 * <p>
 * The load-bearing test here is {@link #theFirstRowReachesTheClientBeforeTheLastOneIsProduced()}. Everything else
 * in this class would pass just as happily against the buffered implementation - a test that only checks the rows
 * came back in NDJSON framing proves the framing and nothing about when the bytes moved.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue7306StreamingQueryIT extends BaseGraphServerTest {

  /**
   * Rows in the large fixture. Sized so the encoded body is several megabytes: the interleaving test below relies
   * on the body being far larger than every buffer between the two ends, so that a client which reads one line and
   * stops leaves the server demonstrably unable to have finished writing.
   */
  private static final int LARGE_ROW_COUNT = 20_000;

  /** Padding per row, to make each line big enough that {@link #LARGE_ROW_COUNT} rows exceed any socket buffer. */
  private static final String PADDING = "x".repeat(400);

  private final HttpClient client = HttpClient.newHttpClient();

  @BeforeEach
  void seedRows() {
    final Database db = getServerDatabase(0, getDatabaseName());
    if (db.getSchema().existsType("S7306Row"))
      return;

    db.transaction(() -> {
      db.command("sql", "CREATE DOCUMENT TYPE S7306Row");
      db.command("sql", "CREATE PROPERTY S7306Row.idx INTEGER");
      db.command("sql", "CREATE PROPERTY S7306Row.pad STRING");
    });

    db.begin();
    for (int i = 0; i < LARGE_ROW_COUNT; i++) {
      db.newDocument("S7306Row").set("idx", i).set("pad", PADDING).save();
      if (i % 5_000 == 0) {
        db.commit();
        db.begin();
      }
    }
    db.commit();

    db.transaction(() -> {
      db.command("sql", "CREATE DOCUMENT TYPE S7306Small");
      db.command("sql", "CREATE PROPERTY S7306Small.name STRING");
      db.command("sql", "INSERT INTO S7306Small SET name = 'alpha'");
      db.command("sql", "INSERT INTO S7306Small SET name = 'beta'");
    });
  }

  // --------------------------------------------------------------------------------------------
  // The property the issue actually asked for
  // --------------------------------------------------------------------------------------------

  /**
   * Proves the response is incremental rather than merely newline-framed.
   * <p>
   * The client reads exactly one line and then stops reading. The server counts every row as it writes it, so at
   * that instant the counter says how much of the result the server had produced when the client already held row
   * one. Against a buffered implementation that number would be the whole result set - the first byte cannot leave
   * before the last row is serialized - so the assertion below fails. Against a streaming one it stalls at
   * whatever the buffers between the two ends hold, which for a body of this size is a small fraction.
   * <p>
   * There is no timing in this test, deliberately: the bound is on rows produced, not on how long anything took,
   * so a stalled JVM makes it no more and no less true.
   */
  @Test
  void theFirstRowReachesTheClientBeforeTheLastOneIsProduced() throws Exception {
    final double before = streamedRows();

    final HttpResponse<InputStream> response = client.send(
        streamingQueryRequest("SELECT FROM S7306Row"), HttpResponse.BodyHandlers.ofInputStream());

    assertThat(response.statusCode()).isEqualTo(200);
    assertThat(response.headers().firstValue("Content-Type").orElse(""))
        .isEqualTo(AbstractQueryHandler.NDJSON_CONTENT_TYPE);

    try (final InputStream body = response.body();
         final BufferedReader reader = new BufferedReader(new InputStreamReader(body, StandardCharsets.UTF_8))) {
      final JSONObject firstLine = new JSONObject(reader.readLine());
      assertThat(firstLine.has("result"))
          .as("the first line of the stream is a result row")
          .isTrue();

      final double producedWhenTheClientHadRowOne = streamedRows() - before;
      assertThat(producedWhenTheClientHadRowOne)
          .as("the server had produced the whole result set before the client saw its first row, "
              + "which is what a buffered response does")
          .isLessThan(LARGE_ROW_COUNT);
      assertThat(producedWhenTheClientHadRowOne)
          .as("at least the row the client is holding must have been produced")
          .isGreaterThanOrEqualTo(1);
    }
  }

  /**
   * The buffered path refuses a result above {@code SERVER_HTTP_QUERY_MAX_RESULT_ROWS} with 413 because it has to
   * hold the whole thing in the heap before it knows how big it is (issue #5719). The streamed path holds nothing,
   * so the same ceiling caps the stream and is reported through the summary's {@code truncated} flag instead of
   * failing a response whose first rows the client may already have consumed.
   * <p>
   * This is a behavioural difference caused by materialization, not a heap measurement: it is deterministic, which
   * an assertion on used memory would not be.
   */
  @Test
  void theCeilingThatRefusesABufferedResponseOnlyCapsAStreamedOne() throws Exception {
    final int ceiling = getServer(0).getConfiguration()
        .getValueAsInteger(GlobalConfiguration.SERVER_HTTP_QUERY_MAX_RESULT_ROWS);
    getServer(0).getConfiguration().setValue(GlobalConfiguration.SERVER_HTTP_QUERY_MAX_RESULT_ROWS, 100);
    try {
      final HttpResponse<String> buffered = client.send(
          bufferedQueryRequest("SELECT FROM S7306Row LIMIT " + LARGE_ROW_COUNT),
          HttpResponse.BodyHandlers.ofString());
      assertThat(buffered.statusCode())
          .as("a buffered response above the ceiling is refused, because it cannot be built to be measured")
          .isEqualTo(413);

      final List<JSONObject> lines = readStream("SELECT FROM S7306Row LIMIT " + LARGE_ROW_COUNT);
      final JSONObject summary = lines.get(lines.size() - 1).getJSONObject("summary");
      assertThat(lines).hasSize(101);
      assertThat(summary.getInt("returned")).isEqualTo(100);
      assertThat(summary.getBoolean("truncated"))
          .as("the stream is capped and says so, rather than being refused")
          .isTrue();
    } finally {
      getServer(0).getConfiguration().setValue(GlobalConfiguration.SERVER_HTTP_QUERY_MAX_RESULT_ROWS, ceiling);
    }
  }

  // --------------------------------------------------------------------------------------------
  // Framing, on each of the three entry points
  // --------------------------------------------------------------------------------------------

  @Test
  void postQueryStreamsOneResultPerLineFollowedByASummary() throws Exception {
    final List<JSONObject> lines = readStream("SELECT FROM S7306Small ORDER BY name");

    assertThat(lines).hasSize(3);
    assertThat(lines.get(0).getJSONObject("result").getString("name")).isEqualTo("alpha");
    assertThat(lines.get(1).getJSONObject("result").getString("name")).isEqualTo("beta");

    final JSONObject summary = lines.get(2).getJSONObject("summary");
    assertThat(summary.getInt("returned")).isEqualTo(2);
    assertThat(summary.getBoolean("truncated")).isFalse();
    assertThat(summary.getString("user")).isEqualTo("root");
  }

  @Test
  void postCommandStreamsTheSameWay() throws Exception {
    final List<JSONObject> lines = readStream("command", "SELECT FROM S7306Small ORDER BY name");

    assertThat(lines).hasSize(3);
    assertThat(lines.get(0).getJSONObject("result").getString("name")).isEqualTo("alpha");
    assertThat(lines.get(2).getJSONObject("summary").getInt("returned")).isEqualTo(2);
  }

  @Test
  void getQueryStreamsTheSameWay() throws Exception {
    final String url = "http://localhost:" + getServer(0).getHttpServer().getPort() + "/api/v1/query/"
        + getDatabaseName() + "/sql/"
        // %20 rather than URLEncoder's '+': this is a PATH segment, where a plus is a literal plus, and the
        // command would reach the handler as "SELECT+FROM+..." and be rejected as a parse error.
        + URLEncoder.encode("SELECT FROM S7306Small ORDER BY name", StandardCharsets.UTF_8).replace("+", "%20");

    final HttpResponse<InputStream> response = client.send(
        authenticated(url).GET().header("Accept", AbstractQueryHandler.NDJSON_CONTENT_TYPE).build(),
        HttpResponse.BodyHandlers.ofInputStream());

    assertThat(response.statusCode()).isEqualTo(200);
    assertThat(response.headers().firstValue("Content-Type").orElse(""))
        .isEqualTo(AbstractQueryHandler.NDJSON_CONTENT_TYPE);

    final List<JSONObject> lines = drain(response.body());
    assertThat(lines).hasSize(3);
    assertThat(lines.get(0).getJSONObject("result").getString("name")).isEqualTo("alpha");
    assertThat(lines.get(2).getJSONObject("summary").getInt("returned")).isEqualTo(2);
  }

  @Test
  void theJsonlAliasSelectsTheSameStream() throws Exception {
    final HttpRequest request = authenticated(queryUrl("query"))
        .POST(HttpRequest.BodyPublishers.ofString(new JSONObject()
            .put("language", "sql").put("command", "SELECT FROM S7306Small").toString()))
        .header("Content-Type", "application/json")
        .header("Accept", AbstractQueryHandler.JSONL_CONTENT_TYPE)
        .build();

    final HttpResponse<InputStream> response = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
    assertThat(response.statusCode()).isEqualTo(200);
    assertThat(drain(response.body())).hasSize(3);
  }

  @Test
  void anEmptyResultStreamsTheSummaryAlone() throws Exception {
    final List<JSONObject> lines = readStream("SELECT FROM S7306Small WHERE name = 'nothing-matches-this'");

    assertThat(lines).hasSize(1);
    assertThat(lines.get(0).getJSONObject("summary").getInt("returned")).isEqualTo(0);
    assertThat(lines.get(0).getJSONObject("summary").getBoolean("truncated")).isFalse();
  }

  /**
   * EXPLAIN carries no rows: its whole payload is the plan. It still answers in the media type the caller
   * negotiated - a request that asked for a stream must never come back as a buffered object in a different
   * content type - with the plan on the summary line.
   */
  @Test
  void explainStreamsItsPlanOnTheSummaryLine() throws Exception {
    final List<JSONObject> lines = readStream("command", "EXPLAIN SELECT FROM S7306Small");

    assertThat(lines).hasSize(1);
    final JSONObject summary = lines.get(0).getJSONObject("summary");
    assertThat(summary.getString("explain")).isNotBlank();
    assertThat(summary.has("explainPlan")).isTrue();
    assertThat(summary.getInt("returned")).isEqualTo(0);
  }

  /**
   * The graph and studio serializers build response-level vertex and edge arrays de-duplicated across the whole
   * result, so no row can be emitted before the last one is read. Refusing names the conflict; answering with a
   * buffered body in a content type the caller did not ask for would be the silent surprise negotiation exists to
   * prevent.
   */
  @Test
  void aSerializerThatCannotStreamIsRefusedRatherThanBuffered() throws Exception {
    final HttpRequest request = authenticated(queryUrl("query"))
        .POST(HttpRequest.BodyPublishers.ofString(new JSONObject()
            .put("language", "sql").put("command", "SELECT FROM S7306Small")
            .put("serializer", "graph").toString()))
        .header("Content-Type", "application/json")
        .header("Accept", AbstractQueryHandler.NDJSON_CONTENT_TYPE)
        .build();

    final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
    assertThat(response.statusCode()).isEqualTo(400);
    assertThat(response.body()).contains("cannot be streamed");
  }

  // --------------------------------------------------------------------------------------------
  // Only a read-only command may be streamed
  // --------------------------------------------------------------------------------------------

  /**
   * {@code POST /api/v1/command} runs inside {@code DatabaseAbstractHandler}'s auto-commit wrapper: the
   * transaction commits AFTER {@code execute()} returns, and a conflict on that commit re-runs {@code execute()}
   * up to the configured retry count. A streamed write would therefore hand the client 200 and the rows before
   * the commit that could still fail, and a retry would write a second copy of the whole stream into a body that
   * has already gone out - two concatenated result sets no client could detect.
   * <p>
   * So a write is refused, and the message says where to go instead rather than only that the request was bad.
   */
  @Test
  void aWriteCommandIsRefusedRatherThanStreamedAheadOfItsCommit() throws Exception {
    final HttpRequest request = authenticated(queryUrl("command"))
        .POST(HttpRequest.BodyPublishers.ofString(new JSONObject()
            .put("language", "sql")
            .put("command", "INSERT INTO S7306Small SET name = 'streamed-write'").toString()))
        .header("Content-Type", "application/json")
        .header("Accept", AbstractQueryHandler.NDJSON_CONTENT_TYPE)
        .build();

    final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
    assertThat(response.statusCode()).isEqualTo(400);
    assertThat(response.body()).contains("Only a read-only command can be streamed");

    // The refusal happens before the command runs: a rejected request must not leave the row behind.
    final HttpResponse<String> check = client.send(
        bufferedQueryRequest("SELECT FROM S7306Small WHERE name = 'streamed-write'"),
        HttpResponse.BodyHandlers.ofString());
    assertThat(new JSONObject(check.body()).getJSONArray("result").length()).isZero();
  }

  /**
   * The same write, without the streaming Accept header, must still work exactly as before. Without this the
   * refusal above could be passing because the write was broken rather than because it was refused.
   */
  @Test
  void theSameWriteStillSucceedsOnTheBufferedPath() throws Exception {
    final HttpRequest request = authenticated(queryUrl("command"))
        .POST(HttpRequest.BodyPublishers.ofString(new JSONObject()
            .put("language", "sql")
            .put("command", "INSERT INTO S7306Small SET name = 'buffered-write'").toString()))
        .header("Content-Type", "application/json")
        .build();

    final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
    assertThat(response.statusCode()).isEqualTo(200);

    final HttpResponse<String> check = client.send(
        bufferedQueryRequest("SELECT FROM S7306Small WHERE name = 'buffered-write'"),
        HttpResponse.BodyHandlers.ofString());
    assertThat(new JSONObject(check.body()).getJSONArray("result").length()).isEqualTo(1);
  }

  /**
   * A DDL statement is not a write of rows but it is not idempotent either, and it commits schema changes through
   * the same wrapper, so it takes the same refusal.
   */
  @Test
  void aDdlCommandIsRefusedToo() throws Exception {
    final HttpRequest request = authenticated(queryUrl("command"))
        .POST(HttpRequest.BodyPublishers.ofString(new JSONObject()
            .put("language", "sql")
            .put("command", "CREATE DOCUMENT TYPE S7306NeverCreated").toString()))
        .header("Content-Type", "application/json")
        .header("Accept", AbstractQueryHandler.NDJSON_CONTENT_TYPE)
        .build();

    final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
    assertThat(response.statusCode()).isEqualTo(400);
    assertThat(getServerDatabase(0, getDatabaseName()).getSchema().existsType("S7306NeverCreated")).isFalse();
  }

  // --------------------------------------------------------------------------------------------
  // The default response is unchanged
  // --------------------------------------------------------------------------------------------

  /**
   * Pins the buffered response shape. Every existing client, the Studio webapp included, depends on it, so the
   * streaming work is additive only for as long as this stays true: the keys, the nesting and the values below are
   * what the endpoint answered before #7306 and must keep answering.
   */
  @Test
  void theBufferedResponseIsUnchangedWhenNothingIsNegotiated() throws Exception {
    final HttpResponse<String> response = client.send(
        bufferedQueryRequest("SELECT name FROM S7306Small ORDER BY name"), HttpResponse.BodyHandlers.ofString());

    assertThat(response.statusCode()).isEqualTo(200);
    assertThat(response.headers().firstValue("Content-Type").orElse("")).contains("application/json");

    final JSONObject body = new JSONObject(response.body());
    assertThat(body.keySet()).containsExactlyInAnyOrder("user", "result", "limit", "returned", "truncated");
    assertThat(body.getString("user")).isEqualTo("root");
    assertThat(body.getInt("returned")).isEqualTo(2);
    assertThat(body.getBoolean("truncated")).isFalse();
    assertThat(body.getJSONArray("result").getJSONObject(0).getString("name")).isEqualTo("alpha");
    assertThat(body.getJSONArray("result").getJSONObject(1).getString("name")).isEqualTo("beta");
  }

  /**
   * An {@code Accept} naming only {@code application/json} must take the buffered path, exactly as omitting the
   * header does. Without this the negotiation could be implemented as "anything but nothing streams", which would
   * change the answer for every client that sends a polite explicit Accept.
   */
  @Test
  void anExplicitJsonAcceptStillTakesTheBufferedPath() throws Exception {
    final HttpRequest request = authenticated(queryUrl("query"))
        .POST(HttpRequest.BodyPublishers.ofString(new JSONObject()
            .put("language", "sql").put("command", "SELECT name FROM S7306Small ORDER BY name").toString()))
        .header("Content-Type", "application/json")
        .header("Accept", "application/json")
        .build();

    final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
    assertThat(response.statusCode()).isEqualTo(200);
    assertThat(response.headers().firstValue("Content-Type").orElse("")).contains("application/json");
    assertThat(new JSONObject(response.body()).getJSONArray("result").length()).isEqualTo(2);
  }

  /**
   * The two responses describe the same rows, so the streamed lines must carry exactly the objects the buffered
   * {@code result} array carries. A drift here would mean a client that switched to streaming silently started
   * seeing differently-shaped rows.
   */
  @Test
  void theStreamedRowsAreTheSameObjectsTheBufferedResultArrayCarries() throws Exception {
    final String query = "SELECT name FROM S7306Small ORDER BY name";

    final JSONObject buffered = new JSONObject(
        client.send(bufferedQueryRequest(query), HttpResponse.BodyHandlers.ofString()).body());
    final List<JSONObject> streamed = readStream(query);

    assertThat(streamed).hasSize(buffered.getJSONArray("result").length() + 1);
    for (int i = 0; i < buffered.getJSONArray("result").length(); i++)
      assertThat(streamed.get(i).getJSONObject("result").toString())
          .isEqualTo(buffered.getJSONArray("result").getJSONObject(i).toString());
  }

  // --------------------------------------------------------------------------------------------

  private static double streamedRows() {
    return Metrics.counter(AbstractQueryHandler.STREAMED_ROWS_METRIC).count();
  }

  private String queryUrl(final String route) {
    return "http://localhost:" + getServer(0).getHttpServer().getPort() + "/api/v1/" + route + "/"
        + getDatabaseName();
  }

  private HttpRequest streamingQueryRequest(final String query) {
    return authenticated(queryUrl("query"))
        .POST(HttpRequest.BodyPublishers.ofString(new JSONObject()
            .put("language", "sql").put("command", query).toString()))
        .header("Content-Type", "application/json")
        .header("Accept", AbstractQueryHandler.NDJSON_CONTENT_TYPE)
        .build();
  }

  private HttpRequest bufferedQueryRequest(final String query) {
    return authenticated(queryUrl("query"))
        .POST(HttpRequest.BodyPublishers.ofString(new JSONObject()
            .put("language", "sql").put("command", query).toString()))
        .header("Content-Type", "application/json")
        .build();
  }

  private List<JSONObject> readStream(final String query) throws Exception {
    return readStream("query", query);
  }

  private List<JSONObject> readStream(final String route, final String query) throws Exception {
    final HttpRequest request = authenticated(queryUrl(route))
        .POST(HttpRequest.BodyPublishers.ofString(new JSONObject()
            .put("language", "sql").put("command", query).toString()))
        .header("Content-Type", "application/json")
        .header("Accept", AbstractQueryHandler.NDJSON_CONTENT_TYPE)
        .build();

    final HttpResponse<InputStream> response = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
    assertThat(response.statusCode()).isEqualTo(200);
    return drain(response.body());
  }

  private static List<JSONObject> drain(final InputStream body) throws Exception {
    final List<JSONObject> lines = new ArrayList<>();
    try (final BufferedReader reader = new BufferedReader(new InputStreamReader(body, StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null)
        if (!line.isBlank())
          lines.add(new JSONObject(line));
    }
    return lines;
  }

  /**
   * The port is read from the running server: {@code arcadedb.server.httpIncomingPort} defaults to the RANGE
   * 2480-2489, so with anything already listening on 2480 the test server binds 2481 and a request hard-coded to
   * 2480 reaches the other process and fails as an authentication error rather than as a port conflict.
   */
  private HttpRequest.Builder authenticated(final String url) {
    return HttpRequest.newBuilder()
        .uri(URI.create(url))
        .setHeader("Authorization", "Basic "
            + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
  }
}
