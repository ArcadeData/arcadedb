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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #5711: a response the row cap cut short must never be indistinguishable from a complete one, and the
 * cap must never override a limit the caller stated itself - neither the {@code limit} field of the request nor
 * the LIMIT clause of the query.
 * <p>
 * The server default cap is lowered to {@link #CAP} rows for these tests so the same semantics can be checked
 * without materializing 20,000 records.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class HttpQueryTruncationIT extends BaseGraphServerTest {
  private static final int    CAP        = 50;
  private static final int    TOTAL_ROWS = 60;
  private static final String TYPE_NAME  = "TruncatedRow";

  private final HttpClient client = HttpClient.newHttpClient();

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    config.setValue(GlobalConfiguration.SERVER_HTTP_QUERY_DEFAULT_LIMIT, CAP);
  }

  @BeforeEach
  void createRows() {
    final Database database = getServerDatabase(0, getDatabaseName());
    // A vertex type so the same rows are reachable from SQL and from Cypher.
    database.getSchema().createVertexType(TYPE_NAME);
    database.transaction(() -> {
      for (int i = 0; i < TOTAL_ROWS; i++)
        database.newVertex(TYPE_NAME).set("i", i).save();
    });
  }

  @Test
  void queryLimitAboveTheDefaultCapIsHonored() throws Exception {
    final JSONObject response = query(new JSONObject()
        .put("language", "sql")
        .put("command", "SELECT i FROM " + TYPE_NAME + " LIMIT " + TOTAL_ROWS));

    assertThat(response.getJSONArray("result").length()).isEqualTo(TOTAL_ROWS);
    assertThat(response.getInt("returned")).isEqualTo(TOTAL_ROWS);
    assertThat(response.getInt("limit")).isEqualTo(TOTAL_ROWS);
    assertThat(response.getBoolean("truncated")).isFalse();
  }

  @Test
  void truncationByTheDefaultCapIsReported() throws Exception {
    final JSONObject response = query(new JSONObject()
        .put("language", "sql")
        .put("command", "SELECT i FROM " + TYPE_NAME));

    assertThat(response.getJSONArray("result").length()).isEqualTo(CAP);
    assertThat(response.getInt("returned")).isEqualTo(CAP);
    assertThat(response.getInt("limit")).isEqualTo(CAP);
    assertThat(response.getBoolean("truncated")).isTrue();
  }

  @Test
  void aCompleteResultIsNeverFlaggedAsTruncated() throws Exception {
    // The result ends exactly at the cap: no row was left behind, so nothing must be reported.
    final JSONObject response = query(new JSONObject()
        .put("language", "sql")
        .put("command", "SELECT i FROM " + TYPE_NAME + " LIMIT " + CAP));

    assertThat(response.getJSONArray("result").length()).isEqualTo(CAP);
    assertThat(response.getInt("returned")).isEqualTo(CAP);
    assertThat(response.getBoolean("truncated")).isFalse();
  }

  @Test
  void aResultOfExactlyTheDefaultCapIsNotFlagged() throws Exception {
    // The boundary the 'cap + 1' probe exists for: the command states no LIMIT, so the handler pushes down
    // 'limit 51', and the engine returns exactly the 50 rows that match. Nothing was left behind, so the
    // response must not claim otherwise - this is the false-positive guard for the probe itself.
    final JSONObject response = query(new JSONObject()
        .put("language", "sql")
        .put("command", "SELECT i FROM " + TYPE_NAME + " WHERE i < " + CAP));

    assertThat(response.getJSONArray("result").length()).isEqualTo(CAP);
    assertThat(response.getInt("returned")).isEqualTo(CAP);
    assertThat(response.getInt("limit")).isEqualTo(CAP);
    assertThat(response.getBoolean("truncated")).isFalse();
  }

  @Test
  void theRequestLimitWinsOverTheQueryLimitAndIsReported() throws Exception {
    final JSONObject response = query(new JSONObject()
        .put("language", "sql")
        .put("command", "SELECT i FROM " + TYPE_NAME + " LIMIT " + TOTAL_ROWS)
        .put("limit", 10));

    assertThat(response.getJSONArray("result").length()).isEqualTo(10);
    assertThat(response.getInt("returned")).isEqualTo(10);
    assertThat(response.getInt("limit")).isEqualTo(10);
    assertThat(response.getBoolean("truncated")).isTrue();
  }

  @Test
  void anUnlimitedRequestLimitReturnsEveryRow() throws Exception {
    final JSONObject response = query(new JSONObject()
        .put("language", "sql")
        .put("command", "SELECT i FROM " + TYPE_NAME)
        .put("limit", -1));

    assertThat(response.getJSONArray("result").length()).isEqualTo(TOTAL_ROWS);
    assertThat(response.getInt("returned")).isEqualTo(TOTAL_ROWS);
    assertThat(response.getInt("limit")).isEqualTo(-1);
    assertThat(response.getBoolean("truncated")).isFalse();
  }

  @Test
  void aParameterizedLimitIsHonoredOnEveryExecution() throws Exception {
    // The cap is read back from the execution plan, and a plan reports the LIMIT value of the execution it was
    // built for. Today a plan carrying a LIMIT step is never cached, so the value is always fresh; the cap is
    // nevertheless never lowered below the configured default, so a plan that did come back from the cache
    // could not cut the second query down to the 5 rows the first one asked for.
    final String command = "SELECT i FROM " + TYPE_NAME + " LIMIT :max";

    final JSONObject first = query(new JSONObject()
        .put("language", "sql")
        .put("command", command)
        .put("params", new JSONObject().put("max", 5)));
    assertThat(first.getJSONArray("result").length()).isEqualTo(5);
    assertThat(first.getBoolean("truncated")).isFalse();

    final JSONObject second = query(new JSONObject()
        .put("language", "sql")
        .put("command", command)
        .put("params", new JSONObject().put("max", 40)));
    assertThat(second.getJSONArray("result").length()).isEqualTo(40);
    assertThat(second.getBoolean("truncated")).isFalse();
  }

  @Test
  void aZeroRequestLimitMeansUnlimited() throws Exception {
    // 0 means unlimited in the serializer, so it must mean unlimited on the way in too: it used to be pushed
    // down as a literal LIMIT 0 and return nothing.
    final JSONObject response = query(new JSONObject()
        .put("language", "sql")
        .put("command", "SELECT i FROM " + TYPE_NAME)
        .put("limit", 0));

    assertThat(response.getJSONArray("result").length()).isEqualTo(TOTAL_ROWS);
    assertThat(response.getInt("limit")).isEqualTo(-1);
    assertThat(response.getBoolean("truncated")).isFalse();
  }

  @Test
  void theGraphSerializerReportsTruncation() throws Exception {
    final JSONObject response = query(new JSONObject()
        .put("language", "sql")
        .put("command", "SELECT FROM " + TYPE_NAME)
        .put("serializer", "graph"));

    assertThat(response.getJSONObject("result").getJSONArray("vertices").length()).isEqualTo(CAP);
    assertThat(response.getInt("returned")).isEqualTo(CAP);
    assertThat(response.getBoolean("truncated")).isTrue();
  }

  @Test
  void aRequestLimitTooLargeForAnIntIsRejected() throws Exception {
    // Narrowing it with Number.intValue() would wrap 3000000000 to a negative value, which reads as
    // "unlimited" and silently turns off the cap the field governs: it is a client error instead.
    final int status = postStatus("query", new JSONObject()
        .put("language", "sql")
        .put("command", "SELECT i FROM " + TYPE_NAME)
        .put("limit", 3_000_000_000L));

    assertThat(status).isEqualTo(400);
  }

  @Test
  void theGetEndpointRejectsAnUnusableLimitTheSameWayThePostEndpointDoes() throws Exception {
    // Both surfaces must answer the same way to the same unusable value: a client error, not a server error.
    assertThat(getStatus("SELECT i FROM " + TYPE_NAME, "abc")).isEqualTo(400);
    assertThat(getStatus("SELECT i FROM " + TYPE_NAME, "3000000000")).isEqualTo(400);
  }

  @Test
  void theHttpAndTheEmbeddedSurfaceAgreeOnTheSameQuery() throws Exception {
    final String command = "SELECT i FROM " + TYPE_NAME + " LIMIT " + TOTAL_ROWS;

    int embeddedRows = 0;
    try (final ResultSet resultSet = getServerDatabase(0, getDatabaseName()).query("sql", command)) {
      while (resultSet.hasNext()) {
        resultSet.next();
        embeddedRows++;
      }
    }

    final JSONObject response = query(new JSONObject().put("language", "sql").put("command", command));

    assertThat(embeddedRows).isEqualTo(TOTAL_ROWS);
    assertThat(response.getInt("returned")).isEqualTo(embeddedRows);
  }

  @Test
  void theStudioSerializerReportsTruncation() throws Exception {
    final JSONObject response = query(new JSONObject()
        .put("language", "sql")
        .put("command", "SELECT i FROM " + TYPE_NAME)
        .put("serializer", "studio"));

    assertThat(response.getJSONObject("result").getJSONArray("records").length()).isEqualTo(CAP);
    assertThat(response.getInt("returned")).isEqualTo(CAP);
    assertThat(response.getBoolean("truncated")).isTrue();
  }

  @Test
  void theDefaultSerializerEmitsExactlyTheCap() throws Exception {
    // Any unknown serializer name falls back to the default one, which used to emit one row above the cap.
    final JSONObject response = query(new JSONObject()
        .put("language", "sql")
        .put("command", "SELECT i FROM " + TYPE_NAME)
        .put("serializer", "json"));

    assertThat(response.getJSONArray("result").length()).isEqualTo(CAP);
    assertThat(response.getInt("returned")).isEqualTo(CAP);
    assertThat(response.getBoolean("truncated")).isTrue();
  }

  @Test
  void aCypherQueryTruncatedByTheDefaultCapIsReported() throws Exception {
    // A Cypher query with no LIMIT of its own is capped by the default, and the response says so instead of looking
    // complete.
    final JSONObject response = query(new JSONObject()
        .put("language", "opencypher")
        .put("command", "MATCH (n:" + TYPE_NAME + ") RETURN n.i AS i"));

    assertThat(response.getJSONArray("result").length()).isEqualTo(CAP);
    assertThat(response.getBoolean("truncated")).isTrue();
  }

  /**
   * Issue #12154: a LIMIT written in openCypher raises the default cap as SQL's does, whether it is a literal or a
   * parameter, on both endpoints, and on the profiled and write paths too.
   */
  @Test
  void aCypherLimitAboveTheDefaultCapIsHonored() throws Exception {
    for (final String endpoint : List.of("query", "command")) {
      assertCypherRows(post(endpoint, new JSONObject()
          .put("language", "opencypher")
          .put("command", "MATCH (n:" + TYPE_NAME + ") RETURN n.i AS i LIMIT " + TOTAL_ROWS)), TOTAL_ROWS, TOTAL_ROWS);

      assertCypherRows(post(endpoint, new JSONObject()
          .put("language", "opencypher")
          .put("command", "MATCH (n:" + TYPE_NAME + ") RETURN n.i AS i LIMIT $p")
          .put("params", new JSONObject().put("p", TOTAL_ROWS - 2))), TOTAL_ROWS - 2, TOTAL_ROWS - 2);

      assertCypherRows(post(endpoint, new JSONObject()
          .put("language", "cypher")
          .put("command", "UNWIND range(1, 1000) AS i RETURN i LIMIT " + TOTAL_ROWS)), TOTAL_ROWS, TOTAL_ROWS);
    }
  }

  @Test
  void aCypherLimitIsOnlyARaiseNeverACut() throws Exception {
    // A LIMIT smaller than the default cap is enforced by the engine: the cap reported stays the default
    assertCypherRows(query(new JSONObject()
        .put("language", "opencypher")
        .put("command", "MATCH (n:" + TYPE_NAME + ") RETURN n.i AS i LIMIT 5")), 5, CAP);

    // A LIMIT above the cap on a smaller result: every row, nothing truncated
    assertCypherRows(query(new JSONObject()
        .put("language", "opencypher")
        .put("command", "MATCH (n:" + TYPE_NAME + ") WHERE n.i < 10 RETURN n.i AS i LIMIT 1000")), 10, 1000);

    // A LIMIT in a WITH bounds nothing the RETURN answers: the default cap still applies, and says so
    final JSONObject withLimit = query(new JSONObject()
        .put("language", "opencypher")
        .put("command", "MATCH (n:" + TYPE_NAME + ") WITH n LIMIT 1000 RETURN n.i AS i"));
    assertThat(withLimit.getJSONArray("result").length()).isEqualTo(CAP);
    assertThat(withLimit.getBoolean("truncated")).isTrue();

    // The request 'limit' still wins over the query's own LIMIT
    final JSONObject requestWins = query(new JSONObject()
        .put("language", "opencypher")
        .put("command", "MATCH (n:" + TYPE_NAME + ") RETURN n.i AS i LIMIT " + TOTAL_ROWS)
        .put("limit", 10));
    assertThat(requestWins.getJSONArray("result").length()).isEqualTo(10);
    assertThat(requestWins.getInt("limit")).isEqualTo(10);
  }

  @Test
  void aCypherLimitRaisesTheCapOnTheWriteProfiledAndUnionPaths() throws Exception {
    // A write statement is drained by the engine before it returns: its RETURN LIMIT still raises the cap
    assertCypherRows(command(new JSONObject()
        .put("language", "opencypher")
        .put("command", "MATCH (n:" + TYPE_NAME + ") SET n.seen = true RETURN n.i AS i LIMIT " + TOTAL_ROWS)), TOTAL_ROWS,
        TOTAL_ROWS);

    // The timed streaming run of 'profileExecution: detailed'
    assertCypherRows(command(new JSONObject()
        .put("language", "opencypher")
        .put("command", "MATCH (n:" + TYPE_NAME + ") RETURN n.i AS i LIMIT " + TOTAL_ROWS)
        .put("profileExecution", "detailed")), TOTAL_ROWS, TOTAL_ROWS);

    // PROFILE, which drains the rows into heap with the profile attached
    assertCypherRows(command(new JSONObject()
        .put("language", "opencypher")
        .put("command", "PROFILE MATCH (n:" + TYPE_NAME + ") RETURN n.i AS i LIMIT " + TOTAL_ROWS)), TOTAL_ROWS, TOTAL_ROWS);

    // UNION ALL of two limited branches answers up to the sum of the two
    assertCypherRows(query(new JSONObject()
        .put("language", "opencypher")
        .put("command", "MATCH (n:" + TYPE_NAME + ") RETURN n.i AS i LIMIT 30 UNION ALL MATCH (n:" + TYPE_NAME
            + ") RETURN n.i AS i LIMIT 30")), TOTAL_ROWS, TOTAL_ROWS);

    // LIMIT 0 bounds a branch to no rows: it does not unbound the union
    assertCypherRows(query(new JSONObject()
        .put("language", "opencypher")
        .put("command", "MATCH (n:" + TYPE_NAME + ") RETURN n.i AS i LIMIT 0 UNION ALL MATCH (n:" + TYPE_NAME
            + ") RETURN n.i AS i LIMIT " + TOTAL_ROWS)), TOTAL_ROWS, TOTAL_ROWS);

    // A branch without a LIMIT bounds nothing, so the union does not either
    final JSONObject unbounded = query(new JSONObject()
        .put("language", "opencypher")
        .put("command", "MATCH (n:" + TYPE_NAME + ") RETURN n.i AS i LIMIT 30 UNION ALL MATCH (n:" + TYPE_NAME
            + ") RETURN n.i AS i"));
    assertThat(unbounded.getJSONArray("result").length()).isEqualTo(CAP);
    assertThat(unbounded.getBoolean("truncated")).isTrue();
  }

  @Test
  void theGetEndpointHonorsTheCypherLimit() throws Exception {
    final String url = getServerHttpUrl("/api/v1/query/") + getDatabaseName() + "/opencypher/"
        + URLEncoder.encode("MATCH (n:" + TYPE_NAME + ") RETURN n.i AS i LIMIT " + TOTAL_ROWS, StandardCharsets.UTF_8)
        .replace("+", "%20");
    final HttpResponse<String> response = client.send(HttpRequest.newBuilder().uri(new URI(url)).GET()
        .setHeader("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
        .build(), BodyHandlers.ofString());
    assertThat(response.statusCode()).isEqualTo(200);
    assertCypherRows(new JSONObject(response.body()), TOTAL_ROWS, TOTAL_ROWS);
  }

  private static void assertCypherRows(final JSONObject response, final int rows, final int limit) {
    assertThat(response.getJSONArray("result").length()).isEqualTo(rows);
    assertThat(response.getInt("returned")).isEqualTo(rows);
    assertThat(response.getInt("limit")).isEqualTo(limit);
    assertThat(response.getBoolean("truncated")).isFalse();
  }

  @Test
  void aWriteCommandAlwaysReportsTheLimitFields() throws Exception {
    final JSONObject response = command(new JSONObject()
        .put("language", "sql")
        .put("command", "CREATE VERTEX " + TYPE_NAME + " SET i = 1000"));

    assertThat(response.getInt("returned")).isEqualTo(1);
    assertThat(response.getBoolean("truncated")).isFalse();
  }

  @Test
  void theGetEndpointHonorsTheQueryLimit() throws Exception {
    final JSONObject response = get("SELECT i FROM " + TYPE_NAME + " LIMIT " + TOTAL_ROWS, null);

    assertThat(response.getJSONArray("result").length()).isEqualTo(TOTAL_ROWS);
    assertThat(response.getInt("limit")).isEqualTo(TOTAL_ROWS);
    assertThat(response.getBoolean("truncated")).isFalse();
  }

  @Test
  void theGetEndpointReportsTruncationByTheDefaultCap() throws Exception {
    final JSONObject response = get("SELECT i FROM " + TYPE_NAME, null);

    assertThat(response.getJSONArray("result").length()).isEqualTo(CAP);
    assertThat(response.getInt("limit")).isEqualTo(CAP);
    assertThat(response.getBoolean("truncated")).isTrue();
  }

  @Test
  void theGetEndpointLimitParameterWinsOverTheQueryLimit() throws Exception {
    final JSONObject response = get("SELECT i FROM " + TYPE_NAME + " LIMIT " + TOTAL_ROWS, "10");

    assertThat(response.getJSONArray("result").length()).isEqualTo(10);
    assertThat(response.getInt("limit")).isEqualTo(10);
    assertThat(response.getBoolean("truncated")).isTrue();
  }

  @Test
  void theRemoteDriverHonorsTheQueryLimit() {
    try (final RemoteDatabase remote = newRemoteDatabase()) {
      assertThat(countRows(remote, "SELECT i FROM " + TYPE_NAME + " LIMIT " + TOTAL_ROWS)).isEqualTo(TOTAL_ROWS);
    }
  }

  @Test
  void theRemoteDriverCanRemoveTheCap() {
    try (final RemoteDatabase remote = newRemoteDatabase()) {
      assertThat(countRows(remote, "SELECT i FROM " + TYPE_NAME)).isEqualTo(CAP);

      remote.setMaxResultRows(-1);
      assertThat(countRows(remote, "SELECT i FROM " + TYPE_NAME)).isEqualTo(TOTAL_ROWS);
    }
  }

  @Test
  void theRemoteDriverWarnsOnlyWhenItDidNotAskForTheCap() {
    final CapturingHandler captured = new CapturingHandler();
    final Logger logger = Logger.getLogger(RemoteDatabase.class.getName());
    logger.addHandler(captured);
    try (final RemoteDatabase remote = newRemoteDatabase()) {
      // Nobody asked for a cap, so the truncation is a partial answer the application must hear about.
      assertThat(countRows(remote, "SELECT i FROM " + TYPE_NAME)).isEqualTo(CAP);
      assertThat(captured.messages()).anyMatch(m -> m.contains("truncated the result set"));

      // The application set its own page size: the truncation is what it asked for, so warning on every page
      // would only flood its log - the same rule the server applies to a request carrying its own 'limit'.
      captured.clear();
      remote.setMaxResultRows(10);
      assertThat(countRows(remote, "SELECT i FROM " + TYPE_NAME)).isEqualTo(10);
      assertThat(captured.messages()).noneMatch(m -> m.contains("truncated the result set"));
    } finally {
      logger.removeHandler(captured);
    }
  }

  private static int countRows(final RemoteDatabase remote, final String query) {
    int rows = 0;
    try (final ResultSet resultSet = remote.query("sql", query)) {
      while (resultSet.hasNext()) {
        resultSet.next();
        rows++;
      }
    }
    return rows;
  }

  private RemoteDatabase newRemoteDatabase() {
    return new RemoteDatabase("127.0.0.1", getServerHttpPort(), getDatabaseName(), "root", DEFAULT_PASSWORD_FOR_TESTS);
  }

  private JSONObject query(final JSONObject payload) throws Exception {
    return post("query", payload);
  }

  private JSONObject command(final JSONObject payload) throws Exception {
    return post("command", payload);
  }

  private JSONObject post(final String endpoint, final JSONObject payload) throws Exception {
    final HttpResponse<String> response = send(endpoint, payload);
    assertThat(response.statusCode()).isEqualTo(200);
    return new JSONObject(response.body());
  }

  private int postStatus(final String endpoint, final JSONObject payload) throws Exception {
    return send(endpoint, payload).statusCode();
  }

  private HttpResponse<String> send(final String endpoint, final JSONObject payload) throws Exception {
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI(getServerHttpUrl("/api/v1/") + endpoint + "/" + getDatabaseName()))
        .POST(HttpRequest.BodyPublishers.ofString(payload.toString()))
        .setHeader("Content-Type", "application/json")
        .setHeader("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
        .build();

    return client.send(request, BodyHandlers.ofString());
  }

  private JSONObject get(final String command, final String limit) throws Exception {
    final HttpResponse<String> response = sendGet(command, limit);
    assertThat(response.statusCode()).isEqualTo(200);
    return new JSONObject(response.body());
  }

  private int getStatus(final String command, final String limit) throws Exception {
    return sendGet(command, limit).statusCode();
  }

  private HttpResponse<String> sendGet(final String command, final String limit) throws Exception {
    // The command travels as a path segment: URLEncoder emits '+' for a space, which is only a space in a
    // query string.
    final String url = getServerHttpUrl("/api/v1/query/") + getDatabaseName() + "/sql/"
        + URLEncoder.encode(command, StandardCharsets.UTF_8).replace("+", "%20")
        + (limit == null ? "" : "?limit=" + limit);

    final HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI(url))
        .GET()
        .setHeader("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
        .build();

    return client.send(request, BodyHandlers.ofString());
  }

  /**
   * Collects the formatted WARNING (and above) messages published to a logger.
   */
  private static final class CapturingHandler extends Handler {
    private final List<String> records = new CopyOnWriteArrayList<>();

    @Override
    public void publish(final LogRecord record) {
      if (record == null || record.getLevel().intValue() < Level.WARNING.intValue())
        return;
      String message = record.getMessage();
      if (message != null && record.getParameters() != null && record.getParameters().length > 0) {
        try {
          message = message.formatted(record.getParameters());
        } catch (final Exception ignored) {
          // keep the raw message: the assertions only look for a substring of the template
        }
      }
      if (message != null)
        records.add(message);
    }

    List<String> messages() {
      return List.copyOf(records);
    }

    void clear() {
      records.clear();
    }

    @Override
    public void flush() {
    }

    @Override
    public void close() {
    }
  }
}
