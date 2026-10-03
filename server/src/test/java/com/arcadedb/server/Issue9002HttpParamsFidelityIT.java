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

import com.arcadedb.database.Database;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.IdempotencyCache;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issues #9002 (a JSON array parameter written to a LIST property was stored as a one-element list), #9003
 * (a params array or list parameter holding a fraction was narrowed to float32), #9004 (an integer beyond the long range wrapped
 * around, a decimal kept only the digits of a double) and #9006 (a session kept running in autocommit after a failed command, and
 * /commit answered 204 for work that was rolled back). Every case runs the same statement the way the embedded API receives it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9002HttpParamsFidelityIT extends BaseGraphServerTest {

  private final HttpClient http = HttpClient.newHttpClient();

  @Override
  protected void populateDatabase() {
    super.populateDatabase();
    final Database db = getDatabase(0);
    db.transaction(() -> {
      final DocumentType t = db.getSchema().createDocumentType("T9002");
      t.createProperty("lst", Type.LIST);
      t.createProperty("dec", Type.DECIMAL);
      t.createProperty("i", Type.INTEGER);
      t.createProperty("d", Type.DOUBLE);
      db.getSchema().createDocumentType("S9006");
    });
  }

  @Test
  void jsonArrayParameterWrittenToAListPropertyIsAList() throws Exception {
    assertThat(post("command", null, "INSERT INTO T9002 SET id = :id, lst = :v, undeclared = :v",
        "{\"id\":2,\"v\":[1,2,3]}").statusCode()).isEqualTo(200);
    assertThat(post("command", null, "INSERT INTO T9002 SET id = :id, lst = :v, undeclared = :v",
        "{\"id\":3,\"v\":[1.5,2.5]}").statusCode()).isEqualTo(200);

    final Database db = getServerDatabase(0, getDatabaseName());
    try (final ResultSet rs = db.query("sql",
        "SELECT lst.size() AS n, undeclared.type() AS t, undeclared.size() AS us FROM T9002 ORDER BY id")) {
      final List<JSONObject> rows = new ArrayList<>();
      while (rs.hasNext())
        rows.add(rs.next().toJSON());
      assertThat(rows).hasSize(2);
      assertThat(rows.get(0).getInt("n")).isEqualTo(3);
      assertThat(rows.get(0).getString("t")).isEqualTo("LIST");
      assertThat(rows.get(0).getInt("us")).isEqualTo(3);
      assertThat(rows.get(1).getInt("n")).isEqualTo(2);
      assertThat(rows.get(1).getString("t")).isEqualTo("LIST");
    }
  }

  @Test
  void positionalParamsKeepTheirPrecisionAndType() throws Exception {
    assertThat(post("command", null, "INSERT INTO T9002 SET id = ?, i = ?, d = ?, u = ?",
        "[2,123456789,3.141592653589793,1e300]").statusCode()).isEqualTo(200);

    final Database db = getServerDatabase(0, getDatabaseName());
    try (final ResultSet rs = db.query("sql", "SELECT FROM T9002 WHERE id = 2")) {
      final var r = rs.next();
      assertThat(r.<Object>getProperty("id")).isEqualTo(2);
      assertThat(r.<Integer>getProperty("i")).isEqualTo(123456789);
      assertThat(r.<Double>getProperty("d")).isEqualTo(3.141592653589793);
      assertThat(r.<Object>getProperty("u")).isEqualTo(1e300);
    }

    final HttpResponse<String> lookup = post("query", null, "SELECT id FROM T9002 WHERE i = ? AND d > ?", "[123456789,3.0]");
    assertThat(new JSONObject(lookup.body()).getJSONArray("result").length()).isEqualTo(1);

    // a named list with fractions keeps both doubles
    assertThat(post("command", null, "INSERT INTO T9002 SET id = 3, v = :v", "{\"v\":[0.1,3.141592653589793]}").statusCode()).isEqualTo(200);
    try (final ResultSet rs = db.query("sql", "SELECT v FROM T9002 WHERE id = 3")) {
      assertThat(rs.next().<Object>getProperty("v")).isEqualTo(List.of(0.1, 3.141592653589793));
    }
  }

  @Test
  void numberBeyondTheLongRangeIsKeptOrNotMatched() throws Exception {
    assertThat(post("command", null, "INSERT INTO T9002 SET id = 1, dec = :v", "{\"v\":123456789012345678901234567890}").statusCode())
        .isEqualTo(200);
    assertThat(post("command", null, "INSERT INTO T9002 SET id = 5, dec = :v", "{\"v\":1.23456789012345678901234567890}").statusCode())
        .isEqualTo(200);

    final Database db = getServerDatabase(0, getDatabaseName());
    try (final ResultSet rs = db.query("sql", "SELECT dec FROM T9002 ORDER BY id")) {
      assertThat(rs.next().<Object>getProperty("dec")).isEqualTo(new BigDecimal("123456789012345678901234567890"));
      assertThat(rs.next().<Object>getProperty("dec")).isEqualTo(new BigDecimal("1.23456789012345678901234567890"));
    }

    // 2^64 + 1 used to wrap around to 1 and answer the record whose id is 1
    final HttpResponse<String> lookup = post("query", null, "SELECT id FROM T9002 WHERE id = :id", "{\"id\":18446744073709551617}");
    assertThat(new JSONObject(lookup.body()).getJSONArray("result").length()).isEqualTo(0);
  }

  @Test
  void commitAfterAFailedCommandIsRefusedAndNothingRunsInAutocommit() throws Exception {
    final HttpResponse<String> begin = post("begin", null, null, null);
    final String session = begin.headers().firstValue("arcadedb-session-id").orElseThrow();
    assertThat(post("command", session, "INSERT INTO S9006 SET name = 'http-1'", null).statusCode()).isEqualTo(200);
    assertThat(post("command", session, "INSERT INTO S9006 SET name = 'http-2' WHERE", null).statusCode()).isEqualTo(400);

    // the session is aborted: the write is refused instead of running in autocommit
    assertThat(post("command", session, "INSERT INTO S9006 SET name = 'http-3'", null).statusCode()).isEqualTo(404);
    assertThat(count("S9006")).isZero();

    final HttpResponse<String> commit = post("commit", session, null, null);
    assertThat(commit.statusCode()).isNotEqualTo(204);
    assertThat(commit.body()).contains("rolled back");
    assertThat(count("S9006")).isZero();

    // the session is gone after the commit answered
    assertThat(post("command", session, "INSERT INTO S9006 SET name = 'http-4'", null).statusCode()).isEqualTo(404);
  }

  @Test
  void aFailedCommandThatLostNothingLeavesTheSessionInATransaction() throws Exception {
    final HttpResponse<String> begin = post("begin", null, null, null);
    final String session = begin.headers().firstValue("arcadedb-session-id").orElseThrow();
    assertThat(post("command", session, "SELECT FROM NoSuchType9006", null).statusCode()).isNotEqualTo(200);

    // still a transaction: the write is not visible before /commit, and it commits there
    assertThat(post("command", session, "INSERT INTO S9006 SET name = 'kept'", null).statusCode()).isEqualTo(200);
    assertThat(count("S9006")).isZero();
    assertThat(post("commit", session, null, null).statusCode()).isEqualTo(204);
    assertThat(count("S9006")).isEqualTo(1);
  }

  @Test
  void rollbackEndsAnAbortedSession() throws Exception {
    final HttpResponse<String> begin = post("begin", null, null, null);
    final String session = begin.headers().firstValue("arcadedb-session-id").orElseThrow();
    assertThat(post("command", session, "INSERT INTO S9006 SET name = 'http-1' WHERE", null).statusCode()).isEqualTo(400);
    assertThat(post("rollback", session, null, null).statusCode()).isEqualTo(204);
    // ending it twice is harmless
    assertThat(post("rollback", session, null, null).statusCode()).isEqualTo(204);
    assertThat(post("command", session, "INSERT INTO S9006 SET name = 'http-2'", null).statusCode()).isEqualTo(404);
  }

  @Test
  void aSuccessfulSessionStillCommits() throws Exception {
    final HttpResponse<String> begin = post("begin", null, null, null);
    final String session = begin.headers().firstValue("arcadedb-session-id").orElseThrow();
    assertThat(post("command", session, "INSERT INTO S9006 SET name = 'ok-1'", null).statusCode()).isEqualTo(200);
    assertThat(post("command", session, "INSERT INTO S9006 SET name = 'ok-2'", null).statusCode()).isEqualTo(200);
    assertThat(post("commit", session, null, null).statusCode()).isEqualTo(204);
    assertThat(count("S9006")).isEqualTo(2);
  }

  @Test
  void everyResponseAdvertisesReplayProtection() throws Exception {
    final HttpResponse<String> response = post("query", null, "SELECT 1", null);
    assertThat(response.headers().firstValue("X-ArcadeDB-Replay-Protection")).hasValue(IdempotencyCache.PROCESS_ID);
  }

  @Test
  void aRetryTellingAnotherServerProcessIsRefusedBeforeItRuns() throws Exception {
    final HttpRequest request = HttpRequest.newBuilder(URI.create(getServerHttpUrl(0, "/api/v1/command/" + getDatabaseName())))
        .header("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)))
        .header("Content-Type", "application/json").header("X-Request-Id", "restart-9002")
        .header("X-ArcadeDB-Replay-Instance", "another-process")
        .POST(HttpRequest.BodyPublishers.ofString("{\"language\":\"sql\",\"command\":\"INSERT INTO S9006 SET name = 'never'\"}")).build();
    assertThat(http.send(request, HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(412);
    assertThat(count("S9006")).isZero();
  }

  private long count(final String type) {
    try (final ResultSet rs = getServerDatabase(0, getDatabaseName()).query("sql", "SELECT count(*) AS c FROM " + type)) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  /** {@code params} is the raw JSON text of the parameters (object or array), or null. */
  private HttpResponse<String> post(final String op, final String session, final String sql, final String params) throws Exception {
    final HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(getServerHttpUrl(0, "/api/v1/" + op + "/" + getDatabaseName())))
        .header("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)))
        .header("Content-Type", "application/json");
    if (session != null)
      b.header("arcadedb-session-id", session);
    if (sql == null)
      b.POST(HttpRequest.BodyPublishers.noBody());
    else {
      // the command text goes through the JSON writer (escaping); the raw params text is appended so its numbers stay as written
      final String commandJson = new JSONObject().put("language", "sql").put("command", sql).toString();
      final String body = commandJson.substring(0, commandJson.length() - 1) + (params != null ? ",\"params\":" + params : "") + "}";
      b.POST(HttpRequest.BodyPublishers.ofString(body));
    }
    return http.send(b.build(), HttpResponse.BodyHandlers.ofString());
  }
}
