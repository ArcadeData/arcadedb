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
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9852: {@code POST /api/v1/command} refused a malformed envelope (a missing {@code command}, a {@code language} that is not a
 * string) with a plain {@link IllegalArgumentException}, and {@link HttpSession#execute} treats anything but a
 * {@link RequestRefusedBeforeExecutionException} as a command that failed part way: it rolled back the transaction the caller had
 * opened with {@code /begin}, although not one statement of the request had reached the engine (issue #9784 gave the same rule to the
 * streaming refusals of this route and left these behind).
 * <p>
 * The test is a table over EVERY refusal the route can raise from the request alone, and pins the rule rather than the cells: after
 * each one the session still sees the rows written before it, and its {@code /commit} is accepted. A refusal added to the route
 * later has to be added here, which is the point.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9852CommandEnvelopeRefusalKeepsSessionTransactionTest extends BaseGraphServerTest {
  private static final String DOC_TYPE = "Doc9852";

  private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

  private static String rootAuth() {
    return "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8));
  }

  private HttpResponse<String> send(final String path, final String sessionId, final String body) throws IOException, InterruptedException {
    final HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(getServerHttpUrl(0, "/api/v1" + path)))
        .timeout(Duration.ofSeconds(30)).header("Authorization", rootAuth()).header("Content-Type", "application/json");
    if (sessionId != null)
      builder.header(HttpSessionManager.ARCADEDB_SESSION_ID, sessionId);
    builder.POST(HttpRequest.BodyPublishers.ofString(body));
    return http.send(builder.build(), HttpResponse.BodyHandlers.ofString());
  }

  private HttpResponse<String> command(final String sessionId, final String sql) throws Exception {
    return send("/command/" + getDatabaseName(), sessionId, new JSONObject().put("language", "sql").put("command", sql).toString());
  }

  /** Every malformed envelope the route can refuse before it reaches the engine, by the name the failure message carries. */
  private static Map<String, String> refusals() {
    final Map<String, String> table = new LinkedHashMap<>();
    table.put("no 'command' field", "{\"language\":\"sql\"}");
    table.put("'command' is null", "{\"language\":\"sql\",\"command\":null}");
    table.put("'command' is a number", "{\"language\":\"sql\",\"command\":5}");
    table.put("'command' is an object", "{\"language\":\"sql\",\"command\":{\"a\":1}}");
    table.put("'language' is a number", "{\"language\":1,\"command\":\"SELECT 1\"}");
    table.put("'language' is an array", "{\"language\":[\"sql\"],\"command\":\"SELECT 1\"}");
    table.put("'serializer' is a number", "{\"language\":\"sql\",\"command\":\"SELECT 1\",\"serializer\":3}");
    table.put("'profileExecution' is a number", "{\"language\":\"sql\",\"command\":\"SELECT 1\",\"profileExecution\":3}");
    table.put("'limit' is a string", "{\"language\":\"sql\",\"command\":\"SELECT 1\",\"limit\":\"ten\"}");
    table.put("'limit' above the int range", "{\"language\":\"sql\",\"command\":\"SELECT 1\",\"limit\":3000000000}");
    table.put("'limit' below the int range", "{\"language\":\"sql\",\"command\":\"SELECT 1\",\"limit\":-3000000000}");
    table.put("'$bytes' marker is a number", "{\"language\":\"sql\",\"command\":\"SELECT :p\",\"params\":{\"p\":{\"$bytes\":5}}}");
    table.put("'$bytes' marker is not base64", "{\"language\":\"sql\",\"command\":\"SELECT :p\",\"params\":{\"p\":{\"$bytes\":\"!!!\"}}}");
    table.put("'$int8' marker is not an array", "{\"language\":\"sql\",\"command\":\"SELECT :p\",\"params\":{\"p\":{\"$int8\":\"x\"}}}");
    table.put("'$int8' element is out of range", "{\"language\":\"sql\",\"command\":\"SELECT :p\",\"params\":{\"p\":{\"$int8\":[1,300]}}}");
    table.put("'$int8' element is fractional", "{\"language\":\"sql\",\"command\":\"SELECT :p\",\"params\":{\"p\":{\"$int8\":[1.5]}}}");
    table.put("parameters nested beyond the decoder's depth",
        "{\"language\":\"sql\",\"command\":\"SELECT :p\",\"params\":{\"p\":" + "[".repeat(40) + "]".repeat(40) + "}}");
    // These two answer 400 through a normal return, not through an exception: the controls that say the table is about the
    // outcome and not about the exception type
    table.put("'command' is empty", "{\"language\":\"sql\",\"command\":\"\"}");
    table.put("'language' is empty", "{\"language\":\"\",\"command\":\"SELECT 1\"}");
    return table;
  }

  @Test
  void everyMalformedEnvelopeIsRefusedAndKeepsTheSessionTransaction() throws Exception {
    assertThat(command(null, "CREATE DOCUMENT TYPE " + DOC_TYPE + " IF NOT EXISTS").statusCode()).isEqualTo(200);

    final HttpResponse<String> begun = send("/begin/" + getDatabaseName(), null, "");
    assertThat(begun.statusCode()).isEqualTo(204);
    final String sessionId = begun.headers().firstValue(HttpSessionManager.ARCADEDB_SESSION_ID).orElse(null);
    assertThat(sessionId).isNotBlank();

    try {
      for (int i = 0; i < 3; i++)
        assertThat(command(sessionId, "INSERT INTO " + DOC_TYPE + " SET name = 'witness" + i + "'").statusCode()).isEqualTo(200);

      for (final Map.Entry<String, String> refusal : refusals().entrySet()) {
        final HttpResponse<String> refused = send("/command/" + getDatabaseName(), sessionId, refusal.getValue());
        assertThat(refused.statusCode()).as("%s must be refused with a 400: %s", refusal.getKey(), refused.body()).isEqualTo(400);

        final HttpResponse<String> count = command(sessionId, "SELECT count() AS cnt FROM " + DOC_TYPE);
        assertThat(count.statusCode()).as("after '%s' the session must still be usable: %s", refusal.getKey(), count.body()).isEqualTo(200);
        assertThat(new JSONObject(count.body()).getJSONArray("result").getJSONObject(0).getLong("cnt"))
            .as("after '%s' the session must still see the rows it wrote", refusal.getKey()).isEqualTo(3L);
      }

      final HttpResponse<String> committed = send("/commit/" + getDatabaseName(), sessionId, "");
      assertThat(committed.statusCode()).as("the commit must be accepted: %s", committed.body()).isEqualTo(204);

      final Database database = getServerDatabase(0, getDatabaseName());
      assertThat(database.countType(DOC_TYPE, false)).as("and the witnesses are in the database").isEqualTo(3L);
    } finally {
      send("/rollback/" + getDatabaseName(), sessionId, "");
    }
  }

  /**
   * The counter-case that keeps the table meaningful: a command the ENGINE refuses, after it was handed the statement, still rolls
   * the session's transaction back. Only the requests that never reached the engine are exempt.
   */
  @Test
  void aCommandFailingInTheEngineStillRollsTheSessionTransactionBack() throws Exception {
    assertThat(command(null, "CREATE DOCUMENT TYPE " + DOC_TYPE + " IF NOT EXISTS").statusCode()).isEqualTo(200);

    final HttpResponse<String> begun = send("/begin/" + getDatabaseName(), null, "");
    final String sessionId = begun.headers().firstValue(HttpSessionManager.ARCADEDB_SESSION_ID).orElse(null);
    assertThat(sessionId).isNotBlank();
    try {
      assertThat(command(sessionId, "INSERT INTO " + DOC_TYPE + " SET name = 'witness'").statusCode()).isEqualTo(200);

      assertThat(command(sessionId, "INSERT INTO NoSuchType9852 SET name = 'x'").statusCode()).isGreaterThanOrEqualTo(400);

      assertThat(command(sessionId, "SELECT FROM " + DOC_TYPE).statusCode()).as("the session was rolled back and refuses more work")
          .isGreaterThanOrEqualTo(400);
    } finally {
      send("/rollback/" + getDatabaseName(), sessionId, "");
    }
  }
}
