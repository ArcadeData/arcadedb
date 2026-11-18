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

import com.arcadedb.exception.InvalidArgumentException;
import com.arcadedb.exception.TypeMismatchException;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11589 over HTTP, the surface it was reported from: SQL and openCypher functions given an argument of the wrong kind or a
 * null answered 500, some with a raw {@code NullPointerException}, {@code ClassCastException} or {@code DateTimeParseException}.
 * Every statement of the report now answers below 500: 400 for the argument the caller got wrong, 200 for the two that were
 * never wrong ({@code out('E', null)}, whose null names no edge type, and {@code intersect(@this)}).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11589FunctionArgumentErrorHttpStatusTest extends BaseGraphServerTest {
  private static final List<String> SQL_ANSWERED = List.of("SELECT out('E11589', null) AS v FROM V11589 WHERE id = 1",
      "SELECT outE('E11589', null) AS v FROM V11589 WHERE id = 1", "SELECT in('E11589', null) AS v FROM V11589 WHERE id = 2",
      "SELECT both('E11589', null) AS v FROM V11589 WHERE id = 1", "SELECT intersect(@this) AS v FROM V11589 WHERE id = 1");

  private static final List<String> SQL_REFUSED = List.of("SELECT geo.lineString(1) AS v FROM V11589 WHERE id = 1",
      "SELECT geo.polygon(@rid) AS v FROM V11589 WHERE id = 1", "SELECT ts.rate(1, 'abc') AS v FROM V11589 WHERE id = 1",
      "SELECT ts.last(1, 'x') AS v FROM V11589 WHERE id = 1", "SELECT duration(1, '') AS v FROM V11589 WHERE id = 1",
      "SELECT cypherrid('a', 'b') AS v FROM V11589 WHERE id = 1",
      "SELECT fulltext.searchIndexMore('x', 1, 2, 3) AS v FROM V11589 WHERE id = 1");

  private static final List<String> CYPHER_REFUSED = List.of("MATCH (n:V11589 {id: 1}) RETURN date.fields('abc') AS v",
      "MATCH (n:V11589 {id: 1}) RETURN duration(1) AS v", "MATCH (n:V11589 {id: 1}) RETURN duration.between(n, date()) AS v",
      "MATCH (n:V11589 {id: 1}) RETURN duration.inDays(1, 2) AS v", "MATCH (n:V11589 {id: 1}) RETURN toIntegerList(1) AS v",
      "MATCH (n:V11589 {id: 1}) RETURN toStringList('a') AS v", "MATCH (n:V11589 {id: 1}) RETURN point.distance(1, 2) AS v",
      "MATCH (n:V11589 {id: 1}) RETURN percentileCont(n.id, 1.5) AS v", "MATCH (n:V11589 {id: 1}) RETURN format(1, 'yyyy') AS v");

  @Test
  void aWrongFunctionArgumentIsAClientError() throws Exception {
    testEachServer(serverIndex -> {
      executeCommand(serverIndex, "opencypher", "CREATE (:V11589 {id: 1})-[:E11589]->(:V11589 {id: 2})");

      for (final String sql : SQL_ANSWERED)
        post(serverIndex, "sql", sql, 200);
      for (final String sql : SQL_REFUSED)
        post(serverIndex, "sql", sql, 400);
      for (final String cypher : CYPHER_REFUSED)
        post(serverIndex, "opencypher", cypher, 400);
      // a null unit is a null answer, as a null temporal value is
      post(serverIndex, "opencypher", "MATCH (n:V11589 {id: 1}) RETURN time.truncate(null, true) AS v", 200);

      // the exception names the kind of mistake, which a Java client rebuilds
      assertThat(post(serverIndex, "opencypher", "MATCH (n:V11589 {id: 1}) RETURN toIntegerList(1) AS v", 400).getString("exception"))
          .isEqualTo(TypeMismatchException.class.getName());
      assertThat(post(serverIndex, "sql", "SELECT ts.rate(1, 'abc') AS v FROM V11589 WHERE id = 1", 400).getString("exception"))
          .isEqualTo(InvalidArgumentException.class.getName());
    });
  }

  /** Posts a statement to the command endpoint, asserts the HTTP status, and returns the parsed body. */
  private JSONObject post(final int serverIndex, final String language, final String command, final int expectedStatus)
      throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL(getServerHttpUrl(serverIndex, "/api/v1/command/graph"))
        .openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));

    final JSONObject payload = new JSONObject();
    payload.put("language", language);
    payload.put("command", command);
    formatPayload(connection, payload);
    connection.connect();

    try {
      final int status = connection.getResponseCode();
      final String body = status < 400 ? readResponse(connection) : readError(connection);
      assertThat(status).as("HTTP status for `%s`; body: %s", command, body).isEqualTo(expectedStatus);
      return new JSONObject(body);
    } finally {
      connection.disconnect();
    }
  }
}
