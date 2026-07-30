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

import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5484, reported against the HTTP command API: {@code RETURN abs('hello')} answered 500
 * "internal server error" with an otherwise correct type message. Handing a STRING to a function declared as
 * {@code abs(input :: INTEGER | FLOAT)} is the client's mistake - Neo4j answers a {@code Neo.ClientError.Statement.TypeError}
 * - so the status must be 400. Same class of fix as issues #5477, #5476, #5294 and #5203.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5484AbsNonNumericHttpStatusIT extends BaseGraphServerTest {

  @Test
  void absOnStringReturns400() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = executeCypher(serverIndex, "RETURN abs('hello') AS r", 400);
      assertThat(json.getString("exception")).isEqualTo(CommandSemanticException.class.getName());
      assertThat(json.getString("detail")).contains("abs()").contains("STRING");
      assertThat(json.getString("error")).doesNotContain("Error on transaction commit");
    });
  }

  @Test
  void absOnBooleanReturns400() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = executeCypher(serverIndex, "RETURN abs(true) AS r", 400);
      assertThat(json.getString("exception")).isEqualTo(CommandSemanticException.class.getName());
      assertThat(json.getString("detail")).contains("abs()").contains("BOOLEAN");
    });
  }

  @Test
  void otherNumericFunctionsOnStringReturn400() throws Exception {
    testEachServer(serverIndex -> {
      for (final String function : new String[] { "sqrt", "ceil", "floor", "sign", "round", "isNaN", "log" }) {
        final JSONObject json = executeCypher(serverIndex, "RETURN " + function + "('hello') AS r", 400);
        assertThat(json.getString("detail")).contains(function + "()").contains("STRING");
      }
    });
  }

  @Test
  void absOnANonNumericPropertyReturns400() throws Exception {
    // Type known only at runtime, so this goes through AbsFunction rather than the parse-time check.
    testEachServer(serverIndex -> {
      executeCypher(serverIndex, "CREATE (:Issue5484 {name: 'hello'})", 200);
      final JSONObject json = executeCypher(serverIndex, "MATCH (n:Issue5484) RETURN abs(n.name) AS r", 400);
      assertThat(json.getString("detail")).contains("abs()").contains("STRING");
      assertThat(json.getString("error")).doesNotContain("Error on transaction commit");
    });
  }

  @Test
  void anUnsupportedDistanceUnitAlsoReturns400() throws Exception {
    // distance() gained an optional-unit declaration here; its unit error travels as an IllegalArgumentException, which
    // the HTTP layer already maps to 400. Asserted end to end so it cannot regress into a 500 unnoticed.
    testEachServer(serverIndex -> {
      final JSONObject json = executeCypher(serverIndex, "RETURN distance(point({latitude: 0, longitude: 0}),"
          + " point({latitude: 0, longitude: 1}), 'furlongs') AS r", 400);
      assertThat(json.getString("detail")).contains("furlongs");
    });
  }

  @Test
  void absOnNullStillReturns200() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = executeCypher(serverIndex, "RETURN abs(null) AS r", 200);
      assertThat(json.getJSONArray("result").getJSONObject(0).isNull("r")).isTrue();
    });
  }

  @Test
  void absOnNumbersStillReturns200() throws Exception {
    testEachServer(serverIndex -> {
      assertThat(executeCypher(serverIndex, "RETURN abs(-1) AS r", 200).getJSONArray("result").getJSONObject(0)
          .getLong("r")).isEqualTo(1L);
      assertThat(executeCypher(serverIndex, "RETURN abs(-1.5) AS r", 200).getJSONArray("result").getJSONObject(0)
          .getDouble("r")).isEqualTo(1.5d);
    });
  }

  private JSONObject executeCypher(final int serverIndex, final String command, final int expectedStatus) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL(
        "http://127.0.0.1:248" + serverIndex + "/api/v1/command/graph").openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    connection.setDoOutput(true);
    try {
      final JSONObject payload = new JSONObject().put("language", "opencypher").put("command", command);
      try (final PrintWriter pw = new PrintWriter(new OutputStreamWriter(connection.getOutputStream()))) {
        pw.write(payload.toString());
      }

      final int statusCode = connection.getResponseCode();
      final String response = expectedStatus == 200 ? readResponse(connection) : readError(connection);

      assertThat(statusCode)
          .as("Cypher type error must return %d, got %d (body=%s)", expectedStatus, statusCode, response)
          .isEqualTo(expectedStatus);

      return new JSONObject(response);
    } finally {
      connection.disconnect();
    }
  }
}
