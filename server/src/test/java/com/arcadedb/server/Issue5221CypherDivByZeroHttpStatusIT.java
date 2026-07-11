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

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5221: Cypher integer division or modulo by zero is a runtime arithmetic
 * error, not a transaction-commit failure. The engine detects the failure correctly (detail
 * {@code "/ by zero"} / {@code "% by zero"}) but the auto-commit transaction wrapper in
 * DatabaseAbstractHandler re-wrapped the {@link CommandExecutionException} in a
 * {@code TransactionException}, so before the #5219 fix the HTTP layer degraded the response to 500
 * "Error on transaction commit", hiding the real cause and wrongly implying a durability/consistency
 * problem. The honest label is "Cannot execute command" (a runtime execution error), matching Neo4j
 * and Memgraph which both report the equivalent integer operations as query-level arithmetic errors.
 * <p>
 * The failure is data-dependent in the general case ({@code n.a / n.b}), so it is a runtime execution
 * error (HTTP 500) rather than a static client error, consistent with the #5219 CommandExecutionException
 * classification. Float division/modulo by zero keep IEEE 754 semantics (Infinity/NaN) and are not errors.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5221CypherDivByZeroHttpStatusIT extends BaseGraphServerTest {

  @Test
  void integerDivisionByZeroReturnsExecutionError() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = executeCypher(serverIndex, "RETURN 5 / 0", 500);
      assertThat(json.getString("exception")).isEqualTo(CommandExecutionException.class.getName());
      assertThat(json.getString("detail")).contains("/ by zero");
      assertThat(json.getString("error")).doesNotContain("Error on transaction commit");
    });
  }

  @Test
  void integerModuloByZeroReturnsExecutionError() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = executeCypher(serverIndex, "RETURN 5 % 0", 500);
      assertThat(json.getString("exception")).isEqualTo(CommandExecutionException.class.getName());
      assertThat(json.getString("detail")).contains("% by zero");
      assertThat(json.getString("error")).doesNotContain("Error on transaction commit");
    });
  }

  @Test
  void computedZeroDivisorReturnsExecutionError() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = executeCypher(serverIndex, "RETURN 1 / (2 - 2)", 500);
      assertThat(json.getString("exception")).isEqualTo(CommandExecutionException.class.getName());
      assertThat(json.getString("detail")).contains("/ by zero");
      assertThat(json.getString("error")).doesNotContain("Error on transaction commit");
    });
  }

  @Test
  void divisionByZeroInsideCaseReturnsExecutionError() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = executeCypher(serverIndex, "RETURN CASE WHEN 1 / 0 > 5 THEN 1 ELSE 0 END", 500);
      assertThat(json.getString("exception")).isEqualTo(CommandExecutionException.class.getName());
      assertThat(json.getString("detail")).contains("/ by zero");
      assertThat(json.getString("error")).doesNotContain("Error on transaction commit");
    });
  }

  @Test
  void floatDivisionByZeroReturnsInfinity() throws Exception {
    testEachServer(serverIndex -> {
      // Float division by zero keeps IEEE 754 semantics (+Infinity), never an error. JSON has no native
      // infinity, so it is serialized as the token "PosInfinity" (matches Neo4j/Memgraph "Infinity").
      final JSONObject json = executeCypher(serverIndex, "RETURN 5.0 / 0.0 AS result", 200);
      final Object value = json.getJSONArray("result").getJSONObject(0).get("result");
      assertThat(asDouble(value)).isEqualTo(Double.POSITIVE_INFINITY);
    });
  }

  @Test
  void floatModuloByZeroReturnsNaN() throws Exception {
    testEachServer(serverIndex -> {
      // Float modulo by zero keeps IEEE 754 semantics (NaN), never an error.
      final JSONObject json = executeCypher(serverIndex, "RETURN 5.0 % 0.0 AS result", 200);
      final Object value = json.getJSONArray("result").getJSONObject(0).get("result");
      assertThat(Double.isNaN(asDouble(value))).isTrue();
    });
  }

  /** JSON has no native Infinity/NaN, so ArcadeDB serializes them as the tokens "PosInfinity"/"NaN". */
  private static double asDouble(final Object value) {
    if (value instanceof Number number)
      return number.doubleValue();
    return switch (value.toString()) {
      case "PosInfinity", "Infinity" -> Double.POSITIVE_INFINITY;
      case "NegInfinity", "-Infinity" -> Double.NEGATIVE_INFINITY;
      case "NaN" -> Double.NaN;
      default -> Double.parseDouble(value.toString());
    };
  }

  @Test
  void serverRemainsOperationalAfterArithmeticError() throws Exception {
    testEachServer(serverIndex -> {
      executeCypher(serverIndex, "RETURN 5 / 0", 500);
      // A subsequent valid query must still succeed: the arithmetic error is limited to the failing
      // statement and never damages transaction state or database consistency.
      final JSONObject json = executeCypher(serverIndex, "RETURN 1 AS result", 200);
      final Object value = json.getJSONArray("result").getJSONObject(0).get("result");
      assertThat(((Number) value).intValue()).isEqualTo(1);
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
      final JSONObject payload = new JSONObject().put("language", "cypher").put("command", command);
      try (final PrintWriter pw = new PrintWriter(new OutputStreamWriter(connection.getOutputStream()))) {
        pw.write(payload.toString());
      }

      final int statusCode = connection.getResponseCode();
      final String response = expectedStatus == 200 ? readResponse(connection) : readError(connection);

      assertThat(statusCode)
          .as("Cypher arithmetic error must return %d, got %d (body=%s)", expectedStatus, statusCode, response)
          .isEqualTo(expectedStatus);

      return new JSONObject(response);
    } finally {
      connection.disconnect();
    }
  }
}
