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

import com.arcadedb.exception.ArithmeticErrorException;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for item 5 of issue #5602, reported against the HTTP command API. An arithmetic error - a 64-bit
 * integer overflow or a division by zero - answered 500 "internal server error" although nothing was wrong with the
 * server: the pair of values the caller sent has no representable answer. Neo4j classifies the whole category as
 * {@code Neo.ClientError.Statement.ArithmeticError}, so the status must be 400.
 * <p>
 * Asserted through HTTP rather than in the engine because the classification only exists at this boundary, and because
 * it has to survive the auto-commit transaction wrapper that wraps the failure on the write path (the shape that made
 * #5191 and #5219 report the wrong status).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5602ArithmeticErrorHttpStatusIT extends BaseGraphServerTest {

  @Test
  void integerOverflowReturns400() throws Exception {
    testEachServer(serverIndex -> {
      for (final String command : new String[] { //
          "RETURN abs(-9223372036854775808) AS r", //
          "RETURN 9223372036854775807 + 1 AS r", //
          "RETURN -9223372036854775808 - 1 AS r", //
          "RETURN 9223372036854775807 * 2 AS r" }) {
        final JSONObject json = executeCypher(serverIndex, command, 400);
        assertThat(json.getString("exception")).isEqualTo(ArithmeticErrorException.class.getName());
        assertThat(json.getString("detail")).contains("overflow");
        assertThat(json.getString("error")).doesNotContain("Error on transaction commit");
      }
    });
  }

  @Test
  void divisionByZeroReturns400() throws Exception {
    testEachServer(serverIndex -> {
      assertThat(executeCypher(serverIndex, "RETURN 1 / 0 AS r", 400).getString("detail")).contains("/ by zero");
      assertThat(executeCypher(serverIndex, "RETURN 1 % 0 AS r", 400).getString("detail")).contains("% by zero");
    });
  }

  @Test
  void anOverflowOnAWriteAlsoReturns400() throws Exception {
    // The write path wraps the failure in the auto-commit TransactionException, which is where a client error most
    // often degrades back to 500. Asserted separately so the wrapped arm cannot rot.
    testEachServer(serverIndex -> {
      executeCypher(serverIndex, "CREATE (:Issue5602 {v: 9223372036854775807})", 200);
      final JSONObject json = executeCypher(serverIndex, "MATCH (n:Issue5602) SET n.v = n.v + 1 RETURN n", 400);
      assertThat(json.getString("detail")).contains("overflow");
      assertThat(json.getString("error")).doesNotContain("Error on transaction commit");
    });
  }

  /**
   * A wrong argument count reaching the runtime guard through {@code CALL} must be the same 400 an expression gives.
   * <p>
   * It is the double-wrap that makes this worth its own test: {@code CallStep} used to flatten the client error into
   * a {@code CommandExecutionException}, and on the auto-commit path the chain became
   * {@code TransactionException -> CommandExecutionException -> CommandSemanticException} - one level deeper than the
   * handler unwraps, so the same mistake answered 500 here while answering 400 in a plain read. {@code CallStep} now
   * rethrows a client error untouched.
   */
  @Test
  void aWrongArgumentCountThroughCallReturns400OnTheWritePath() throws Exception {
    testEachServer(serverIndex -> {
      // text.hammingDistance declares 2 arguments; the CREATE puts the statement on the auto-commit write path.
      final JSONObject json = executeCypher(serverIndex,
          "CREATE (n:Issue5602Call) WITH n CALL text.hammingDistance('a') YIELD * RETURN n", 400);
      assertThat(json.getString("detail")).contains("hammingDistance").contains("2 arguments");
      assertThat(json.getString("error")).doesNotContain("Error on transaction commit");
    });
  }

  @Test
  void arithmeticThatDoesNotOverflowStillReturns200() throws Exception {
    testEachServer(serverIndex -> {
      assertThat(executeCypher(serverIndex, "RETURN 9223372036854775806 + 1 AS r", 200).getJSONArray("result")
          .getJSONObject(0).getLong("r")).isEqualTo(Long.MAX_VALUE);
      // A float overflow is Infinity under IEEE 754, as in Neo4j, and must not have become an error. JSON has no
      // literal for it, so the serializer renders it as a name - the assertion is that the request succeeded at all.
      assertThat(executeCypher(serverIndex, "RETURN 1.0 / 0.0 AS r", 200).getJSONArray("result").getJSONObject(0)
          .get("r").toString()).contains("Infinity");
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
          .as("arithmetic error must return %d, got %d (body=%s)", expectedStatus, statusCode, response)
          .isEqualTo(expectedStatus);

      return new JSONObject(response);
    } finally {
      connection.disconnect();
    }
  }
}
