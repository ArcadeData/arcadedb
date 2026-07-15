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
 * Regression test for issue #5296: calling the Cypher {@code left()} or {@code right()} function with a
 * negative length is an invalid argument value and must surface over HTTP as 400 Bad Request with a
 * descriptive message, never 500 "Cannot execute command". Neo4j and Memgraph both return a client error.
 * ArcadeDB already rejected the negative length with a descriptive message, but threw a
 * {@link com.arcadedb.exception.CommandExecutionException}, which the HTTP handler classifies as a
 * server-side runtime error (500). Throwing {@link CommandSemanticException} (which extends
 * CommandParsingException) instead makes the handler report it as a 400 client error.
 *
 * Non-negative controls (positive, zero, larger-than-string) must keep working (200).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5296LeftRightNegativeLengthHttpStatusIT extends BaseGraphServerTest {

  @Test
  void leftNegativeLengthReturns400() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = executeCypher(serverIndex, "RETURN left('hello', -2) AS r", 400);
      assertThat(json.getString("exception")).isEqualTo(CommandSemanticException.class.getName());
      assertThat(json.getString("detail")).contains("left").contains("negative");
      assertThat(json.getString("error")).doesNotContain("Error on transaction commit");
    });
  }

  @Test
  void rightNegativeLengthReturns400() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = executeCypher(serverIndex, "RETURN right('hello', -2) AS r", 400);
      assertThat(json.getString("exception")).isEqualTo(CommandSemanticException.class.getName());
      assertThat(json.getString("detail")).contains("right").contains("negative");
      assertThat(json.getString("error")).doesNotContain("Error on transaction commit");
    });
  }

  @Test
  void computedNegativeLengthReturns400() throws Exception {
    // The impact scenario from the issue: an arithmetic expression that evaluates to a negative length must
    // still be a 400 client error, not an HTTP 500.
    testEachServer(serverIndex -> {
      final JSONObject json = executeCypher(serverIndex, "RETURN left('hello', 1 - 3) AS r", 400);
      assertThat(json.getString("exception")).isEqualTo(CommandSemanticException.class.getName());
      assertThat(json.getString("error")).doesNotContain("Error on transaction commit");
    });
  }

  @Test
  void nonNegativeControlsStillWork() throws Exception {
    testEachServer(serverIndex -> {
      assertThat(firstResult(executeCypher(serverIndex, "RETURN left('hello', 2) AS r", 200))).isEqualTo("he");
      assertThat(firstResult(executeCypher(serverIndex, "RETURN right('hello', 2) AS r", 200))).isEqualTo("lo");
      assertThat(firstResult(executeCypher(serverIndex, "RETURN left('hello', 0) AS r", 200))).isEqualTo("");
      assertThat(firstResult(executeCypher(serverIndex, "RETURN right('hello', 0) AS r", 200))).isEqualTo("");
      assertThat(firstResult(executeCypher(serverIndex, "RETURN left('hello', 10) AS r", 200))).isEqualTo("hello");
      assertThat(firstResult(executeCypher(serverIndex, "RETURN right('hello', 10) AS r", 200))).isEqualTo("hello");
    });
  }

  private String firstResult(final JSONObject json) {
    return json.getJSONArray("result").getJSONObject(0).getString("r");
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
          .as("left()/right() negative-length must return %d, got %d (body=%s)", expectedStatus, statusCode, response)
          .isEqualTo(expectedStatus);

      return new JSONObject(response);
    } finally {
      connection.disconnect();
    }
  }
}
