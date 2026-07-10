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
 * Regression test for issues #5203 and #5204: a Cypher runtime type error - passing an unsupported
 * argument type to a scalar function such as {@code toString(list)} (#5203) or {@code type(node)}
 * (#5204) - must surface over HTTP as 400 Bad Request with the descriptive type message, never 500
 * "Error on transaction commit". These are client errors (the query passes the wrong value type to a
 * function), classified by the openCypher TCK as a runtime TypeError/InvalidArgumentValue. ArcadeDB
 * detected the problem correctly but mis-classified it as a transaction-commit failure because the
 * type-validation exception was wrapped in a {@code TransactionException} by the auto-commit wrapper.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5203CypherTypeErrorHttpStatusIT extends BaseGraphServerTest {

  @Test
  void toStringOnListReturns400() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = executeCypher(serverIndex, "RETURN toString([1, 2, 3]) AS result", 400);
      assertThat(json.getString("exception")).isEqualTo(CommandSemanticException.class.getName());
      // The real type message must reach the client, not "Error on transaction commit".
      assertThat(json.getString("detail")).contains("toString").contains("List");
      assertThat(json.getString("error")).doesNotContain("Error on transaction commit");
    });
  }

  @Test
  void toStringOnEmptyListReturns400() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = executeCypher(serverIndex, "RETURN toString([]) AS result", 400);
      assertThat(json.getString("exception")).isEqualTo(CommandSemanticException.class.getName());
      assertThat(json.getString("error")).doesNotContain("Error on transaction commit");
    });
  }

  @Test
  void typeOnNodeReturns400() throws Exception {
    testEachServer(serverIndex -> {
      executeCypher(serverIndex, "CREATE (:TypeErrNode {id: 1})", 200);
      final JSONObject json = executeCypher(serverIndex, "MATCH (a:TypeErrNode) RETURN type(a) AS result", 400);
      // A statically-inferable node argument is caught by the semantic validator as a CommandParsingException,
      // while a type that can only be resolved at runtime reaches TypeFunction as a CommandSemanticException;
      // both extend CommandParsingException and both must be client errors (400), never a 500 commit failure.
      assertThat(json.getString("exception")).contains("Command").contains("Exception");
      // The real type message must reach the client, not "Error on transaction commit".
      assertThat(json.getString("detail")).contains("type").contains("relationship");
      assertThat(json.getString("error")).doesNotContain("Error on transaction commit");
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
          .as("Cypher type error must return %d, got %d (body=%s)", expectedStatus, statusCode, response)
          .isEqualTo(expectedStatus);

      return new JSONObject(response);
    } finally {
      connection.disconnect();
    }
  }
}
