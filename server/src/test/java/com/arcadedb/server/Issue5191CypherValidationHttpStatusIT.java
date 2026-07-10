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

import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5191: a Cypher validation error (invalid query text) must surface over
 * HTTP as 400 Bad Request with the descriptive validation message, never 500 "Error on transaction
 * commit". These are client errors (the query is malformed), not internal server faults - Neo4j,
 * Memgraph and FalkorDB all return descriptive client errors for the same queries. ArcadeDB detected
 * the problem correctly ({@link CommandParsingException}) but mis-classified it as a transaction-commit
 * failure because the exception was wrapped in a {@code TransactionException} by the auto-commit wrapper.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5191CypherValidationHttpStatusIT extends BaseGraphServerTest {

  @Test
  void mergeRebindReturns400() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = executeCypher(serverIndex, "MERGE (a:U {id: 1}) MERGE (a:U {id: 2}) RETURN a.id", 400);
      assertThat(json.getString("exception")).isEqualTo(CommandParsingException.class.getName());
      // The real validation message must reach the client, not "Error on transaction commit".
      assertThat(json.getString("detail")).contains("VariableAlreadyBound").contains("already defined");
      assertThat(json.getString("error")).doesNotContain("Error on transaction commit");
    });
  }

  @Test
  void unknownVariableReturns400() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = executeCypher(serverIndex, "RETURN unknownVariable", 400);
      // CommandSemanticException extends CommandParsingException, so it is mapped by the same 400 branch.
      assertThat(json.getString("exception")).contains("CommandSemanticException");
      assertThat(json.getString("detail")).contains("UndefinedVariable");
    });
  }

  @Test
  void syntaxErrorReturns400() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = executeCypher(serverIndex, "RETURN 1 +", 400);
      // Whatever the specific parsing message, it must be a client error (400) exposing the real cause.
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
      final String response = readError(connection);

      assertThat(statusCode)
          .as("Cypher validation error must return %d, got %d (body=%s)", expectedStatus, statusCode, response)
          .isEqualTo(expectedStatus);

      return new JSONObject(response);
    } finally {
      connection.disconnect();
    }
  }
}
