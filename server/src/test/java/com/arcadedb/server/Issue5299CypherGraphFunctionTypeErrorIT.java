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
 * Regression test for issue #5299: the graph introspection functions {@code labels()}, {@code keys()} and
 * {@code properties()} must report an unsupported argument type as a 400 client error, the same way
 * {@code type()} already does since #5204. Passing a scalar such as {@code labels(42)} is a query mistake,
 * not a server failure, and Neo4j rejects it as a type error.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5299CypherGraphFunctionTypeErrorIT extends BaseGraphServerTest {

  @Test
  void labelsOnIntegerReturns400() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = executeCypher(serverIndex, "RETURN labels(42) AS r", 400);
      assertThat(json.getString("exception")).isEqualTo(CommandSemanticException.class.getName());
      assertThat(json.getString("detail")).contains("labels").contains("node");
      assertThat(json.getString("error")).doesNotContain("Error on transaction commit");
    });
  }

  @Test
  void keysOnIntegerReturns400() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = executeCypher(serverIndex, "RETURN keys(42) AS r", 400);
      assertThat(json.getString("exception")).isEqualTo(CommandSemanticException.class.getName());
      assertThat(json.getString("detail")).contains("keys").contains("node");
      assertThat(json.getString("error")).doesNotContain("Error on transaction commit");
    });
  }

  @Test
  void propertiesOnIntegerReturns400() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = executeCypher(serverIndex, "RETURN properties(42) AS r", 400);
      assertThat(json.getString("exception")).isEqualTo(CommandSemanticException.class.getName());
      assertThat(json.getString("detail")).contains("properties").contains("node");
      assertThat(json.getString("error")).doesNotContain("Error on transaction commit");
    });
  }

  @Test
  void nullArgumentStillReturnsNull() throws Exception {
    testEachServer(serverIndex -> {
      // Neo4j returns null for a null argument on all three functions: that behaviour must not regress.
      for (final String function : new String[] { "labels", "keys", "properties" }) {
        final JSONObject json = executeCypher(serverIndex, "RETURN " + function + "(null) AS r", 200);
        assertThat(json.getJSONArray("result").getJSONObject(0).isNull("r"))
            .as("%s(null) must stay null", function)
            .isTrue();
      }
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
