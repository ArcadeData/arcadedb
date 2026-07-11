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
 * Regression test for issue #5224: a Cypher UNION whose branches expose different implicit return-column
 * names must surface over HTTP as 400 Bad Request with the descriptive validation message, never a
 * 500 "Error on transaction commit". Neo4j rejects the same query with
 * "All sub queries in an UNION must have the same return column names".
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5224UnionColumnNamesHttpStatusIT extends BaseGraphServerTest {

  @Test
  void mismatchedUnionColumnNamesReturns400() throws Exception {
    testEachServer(serverIndex -> {
      // Seed the data (both branches match at least one row).
      executeCypher(serverIndex, "CREATE (a:A {id: 1}), (b:B {id: 2})", 200);

      final JSONObject json = executeCypher(serverIndex,
          "MATCH (a:A) RETURN a.id UNION MATCH (b:B) RETURN b.id", 400);
      assertThat(json.getString("exception")).contains(CommandParsingException.class.getSimpleName());
      assertThat(json.getString("detail")).contains("same return column names");
      assertThat(json.getString("error")).doesNotContain("Error on transaction commit");
    });
  }

  @Test
  void matchingAliasesReturns200() throws Exception {
    testEachServer(serverIndex -> {
      executeCypher(serverIndex, "CREATE (a:A {id: 1}), (b:B {id: 2})", 200);
      executeCypher(serverIndex, "MATCH (a:A) RETURN a.id AS x UNION MATCH (b:B) RETURN b.id AS x", 200);
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
      final String response = statusCode < 400 ? readResponse(connection) : readError(connection);

      assertThat(statusCode)
          .as("expected %d, got %d (body=%s)", expectedStatus, statusCode, response)
          .isEqualTo(expectedStatus);

      return new JSONObject(response);
    } finally {
      connection.disconnect();
    }
  }
}
