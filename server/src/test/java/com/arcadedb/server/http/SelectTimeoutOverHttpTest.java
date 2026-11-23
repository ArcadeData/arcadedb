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

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11804: the HTTP command endpoint appends a LIMIT to a SELECT that states none, and appended after a TIMEOUT clause it
 * made a valid statement a syntax error. A statement with a LIMIT of its own and a {@code TIMEOUT ... EXCEPTION} got a second one.
 */
class SelectTimeoutOverHttpTest extends BaseGraphServerTest {

  /** The statements and the rows each returns, in the order they are tried so that a failure names the same one every run. */
  private static final List<Map.Entry<String, Integer>> SELECTS = List.of(//
      Map.entry("SELECT n FROM Timed TIMEOUT 5000", 3), //
      Map.entry("SELECT n FROM Timed TIMEOUT 5000 EXCEPTION", 3), //
      Map.entry("SELECT n FROM Timed ORDER BY n timeout 5000 return", 3), //
      Map.entry("SELECT n FROM (SELECT n FROM Timed) TIMEOUT 5000", 3), //
      Map.entry("SELECT n FROM Timed LIMIT 2 TIMEOUT 5000 EXCEPTION", 2), //
      Map.entry("SELECT n FROM Timed SKIP 1 LIMIT 1 TIMEOUT 5000 RETURN", 1), //
      Map.entry("SELECT n FROM Timed TIMEOUT /* budget */ 5000", 3), //
      Map.entry("SELECT n FROM Timed -- note\nTIMEOUT 5000", 3), //
      Map.entry("SELECT n FROM Timed TIMEOUT 5000L", 3), //
      Map.entry("SELECT n FROM Timed LIMIT 2 TIMEOUT 0x1388 RETURN", 2), //
      // A keyword written right after the number is a token of its own to the lexer
      Map.entry("SELECT n FROM Timed TIMEOUT 5000RETURN", 3), //
      Map.entry("SELECT n FROM Timed LIMIT 2 TIMEOUT 5000EXCEPTION", 2), //
      // The grammar ends a SELECT with "timeout? PARALLEL?": the LIMIT goes before the keyword the text ends with
      Map.entry("SELECT n FROM Timed PARALLEL", 3), //
      Map.entry("SELECT n FROM Timed TIMEOUT 5000 PARALLEL", 3), //
      Map.entry("SELECT n FROM Timed LIMIT 2 PARALLEL", 2), //
      Map.entry("SELECT n FROM Timed LIMIT 2 TIMEOUT 5000 EXCEPTION PARALLEL", 2), //
      // A text that only mentions the words is no clause
      Map.entry("SELECT n FROM Timed WHERE n > 0 AND 'timeout 5000' = 'timeout 5000'", 3), //
      Map.entry("SELECT n FROM Timed WHERE n > 0 AND 'parallel' = 'parallel'", 3));

  /** MATCH ends with the same clause ({@code skip? limit? timeout?}). */
  private static final List<Map.Entry<String, Integer>> MATCHES = List.of(//
      Map.entry("MATCH {type: TimedVertex, as: v} RETURN v.n TIMEOUT 5000", 3), //
      Map.entry("MATCH {type: TimedVertex, as: v} RETURN v.n TIMEOUT 5000 EXCEPTION", 3), //
      Map.entry("MATCH {type: TimedVertex, as: v} RETURN v.n LIMIT 2 TIMEOUT 5000 EXCEPTION", 2), //
      Map.entry("MATCH {type: TimedVertex, as: v} RETURN v.n SKIP 1 LIMIT 1 TIMEOUT 5000 RETURN", 1));

  @Test
  void aSelectEndingWithATimeoutClauseRuns() throws Exception {
    testEachServer(serverIndex -> {
      executeCommand(serverIndex, "sql", "CREATE DOCUMENT TYPE Timed");
      executeCommand(serverIndex, "sql", "INSERT INTO Timed CONTENT [{\"n\": 1}, {\"n\": 2}, {\"n\": 3}]");

      for (final Map.Entry<String, Integer> statement : SELECTS) {
        assertRows(statement, executeCommand(serverIndex, "sql", statement.getKey()), "/command");
        assertRows(statement, executeQuery(serverIndex, statement.getKey()), "/query");
      }
    });
  }

  @Test
  void aMatchEndingWithATimeoutClauseRuns() throws Exception {
    testEachServer(serverIndex -> {
      executeCommand(serverIndex, "sql", "CREATE VERTEX TYPE TimedVertex");
      executeCommand(serverIndex, "sql", "CREATE VERTEX TimedVertex SET n = 1");
      executeCommand(serverIndex, "sql", "CREATE VERTEX TimedVertex SET n = 2");
      executeCommand(serverIndex, "sql", "CREATE VERTEX TimedVertex SET n = 3");

      for (final Map.Entry<String, Integer> statement : MATCHES) {
        assertRows(statement, executeCommand(serverIndex, "sql", statement.getKey()), "/command");
        assertRows(statement, executeQuery(serverIndex, statement.getKey()), "/query");
      }
    });
  }

  private static void assertRows(final Map.Entry<String, Integer> statement, final JSONObject response, final String endpoint) {
    assertThat(response).as("refused over HTTP %s: %s", endpoint, statement.getKey()).isNotNull();
    assertThat(response.getJSONObject("result").getJSONArray("records").length()).as(endpoint + " " + statement.getKey())
        .isEqualTo(statement.getValue());
  }

  /** The same statement through the query endpoint, a handler of its own that shares the command handler's cap. */
  private JSONObject executeQuery(final int serverIndex, final String statement) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL(
        getServerHttpUrl(serverIndex, "/api/v1/query/graph")).openConnection();
    try {
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      formatPayload(connection, "sql", statement, "studio", Collections.emptyMap());
      connection.connect();
      if (connection.getResponseCode() != 200)
        return null;
      return new JSONObject(readResponse(connection));
    } finally {
      connection.disconnect();
    }
  }
}
