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

import com.arcadedb.exception.InvalidArgumentException;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12847, over the wire: a function or procedure argument the caller got wrong answers 400 naming it, never 500. The engine
 * half is {@code Issue12847FunctionArgumentMistakesTest}; this pins the status {@code AbstractServerHttpHandler} answers each with.
 */
class Issue12847FunctionArgumentMistakesHttpTest extends BaseGraphServerTest {

  @Test
  void trimWithANullModeAnswersNull() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = execute(serverIndex, "RETURN trim(null, 'P', 'x') AS v", 200);
      assertThat(json.getJSONArray("result").getJSONObject(0).isNull("v")).isTrue();
    });
  }

  @Test
  void argumentMistakesReturn400() throws Exception {
    testEachServer(serverIndex -> {
      createSchema(serverIndex);
      assertAnswer(serverIndex, "MATCH (n:P12847 {id: 1}) CALL algo.randomWalk(n, 'x', 'K12847') YIELD * RETURN count(*)",
          IllegalArgumentException.class, "algo.randomWalk(): steps must be a number, got String");
      assertAnswer(serverIndex, "MATCH (n:P12847 {id: 1}) CALL algo.randomWalk(n, [[1]], [null]) YIELD * RETURN count(*)",
          IllegalArgumentException.class, "algo.randomWalk(): steps must be a number");
      assertAnswer(serverIndex, "RETURN facet.searchValues('P12847', 'name', 'a', 5) AS v", InvalidArgumentException.class,
          "facet.searchValues: no COLUMNAR index on P12847.name");
      assertAnswer(serverIndex, "CALL db.index.fulltext.queryRelationships('K12847[x]', null) YIELD * RETURN count(*)",
          InvalidArgumentException.class, "db.index.fulltext.queryRelationships(): queryString cannot be null");
      assertAnswer(serverIndex, "CALL db.index.fulltext.queryRelationships('P12847[id]', 'a') YIELD * RETURN count(*)",
          InvalidArgumentException.class, "Index 'P12847[id]' is not a full-text index");
      assertAnswer(serverIndex, "CALL fulltext.searchIndex('P12847[id]', 'alpha') YIELD * RETURN count(*)",
          InvalidArgumentException.class, "Index 'P12847[id]' is not a full-text index");
    });
  }

  private void createSchema(final int serverIndex) throws Exception {
    for (final String statement : new String[] { "CREATE VERTEX TYPE P12847 IF NOT EXISTS",
        "CREATE PROPERTY P12847.id IF NOT EXISTS INTEGER", "CREATE PROPERTY P12847.name IF NOT EXISTS STRING",
        "CREATE INDEX IF NOT EXISTS ON P12847 (name) FULL_TEXT", "CREATE INDEX IF NOT EXISTS ON P12847 (id) UNIQUE",
        "CREATE EDGE TYPE K12847 IF NOT EXISTS", "CREATE PROPERTY K12847.x IF NOT EXISTS STRING",
        "CREATE INDEX IF NOT EXISTS ON K12847 (x) NOTUNIQUE", "DELETE FROM P12847",
        "INSERT INTO P12847 SET id = 1, name = 'alpha'" }) {
      final HttpURLConnection connection = post(serverIndex, "sql", statement);
      try {
        final int statusCode = connection.getResponseCode();
        assertThat(statusCode).as("%s: %s", statement, statusCode < 400 ? "" : readError(connection)).isEqualTo(200);
      } finally {
        connection.disconnect();
      }
    }
  }

  private void assertAnswer(final int serverIndex, final String command, final Class<? extends Throwable> expectedException,
      final String expectedDetail) throws Exception {
    final JSONObject json = execute(serverIndex, command, 400);
    assertThat(json.getString("exception")).as(command).isEqualTo(expectedException.getName());
    assertThat(json.getString("detail")).as(command).contains(expectedDetail);
  }

  private JSONObject execute(final int serverIndex, final String command, final int expectedStatus) throws Exception {
    final HttpURLConnection connection = post(serverIndex, "opencypher", command);
    try {
      final int statusCode = connection.getResponseCode();
      final String response = statusCode < 400 ? readResponse(connection) : readError(connection);
      assertThat(statusCode).as("%s: expected %d, got %d (body=%s)", command, expectedStatus, statusCode, response)
          .isEqualTo(expectedStatus);
      return new JSONObject(response);
    } finally {
      connection.disconnect();
    }
  }

  private HttpURLConnection post(final int serverIndex, final String language, final String command) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL(getServerHttpUrl(serverIndex, "/api/v1/command/graph"))
        .openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    connection.setDoOutput(true);
    final JSONObject payload = new JSONObject().put("language", language).put("command", command);
    try (final PrintWriter pw = new PrintWriter(new OutputStreamWriter(connection.getOutputStream()))) {
      pw.write(payload.toString());
    }
    return connection;
  }
}
