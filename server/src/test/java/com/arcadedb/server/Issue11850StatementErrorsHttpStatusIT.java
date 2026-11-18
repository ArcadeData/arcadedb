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

import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issues #11850 and #11796 over {@code POST /api/v1/command}: statements with a user error, and statements the engine does not
 * implement, answered HTTP 500 (a NullPointerException, a StackOverflowError, a bare UnsupportedOperationException, refusals
 * raised as CommandExecutionException). Each now answers 200 with a sensible result or 400 naming the problem.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11850StatementErrorsHttpStatusIT extends BaseGraphServerTest {

  @Test
  void statementErrorsAnswer400AndSelfReferencesAnswer200() throws Exception {
    testEachServer(serverIndex -> {
      for (final String ddl : new String[] { "CREATE VERTEX TYPE S11850V IF NOT EXISTS", "CREATE PROPERTY S11850V.i IF NOT EXISTS INTEGER",
          "CREATE PROPERTY S11850V.s IF NOT EXISTS STRING", "CREATE INDEX IF NOT EXISTS ON S11850V (i) NOTUNIQUE",
          "CREATE INDEX IF NOT EXISTS ON S11850V (s) FULL_TEXT", "CREATE DOCUMENT TYPE S11850D IF NOT EXISTS",
          "CREATE VERTEX TYPE S11850P IF NOT EXISTS", "CREATE EDGE TYPE S11850K IF NOT EXISTS",
          "INSERT INTO S11850V SET i = 1, s = 'one word'", "INSERT INTO S11850D SET x = 1" })
        command(serverIndex, "sql", ddl, 200);
      command(serverIndex, "opencypher", "CREATE (:S11850P {i: 20})-[:S11850K]->(:S11850P {i: 21})", 200);

      // 200: @rid of a record not saved yet is null, a saved record holding itself is a link and its JSON ends
      command(serverIndex, "sql", "INSERT INTO S11850D SET m = @rid", 200);
      final JSONObject updated = command(serverIndex, "sql", "UPDATE S11850D SET self = @this RETURN AFTER", 200);
      assertThat(updated.toString()).contains("\"self\"");
      for (final String cypher : new String[] { "MATCH (n:S11850P {i: 20}) SET n.z = n RETURN n.z",
          "MATCH (n:S11850P {i: 20}) SET n.z = [1, n] RETURN n.z", "MATCH (n:S11850P {i: 20}) SET n += {z: n} RETURN n.z",
          "MATCH p = (n:S11850P {i: 20})-[:S11850K]->() SET n.z = p RETURN n.z" })
        command(serverIndex, "opencypher", cypher, 200);

      // 400: the statement's error
      for (final String sql : new String[] { "INSERT INTO S11850D SET s = @this", "SELECT percentile(i, null) FROM S11850V",
          "SELECT FROM index:`S11850V[i]` WHERE asSet()", "SELECT search_index('S11850V[s]', null) FROM S11850V",
          "SELECT expand(true, 1)", "SELECT concat(list(mode(i))) FROM S11850V",
          // issue #11796: never implemented, refused with a message naming the statement
          "ALTER BUCKET s11850d NAME renamed", "ALTER BUCKET s11850d CUSTOM k = 'v'", "TRUNCATE RECORD #1:0",
          "TRUNCATE RECORD [#1:0]", "ALIGN DATABASE" }) {
        final JSONObject error = command(serverIndex, "sql", sql, 400);
        assertThat(error.getString("detail", "")).as(sql).isNotEmpty();
      }
      for (final String cypher : new String[] { "RETURN duration(1) AS d", "RETURN duration(true) AS d",
          "WITH null AS x MERGE (n:S11850P {i: x}) RETURN n" })
        command(serverIndex, "opencypher", cypher, 400);
    });
  }

  private JSONObject command(final int serverIndex, final String language, final String command, final int expectedStatus)
      throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL(getServerHttpUrl(serverIndex, "/api/v1/command/graph"))
        .openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    connection.setDoOutput(true);
    try {
      final JSONObject payload = new JSONObject().put("language", language).put("command", command);
      try (final PrintWriter pw = new PrintWriter(new OutputStreamWriter(connection.getOutputStream()))) {
        pw.write(payload.toString());
      }
      final int statusCode = connection.getResponseCode();
      final String response;
      try (final InputStream in = statusCode < 400 ? connection.getInputStream() : connection.getErrorStream()) {
        response = in != null ? new String(in.readAllBytes(), StandardCharsets.UTF_8) : "{}";
      }
      assertThat(statusCode).as("%s: %s answered %d (body=%s)", language, command, statusCode, response).isEqualTo(expectedStatus);
      return new JSONObject(response);
    } finally {
      connection.disconnect();
    }
  }
}
