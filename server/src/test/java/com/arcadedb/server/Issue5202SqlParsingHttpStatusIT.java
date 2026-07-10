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

import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5202: SQL statements using clauses that ArcadeDB's SQL dialect does not
 * support ({@code HAVING}, {@code UNION}, {@code INTERSECT}) fail at parse time with a
 * {@link CommandSQLParsingException}. Over HTTP these must surface as 400 Bad Request with the
 * descriptive parsing message, never 500 "Error on transaction commit". The query text is invalid
 * (a client error), so no transaction commit is even involved - the failure happens during parsing,
 * before execution. This is the SQL sibling of issue #5191 (Cypher validation): the same auto-commit
 * wrapper in {@code DatabaseAbstractHandler} wraps the parsing exception in a {@code TransactionException},
 * and {@code AbstractServerHttpHandler} now unwraps {@code CommandParsingException} (superclass of
 * {@code CommandSQLParsingException}) to report 400 with the real cause.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5202SqlParsingHttpStatusIT extends BaseGraphServerTest {

  @Test
  void havingReturns400() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = executeSql(serverIndex, "SELECT count(*) AS cnt FROM A HAVING count(*) > 0", 400);
      assertThat(json.getString("exception")).isEqualTo(CommandSQLParsingException.class.getName());
      // The real parsing message must reach the client, not "Error on transaction commit".
      assertThat(json.getString("detail")).contains("SQL syntax error");
      assertThat(json.getString("error")).doesNotContain("Error on transaction commit");
    });
  }

  @Test
  void unionReturns400() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = executeSql(serverIndex, "SELECT value FROM A UNION SELECT value FROM A", 400);
      assertThat(json.getString("exception")).isEqualTo(CommandSQLParsingException.class.getName());
      assertThat(json.getString("detail")).contains("SQL syntax error");
      assertThat(json.getString("error")).doesNotContain("Error on transaction commit");
    });
  }

  @Test
  void intersectReturns400() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = executeSql(serverIndex, "SELECT value FROM A INTERSECT SELECT value FROM A", 400);
      assertThat(json.getString("exception")).isEqualTo(CommandSQLParsingException.class.getName());
      assertThat(json.getString("detail")).contains("SQL syntax error");
      assertThat(json.getString("error")).doesNotContain("Error on transaction commit");
    });
  }

  private JSONObject executeSql(final int serverIndex, final String command, final int expectedStatus) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL(
        "http://127.0.0.1:248" + serverIndex + "/api/v1/command/graph").openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    connection.setDoOutput(true);
    try {
      final JSONObject payload = new JSONObject().put("language", "sql").put("command", command);
      try (final PrintWriter pw = new PrintWriter(new OutputStreamWriter(connection.getOutputStream()))) {
        pw.write(payload.toString());
      }

      final int statusCode = connection.getResponseCode();
      final String response = readError(connection);

      assertThat(statusCode)
          .as("SQL parsing error must return %d, got %d (body=%s)", expectedStatus, statusCode, response)
          .isEqualTo(expectedStatus);

      return new JSONObject(response);
    } finally {
      connection.disconnect();
    }
  }
}
