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

import com.arcadedb.index.fulltext.FullTextQueryParseException;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7862, over the wire: a Lucene expression the parser refuses, reaching the server through
 * {@code SEARCH_INDEX()} or a {@code db.index.fulltext.query*} procedure, must answer 400 with the parse
 * message - not 500 with a stack trace in the server log, which is what #7393 established for the hybrid and
 * full-text search APIs and what these four call sites were still doing.
 * <p>
 * The engine half is {@code Issue7862MalformedFullTextQueryAtEveryCallSiteTest}; this pins the HTTP half, which
 * is a ladder of its own in {@code AbstractServerHttpHandler} rather than {@code ErrorCategory}, and therefore
 * had to be given its own arm.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7862FullTextParseErrorHttpStatusIT extends BaseGraphServerTest {

  @Test
  void aMalformedSearchIndexQueryReturns400() throws Exception {
    testEachServer(serverIndex -> {
      createFullTextIndex(serverIndex);

      final JSONObject json = executeCommand(serverIndex, "sql",
          "SELECT FROM Doc7862 WHERE SEARCH_INDEX('Doc7862[content]', 'foo AND') = true", 400);

      assertThat(json.getString("exception")).isEqualTo(FullTextQueryParseException.class.getName());
      assertThat(json.getString("detail")).contains("Invalid search query");
      assertThat(json.getString("error")).doesNotContain("Internal error");
    });
  }

  @Test
  void aMalformedCypherFullTextProcedureQueryReturns400() throws Exception {
    testEachServer(serverIndex -> {
      createFullTextIndex(serverIndex);

      final JSONObject json = executeCommand(serverIndex, "cypher",
          "CALL db.index.fulltext.queryNodes('Doc7862[content]', 'foo AND')", 400);

      assertThat(json.getString("exception")).isEqualTo(FullTextQueryParseException.class.getName());
      assertThat(json.getString("detail")).contains("Invalid search query");
    });
  }

  /**
   * The counter-case: a well-formed query on the same route still answers 200, so the 400s above are the
   * parser's verdict and not the route being broken.
   */
  @Test
  void aWellFormedSearchIndexQueryStillReturns200() throws Exception {
    testEachServer(serverIndex -> {
      createFullTextIndex(serverIndex);

      executeCommand(serverIndex, "sql",
          "SELECT FROM Doc7862 WHERE SEARCH_INDEX('Doc7862[content]', 'gearbox') = true", 200);
    });
  }

  private void createFullTextIndex(final int serverIndex) throws Exception {
    executeCommand(serverIndex, "sqlscript",
        "CREATE VERTEX TYPE Doc7862 IF NOT EXISTS;"
            + "CREATE PROPERTY Doc7862.content IF NOT EXISTS STRING;"
            + "CREATE INDEX IF NOT EXISTS ON Doc7862 (content) FULL_TEXT;", 200);
  }

  private JSONObject executeCommand(final int serverIndex, final String language, final String command,
      final int expectedStatus) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL(
        "http://127.0.0.1:248" + serverIndex + "/api/v1/command/graph").openConnection();
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
      final String response = statusCode < 400 ? readResponse(connection) : readError(connection);

      assertThat(statusCode).as("expected %d, got %d (body=%s)", expectedStatus, statusCode, response)
          .isEqualTo(expectedStatus);

      return new JSONObject(response);
    } finally {
      connection.disconnect();
    }
  }
}
