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
package com.arcadedb.server.gremlin;

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5219: a Gremlin query with valid syntax whose eager terminal step raises a
 * runtime error - e.g. {@code g.V().hasLabel('X').drop().next()} on an empty traversal raising
 * {@link java.util.NoSuchElementException} - must be surfaced as a runtime execution error, never as a
 * parsing error. Over HTTP this maps to 500 (Apache TinkerPop's own {@code SERVER_ERROR_SCRIPT_EVALUATION}
 * mapping; 400 is reserved for malformed queries), and the response must not be misreported as
 * "Error on parsing gremlin query" nor as "Error on transaction commit".
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GremlinNextOnEmptyHttpStatusIT extends BaseGraphServerTest {

  @Test
  void nextOnEmptyTraversalReturns500RuntimeError() throws Exception {
    testEachServer(serverIndex -> {
      // V1 exists in the base schema but no vertex matches 'NoSuchLabel', so .next() has nothing to consume.
      final JSONObject json = executeGremlin(serverIndex, "g.V().hasLabel('NoSuchLabel').drop().next()", 500);
      assertThat(json.getString("exception")).isEqualTo(CommandExecutionException.class.getName());
      // It is a runtime error, not a parse error and not a commit failure.
      assertThat(json.getString("error")).doesNotContain("parsing");
      assertThat(json.getString("error")).doesNotContain("Error on transaction commit");
    });
  }

  @Test
  void dropIterateOnEmptyReturns200() throws Exception {
    testEachServer(serverIndex -> {
      // .iterate() consumes the (empty) traversal without demanding an element: success.
      executeGremlin(serverIndex, "g.V().hasLabel('NoSuchLabel').drop().iterate()", 200);
    });
  }

  private JSONObject executeGremlin(final int serverIndex, final String command, final int expectedStatus) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL(
        "http://127.0.0.1:248" + serverIndex + "/api/v1/command/" + getDatabaseName()).openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    connection.setDoOutput(true);
    try {
      final JSONObject payload = new JSONObject().put("language", "gremlin").put("command", command);
      try (final PrintWriter pw = new PrintWriter(new OutputStreamWriter(connection.getOutputStream()))) {
        pw.write(payload.toString());
      }

      final int statusCode = connection.getResponseCode();
      final String response = statusCode < 400 ? readResponse(connection) : readError(connection);

      assertThat(statusCode)
          .as("Gremlin next-on-empty must return %d, got %d (body=%s)", expectedStatus, statusCode, response)
          .isEqualTo(expectedStatus);

      return new JSONObject(response);
    } finally {
      connection.disconnect();
    }
  }
}
