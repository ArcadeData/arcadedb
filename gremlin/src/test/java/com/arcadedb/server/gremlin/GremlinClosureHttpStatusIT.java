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

import com.arcadedb.exception.CommandParsingException;
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
 * Regression test for issue #5201: a Gremlin query using a Groovy closure (e.g. {@code filter { ... }}) is
 * rejected by the secure gremlin-lang (java) engine. Over HTTP this must surface as 400 Bad Request with the
 * real parser message, never 500 "Error on transaction commit". The query text is invalid for the secure
 * engine (a client error), not an internal server fault: the parse failure was mis-classified as a
 * transaction-commit failure because the {@link CommandParsingException} was wrapped in a
 * {@code TransactionException} by the auto-commit wrapper.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GremlinClosureHttpStatusIT extends BaseGraphServerTest {

  @Test
  void closureQueryReturns400() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = executeGremlin(serverIndex,
          "g.V().hasLabel('V1').filter { it.get().property('id').value() > 0 }.values('id').fold()", 400);
      assertThat(json.getString("exception")).isEqualTo(CommandParsingException.class.getName());
      // The real parser message must reach the client, not the misleading transaction-commit failure.
      assertThat(json.getString("error")).doesNotContain("Error on transaction commit");
    });
  }

  @Test
  void equivalentNonClosureFormReturns200() throws Exception {
    testEachServer(serverIndex -> {
      // The workaround documented in the issue must keep working: predicate form instead of a Groovy closure.
      executeGremlin(serverIndex, "g.V().hasLabel('V1').has('id', gt(-1)).values('id').fold()", 200);
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
          .as("Gremlin closure query must return %d, got %d (body=%s)", expectedStatus, statusCode, response)
          .isEqualTo(expectedStatus);

      return new JSONObject(response);
    } finally {
      connection.disconnect();
    }
  }
}
