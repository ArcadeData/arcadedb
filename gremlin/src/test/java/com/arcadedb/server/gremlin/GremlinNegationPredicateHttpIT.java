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
 * Regression test for issue #5192: valid Gremlin negation predicates {@code has('id', neq(1))} and
 * {@code has('id', not(eq(1)))} sent over HTTP {@code /api/v1/command} must return 200 with the filtered
 * result, not 500 "Error on transaction commit".
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GremlinNegationPredicateHttpIT extends BaseGraphServerTest {

  @Test
  void neqPredicateReturns200() throws Exception {
    testEachServer(serverIndex -> {
      setupData(serverIndex);
      final JSONObject json = executeGremlin(serverIndex,
          "g.V().hasLabel('Neg5192').has('id', neq(1)).values('id').fold()", 200);
      assertThat(json.getJSONArray("result").getJSONObject(0).getJSONArray("result").toList()).containsExactly(2);
    });
  }

  @Test
  void notEqPredicateReturns200() throws Exception {
    testEachServer(serverIndex -> {
      setupData(serverIndex);
      final JSONObject json = executeGremlin(serverIndex,
          "g.V().hasLabel('Neg5192').has('id', not(eq(1))).values('id').fold()", 200);
      assertThat(json.getJSONArray("result").getJSONObject(0).getJSONArray("result").toList()).containsExactly(2);
    });
  }

  @Test
  void invalidNotFormReturns400NotTransactionCommitError() throws Exception {
    testEachServer(serverIndex -> {
      setupData(serverIndex);
      // not(1) is not a valid TinkerPop predicate: it must surface as a 400 parse error with the real
      // parser message, never as 500 "Error on transaction commit" (see #5201).
      final JSONObject json = executeGremlin(serverIndex,
          "g.V().hasLabel('Neg5192').has('id', not(1)).values('id').fold()", 400);
      assertThat(json.getString("error")).doesNotContain("Error on transaction commit");
    });
  }

  private void setupData(final int serverIndex) throws Exception {
    executeGremlin(serverIndex, "g.addV('Neg5192').property('id', 1).iterate()", 200);
    executeGremlin(serverIndex, "g.addV('Neg5192').property('id', 2).iterate()", 200);
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
          .as("Gremlin negation predicate query must return %d, got %d (body=%s)", expectedStatus, statusCode, response)
          .isEqualTo(expectedStatus);

      return new JSONObject(response);
    } finally {
      connection.disconnect();
    }
  }
}
