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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11886: a traversal TinkerPop refuses for its own shape or for the type of the values it meets is an error in the request
 * and answers HTTP 400 with TinkerPop's message, as a Gremlin syntax error does (#5191, #5201). It answered 500 "Internal error".
 * A valid traversal whose eager terminal step fails at runtime keeps its 500 ({@link GremlinNextOnEmptyHttpStatusIT}).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11886GremlinTraversalErrorHttpStatusIT extends BaseGraphServerTest {

  @ParameterizedTest
  @ValueSource(strings = { //
      "g.V().sum()", //
      "g.E().mean().fold()", //
      "g.V().inV()", //
      "g.V().limit(1).label().values('x')", //
      "g.V().bothE(null, 'x')", //
      "g.V().emit()", //
      "g.V().hasLabel('V1').otherV()", //
      "g.V().to('V1')" })
  void traversalErrorAnswers400(final String traversal) throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = executeGremlin(serverIndex, traversal, 400);
      assertThat(json.getString("detail", json.getString("error", ""))).as(traversal).contains("Invalid gremlin traversal");
    });
  }

  private JSONObject executeGremlin(final int serverIndex, final String command, final int expectedStatus) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL(
        "http://127.0.0.1:" + getServerHttpPort(serverIndex) + "/api/v1/command/" + getDatabaseName()).openConnection();
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
      assertThat(statusCode).as("%s must answer %d, got %d (body=%s)", command, expectedStatus, statusCode, response)
          .isEqualTo(expectedStatus);
      return new JSONObject(response);
    } finally {
      connection.disconnect();
    }
  }
}
