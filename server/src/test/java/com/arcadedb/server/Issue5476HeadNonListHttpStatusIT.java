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

import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5476: {@code RETURN head(42)} (and the same for last()/tail() with any non-list
 * argument) used to answer HTTP 200 with {@code {"r": null}}, so a query with a type error looked like a
 * successful query returning no value. The Cypher signature is {@code head(list :: LIST<ANY>) :: ANY}, so a
 * non-list argument is a client error and must surface as 400 Bad Request with a descriptive type message,
 * matching Neo4j and Memgraph. Same class of fix as issues #5203 and #5294.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5476HeadNonListHttpStatusIT extends BaseGraphServerTest {

  @Test
  void headOnIntegerReturns400() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = executeCypher(serverIndex, "RETURN head(42) AS r", 400);
      assertThat(json.getString("exception")).isEqualTo(CommandSemanticException.class.getName());
      assertThat(json.getString("detail")).contains("head()").contains("LIST").contains("INTEGER");
      assertThat(json.getString("error")).doesNotContain("Error on transaction commit");
    });
  }

  @Test
  void headOnFloatReturns400() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = executeCypher(serverIndex, "RETURN head(3.14) AS r", 400);
      assertThat(json.getString("exception")).isEqualTo(CommandSemanticException.class.getName());
      assertThat(json.getString("detail")).contains("head()").contains("FLOAT");
    });
  }

  @Test
  void headOnBooleanReturns400() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = executeCypher(serverIndex, "RETURN head(true) AS r", 400);
      assertThat(json.getString("exception")).isEqualTo(CommandSemanticException.class.getName());
      assertThat(json.getString("detail")).contains("head()").contains("BOOLEAN");
    });
  }

  @Test
  void lastAndTailOnNonListReturn400() throws Exception {
    testEachServer(serverIndex -> {
      assertThat(executeCypher(serverIndex, "RETURN last('abc') AS r", 400).getString("detail")).contains("last()")
          .contains("STRING");
      assertThat(executeCypher(serverIndex, "RETURN tail(42) AS r", 400).getString("detail")).contains("tail()");
    });
  }

  @Test
  void headOnListStillReturns200() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = executeCypher(serverIndex, "RETURN head([42]) AS r", 200);
      assertThat(json.getJSONArray("result").getJSONObject(0).getLong("r")).isEqualTo(42L);
    });
  }

  @Test
  void headOnNullStillReturns200() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = executeCypher(serverIndex, "RETURN head(null) AS r", 200);
      assertThat(json.getJSONArray("result").getJSONObject(0).isNull("r")).isTrue();
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
      final String response = expectedStatus == 200 ? readResponse(connection) : readError(connection);

      assertThat(statusCode)
          .as("Cypher list-function type error must return %d, got %d (body=%s)", expectedStatus, statusCode, response)
          .isEqualTo(expectedStatus);

      return new JSONObject(response);
    } finally {
      connection.disconnect();
    }
  }
}
