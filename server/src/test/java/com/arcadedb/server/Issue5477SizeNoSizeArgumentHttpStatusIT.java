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
 * Regression test for issue #5477, reported against the HTTP command API: {@code RETURN size(42)} answered 200 with
 * {@code {"r": null}}, which a client cannot tell apart from legal null propagation. It must be 400 Bad Request with a
 * descriptive type message, while {@code size(null)} keeps answering 200 with {@code null}. Same class of fix as issues
 * #5476, #5294 and #5203.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5477SizeNoSizeArgumentHttpStatusIT extends BaseGraphServerTest {

  @Test
  void sizeOnIntegerReturns400() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = executeCypher(serverIndex, "RETURN size(42) AS r", 400);
      assertThat(json.getString("exception")).isEqualTo(CommandSemanticException.class.getName());
      assertThat(json.getString("detail")).contains("size()").contains("INTEGER");
      assertThat(json.getString("error")).doesNotContain("Error on transaction commit");
    });
  }

  @Test
  void sizeOnBooleanReturns400() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = executeCypher(serverIndex, "RETURN size(true) AS r", 400);
      assertThat(json.getString("exception")).isEqualTo(CommandSemanticException.class.getName());
      assertThat(json.getString("detail")).contains("size()").contains("BOOLEAN");
    });
  }

  @Test
  void isEmptyOnIntegerReturns400() throws Exception {
    // isEmpty() already rejected the argument but reported it as a 500 server failure.
    testEachServer(serverIndex -> {
      final JSONObject json = executeCypher(serverIndex, "RETURN isEmpty(42) AS r", 400);
      assertThat(json.getString("detail")).contains("isEmpty()");
      assertThat(json.getString("error")).doesNotContain("Error on transaction commit");
    });
  }

  @Test
  void sizeOnNullStillReturns200() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = executeCypher(serverIndex, "RETURN size(null) AS r", 200);
      assertThat(json.getJSONArray("result").getJSONObject(0).isNull("r")).isTrue();
    });
  }

  @Test
  void sizeOnStringListAndMapStillReturns200() throws Exception {
    testEachServer(serverIndex -> {
      assertThat(executeCypher(serverIndex, "RETURN size('abc') AS r", 200).getJSONArray("result").getJSONObject(0)
          .getLong("r")).isEqualTo(3L);
      assertThat(executeCypher(serverIndex, "RETURN size([1,2,3]) AS r", 200).getJSONArray("result").getJSONObject(0)
          .getLong("r")).isEqualTo(3L);
      assertThat(executeCypher(serverIndex, "RETURN size({a:1}) AS r", 200).getJSONArray("result").getJSONObject(0)
          .getLong("r")).isEqualTo(1L);
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
      final JSONObject payload = new JSONObject().put("language", "opencypher").put("command", command);
      try (final PrintWriter pw = new PrintWriter(new OutputStreamWriter(connection.getOutputStream()))) {
        pw.write(payload.toString());
      }

      final int statusCode = connection.getResponseCode();
      final String response = expectedStatus == 200 ? readResponse(connection) : readError(connection);

      assertThat(statusCode)
          .as("Cypher type error must return %d, got %d (body=%s)", expectedStatus, statusCode, response)
          .isEqualTo(expectedStatus);

      return new JSONObject(response);
    } finally {
      connection.disconnect();
    }
  }
}
