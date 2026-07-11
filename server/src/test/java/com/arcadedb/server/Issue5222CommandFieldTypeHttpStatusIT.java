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

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5222: the HTTP command endpoint used an unchecked Java cast to {@link String}
 * on the {@code language} and {@code command} request fields. A number, array or object value produced a
 * raw {@link ClassCastException} reported as HTTP 500 and leaked internal Java class names to the client.
 * These are client input errors (wrong JSON field type) and must surface as HTTP 400 with a clean,
 * field-level validation message.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5222CommandFieldTypeHttpStatusIT extends BaseGraphServerTest {

  @Test
  void languageAsNumberReturns400() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = execute(serverIndex, new JSONObject().put("language", 123).put("command", "RETURN 1"), 400);
      assertThat(json.getString("detail")).contains("language").contains("string");
      assertThat(json.getString("error")).doesNotContain("ClassCastException");
    });
  }

  @Test
  void languageAsArrayReturns400() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = execute(serverIndex,
          new JSONObject().put("language", new JSONArray().put("cypher")).put("command", "RETURN 1"), 400);
      assertThat(json.getString("detail")).contains("language").contains("string");
    });
  }

  @Test
  void languageAsObjectReturns400() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = execute(serverIndex,
          new JSONObject().put("language", new JSONObject().put("key", "value")).put("command", "RETURN 1"), 400);
      assertThat(json.getString("detail")).contains("language").contains("string");
    });
  }

  @Test
  void commandAsNumberReturns400() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = execute(serverIndex, new JSONObject().put("language", "cypher").put("command", 123), 400);
      assertThat(json.getString("detail")).contains("command").contains("string");
      assertThat(json.getString("error")).doesNotContain("ClassCastException");
    });
  }

  @Test
  void commandAsArrayReturns400() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = execute(serverIndex,
          new JSONObject().put("language", "cypher").put("command", new JSONArray().put("RETURN 1")), 400);
      assertThat(json.getString("detail")).contains("command").contains("string");
    });
  }

  @Test
  void commandAsObjectReturns400() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = execute(serverIndex,
          new JSONObject().put("language", "cypher").put("command", new JSONObject().put("query", "RETURN 1")), 400);
      assertThat(json.getString("detail")).contains("command").contains("string");
    });
  }

  @Test
  void validStringFieldsStillWork() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = execute(serverIndex, new JSONObject().put("language", "cypher").put("command", "RETURN 1 as one"), 200);
      assertThat(json.has("result")).isTrue();
    });
  }

  private JSONObject execute(final int serverIndex, final JSONObject payload, final int expectedStatus) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL(
        "http://127.0.0.1:248" + serverIndex + "/api/v1/command/graph").openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    connection.setDoOutput(true);
    try {
      try (final PrintWriter pw = new PrintWriter(new OutputStreamWriter(connection.getOutputStream()))) {
        pw.write(payload.toString());
      }

      final int statusCode = connection.getResponseCode();
      final String response = expectedStatus == 200 ? readResponse(connection) : readError(connection);

      assertThat(statusCode)
          .as("Invalid field type must return %d, got %d (body=%s)", expectedStatus, statusCode, response)
          .isEqualTo(expectedStatus);

      return new JSONObject(response);
    } finally {
      connection.disconnect();
    }
  }
}
