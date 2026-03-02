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
package com.arcadedb.server.http;

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

class QueryEndpointReadOnlyTest extends BaseGraphServerTest {

  private static final String DATABASE_NAME = "graph";

  @Test
  void queryEndpointRejectsSqlInsert() throws Exception {
    testEachServer((serverIndex) -> {
      executeCommand(serverIndex, "sql", "CREATE DOCUMENT TYPE ReadOnlyTest IF NOT EXISTS");

      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/query/" + DATABASE_NAME).openConnection();
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));

      final JSONObject payload = new JSONObject();
      payload.put("language", "sql");
      payload.put("command", "INSERT INTO ReadOnlyTest SET name='should_fail'");

      formatPayload(connection, payload);
      connection.connect();

      try {
        assertThat(connection.getResponseCode()).isEqualTo(400);
        final String error = readError(connection);
        assertThat(error).contains("idempotent");
      } finally {
        connection.disconnect();
      }
    });
  }

  @Test
  void queryEndpointRejectsSqlScriptInsert() throws Exception {
    testEachServer((serverIndex) -> {
      executeCommand(serverIndex, "sql", "CREATE DOCUMENT TYPE ReadOnlyTest2 IF NOT EXISTS");

      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/query/" + DATABASE_NAME).openConnection();
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));

      final JSONObject payload = new JSONObject();
      payload.put("language", "sqlscript");
      payload.put("command", "INSERT INTO ReadOnlyTest2 SET name='should_fail';");

      formatPayload(connection, payload);
      connection.connect();

      try {
        assertThat(connection.getResponseCode()).isEqualTo(400);
        final String error = readError(connection);
        assertThat(error).contains("idempotent");
      } finally {
        connection.disconnect();
      }
    });
  }

  @Test
  void queryEndpointAllowsSqlSelect() throws Exception {
    testEachServer((serverIndex) -> {
      executeCommand(serverIndex, "sql", "CREATE DOCUMENT TYPE ReadOnlyTest3 IF NOT EXISTS");
      executeCommand(serverIndex, "sql", "INSERT INTO ReadOnlyTest3 SET name='readable'");

      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/query/" + DATABASE_NAME).openConnection();
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));

      final JSONObject payload = new JSONObject();
      payload.put("language", "sql");
      payload.put("command", "SELECT FROM ReadOnlyTest3");

      formatPayload(connection, payload);
      connection.connect();

      try {
        final String response = readResponse(connection);
        assertThat(connection.getResponseCode()).isEqualTo(200);

        final JSONObject responseJson = new JSONObject(response);
        assertThat(responseJson.has("result")).isTrue();
      } finally {
        connection.disconnect();
      }
    });
  }
}
