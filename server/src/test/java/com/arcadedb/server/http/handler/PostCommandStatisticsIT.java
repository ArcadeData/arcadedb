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
package com.arcadedb.server.http.handler;

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URI;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

class PostCommandStatisticsIT extends BaseGraphServerTest {

  @Test
  void writeCommandReturnsCamelCaseStats() throws Exception {
    final JSONObject response = command("opencypher",
        "CREATE (:HttpStat {name:'x'})-[:REL]->(:HttpStat2 {name:'y'})");
    assertThat(response.has("stats")).isTrue();
    final JSONObject stats = response.getJSONObject("stats");
    assertThat(stats.getInt("nodesCreated")).isEqualTo(2);
    assertThat(stats.getInt("relationshipsCreated")).isEqualTo(1);
    assertThat(stats.getInt("propertiesSet")).isEqualTo(2);
    assertThat(stats.getBoolean("containsUpdates")).isTrue();
  }

  @Test
  void readCommandOmitsStats() throws Exception {
    command("opencypher", "CREATE (:HttpStatRead {id:1})");
    final JSONObject response = command("opencypher", "MATCH (n:HttpStatRead) RETURN n");
    assertThat(response.has("stats")).isFalse();
  }

  private JSONObject command(final String language, final String cmd) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(
        "http://127.0.0.1:2480/api/v1/command/graph").toURL().openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    final JSONObject payload = new JSONObject();
    payload.put("language", language);
    payload.put("command", cmd);
    payload.put("serializer", "studio");
    formatPayload(connection, payload);
    connection.connect();
    try {
      final String response = readResponse(connection);
      assertThat(connection.getResponseCode()).isEqualTo(200);
      return new JSONObject(response);
    } finally {
      connection.disconnect();
    }
  }
}
