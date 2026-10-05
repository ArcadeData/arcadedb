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

import com.arcadedb.serializer.json.JSONArray;
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
 * https://github.com/ArcadeData/arcadedb/issues/9141
 * <p>
 * Over {@code POST /api/v1/command} the map results of a Gremlin query lost entries whose keys print alike: the element's id and
 * label to a property named {@code id} or {@code label}, and the groups of a {@code groupCount()} that differ only by type. A null
 * group key was an internal error.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9141GremlinMapResultHttpIT extends BaseGraphServerTest {

  @Test
  void elementMapKeepsIdAndLabelBesideTheProperties() throws Exception {
    testEachServer(serverIndex -> {
      executeGremlin(serverIndex, "g.addV('Map9141').property('name','a').property('id','user-42').property('label','VIP').iterate()");
      final JSONObject json = executeGremlin(serverIndex, "g.V().hasLabel('Map9141').elementMap()");
      final String result = json.getJSONArray("result").toString();
      assertThat(result).contains("user-42").contains("VIP").contains("Map9141");

      // THE T.id AND T.label TOKENS ARE SERIALIZED AS TEXT, NEXT TO THE PROPERTIES NAMED THE SAME
      final JSONArray entries = json.getJSONArray("result").getJSONObject(0).getJSONArray("result");
      int idKeys = 0;
      int labelKeys = 0;
      for (int i = 0; i < entries.length(); i++) {
        final Object key = entries.getJSONObject(i).get("key");
        assertThat(key).isInstanceOf(String.class);
        if ("id".equals(key))
          ++idKeys;
        else if ("label".equals(key))
          ++labelKeys;
      }
      assertThat(idKeys).isEqualTo(2);
      assertThat(labelKeys).isEqualTo(2);
    });
  }

  @Test
  void groupCountKeepsKeysThatDifferByType() throws Exception {
    testEachServer(serverIndex -> {
      executeGremlin(serverIndex, "g.addV('Cnt9141').property('val', 1).iterate()");
      executeGremlin(serverIndex, "g.addV('Cnt9141').property('val', 1L).iterate()");
      executeGremlin(serverIndex, "g.addV('Cnt9141').property('val', '1').iterate()");
      final JSONArray rows = executeGremlin(serverIndex, "g.V().hasLabel('Cnt9141').groupCount().by('val')").getJSONArray("result");
      final JSONArray entries = rows.getJSONObject(0).getJSONArray("result");
      assertThat(entries.length()).isEqualTo(3);
    });
  }

  @Test
  void nullGroupKeyIsAnswered() throws Exception {
    testEachServer(serverIndex -> {
      executeGremlin(serverIndex, "g.addV('Null9141').iterate()");
      final JSONObject json = executeGremlin(serverIndex, "g.V().hasLabel('Null9141').group().by(constant(null)).by(count())");
      assertThat(json.getJSONArray("result").getJSONObject(0).getLong("null")).isEqualTo(1L);
    });
  }

  private JSONObject executeGremlin(final int serverIndex, final String command) throws Exception {
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
      assertThat(statusCode).as("%s -> %s", command, response).isEqualTo(200);
      return new JSONObject(response);
    } finally {
      connection.disconnect();
    }
  }
}
