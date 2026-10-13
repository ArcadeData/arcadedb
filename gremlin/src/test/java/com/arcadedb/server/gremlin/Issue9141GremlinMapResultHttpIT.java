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
      final JSONObject row = json.getJSONArray("result").getJSONObject(0);

      // A FLAT MAP: THE TOKENS ARE @rid / @type, THE PROPERTIES NAMED id / label ARE LEFT ALONE (#9584, #9576)
      assertThat(row.has("result")).isFalse();
      assertThat(row.getString("@type")).isEqualTo("Map9141");
      assertThat(row.getString("@rid")).startsWith("#");
      assertThat(row.getString("id")).isEqualTo("user-42");
      assertThat(row.getString("label")).isEqualTo("VIP");
    });
  }

  @Test
  void groupCountKeepsKeysThatDifferByType() throws Exception {
    testEachServer(serverIndex -> {
      executeGremlin(serverIndex, "g.addV('Cnt9141').property('val', 1).iterate()");
      executeGremlin(serverIndex, "g.addV('Cnt9141').property('val', 1L).iterate()");
      executeGremlin(serverIndex, "g.addV('Cnt9141').property('val', '1').iterate()");
      final JSONArray rows = executeGremlin(serverIndex, "g.V().hasLabel('Cnt9141').groupCount().by('val')").getJSONArray("result");
      assertThat(rows.length()).isEqualTo(1);
      assertThat(rows.getJSONObject(0).has("result")).isFalse();
      assertThat(rows.getJSONObject(0).keySet()).hasSize(3);
    });
  }

  // #9576: ONE QUERY, TWO ROWS, ONE SHAPE
  @Test
  void rowsOfOneResponseShareTheShape() throws Exception {
    testEachServer(serverIndex -> {
      executeGremlin(serverIndex, "g.addV('Mix9576').property('name','a').property('id','user-42').iterate()");
      executeGremlin(serverIndex, "g.addV('Mix9576').property('name','b').iterate()");
      final JSONArray rows = executeGremlin(serverIndex, "g.V().hasLabel('Mix9576').elementMap()").getJSONArray("result");
      assertThat(rows.length()).isEqualTo(2);
      for (int i = 0; i < rows.length(); i++) {
        assertThat(rows.getJSONObject(i).has("result")).isFalse();
        assertThat(rows.getJSONObject(i).getString("@type")).isEqualTo("Mix9576");
        assertThat(rows.getJSONObject(i).getString("@rid")).startsWith("#");
      }
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

  // #9781: A MAP ONE by() DEEPER FOLLOWS THE NAMES OF THE ROW, SO THE RECORD ID IS NEVER LOST TO A PROPERTY CALLED id
  @Test
  void nestedMapsKeepTheTokenNamesOfTheRow() throws Exception {
    testEachServer(serverIndex -> {
      executeGremlin(serverIndex, "g.addV('Nest9781').property('name','a').property('id','uid').iterate()");
      executeGremlin(serverIndex, "g.addV('Nest9781').property('name','b').iterate()");

      final JSONObject top = executeGremlin(serverIndex, "g.V().has('name','a').elementMap()").getJSONArray("result").getJSONObject(0);
      final JSONArray rows = executeGremlin(serverIndex, "g.V().hasLabel('Nest9781').order().by('name').project('x').by(elementMap())")
          .getJSONArray("result");
      assertThat(rows.length()).isEqualTo(2);
      final JSONObject a = rows.getJSONObject(0).getJSONObject("x");
      assertThat(a.getString("@rid")).isEqualTo(top.getString("@rid"));
      assertThat(a.getString("@type")).isEqualTo("Nest9781");
      assertThat(a.getString("id")).isEqualTo("uid");
      final JSONObject b = rows.getJSONObject(1).getJSONObject("x");
      assertThat(b.has("id")).as("the rid of a vertex without the property is not reported as an id").isFalse();
      assertThat(b.getString("@rid")).startsWith("#");

      final JSONObject grouped = executeGremlin(serverIndex, "g.V().hasLabel('Nest9781').group().by('name').by(elementMap())")
          .getJSONArray("result").getJSONObject(0);
      assertThat(grouped.getJSONObject("a").getString("@rid")).isEqualTo(top.getString("@rid"));

      final JSONObject folded = executeGremlin(serverIndex, "g.V().has('name','a').elementMap().fold()").getJSONArray("result")
          .getJSONObject(0);
      assertThat(folded.getJSONArray("result").getJSONObject(0).getString("@rid")).isEqualTo(top.getString("@rid"));
    });
  }

  @Test
  void aNestedNullGroupKeyIsAnswered() throws Exception {
    testEachServer(serverIndex -> {
      executeGremlin(serverIndex, "g.addV('NestNull9781').iterate()");
      final JSONObject json = executeGremlin(serverIndex,
          "g.V().hasLabel('NestNull9781').project('g').by(group().by(constant(null)).by(count()))");
      assertThat(json.getJSONArray("result").getJSONObject(0).getJSONObject("g").getLong("null")).isEqualTo(1L);
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
