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

import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12871: over {@code POST /api/v1/command}, a property write on an edge of a {@code LIGHTWEIGHT} type is a caller mistake
 * and answers 400 on every path, as openCypher {@code CREATE (a)-[:K {x: 1}]->(b)} already did, where SQL {@code CREATE EDGE ...
 * SET/CONTENT}, SQL {@code UPDATE} and openCypher {@code SET} answered 500. An empty {@code CONTENT {}} describes no property and
 * creates the edge.
 */
class Issue12871LightweightEdgePropertiesStatusIT extends BaseGraphServerTest {

  private static final String FROM_TO = " from (select from P12871 where k = 1) to (select from P12871 where k = 2)";

  @Test
  void anEmptyContentCreatesTheEdge() throws Exception {
    testEachServer(serverIndex -> {
      prepare(serverIndex);
      post(serverIndex, "sql", "create edge K12871" + FROM_TO + " content {}", null, 200);
      post(serverIndex, "sql", "create edge K12871" + FROM_TO + " content :props",
          new JSONObject().put("props", new JSONObject()), 200);
      assertThat(post(serverIndex, "sql", "select count(*) as c from K12871", null, 200).getJSONArray("result").getJSONObject(0)
          .getLong("c")).isEqualTo(2L);
    });
  }

  @Test
  void aCreateEdgeContentWithAPropertyAnswers400() throws Exception {
    testEachServer(serverIndex -> {
      prepare(serverIndex);
      assertRefused(post(serverIndex, "sql", "create edge K12871" + FROM_TO + " content {\"x\": 1}", null, 400));
    });
  }

  @Test
  void aCreateEdgeSetAnswers400() throws Exception {
    testEachServer(serverIndex -> {
      prepare(serverIndex);
      assertRefused(post(serverIndex, "sql", "create edge K12871" + FROM_TO + " set x = 1", null, 400));
    });
  }

  @Test
  void anUpdateSetAnswers400() throws Exception {
    testEachServer(serverIndex -> {
      prepareWithEdge(serverIndex);
      assertRefused(post(serverIndex, "sql", "update K12871 set x = 1", null, 400));
    });
  }

  @Test
  void aCypherSetPropertyAnswers400() throws Exception {
    testEachServer(serverIndex -> {
      prepareWithEdge(serverIndex);
      assertRefused(post(serverIndex, "opencypher", "MATCH (:P12871 {k: 1})-[r:K12871]->() SET r.x = 5", null, 400));
    });
  }

  @Test
  void aCypherSetMergeMapAnswers400() throws Exception {
    testEachServer(serverIndex -> {
      prepareWithEdge(serverIndex);
      assertRefused(post(serverIndex, "opencypher", "MATCH (:P12871 {k: 1})-[r:K12871]->() SET r += {y: 1}", null, 400));
    });
  }

  @Test
  void aCypherSetReplaceMapAnswers400() throws Exception {
    testEachServer(serverIndex -> {
      prepareWithEdge(serverIndex);
      assertRefused(post(serverIndex, "opencypher", "MATCH (:P12871 {k: 1})-[r:K12871]->() SET r = {y: 1}", null, 400));
    });
  }

  private static void assertRefused(final JSONObject body) {
    assertThat(body.getString("exception")).isEqualTo(IllegalArgumentException.class.getName());
    assertThat(body.getString("detail")).contains("Edge type 'K12871' is declared LIGHTWEIGHT, so its edges cannot have properties");
  }

  private void prepare(final int serverIndex) throws Exception {
    post(serverIndex, "sql", "create vertex type P12871", null, 200);
    post(serverIndex, "sql", "create edge type K12871 lightweight", null, 200);
    post(serverIndex, "sql", "create vertex P12871 set k = 1", null, 200);
    post(serverIndex, "sql", "create vertex P12871 set k = 2", null, 200);
  }

  private void prepareWithEdge(final int serverIndex) throws Exception {
    prepare(serverIndex);
    post(serverIndex, "sql", "create edge K12871" + FROM_TO, null, 200);
  }

  /**
   * Posts the command to {@code /api/v1/command/graph} on {@code serverIndex}, asserts the HTTP status and returns the parsed body:
   * the response stream below 400, the error stream from 400 on.
   */
  private JSONObject post(final int serverIndex, final String language, final String command, final JSONObject params,
      final int expectedStatus) throws Exception {
    final JSONObject payload = new JSONObject().put("language", language).put("command", command);
    if (params != null)
      payload.put("params", params);

    final HttpURLConnection connection = (HttpURLConnection) new URL(getServerHttpUrl(serverIndex, "/api/v1/command/graph"))
        .openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    connection.setDoOutput(true);
    try {
      try (final PrintWriter pw = new PrintWriter(new OutputStreamWriter(connection.getOutputStream()))) {
        pw.write(payload.toString());
      }
      final int statusCode = connection.getResponseCode();
      final String response;
      try (final InputStream in = statusCode < 400 ? connection.getInputStream() : connection.getErrorStream()) {
        response = in != null ? new String(in.readAllBytes(), StandardCharsets.UTF_8) : "{}";
      }
      assertThat(statusCode).as("%s answered %d, expected %d (body=%s)", payload, statusCode, expectedStatus, response)
          .isEqualTo(expectedStatus);
      return new JSONObject(response);
    } finally {
      connection.disconnect();
    }
  }
}
