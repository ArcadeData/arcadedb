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
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #4480: an OpenCypher relationship-type disjunction ([:A|B]) must
 * match when either relationship type matches, exercised over the HTTP /api/v1/command endpoint
 * (the path used in the original bug report).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue4480RelTypeDisjunctionHttpIT extends BaseGraphServerTest {

  @Test
  void relTypeDisjunctionMatchesOverHttp() throws Exception {
    testEachServer(serverIndex -> {
      command(serverIndex, "opencypher", "CREATE (a:T {id:1})-[:R1]->(b:T {id:2})");

      assertThat(countCommand(serverIndex, "MATCH (a:T)-[:R1]->(b:T) RETURN count(*) AS c")).isEqualTo(1);
      assertThat(countCommand(serverIndex, "MATCH (a:T)-[:R1|R2]->(b:T) RETURN count(*) AS c")).isEqualTo(1);
      assertThat(countCommand(serverIndex, "MATCH (a:T)-[:R2|R1]->(b:T) RETURN count(*) AS c")).isEqualTo(1);
      assertThat(countCommand(serverIndex,
          "MATCH (a:T)-[r]->(b:T) WHERE type(r) IN ['R1','R2'] RETURN count(*) AS c")).isEqualTo(1);
    });
  }

  private int countCommand(final int serverIndex, final String cypher) throws Exception {
    final String body = command(serverIndex, "opencypher", cypher);
    final JSONObject json = new JSONObject(body);
    return json.getJSONArray("result").getJSONObject(0).getInt("c");
  }

  private String command(final int serverIndex, final String language, final String cypher) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL(
        "http://127.0.0.1:248" + serverIndex + "/api/v1/command/graph").openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    connection.setDoOutput(true);
    try {
      final JSONObject payload = new JSONObject().put("language", language).put("command", cypher);
      try (final PrintWriter pw = new PrintWriter(new OutputStreamWriter(connection.getOutputStream()))) {
        pw.write(payload.toString());
      }
      assertThat(connection.getResponseCode()).isEqualTo(200);
      try (final InputStream in = connection.getInputStream()) {
        return new String(in.readAllBytes());
      }
    } finally {
      connection.disconnect();
    }
  }
}
