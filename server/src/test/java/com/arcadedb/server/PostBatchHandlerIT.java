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
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Test;

import java.io.DataOutputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

class PostBatchHandlerIT extends BaseGraphServerTest {

  @Test
  void jsonlVerticesAndEdges() throws Exception {
    testEachServer((serverIndex) -> {
      final String body = """
          {"@type":"vertex","@class":"V1","@id":"a","id":100}
          {"@type":"vertex","@class":"V1","@id":"b","id":101}
          {"@type":"edge","@class":"E1","@from":"a","@to":"b"}
          """;

      final JSONObject result = postBatch(serverIndex, body, "application/x-ndjson", "");
      assertThat(result.getInt("verticesCreated")).isEqualTo(2);
      assertThat(result.getInt("edgesCreated")).isEqualTo(1);
      assertThat(result.has("idMapping")).isTrue();
      assertThat(result.getLong("elapsedMs")).isGreaterThanOrEqualTo(0);

      // Verify the graph was created correctly
      final JSONObject query = executeCommand(serverIndex, "sql", "SELECT FROM V1 WHERE id >= 100 ORDER BY id ASC");
      assertThat(query.getJSONObject("result").getJSONArray("records").length()).isEqualTo(2);

      // Verify edge exists
      final JSONObject edgeQuery = executeCommand(serverIndex, "sql", "SELECT FROM E1");
      assertThat(edgeQuery.getJSONObject("result").getJSONArray("records").length()).isGreaterThanOrEqualTo(1);
    });
  }

  @Test
  void csvVerticesAndEdges() throws Exception {
    testEachServer((serverIndex) -> {
      final String body = """
          @type,@class,@id,id
          vertex,V1,c1,200
          vertex,V1,c2,201
          ---
          @type,@class,@from,@to
          edge,E1,c1,c2
          """;

      final JSONObject result = postBatch(serverIndex, body, "text/csv", "");
      assertThat(result.getInt("verticesCreated")).isEqualTo(2);
      assertThat(result.getInt("edgesCreated")).isEqualTo(1);

      // Verify
      final JSONObject query = executeCommand(serverIndex, "sql", "SELECT FROM V1 WHERE id >= 200 ORDER BY id ASC");
      assertThat(query.getJSONObject("result").getJSONArray("records").length()).isEqualTo(2);
    });
  }

  @Test
  void vertexOnlyImport() throws Exception {
    testEachServer((serverIndex) -> {
      final String body = """
          {"@type":"vertex","@class":"V1","id":300}
          {"@type":"vertex","@class":"V1","id":301}
          {"@type":"vertex","@class":"V1","id":302}
          """;

      final JSONObject result = postBatch(serverIndex, body, "application/x-ndjson", "");
      assertThat(result.getInt("verticesCreated")).isEqualTo(3);
      assertThat(result.getInt("edgesCreated")).isEqualTo(0);
    });
  }

  @Test
  void edgeWithExistingRid() throws Exception {
    testEachServer((serverIndex) -> {
      // First create some vertices and get their RIDs
      final String createBody = """
          {"@type":"vertex","@class":"V1","@id":"x1","id":400}
          {"@type":"vertex","@class":"V1","@id":"x2","id":401}
          """;

      final JSONObject createResult = postBatch(serverIndex, createBody, "application/x-ndjson", "");
      final JSONObject mapping = createResult.getJSONObject("idMapping");
      final String rid1 = mapping.getString("x1");
      final String rid2 = mapping.getString("x2");

      // Now create edges using RIDs directly
      final String edgeBody = "{\"@type\":\"edge\",\"@class\":\"E1\",\"@from\":\"" + rid1 + "\",\"@to\":\"" + rid2 + "\"}\n";

      final JSONObject edgeResult = postBatch(serverIndex, edgeBody, "application/x-ndjson", "");
      assertThat(edgeResult.getInt("edgesCreated")).isEqualTo(1);
    });
  }

  @Test
  void unknownTempIdReturnsError() throws Exception {
    testEachServer((serverIndex) -> {
      final String body = """
          {"@type":"edge","@class":"E1","@from":"nonexistent","@to":"also_nonexistent"}
          """;

      final HttpURLConnection conn = openBatchConnection(serverIndex, "application/x-ndjson", "");
      writeBody(conn, body);
      conn.connect();

      // Should return an error (IllegalArgumentException -> 400)
      assertThat(conn.getResponseCode()).isEqualTo(400);
      conn.disconnect();
    });
  }

  @Test
  void lightEdgesParameter() throws Exception {
    testEachServer((serverIndex) -> {
      final String body = """
          {"@type":"vertex","@class":"V1","@id":"le1","id":500}
          {"@type":"vertex","@class":"V1","@id":"le2","id":501}
          {"@type":"edge","@class":"E1","@from":"le1","@to":"le2"}
          """;

      final JSONObject result = postBatch(serverIndex, body, "application/x-ndjson", "lightEdges=true");
      assertThat(result.getInt("verticesCreated")).isEqualTo(2);
      assertThat(result.getInt("edgesCreated")).isEqualTo(1);
    });
  }

  @Test
  void edgeWithProperties() throws Exception {
    testEachServer((serverIndex) -> {
      final String body = """
          {"@type":"vertex","@class":"V1","@id":"ep1","id":600}
          {"@type":"vertex","@class":"V1","@id":"ep2","id":601}
          {"@type":"edge","@class":"E1","@from":"ep1","@to":"ep2","weight":0.75,"label":"friend"}
          """;

      final JSONObject result = postBatch(serverIndex, body, "application/x-ndjson", "");
      assertThat(result.getInt("edgesCreated")).isEqualTo(1);
    });
  }

  private JSONObject postBatch(final int serverIndex, final String body, final String contentType,
      final String queryParams) throws Exception {
    final HttpURLConnection conn = openBatchConnection(serverIndex, contentType, queryParams);
    writeBody(conn, body);
    conn.connect();

    try {
      final String response = readResponse(conn);
      assertThat(conn.getResponseCode()).isEqualTo(200);
      return new JSONObject(response);
    } finally {
      conn.disconnect();
    }
  }

  private HttpURLConnection openBatchConnection(final int serverIndex, final String contentType,
      final String queryParams) throws Exception {
    String url = "http://127.0.0.1:248" + serverIndex + "/api/v1/batch/graph";
    if (queryParams != null && !queryParams.isEmpty())
      url += "?" + queryParams;

    final HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
    conn.setRequestMethod("POST");
    conn.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    conn.setRequestProperty("Content-Type", contentType);
    conn.setDoOutput(true);
    return conn;
  }

  private void writeBody(final HttpURLConnection conn, final String body) throws Exception {
    final byte[] data = body.getBytes(StandardCharsets.UTF_8);
    conn.setRequestProperty("Content-Length", Integer.toString(data.length));
    try (final DataOutputStream wr = new DataOutputStream(conn.getOutputStream())) {
      wr.write(data);
    }
  }
}
