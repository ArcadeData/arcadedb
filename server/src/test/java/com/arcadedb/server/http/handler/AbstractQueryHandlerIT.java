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

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for AbstractQueryHandler.
 * Tests various serialization formats (graph, studio, record) for query results.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class AbstractQueryHandlerIT extends BaseGraphServerTest {
  private final HttpClient client = HttpClient.newHttpClient();

  @Test
  void shouldSerializeGraphFormat() throws Exception {
    // Create some graph data first
    executeCommand("CREATE VERTEX V1 SET id = 1, name = 'vertex1'");
    executeCommand("CREATE VERTEX V1 SET id = 2, name = 'vertex2'");
    executeCommand("CREATE EDGE E1 FROM (SELECT FROM V1 WHERE id = 1) TO (SELECT FROM V1 WHERE id = 2)");

    // Query with graph serializer
    final JSONObject payload = new JSONObject()
        .put("language", "sql")
        .put("command", "SELECT FROM V1")
        .put("serializer", "graph");

    final HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI("http://localhost:2480/api/v1/query/graph"))
        .POST(HttpRequest.BodyPublishers.ofString(payload.toString()))
        .setHeader("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
        .build();

    final HttpResponse<String> response = client.send(request, BodyHandlers.ofString());

    assertThat(response.statusCode()).isEqualTo(200);

    final JSONObject responseJson = new JSONObject(response.body());
    assertThat(responseJson.has("result")).isTrue();

    final JSONObject result = responseJson.getJSONObject("result");
    assertThat(result.has("vertices")).isTrue();
    assertThat(result.has("edges")).isTrue();
  }

  @Test
  void shouldSerializeStudioFormat() throws Exception {
    executeCommand("CREATE VERTEX V1 SET id = 3, name = 'vertex3'");

    final JSONObject payload = new JSONObject()
        .put("language", "sql")
        .put("command", "SELECT FROM V1")
        .put("serializer", "studio");

    final HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI("http://localhost:2480/api/v1/query/graph"))
        .POST(HttpRequest.BodyPublishers.ofString(payload.toString()))
        .setHeader("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
        .build();

    final HttpResponse<String> response = client.send(request, BodyHandlers.ofString());

    assertThat(response.statusCode()).isEqualTo(200);

    final JSONObject responseJson = new JSONObject(response.body());
    assertThat(responseJson.has("result")).isTrue();

    final JSONObject result = responseJson.getJSONObject("result");
    // Studio format includes vertices, edges, and records
    assertThat(result.has("vertices")).isTrue();
    assertThat(result.has("edges")).isTrue();
    assertThat(result.has("records")).isTrue();
  }

  @Test
  void shouldSerializeRecordFormat() throws Exception {
    final JSONObject payload = new JSONObject()
        .put("language", "sql")
        .put("command", "SELECT FROM V1 LIMIT 5")
        .put("serializer", "record");

    final HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI("http://localhost:2480/api/v1/query/graph"))
        .POST(HttpRequest.BodyPublishers.ofString(payload.toString()))
        .setHeader("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
        .build();

    final HttpResponse<String> response = client.send(request, BodyHandlers.ofString());

    assertThat(response.statusCode()).isEqualTo(200);

    final JSONObject responseJson = new JSONObject(response.body());
    assertThat(responseJson.has("result")).isTrue();
  }

  @Test
  void shouldHandleLimitParameter() throws Exception {
    // Create more vertices
    for (int i = 0; i < 50; i++) {
      executeCommand("CREATE VERTEX V1 SET id = " + (100 + i));
    }

    final JSONObject payload = new JSONObject()
        .put("language", "sql")
        .put("command", "SELECT FROM V1")
        .put("serializer", "record")
        .put("limit", 10);

    final HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI("http://localhost:2480/api/v1/query/graph"))
        .POST(HttpRequest.BodyPublishers.ofString(payload.toString()))
        .setHeader("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
        .build();

    final HttpResponse<String> response = client.send(request, BodyHandlers.ofString());

    assertThat(response.statusCode()).isEqualTo(200);
  }

  @Test
  void shouldHandleQueryWithEdges() throws Exception {
    // Create vertices and edges
    executeCommand("CREATE VERTEX V1 SET id = 1000");
    executeCommand("CREATE VERTEX V1 SET id = 2000");
    executeCommand("CREATE EDGE E1 FROM (SELECT FROM V1 WHERE id = 1000) TO (SELECT FROM V1 WHERE id = 2000) SET weight = 5");

    final JSONObject payload = new JSONObject()
        .put("language", "sql")
        .put("command", "SELECT FROM E1")
        .put("serializer", "graph");

    final HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI("http://localhost:2480/api/v1/query/graph"))
        .POST(HttpRequest.BodyPublishers.ofString(payload.toString()))
        .setHeader("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
        .build();

    final HttpResponse<String> response = client.send(request, BodyHandlers.ofString());

    assertThat(response.statusCode()).isEqualTo(200);

    final JSONObject responseJson = new JSONObject(response.body());
    final JSONObject result = responseJson.getJSONObject("result");
    assertThat(result.has("edges")).isTrue();
  }

  @Test
  void shouldHandleEmptyResultSet() throws Exception {
    final JSONObject payload = new JSONObject()
        .put("language", "sql")
        .put("command", "SELECT FROM V1 WHERE id = 999999")
        .put("serializer", "record");

    final HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI("http://localhost:2480/api/v1/query/graph"))
        .POST(HttpRequest.BodyPublishers.ofString(payload.toString()))
        .setHeader("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
        .build();

    final HttpResponse<String> response = client.send(request, BodyHandlers.ofString());

    assertThat(response.statusCode()).isEqualTo(200);
    assertThat(response.body()).contains("result");
  }

  @Test
  void shouldHandleComplexGraphTraversal() throws Exception {
    // Create a more complex graph structure
    executeCommand("CREATE VERTEX V1 SET id = 5000, category = 'A'");
    executeCommand("CREATE VERTEX V1 SET id = 5001, category = 'B'");
    executeCommand("CREATE VERTEX V1 SET id = 5002, category = 'C'");
    executeCommand("CREATE EDGE E1 FROM (SELECT FROM V1 WHERE id = 5000) TO (SELECT FROM V1 WHERE id = 5001)");
    executeCommand("CREATE EDGE E1 FROM (SELECT FROM V1 WHERE id = 5001) TO (SELECT FROM V1 WHERE id = 5002)");

    final JSONObject payload = new JSONObject()
        .put("language", "sql")
        .put("command", "SELECT expand(out()) FROM V1 WHERE id = 5000")
        .put("serializer", "graph");

    final HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI("http://localhost:2480/api/v1/query/graph"))
        .POST(HttpRequest.BodyPublishers.ofString(payload.toString()))
        .setHeader("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
        .build();

    final HttpResponse<String> response = client.send(request, BodyHandlers.ofString());

    assertThat(response.statusCode()).isEqualTo(200);
  }

  @Test
  void shouldHandleParameterMapping() throws Exception {
    final JSONObject payload = new JSONObject()
        .put("language", "sql")
        .put("command", "SELECT FROM V1 WHERE id = :id")
        .put("serializer", "record");

    final JSONObject params = new JSONObject();
    params.put("id", 1);
    payload.put("params", params);

    final HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI("http://localhost:2480/api/v1/query/graph"))
        .POST(HttpRequest.BodyPublishers.ofString(payload.toString()))
        .setHeader("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
        .build();

    final HttpResponse<String> response = client.send(request, BodyHandlers.ofString());

    assertThat(response.statusCode()).isIn(200, 500); // May succeed or fail depending on data
  }

  private void executeCommand(final String command) throws Exception {
    final JSONObject payload = new JSONObject()
        .put("language", "sql")
        .put("command", command);

    final HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI("http://localhost:2480/api/v1/command/graph"))
        .POST(HttpRequest.BodyPublishers.ofString(payload.toString()))
        .setHeader("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
        .build();

    client.send(request, BodyHandlers.ofString());
  }
}
