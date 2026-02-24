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

import com.arcadedb.database.Database;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Server integration test for issue #3514:
 * UNWIND + MATCH + MERGE with nested property access (row.features.chunk) via HTTP
 * must succeed without "Not a JSON Object: []" or security exceptions.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue3514UnwindMergeNestedPropsIT extends BaseGraphServerTest {

  private String sourceId;
  private String targetId;
  private String chunkId;

  @Override
  protected String getDatabaseName() {
    return "issue3514";
  }

  @Override
  protected void populateDatabase() {
    final Database database = getDatabase(0);

    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("causal_action");
    database.getSchema().createEdgeType("risk_area");

    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Node");
      a.save();
      sourceId = a.getIdentity().toString();

      final MutableVertex b = database.newVertex("Node");
      b.save();
      targetId = b.getIdentity().toString();

      final MutableVertex chunk = database.newVertex("Node");
      chunk.save();
      chunkId = chunk.getIdentity().toString();
    });
  }

  /**
   * Reproduces the exact query pattern from issue #3514 via HTTP:
   * UNWIND $batch as row
   * MATCH (a) WHERE ID(a) = row.source_id
   * MATCH (b) WHERE ID(b) = row.target_id
   * MERGE (a)-[r:`causal_action`{chunk: row.features.chunk}]->(b)
   * RETURN a, b, r
   *
   * The batch parameter contains nested properties like {"features": {"chunk": "#X:X"}}.
   * This used to fail with "Not a JSON Object: []" or security exceptions.
   */
  @Test
  void unwindMatchMergeWithNestedPropertyAccessViaHttp() throws Exception {
    testEachServer((serverIndex) -> {
      final String query = "UNWIND $batch as row " +
          "MATCH (a) WHERE ID(a) = row.source_id " +
          "MATCH (b) WHERE ID(b) = row.target_id " +
          "MERGE (a)-[r:`causal_action`{chunk: row.features.chunk}]->(b) " +
          "RETURN a, b, r";

      // Build the nested batch parameter as JSON, matching exactly what a Python client sends
      final JSONObject featuresMap = new JSONObject().put("chunk", chunkId);
      final JSONObject rowObj = new JSONObject()
          .put("source_id", sourceId)
          .put("target_id", targetId)
          .put("features", featuresMap);
      final JSONArray batchArray = new JSONArray().put(rowObj);
      final JSONObject params = new JSONObject().put("batch", batchArray);

      final JSONObject payload = new JSONObject()
          .put("language", "opencypher")
          .put("command", query)
          .put("params", params);

      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/command/" + getDatabaseName()).openConnection();

      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(
              ("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      connection.setRequestProperty("Content-Type", "application/json");

      formatPayload(connection, payload);
      connection.connect();

      try {
        assertThat(connection.getResponseCode())
            .as("MERGE with nested property access should succeed (HTTP 200)")
            .isEqualTo(200);

        final String response = readResponse(connection);
        final JSONObject json = new JSONObject(response);

        assertThat(json.has("result")).isTrue();
        final JSONArray result = json.getJSONArray("result");
        assertThat(result.length())
            .as("MERGE should return exactly 1 result row")
            .isEqualTo(1);

        final JSONObject row = result.getJSONObject(0);
        assertThat(row.has("r"))
            .as("Result must contain the edge 'r'")
            .isTrue();
      } finally {
        connection.disconnect();
      }
    });
  }

  /**
   * Verifies MERGE is idempotent: running the same query twice must not create duplicate edges.
   */
  @Test
  void unwindMatchMergeIdempotentViaHttp() throws Exception {
    testEachServer((serverIndex) -> {
      final String query = "UNWIND $batch as row " +
          "MATCH (a) WHERE ID(a) = row.source_id " +
          "MATCH (b) WHERE ID(b) = row.target_id " +
          "MERGE (a)-[r:`causal_action`{chunk: row.features.chunk}]->(b) " +
          "RETURN a, b, r";

      final JSONObject featuresMap = new JSONObject().put("chunk", chunkId);
      final JSONObject rowObj = new JSONObject()
          .put("source_id", sourceId)
          .put("target_id", targetId)
          .put("features", featuresMap);
      final JSONArray batchArray = new JSONArray().put(rowObj);
      final JSONObject params = new JSONObject().put("batch", batchArray);

      final JSONObject payload = new JSONObject()
          .put("language", "opencypher")
          .put("command", query)
          .put("params", params);

      // First call: creates the edge
      for (int call = 0; call < 2; call++) {
        final HttpURLConnection connection = (HttpURLConnection) new URL(
            "http://127.0.0.1:248" + serverIndex + "/api/v1/command/" + getDatabaseName()).openConnection();

        connection.setRequestMethod("POST");
        connection.setRequestProperty("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(
                ("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
        connection.setRequestProperty("Content-Type", "application/json");

        formatPayload(connection, payload);
        connection.connect();

        try {
          assertThat(connection.getResponseCode())
              .as("Call " + (call + 1) + " should succeed (HTTP 200)")
              .isEqualTo(200);
        } finally {
          connection.disconnect();
        }
      }

      // Verify only one edge was created
      final HttpURLConnection countConn = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/command/" + getDatabaseName()).openConnection();

      countConn.setRequestMethod("POST");
      countConn.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(
              ("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      countConn.setRequestProperty("Content-Type", "application/json");

      final JSONObject countPayload = new JSONObject()
          .put("language", "opencypher")
          .put("command", "MATCH (a)-[r:`causal_action`]->(b) RETURN count(r) as cnt");

      formatPayload(countConn, countPayload);
      countConn.connect();

      try {
        assertThat(countConn.getResponseCode()).isEqualTo(200);
        final JSONObject countResponse = new JSONObject(readResponse(countConn));
        final long count = countResponse.getJSONArray("result").getJSONObject(0).getLong("cnt");
        assertThat(count)
            .as("Only one causal_action edge should exist after two identical MERGE calls")
            .isEqualTo(1L);
      } finally {
        countConn.disconnect();
      }
    });
  }

  /**
   * Verifies MERGE auto-creates the edge type when it doesn't exist in the schema,
   * and the root user still has CREATE access on the new type (no security exception).
   */
  @Test
  void unwindMatchMergeAutoCreatesEdgeTypeViaHttp() throws Exception {
    testEachServer((serverIndex) -> {
      final String query = "UNWIND $batch as row " +
          "MATCH (a) WHERE ID(a) = row.source_id " +
          "MATCH (b) WHERE ID(b) = row.target_id " +
          "MERGE (a)-[r:`new_relation_type`{chunk: row.features.chunk}]->(b) " +
          "RETURN a, b, r";

      final JSONObject featuresMap = new JSONObject().put("chunk", chunkId);
      final JSONObject rowObj = new JSONObject()
          .put("source_id", sourceId)
          .put("target_id", targetId)
          .put("features", featuresMap);
      final JSONArray batchArray = new JSONArray().put(rowObj);
      final JSONObject params = new JSONObject().put("batch", batchArray);

      final JSONObject payload = new JSONObject()
          .put("language", "opencypher")
          .put("command", query)
          .put("params", params);

      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/command/" + getDatabaseName()).openConnection();

      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(
              ("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      connection.setRequestProperty("Content-Type", "application/json");

      formatPayload(connection, payload);
      connection.connect();

      try {
        assertThat(connection.getResponseCode())
            .as("MERGE should succeed even when auto-creating a new edge type (no security exception)")
            .isEqualTo(200);

        final String response = readResponse(connection);
        final JSONObject json = new JSONObject(response);
        assertThat(json.getJSONArray("result").length())
            .as("Should return 1 result row")
            .isEqualTo(1);
      } finally {
        connection.disconnect();
      }
    });
  }

  /**
   * Regression test for the security race condition in issue #3514:
   * Rapidly creating many new edge types via MERGE must all succeed without
   * "User 'root' is not allowed to create records" errors.
   * <p>
   * Before the fix, the fileAccessMap in ServerSecurityDatabaseUser could be stale
   * when a new type was created, causing access to be denied for the new type's files.
   */
  @Test
  void rapidMergeAutoCreatesMultipleEdgeTypesViaHttp() throws Exception {
    testEachServer((serverIndex) -> {
      final int typeCount = 10;
      for (int i = 0; i < typeCount; i++) {
        final String edgeType = "auto_edge_" + i;
        final String query = "UNWIND $batch as row " +
            "MATCH (a) WHERE ID(a) = row.source_id " +
            "MATCH (b) WHERE ID(b) = row.target_id " +
            "MERGE (a)-[r:`" + edgeType + "`{chunk: row.features.chunk}]->(b) " +
            "RETURN a, b, r";

        final JSONObject featuresMap = new JSONObject().put("chunk", chunkId);
        final JSONObject rowObj = new JSONObject()
            .put("source_id", sourceId)
            .put("target_id", targetId)
            .put("features", featuresMap);
        final JSONArray batchArray = new JSONArray().put(rowObj);
        final JSONObject params = new JSONObject().put("batch", batchArray);

        final JSONObject payload = new JSONObject()
            .put("language", "opencypher")
            .put("command", query)
            .put("params", params);

        final HttpURLConnection connection = (HttpURLConnection) new URL(
            "http://127.0.0.1:248" + serverIndex + "/api/v1/command/" + getDatabaseName()).openConnection();

        connection.setRequestMethod("POST");
        connection.setRequestProperty("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(
                ("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
        connection.setRequestProperty("Content-Type", "application/json");

        formatPayload(connection, payload);
        connection.connect();

        try {
          assertThat(connection.getResponseCode())
              .as("MERGE auto-creating edge type '" + edgeType + "' should succeed (no security exception)")
              .isEqualTo(200);
        } finally {
          connection.disconnect();
        }
      }
    });
  }

  /**
   * Verifies that the nested property value (row.features.chunk) is correctly stored on the edge.
   */
  @Test
  void unwindMatchMergeNestedPropertyValueIsStoredOnEdge() throws Exception {
    testEachServer((serverIndex) -> {
      final String query = "UNWIND $batch as row " +
          "MATCH (a) WHERE ID(a) = row.source_id " +
          "MATCH (b) WHERE ID(b) = row.target_id " +
          "MERGE (a)-[r:`risk_area`{chunk: row.features.chunk}]->(b) " +
          "RETURN r.chunk as edgeChunk";

      final JSONObject featuresMap = new JSONObject().put("chunk", chunkId);
      final JSONObject rowObj = new JSONObject()
          .put("source_id", sourceId)
          .put("target_id", targetId)
          .put("features", featuresMap);
      final JSONArray batchArray = new JSONArray().put(rowObj);
      final JSONObject params = new JSONObject().put("batch", batchArray);

      final JSONObject payload = new JSONObject()
          .put("language", "opencypher")
          .put("command", query)
          .put("params", params);

      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/command/" + getDatabaseName()).openConnection();

      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(
              ("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      connection.setRequestProperty("Content-Type", "application/json");

      formatPayload(connection, payload);
      connection.connect();

      try {
        assertThat(connection.getResponseCode())
            .as("MERGE with nested property access should succeed")
            .isEqualTo(200);

        final String response = readResponse(connection);
        final JSONObject json = new JSONObject(response);
        final JSONArray result = json.getJSONArray("result");
        assertThat(result.length()).isEqualTo(1);

        final JSONObject row = result.getJSONObject(0);
        assertThat(row.getString("edgeChunk"))
            .as("The nested property row.features.chunk should be stored on the edge")
            .isEqualTo(chunkId);
      } finally {
        connection.disconnect();
      }
    });
  }
}
