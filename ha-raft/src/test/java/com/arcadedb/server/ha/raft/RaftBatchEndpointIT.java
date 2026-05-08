/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.server.ha.raft;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import java.io.DataOutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for the {@code POST /api/v1/batch/{db}} endpoint when running on a Raft HA
 * cluster. Covers:
 * <ul>
 *   <li>Issue #4123: HTTP 500 with {@code phase1.result is null} NPE on the LEADER, because the
 *       endpoint defaults to {@code wal=false} (set by the GraphBatch builder) and the Raft commit
 *       path required WAL bytes to replicate. The first commit reached
 *       {@code phase1.result.toByteArray()} with a null result and exploded.</li>
 *   <li>Issue #4122: HTTP 500 {@code "Error on updating dictionary for key 'key_3'"} on a FOLLOWER
 *       when the request introduces multiple new property keys, because the endpoint did not
 *       forward to the leader and a follower cannot independently mutate the schema dictionary.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RaftBatchEndpointIT extends BaseRaftHATest {

  private static final String VERTEX_TYPE = "BatchNode";
  private static final String EDGE_TYPE   = "BatchEdge";

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "majority");
  }

  @Override
  protected int getServerCount() {
    return 3;
  }

  /**
   * Reproduces issue #4123: the /batch endpoint sent to the leader returned HTTP 500 with
   * {@code "Cannot invoke ... Binary.toByteArray() because phase1.result is null"} because the
   * endpoint left WAL disabled by default. Without WAL data, the Raft commit path could not
   * package the transaction for replication.
   */
  @Test
  void postBatchOnLeaderWithDefaultWAL() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    createSchema(leaderIndex);

    final int numVertices = 200;
    final int numEdges = 200;
    final String body = buildBatchPayload(numVertices, numEdges);

    final HttpResult result = postBatch(leaderIndex, body, "batchSize=50000&commitEvery=50000");

    assertThat(result.statusCode())
        .as("POST /batch on leader must succeed (issue #4123)\nbody=%s", result.body())
        .isEqualTo(200);

    final JSONObject json = new JSONObject(result.body());
    assertThat(json.getInt("verticesCreated")).isEqualTo(numVertices);
    assertThat(json.getInt("edgesCreated")).isEqualTo(numEdges);

    assertClusterConsistency();
    for (int i = 0; i < getServerCount(); i++) {
      final Database nodeDb = getServerDatabase(i, getDatabaseName());
      assertThat(nodeDb.countType(VERTEX_TYPE, true))
          .as("Server %d should have %d %s records", i, numVertices, VERTEX_TYPE)
          .isEqualTo(numVertices);
      assertThat(nodeDb.countType(EDGE_TYPE, true))
          .as("Server %d should have %d %s records", i, numEdges, EDGE_TYPE)
          .isEqualTo(numEdges);
    }
  }

  /**
   * Reproduces issue #4122: posting a /batch payload to a FOLLOWER with several new property
   * keys returned HTTP 500 {@code "Error on updating dictionary for key 'key_3'"}. The follower
   * cannot mutate the schema dictionary itself, so the endpoint must forward to the leader.
   */
  @Test
  void postBatchOnFollowerWithMultipleNewKeys() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int followerIndex = (leaderIndex + 1) % getServerCount();

    createSchema(leaderIndex);
    waitForReplicationIsCompleted(followerIndex);

    // Match the user's reproduction: each row uses an entirely fresh set of property names so the
    // schema dictionary must allocate N brand-new entries on the leader for every batch. Repeating
    // with a previous run's keys would only add 0-2 dictionary entries per batch and miss the bug.
    final int[] sizes = { 3, 5, 10, 15, 20, 30, 50 };
    int expectedTotalVertices = 0;
    int run = 0;
    for (final int n : sizes) {
      final StringBuilder line = new StringBuilder("{\"@type\":\"vertex\",\"@class\":\"")
          .append(VERTEX_TYPE).append("\"");
      for (int i = 0; i < n; i++)
        line.append(",\"r").append(run).append("_key_").append(i).append("\":\"val_").append(i).append("\"");
      line.append("}\n");
      run++;

      final HttpResult result = postBatch(followerIndex, line.toString(), "");
      assertThat(result.statusCode())
          .as("POST /batch on follower with %d new keys must succeed (issue #4122)\nbody=%s", n, result.body())
          .isEqualTo(200);

      final JSONObject json = new JSONObject(result.body());
      assertThat(json.getInt("verticesCreated")).isEqualTo(1);
      expectedTotalVertices++;
    }

    assertClusterConsistency();
    final long expected = expectedTotalVertices;
    for (int i = 0; i < getServerCount(); i++) {
      final Database nodeDb = getServerDatabase(i, getDatabaseName());
      assertThat(nodeDb.countType(VERTEX_TYPE, true))
          .as("Server %d should have %d %s records", i, expected, VERTEX_TYPE)
          .isEqualTo(expected);
    }
  }

  private void createSchema(final int serverIndex) throws Exception {
    httpCommand(serverIndex, "CREATE VERTEX TYPE " + VERTEX_TYPE + " IF NOT EXISTS");
    httpCommand(serverIndex, "CREATE PROPERTY " + VERTEX_TYPE + ".node_id IF NOT EXISTS STRING");
    httpCommand(serverIndex, "CREATE INDEX IF NOT EXISTS ON " + VERTEX_TYPE + " (node_id) UNIQUE");
    httpCommand(serverIndex, "CREATE EDGE TYPE " + EDGE_TYPE + " IF NOT EXISTS");
    waitForAllServers();
  }

  private static String buildBatchPayload(final int numVertices, final int numEdges) {
    final StringBuilder body = new StringBuilder();
    for (int i = 0; i < numVertices; i++) {
      body.append("{\"@type\":\"vertex\",\"@class\":\"").append(VERTEX_TYPE)
          .append("\",\"@id\":\"n").append(i).append("\",\"node_id\":\"n").append(i)
          .append("\",\"label\":\"Node ").append(i).append("\"}\n");
    }
    for (int i = 0; i < numEdges; i++) {
      body.append("{\"@type\":\"edge\",\"@class\":\"").append(EDGE_TYPE)
          .append("\",\"@from\":\"n").append(i).append("\",\"@to\":\"n").append((i + 1) % numVertices)
          .append("\",\"weight\":").append(i).append("}\n");
    }
    return body.toString();
  }

  private record HttpResult(int statusCode, String body) {
  }

  private HttpResult postBatch(final int serverIndex, final String body, final String queryString) throws Exception {
    String url = "http://127.0.0.1:248" + serverIndex + "/api/v1/batch/" + getDatabaseName();
    if (queryString != null && !queryString.isEmpty())
      url += "?" + queryString;

    final HttpURLConnection conn = (HttpURLConnection) new URI(url).toURL().openConnection();
    try {
      conn.setRequestMethod("POST");
      conn.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(
              ("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));
      conn.setRequestProperty("Content-Type", "application/x-ndjson");
      conn.setDoOutput(true);

      final byte[] data = body.getBytes(StandardCharsets.UTF_8);
      conn.setRequestProperty("Content-Length", Integer.toString(data.length));
      try (final DataOutputStream out = new DataOutputStream(conn.getOutputStream())) {
        out.write(data);
      }
      conn.connect();

      final int code = conn.getResponseCode();
      final String response = code >= 200 && code < 400
          ? readResponse(conn)
          : readError(conn);
      return new HttpResult(code, response);
    } finally {
      conn.disconnect();
    }
  }

  private String httpCommand(final int serverIndex, final String sql) throws Exception {
    final HttpURLConnection conn = (HttpURLConnection) new URI(
        "http://127.0.0.1:248" + serverIndex + "/api/v1/command/" + getDatabaseName())
        .toURL().openConnection();
    try {
      conn.setRequestMethod("POST");
      conn.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(
              ("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));
      conn.setDoOutput(true);
      final JSONObject payload = new JSONObject();
      payload.put("language", "sql");
      payload.put("command", sql);
      conn.setRequestProperty("Content-Type", "application/json");
      try (final DataOutputStream out = new DataOutputStream(conn.getOutputStream())) {
        out.write(payload.toString().getBytes(StandardCharsets.UTF_8));
      }
      conn.connect();

      final int code = conn.getResponseCode();
      if (code != 200) {
        final String err = readError(conn);
        throw new AssertionError("HTTP " + code + " for SQL: " + sql + " body=" + err);
      }
      return readResponse(conn);
    } finally {
      conn.disconnect();
    }
  }
}
