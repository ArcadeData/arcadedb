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
package com.arcadedb.server.ha.raft;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7311: a batch that lands on a FOLLOWER is relayed to the leader over a second HTTP request, so the
 * streaming encoding the client negotiated has to travel with it. Without that a client asking a follower for
 * per-chunk acknowledgements would receive the leader's buffered object under {@code application/json} - a
 * silent downgrade of the one thing it asked for, and a body its NDJSON reader cannot parse.
 * <p>
 * The read-your-writes bookmark of issue #5862 is checked on the same request: on this encoding it cannot be a
 * response header (the response has started by the time its value is known), so the relayed answer has to carry
 * the leader's {@code commitIndex} inside the terminal line or a READ_YOUR_WRITES client loses it at the hop.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
@Tag("slow")
class RaftBatchStreamingForwardIT extends BaseRaftHATest {

  private static final String VERTEX_TYPE = "StreamNode";
  private static final String EDGE_TYPE   = "StreamEdge";
  private static final String NDJSON      = "application/x-ndjson";

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "majority");
  }

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void aStreamedBatchPostedToAFollowerIsRelayedAsAStream() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int followerIndex = (leaderIndex + 1) % getServerCount();

    httpCommand(leaderIndex, "CREATE VERTEX TYPE " + VERTEX_TYPE + " IF NOT EXISTS");
    httpCommand(leaderIndex, "CREATE PROPERTY " + VERTEX_TYPE + ".node_id IF NOT EXISTS STRING");
    waitForAllServers();
    waitForReplicationIsCompleted(followerIndex);

    final int vertices = 40;
    final StringBuilder body = new StringBuilder();
    for (int i = 0; i < vertices; i++)
      body.append("{\"@type\":\"vertex\",\"@class\":\"").append(VERTEX_TYPE).append("\",\"@id\":\"s")
          .append(i).append("\",\"node_id\":\"s").append(i).append("\"}\n");

    final List<JSONObject> events = postStreamedBatch(followerIndex, body.toString(), "vertexBatchSize=5");

    assertThat(events.stream().filter(e -> e.has("progress")).count())
        .as("the leader's chunk acknowledgements must reach the client through the follower, not be swallowed "
            + "by it and replaced with a single object")
        .isGreaterThan(1);

    final JSONObject last = events.getLast();
    assertThat(last.has("summary")).as("a relayed stream still ends with its terminal line, got " + last).isTrue();

    final JSONObject summary = last.getJSONObject("summary");
    assertThat(summary.getLong("verticesCreated")).isEqualTo(vertices);
    assertThat(summary.has("commitIndex"))
        .as("the read-your-writes bookmark travels in the terminal line on this encoding, and must survive the hop")
        .isTrue();

    assertClusterConsistency();
    for (int i = 0; i < getServerCount(); i++)
      assertThat(getServerDatabase(i, getDatabaseName()).countType(VERTEX_TYPE, true))
          .as("Server %d must hold every relayed vertex", i)
          .isEqualTo(vertices);
  }

  /**
   * The bookmark has to travel on a FAILED load too, and that is not a detail: a batch is not atomic, so a load
   * that failed mid-stream still committed the chunks before the failure, and a READ_YOUR_WRITES client has to
   * be able to read exactly those back. The buffered encoding says so by emitting {@code X-ArcadeDB-Commit-Index}
   * on its 400/408 answers (issue #5862); here it is a field of the terminal {@code error} line, which is what
   * {@code RemoteDatabase.readStreamedBatch} reads and what the OpenAPI document declares.
   */
  @Test
  void aRelayedStreamThatFailsMidLoadStillCarriesTheBookmark() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int followerIndex = (leaderIndex + 1) % getServerCount();

    httpCommand(leaderIndex, "CREATE VERTEX TYPE " + VERTEX_TYPE + " IF NOT EXISTS");
    httpCommand(leaderIndex, "CREATE PROPERTY " + VERTEX_TYPE + ".node_id IF NOT EXISTS STRING");
    httpCommand(leaderIndex, "CREATE EDGE TYPE " + EDGE_TYPE + " IF NOT EXISTS");
    waitForAllServers();
    waitForReplicationIsCompleted(followerIndex);

    final StringBuilder body = new StringBuilder();
    for (int i = 0; i < 6; i++)
      body.append("{\"@type\":\"vertex\",\"@class\":\"").append(VERTEX_TYPE).append("\",\"@id\":\"f")
          .append(i).append("\",\"node_id\":\"f").append(i).append("\"}\n");
    // An endpoint no vertex of this payload declares: the load fails with vertices already committed, and with
    // the 200 of the stream already on the wire.
    body.append("{\"@type\":\"edge\",\"@class\":\"").append(EDGE_TYPE)
        .append("\",\"@from\":\"f0\",\"@to\":\"no-such-vertex\"}\n");

    final List<JSONObject> events = postStreamedBatch(followerIndex, body.toString(), "vertexBatchSize=2");

    final JSONObject last = events.getLast();
    assertThat(last.has("error")).as("expected a terminal error line, got " + last).isTrue();

    final JSONObject error = last.getJSONObject("error");
    assertThat(error.getInt("status")).isEqualTo(400);
    assertThat(error.getBoolean("partialCommit"))
        .as("the vertices committed before the failure are durable")
        .isTrue();
    assertThat(error.has("commitIndex"))
        .as("a READ_YOUR_WRITES client must be able to read back exactly the chunks that did commit")
        .isTrue();
  }

  private List<JSONObject> postStreamedBatch(final int serverIndex, final String body, final String queryString)
      throws Exception {
    String url = "http://127.0.0.1:" + httpPort(serverIndex) + "/api/v1/batch/" + getDatabaseName();
    if (queryString != null && !queryString.isEmpty())
      url += "?" + queryString;

    final HttpURLConnection conn = (HttpURLConnection) new URI(url).toURL().openConnection();
    try {
      conn.setRequestMethod("POST");
      conn.setRequestProperty("Authorization", basicAuth());
      conn.setRequestProperty("Content-Type", NDJSON);
      conn.setRequestProperty("Accept", NDJSON);
      conn.setDoOutput(true);

      final byte[] data = body.getBytes(StandardCharsets.UTF_8);
      conn.setFixedLengthStreamingMode(data.length);
      try (final DataOutputStream out = new DataOutputStream(conn.getOutputStream())) {
        out.write(data);
      }

      assertThat(conn.getResponseCode()).isEqualTo(200);
      assertThat(conn.getContentType())
          .as("a follower must not answer a negotiated stream with the buffered encoding")
          .contains(NDJSON);

      final List<JSONObject> events = new ArrayList<>();
      try (final BufferedReader in = new BufferedReader(
          new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
        for (String line = in.readLine(); line != null; line = in.readLine())
          if (!line.isBlank())
            events.add(new JSONObject(line));
      }
      return events;
    } finally {
      conn.disconnect();
    }
  }

  private String httpCommand(final int serverIndex, final String sql) throws Exception {
    final HttpURLConnection conn = (HttpURLConnection) new URI(
        "http://127.0.0.1:" + httpPort(serverIndex) + "/api/v1/command/" + getDatabaseName())
        .toURL().openConnection();
    try {
      conn.setRequestMethod("POST");
      conn.setRequestProperty("Authorization", basicAuth());
      conn.setRequestProperty("Content-Type", "application/json");
      conn.setDoOutput(true);

      final JSONObject payload = new JSONObject();
      payload.put("language", "sql");
      payload.put("command", sql);
      try (final DataOutputStream out = new DataOutputStream(conn.getOutputStream())) {
        out.write(payload.toString().getBytes(StandardCharsets.UTF_8));
      }
      conn.connect();
      return readResponse(conn);
    } finally {
      conn.disconnect();
    }
  }

  /**
   * The port a node actually bound, never the 248n it was asked for. Anything already listening there - an
   * IDE-left server, another agent's run - would otherwise take these requests and answer them as a different
   * build, which surfaces as an authentication failure rather than as a port conflict, and only for whichever
   * node the follower index happened to land on.
   */
  private int httpPort(final int serverIndex) {
    return getServer(serverIndex).getHttpServer().getPort();
  }

  private static String basicAuth() {
    return "Basic " + Base64.getEncoder().encodeToString(
        ("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8));
  }
}
