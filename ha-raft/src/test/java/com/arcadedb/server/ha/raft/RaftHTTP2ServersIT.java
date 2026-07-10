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

import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import static com.arcadedb.schema.Property.RID_PROPERTY;
import static org.assertj.core.api.Assertions.assertThat;

class RaftHTTP2ServersIT extends BaseRaftHATest {

  @Test
  void serverInfo() throws Exception {
    testEachServer(serverIndex -> {
      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/server?mode=cluster").openConnection();
      connection.setRequestMethod("GET");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      try {
        connection.connect();
        final String response = readResponse(connection);
        LogManager.instance().log(this, Level.FINE, "Response: %s", null, response);
        assertThat(connection.getResponseCode()).isEqualTo(200);
        assertThat(connection.getResponseMessage()).isEqualTo("OK");

        final JSONObject parsed = new JSONObject(response);
        assertThat(parsed.has("ha")).as("?mode=cluster must include 'ha' section when HA is running").isTrue();

        final JSONObject ha = parsed.getJSONObject("ha");
        assertThat(ha.has("clusterName")).isTrue();
        assertThat(ha.has("leader")).isTrue();
        assertThat(ha.has("network")).isTrue();
        assertThat(ha.getJSONObject("network").has("replicas")).isTrue();
        // With Raft, every node knows all peers from the group config, so the replicas array
        // is populated even on followers without the old leader-forwarding round trip.
        assertThat(ha.getJSONObject("network").getJSONArray("replicas").length()).isPositive();
      } finally {
        connection.disconnect();
      }
    });
  }

  @Test
  void propagationOfSchema() throws Exception {
    testEachServer(serverIndex -> {
      final String response = command(serverIndex, "create vertex type RaftVertexType" + serverIndex);
      assertThat(response).withFailMessage("Type RaftVertexType" + serverIndex + " not found on server " + serverIndex)
          .contains("RaftVertexType" + serverIndex);
    });

    Awaitility.await()
        .atMost(10, TimeUnit.SECONDS)
        .pollInterval(100, TimeUnit.MILLISECONDS)
        .until(() -> {
          for (int i = 0; i < getServerCount(); i++) {
            try {
              command(i, "select from RaftVertexType" + i);
            } catch (final Exception e) {
              return false;
            }
          }
          return true;
        });
  }

  @Test
  void checkQuery() throws Exception {
    testEachServer(serverIndex -> {
      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/query/graph/sql/select%20from%20V1%20limit%201").openConnection();
      connection.setRequestMethod("GET");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      connection.connect();
      try {
        final String response = readResponse(connection);
        assertThat(connection.getResponseCode()).isEqualTo(200);
        assertThat(response).contains("V1");
      } finally {
        connection.disconnect();
      }
    });
  }

  @Test
  void checkDeleteGraphElements() throws Exception {
    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    testEachServer(serverIndex -> {
      final String v1 = new JSONObject(
          command(serverIndex, "create vertex V1 content {\"name\":\"Jay\",\"surname\":\"Miner\",\"age\":69}"))
          .getJSONArray("result").getJSONObject(0).getString(RID_PROPERTY);

      waitForAllServers();

      testEachServer(checkServer ->
          assertThat(new JSONObject(command(checkServer, "select from " + v1)).getJSONArray("result")).isNotEmpty());

      final String v2 = new JSONObject(
          command(serverIndex, "create vertex V1 content {\"name\":\"John\",\"surname\":\"Red\",\"age\":50}"))
          .getJSONArray("result").getJSONObject(0).getString(RID_PROPERTY);

      waitForAllServers();

      testEachServer(checkServer ->
          assertThat(new JSONObject(command(checkServer, "select from " + v2)).getJSONArray("result")).isNotEmpty());

      final String e1 = new JSONObject(command(serverIndex, "create edge E1 from " + v1 + " to " + v2))
          .getJSONArray("result").getJSONObject(0).getString(RID_PROPERTY);

      waitForAllServers();

      testEachServer(checkServer ->
          assertThat(new JSONObject(command(checkServer, "select from " + e1)).getJSONArray("result")).isNotEmpty());

      command(serverIndex, "delete from " + v1);
      waitForAllServers();

      testEachServer(checkServer -> {
        try {
          final JSONObject jsonResponse = new JSONObject(command(checkServer, "select from " + v1));
          assertThat(jsonResponse.getJSONArray("result").length()).isEqualTo(0);
        } catch (final IOException e) {
          // HTTP error means record not found - acceptable
        }
        try {
          final JSONObject jsonResponse = new JSONObject(command(checkServer, "select from " + e1));
          assertThat(jsonResponse.getJSONArray("result").length()).isEqualTo(0);
        } catch (final IOException e) {
          // HTTP error means edge not found - acceptable
        }
      });
    });
  }

  @Test
  void clusterTopologyIsConsistentAcrossNodes() throws Exception {
    // Every node must report the same leaderAddress and the same set of replicaAddresses.
    // Regression: follower nodes were excluding themselves (localPeerId) from replicaAddresses
    // instead of excluding the leader, causing the leader to appear inside replicaAddresses
    // and the follower itself to be missing from the list.
    String firstLeaderAddress = null;
    String firstReplicaAddresses = null;

    for (int i = 0; i < getServerCount(); i++) {
      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:" + getServer(i).getHttpServer().getPort() + "/api/v1/server?mode=cluster").openConnection();
      connection.setRequestMethod("GET");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      try {
        connection.connect();
        assertThat(connection.getResponseCode()).isEqualTo(200);
        final JSONObject parsed = new JSONObject(readResponse(connection));
        final JSONObject ha = parsed.getJSONObject("ha");

        final String leaderAddress = ha.getString("leaderAddress");
        final String replicaAddresses = ha.getString("replicaAddresses");

        // The leader must not appear in the replica list
        assertThat(replicaAddresses).as("server %d: leaderAddress must not appear in replicaAddresses", i)
            .doesNotContain(leaderAddress);

        // All responses must agree on the same leader
        if (firstLeaderAddress == null) {
          firstLeaderAddress = leaderAddress;
          firstReplicaAddresses = replicaAddresses;
        } else {
          assertThat(leaderAddress).as("server %d: leaderAddress must be consistent across all nodes", i)
              .isEqualTo(firstLeaderAddress);
          assertThat(replicaAddresses).as("server %d: replicaAddresses must be consistent across all nodes", i)
              .isEqualTo(firstReplicaAddresses);
        }
      } finally {
        connection.disconnect();
      }
    }
  }

  @Test
  void hAConfiguration() throws Exception {
    // Verify the cluster endpoint reports exactly one leader across both nodes
    int leaderCount = 0;
    for (int i = 0; i < getServerCount(); i++) {
      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + i + "/api/v1/cluster").openConnection();
      connection.setRequestMethod("GET");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      try {
        connection.connect();
        final String response = readResponse(connection);
        assertThat(connection.getResponseCode()).isEqualTo(200);
        assertThat(response).contains("\"implementation\":\"raft\"");
        final JSONObject json = new JSONObject(response);
        if (json.getBoolean("isLeader"))
          leaderCount++;
      } finally {
        connection.disconnect();
      }
    }
    assertThat(leaderCount).isEqualTo(1);
  }
}
