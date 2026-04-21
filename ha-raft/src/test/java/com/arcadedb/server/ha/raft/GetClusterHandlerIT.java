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

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

class GetClusterHandlerIT extends BaseRaftHATest {

  @Override
  protected int getServerCount() {
    return 2;
  }

  @Test
  void clusterEndpointReturnsStatus() throws Exception {
    final JSONObject response = queryClusterEndpoint(0);

    assertThat(response.getString("implementation")).isEqualTo("raft");
    assertThat(response.getString("clusterName")).isNotEmpty();
    assertThat(response.getString("localPeerId")).startsWith("localhost_");
    assertThat(response.has("isLeader")).isTrue();
    assertThat(response.has("leaderId")).isTrue();

    final JSONArray peers = response.getJSONArray("peers");
    assertThat(peers.length()).isEqualTo(2);

    for (int i = 0; i < peers.length(); i++) {
      final JSONObject peer = peers.getJSONObject(i);
      assertThat(peer.getString("id")).startsWith("localhost_");
      assertThat(peer.getString("address")).isNotEmpty();
      assertThat(peer.getString("role")).isIn("LEADER", "FOLLOWER");
    }
  }

  @Test
  void exactlyOneLeaderInCluster() throws Exception {
    int leaderCount = 0;
    for (int i = 0; i < getServerCount(); i++) {
      final JSONObject response = queryClusterEndpoint(i);
      if (response.getBoolean("isLeader"))
        leaderCount++;
    }
    assertThat(leaderCount).isEqualTo(1);
  }

  @Test
  void allNodesAgreeOnLeader() throws Exception {
    final JSONObject response0 = queryClusterEndpoint(0);
    final JSONObject response1 = queryClusterEndpoint(1);

    assertThat(response0.get("leaderId")).isEqualTo(response1.get("leaderId"));
  }

  private JSONObject queryClusterEndpoint(final int serverIndex) throws Exception {
    final int httpPort = 2480 + serverIndex;
    final URL url = new URL("http://localhost:" + httpPort + "/api/v1/cluster");
    final HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("GET");
    conn.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));

    try {
      assertThat(conn.getResponseCode()).isEqualTo(200);
      final String body = new String(conn.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
      return new JSONObject(body);
    } finally {
      conn.disconnect();
    }
  }
}
