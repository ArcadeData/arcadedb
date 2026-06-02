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

import com.arcadedb.database.Database;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for the operator-triggered emergency resync endpoint
 * (POST /api/v1/cluster/resync/{database}) and {@link ArcadeStateMachine#resyncDatabaseFromLeader}.
 */
class RaftForceResyncIT extends BaseRaftHATest {

  @Override
  protected int getServerCount() {
    // 3 nodes so a majority (2) is still available while one follower is being resynced.
    return 3;
  }

  @Test
  void followerResyncFromLeaderKeepsDataAndResumesReplication() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int followerIndex = (leaderIndex + 1) % getServerCount();

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    leaderDb.transaction(() -> {
      if (!leaderDb.getSchema().existsType("ResyncTest"))
        leaderDb.getSchema().createVertexType("ResyncTest");
      for (int i = 0; i < 25; i++)
        leaderDb.newVertex("ResyncTest").set("index", i).save();
    });

    assertClusterConsistency();
    assertThat(getServerDatabase(followerIndex, getDatabaseName()).countType("ResyncTest", true))
        .as("Follower has the initial data before resync").isEqualTo(25);

    // Trigger the emergency resync on the follower: it drops its local copy and re-downloads a full
    // snapshot from the leader.
    final JSONObject response = resync(followerIndex, getDatabaseName());
    assertThat(response.getString("result", "")).contains("resynced from leader");

    assertThat(getServerDatabase(followerIndex, getDatabaseName()).countType("ResyncTest", true))
        .as("Follower has the same data after resync").isEqualTo(25);

    // Forward replication must resume after the resync: new writes on the leader reach the follower.
    leaderDb.transaction(() -> {
      for (int i = 25; i < 40; i++)
        leaderDb.newVertex("ResyncTest").set("index", i).save();
    });

    assertClusterConsistency();
    assertThat(getServerDatabase(followerIndex, getDatabaseName()).countType("ResyncTest", true))
        .as("Follower receives writes committed after the resync").isEqualTo(40);
  }

  @Test
  void resyncOnLeaderIsRejected() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);

    final int httpPort = getServer(leaderIndex).getHttpServer().getPort();
    final HttpURLConnection conn = (HttpURLConnection) new URI(
        "http://localhost:" + httpPort + "/api/v1/cluster/resync/" + getDatabaseName()).toURL().openConnection();
    conn.setRequestMethod("POST");
    conn.setRequestProperty("Authorization", basicAuth());

    assertThat(conn.getResponseCode()).isEqualTo(400);
    final String body = new String(conn.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
    assertThat(new JSONObject(body).getString("error", "")).contains("leader");
    conn.disconnect();
  }

  private JSONObject resync(final int serverIndex, final String databaseName) throws Exception {
    final int httpPort = getServer(serverIndex).getHttpServer().getPort();
    final HttpURLConnection conn = (HttpURLConnection) new URI(
        "http://localhost:" + httpPort + "/api/v1/cluster/resync/" + databaseName).toURL().openConnection();
    conn.setRequestMethod("POST");
    conn.setRequestProperty("Authorization", basicAuth());
    try {
      assertThat(conn.getResponseCode()).isEqualTo(200);
      return new JSONObject(new String(conn.getInputStream().readAllBytes(), StandardCharsets.UTF_8));
    } finally {
      conn.disconnect();
    }
  }

  private static String basicAuth() {
    return "Basic " + Base64.getEncoder().encodeToString(
        ("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8));
  }
}
