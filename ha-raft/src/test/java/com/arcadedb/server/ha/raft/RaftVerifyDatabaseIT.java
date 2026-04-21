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
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

class RaftVerifyDatabaseIT extends BaseRaftHATest {

  @Override
  protected int getServerCount() {
    return 2;
  }

  @Test
  void verifyDatabaseReportsMatchingChecksums() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    leaderDb.transaction(() -> {
      if (!leaderDb.getSchema().existsType("VerifyTest"))
        leaderDb.getSchema().createVertexType("VerifyTest");
      for (int i = 0; i < 10; i++)
        leaderDb.newVertex("VerifyTest").set("index", i).save();
    });

    assertClusterConsistency();

    final int httpPort = getServer(leaderIndex).getHttpServer().getPort();
    final HttpURLConnection conn = (HttpURLConnection) new URI(
        "http://localhost:" + httpPort + "/api/v1/cluster/verify/" + getDatabaseName()).toURL().openConnection();
    conn.setRequestMethod("POST");
    conn.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));

    assertThat(conn.getResponseCode()).isEqualTo(200);
    final String responseBody = new String(conn.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    final JSONObject response = new JSONObject(responseBody);

    // Leader response wraps everything under "result"
    final JSONObject result = response.getJSONObject("result");
    assertThat(result.getString("database")).isEqualTo(getDatabaseName());
    final JSONArray peers = result.getJSONArray("peers");
    assertThat(peers.length()).isGreaterThan(0);

    for (int i = 0; i < peers.length(); i++) {
      final JSONObject peer = peers.getJSONObject(i);
      assertThat(peer.getString("status", "ERROR"))
          .as("Checksum mismatch on peer %s: %s", peer.getString("peerId", ""), peer)
          .isEqualTo("CONSISTENT");
    }

    conn.disconnect();
  }
}
