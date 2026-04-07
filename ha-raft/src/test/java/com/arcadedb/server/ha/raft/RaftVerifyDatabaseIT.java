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

    final int httpPort = 2480 + leaderIndex;
    final HttpURLConnection conn = (HttpURLConnection) new URI(
        "http://localhost:" + httpPort + "/api/v1/cluster/verify/" + getDatabaseName()).toURL().openConnection();
    conn.setRequestMethod("POST");
    conn.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));

    assertThat(conn.getResponseCode()).isEqualTo(200);
    final String responseBody = new String(conn.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    final JSONObject response = new JSONObject(responseBody);

    assertThat(response.getString("database")).isEqualTo(getDatabaseName());
    final JSONArray nodes = response.getJSONArray("nodes");
    assertThat(nodes.length()).isGreaterThan(0);

    for (int i = 0; i < nodes.length(); i++) {
      final JSONObject node = nodes.getJSONObject(i);
      assertThat(node.getBoolean("match"))
          .as("Checksum mismatch on node %s: %s", node.getString("peerId", ""), node)
          .isTrue();
    }

    conn.disconnect();
  }
}
