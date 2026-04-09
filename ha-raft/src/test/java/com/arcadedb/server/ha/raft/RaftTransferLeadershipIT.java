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
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

class RaftTransferLeadershipIT extends BaseRaftHATest {

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void transferLeadershipViaHttpEndpoint() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);

    final int targetIndex = leaderIndex == 0 ? 1 : 0;

    final int httpPort = 2480 + leaderIndex;
    final HttpURLConnection conn = (HttpURLConnection) new URI(
        "http://localhost:" + httpPort + "/api/v1/cluster/leader").toURL().openConnection();
    conn.setRequestMethod("POST");
    conn.setDoOutput(true);
    conn.setRequestProperty("Content-Type", "application/json");
    conn.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));

    final String body = new JSONObject().put("peerId", peerIdForIndex(targetIndex)).put("timeoutMs", 10_000).toString();
    conn.getOutputStream().write(body.getBytes(StandardCharsets.UTF_8));

    final int responseCode = conn.getResponseCode();
    final String responseBody;
    if (responseCode >= 400 && conn.getErrorStream() != null)
      responseBody = new String(conn.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
    else if (conn.getInputStream() != null)
      responseBody = new String(conn.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    else
      responseBody = "";
    conn.disconnect();
    assertThat(responseCode).as("Response: %s", responseBody).isEqualTo(200);

    Thread.sleep(3_000);

    final Database newLeaderDb = getServerDatabase(targetIndex, getDatabaseName());
    newLeaderDb.transaction(() -> {
      if (!newLeaderDb.getSchema().existsType("TransferTest"))
        newLeaderDb.getSchema().createVertexType("TransferTest");
      newLeaderDb.newVertex("TransferTest").set("value", 1).save();
    });

    assertClusterConsistency();
  }
}
