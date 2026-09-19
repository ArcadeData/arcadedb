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
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7549: {@code GET /api/v1/cluster} reports each peer's advertised capabilities from EVERY node, so the
 * question an operator actually asks it - "is the rolling upgrade finished, i.e. may I make a group change" -
 * can be answered without first locating the leader.
 * <p>
 * The capability monitor ran only while a node was the leader ({@code startLagMonitor} started it,
 * {@code stopLagMonitor} ended it), because issue #7219's only consumer was the leader-side schema-delta
 * decision. Neither this endpoint nor the security routes that #7511 gated on the same advertisement forward to
 * the leader, so a client or load balancer polling a follower got peer rows with no {@code capabilities} field
 * and no {@code capabilitiesUnknownReason} either.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7549FollowerCapabilityReportingIT extends BaseRaftHATest {

  @Override
  protected int getServerCount() {
    return 2;
  }

  /**
   * Polled on the FOLLOWER, which is the node the endpoint used to have nothing to say about anyone else on.
   * <p>
   * Every row is asserted, the follower's own included: its own is rendered from what this node advertises
   * about itself (it never probes itself), and the leader's row is the one that needs a probe round to exist -
   * which is exactly the round a follower did not run.
   */
  @Test
  void aFollowerReportsTheCapabilitiesOfEveryPeer() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a leader must be elected").isGreaterThanOrEqualTo(0);
    final int followerIndex = leaderIndex == 0 ? 1 : 0;

    // The first refresh round runs with no initial delay, but it still has to complete a probe per peer.
    Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(500, TimeUnit.MILLISECONDS).untilAsserted(() -> {
      final JSONObject response = queryClusterEndpoint(followerIndex);
      assertThat(response.getBoolean("isLeader")).as("this is the follower").isFalse();

      final JSONArray peers = response.getJSONArray("peers");
      assertThat(peers.length()).isEqualTo(2);
      for (int i = 0; i < peers.length(); i++) {
        final JSONObject peer = peers.getJSONObject(i);
        assertThat(peer.has("capabilities"))
            .as("a follower knows what peer '%s' advertises, without anyone having to find the leader first",
                peer.getString("id"))
            .isTrue();
      }
    });
  }

  private JSONObject queryClusterEndpoint(final int serverIndex) throws Exception {
    final URL url = new URL("http://localhost:" + (2480 + serverIndex) + "/api/v1/cluster");
    final HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("GET");
    conn.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));
    try {
      assertThat(conn.getResponseCode()).isEqualTo(200);
      return new JSONObject(new String(conn.getInputStream().readAllBytes(), StandardCharsets.UTF_8));
    } finally {
      conn.disconnect();
    }
  }
}
