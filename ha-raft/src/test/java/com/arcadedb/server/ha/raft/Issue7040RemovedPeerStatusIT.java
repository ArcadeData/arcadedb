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
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #7040 (follow-up to #5275): after a peer is removed from the live Raft configuration,
 * {@code /api/v1/cluster} used to keep reporting it as a healthy {@code FOLLOWER} with an address, because the peer
 * list was built from the static group only. It must now flag the peer as out of the configuration and raise the
 * membership alert, while the members still in the configuration read as before.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7040RemovedPeerStatusIT extends BaseRaftHATest {

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void removedPeerIsReportedOutOfConfigurationWithAnAlert() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);
    final int removedIndex = (leaderIndex + 1) % getServerCount();
    final String removedPeerId = peerIdForIndex(removedIndex);

    final RaftHAServer leaderRaft = getRaftPlugin(leaderIndex).getRaftHAServer();
    assertThat(leaderRaft.getLivePeers()).hasSize(3);

    // Before the removal every declared peer is a member: no flag, no alert.
    final JSONObject before = queryClusterEndpoint(leaderIndex);
    assertThat(before.getJSONArray("peers").length()).isEqualTo(3);
    for (int i = 0; i < 3; i++) {
      final JSONObject peer = before.getJSONArray("peers").getJSONObject(i);
      assertThat(peer.getBoolean("inConfiguration")).isTrue();
      assertThat(peer.getString("role")).isIn("LEADER", "FOLLOWER");
    }
    assertThat(alertIds(before)).doesNotContain("peers-not-in-configuration");

    // The reported scenario: the peer is dropped from the committed configuration while it is down.
    getServer(removedIndex).stop();
    leaderRaft.removePeer(removedPeerId, true);
    Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(500, TimeUnit.MILLISECONDS)
        .untilAsserted(() -> assertThat(leaderRaft.getLivePeers()).hasSize(2));

    final JSONObject after = queryClusterEndpoint(leaderIndex);
    final JSONArray peers = after.getJSONArray("peers");
    assertThat(peers.length()).as("the removed peer stays listed: it is still declared").isEqualTo(3);

    boolean removedSeen = false;
    for (int i = 0; i < peers.length(); i++) {
      final JSONObject peer = peers.getJSONObject(i);
      if (removedPeerId.equals(peer.getString("id"))) {
        removedSeen = true;
        assertThat(peer.getBoolean("inConfiguration")).isFalse();
        assertThat(peer.getString("role")).isEqualTo(GetClusterHandler.ROLE_NOT_IN_CONFIGURATION);
      } else {
        assertThat(peer.getBoolean("inConfiguration")).isTrue();
        assertThat(peer.getString("role")).isIn("LEADER", "FOLLOWER");
      }
    }
    assertThat(removedSeen).isTrue();

    final JSONArray alerts = after.getJSONArray("alerts");
    JSONObject membershipAlert = null;
    for (int i = 0; i < alerts.length(); i++)
      if ("peers-not-in-configuration".equals(alerts.getJSONObject(i).getString("id")))
        membershipAlert = alerts.getJSONObject(i);
    assertThat(membershipAlert).as("the divergence is told to the operator, not left to diff by eye").isNotNull();
    assertThat(membershipAlert.getString("severity")).isEqualTo(ClusterAlerts.SEVERITY_WARNING);
    assertThat(membershipAlert.getJSONObject("details").getJSONArray("peers").toList()).containsExactly(removedPeerId);

    // The sibling views agree (module CLAUDE.md, "Peer-list filtering"): the removed peer is no replica in the server
    // stats, nor in the replica address list a client or a peer-to-peer transfer would dial.
    final List<?> replicas = (List<?>) leaderRaft.getStats().get("replicas");
    for (final Object replica : replicas)
      assertThat(((Map<?, ?>) replica).get("id")).isNotEqualTo(removedPeerId);
    assertThat(replicas).hasSize(1);
    assertThat(leaderRaft.getReplicaAddresses()).doesNotContain("localhost:" + (2480 + removedIndex));

    // A follower that is still a member sees the same picture: the live configuration is committed cluster-wide.
    final int otherFollower = (leaderIndex + 2) % getServerCount();
    final JSONObject fromFollower = queryClusterEndpoint(otherFollower);
    assertThat(alertIds(fromFollower)).contains("peers-not-in-configuration");
  }

  private static List<String> alertIds(final JSONObject response) {
    final List<String> ids = new ArrayList<>();
    final JSONArray alerts = response.getJSONArray("alerts");
    for (int i = 0; i < alerts.length(); i++)
      ids.add(alerts.getJSONObject(i).getString("id"));
    return ids;
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
      return new JSONObject(new String(conn.getInputStream().readAllBytes(), StandardCharsets.UTF_8));
    } finally {
      conn.disconnect();
    }
  }
}
