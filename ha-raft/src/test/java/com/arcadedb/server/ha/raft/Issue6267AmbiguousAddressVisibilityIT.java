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

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code GET /api/v1/cluster} on a cluster whose peer addresses identify nobody (issue #6267).
 * <p>
 * With no {@code http} port declared in {@code arcadedb.ha.serverList} every peer's HTTP endpoint is derived as
 * its Raft host plus <em>this</em> node's port, so on a cluster whose nodes differ by port they all collapse onto
 * one address. Two things used to hide that from the operator, and both pointed the reassuring way:
 * <ul>
 *   <li>the status endpoint reported a plausible address per peer with nothing to say that it named none of
 *   them, so the misconfiguration surfaced only when a resync or a verify eventually refused to dial;</li>
 *   <li>the presence matrix <em>dialled</em> that address and attributed the answer to the peer, so every peer
 *   reported the local node's own database list and the matrix showed every database present everywhere - the
 *   same false all-clear the verify endpoint gave before issue #6221.</li>
 * </ul>
 * The ambiguity is injected by emptying the leader's resolved HTTP address map for the duration of one test, as
 * {@link Issue6221VerifyFanOutGuardIT} does, rather than by starting the cluster misconfigured: it reproduces the
 * production condition at the point where it matters without making cluster startup part of what is under test.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6267AmbiguousAddressVisibilityIT extends BaseRaftHATest {

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  @Timeout(120)
  void anAmbiguousPeerAddressIsFlaggedInTheClusterStatus() throws Exception {
    final int leader = findLeaderIndex();
    assertThat(leader).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final RaftHAServer raft = getRaftPlugin(leader).getRaftHAServer();
    // The live map, by contract (see getHttpAddresses): writing to it is how this test puts the running node
    // into the misconfigured state.
    final Map<RaftPeerId, String> httpAddresses = raft.getHttpAddresses();
    final Map<RaftPeerId, String> declared = new HashMap<>(httpAddresses);
    try {
      httpAddresses.clear(); // nothing declared: every peer now derives to this node's own endpoint

      final JSONArray peers = getCluster(leader, "").getJSONArray("peers");
      assertThat(peers.length()).isEqualTo(getServerCount());
      for (int i = 0; i < peers.length(); i++) {
        final JSONObject peer = peers.getJSONObject(i);
        assertThat(peer.getBoolean("httpAddressAmbiguous", false))
            .as("every peer resolves to this node's endpoint, so none of them is identified by it: %s", peer)
            .isTrue();
        assertThat(peer.getString("httpAddress", ""))
            .as("the resolved address is still reported - it is what the flag is about")
            .isNotEmpty();
      }
    } finally {
      httpAddresses.putAll(declared);
    }
  }

  /**
   * Control, on the same cluster: with the addresses it actually has, every peer owns its endpoint and no flag
   * is emitted. Without it the assertion above would also pass against a build that flagged every cluster.
   */
  @Test
  @Timeout(120)
  void aCorrectlyAddressedClusterCarriesNoAmbiguityFlag() throws Exception {
    final int leader = findLeaderIndex();
    assertThat(leader).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final JSONArray peers = getCluster(leader, "").getJSONArray("peers");
    assertThat(peers.length()).isEqualTo(getServerCount());
    for (int i = 0; i < peers.length(); i++) {
      final JSONObject peer = peers.getJSONObject(i);
      assertThat(peer.has("httpAddressAmbiguous"))
          .as("a declared, distinct endpoint per peer carries no flag: %s", peer)
          .isFalse();
    }
  }

  /**
   * The presence matrix asks each peer which databases it holds and attributes the answer to that peer, which
   * makes it exactly the kind of unattended dial {@code PeerDialAddress} exists to guard. Unguarded, every peer
   * was queried on the leader's own address and reported the leader's databases as its own.
   */
  @Test
  @Timeout(120)
  void thePresenceMatrixReportsAPeerItCannotIdentifyAsUnreachable() throws Exception {
    final int leader = findLeaderIndex();
    assertThat(leader).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final RaftHAServer raft = getRaftPlugin(leader).getRaftHAServer();
    final Map<RaftPeerId, String> httpAddresses = raft.getHttpAddresses();
    final Map<RaftPeerId, String> declared = new HashMap<>(httpAddresses);
    try {
      httpAddresses.clear();

      final JSONObject presence = getCluster(leader, "?presence=true").getJSONObject("databasePresence");
      final JSONArray unreachable = presence.getJSONArray("unreachable");

      assertThat(unreachable.length())
          .as("the two peers the leader cannot identify were not queried: %s", presence)
          .isEqualTo(getServerCount() - 1);
      for (int i = 0; i < unreachable.length(); i++)
        assertThat(unreachable.getString(i)).isNotEqualTo(raft.getLocalPeerId().toString());

      // And nothing was attributed to them: a peer that was never asked holds no databases in the matrix.
      final JSONArray databases = presence.getJSONArray("databases");
      for (int i = 0; i < databases.length(); i++) {
        final JSONObject database = databases.getJSONObject(i);
        final JSONArray present = database.getJSONArray("present");
        assertThat(present.length())
            .as("only the local node answered for '%s': %s", database.getString("name"), present)
            .isEqualTo(1);
        assertThat(present.getString(0)).isEqualTo(raft.getLocalPeerId().toString());
      }
    } finally {
      httpAddresses.putAll(declared);
    }
  }

  /** GETs the cluster endpoint of one server as the root operator, returning the parsed 200 body. */
  private JSONObject getCluster(final int serverIndex, final String query) throws Exception {
    final HttpURLConnection conn = (HttpURLConnection) new URI(
        "http://localhost:" + getServer(serverIndex).getHttpServer().getPort() + "/api/v1/cluster" + query)
        .toURL().openConnection();
    try {
      conn.setRequestMethod("GET");
      conn.setRequestProperty("Authorization", "Basic " + Base64.getEncoder()
          .encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));

      assertThat(conn.getResponseCode()).isEqualTo(200);
      return new JSONObject(new String(conn.getInputStream().readAllBytes(), StandardCharsets.UTF_8));
    } finally {
      conn.disconnect();
    }
  }
}
