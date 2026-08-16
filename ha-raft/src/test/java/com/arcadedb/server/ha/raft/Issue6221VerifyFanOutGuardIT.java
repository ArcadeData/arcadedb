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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.LeaderForwardContext;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code POST /api/v1/cluster/verify/{database}} exists to tell an operator whether the cluster's copies of a
 * database agree, and it fanned out to every peer on an address nothing had checked (issue #6221).
 * <p>
 * When {@code arcadedb.ha.serverList} declares no {@code http} port, a peer's HTTP endpoint is <em>derived</em>
 * as its Raft host plus <em>this</em> node's HTTP port, so on a cluster whose nodes differ by port rather than by
 * host every peer - this node included - resolves to the node doing the resolving. Two things then went wrong at
 * once, and both point the reassuring way:
 * <ul>
 *   <li>the leader compared itself against itself, matched on every file, and reported the <b>peer</b> as
 *   {@code CONSISTENT}, rolling up to {@code ALL_CONSISTENT} - a clean bill of health from the divergence
 *   detector, produced without contacting the node it names;</li>
 *   <li>the query landed back on the leader, where {@code isLeader()} is still true, so it fanned out again:
 *   (N-1) times per level, each level CRC-ing every byte of the database before recursing, each hop holding an
 *   Undertow worker thread. Nothing bounded the depth.</li>
 * </ul>
 * Both ways out are asserted here, because neither covers the other: the <b>dial</b> side refuses an address that
 * does not identify one peer or that is this node's own, and the <b>receiving</b> side answers a query a peer
 * already fanned out to it with its own checksums instead of fanning out again - the general bound, for an
 * address that names the wrong <em>peer</em> rather than this node, which no local self-check can see.
 * <p>
 * The ambiguity is injected by emptying the leader's resolved HTTP address map for the duration of one test,
 * exactly as {@link Issue6191FollowerForwardLoopIT} does, rather than by configuring the cluster without HTTP
 * ports: it reproduces the production condition at the point where it matters without making cluster startup
 * part of what is under test.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6221VerifyFanOutGuardIT extends BaseRaftHATest {

  @Override
  protected int getServerCount() {
    return 3;
  }

  /**
   * The reachable deployment: peers on one host, no {@code http} port declared. Every peer must come back as
   * unverified, and the run as a whole must not claim agreement. Before the fix this reported every peer
   * {@code CONSISTENT} against the leader's own checksums - when it returned at all.
   */
  @Test
  @Timeout(120)
  void aVerifyThatCannotIdentifyItsPeersReportsThemUnverifiedRatherThanConsistent() throws Exception {
    final int leader = findLeaderIndex();
    assertThat(leader).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final RaftHAServer raft = getRaftPlugin(leader).getRaftHAServer();
    // The live map, by contract (see getHttpAddresses): writing to it is how this test puts the running node into
    // the misconfigured state.
    final Map<RaftPeerId, String> httpAddresses = raft.getHttpAddresses();
    final Map<RaftPeerId, String> declared = new HashMap<>(httpAddresses);
    try {
      httpAddresses.clear(); // nothing declared: every peer now derives to this node's own endpoint

      final JSONObject result = verifyOnLeader(leader).getJSONObject("result");

      final JSONArray peers = result.getJSONArray("peers");
      assertThat(peers.length()).as("a 3-node cluster has two peers to verify against").isEqualTo(2);
      for (int i = 0; i < peers.length(); i++) {
        final JSONObject peer = peers.getJSONObject(i);
        assertThat(peer.getString("status", ""))
            .as("a peer that was never contacted cannot be reported as agreeing: %s", peer)
            .isEqualTo("ERROR");
        assertThat(peer.getString("error", ""))
            .as("and the report must say what an operator has to change")
            .contains("not verified")
            .contains(GlobalConfiguration.HA_SERVER_LIST.getKey());
        assertThat(peer.has("matchingFiles"))
            .as("nothing was compared, so there is no match count to show")
            .isFalse();
      }

      assertThat(result.getString("overallStatus", ""))
          .as("an unverified peer is not a consistent one, and it is not an observed divergence either")
          .isEqualTo("VERIFICATION_INCOMPLETE");
    } finally {
      httpAddresses.putAll(declared);
    }
  }

  /**
   * The bound a local self-check cannot provide: a resolved address can name a real node that is not the one
   * meant, and if that node is the leader it fans the query out again. A query that arrives carrying a peer's
   * one-hop marker is answered with local checksums, on the leader as much as anywhere else.
   * <p>
   * Driven over real HTTP because the marker is a request header, and sent the way a peer sends it - with the
   * cluster token, the only form in which the marker is trusted.
   */
  @Test
  @Timeout(120)
  void aQueryAPeerAlreadyFannedOutIsAnsweredLocallyInsteadOfFannedOutAgain() throws Exception {
    final int leader = findLeaderIndex();
    assertThat(leader).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final String clusterToken = getRaftPlugin(leader).getRaftHAServer().getClusterToken();
    assertThat(clusterToken).as("peers authenticate to each other with a cluster token").isNotBlank();

    final JSONObject fannedOut = post(leader, conn -> {
      conn.setRequestProperty("X-ArcadeDB-Cluster-Token", clusterToken);
      conn.setRequestProperty("X-ArcadeDB-Forwarded-User", "root");
      conn.setRequestProperty(LeaderForwardContext.FORWARDED_TO_LEADER_HEADER, "true");
    });

    assertThat(fannedOut.has("result"))
        .as("the leader must not fan out a query that was itself fanned out to it")
        .isFalse();
    assertThat(fannedOut.has("localChecksums"))
        .as("it answers with its own checksums, which is what the node that asked needs")
        .isTrue();

    // Control: the same request from a peer WITHOUT the marker is still fanned out, so the assertion above is
    // about the marker rather than about the cluster-token authentication that carries it.
    final JSONObject unmarked = post(leader, conn -> {
      conn.setRequestProperty("X-ArcadeDB-Cluster-Token", clusterToken);
      conn.setRequestProperty("X-ArcadeDB-Forwarded-User", "root");
    });
    assertThat(unmarked.has("result")).isTrue();
  }

  /**
   * The same rule applied to the case that has nothing to do with addressing: a peer that the guard passes and
   * the network then refuses. It is not a peer that agrees, and it is not an observed divergence either - it used
   * to be rolled up as {@code INCONSISTENCY_DETECTED}, which sends an operator hunting for a divergence nobody
   * has seen when the truth is that a node is down.
   * <p>
   * The unreachable address is a real, unambiguous, not-ours endpoint that nothing is listening on, so the dial
   * guard hands it over and the connection is refused: this pins the classification rather than the guard, which
   * is what makes it worth having next to the tests above.
   */
  @Test
  @Timeout(120)
  void aPeerTheNetworkRefusesIsUnverifiedRatherThanDivergent() throws Exception {
    final int leader = findLeaderIndex();
    assertThat(leader).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int unreachable = firstFollower(leader);

    final RaftHAServer raft = getRaftPlugin(leader).getRaftHAServer();
    final Map<RaftPeerId, String> httpAddresses = raft.getHttpAddresses();
    final Map<RaftPeerId, String> declared = new HashMap<>(httpAddresses);
    try {
      // A port nothing listens on: the address identifies one peer and is not this node's, so it is dialled.
      httpAddresses.put(RaftPeerId.valueOf(peerIdForIndex(unreachable)), "127.0.0.1:" + closedPort());

      final JSONObject result = verifyOnLeader(leader).getJSONObject("result");

      final JSONArray peers = result.getJSONArray("peers");
      int errors = 0;
      int consistent = 0;
      for (int i = 0; i < peers.length(); i++) {
        final String status = peers.getJSONObject(i).getString("status", "");
        if ("ERROR".equals(status))
          ++errors;
        else if ("CONSISTENT".equals(status))
          ++consistent;
      }
      assertThat(errors).as("the peer nothing answers for could not be verified: %s", peers).isEqualTo(1);
      assertThat(consistent).as("the reachable peer was still compared and still agrees").isEqualTo(1);

      assertThat(result.getString("overallStatus", ""))
          .as("a node being down is not a divergence, and it is not agreement either")
          .isEqualTo("VERIFICATION_INCOMPLETE");
    } finally {
      httpAddresses.putAll(declared);
    }
  }

  /**
   * Control: with the addresses the cluster actually has, the verify contacts every peer and reports agreement.
   * Without it the two tests above would also pass against a build that refused every peer.
   */
  @Test
  @Timeout(120)
  void aVerifyWithTheClustersRealAddressesStillReportsAgreement() throws Exception {
    final int leader = findLeaderIndex();
    assertThat(leader).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);

    getServerDatabase(leader, getDatabaseName()).transaction(() -> {
      final var db = getServerDatabase(leader, getDatabaseName());
      if (!db.getSchema().existsType("Issue6221Verify"))
        db.getSchema().createVertexType("Issue6221Verify");
      for (int i = 0; i < 10; i++)
        db.newVertex("Issue6221Verify").set("index", i).save();
    });
    assertClusterConsistency();

    final JSONObject result = verifyOnLeader(leader).getJSONObject("result");

    final JSONArray peers = result.getJSONArray("peers");
    assertThat(peers.length()).isEqualTo(2);
    for (int i = 0; i < peers.length(); i++)
      assertThat(peers.getJSONObject(i).getString("status", "ERROR"))
          .as("peer %s", peers.getJSONObject(i))
          .isEqualTo("CONSISTENT");
    assertThat(result.getString("overallStatus", "")).isEqualTo("ALL_CONSISTENT");
  }

  /** Runs the verify on {@code serverIndex} as the root operator, the way Studio and an operator's curl do. */
  private JSONObject verifyOnLeader(final int serverIndex) throws Exception {
    return post(serverIndex, conn -> conn.setRequestProperty("Authorization", "Basic " + Base64.getEncoder()
        .encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8))));
  }

  /** POSTs the verify endpoint of one server with the caller's headers, returning the parsed 200 body. */
  private JSONObject post(final int serverIndex, final RequestCustomizer customizer) throws Exception {
    final HttpURLConnection conn = (HttpURLConnection) new URI(
        "http://localhost:" + getServer(serverIndex).getHttpServer().getPort() + "/api/v1/cluster/verify/"
            + getDatabaseName()).toURL().openConnection();
    try {
      conn.setRequestMethod("POST");
      conn.setRequestProperty("Content-Type", "application/json");
      customizer.customize(conn);
      conn.setDoOutput(true);
      try (final var out = conn.getOutputStream()) {
        out.write("{}".getBytes(StandardCharsets.UTF_8));
      }

      assertThat(conn.getResponseCode()).isEqualTo(200);
      return new JSONObject(new String(conn.getInputStream().readAllBytes(), StandardCharsets.UTF_8));
    } finally {
      conn.disconnect();
    }
  }

  /**
   * A local port nothing is listening on: bound to let the OS pick a free one, then released. A port that was
   * free a moment ago is the closest a test can get to "connection refused" without hard-coding a number some
   * other process on the CI runner may own.
   */
  private static int closedPort() throws Exception {
    try (final ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }

  private int firstFollower(final int leader) {
    for (int i = 0; i < getServerCount(); i++)
      if (i != leader)
        return i;
    throw new IllegalStateException("no follower in a " + getServerCount() + "-node cluster");
  }

  @FunctionalInterface
  private interface RequestCustomizer {
    void customize(HttpURLConnection conn);
  }
}
