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
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.LeaderForwardContext;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.DataOutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A write issued on a follower is forwarded to the leader over HTTP, and the address it is forwarded to is
 * only as good as the configuration behind it. When {@code arcadedb.ha.serverList} declares no {@code http}
 * port, the address is <em>derived</em> from the peer's Raft host plus <em>this</em> node's HTTP port - so on
 * a cluster whose nodes differ by port rather than by host, every peer, the leader included, resolves to the
 * node doing the resolving. The POST comes back to a follower, which is still not the leader, and is
 * forwarded again: a cycle bounded only by HTTP timeouts and the size of the worker pool, one held request
 * thread per hop (issue #6191).
 * <p>
 * Both ways out are asserted here, because neither covers the other:
 * <ul>
 *   <li>the <b>dial</b> side refuses before the request leaves, when the address resolved for the leader is
 *   this node's own - the same question {@code triggerSnapshotDownload()} has asked since issue #6111;</li>
 *   <li>the <b>receiving</b> side refuses a write that a peer already forwarded, whichever node it lands on.
 *   That is the general bound: an address can name the wrong <em>peer</em> rather than this node, which no
 *   local self-check can see, and the cycle is then two hops long instead of one.</li>
 * </ul>
 * The ambiguity is injected by emptying (or misdirecting) one follower's resolved HTTP address map for the
 * duration of a single test, rather than by configuring the cluster without HTTP ports: it reproduces exactly
 * the production condition at the point where it matters, without making cluster startup - which dials those
 * same addresses to probe peers - part of what is under test. The map is the live one and the cluster's own
 * background threads read it throughout; that is safe because it is a {@code ConcurrentHashMap}, which is
 * what it has to be anyway - {@code RaftClusterManager} adds and removes entries as peers join and leave.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6191FollowerForwardLoopIT extends BaseRaftHATest {

  @Override
  protected int getServerCount() {
    return 3;
  }

  /**
   * The reachable deployment: peers on one host, no {@code http} port declared. Before the fix this POSTed to
   * itself and never returned, so the timeout is the assertion of last resort.
   */
  @Test
  @Timeout(120)
  void aFollowerRefusesAWriteWhoseLeaderAddressResolvesToItself() {
    final int leader = findLeaderIndex();
    assertThat(leader).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int follower = firstFollower(leader);

    final RaftHAServer raft = getRaftPlugin(follower).getRaftHAServer();
    // The live map, by contract (see getHttpAddresses): writing to it is how this test puts the running node
    // into the misconfigured state. A defensive copy there would leave these tests passing while injecting
    // nothing, so if that accessor ever starts returning one, this is the line that has to change with it.
    final Map<RaftPeerId, String> httpAddresses = raft.getHttpAddresses();
    final Map<RaftPeerId, String> declared = new HashMap<>(httpAddresses);
    try {
      httpAddresses.clear(); // nothing declared: every peer now derives to this node's own endpoint

      final String ownAddress = "localhost:" + getServer(follower).getHttpServer().getPort();
      assertThat(raft.getLeaderHttpAddress())
          .as("the derived leader address is this follower's own endpoint - the defect being guarded")
          .isEqualTo(ownAddress);
      assertThat(raft.isOwnHttpAddress(raft.getLeaderHttpAddress())).isTrue();

      assertThatThrownBy(() -> getServerDatabase(follower, getDatabaseName())
          .command("sql", "CREATE VERTEX TYPE Issue6191SelfForward"))
          .as("the write must be refused, not POSTed back to this node to be forwarded again")
          .isInstanceOf(ServerIsNotTheLeaderException.class)
          .hasMessageContaining("is this node's own")
          .hasMessageContaining(GlobalConfiguration.HA_SERVER_LIST.getKey());
    } finally {
      httpAddresses.putAll(declared);
    }
  }

  /**
   * The cycle a local self-check cannot see: the resolved address names a real peer, just not the leader. The
   * second node has to be the one that stops it, and the refusal has to reach the first as itself - a
   * {@link ServerIsNotTheLeaderException} the caller can retry on, not a flattened transaction failure.
   * <p>
   * The <b>type</b> is the assertion that matters and it travels in the error body's {@code exception} field,
   * which is emitted in every server mode. The message text travels in {@code detail}, which
   * {@code AbstractServerHttpHandler.buildErrorBody} conceals when the server runs in production mode
   * (pre-existing behaviour, not specific to this path), so the text assertion here holds because tests run
   * in the default development mode.
   */
  @Test
  @Timeout(120)
  void aWriteForwardedOntoAFollowerIsRefusedInsteadOfForwardedAgain() {
    final int leader = findLeaderIndex();
    assertThat(leader).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int follower = firstFollower(leader);
    final int otherFollower = secondFollower(leader, follower);

    final RaftHAServer raft = getRaftPlugin(follower).getRaftHAServer();
    final Map<RaftPeerId, String> httpAddresses = raft.getHttpAddresses();
    final Map<RaftPeerId, String> declared = new HashMap<>(httpAddresses);
    try {
      // This follower now believes the leader listens where the OTHER follower does.
      httpAddresses.put(RaftPeerId.valueOf(peerIdForIndex(leader)),
          "localhost:" + getServer(otherFollower).getHttpServer().getPort());

      assertThatThrownBy(() -> getServerDatabase(follower, getDatabaseName())
          .command("sql", "CREATE VERTEX TYPE Issue6191CrossForward"))
          .as("the node that received the forwarded write must refuse it rather than forward it on")
          .isInstanceOf(ServerIsNotTheLeaderException.class)
          .hasMessageContaining("already forwarded");
    } finally {
      httpAddresses.putAll(declared);
    }
  }

  /**
   * Control: with the addresses the cluster actually has, a write issued on a follower is forwarded to the
   * leader and executed. Without it the two tests above would also pass against a build that refused every
   * forwarded write.
   */
  @Test
  @Timeout(120)
  void aFollowerWriteWithCorrectAddressesStillReachesTheLeader() {
    final int leader = findLeaderIndex();
    assertThat(leader).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int follower = firstFollower(leader);

    getServerDatabase(follower, getDatabaseName()).command("sql", "CREATE VERTEX TYPE Issue6191Forwarded");
    waitForAllServers();

    assertThat(getServerDatabase(leader, getDatabaseName()).getSchema().existsType("Issue6191Forwarded"))
        .as("the forwarded DDL must have been executed by the leader")
        .isTrue();
  }

  /**
   * The bulk-load endpoint relays to the leader too, with a whole upload buffered per hop, so it has to
   * enforce the same one-hop rule. Driven over real HTTP because the marker is a request header, and sent
   * the way a peer sends it - with the cluster token, the only form in which the marker is trusted.
   */
  @Test
  @Timeout(120)
  void aBatchForwardedOntoAFollowerIsRefusedInsteadOfForwardedAgain() throws Exception {
    final int leader = findLeaderIndex();
    assertThat(leader).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int follower = firstFollower(leader);
    final String clusterToken = getRaftPlugin(follower).getRaftHAServer().getClusterToken();
    assertThat(clusterToken).as("peers authenticate to each other with a cluster token").isNotBlank();

    final HttpURLConnection conn = (HttpURLConnection) new URI(
        "http://127.0.0.1:" + getServer(follower).getHttpServer().getPort() + "/api/v1/batch/" + getDatabaseName())
        .toURL().openConnection();
    try {
      conn.setRequestMethod("POST");
      conn.setRequestProperty("X-ArcadeDB-Cluster-Token", clusterToken);
      conn.setRequestProperty("X-ArcadeDB-Forwarded-User", "root");
      conn.setRequestProperty("Content-Type", "application/x-ndjson");
      conn.setRequestProperty(LeaderForwardContext.FORWARDED_TO_LEADER_HEADER, "true");
      conn.setDoOutput(true);
      final byte[] payload = "{\"@type\":\"Issue6191Batch\",\"id\":1}\n".getBytes(StandardCharsets.UTF_8);
      conn.setRequestProperty("Content-Length", Integer.toString(payload.length));
      try (final DataOutputStream out = new DataOutputStream(conn.getOutputStream())) {
        out.write(payload);
      }

      assertThat(conn.getResponseCode())
          .as("a batch that arrives already forwarded must be refused by this follower, not relayed again")
          .isEqualTo(400);
      assertThat(new String(conn.getErrorStream().readAllBytes(), StandardCharsets.UTF_8))
          .contains("already forwarded")
          .contains(GlobalConfiguration.HA_SERVER_LIST.getKey());
    } finally {
      conn.disconnect();
    }
  }

  /**
   * The gate on the marker: it is a statement one node makes to another, so it is honored only on a request
   * that authenticated with the cluster token. A client that copies the header onto its own write - or a
   * proxy that relays unknown {@code X-ArcadeDB-*} headers through - must not be able to turn a transparent
   * forward-to-leader into a refusal for itself.
   */
  @Test
  @Timeout(120)
  void theMarkerFromAnOrdinaryClientDoesNotSuppressForwarding() throws Exception {
    final int leader = findLeaderIndex();
    assertThat(leader).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int follower = firstFollower(leader);

    final HttpURLConnection conn = (HttpURLConnection) new URI(
        "http://127.0.0.1:" + getServer(follower).getHttpServer().getPort() + "/api/v1/command/" + getDatabaseName())
        .toURL().openConnection();
    try {
      conn.setRequestMethod("POST");
      conn.setRequestProperty("Authorization", "Basic " + Base64.getEncoder().encodeToString(
          ("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));
      conn.setRequestProperty("Content-Type", "application/json");
      conn.setRequestProperty(LeaderForwardContext.FORWARDED_TO_LEADER_HEADER, "true");
      conn.setDoOutput(true);
      final byte[] payload = ("{\"language\":\"sql\",\"command\":\"CREATE VERTEX TYPE Issue6191ClientMarker\"}")
          .getBytes(StandardCharsets.UTF_8);
      try (final DataOutputStream out = new DataOutputStream(conn.getOutputStream())) {
        out.write(payload);
      }

      assertThat(conn.getResponseCode())
          .as("a client's own header must not stop this follower from forwarding its write to the leader")
          .isEqualTo(200);
    } finally {
      conn.disconnect();
    }

    waitForAllServers();
    assertThat(getServerDatabase(leader, getDatabaseName()).getSchema().existsType("Issue6191ClientMarker"))
        .as("the write was forwarded and executed by the leader")
        .isTrue();
  }

  private int firstFollower(final int leader) {
    for (int i = 0; i < getServerCount(); i++)
      if (i != leader)
        return i;
    throw new IllegalStateException("no follower in a " + getServerCount() + "-node cluster");
  }

  private int secondFollower(final int leader, final int firstFollower) {
    for (int i = 0; i < getServerCount(); i++)
      if (i != leader && i != firstFollower)
        return i;
    throw new IllegalStateException("no second follower in a " + getServerCount() + "-node cluster");
  }
}
