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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.apache.ratis.protocol.RaftPeerId;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests of a member that leaves the committed Raft configuration and comes back, over a three-node cluster
 * with persistent Raft storage: a graceful stop does not shrink the configuration, the Kubernetes auto-join probe re-adds
 * a removed peer, and {@code connect cluster} re-joins one over HTTP.
 */
class ClusterRejoinMembershipIT extends BaseRaftHATest {

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected boolean persistentRaftStorage() {
    // The restarting node must resume its persisted Raft storage: the issue-#5275 strand only exists
    // because persisted storage skips the (pre-fix) auto-join.
    return true;
  }

  // Regression for #5275: a graceful stop keeps the member and the auto-join probe re-adds a removed peer
  /**
   * Regression tests for issue #5275: a Kubernetes pod recreation silently removed the peer from the
   * committed Raft configuration (shutdown auto-leave) and a node restarting with persisted Raft storage
   * was never re-added (the boot-time auto-join only ran when storage was missing), permanently shrinking
   * a 3-node group to 2 with no operator-visible signal.
   * <p>
   * The fix has two halves, both covered here:
   * <ol>
   *   <li>a graceful node stop must NOT shrink the committed Raft configuration - shutdown means
   *       "temporarily unreachable", not "gone"; membership changes are explicit-only;</li>
   *   <li>the {@link KubernetesAutoJoin} probe doubles as a startup membership self-check: a node whose
   *       persisted storage claims membership but which the live configuration no longer contains must
   *       re-add itself instead of waiting forever for a leader that will never dial a non-member.
   *       {@code HA_K8S} cannot be enabled in-process (it requires a Kubernetes pod identity), so the
   *       probe is exercised directly, exactly as the k8s start path now invokes it on every boot.</li>
   * </ol>
   */
  @Tag("slow")
  @Test
  void gracefulStopDoesNotShrinkRaftConfiguration() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);
    final int followerIndex = (leaderIndex + 1) % getServerCount();

    final RaftHAServer leaderRaft = getRaftPlugin(leaderIndex).getRaftHAServer();
    assertThat(leaderRaft.getLivePeers()).hasSize(3);

    // Graceful stop of a follower (what a pod deletion's SIGTERM produces).
    getServer(followerIndex).stop();

    // The committed configuration must still contain all 3 peers: the stopped node is unreachable,
    // not removed. Assert it holds (not just transiently) for a few seconds.
    for (int i = 0; i < 5; i++) {
      assertThat(leaderRaft.getLivePeers())
          .as("committed Raft configuration must not shrink on a graceful member stop")
          .hasSize(3);
      Thread.sleep(1_000);
    }

    // And the node resumes as a member on restart, no re-add needed.
    startServer(followerIndex);
    Awaitility.await().atMost(60, TimeUnit.SECONDS).pollInterval(500, TimeUnit.MILLISECONDS)
        .untilAsserted(() -> {
          final RaftHAPlugin plugin = getRaftPlugin(followerIndex);
          assertThat(plugin).isNotNull();
          assertThat(plugin.getRaftHAServer()).isNotNull();
          assertThat(plugin.getRaftHAServer().getLeaderId()).isNotNull();
          assertThat(leaderRaft.getLivePeers()).hasSize(3);
        });
  }

  // Regression for #5275: a graceful stop keeps the member and the auto-join probe re-adds a removed peer
  @Tag("slow")
  @Test
  void removedPeerWithPersistedStorageSelfHealsViaAutoJoinProbe() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);
    final int followerIndex = (leaderIndex + 1) % getServerCount();
    final String followerPeerId = peerIdForIndex(followerIndex);

    final RaftHAServer leaderRaft = getRaftPlugin(leaderIndex).getRaftHAServer();
    assertThat(leaderRaft.getLivePeers()).hasSize(3);

    // Reproduce the reported state: the peer is dropped from the committed configuration while it is
    // down (as the pre-#5275 shutdown auto-leave did on every pod recreation).
    getServer(followerIndex).stop();
    leaderRaft.removePeer(followerPeerId, true);
    Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(500, TimeUnit.MILLISECONDS)
        .untilAsserted(() -> assertThat(leaderRaft.getLivePeers()).hasSize(2));

    // The node restarts with persisted Raft storage still claiming membership of the 3-node group.
    startServer(followerIndex);
    final RaftHAServer followerRaft = getRaftPlugin(followerIndex).getRaftHAServer();
    assertThat(followerRaft).isNotNull();

    // Startup membership self-check (what the k8s start path now runs on every boot): the probe must
    // detect this node is absent from the live configuration and atomically re-add it.
    final KubernetesAutoJoin.Outcome outcome = new KubernetesAutoJoin(getServer(followerIndex),
        followerRaft.getRaftGroup(), followerRaft.getLocalPeerId(), followerRaft.getRaftProperties()).tryAutoJoin();
    assertThat(outcome).isEqualTo(KubernetesAutoJoin.Outcome.JOINED);

    Awaitility.await().atMost(60, TimeUnit.SECONDS).pollInterval(500, TimeUnit.MILLISECONDS)
        .untilAsserted(() -> {
          assertThat(leaderRaft.getLivePeers())
              .as("removed peer must be re-added to the committed configuration by the self-check probe")
              .hasSize(3);
          assertThat(followerRaft.getLeaderId()).isNotNull();
        });
  }

  // Regression for #7401: connect cluster joins the peer to the committed Raft configuration
  /**
   * Issue #7401: {@code connect cluster <address>} joins the named server to this cluster, over a live
   * Raft cluster and through the HTTP verb an operator actually types.
   * <p>
   * This is the test the issue exists for. Everything else added for #7401 can pass against a verb that
   * parses its argument beautifully and changes no membership; only a committed Raft configuration proves
   * the join, which is why the assertions read {@code getLivePeers()} - the configuration Raft is actually
   * running - and not {@code getConfiguredServers()}, which is the static server list read once at startup
   * and would be unmoved by a successful join.
   * <p>
   * <b>The joined server has to be running.</b> A first draft of this class joined an address nothing
   * listened on, on the theory that a committed member need not be reachable; Ratis refused it - the ADD
   * does not commit until the new peer has caught up, and the verb answered 500 after sixty attempts.
   * That is the right behaviour and worth writing down, because it is also the shape of the operator
   * mistake this verb invites: {@code connect cluster} names a server that must already be up.
   * <p>
   * The scenario is therefore the one of {@link #removedPeerWithPersistedStorageSelfHealsViaAutoJoinProbe()}: - a member is
   * dropped from the committed configuration and comes back - with {@code connect cluster} standing in for
   * the auto-join probe that re-adds it there. Each test leaves the three-node configuration it was given.
   * <p>
   * The verb, end to end, issued on the leader: HTTP 200, and the peer is back in the committed
   * configuration under the id the server-list rule derives from the address it was given. Issuing it a
   * second time is issuing it once - an operator retrying after a lost response, or a script run twice,
   * must not be told the cluster is broken and must not produce a second entry for one process.
   */
  @Test
  void connectClusterAddsThePeerToTheCommittedConfiguration() throws Exception {
    final int leader = findLeaderIndex();
    assertThat(leader).as("a leader must be elected before membership can change").isNotNegative();
    final int rejoining = (leader + 1) % getServerCount();

    dropFromConfigurationAndRestart(leader, rejoining);

    final Response response = serverCommand(leader, "connect cluster " + joinAddressOf(rejoining));

    assertThat(response.status()).as("body: %s", response.body()).isEqualTo(200);
    assertThat(peerIds(leader)).contains(peerIdForIndex(rejoining)).hasSize(getServerCount());

    final Response again = serverCommand(leader, "connect cluster " + joinAddressOf(rejoining));

    assertThat(again.status()).as("body: %s", again.body()).isEqualTo(200);
    assertThat(peerIds(leader)).hasSize(getServerCount());
  }

  // Regression for #7401: connect cluster joins the peer to the committed Raft configuration
  /**
   * The command is not leader-only, and this is what makes that more than a comment:
   * {@code PostServerCommandHandler} forwards neither half of the cluster pair to the leader, so a
   * request that lands on a follower - which is what a Kubernetes ClusterIP Service produces, since it
   * load-balances across every ready endpoint - has to reach the leader through the Raft client
   * underneath the membership change, or answer an error an operator can act on.
   */
  @Test
  void connectClusterIssuedOnAFollowerStillJoinsThePeer() throws Exception {
    final int leader = findLeaderIndex();
    assertThat(leader).isNotNegative();
    final int rejoining = (leader + 1) % getServerCount();
    final int follower = (leader + 2) % getServerCount();

    // Issue #8689: the leader's map is NOT repaired here. removePeer dropped the rejoining node's entry, so the only
    // place the right HTTP port can come from is the declaration typed on the follower - and the security seed, with
    // the #7511 capability probe its group and API-token entries wait on, runs on the leader. Before the fix the
    // leader derived the address from the peer's Raft host plus its OWN HTTP port, which on this fixture (Raft and
    // HTTP ports not in step) is the leader itself, and the verb answered 503 with failedSeeds [groups, API tokens].
    dropFromConfigurationAndRestart(leader, rejoining, false);
    final RaftPeerId rejoiningId = RaftPeerId.valueOf(peerIdForIndex(rejoining));
    assertThat(getRaftPlugin(leader).getRaftHAServer().getHttpAddresses())
        .as("the leader holds no address for the peer it removed").doesNotContainKey(rejoiningId);
    // A brand-new peer has never answered the leader's capability probe. This one did before it was removed, and
    // until that answer ages out it would let the seed through whatever address the leader holds, so wait for the
    // leader's own refresh to drop it - which it does because it can no longer reach the peer.
    final PeerCapabilityRegistry leaderCapabilities = getRaftPlugin(leader).getRaftHAServer().getPeerCapabilityRegistry();
    Awaitility.await().atMost(60, TimeUnit.SECONDS).pollInterval(500, TimeUnit.MILLISECONDS)
        .until(() -> leaderCapabilities.freshAdvertisementOf(rejoiningId.toString()) == null);

    final Response response = serverCommand(follower, "connect cluster " + joinAddressOf(rejoining));

    assertThat(response.status()).as("body: %s", response.body()).isEqualTo(200);
    assertThat(peerIds(leader)).as("the leader's committed configuration").contains(peerIdForIndex(rejoining));
    assertThat(getRaftPlugin(leader).getRaftHAServer().getHttpAddresses().get(rejoiningId))
        .as("the address declared on the follower, recorded by the leader")
        .isEqualTo("localhost:" + getServerHttpPort(rejoining));
  }

  // Regression for #7401: connect cluster joins the peer to the committed Raft configuration
  /**
   * A malformed address is the caller's mistake and answers 400, not the 500 the underlying
   * {@code ServerException} would otherwise produce. With HA enabled the parse is actually reached,
   * which is why this case lives here rather than in the no-HA fixture, where the refusal comes first.
   */
  @Test
  void aMalformedAddressAnswers400AndChangesNothing() throws Exception {
    final int leader = findLeaderIndex();
    assertThat(leader).isNotNegative();
    final List<String> before = peerIds(leader);

    final Response response = serverCommand(leader, "connect cluster db2:2435,db3:2436");

    assertThat(response.status()).as("body: %s", response.body()).isEqualTo(400);
    assertThat(response.body()).contains("one server at a time");
    assertThat(peerIds(leader)).isEqualTo(before);
  }

  // -----------------------------------------------------------------------------------------------

  /**
   * Puts the cluster in the state {@code connect cluster} is for: {@code rejoining} is running and
   * reachable but is not in the committed configuration. Stopping it before the removal, rather than
   * removing it where it stands, is the sequence of {@link #removedPeerWithPersistedStorageSelfHealsViaAutoJoinProbe()} and avoids a
   * running non-member campaigning for an election nobody wants during the window.
   */
  private void dropFromConfigurationAndRestart(final int leader, final int rejoining) {
    dropFromConfigurationAndRestart(leader, rejoining, true);
  }

  /**
   * @param repairHttpAddresses whether to put the fixture's bound ports back into every node's HTTP-address map, which
   *                            removePeer emptied on the leader for this node
   */
  private void dropFromConfigurationAndRestart(final int leader, final int rejoining,
      final boolean repairHttpAddresses) {
    final RaftHAServer leaderRaft = getRaftPlugin(leader).getRaftHAServer();
    assertThat(leaderRaft.getLivePeers()).hasSize(getServerCount());

    // Issue #8330: back on the HTTP port it owned, as BaseGraphServerTest.endTest restarts one, so every other
    // node's map still names it and the restart cannot re-scan onto a port the fixture steered past.
    final int boundHttpPort = getServerHttpPort(rejoining);
    getServer(rejoining).getConfiguration().setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_PORT,
        String.valueOf(boundHttpPort));

    getServer(rejoining).stop();
    leaderRaft.removePeer(peerIdForIndex(rejoining), true);
    Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(500, TimeUnit.MILLISECONDS)
        .untilAsserted(() -> assertThat(leaderRaft.getLivePeers()).hasSize(getServerCount() - 1));

    // Issue #8330: removePeer dropped this node's HTTP address from the leader's map, and the leader's security seed
    // probes it there; without it the seed's group and API-token entries are refused by the #7511 gate. startServer
    // puts the bound ports back into every node's map; the #8689 case keeps the leader's map as removePeer left it.
    if (repairHttpAddresses)
      startServer(rejoining);
    else
      startServerKeepingDeclaredPeerHttpAddresses(rejoining);
    assertThat(getServerHttpPort(rejoining)).as("the restarted node's HTTP port").isEqualTo(boundHttpPort);
    Awaitility.await().atMost(60, TimeUnit.SECONDS).pollInterval(500, TimeUnit.MILLISECONDS)
        .untilAsserted(() -> assertThat(getRaftPlugin(rejoining)).isNotNull());
    assertThat(peerIds(leader)).doesNotContain(peerIdForIndex(rejoining));
  }

  /**
   * The address {@code connect cluster} is given for a fixture server: its Raft address (the peer id with one
   * character changed) plus the HTTP port it bound. The port is declared because this fixture's Raft and HTTP ports
   * are not in step, so the derived {@code raftPort + offset} address would name nobody (issue #8330).
   */
  private String joinAddressOf(final int serverIndex) {
    return peerIdForIndex(serverIndex).replace('_', ':') + ":" + getServerHttpPort(serverIndex);
  }

  private List<String> peerIds(final int serverIndex) {
    return getRaftPlugin(serverIndex).getRaftHAServer().getLivePeers().stream()
        .map(peer -> peer.getId().toString()).sorted().toList();
  }

  private record Response(int status, String body) {
  }

  private Response serverCommand(final int serverIndex, final String command) throws Exception {
    final int port = getServerHttpPort(serverIndex);
    final HttpURLConnection conn = (HttpURLConnection) new URI(
        "http://localhost:" + port + "/api/v1/server").toURL().openConnection();
    conn.setRequestMethod("POST");
    conn.setRequestProperty("Content-Type", "application/json");
    conn.setRequestProperty("Authorization", "Basic " + Base64.getEncoder().encodeToString(
        ("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));
    conn.setDoOutput(true);
    try {
      conn.getOutputStream().write(
          new JSONObject().put("command", command).toString().getBytes(StandardCharsets.UTF_8));
      final int status = conn.getResponseCode();
      final InputStream stream = status < 400 ? conn.getInputStream() : conn.getErrorStream();
      final String body = stream == null ? "" : new String(stream.readAllBytes(), StandardCharsets.UTF_8);
      return new Response(status, body);
    } finally {
      conn.disconnect();
    }
  }
}
