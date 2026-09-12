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

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.awaitility.Awaitility;
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
 * The scenario is therefore the one {@code Issue5275MembershipSelfHealIT} established - a member is
 * dropped from the committed configuration and comes back - with {@code connect cluster} standing in for
 * the auto-join probe that re-adds it there. Each test leaves the three-node configuration it was given.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue7401ConnectClusterJoinsPeerIT extends BaseRaftHATest {

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected boolean persistentRaftStorage() {
    // The re-joining node resumes its own Raft log, so the ADD commits on a catch-up rather than on a
    // full snapshot install. Same reason Issue5275MembershipSelfHealIT sets it.
    return true;
  }

  /**
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

    final Response response = serverCommand(leader, "connect cluster " + raftAddressOf(rejoining));

    assertThat(response.status()).as("body: %s", response.body()).isEqualTo(200);
    assertThat(peerIds(leader)).contains(peerIdForIndex(rejoining)).hasSize(getServerCount());

    final Response again = serverCommand(leader, "connect cluster " + raftAddressOf(rejoining));

    assertThat(again.status()).as("body: %s", again.body()).isEqualTo(200);
    assertThat(peerIds(leader)).hasSize(getServerCount());
  }

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

    dropFromConfigurationAndRestart(leader, rejoining);

    final Response response = serverCommand(follower, "connect cluster " + raftAddressOf(rejoining));

    assertThat(response.status()).as("body: %s", response.body()).isEqualTo(200);
    assertThat(peerIds(leader)).as("the leader's committed configuration").contains(peerIdForIndex(rejoining));
  }

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
   * removing it where it stands, is {@code Issue5275MembershipSelfHealIT}'s sequence and avoids a
   * running non-member campaigning for an election nobody wants during the window.
   */
  private void dropFromConfigurationAndRestart(final int leader, final int rejoining) {
    final RaftHAServer leaderRaft = getRaftPlugin(leader).getRaftHAServer();
    assertThat(leaderRaft.getLivePeers()).hasSize(getServerCount());

    getServer(rejoining).stop();
    leaderRaft.removePeer(peerIdForIndex(rejoining), true);
    Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(500, TimeUnit.MILLISECONDS)
        .untilAsserted(() -> assertThat(leaderRaft.getLivePeers()).hasSize(getServerCount() - 1));

    getServer(rejoining).start();
    Awaitility.await().atMost(60, TimeUnit.SECONDS).pollInterval(500, TimeUnit.MILLISECONDS)
        .untilAsserted(() -> assertThat(getRaftPlugin(rejoining)).isNotNull());
    assertThat(peerIds(leader)).doesNotContain(peerIdForIndex(rejoining));
  }

  /**
   * The Raft address of a fixture server, recovered from its peer id. The two are the same string with
   * one character changed, which is the rule this verb relies on - so deriving one from the other here
   * keeps the test from hardcoding a port the base class owns.
   */
  private String raftAddressOf(final int serverIndex) {
    return peerIdForIndex(serverIndex).replace('_', ':');
  }

  private List<String> peerIds(final int serverIndex) {
    return getRaftPlugin(serverIndex).getRaftHAServer().getLivePeers().stream()
        .map(peer -> peer.getId().toString()).sorted().toList();
  }

  private record Response(int status, String body) {
  }

  private Response serverCommand(final int serverIndex, final String command) throws Exception {
    final int port = getServer(serverIndex).getHttpServer().getPort();
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
