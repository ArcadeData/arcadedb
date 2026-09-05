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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.exception.ConfigurationException;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.handler.ExecutionResponse;
import com.arcadedb.server.security.ServerSecurityUser;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.AdminApi;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Regression test for issue #7134: the TARGETED leadership-transfer path had no {@code isLeader()} guard,
 * so {@code POST /api/v1/cluster/leader} (with a target) and {@code POST /api/v1/cluster/stepdown} served
 * by a FOLLOWER drove an unrequested leadership change on the real leader - Ratis routes a
 * {@code TransferLeadershipRequest} submitted through a follower's client to the current leader, so the
 * call succeeds rather than failing locally.
 * <p>
 * That is what happens whenever the call goes through a Kubernetes ClusterIP Service instead of directly to
 * the leader pod, since the Service load-balances across every ready endpoint. #4809 added exactly this
 * guard to the no-target path; the targeted one was left open.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7134TargetedTransferRequiresLeaderTest {

  private static final String TARGET_PEER = "peer-c_2436";
  private static final String REAL_LEADER = "peer-b_2435";

  /**
   * The crux: a follower asked to hand leadership to a specific peer must refuse locally instead of
   * letting Ratis route the request to the leader that never asked for an election.
   */
  @Test
  void aTargetedTransferOnAFollowerIsRefusedWithoutReachingRatis() {
    final RaftHAServer raft = mock(RaftHAServer.class);
    when(raft.isLeader()).thenReturn(false);
    when(raft.getLeaderId()).thenReturn(RaftPeerId.valueOf(REAL_LEADER));

    final RaftClusterManager manager = new RaftClusterManager(raft);

    assertThatThrownBy(() -> manager.transferLeadership(TARGET_PEER, 10_000))
        .isInstanceOf(NotTheLeaderRefusalException.class)
        .hasMessageContaining("not the leader")
        .as("the error must name the leader so the caller can retry against the right node")
        .hasMessageContaining(REAL_LEADER);

    // Nothing may be submitted: a request that reaches Ratis is a request the leader acts on.
    verify(raft, never()).getClient();
  }

  /** The same refusal when no leader is known at all: still nothing to transfer from here. */
  @Test
  void aTargetedTransferOnAFollowerWithNoKnownLeaderIsAlsoRefused() {
    final RaftHAServer raft = mock(RaftHAServer.class);
    when(raft.isLeader()).thenReturn(false);
    when(raft.getLeaderId()).thenReturn(null);

    final RaftClusterManager manager = new RaftClusterManager(raft);

    assertThatThrownBy(() -> manager.transferLeadership(TARGET_PEER, 10_000))
        .isInstanceOf(NotTheLeaderRefusalException.class)
        .hasMessageContaining("not the leader");
    verify(raft, never()).getClient();
  }

  /** Control: the leader still transfers, or the guard above would have broken the endpoint outright. */
  @Test
  void theLeaderStillTransfersToTheRequestedTarget() throws Exception {
    final RaftHAServer raft = mock(RaftHAServer.class);
    when(raft.isLeader()).thenReturn(true);

    final RaftClientReply reply = mock(RaftClientReply.class);
    when(reply.isSuccess()).thenReturn(true);
    final AdminApi admin = mock(AdminApi.class);
    when(admin.transferLeadership(any(RaftPeerId.class), anyLong())).thenReturn(reply);
    final RaftClient client = mock(RaftClient.class);
    when(client.admin()).thenReturn(admin);
    when(raft.getClient()).thenReturn(client);

    final RaftClusterManager manager = new RaftClusterManager(raft);

    assertThatCode(() -> manager.transferLeadership(TARGET_PEER, 10_000)).doesNotThrowAnyException();
    verify(admin).transferLeadership(RaftPeerId.valueOf(TARGET_PEER), 10_000);
  }

  /**
   * {@code stepDown()} is the other entry point, and it picks its own target, so the guard has to sit in it
   * too: without one, a step-down sent to a follower selects a peer and transfers on the leader's behalf.
   */
  @Test
  void stepDownOnANodeThatIsNotTheLeaderIsRefused() {
    final RaftHAServer raft = detachedServer();

    assertThatThrownBy(raft::stepDown)
        .isInstanceOf(NotTheLeaderRefusalException.class)
        .hasMessageContaining("not the leader");
  }

  /**
   * The no-target overload keeps the boolean contract #4809 published and
   * {@code Issue4809NoTargetTransferLeadershipIT} pins: a Java caller reads {@code false}, not an exception.
   * The endpoint's 409 comes from a guard in the handler instead - see below - so the HTTP contract is uniform
   * without changing what this method promises embedded callers.
   */
  @Test
  void theNoTargetTransferKeepsItsBooleanContractOnAFollower() {
    final RaftHAServer raft = mock(RaftHAServer.class);
    when(raft.isLeader()).thenReturn(false);
    when(raft.getClient()).thenReturn(mock(RaftClient.class));

    final RaftClusterManager manager = new RaftClusterManager(raft);

    assertThat(manager.transferLeadership(10_000)).isFalse();
  }

  /**
   * The other half of the 409 contract: a transfer the leader attempted and failed is NOT a follower refusal,
   * and must keep the response it always had. Collapsing the two would tell a caller to reissue the request
   * against the very node that just failed it.
   */
  @Test
  void aLeaderSideTransferFailureIsNotReportedAsAFollowerRefusal() throws Exception {
    final RaftHAServer raft = mock(RaftHAServer.class);
    doThrow(new ConfigurationException("Failed to transfer leadership to " + TARGET_PEER + ": timeout"))
        .when(raft).transferLeadership(any(String.class), anyLong());

    final PostTransferLeaderHandler handler = new PostTransferLeaderHandler(mock(HttpServer.class), pluginFor(raft));

    assertThatThrownBy(() -> handler.execute(null, rootUser(), new JSONObject().put("peerId", TARGET_PEER)))
        .as("the handler must not swallow a leader-side failure into a 409")
        .isInstanceOf(ConfigurationException.class);
  }

  /** The HTTP contract: a follower answers 409 naming the leader, not 200 for an effect that landed elsewhere. */
  @Test
  void theStepDownEndpointAnswers409OnAFollower() throws Exception {
    final RaftHAServer raft = mock(RaftHAServer.class);
    doThrowNotLeader(raft);

    final ExecutionResponse response = new PostStepDownHandler(mock(HttpServer.class), pluginFor(raft))
        .execute(null, rootUser(), new JSONObject());

    assertThat(response.getCode()).isEqualTo(409);
    assertThat(response.getResponse()).contains(REAL_LEADER);
  }

  /**
   * The no-target request through the endpoint, not just through the manager: this is the shape a Kubernetes
   * Service actually delivers - a bare {@code {}} body that lands on whichever pod the load balancer picked.
   */
  @Test
  void theNoTargetTransferEndpointAnswers409OnAFollower() throws Exception {
    final RaftHAServer raft = mock(RaftHAServer.class);
    when(raft.isLeader()).thenReturn(false);
    when(raft.getLeaderId()).thenReturn(RaftPeerId.valueOf(REAL_LEADER));

    final ExecutionResponse response = new PostTransferLeaderHandler(mock(HttpServer.class), pluginFor(raft))
        .execute(null, rootUser(), new JSONObject());

    assertThat(response.getCode()).isEqualTo(409);
    assertThat(response.getResponse()).contains(REAL_LEADER);
    verify(raft, never()).transferLeadership(anyLong());
  }

  @Test
  void theTargetedTransferEndpointAnswers409OnAFollower() throws Exception {
    final RaftHAServer raft = mock(RaftHAServer.class);
    doThrowNotLeaderOnTransfer(raft);

    final ExecutionResponse response = new PostTransferLeaderHandler(mock(HttpServer.class), pluginFor(raft))
        .execute(null, rootUser(), new JSONObject().put("peerId", TARGET_PEER));

    assertThat(response.getCode()).isEqualTo(409);
    assertThat(response.getResponse()).contains(REAL_LEADER);
  }

  private static NotTheLeaderRefusalException refusal() {
    return new NotTheLeaderRefusalException("Refusing", RaftPeerId.valueOf(REAL_LEADER));
  }

  private static void doThrowNotLeader(final RaftHAServer raft) {
    doThrow(refusal()).when(raft).stepDown();
  }

  private static void doThrowNotLeaderOnTransfer(final RaftHAServer raft) {
    doThrow(refusal()).when(raft).transferLeadership(any(String.class), anyLong());
  }

  private static RaftHAPlugin pluginFor(final RaftHAServer raft) {
    final RaftHAPlugin plugin = mock(RaftHAPlugin.class);
    when(plugin.getRaftHAServer()).thenReturn(raft);
    return plugin;
  }

  private static ServerSecurityUser rootUser() {
    final ServerSecurityUser user = mock(ServerSecurityUser.class);
    when(user.getName()).thenReturn("root");
    return user;
  }

  /** A {@link RaftHAServer} with Ratis never started, so {@code isLeader()} is false the way a follower's is. */
  private static RaftHAServer detachedServer() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_SERVER_LIST, "localhost:2434:2480,localhost:2435:2481,localhost:2436:2482");

    final ArcadeDBServer mockServer = mock(ArcadeDBServer.class);
    when(mockServer.getServerName()).thenReturn("ArcadeDB_0");

    return new RaftHAServer(mockServer, config);
  }
}
