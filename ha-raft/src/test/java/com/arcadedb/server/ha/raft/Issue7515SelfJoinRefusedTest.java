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
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7515: a running node cannot be made to join a cluster it is not configured for, and an add-peer request
 * that names <b>this</b> node is the shape that mistake takes.
 * <p>
 * The issue asked for a decision rather than an implementation, and the decision is recorded in
 * {@link SelfJoinNotSupportedException} and in {@code ServerControlPlane.connectCluster}: the other direction is
 * not offered, because a Raft membership change may only be issued by the leader of the cluster being joined and
 * this node holds no credentials for it. What is fixed here is the part that made the asymmetry read as an
 * oversight - such a request used to be answered {@code 200 Peer ... added} and do nothing, because
 * {@code RaftClusterManager.buildAddArgs} finds the id already in the committed configuration and treats it as
 * the idempotent no-op it is for any OTHER peer.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7515SelfJoinRefusedTest {

  private static final RaftPeerId LOCAL = RaftPeerId.valueOf("127.0.0.1_2434");

  /** The reported case: the address an operator types is their own node's, and it is refused rather than accepted. */
  @Test
  void addingThisNodeToItsOwnClusterIsRefused() {
    assertThatThrownBy(() -> RaftHAServer.ensureNotSelf(LOCAL, peer(LOCAL, "127.0.0.1:2434")))
        .isInstanceOf(SelfJoinNotSupportedException.class)
        // An IllegalArgumentException, because the shared mappers key HTTP 400 / gRPC INVALID_ARGUMENT on that
        // type - the same choice UnreachablePeerException made, and for the same reason: the request named
        // something the server cannot fix by being asked again.
        .isInstanceOf(IllegalArgumentException.class);
  }

  /**
   * The refusal has to be actionable, or it only moves the operator's confusion. It names the node, the address,
   * the request that DOES work, and the configuration key for the restart route.
   */
  @Test
  void theRefusalNamesBothWaysOfActuallyJoiningACluster() {
    assertThatThrownBy(() -> RaftHAServer.ensureNotSelf(LOCAL, peer(LOCAL, "127.0.0.1:2434")))
        .hasMessageContaining("127.0.0.1_2434")
        .hasMessageContaining("127.0.0.1:2434")
        .hasMessageContaining("this node itself")
        .hasMessageContaining("ALREADY a member")
        .hasMessageContaining(GlobalConfiguration.HA_SERVER_LIST.getKey());
  }

  /**
   * The guard must not narrow what the verb is for. Every real add names another node, and re-adding one that is
   * already a committed member stays the idempotent no-op {@code connect cluster} documents and
   * {@code Issue7401ConnectClusterJoinsPeerIT} pins.
   */
  @Test
  void addingAnyOtherPeerIsUntouched() {
    assertThatCode(() -> RaftHAServer.ensureNotSelf(LOCAL, peer(RaftPeerId.valueOf("127.0.0.1_2435"), "127.0.0.1:2435")))
        .doesNotThrowAnyException();
  }

  /**
   * Before {@code start()} has resolved a local id there is no "this node" to compare against, and an add cannot
   * reach a cluster anyway - so the guard stands down rather than refusing on a null.
   */
  @Test
  void aNodeWithNoResolvedLocalIdRefusesNothing() {
    assertThatCode(() -> RaftHAServer.ensureNotSelf(null, peer(LOCAL, "127.0.0.1:2434")))
        .doesNotThrowAnyException();
  }

  /** The id is what identifies the node, not the spelling of the address the request happened to use. */
  @Test
  void theSameNodeUnderADifferentAddressSpellingIsStillRefused() {
    assertThatThrownBy(() -> RaftHAServer.ensureNotSelf(LOCAL, peer(LOCAL, "localhost:2434")))
        .isInstanceOf(SelfJoinNotSupportedException.class)
        .hasMessageContaining("localhost:2434");
  }

  /** The id every entry point derives is the address with its colons replaced, so a self-add is reachable from one. */
  @Test
  void theIdDerivedFromAnAddressIsTheOneTheGuardCompares() {
    final RaftPeer derived = RaftPeerAddressResolver.parseJoinTarget("127.0.0.1:2434", 2434, "").peer();

    assertThat(derived.getId()).isEqualTo(LOCAL);
    assertThatThrownBy(() -> RaftHAServer.ensureNotSelf(LOCAL, derived))
        .isInstanceOf(SelfJoinNotSupportedException.class);
  }

  private static RaftPeer peer(final RaftPeerId id, final String address) {
    return RaftPeer.newBuilder().setId(id).setAddress(address).build();
  }
}
