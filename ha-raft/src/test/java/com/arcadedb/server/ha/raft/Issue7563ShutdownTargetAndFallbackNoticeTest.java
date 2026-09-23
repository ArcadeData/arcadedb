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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerException;

import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Which peer an operator's {@code shutdown <name>} names, and the one line a cluster gets when a peer-to-peer
 * dial has to fall back to the plain listener on an SSL cluster (issue #7546, folded into issue #7563).
 * <p>
 * The naming rules are tested through {@link RaftHAPlugin#resolveShutdownTarget} rather than through
 * {@code shutdownRemoteServer}, because one of the three answers - "the peer you named is this node" - selects
 * a branch that ends in {@code System.exit}. What is pinned here is the verdict; the branch it selects is one
 * delegation to the local path {@code ServerControlPlane.shutdownServer("")} already takes.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7563ShutdownTargetAndFallbackNoticeTest {

  /** Three nodes whose HOSTS are the names an operator types, as a Kubernetes StatefulSet produces them. */
  private static final String NAMED_HOSTS = "arcadedb-0:2434:2480,arcadedb-1:2435:2481,arcadedb-2:2436:2482";

  @BeforeEach
  @AfterEach
  void rearmTheNotice() {
    PlainHttpFallbackNotice.rearmForTests();
  }

  // ------------------------------------------------------------------ which peer the name selects

  /** The ordinary case: one peer answers to the name, and it is not us. */
  @Test
  void aNameThatMatchesOnePeerSelectsIt() {
    final RaftHAServer raft = newDetachedServer(NAMED_HOSTS);

    assertThat(RaftHAPlugin.resolveShutdownTarget(raft, "arcadedb-1"))
        .isEqualTo(RaftPeerId.valueOf("arcadedb-1_2435"));
  }

  /**
   * A peer is named {@code host_raftPort} and nobody types that, so the name an operator gives is matched
   * against the START of the peer id and of the declared address rather than against the whole of either. The
   * peer that IS {@code arcadedb-1} is selected, and {@code arcadedb-10} - which the old {@code contains} rule
   * matched just as happily - is not (code review on PR #7854).
   */
  @Test
  void aNameIsMatchedAsAWholeNameRatherThanAsAnySubstring() {
    final RaftHAServer raft = newDetachedServer("arcadedb-1:2435:2481,arcadedb-10:2436:2482,arcadedb-2:2437:2483");

    assertThat(RaftHAPlugin.resolveShutdownTarget(raft, "arcadedb-1"))
        .isEqualTo(RaftPeerId.valueOf("arcadedb-1_2435"));
  }

  /**
   * The regression the rule above exists for, and the one the ambiguity guard could never catch: with no peer
   * literally named {@code arcadedb-1}, {@code contains} found exactly ONE match - {@code arcadedb-10} - so
   * there was nothing to declare ambiguous and the wrong node was stopped. It is now not a match at all.
   */
  @Test
  void aNameThatIsOnlyAPrefixOfAnotherPeerStopsNothing() {
    final RaftHAServer raft = newDetachedServer("arcadedb-0:2434:2480,arcadedb-10:2436:2482,arcadedb-2:2437:2483");

    assertThatThrownBy(() -> RaftHAPlugin.resolveShutdownTarget(raft, "arcadedb-1"))
        .as("arcadedb-10 is not the server the operator named")
        .isInstanceOf(ServerException.class)
        .hasMessageContaining("Cannot find server 'arcadedb-1'");
  }

  /**
   * A name that genuinely answers for two peers is still refused rather than resolved to the first. The
   * realistic shape of that is two nodes on one host: the name is the whole host of both, so it names both.
   */
  @Test
  void aNameThatMatchesTwoPeersIsRefusedRatherThanResolvedToTheFirst() {
    final RaftHAServer raft = newDetachedServer("localhost:2434:2480,localhost:2435:2481,localhost:2436:2482");

    assertThatThrownBy(() -> RaftHAPlugin.resolveShutdownTarget(raft, "localhost"))
        .isInstanceOf(ServerException.class)
        .hasMessageContaining("matches 3 peers");
  }

  /**
   * The Kubernetes shorthand the whole-name rule has to keep working: a pod is reached at its full service
   * FQDN, and an operator types the pod name. The boundary is the {@code .} that starts the next DNS label.
   */
  @Test
  void aPodNameStillNamesItsFullyQualifiedPeer() {
    final RaftHAServer raft = newDetachedServer(
        "arcadedb-0.arcadedb.ns.svc.cluster.local:2434:2480,arcadedb-1.arcadedb.ns.svc.cluster.local:2435:2481");

    assertThat(RaftHAPlugin.resolveShutdownTarget(raft, "arcadedb-1"))
        .isEqualTo(RaftPeerId.valueOf("arcadedb-1.arcadedb.ns.svc.cluster.local_2435"));
  }

  /** The rule itself, at the three boundaries it accepts and the one it must not. */
  @Test
  void aNameEndsAtASeparatorAndNotInTheMiddleOfALabel() {
    assertThat(RaftHAPlugin.namesPeer("arcadedb-1", "arcadedb-1")).as("the whole name").isTrue();
    assertThat(RaftHAPlugin.namesPeer("arcadedb-1_2435", "arcadedb-1")).as("peer id: _ before the Raft port").isTrue();
    assertThat(RaftHAPlugin.namesPeer("arcadedb-1:2481", "arcadedb-1")).as("address: : before the port").isTrue();
    assertThat(RaftHAPlugin.namesPeer("arcadedb-1.ns.svc", "arcadedb-1")).as("FQDN: . before the next label").isTrue();

    assertThat(RaftHAPlugin.namesPeer("arcadedb-10", "arcadedb-1")).as("a longer label is another server").isFalse();
    assertThat(RaftHAPlugin.namesPeer("arcadedb-10_2436", "arcadedb-1")).as("and so is its peer id").isFalse();
    assertThat(RaftHAPlugin.namesPeer("xarcadedb-1", "arcadedb-1")).as("the match is anchored at the start").isFalse();
    assertThat(RaftHAPlugin.namesPeer(null, "arcadedb-1")).as("a peer with no declared address").isFalse();
  }

  /** A name nobody answers to is still the error it always was. */
  @Test
  void aNameNoPeerAnswersToIsRefused() {
    final RaftHAServer raft = newDetachedServer(NAMED_HOSTS);

    assertThatThrownBy(() -> RaftHAPlugin.resolveShutdownTarget(raft, "arcadedb-7"))
        .isInstanceOf(ServerException.class)
        .hasMessageContaining("Cannot find server 'arcadedb-7'");
  }

  /**
   * The case the {@link PeerDialAddress} self-dial refusal would otherwise turn into an error with the node
   * still running: the name is ours. It resolves to the local peer id, which is what sends the request down
   * the local shutdown path instead of down the dial.
   */
  @Test
  void aNameThatIsThisNodeResolvesToTheLocalPeerRatherThanToADial() {
    final RaftHAServer raft = newDetachedServer(NAMED_HOSTS);

    final RaftPeerId target = RaftHAPlugin.resolveShutdownTarget(raft, "arcadedb-0");

    assertThat(target).isEqualTo(raft.getLocalPeerId());
    assertThat(PeerDialAddress.resolve(raft, target, "peer").refused())
        .as("and the dial it is NOT sent down would have refused it, which is why the branch exists")
        .isTrue();
  }

  // ------------------------------------------------------------------ the plain-HTTP fallback notice

  /** One latch for every dial that can fall back, so the second one to notice does not repeat the first. */
  @Test
  void theFallbackNoticeIsSaidOnceForTheWholeFamilyOfDials() {
    assertThat(PlainHttpFallbackNotice.sayOnce(BootstrapElection.class, "probing its bootstrap-state"))
        .as("the first dial to fall back says it")
        .isTrue();
    assertThat(PlainHttpFallbackNotice.sayOnce(RaftHAPlugin.class, "sending it the shutdown command"))
        .as("a different dial falling back the same way adds nothing an operator can act on")
        .isFalse();
    assertThat(PlainHttpFallbackNotice.sayOnce(BootstrapElection.class, "probing its bootstrap-state"))
        .as("nor does the same one on its next round")
        .isFalse();
  }

  /**
   * A {@link RaftHAServer} built from {@code serverList} with Ratis never started: the peer group and the
   * declared address maps are populated by the constructor, which is all these paths read. The node names
   * itself with the {@code prefix_N} convention, so it is the FIRST entry of the list.
   */
  private static RaftHAServer newDetachedServer(final String serverList) {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_SERVER_LIST, serverList);

    final ArcadeDBServer mockServer = mock(ArcadeDBServer.class);
    when(mockServer.getServerName()).thenReturn("ArcadeDB_0");

    return new RaftHAServer(mockServer, config);
  }
}
