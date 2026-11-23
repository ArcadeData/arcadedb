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

import com.arcadedb.server.ha.raft.RaftPeerAddressResolver.JoinTarget;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.AdminApi;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.SetConfigurationRequest;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests of {@link RaftPeerAddressResolver}: the local peer synthesized for a Kubernetes scale-up pod and the
 * translation of a {@code connect cluster} argument into the peer to add, priority included.
 */
class RaftPeerAddressResolverTest {

  /** {@code GlobalConfiguration.HA_RAFT_PORT}'s default, the port an entry without one inherits. */
  private static final int DEFAULT_RAFT_PORT = 2434;

  /** The membership the join is made into: committed and live alike. */
  private static final Set<String> COMMITTED = Set.of("A", "B", "C");

  // Regression for #4836: a scale-up pod beyond the static server list gets a synthesized local peer
  // The pre-fix behavior that crash-looped the pod: an ordinal past the static list throws.
  /**
   * Regression for issue #4836: scaling a Kubernetes StatefulSet beyond the static {@code HA_SERVER_LIST}
   * must not crash-loop the new pods. A pod whose ordinal is {@code >= peers.size()} is a scale-up node
   * that is not in the static list; in K8s mode its local Raft peer is synthesized from the StatefulSet
   * naming convention + DNS suffix so it can boot and auto-join, instead of throwing "index out of range".
   */
  @Test
  void outOfRangeOrdinalStillThrowsWithoutK8s() {
    final var parsed = RaftPeerAddressResolver.parsePeerList(
        "arcadedb-0.svc:2434,arcadedb-1.svc:2434,arcadedb-2.svc:2434", 2434);
    assertThatThrownBy(() -> RaftPeerAddressResolver.findLocalPeerId(
        parsed.peers(), parsed.peerNames(), "arcadedb-3", null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("out of range");
  }

  // Regression for #4836: a scale-up pod beyond the static server list gets a synthesized local peer
  // In K8s mode a scale-up ordinal is synthesized rather than rejected.
  @Test
  void synthesizesLocalPeerForScaleUpOrdinalInK8s() {
    final var parsed = RaftPeerAddressResolver.parsePeerList(
        "arcadedb-0.svc:2434,arcadedb-1.svc:2434,arcadedb-2.svc:2434", 2434);
    final RaftPeer synth = RaftPeerAddressResolver.synthesizeK8sScaleUpPeer(
        true, parsed.peers(), "arcadedb-3", ".svc", 2434);
    assertThat(synth).isNotNull();
    assertThat(synth.getAddress()).isEqualTo("arcadedb-3.svc:2434");
    assertThat(synth.getId().toString()).isEqualTo("arcadedb-3.svc_2434");
  }

  // Regression for #4836: a scale-up pod beyond the static server list gets a synthesized local peer
  // Once synthesized and appended, the local peer resolves normally by its (now in-range) ordinal.
  @Test
  void augmentedListResolvesScaleUpNode() {
    final var parsed = RaftPeerAddressResolver.parsePeerList(
        "arcadedb-0.svc:2434,arcadedb-1.svc:2434,arcadedb-2.svc:2434", 2434);
    final RaftPeer synth = RaftPeerAddressResolver.synthesizeK8sScaleUpPeer(
        true, parsed.peers(), "arcadedb-3", ".svc", 2434);
    final List<RaftPeer> augmented = new ArrayList<>(parsed.peers());
    augmented.add(synth);
    assertThat(synth.getId()).isEqualTo(augmented.get(3).getId());
  }

  // Regression for #4836: a scale-up pod beyond the static server list gets a synthesized local peer
  // No synthesis outside K8s mode: the static-list contract is unchanged.
  @Test
  void noSynthesisWhenK8sDisabled() {
    final var parsed = RaftPeerAddressResolver.parsePeerList("arcadedb-0.svc:2434,arcadedb-1.svc:2434", 2434);
    assertThat(RaftPeerAddressResolver.synthesizeK8sScaleUpPeer(false, parsed.peers(), "arcadedb-3", ".svc", 2434))
        .isNull();
  }

  // Regression for #4836: a scale-up pod beyond the static server list gets a synthesized local peer
  // An in-range ordinal must resolve through the normal index path, not be synthesized.
  @Test
  void noSynthesisForInRangeOrdinal() {
    final var parsed = RaftPeerAddressResolver.parsePeerList(
        "arcadedb-0.svc:2434,arcadedb-1.svc:2434,arcadedb-2.svc:2434", 2434);
    assertThat(RaftPeerAddressResolver.synthesizeK8sScaleUpPeer(true, parsed.peers(), "arcadedb-1", ".svc", 2434))
        .isNull();
  }

  // Regression for #4836: a scale-up pod beyond the static server list gets a synthesized local peer
  // A name with no parseable -N/_N ordinal cannot be a StatefulSet scale-up pod: no synthesis.
  @Test
  void noSynthesisWhenNameHasNoOrdinal() {
    final var parsed = RaftPeerAddressResolver.parsePeerList("arcadedb-0.svc:2434,arcadedb-1.svc:2434", 2434);
    assertThat(RaftPeerAddressResolver.synthesizeK8sScaleUpPeer(true, parsed.peers(), "arcadedb", ".svc", 2434))
        .isNull();
  }

  // Regression for #4836: a scale-up pod beyond the static server list gets a synthesized local peer
  // Empty DNS suffix (non-DNS K8s setups) still synthesizes a usable bare-host peer.
  @Test
  void synthesizesWithEmptyDnsSuffix() {
    final var parsed = RaftPeerAddressResolver.parsePeerList("arcadedb-0:2434,arcadedb-1:2434", 2434);
    final RaftPeer synth = RaftPeerAddressResolver.synthesizeK8sScaleUpPeer(
        true, parsed.peers(), "arcadedb-2", "", 2434);
    assertThat(synth).isNotNull();
    assertThat(synth.getAddress()).isEqualTo("arcadedb-2:2434");
  }

  private static JoinTarget parse(final String address) {
    return RaftPeerAddressResolver.parseJoinTarget(address, DEFAULT_RAFT_PORT, "");
  }

  // Regression for #7401: connect cluster maps a server-list entry to the peer id the joiner derives for itself
  /**
   * Issue #7401: {@code connect cluster <address>} takes one entry of {@code arcadedb.ha.serverList} and
   * turns it into the peer to add.
   * <p>
   * That is the whole of the argument mapping the verb was missing, and the property worth pinning is not
   * that the mapping produces <em>a</em> peer id but that it produces <b>the same</b> id the joining server
   * derives for itself. A command that invents an id the peer does not answer to leaves the cluster with a
   * configuration entry for a process that will never appear, which no amount of Raft retrying can fix -
   * so {@link #theJoinPathAndTheServerListPathAgreeOnTheSameAddress()} compares the two derivations
   * directly rather than asserting a literal on one of them.
   */
  @Test
  void hostAndRaftPortYieldTheUnderscoredPeerId() {
    final JoinTarget target = parse("db2:2435");

    assertThat(target.peer().getId().toString()).isEqualTo("db2_2435");
    assertThat(target.peer().getAddress()).isEqualTo("db2:2435");
    assertThat(target.httpAddress()).isNull();
    assertThat(target.name()).isNull();
  }

  // Regression for #7401: connect cluster maps a server-list entry to the peer id the joiner derives for itself
  /**
   * A bare host inherits {@code arcadedb.ha.raftPort}, exactly as the same entry would in the server
   * list. The derived id therefore carries the default port, which is what the joining node - reading
   * the same default - calls itself.
   */
  @Test
  void aBareHostInheritsTheDefaultRaftPort() {
    final JoinTarget target = parse("db2");

    assertThat(target.peer().getAddress()).isEqualTo("db2:" + DEFAULT_RAFT_PORT);
    assertThat(target.peer().getId().toString()).isEqualTo("db2_" + DEFAULT_RAFT_PORT);
  }

  // Regression for #7401: connect cluster maps a server-list entry to the peer id the joiner derives for itself
  /**
   * The positional form's HTTP port is carried through, because the address {@code RaftClusterManager}
   * would otherwise derive is the Raft port plus <em>the adding node's</em> HTTP offset - right only for
   * a homogeneous cluster.
   */
  @Test
  void anExplicitHttpPortIsCarriedThrough() {
    final JoinTarget target = parse("db2:2435:2481");

    assertThat(target.peer().getAddress()).isEqualTo("db2:2435");
    assertThat(target.httpAddress()).isEqualTo("db2:2481");
  }

  // Regression for #7401: connect cluster maps a server-list entry to the peer id the joiner derives for itself
  @Test
  void theNamePrefixBecomesTheDisplayName() {
    final JoinTarget target = parse("frankfurt@db2:2435");

    assertThat(target.name()).isEqualTo("frankfurt");
    assertThat(target.peer().getId().toString()).isEqualTo("db2_2435");
  }

  // Regression for #7401: connect cluster maps a server-list entry to the peer id the joiner derives for itself
  @Test
  void theObjectFormIsAcceptedToo() {
    final JoinTarget target = parse("db2:{raft:2435,http:2481,priority:7}");

    assertThat(target.peer().getAddress()).isEqualTo("db2:2435");
    assertThat(target.httpAddress()).isEqualTo("db2:2481");
    assertThat(target.peer().getPriority()).isEqualTo(7);
  }

  // Regression for #7401: connect cluster maps a server-list entry to the peer id the joiner derives for itself
  /**
   * The assertion this class exists for: join and startup must derive one identity from one address.
   * Comparing the two derivations is what a literal cannot do - a literal passes just as well against
   * two rules that happen to agree on {@code db2:2435} and part company on the object form or on a
   * bare host.
   */
  @Test
  void theJoinPathAndTheServerListPathAgreeOnTheSameAddress() {
    for (final String entry : new String[] { "db2", "db2:2435", "db2:2435:2481",
        "frankfurt@db2:2435", "db2:{raft:2435,http:2481}" }) {
      final JoinTarget joined = parse(entry);
      final RaftPeerAddressResolver.ParsedPeerList declared =
          RaftPeerAddressResolver.parsePeerList(entry, DEFAULT_RAFT_PORT, "");

      assertThat(joined.peer().getId())
          .as("connect cluster '%s' must derive the id the server list derives", entry)
          .isEqualTo(declared.peers().getFirst().getId());
      assertThat(joined.peer().getAddress()).isEqualTo(declared.peers().getFirst().getAddress());
    }
  }

  // Regression for #7401: connect cluster maps a server-list entry to the peer id the joiner derives for itself
  /** The Kubernetes DNS suffix is applied here as it is at startup, or the short pod name resolves nowhere. */
  @Test
  void theKubernetesDnsSuffixIsApplied() {
    final JoinTarget target = RaftPeerAddressResolver.parseJoinTarget("arcadedb-2", DEFAULT_RAFT_PORT,
        ".arcadedb.default.svc.cluster.local");

    assertThat(target.peer().getAddress())
        .isEqualTo("arcadedb-2.arcadedb.default.svc.cluster.local:" + DEFAULT_RAFT_PORT);
  }

  // Regression for #7401: connect cluster maps a server-list entry to the peer id the joiner derives for itself
  /**
   * A blank argument is the caller's to fix, not a server precondition: {@code IllegalArgumentException}
   * is what HTTP answers 400 and gRPC {@code INVALID_ARGUMENT} for. A bare {@code connect cluster} over
   * HTTP arrives here as {@code ""} from {@code extractTarget}, which is why the empty string is tested
   * and not only {@code null}.
   */
  @Test
  void aBlankAddressIsRejectedAsACallerError() {
    assertThatThrownBy(() -> parse(""))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("requires the address");

    assertThatThrownBy(() -> parse("   "))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("requires the address");

    assertThatThrownBy(() -> parse(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("requires the address");
  }

  // Regression for #7401: connect cluster maps a server-list entry to the peer id the joiner derives for itself
  /**
   * The server list is comma-separated and this argument is one entry of it, so a list has to be
   * refused rather than silently joining only its first element - the failure mode an operator would
   * not notice until the cluster was one node short.
   */
  @Test
  void aCommaSeparatedListIsRefusedRatherThanTruncated() {
    assertThatThrownBy(() -> parse("db2:2435,db3:2436"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("one server at a time");
  }

  // Regression for #7401: connect cluster maps a server-list entry to the peer id the joiner derives for itself
  /**
   * A malformed entry reaches the caller as its own mistake. The underlying parser raises
   * {@code ServerException}, which the transports would answer 500 / {@code INTERNAL}: converting it
   * here is what keeps a typo from reading as a server fault.
   */
  @Test
  void aMalformedAddressIsReportedAsACallerErrorNotAServerFault() {
    assertThatThrownBy(() -> parse("db2:2435:2481:0:2491:extra"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid server address");

    assertThatThrownBy(() -> parse("@db2:2435"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid server address");
  }

  private static RaftPeer peer(final String id) {
    return RaftPeer.newBuilder().setId(RaftPeerId.valueOf(id)).setAddress("localhost:2444").build();
  }

  /**
   * Captures the configuration change {@code addPeer} asks Ratis to commit for the peer parsed out of
   * {@code entry}.
   */
  private static RaftPeer addedPeerFor(final String entry) throws Exception {
    final FakeRaftHAServer server = FakeRaftHAServer.detached();
    final RaftClient client = mock(RaftClient.class);
    final AdminApi admin = mock(AdminApi.class);
    final RaftClientReply reply = mock(RaftClientReply.class);

    final ArgumentCaptor<SetConfigurationRequest.Arguments> captor =
        ArgumentCaptor.forClass(SetConfigurationRequest.Arguments.class);

    server.client(client);
    when(client.admin()).thenReturn(admin);
    when(reply.isSuccess()).thenReturn(true);
    when(admin.setConfiguration(captor.capture())).thenReturn(reply);
    final List<RaftPeer> committed = COMMITTED.stream().map(RaftPeerAddressResolverTest::peer).toList();
    server.livePeers(committed);
    server.committedPeers(committed);
    server.httpAddresses(new HashMap<>());
    server.raftGroup(RaftGroup.valueOf(RaftGroupId.randomId()));

    final JoinTarget target = RaftPeerAddressResolver.parseJoinTarget(entry, DEFAULT_RAFT_PORT, "");
    new RaftClusterManager(server).addPeer(target.peer(), target.name());

    // The change carries the committed membership plus the new peer (it is built from the committed configuration)
    final List<RaftPeer> newConf = captor.getValue().getServersInNewConf();
    assertThat(newConf).extracting(p -> p.getId().toString()).hasSize(COMMITTED.size() + 1).containsAll(COMMITTED);
    return newConf.stream().filter(p -> !COMMITTED.contains(p.getId().toString())).findFirst().orElseThrow();
  }

  // Regression for #7401: the leader-election priority of the address reaches the configuration change
  /**
   * Issue #7401: the leader-election priority an address declares has to reach the peer that is actually
   * added, not just the parse.
   * <p>
   * This is the gap a review of the first implementation found, and it is worth its own class because of
   * how invisible it was. {@code RaftPeerAddressResolverTest.theObjectFormIsAcceptedToo} asserts
   * {@code target.peer().getPriority()} and passed the whole time; the value was then dropped one layer
   * down, where {@code addPeer} rebuilt a fresh {@link RaftPeer} from an id and an address and left
   * everything else at {@link RaftPeer.Builder}'s defaults. Every other assertion about the join - the
   * id, the address, the {@code Mode.ADD}, the idempotence - held perfectly while the cluster got a
   * configuration the operator had not asked for.
   * <p>
   * The consequence is not cosmetic. {@code RaftHAServer.selectStepDownTargets} reads the live
   * {@code getPriority()} of each peer and, once any peer has a positive priority, skips the priority-0
   * ones as non-electable witnesses - so a priority quietly reset to 0 changes which nodes can take
   * leadership.
   * <p>
   * The assertion therefore reads the {@link SetConfigurationRequest.Arguments} that Ratis is asked to
   * commit, which is the last point the value can still be lost, and it starts from
   * {@code parseJoinTarget} rather than from a hand-built peer so the parse and the membership change
   * are pinned as one path.
   */
  @Test
  void theObjectFormPriorityReachesTheConfigurationChange() throws Exception {
    final RaftPeer added = addedPeerFor("db2:{raft:2435,http:2481,priority:7}");

    assertThat(added.getPriority()).isEqualTo(7);
    assertThat(added.getId().toString()).isEqualTo("db2_2435");
    assertThat(added.getAddress()).isEqualTo("db2:2435");
  }

  // Regression for #7401: the leader-election priority of the address reaches the configuration change
  /** The four-field positional form declares a priority too, and must not be the path that loses it. */
  @Test
  void thePositionalFormPriorityReachesTheConfigurationChange() throws Exception {
    assertThat(addedPeerFor("db2:2435:2481:5").getPriority()).isEqualTo(5);
  }

  // Regression for #7401: the leader-election priority of the address reaches the configuration change
  /**
   * A named peer keeps both halves: the {@code name@} prefix is stripped before the address is parsed,
   * and that must not cost the priority that follows it.
   */
  @Test
  void aNamedEntryKeepsItsPriority() throws Exception {
    assertThat(addedPeerFor("frankfurt@db2:{raft:2435,priority:9}").getPriority()).isEqualTo(9);
  }

  // Regression for #7401: the leader-election priority of the address reaches the configuration change
  /**
   * An entry that declares no priority gets 0, which is what {@code RaftPeer.Builder} defaults to and
   * what every caller of the three-argument {@code addPeer} - {@code POST /api/v1/cluster/peer}, whose
   * payload has no priority field - has always produced. Without this the test above would also pass
   * against an implementation that invented a priority of its own.
   */
  @Test
  void anEntryWithoutAPriorityIsAddedWithTheDefault() throws Exception {
    assertThat(addedPeerFor("db2:2435").getPriority()).isZero();
  }
}
