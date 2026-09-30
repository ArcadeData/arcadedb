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
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.AdminApi;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Regression test for issue #8665: the leader service-gap hand-off (#8529) picked its target from the log index alone,
 * which a service gap does not show in, so it could not tell a target that shares the gap. A database missing on every
 * node, or a node-global stale-snapshot gap after a cluster-wide crash, made each hand-off succeed and the next leader
 * hand off in turn. Each peer now reports its own gap on the capability poll the leader already runs, the leader
 * screens those peers out of the hand-off candidates, and a hand-off with no eligible peer no longer falls back to a
 * step-down with no target.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8665PeerServiceGapHandOffTargetTest {

  private static final RaftPeerId SELF = RaftPeerId.valueOf("peer-a_2434");
  private static final RaftPeerId B    = RaftPeerId.valueOf("peer-b_2435");
  private static final RaftPeerId C    = RaftPeerId.valueOf("peer-c_2436");

  // -- the wire: a peer says it has a gap ---------------------------------------------------------------------------

  @Test
  void aNodeWithAGapSaysSo() {
    final JSONObject json = PostCapabilitiesHandler.advertisement("peer-b_2435", Set.of("schema-delta"), true);

    assertThat(json.getBoolean(PostCapabilitiesHandler.SERVICE_GAP, false)).isTrue();
  }

  @Test
  void aHealthyNodesDocumentCarriesNoGapField() {
    assertThat(PostCapabilitiesHandler.advertisement("peer-b_2435", Set.of("schema-delta"), false)
        .has(PostCapabilitiesHandler.SERVICE_GAP)).isFalse();
    assertThat(PostCapabilitiesHandler.advertisement("peer-b_2435", Set.of("schema-delta")).has(PostCapabilitiesHandler.SERVICE_GAP))
        .isFalse();
  }

  @Test
  void theQueryReadsTheGapAndAPeerThatPredatesItReadsAsNone() throws Exception {
    final String url = "http://peer-b:2480/api/v1/cluster/capabilities";

    assertThat(PeerCapabilityQuery.parse("peer-b_2435",
        PostCapabilitiesHandler.advertisement("peer-b_2435", Set.of(), true).toString(), url).serviceGap()).isTrue();
    assertThat(PeerCapabilityQuery.parse("peer-b_2435",
        "{\"peerId\":\"peer-b_2435\",\"version\":\"26.5.1\",\"capabilities\":[]}", url).serviceGap())
        .as("a build that predates the field").isFalse();
  }

  // -- what the leader remembers ------------------------------------------------------------------------------------

  @Test
  void theRegistryReportsThePeersThatHaveAGap() {
    final PeerCapabilityRegistry registry = new PeerCapabilityRegistry();
    final long generation = registry.generation();
    registry.record(generation, B.toString(), Set.of(), "26.10.1", true);
    registry.record(generation, C.toString(), Set.of(), "26.10.1", false);

    assertThat(registry.peersWithServiceGap()).containsExactly(B.toString());

    registry.record(generation, B.toString(), Set.of(), "26.10.1", false);
    assertThat(registry.peersWithServiceGap()).as("the gap closed, the next poll says so").isEmpty();
  }

  @Test
  void aStaleAnswerIsNotBelievedAndAnOldBuildIsNoGap() {
    final AtomicLong clock = new AtomicLong(1_000L);
    final PeerCapabilityRegistry registry = new PeerCapabilityRegistry(5_000L);
    registry.setClock(clock::get);
    final long generation = registry.generation();
    registry.record(generation, B.toString(), Set.of(), "26.10.1", true);
    registry.record(generation, C.toString(), Set.of(), "26.5.1");

    clock.addAndGet(5_001L);
    assertThat(registry.peersWithServiceGap()).as("an answer past its TTL says nothing").isEmpty();
  }

  @Test
  void anUnknownPeerIsNotCountedAsHavingAGap() {
    assertThat(new PeerCapabilityRegistry().peersWithServiceGap()).isEmpty();
  }

  // -- the selection ------------------------------------------------------------------------------------------------

  @Test
  void aPeerThatSharesTheGapIsNotAHandOffCandidate() {
    final Set<String> reachable = Set.of(B.toString(), C.toString());

    final Set<String> eligible = RaftHAServer.withoutServiceGapPeers(reachable, Set.of(B.toString()));

    assertThat(eligible).containsExactly(C.toString());
    assertThat(RaftHAServer.selectStepDownTargets(peers(), SELF, null, eligible)).extracting(p -> p.getId().toString())
        .containsExactly(C.toString());
    assertThat(reachable).as("the caller's set is not modified").hasSize(2);
  }

  @Test
  void whenEveryPeerSharesTheGapNoOneIsACandidate() {
    final Set<String> eligible = RaftHAServer.withoutServiceGapPeers(Set.of(B.toString(), C.toString()),
        Set.of(B.toString(), C.toString()));

    assertThat(RaftHAServer.hasHandoffTarget(peers(), SELF, null, eligible)).isFalse();
  }

  @Test
  void aHealthyClusterKeepsItsReachableSetAsIs() {
    final Set<String> reachable = Set.of(B.toString(), C.toString());

    assertThat(RaftHAServer.withoutServiceGapPeers(reachable, Set.of())).isSameAs(reachable);
  }

  // -- no eligible peer: back off, do not rotate --------------------------------------------------------------------

  @Test
  void withNoEligiblePeerTheHandOffDoesNotStepDownWithNoTarget() throws Exception {
    final AdminApi admin = mock(AdminApi.class);
    final RaftClient client = mock(RaftClient.class);
    when(client.admin()).thenReturn(admin);
    final RaftHAServer raft = mock(RaftHAServer.class);
    when(raft.getClient()).thenReturn(client);
    when(raft.getLocalPeerId()).thenReturn(SELF);
    when(raft.isLeader()).thenReturn(true);
    when(raft.getLivePeers()).thenReturn(peers());
    when(raft.handoffReachablePeers()).thenReturn(Set.of()); // both peers report the gap this leader has
    final RaftClusterManager manager = new RaftClusterManager(raft);

    assertThat(manager.transferLeadership(10_000L, false)).isFalse();

    verify(admin, never()).transferLeadership(org.mockito.ArgumentMatchers.any(), anyLong());
    verify(client, never()).getGroupId();
  }

  @Test
  void theGapHandOffAsksForNoBareStepDown() throws Exception {
    final RaftHAServer raft = mock(RaftHAServer.class);
    when(raft.isLeader()).thenReturn(true);
    when(raft.transferLeadership(anyLong(), eq(false))).thenReturn(false);
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.setRaftHAServer(raft);

    sm.runUnderInstallGate("db", () -> assertThat(sm.handOffLeadershipWhileReplacingDatabase()).isFalse());

    verify(raft).transferLeadership(anyLong(), eq(false));
    verify(raft, never()).transferLeadership(anyLong());
  }

  private static List<RaftPeer> peers() {
    return List.of(peer(SELF), peer(B), peer(C));
  }

  private static RaftPeer peer(final RaftPeerId id) {
    return RaftPeer.newBuilder().setId(id).setAddress("localhost:0").build();
  }
}
