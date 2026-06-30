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

import org.apache.ratis.protocol.RaftPeer;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression for issue #4836: scaling a Kubernetes StatefulSet beyond the static {@code HA_SERVER_LIST}
 * must not crash-loop the new pods. A pod whose ordinal is {@code >= peers.size()} is a scale-up node
 * that is not in the static list; in K8s mode its local Raft peer is synthesized from the StatefulSet
 * naming convention + DNS suffix so it can boot and auto-join, instead of throwing "index out of range".
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue4836K8sScaleUpTest {

  // The pre-fix behavior that crash-looped the pod: an ordinal past the static list throws.
  @Test
  void outOfRangeOrdinalStillThrowsWithoutK8s() {
    final var parsed = RaftPeerAddressResolver.parsePeerList(
        "arcadedb-0.svc:2434,arcadedb-1.svc:2434,arcadedb-2.svc:2434", 2434);
    assertThatThrownBy(() -> RaftPeerAddressResolver.findLocalPeerId(
        parsed.peers(), parsed.peerNames(), "arcadedb-3", null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("out of range");
  }

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

  // No synthesis outside K8s mode: the static-list contract is unchanged.
  @Test
  void noSynthesisWhenK8sDisabled() {
    final var parsed = RaftPeerAddressResolver.parsePeerList("arcadedb-0.svc:2434,arcadedb-1.svc:2434", 2434);
    assertThat(RaftPeerAddressResolver.synthesizeK8sScaleUpPeer(false, parsed.peers(), "arcadedb-3", ".svc", 2434))
        .isNull();
  }

  // An in-range ordinal must resolve through the normal index path, not be synthesized.
  @Test
  void noSynthesisForInRangeOrdinal() {
    final var parsed = RaftPeerAddressResolver.parsePeerList(
        "arcadedb-0.svc:2434,arcadedb-1.svc:2434,arcadedb-2.svc:2434", 2434);
    assertThat(RaftPeerAddressResolver.synthesizeK8sScaleUpPeer(true, parsed.peers(), "arcadedb-1", ".svc", 2434))
        .isNull();
  }

  // A name with no parseable -N/_N ordinal cannot be a StatefulSet scale-up pod: no synthesis.
  @Test
  void noSynthesisWhenNameHasNoOrdinal() {
    final var parsed = RaftPeerAddressResolver.parsePeerList("arcadedb-0.svc:2434,arcadedb-1.svc:2434", 2434);
    assertThat(RaftPeerAddressResolver.synthesizeK8sScaleUpPeer(true, parsed.peers(), "arcadedb", ".svc", 2434))
        .isNull();
  }

  // Empty DNS suffix (non-DNS K8s setups) still synthesizes a usable bare-host peer.
  @Test
  void synthesizesWithEmptyDnsSuffix() {
    final var parsed = RaftPeerAddressResolver.parsePeerList("arcadedb-0:2434,arcadedb-1:2434", 2434);
    final RaftPeer synth = RaftPeerAddressResolver.synthesizeK8sScaleUpPeer(
        true, parsed.peers(), "arcadedb-2", "", 2434);
    assertThat(synth).isNotNull();
    assertThat(synth.getAddress()).isEqualTo("arcadedb-2:2434");
  }
}
