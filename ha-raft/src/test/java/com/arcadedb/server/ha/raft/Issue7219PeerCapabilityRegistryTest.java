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

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7219: the leader may write an optional wire-format section only when EVERY peer has proved it can
 * decode it. This pins the arms of that decision.
 * <p>
 * The arms are pinned here rather than in the cluster, for the same reason
 * {@code RaftReplicatedDatabase.baseIsUsable} is: every one of them fails SILENTLY. A registry that answers
 * "capable" for a peer it has never heard of does not break replication - it makes the leader write a section the
 * peer cannot read, which shows up later as a schema that quietly stopped matching. Nothing in an integration
 * test goes red on that.
 */
class Issue7219PeerCapabilityRegistryTest {

  private static final String CAP   = PeerCapabilities.SCHEMA_DELTA;
  private static final String PEER1 = "arcadedb1";
  private static final String PEER2 = "arcadedb2";

  private final AtomicLong             now      = new AtomicLong(1_000_000L);
  private final PeerCapabilityRegistry registry = newRegistry();

  private PeerCapabilityRegistry newRegistry() {
    final PeerCapabilityRegistry created = new PeerCapabilityRegistry(PeerCapabilityRegistry.ADVERTISEMENT_TTL_MS);
    created.setClock(now::get);
    return created;
  }

  @Test
  void aClusterWithNoOtherPeerIsCapable() {
    // A single-node cluster has nobody who could fail to understand the bytes. Vacuously true, and it has to be
    // true or a one-node deployment would never use any optional section.
    assertThat(registry.allPeersSupport(List.of(), CAP)).isTrue();
    assertThat(registry.peersMissing(List.of(), CAP)).isEmpty();
  }

  @Test
  void anUnknownPeerBlocksTheCapability() {
    // Never probed: a peer just added to the configuration, or one whose address PeerDialAddress refuses to
    // hand out because it identifies no single node (#6202). Both arrive here as "no entry".
    assertThat(registry.allPeersSupport(List.of(PEER1), CAP)).isFalse();
    assertThat(registry.peersMissing(List.of(PEER1), CAP)).containsExactly(PEER1);
  }

  @Test
  void aPeerThatAdvertisedTheCapabilityIsCapable() {
    registry.record(registry.generation(), PEER1, Set.of(CAP), "26.10.1");

    assertThat(registry.allPeersSupport(List.of(PEER1), CAP)).isTrue();
    assertThat(registry.freshAdvertisementOf(PEER1).version()).isEqualTo("26.10.1");
  }

  @Test
  void aPeerThatAnsweredWithoutTheCapabilityBlocksIt() {
    // A build that HAS the capability endpoint but does not decode this particular section - the shape every
    // future capability will arrive in, and the one a version comparison would get wrong.
    registry.record(registry.generation(), PEER1, Set.of("some-other-capability"), "26.10.1");

    assertThat(registry.allPeersSupport(List.of(PEER1), CAP)).isFalse();
    assertThat(registry.peersMissing(List.of(PEER1), CAP)).containsExactly(PEER1);
  }

  @Test
  void oneIncapablePeerBlocksTheWholeCluster() {
    registry.record(registry.generation(), PEER1, Set.of(CAP), "26.10.1");

    assertThat(registry.allPeersSupport(List.of(PEER1, PEER2), CAP))
        .as("PEER2 has never answered, so the cluster is not covered even though PEER1 is")
        .isFalse();
    assertThat(registry.peersMissing(List.of(PEER1, PEER2), CAP)).containsExactly(PEER2);
  }

  @Test
  void anExpiredAdvertisementBlocksTheCapability() {
    registry.record(registry.generation(), PEER1, Set.of(CAP), "26.10.1");
    assertThat(registry.allPeersSupport(List.of(PEER1), CAP)).isTrue();

    // Still inside the window: several missed refresh rounds are absorbed on purpose, so a GC pause on one peer
    // does not cost the whole cluster its deltas.
    now.addAndGet(PeerCapabilityRegistry.ADVERTISEMENT_TTL_MS);
    assertThat(registry.allPeersSupport(List.of(PEER1), CAP))
        .as("an answer exactly at the TTL boundary is still believed")
        .isTrue();

    // Past it: a peer rolled back to an older build sits behind the same id, and this is the bound on how long
    // the leader keeps writing sections it can no longer read.
    now.addAndGet(1L);
    assertThat(registry.allPeersSupport(List.of(PEER1), CAP)).isFalse();
    assertThat(registry.freshAdvertisementOf(PEER1)).isNull();
  }

  @Test
  void aPeerThatLostTheCapabilityBlocksItAgain() {
    registry.record(registry.generation(), PEER1, Set.of(CAP), "26.10.1");
    assertThat(registry.allPeersSupport(List.of(PEER1), CAP)).isTrue();

    // What a failed probe does - a 404 from a build without the route, or an unreachable peer. Forgetting rather
    // than keeping matters: the peer may have been replaced by an older build, and believing its last answer
    // until the TTL ran out would be believing it for a reason that no longer holds.
    registry.forget(registry.generation(), PEER1, "the probe failed");

    assertThat(registry.allPeersSupport(List.of(PEER1), CAP)).isFalse();
  }

  @Test
  void aPeerOutsideTheConfigurationDoesNotBlock() {
    registry.record(registry.generation(), PEER1, Set.of(CAP), "26.10.1");

    // PEER2 was never asked about, because the question is only ever about the peers the leader replicates to.
    assertThat(registry.allPeersSupport(List.of(PEER1), CAP)).isTrue();
  }

  @Test
  void retainOnlyDropsPeersNoLongerInTheConfiguration() {
    registry.record(registry.generation(), PEER1, Set.of(CAP), "26.10.1");
    registry.record(registry.generation(), PEER2, Set.of(CAP), "26.10.1");

    registry.retainOnly(registry.generation(), List.of(PEER1));

    assertThat(registry.freshAdvertisementOf(PEER1)).isNotNull();
    assertThat(registry.freshAdvertisementOf(PEER2))
        .as("a peer the configuration dropped must not keep an advertisement for the leader's whole uptime")
        .isNull();
  }

  @Test
  void aRecordedCapabilitySetCannotBeMutatedByItsCaller() {
    // The refresh loop parses into its own set; a recorded answer that shared that set could change under the
    // gate between two DDLs.
    final Set<String> parsed = new java.util.LinkedHashSet<>(Set.of(CAP));
    registry.record(registry.generation(), PEER1, parsed, "26.10.1");
    parsed.clear();

    assertThat(registry.allPeersSupport(List.of(PEER1), CAP)).isTrue();
  }

  @Test
  void everyMissingPeerIsNamed() {
    // peersMissing is not just a boolean in disguise: it is what the operator log names, and "which node is
    // holding the cluster back" is the only actionable half of the answer.
    registry.record(registry.generation(), PEER1, Set.of(CAP), "26.10.1");

    assertThat(registry.peersMissing(List.of(PEER1, PEER2, "arcadedb3"), CAP))
        .containsExactly(PEER2, "arcadedb3");
  }
}
