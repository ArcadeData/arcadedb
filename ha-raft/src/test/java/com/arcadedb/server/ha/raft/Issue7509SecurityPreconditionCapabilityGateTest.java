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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7509's rolling-upgrade guard: the compare-and-set precondition is written only while EVERY peer has
 * proved it can read one.
 * <p>
 * Without the gate a mixed cluster is worse off than an ungated one, not better. A peer that predates the
 * section skips it and installs the document unconditionally, so a losing entry would be REFUSED on the
 * upgraded nodes and APPLIED on the older one: the security state diverges, and the same credentials then
 * resolve differently depending on which node answers - the failure #7373 exists to prevent. Withholding the
 * precondition keeps the pre-#7509 behaviour uniform instead, and the check resumes by itself once the last
 * node is upgraded.
 */
class Issue7509SecurityPreconditionCapabilityGateTest {

  private static final String FINGERPRINT = "0f".repeat(32);

  @Test
  void thisBuildAdvertisesThatItReadsASecurityPrecondition() {
    // Without this a cluster of nodes that can all read one would never agree to write one.
    assertThat(PeerCapabilities.LOCAL).contains(PeerCapabilities.SECURITY_PRECONDITION);
  }

  @Test
  void theFingerprintIsWrittenWhenNoPeerIsMissingTheCapability() {
    assertThat(new RaftHAPlugin().preconditionForPeers(FINGERPRINT, List.of())).isEqualTo(FINGERPRINT);
  }

  @Test
  void theFingerprintIsWithheldWhileAnyPeerIsMissingTheCapability() {
    assertThat(new RaftHAPlugin().preconditionForPeers(FINGERPRINT, List.of("arcadedb2"))).isNull();
  }

  /** A seed carries no fingerprint to begin with, gate or no gate. */
  @Test
  void aSubmissionWithNoPreconditionStaysWithoutOne() {
    assertThat(new RaftHAPlugin().preconditionForPeers(null, List.of())).isNull();
  }

  /**
   * What "missing" means is the registry's answer, and it is fail-closed: a peer that is unknown - never probed,
   * unreachable, or probed and stale - counts as missing exactly like one that answered without the token. That is
   * what makes the gate safe without an operator sequencing the upgrade by hand.
   */
  @Test
  void anUnknownPeerCountsAsMissingTheCapability() {
    final PeerCapabilityRegistry registry = new PeerCapabilityRegistry();
    final long generation = registry.generation();
    registry.record(generation, "arcadedb0", Set.of(PeerCapabilities.SECURITY_PRECONDITION), "26.10.1");

    assertThat(registry.peersMissing(List.of("arcadedb0"), PeerCapabilities.SECURITY_PRECONDITION)).isEmpty();
    assertThat(registry.peersMissing(List.of("arcadedb0", "arcadedb1"), PeerCapabilities.SECURITY_PRECONDITION))
        .as("a peer that has never advertised anything must not be credited with the capability")
        .containsExactly("arcadedb1");
  }

  @Test
  void aPeerThatAdvertisesWithoutTheTokenCountsAsMissingIt() {
    final PeerCapabilityRegistry registry = new PeerCapabilityRegistry();
    final long generation = registry.generation();
    registry.record(generation, "arcadedb1", Set.of(PeerCapabilities.SCHEMA_DELTA), "26.9.1");

    assertThat(registry.peersMissing(List.of("arcadedb1"), PeerCapabilities.SECURITY_PRECONDITION))
        .containsExactly("arcadedb1");
  }
}
