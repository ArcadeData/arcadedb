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

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.util.SizeInBytes;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the pure-function helpers in {@link KubernetesAutoJoin}. The {@code tryAutoJoin}
 * network path requires a running Raft cluster and is exercised separately via integration
 * tests; this class covers the two helpers that are testable in isolation:
 * <ul>
 *   <li>{@link KubernetesAutoJoin#computeJitterMinMs(String)} - ordinal-derived jitter window</li>
 *   <li>{@link KubernetesAutoJoin#buildProbePropertiesForTest()} - TLS / flow-control inheritance</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class KubernetesAutoJoinHelpersTest {

  private static final long FALLBACK_MIN_MS    = 500L;
  private static final long ORDINAL_SLOT_MS    = 500L;
  private static final long JITTER_MAX_CAP_MS  = 2_900L; // AUTO_JOIN_JITTER_MAX_MS - 100

  // -- computeJitterMinMs --

  @Test
  void jitterMinFallsBackWhenHostnameUnset() {
    assertThat(KubernetesAutoJoin.computeJitterMinMs(null)).isEqualTo(FALLBACK_MIN_MS);
  }

  @Test
  void jitterMinFallsBackWhenHostnameHasNoDash() {
    assertThat(KubernetesAutoJoin.computeJitterMinMs("arcadedb")).isEqualTo(FALLBACK_MIN_MS);
  }

  @Test
  void jitterMinFallsBackWhenHostnameEndsWithDash() {
    // "pod-" → substring after last dash is empty → NFE → fallback.
    assertThat(KubernetesAutoJoin.computeJitterMinMs("pod-")).isEqualTo(FALLBACK_MIN_MS);
  }

  @Test
  void jitterMinFallsBackWhenOrdinalIsNotNumeric() {
    assertThat(KubernetesAutoJoin.computeJitterMinMs("arcadedb-abc")).isEqualTo(FALLBACK_MIN_MS);
  }

  @Test
  void jitterMinFallsBackWhenOrdinalIsNegative() {
    // Integer.parseInt("-1") succeeds → ordinal = -1 → condition (ordinal >= 0) false → fallback.
    assertThat(KubernetesAutoJoin.computeJitterMinMs("arcadedb--1")).isEqualTo(FALLBACK_MIN_MS);
  }

  @Test
  void jitterMinForOrdinalZeroIsZero() {
    assertThat(KubernetesAutoJoin.computeJitterMinMs("arcadedb-0")).isZero();
  }

  @Test
  void jitterMinScalesWithOrdinal() {
    assertThat(KubernetesAutoJoin.computeJitterMinMs("arcadedb-1")).isEqualTo(ORDINAL_SLOT_MS);
    assertThat(KubernetesAutoJoin.computeJitterMinMs("arcadedb-3")).isEqualTo(3 * ORDINAL_SLOT_MS);
    assertThat(KubernetesAutoJoin.computeJitterMinMs("arcadedb-5")).isEqualTo(5 * ORDINAL_SLOT_MS);
  }

  @Test
  void jitterMinClampsAtMaxCap() {
    // Ordinal high enough to push past the cap; expect clamp to MAX - 100 = 2900.
    assertThat(KubernetesAutoJoin.computeJitterMinMs("arcadedb-100")).isEqualTo(JITTER_MAX_CAP_MS);
  }

  @Test
  void jitterMinStripsOnlyTheLastDashSegment() {
    // "my-service-4" → ordinal is 4 (after LAST dash), not the whole "service-4".
    assertThat(KubernetesAutoJoin.computeJitterMinMs("my-service-4")).isEqualTo(4 * ORDINAL_SLOT_MS);
  }

  // -- buildProbeProperties (TLS / flow-control inheritance contract) --

  @Test
  void buildProbePropertiesInheritsCallerSettings() {
    // Set a distinctive value on the input properties; the probe must preserve it.
    final RaftProperties input = new RaftProperties();
    GrpcConfigKeys.setFlowControlWindow(input, SizeInBytes.valueOf("8MB"));
    // An arbitrary unrelated key to prove wholesale inheritance, not a whitelist copy.
    input.set("arcadedb.test.inheritance.marker", "MUST_SURVIVE");

    final KubernetesAutoJoin autoJoin = new KubernetesAutoJoin(null, dummyRaftGroup(), dummyPeerId(), input);
    final RaftProperties probe = autoJoin.buildProbePropertiesForTest();

    assertThat(GrpcConfigKeys.flowControlWindow(probe, s -> {}).getSize())
        .isEqualTo(SizeInBytes.valueOf("8MB").getSize());
    assertThat(probe.get("arcadedb.test.inheritance.marker")).isEqualTo("MUST_SURVIVE");
  }

  @Test
  void buildProbePropertiesForcesGrpcRpcType() {
    final RaftProperties input = new RaftProperties();
    input.set("raft.server.rpc.type", "SOMETHING_ELSE");
    final KubernetesAutoJoin autoJoin = new KubernetesAutoJoin(null, dummyRaftGroup(), dummyPeerId(), input);
    final RaftProperties probe = autoJoin.buildProbePropertiesForTest();
    assertThat(probe.get("raft.server.rpc.type")).isEqualTo("GRPC");
  }

  @Test
  void buildProbePropertiesOverridesShortTimeouts() {
    // Start with a long production-style election timeout; probe must override with short ones.
    final RaftProperties input = new RaftProperties();
    RaftServerConfigKeys.Rpc.setTimeoutMin(input,
        org.apache.ratis.util.TimeDuration.valueOf(30, java.util.concurrent.TimeUnit.SECONDS));
    RaftServerConfigKeys.Rpc.setTimeoutMax(input,
        org.apache.ratis.util.TimeDuration.valueOf(60, java.util.concurrent.TimeUnit.SECONDS));

    final KubernetesAutoJoin autoJoin = new KubernetesAutoJoin(null, dummyRaftGroup(), dummyPeerId(), input);
    final RaftProperties probe = autoJoin.buildProbePropertiesForTest();

    final long minSec = RaftServerConfigKeys.Rpc.timeoutMin(probe).toLong(java.util.concurrent.TimeUnit.SECONDS);
    final long maxSec = RaftServerConfigKeys.Rpc.timeoutMax(probe).toLong(java.util.concurrent.TimeUnit.SECONDS);
    assertThat(minSec).as("probe min timeout should be in the 3-5 s range").isBetween(3L, 5L);
    assertThat(maxSec).as("probe max timeout should be in the 3-5 s range").isBetween(3L, 5L);
  }

  @Test
  void buildProbePropertiesReturnsDistinctInstance() {
    // Mutating the probe properties must not leak back into the server's main properties.
    final RaftProperties input = new RaftProperties();
    final KubernetesAutoJoin autoJoin = new KubernetesAutoJoin(null, dummyRaftGroup(), dummyPeerId(), input);
    final RaftProperties probe = autoJoin.buildProbePropertiesForTest();
    probe.set("arcadedb.test.mutation.check", "probe-only");
    assertThat(input.get("arcadedb.test.mutation.check")).isNull();
  }

  private static RaftGroup dummyRaftGroup() {
    // A peer list with the local peer only is sufficient - tryAutoJoin is not invoked here.
    final RaftPeer self = RaftPeer.newBuilder().setId(RaftPeerId.valueOf("local_2424"))
        .setAddress("localhost:2424").build();
    return RaftGroup.valueOf(RaftGroupId.valueOf(UUID.randomUUID()), List.of(self));
  }

  private static RaftPeerId dummyPeerId() {
    return RaftPeerId.valueOf("local_2424");
  }
}
