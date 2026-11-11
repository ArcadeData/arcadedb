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
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11458: the capability poll carries the peer's applied Raft index, which is how the leader learns a follower's
 * apply backlog (Ratis replies carry only the commit index).
 */
class PostCapabilitiesAppliedIndexTest {

  @Test
  void advertisementCarriesTheAppliedIndex() throws IOException {
    final JSONObject json = PostCapabilitiesHandler.advertisement("p1", Set.of(), false, Set.of(), Map.of(), 4242L);
    assertThat(json.getLong(PostCapabilitiesHandler.APPLIED_INDEX, -1L)).isEqualTo(4242L);

    final PeerCapabilityQuery.Advertisement parsed = PeerCapabilityQuery.parse("p1", json.toString(), "http://x");
    assertThat(parsed.appliedIndex()).isEqualTo(4242L);
  }

  @Test
  void unknownAppliedIndexIsOmittedAndReadBackAsUnknown() throws IOException {
    final JSONObject json = PostCapabilitiesHandler.advertisement("p1", Set.of(), false, Set.of(), Map.of(), -1L);
    assertThat(json.has(PostCapabilitiesHandler.APPLIED_INDEX)).isFalse();
    assertThat(PeerCapabilityQuery.parse("p1", json.toString(), "http://x").appliedIndex()).isEqualTo(-1L);
  }

  @Test
  void aPeerThatPredatesTheFieldReadsAsUnknown() throws IOException {
    final PeerCapabilityQuery.Advertisement parsed = PeerCapabilityQuery.parse("p1",
        "{\"peerId\":\"p1\",\"version\":\"1\",\"capabilities\":[]}", "http://x");
    assertThat(parsed.appliedIndex()).isEqualTo(-1L);
  }

  @Test
  void registryServesTheAppliedIndexOnlyWhileFresh() {
    final long[] now = { 0L };
    final PeerCapabilityRegistry registry = new PeerCapabilityRegistry(1_000L);
    registry.setClock(() -> now[0]);
    final long generation = registry.generation();
    registry.record(generation, "p1", Set.of(), "1", false, Set.of(), 77L);
    assertThat(registry.freshAppliedIndexOf("p1")).isEqualTo(77L);
    assertThat(registry.freshAppliedIndexOf("unknown")).isEqualTo(-1L);
    now[0] = 5_000L;
    assertThat(registry.freshAppliedIndexOf("p1")).as("a stale report must not hold a follower back").isEqualTo(-1L);
  }
}
