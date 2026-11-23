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
import com.arcadedb.server.ha.raft.ratis.ApplyBackPressure;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11508: the apply back-pressure of issue #11458 protects the FOLLOWER's heap, so its budget cannot come from the
 * leader's heap alone. Each node advertises the budget it resolved for itself on the capability poll, and the leader
 * holds entries back from a follower at the smaller of its own budget and the one that follower advertised.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11508FollowerApplyBudgetTest {

  private static final long MB = 1L << 20;

  // The leader of the issue: an 8GB heap gives a 2GB budget; the follower runs with 1GB, so its own budget is 256MB
  private static final long LEADER_BUDGET   = 2048 * MB;
  private static final long FOLLOWER_BUDGET = 256 * MB;

  @Test
  void aFollowerWithASmallerBudgetIsPausedAtItsOwnBudget() {
    final ApplyBackPressure backPressure = new ApplyBackPressure(LEADER_BUDGET, peer -> 0L,
        peer -> "small".equals(peer) ? FOLLOWER_BUDGET : -1L);

    // 10k unapplied entries of 59KB are about 560MB: inside the leader's 2GB, more than twice the follower's 256MB
    assertThat(backPressure.shouldPause("small", 10_000L, 59_000L)).as("over the follower's own budget").isTrue();
    assertThat(backPressure.shouldPause("legacy", 10_000L, 59_000L)).as("a peer that advertises no budget: the leader's").isFalse();
    assertThat(backPressure.shouldPause("small", 4_000L, 59_000L)).as("226MB, inside the follower's budget").isFalse();
    assertThat(backPressure.shouldPause("legacy", 40_000L, 59_000L)).as("2.2GB, over the leader's budget").isTrue();
  }

  @Test
  void aFollowerWithALargerBudgetIsStillBoundByTheLeaders() {
    final ApplyBackPressure backPressure = new ApplyBackPressure(FOLLOWER_BUDGET, peer -> 0L, peer -> LEADER_BUDGET);
    assertThat(backPressure.shouldPause("big", 10_000L, 59_000L)).isTrue();
    assertThat(backPressure.effectiveBudgetBytes("big")).isEqualTo(FOLLOWER_BUDGET);
  }

  @Test
  void theEffectiveBudgetIsTheSmallerOfTheTwoAndTheLeadersWhenTheFollowerSaysNothing() {
    final ApplyBackPressure backPressure = new ApplyBackPressure(LEADER_BUDGET, peer -> 0L,
        peer -> "small".equals(peer) ? FOLLOWER_BUDGET : "zero".equals(peer) ? 0L : -1L);
    assertThat(backPressure.effectiveBudgetBytes("small")).isEqualTo(FOLLOWER_BUDGET);
    assertThat(backPressure.effectiveBudgetBytes("legacy")).isEqualTo(LEADER_BUDGET);
    assertThat(backPressure.effectiveBudgetBytes("zero")).as("0 is not a budget a follower can impose").isEqualTo(LEADER_BUDGET);
  }

  @Test
  void aLeaderThatDisabledTheBackPressureNeverPausesWhateverTheFollowerAdvertises() {
    final ApplyBackPressure backPressure = new ApplyBackPressure(0L, peer -> 0L, peer -> 1L);
    assertThat(backPressure.shouldPause("small", 10_000_000L, 1_000_000L)).isFalse();
    assertThat(backPressure.effectiveBudgetBytes("small")).isZero();
  }

  @Test
  void theSingleSourceConstructorKeepsTheLeaderOnlyBehaviour() {
    final ApplyBackPressure backPressure = new ApplyBackPressure(LEADER_BUDGET, peer -> 0L);
    assertThat(backPressure.shouldPause("any", 10_000L, 59_000L)).isFalse();
    assertThat(backPressure.shouldPause("any", 40_000L, 59_000L)).isTrue();
  }

  @Test
  void advertisementCarriesTheBudgetAndItIsReadBack() throws IOException {
    final JSONObject json = PostCapabilitiesHandler.advertisement("p1", Set.of(), false, Set.of(), Map.of(), 42L, FOLLOWER_BUDGET);
    assertThat(json.getLong(PostCapabilitiesHandler.APPLY_BACKLOG_BUDGET, -1L)).isEqualTo(FOLLOWER_BUDGET);

    final PeerCapabilityQuery.Advertisement parsed = PeerCapabilityQuery.parse("p1", json.toString(), "http://x");
    assertThat(parsed.applyBacklogBudget()).isEqualTo(FOLLOWER_BUDGET);
    assertThat(parsed.appliedIndex()).isEqualTo(42L);
  }

  @Test
  void aNodeWithNoBudgetOmitsTheFieldSoItsDocumentStaysAsItWas() throws IOException {
    for (final long none : new long[] { 0L, -1L }) {
      final JSONObject json = PostCapabilitiesHandler.advertisement("p1", Set.of(), false, Set.of(), Map.of(), 42L, none);
      assertThat(json.has(PostCapabilitiesHandler.APPLY_BACKLOG_BUDGET)).isFalse();
      assertThat(json.toString()).isEqualTo(
          PostCapabilitiesHandler.advertisement("p1", Set.of(), false, Set.of(), Map.of(), 42L).toString());
      assertThat(PeerCapabilityQuery.parse("p1", json.toString(), "http://x").applyBacklogBudget()).isEqualTo(-1L);
    }
  }

  @Test
  void aPeerThatPredatesTheFieldReadsAsNoBudget() throws IOException {
    final PeerCapabilityQuery.Advertisement parsed = PeerCapabilityQuery.parse("p1",
        "{\"peerId\":\"p1\",\"version\":\"1\",\"capabilities\":[],\"appliedIndex\":7}", "http://x");
    assertThat(parsed.applyBacklogBudget()).isEqualTo(-1L);
    assertThat(parsed.appliedIndex()).isEqualTo(7L);
  }

  @Test
  void aMalformedBudgetCostsThePeerItsBudgetNeverItsAnswer() throws IOException {
    for (final String value : new String[] { "\"lots\"", "0", "-5", "true", "[1]", "{}", "null", "0.5", "\"\"" }) {
      final PeerCapabilityQuery.Advertisement parsed = PeerCapabilityQuery.parse("p1",
          "{\"peerId\":\"p1\",\"version\":\"1\",\"capabilities\":[\"schema-delta\"],\"appliedIndex\":7,\"applyBacklogBudget\":"
              + value + "}", "http://x");
      assertThat(parsed.applyBacklogBudget()).as("applyBacklogBudget: %s", value).isEqualTo(-1L);
      assertThat(parsed.capabilities()).as("applyBacklogBudget: %s", value).containsExactly("schema-delta");
      assertThat(parsed.appliedIndex()).as("applyBacklogBudget: %s", value).isEqualTo(7L);
    }
  }

  @Test
  void aBudgetPastWhatALongHoldsIsClampedAndLeavesTheLeadersBudgetInForce() throws IOException {
    for (final String value : new String[] { "1e400", "9223372036854775808", "1e19" }) {
      final long budget = PeerCapabilityQuery.parse("p1",
          "{\"peerId\":\"p1\",\"version\":\"1\",\"capabilities\":[],\"applyBacklogBudget\":" + value + "}",
          "http://x").applyBacklogBudget();
      assertThat(budget).as("applyBacklogBudget: %s", value).isEqualTo(Long.MAX_VALUE);

      final ApplyBackPressure backPressure = new ApplyBackPressure(LEADER_BUDGET, peer -> 0L, peer -> budget);
      assertThat(backPressure.effectiveBudgetBytes("p1")).isEqualTo(LEADER_BUDGET);
      assertThat(backPressure.shouldPause("p1", 40_000L, 59_000L)).as("2.2GB, over the leader's budget").isTrue();
    }
  }

  @Test
  void aFractionalBudgetIsReadAsItsWholeBytes() throws IOException {
    final PeerCapabilityQuery.Advertisement parsed = PeerCapabilityQuery.parse("p1",
        "{\"peerId\":\"p1\",\"version\":\"1\",\"capabilities\":[],\"applyBacklogBudget\":1048576.9}", "http://x");
    assertThat(parsed.applyBacklogBudget()).isEqualTo(MB);
  }

  @Test
  void registryServesTheBudgetOnlyWhileFresh() {
    final long[] now = { 0L };
    final PeerCapabilityRegistry registry = new PeerCapabilityRegistry(1_000L);
    registry.setClock(() -> now[0]);
    final long generation = registry.generation();
    registry.record(generation, "small", Set.of(), "1", false, Set.of(), 77L, FOLLOWER_BUDGET);
    registry.record(generation, "legacy", Set.of(), "1", false, Set.of(), 77L);

    assertThat(registry.freshApplyBacklogBudgetOf("small")).isEqualTo(FOLLOWER_BUDGET);
    assertThat(registry.freshApplyBacklogBudgetOf("legacy")).isEqualTo(-1L);
    assertThat(registry.freshApplyBacklogBudgetOf("unknown")).isEqualTo(-1L);
    now[0] = 5_000L;
    assertThat(registry.freshApplyBacklogBudgetOf("small")).isEqualTo(-1L);
  }

  /** The wiring RaftHAServer makes: both figures of a follower come from what it last advertised. */
  @Test
  void theLeaderSizesEachFollowerFromWhatItAdvertised() throws IOException {
    final PeerCapabilityRegistry registry = new PeerCapabilityRegistry();
    final ApplyBackPressure backPressure = new ApplyBackPressure(LEADER_BUDGET, registry::freshAppliedIndexOf,
        registry::freshApplyBacklogBudgetOf);

    final long generation = registry.generation();
    for (final String peer : new String[] { "small", "same" }) {
      final long budget = "small".equals(peer) ? FOLLOWER_BUDGET : LEADER_BUDGET;
      final PeerCapabilityQuery.Advertisement advertisement = PeerCapabilityQuery.parse(peer,
          PostCapabilitiesHandler.advertisement(peer, Set.of(), false, Set.of(), Map.of(), 0L, budget).toString(), "http://x");
      registry.record(generation, peer, advertisement.capabilities(), advertisement.version(), advertisement.serviceGap(),
          advertisement.quarantined(), advertisement.appliedIndex(), advertisement.applyBacklogBudget());
    }

    assertThat(backPressure.shouldPause("small", 10_000L, 59_000L)).as("the follower with the smaller heap").isTrue();
    assertThat(backPressure.shouldPause("same", 10_000L, 59_000L)).as("the follower sized like the leader").isFalse();
  }
}
