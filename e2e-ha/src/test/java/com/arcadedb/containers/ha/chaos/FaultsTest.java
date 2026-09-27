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

package com.arcadedb.containers.ha.chaos;

import com.arcadedb.containers.ha.chaos.ClusterState.NodeState;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FaultsTest {

  private static int firstIn(final ClusterState state, final NodeState wanted) {
    for (int i = 0; i < state.size(); i++)
      if (state.state(i) == wanted)
        return i;
    return -1;
  }

  @Test
  void minorityRule() {
    final ClusterState three = new ClusterState(3);
    assertThat(three.minority()).isEqualTo(1);
    assertThat(three.canImpair(1)).isTrue();
    assertThat(three.canImpair(2)).isFalse();
    three.set(0, NodeState.DOWN);
    assertThat(three.canImpair(1)).isFalse();

    final ClusterState five = new ClusterState(5);
    assertThat(five.minority()).isEqualTo(2);
    assertThat(five.canImpair(2)).isTrue();
    assertThat(five.canImpair(3)).isFalse();
  }

  @Test
  void killTakesOneNodeDownOnThreeNodesAndHealRestartsIt() throws Exception {
    for (long seed = 0; seed < 50; seed++) {
      final ClusterState state = new ClusterState(3);
      final FakeNodeControl control = new FakeNodeControl();
      final NodeFault fault = new NodeFault(NodeFault.Kind.KILL);
      fault.inject(state, control, new Random(seed));
      assertThat(state.impairedCount()).isEqualTo(1);
      final int down = firstIn(state, NodeState.DOWN);
      assertThat(control.calls).contains("kill:" + down);
      fault.heal(state, control);
      assertThat(state.impairedCount()).isZero();
      assertThat(control.calls).contains("start:" + down);
    }
  }

  @Test
  void fiveNodesNeverLoseTheMajority() throws Exception {
    boolean sawTwo = false;
    for (long seed = 0; seed < 200; seed++)
      for (final NodeFault.Kind kind : NodeFault.Kind.values()) {
        final ClusterState state = new ClusterState(5);
        final NodeFault fault = new NodeFault(kind);
        fault.inject(state, new FakeNodeControl(), new Random(seed));
        assertThat(state.impairedCount()).isBetween(1, 2);
        sawTwo |= state.impairedCount() == 2;
        fault.heal(state, new FakeNodeControl());
        assertThat(state.impairedCount()).isZero();
      }
    assertThat(sawTwo).isTrue();
  }

  @Test
  void leaderRoleTargetsTheLeaderAndFollowerRoleNeverDoes() throws Exception {
    boolean sawLeader = false;
    boolean sawFollower = false;
    for (long seed = 0; seed < 100; seed++) {
      final ClusterState state = new ClusterState(3);
      final FakeNodeControl control = new FakeNodeControl();
      control.leader = 2;
      // Spread the seeds: java.util.Random's first nextBoolean() is true for every small sequential seed
      final String description = new NodeFault(NodeFault.Kind.PAUSE).inject(state, control, new Random(seed * 0x9E3779B97F4A7C15L));
      if (description.startsWith("LEADER")) {
        assertThat(state.state(2)).isEqualTo(NodeState.PAUSED);
        sawLeader = true;
      } else {
        assertThat(state.state(2)).isEqualTo(NodeState.UP);
        sawFollower = true;
      }
    }
    assertThat(sawLeader).isTrue();
    assertThat(sawFollower).isTrue();
  }

  @Test
  void splitIsolatesTheLeaderSideMinority() throws Exception {
    final ClusterState five = new ClusterState(5);
    final FakeNodeControl control = new FakeNodeControl();
    control.leader = 4;
    final SplitFault split = new SplitFault();
    split.inject(five, control, new Random(1));
    assertThat(five.state(4)).isEqualTo(NodeState.ISOLATED);
    assertThat(five.impairedCount()).isEqualTo(2);
    split.heal(five, control);
    assertThat(control.calls).contains("reconnect:4");
    assertThat(five.impairedCount()).isZero();

    final ClusterState three = new ClusterState(3);
    control.leader = 1;
    new SplitFault().inject(three, control, new Random(1));
    assertThat(three.impairedCount()).isEqualTo(1);
    assertThat(three.state(1)).isEqualTo(NodeState.ISOLATED);
  }

  @Test
  void rollingRestartRestartsEveryNodeInOrder() throws Exception {
    final ClusterState state = new ClusterState(3);
    final FakeNodeControl control = new FakeNodeControl();
    new RollingRestartFault(Duration.ofSeconds(1)).inject(state, control, new Random(1));
    assertThat(control.calls).containsSubsequence("stop:0", "start:0", "awaitLeader", "stop:1", "start:1", "awaitLeader",
        "stop:2", "start:2", "awaitLeader");
    assertThat(state.impairedCount()).isZero();
  }

  @Test
  void rollingRestartWithoutALeaderIsAnAvailabilityFailure() {
    final FakeNodeControl control = new FakeNodeControl();
    control.leaderAvailable = false;
    assertThatThrownBy(() -> new RollingRestartFault(Duration.ofSeconds(1)).inject(new ClusterState(3), control, new Random(1)))
        .isInstanceOfSatisfying(ChaosFailure.class, e -> assertThat(e.kind()).isEqualTo(ResultKind.AVAILABILITY));
  }

  @Test
  void latencyStaysWithinBoundsAndHealClearsToxics() throws Exception {
    for (long seed = 0; seed < 50; seed++) {
      final ClusterState state = new ClusterState(3);
      final FakeNodeControl control = new FakeNodeControl();
      final ToxicFault fault = new ToxicFault(ToxicFault.Variant.LATENCY);
      fault.inject(state, control, new Random(seed));
      final String[] call = control.calls.getFirst().split(":");
      assertThat(call[0]).isEqualTo("latency");
      assertThat(Integer.parseInt(call[2])).isBetween(200, 2000);
      assertThat(state.state(Integer.parseInt(call[1]))).isEqualTo(NodeState.DEGRADED);
      fault.heal(state, control);
      assertThat(control.calls).contains("clearToxics:" + call[1]);
      assertThat(state.impairedCount()).isZero();
    }
  }

  @Test
  void lossToxicityStaysWithinBounds() throws Exception {
    for (long seed = 0; seed < 50; seed++) {
      final FakeNodeControl control = new FakeNodeControl();
      new ToxicFault(ToxicFault.Variant.LOSS).inject(new ClusterState(3), control, new Random(seed));
      final float toxicity = Float.parseFloat(control.calls.getFirst().split(":")[2]);
      assertThat(toxicity).isBetween(0.05f, 0.20f);
    }
  }

  private static List<String> script(final long seed) throws Exception {
    final Random random = new Random(seed);
    final ClusterState state = new ClusterState(5);
    final FakeNodeControl control = new FakeNodeControl();
    control.leader = 1;
    final FaultPicker picker = new FaultPicker(ChaosConfig.parseFaults(""), Duration.ofSeconds(1));
    for (int step = 0; step < 30; step++) {
      final Fault fault = picker.pick(state, random);
      control.calls.add("fault:" + fault.name());
      fault.inject(state, control, random);
      fault.heal(state, control);
    }
    return control.calls;
  }

  @Test
  void sameSeedSameDecisions() throws Exception {
    assertThat(script(42)).isEqualTo(script(42));
    assertThat(script(42)).isNotEqualTo(script(43));
  }

  @Test
  void weightsAreRespected() {
    final FaultPicker picker = new FaultPicker(ChaosConfig.parseFaults("kill:9,pause:1"), Duration.ofSeconds(1));
    final ClusterState state = new ClusterState(3);
    final Random random = new Random(7);
    int kills = 0;
    for (int i = 0; i < 10_000; i++)
      if (picker.pick(state, random).name().equals("kill"))
        ++kills;
    assertThat(kills).isBetween(8_500, 9_500);
  }

  @Test
  void noApplicableFaultReturnsNull() {
    final FaultPicker picker = new FaultPicker(ChaosConfig.parseFaults("split"), Duration.ofSeconds(1));
    final ClusterState state = new ClusterState(3);
    state.set(0, NodeState.DOWN);
    assertThat(picker.pick(state, new Random(1))).isNull();
  }
}
