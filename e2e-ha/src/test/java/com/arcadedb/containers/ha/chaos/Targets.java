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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * Target selection shared by the faults. Every method consumes the random the same way for a given cluster size, so
 * one seed yields one sequence of decisions whoever the leader happens to be.
 */
final class Targets {
  enum Role {LEADER, FOLLOWER, ANY}

  private Targets() {
  }

  static Role role(final Random random) {
    return random.nextBoolean() ? Role.LEADER : Role.FOLLOWER;
  }

  /** One node, or two when the random asks for two and the cluster can afford it. */
  static int count(final ClusterState state, final Random random) {
    final boolean wantTwo = random.nextBoolean();
    return wantTwo && state.canImpair(2) ? 2 : 1;
  }

  static int[] pick(final ClusterState state, final NodeControl control, final Random random, final Role role,
      final int count) {
    final List<Integer> candidates = new ArrayList<>();
    for (int i = 0; i < state.size(); i++)
      if (state.state(i) == NodeState.UP)
        candidates.add(i);
    Collections.shuffle(candidates, random);

    final int leader = role == Role.ANY ? -1 : control.findLeader();
    if (leader >= 0 && candidates.remove(Integer.valueOf(leader)) && role == Role.LEADER)
      candidates.addFirst(leader);

    if (candidates.size() < count)
      throw new ChaosFailure(ResultKind.HARNESS, "Need " + count + " " + role + " targets, UP candidates: " + candidates);
    final int[] targets = new int[count];
    for (int i = 0; i < count; i++)
      targets[i] = candidates.get(i);
    return targets;
  }
}
