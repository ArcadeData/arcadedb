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

import java.util.Arrays;
import java.util.Random;

/**
 * Cuts the leader plus enough followers to form the largest minority off the network, forcing an election on the
 * majority side. On 3 nodes this is the leader alone. The disconnected nodes cannot reach each other either.
 */
public final class SplitFault implements Fault {
  private int[] targets = new int[0];

  @Override
  public String name() {
    return "split";
  }

  @Override
  public boolean canApply(final ClusterState state) {
    return state.impairedCount() == 0;
  }

  @Override
  public String inject(final ClusterState state, final NodeControl control, final Random random) throws Exception {
    targets = Targets.pick(state, control, random, Targets.Role.LEADER, state.minority());
    for (final int node : targets) {
      control.disconnect(node);
      state.set(node, NodeState.ISOLATED);
    }
    return "LEADER-side minority " + Arrays.toString(targets);
  }

  @Override
  public void heal(final ClusterState state, final NodeControl control) throws Exception {
    for (final int node : targets) {
      control.reconnect(node);
      state.set(node, NodeState.UP);
    }
    targets = new int[0];
  }

  @Override
  public boolean expectsWritesAvailable() {
    return true;
  }
}
