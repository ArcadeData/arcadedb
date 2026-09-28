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

import java.util.Arrays;

/**
 * What the runner has done to each node. Only the runner thread reads or writes it. A fault may impair at most a
 * minority of nodes, counting nodes already impaired, so the cluster always keeps a possible majority.
 */
public final class ClusterState {
  public enum NodeState {UP, DOWN, PAUSED, ISOLATED, DEGRADED}

  private final NodeState[] states;

  public ClusterState(final int nodes) {
    states = new NodeState[nodes];
    Arrays.fill(states, NodeState.UP);
  }

  public int size() {
    return states.length;
  }

  public int minority() {
    return (states.length - 1) / 2;
  }

  public NodeState state(final int node) {
    return states[node];
  }

  public void set(final int node, final NodeState state) {
    states[node] = state;
  }

  public int impairedCount() {
    int count = 0;
    for (final NodeState state : states)
      if (state != NodeState.UP)
        ++count;
    return count;
  }

  public boolean canImpair(final int additional) {
    return impairedCount() + additional <= minority();
  }

  @Override
  public String toString() {
    return Arrays.toString(states);
  }
}
