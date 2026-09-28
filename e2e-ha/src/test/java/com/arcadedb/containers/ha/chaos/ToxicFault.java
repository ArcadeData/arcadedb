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
 * Degrades the outbound Raft traffic of one node (two on 5 nodes) through its Toxiproxy proxy: 200-2000 ms latency with 25%
 * jitter, or connection drops with 5-20% probability.
 */
public final class ToxicFault implements Fault {
  public enum Variant {LATENCY, LOSS}

  private final Variant variant;
  private       int[]   targets = new int[0];

  public ToxicFault(final Variant variant) {
    this.variant = variant;
  }

  @Override
  public String name() {
    return variant == Variant.LATENCY ? "latency" : "loss";
  }

  @Override
  public boolean canApply(final ClusterState state) {
    return state.canImpair(1);
  }

  @Override
  public String inject(final ClusterState state, final NodeControl control, final Random random) throws Exception {
    final int count = Targets.count(state, random);
    targets = Targets.pick(state, control, random, Targets.Role.ANY, count);
    final StringBuilder description = new StringBuilder("ANY ").append(Arrays.toString(targets));
    for (final int node : targets) {
      if (variant == Variant.LATENCY) {
        final int latencyMs = 200 + random.nextInt(1801);
        control.addLatency(node, latencyMs, latencyMs / 4);
        description.append(' ').append(latencyMs).append("ms");
      } else {
        final float toxicity = (5 + random.nextInt(16)) / 100f;
        control.addLoss(node, toxicity);
        description.append(' ').append(toxicity);
      }
      state.set(node, NodeState.DEGRADED);
    }
    return description.toString();
  }

  @Override
  public void heal(final ClusterState state, final NodeControl control) throws Exception {
    for (final int node : targets) {
      control.clearToxics(node);
      state.set(node, NodeState.UP);
    }
    targets = new int[0];
  }

  @Override
  public boolean expectsWritesAvailable() {
    return true;
  }
}
