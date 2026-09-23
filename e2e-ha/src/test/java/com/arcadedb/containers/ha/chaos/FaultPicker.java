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

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Weighted, seeded choice among the enabled faults that can apply to the current cluster state.
 */
public final class FaultPicker {
  private final List<Fault> faults = new ArrayList<>();
  private final int[]       weights;

  public FaultPicker(final Map<String, Integer> faultWeights, final Duration electionTimeout) {
    weights = new int[faultWeights.size()];
    int i = 0;
    for (final Map.Entry<String, Integer> entry : faultWeights.entrySet()) {
      faults.add(create(entry.getKey(), electionTimeout));
      weights[i++] = entry.getValue();
    }
  }

  static Fault create(final String name, final Duration electionTimeout) {
    return switch (name) {
      case "kill" -> new NodeFault(NodeFault.Kind.KILL);
      case "stop" -> new NodeFault(NodeFault.Kind.STOP);
      case "pause" -> new NodeFault(NodeFault.Kind.PAUSE);
      case "isolate" -> new NodeFault(NodeFault.Kind.ISOLATE);
      case "split" -> new SplitFault();
      case "rolling" -> new RollingRestartFault(electionTimeout);
      case "latency" -> new ToxicFault(ToxicFault.Variant.LATENCY);
      case "loss" -> new ToxicFault(ToxicFault.Variant.LOSS);
      default -> throw new IllegalArgumentException("Unknown fault: " + name);
    };
  }

  /**
   * @return a fault that can apply, or null when none can. Always consumes exactly one draw from the random.
   */
  public Fault pick(final ClusterState state, final Random random) {
    int total = 0;
    for (int i = 0; i < faults.size(); i++)
      if (faults.get(i).canApply(state))
        total += weights[i];
    final int roll = random.nextInt(Math.max(total, 1));
    if (total == 0)
      return null;
    int cumulative = 0;
    for (int i = 0; i < faults.size(); i++) {
      if (!faults.get(i).canApply(state))
        continue;
      cumulative += weights[i];
      if (roll < cumulative)
        return faults.get(i);
    }
    throw new IllegalStateException("Weighted pick fell through: roll " + roll + " of " + total);
  }
}
