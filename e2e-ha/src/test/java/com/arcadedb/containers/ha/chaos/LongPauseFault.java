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

import java.time.Duration;
import java.util.Arrays;
import java.util.Random;

/**
 * Freezes one follower ({@code docker pause}, the cgroup-freezer equivalent of SIGSTOP) for longer than the Ratis 60 s
 * JVM-pause close threshold, then thaws it (issue #8954). Since #8902 ArcadeDB disables that close by default
 * ({@code arcadedb.ha.jvmPauseCloseThresholdMs=0}), so the thawed follower must either catch up on its own or be
 * recovered by the health monitor with an in-place restart that keeps its storage. The checkpoint after the step proves
 * the catch-up; the runner fails the step when any node reformatted its Raft storage while it ran.
 * <p>
 * An in-JVM test cannot stop the world for one server of a cluster sharing the JVM; the chaos nodes are separate
 * processes, so only the frozen follower sees the pause.
 */
public final class LongPauseFault implements Fault {
  /** Longer than the Ratis default close threshold (60 s), with margin for the pause monitor's sampling. */
  static final Duration MIN_HOLD = Duration.ofSeconds(75);

  private int[] targets = new int[0];

  @Override
  public String name() {
    return "longpause";
  }

  @Override
  public boolean canApply(final ClusterState state) {
    return state.canImpair(1);
  }

  @Override
  public String inject(final ClusterState state, final NodeControl control, final Random random) throws Exception {
    targets = Targets.pick(state, control, random, Targets.Role.FOLLOWER, 1);
    for (final int node : targets) {
      control.pause(node);
      state.set(node, NodeState.PAUSED);
    }
    return Targets.Role.FOLLOWER + " " + Arrays.toString(targets);
  }

  @Override
  public void heal(final ClusterState state, final NodeControl control) throws Exception {
    for (final int node : targets) {
      control.unpause(node);
      state.set(node, NodeState.UP);
    }
    targets = new int[0];
  }

  @Override
  public boolean expectsWritesAvailable() {
    return true;
  }

  @Override
  public Duration minHold() {
    return MIN_HOLD;
  }

  @Override
  public boolean forbidsReformat() {
    return true;
  }
}
