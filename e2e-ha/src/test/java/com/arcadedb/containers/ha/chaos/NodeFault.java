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
 * Faults that act on whole nodes: SIGKILL, graceful stop, freeze ({@code docker pause}) and network isolation. The
 * target is the leader or a follower with equal probability; on 5 nodes it may be two nodes.
 */
public final class NodeFault implements Fault {
  public enum Kind {
    KILL("kill", NodeState.DOWN), STOP("stop", NodeState.DOWN), PAUSE("pause", NodeState.PAUSED),
    ISOLATE("isolate", NodeState.ISOLATED);

    private final String    faultName;
    private final NodeState impaired;

    Kind(final String faultName, final NodeState impaired) {
      this.faultName = faultName;
      this.impaired = impaired;
    }
  }

  private final Kind  kind;
  private       int[] targets = new int[0];

  public NodeFault(final Kind kind) {
    this.kind = kind;
  }

  @Override
  public String name() {
    return kind.faultName;
  }

  @Override
  public boolean canApply(final ClusterState state) {
    return state.canImpair(1);
  }

  @Override
  public String inject(final ClusterState state, final NodeControl control, final Random random) throws Exception {
    final Targets.Role role = Targets.role(random);
    final int count = Targets.count(state, random);
    targets = Targets.pick(state, control, random, role, count);
    for (final int node : targets) {
      switch (kind) {
      case KILL -> control.kill(node);
      case STOP -> control.stopGracefully(node);
      case PAUSE -> control.pause(node);
      case ISOLATE -> control.disconnect(node);
      }
      state.set(node, kind.impaired);
    }
    return role + " " + Arrays.toString(targets);
  }

  @Override
  public void heal(final ClusterState state, final NodeControl control) throws Exception {
    for (final int node : targets) {
      switch (kind) {
      case KILL, STOP -> control.start(node);
      case PAUSE -> control.unpause(node);
      case ISOLATE -> control.reconnect(node);
      }
      state.set(node, NodeState.UP);
    }
    targets = new int[0];
  }

  @Override
  public boolean expectsWritesAvailable() {
    return true;
  }
}
