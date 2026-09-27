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
import java.util.Random;

/**
 * Gracefully restarts every node in index order, waiting for a leader after each one. The whole fault happens inside
 * {@link #inject}; {@link #heal} has nothing left to do.
 */
public final class RollingRestartFault implements Fault {
  private final Duration electionTimeout;

  public RollingRestartFault(final Duration electionTimeout) {
    this.electionTimeout = electionTimeout;
  }

  @Override
  public String name() {
    return "rolling";
  }

  @Override
  public boolean canApply(final ClusterState state) {
    return state.impairedCount() == 0;
  }

  @Override
  public String inject(final ClusterState state, final NodeControl control, final Random random) throws Exception {
    for (int node = 0; node < state.size(); node++) {
      control.stopGracefully(node);
      state.set(node, NodeState.DOWN);
      control.start(node);
      state.set(node, NodeState.UP);
      if (!control.awaitLeader(electionTimeout))
        throw new ChaosFailure(ResultKind.AVAILABILITY,
            "No leader known by every node within " + electionTimeout + " after restarting node " + node
                + " during a rolling restart. Nodes report: " + control.leaderView());
    }
    return "ALL in index order";
  }

  @Override
  public void heal(final ClusterState state, final NodeControl control) {
    // every node was already restarted by inject
  }

  @Override
  public boolean expectsWritesAvailable() {
    return false;
  }
}
