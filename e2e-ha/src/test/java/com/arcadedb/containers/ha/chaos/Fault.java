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
import java.util.Random;

/**
 * One kind of failure. {@link #inject} picks targets from the seeded random and impairs them; {@link #heal} restores
 * exactly those targets. Only one fault is active at a time.
 */
public interface Fault {
  String name();

  boolean canApply(ClusterState state);

  /** @return the role and targets, for the step log, e.g. {@code "LEADER [2]"} */
  String inject(ClusterState state, NodeControl control, Random random) throws Exception;

  void heal(ClusterState state, NodeControl control) throws Exception;

  /** True when a majority stays connected, so acknowledged writes must keep flowing once a leader is elected. */
  boolean expectsWritesAvailable();

  /** The shortest hold this fault needs to mean anything; a shorter configured hold is raised to it. */
  default Duration minHold() {
    return Duration.ZERO;
  }

  /**
   * True when a reformat of its Raft storage by any node while this fault's step runs (inject, hold, heal and checkpoint) is a
   * violation: the fault alone gives no node a reason to discard its log, so a reformat means recovery took the destructive
   * path. That holds when a majority stays up and no node is cut off or killed uncleanly: the freeze faults and the graceful
   * restarts. A SIGKILL, a partition or a toxic link can leave a minority genuinely diverged, so for those a reformat can be the
   * right recovery.
   * <p>
   * This decides only whether a reformat FAILS the step. The runner reads the counters on every step and reports a reformat of any
   * step in {@code steps.log} and in {@code summary.json}, so the decision about which faults may reformat is not also the
   * decision about whether anyone can see it (issue #10159). Every fault states its answer: see {@code FaultsTest}.
   */
  default boolean forbidsReformat() {
    return false;
  }

  /**
   * The nodes whose server process this fault restarted during its step, valid once {@link #heal} has run. The in-place restart
   * counts are kept by the server process, so a restarted node counts from zero again: for these nodes the count after the step
   * is what the step did, and for every other node it is the count after minus the count before. A count that went down does not
   * tell a restart from a miscount, and one that came back equal tells nothing, which is why the fault says it.
   */
  default int[] restartedNodes(final ClusterState state) {
    return new int[0];
  }
}
