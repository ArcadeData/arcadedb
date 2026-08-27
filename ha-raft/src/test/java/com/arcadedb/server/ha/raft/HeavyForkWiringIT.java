/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.server.ha.raft;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Checks that the failsafe split in {@code ha-raft/pom.xml} is still wired the way issue #6343 built it: that a
 * class carrying {@code @Tag("ha-heavy")} actually lands in the second execution, and not merely that somebody
 * wrote the tag on it.
 * <p>
 * The two facts are further apart than they look. The split works by re-targeting the parent's unnamed failsafe
 * execution from this module's POM through {@code <id>default</id>} - correct, but obscure enough that a future
 * edit could undo it while leaving everything an observer normally looks at intact: the POM still declares two
 * executions, the five heavy classes still carry the tag, {@code HeavyItForkIsolationTest} is still green, and the
 * lane still passes in about the same time. The classes would simply be back in the shared fork, which is the
 * exact condition the split exists to prevent and the one thing none of those checks can see.
 * <p>
 * So each execution stamps its own name into {@code arcadedb.it.fork}, and this IT - itself tagged - asserts it
 * reads back the heavy one. A broken split runs this class in the default fork, where the stamp says
 * {@code default}, and the failure names the block to go and look at.
 * <p>
 * It is cheap to have: {@code reuseForks=false} gives it a JVM of its own, and that JVM does nothing but read two
 * system properties. It costs one fork start, on a lane with roughly twenty minutes of headroom, to make a piece
 * of build wiring self-verifying instead of merely commented.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("ha-heavy")
class HeavyForkWiringIT {

  /**
   * Set by both failsafe executions to the name of the execution that owns the fork. Absent when the class is
   * run outside Maven - from an IDE, say - where there is no split to check and nothing to assert.
   */
  private static final String FORK_PROPERTY = "arcadedb.it.fork";

  @Test
  void aTaggedItRunsInTheHeavyFork() {
    final String fork = System.getProperty(FORK_PROPERTY);
    assumeTrue(fork != null, "not run through failsafe (no " + FORK_PROPERTY + " stamp); nothing to check");

    assertThat(fork)
        .as("""
            This IT carries @Tag("ha-heavy") but ran in the "%s" fork.

            The failsafe split in ha-raft/pom.xml has stopped routing tagged classes to the
            heavy-its-in-their-own-fork execution, so the heaviest ITs in this module are sharing a JVM with the
            other ~118 again - which is what issue #6343 built the split to stop. The tags themselves are fine, or
            this class would not be looking for the stamp at all: the wiring is what broke.""".formatted(fork))
        .isEqualTo("ha-heavy");
  }

  /**
   * A second, quieter thing worth pinning: an execution-level {@code <systemPropertyVariables>} merges with the
   * parent's rather than replacing it. The property below comes from the plugin-level configuration in the root
   * POM, and if that merge ever stopped happening, the heavy fork would silently lose every setting the parent
   * supplies there - starting with this one and including anything added later. Asserted here because the
   * assumption is invisible everywhere else, and because the stamp above depends on the same merge working.
   */
  @Test
  void theExecutionInheritsTheParentSystemProperties() {
    assumeTrue(System.getProperty(FORK_PROPERTY) != null, "not run through failsafe; nothing to check");

    assertThat(System.getProperty("polyglot.engine.WarnInterpreterOnly"))
        .as("the heavy execution's <systemPropertyVariables> must MERGE with the root POM's, not replace them")
        .isEqualTo("false");
  }
}
