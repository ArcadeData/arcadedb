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

/**
 * Regression test for issue #4809: the no-target {@code transferLeadership(timeoutMs)} reported
 * success whenever this node simply stopped being the leader ({@code reply.isSuccess() || !isLeader()},
 * and {@code if (!isLeader()) return true} in the catch). That conflates a real, controlled handoff
 * with a transient candidate / leaderless window or an unrelated election.
 * <p>
 * The fix requires a positive confirmation that leadership settled on a concrete, DIFFERENT peer
 * (mirroring the targeted variant, which already confirms the actual leader equals the target), and
 * guards against transferring from a node that is not the leader at all.
 */
@Tag("slow")
class Issue4809NoTargetTransferLeadershipIT extends BaseRaftHATest {

  @Override
  protected int getServerCount() {
    return 3;
  }

  /**
   * A genuine no-target transfer issued on the leader must report success: the fix's
   * settled-different-leader confirmation must not introduce a false negative on a real handoff.
   */
  @Test
  void noTargetTransferOnLeaderSucceeds() {
    final int leaderIndex = waitForLeaderIndex(30_000);
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final boolean transferred = getRaftPlugin(leaderIndex).getRaftHAServer().transferLeadership(15_000);
    assertThat(transferred)
        .as("A genuine no-target transfer on the leader must report success (confirmed handoff to a different peer)")
        .isTrue();
  }

  /**
   * The core false-positive fix: a node that is not the leader has nothing to hand off, so the
   * no-target transfer must report failure and must not trigger any handoff on the real leader.
   * Before the fix, the {@code !isLeader()} heuristic could report success for such a call.
   */
  @Test
  void transferOnNonLeaderReturnsFalseWithoutChangingLeader() throws Exception {
    final int leaderIndex = waitForLeaderIndex(30_000);
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    int followerIndex = -1;
    for (int i = 0; i < getServerCount(); i++) {
      if (i != leaderIndex && getRaftPlugin(i) != null) {
        followerIndex = i;
        break;
      }
    }
    assertThat(followerIndex).as("A follower must exist").isGreaterThanOrEqualTo(0);

    final boolean result = getRaftPlugin(followerIndex).getRaftHAServer().transferLeadership(5_000);
    assertThat(result).as("Transfer requested on a non-leader must return false, not a false positive").isFalse();

    // The rejected request must change nothing: the original leader is still the leader.
    Thread.sleep(2_000);
    assertThat(findLeaderIndex())
        .as("Leadership must be unchanged after a rejected no-target transfer on a follower")
        .isEqualTo(leaderIndex);
  }

  /**
   * Polls {@link #findLeaderIndex()} until a leader is established, returning its index, or -1 if none
   * settles within {@code timeoutMs}. Tolerates the transient leaderless windows that occur right
   * after cluster startup.
   */
  private int waitForLeaderIndex(final long timeoutMs) {
    final long deadline = System.currentTimeMillis() + timeoutMs;
    while (System.currentTimeMillis() < deadline) {
      final int leaderIndex = findLeaderIndex();
      if (leaderIndex >= 0)
        return leaderIndex;
      try {
        Thread.sleep(200);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        return -1;
      }
    }
    return findLeaderIndex();
  }
}
