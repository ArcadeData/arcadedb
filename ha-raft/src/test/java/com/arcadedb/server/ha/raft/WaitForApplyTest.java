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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WaitForApplyTest extends BaseRaftHATest {

  @Override
  protected int getServerCount() {
    return 2;
  }

  @Test
  void getLastAppliedIndexIncrementsAfterWrite() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);

    final RaftHAServer raft = getRaftPlugin(leaderIndex).getRaftHAServer();
    final long indexBefore = raft.getLastAppliedIndex();

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    leaderDb.transaction(() -> {
      if (!leaderDb.getSchema().existsType("ApplyTest"))
        leaderDb.getSchema().createVertexType("ApplyTest");
    });

    assertClusterConsistency();
    final long indexAfter = raft.getLastAppliedIndex();
    assertThat(indexAfter).isGreaterThan(indexBefore);
  }

  @Test
  void waitForAppliedIndexReturnsImmediatelyForPastIndex() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);

    final RaftHAServer raft = getRaftPlugin(leaderIndex).getRaftHAServer();

    final long start = System.currentTimeMillis();
    raft.waitForAppliedIndex(0);
    final long elapsed = System.currentTimeMillis() - start;
    assertThat(elapsed).isLessThan(1000);
  }

  /**
   * LINEARIZABLE contract: a read index that the local state machine can never reach within the
   * quorum timeout MUST fail the read (throw) rather than silently degrade to EVENTUAL and serve
   * stale data. Regression guard for the stale LINEARIZABLE follower-read bug.
   */
  @Test
  void waitForAppliedIndexThrowsOnTimeoutWhenLinearizable() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);

    final RaftHAServer raft = getRaftPlugin(leaderIndex).getRaftHAServer();
    final long quorumTimeout = GlobalConfiguration.HA_QUORUM_TIMEOUT.getValueAsLong();

    // A target far above any applied index is unreachable, so the strict path must throw.
    final long unreachable = raft.getLastAppliedIndex() + 1_000_000L;

    final long start = System.currentTimeMillis();
    assertThatThrownBy(() -> raft.waitForAppliedIndex(unreachable, true))
        .isInstanceOf(ReplicationException.class)
        .hasMessageContaining("LINEARIZABLE");
    final long elapsed = System.currentTimeMillis() - start;
    // It must actually wait up to the quorum timeout before giving up (not fail-fast on a transient).
    assertThat(elapsed).isGreaterThanOrEqualTo(quorumTimeout - 500);
  }

  /**
   * READ_YOUR_WRITES / bookmark contract is preserved: the lenient one-arg overload degrades to
   * best-effort on timeout (logs and returns) instead of throwing.
   */
  @Test
  void waitForAppliedIndexDegradesOnTimeoutWhenLenient() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);

    final RaftHAServer raft = getRaftPlugin(leaderIndex).getRaftHAServer();
    final long unreachable = raft.getLastAppliedIndex() + 1_000_000L;

    // Must not throw - returns after the quorum timeout.
    raft.waitForAppliedIndex(unreachable);
  }
}
