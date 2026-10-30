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

import com.arcadedb.index.columnar.ColumnarIndexManager;
import com.arcadedb.index.columnar.ColumnarTestData;
import com.arcadedb.log.LogManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The leader stops while every node holds replicated or committed pages its projection did not apply yet (plan 3, Task 23). A
 * survivor's projection does not depend on the role it had when a page was marked: the new leader keeps applying the pages it marked
 * as a follower and then projects its own local commits, and the old leader, restarted, catches up through the Raft log with no
 * rebuild.
 */
class ColumnarLeaderFailoverIT extends ColumnarHATestSupport {

  @Override
  protected int[] getServerToCheck() {
    return startedServers();
  }

  @Test
  @Timeout(180)
  void leaderFailoverMidApply() throws Exception {
    final int oldLeader = requireLeaderIndex();
    final ColumnarTestData data = new ColumnarTestData(2307L);
    data.deleteRatio = 0.1;
    data.createSchema(database(oldLeader));
    assertClusterConsistency();
    assertEquivalentEverywhere();
    for (int i = 0; i < getServerCount(); i++)
      assertThat(manager(i).getStats().rebuildsTotal())
          .as("server %d: indexes created on an empty type rebuild nothing (%s)", i, rebuildsByReason(i)).isZero();

    final AutoCloseable[] holds = new AutoCloseable[getServerCount()];
    final ColumnarIndexManager[] held = new ColumnarIndexManager[getServerCount()];
    try {
      for (int i = 0; i < getServerCount(); i++) {
        held[i] = manager(i);
        holds[i] = held[i].suspendApply();
      }
      data.insertAssets(database(oldLeader), 2_000);
      data.mutate(database(oldLeader), 1_000);
      assertClusterConsistency();
      for (int i = 0; i < getServerCount(); i++)
        assertThat(held[i].awaitCaughtUp(0)).as("the fixture: server %d holds pages it did not apply", i).isFalse();

      LogManager.instance().log(this, Level.INFO, "TEST: stopping leader %d with the columnar apply held on every node", oldLeader);
      getServer(oldLeader).stop();
      final int newLeader = requireLeaderIndex();
      assertThat(newLeader).isNotEqualTo(oldLeader);

      data.insertAssets(database(newLeader), 1_200);
      data.mutate(database(newLeader), 800);
      assertClusterConsistency();
    } finally {
      for (final AutoCloseable hold : holds)
        if (hold != null)
          hold.close();
    }

    assertEquivalentEverywhere();
    for (final int i : startedServers())
      assertThat(manager(i).getStats().rebuildsTotal())
          .as("survivor %d applied its held pages, it rebuilt nothing (%s)", i, rebuildsByReason(i)).isZero();

    restartServer(oldLeader);
    assertClusterConsistency();
    assertEquivalentOn(oldLeader);
    assertThat(manager(oldLeader).getStats().rebuildsTotal())
        .as("the restarted old leader caught up without a rebuild (%s)", rebuildsByReason(oldLeader)).isZero();
    assertEquivalentEverywhere();
  }
}
