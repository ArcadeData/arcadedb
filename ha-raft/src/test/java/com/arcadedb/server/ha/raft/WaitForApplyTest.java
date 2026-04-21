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

import com.arcadedb.database.Database;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

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
}
