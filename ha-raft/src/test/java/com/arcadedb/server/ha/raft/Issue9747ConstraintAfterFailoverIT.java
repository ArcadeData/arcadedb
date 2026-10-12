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
package com.arcadedb.server.ha.raft;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.exception.ValidationException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9747, the consequence the issue names: a constraint set on the leader by {@code ALTER PROPERTY} has to be
 * enforced by the node that becomes leader after a failover. Before the fix the follower's schema never received it, so
 * once that follower was elected it accepted the very record the old leader refused.
 */
class Issue9747ConstraintAfterFailoverIT extends BaseRaftHATest {

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "majority");
  }

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void theNewLeaderEnforcesAConstraintSetOnTheOldOne() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);

    final Database leader = getServerDatabase(leaderIndex, getDatabaseName());
    leader.command("sql", "CREATE DOCUMENT TYPE C");
    leader.command("sql", "CREATE PROPERTY C.x INTEGER");
    leader.command("sql", "ALTER PROPERTY C.x MANDATORY true");
    leader.command("sql", "ALTER DATABASE `arcadedb.dateFormat` 'dd-MM-yyyy'");

    assertThatThrownBy(() -> leader.transaction(() -> leader.newDocument("C").set("y", 1).save()))
        .isInstanceOf(ValidationException.class);
    assertClusterConsistency();

    getServer(leaderIndex).stop();
    final int newLeaderIndex = waitForNewLeader(leaderIndex);
    assertThat(newLeaderIndex).as("a new leader must be elected").isGreaterThanOrEqualTo(0).isNotEqualTo(leaderIndex);

    final Database newLeader = getServerDatabase(newLeaderIndex, getDatabaseName());
    assertThatThrownBy(() -> newLeader.transaction(() -> newLeader.newDocument("C").set("y", 1).save()))
        .as("the new leader must refuse a record missing the MANDATORY property")
        .isInstanceOf(ValidationException.class);
    assertThat(newLeader.getSchema().getDateFormat()).isEqualTo("dd-MM-yyyy");
  }

  @Override
  protected void checkDatabasesAreIdentical() {
    // One server is stopped on purpose; the assertions above are about the survivor that took over
  }

  @Override
  protected int[] getServerToCheck() {
    return startedServers();
  }

  private int waitForNewLeader(final int excludeIndex) {
    final long deadline = System.currentTimeMillis() + 30_000;
    while (System.currentTimeMillis() < deadline) {
      for (int i = 0; i < getServerCount(); i++) {
        if (i == excludeIndex)
          continue;
        final RaftHAPlugin plugin = getRaftPlugin(i);
        if (plugin != null && plugin.isLeader())
          return i;
      }
      try {
        Thread.sleep(500);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        return -1;
      }
    }
    return -1;
  }
}
