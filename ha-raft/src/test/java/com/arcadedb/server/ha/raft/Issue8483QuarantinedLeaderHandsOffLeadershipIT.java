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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #8483: a leader that quarantined one of its own databases kept leadership, so the
 * database could be resynced nowhere - the leader refuses to pull from itself, and since #8468 it refuses to serve the
 * quarantined copy to the followers too - until an operator moved the leadership by hand.
 * <p>
 * The leader now hands leadership to a healthy peer, and once it is a follower its own health tick resyncs the
 * database from the new leader.
 */
class Issue8483QuarantinedLeaderHandsOffLeadershipIT extends BaseRaftHATest {

  private static final long AWAIT_MS = 90_000L;

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    // The base disables the health monitor; the handoff backstop and the ex-leader's resync both run on its tick.
    config.setValue(GlobalConfiguration.HA_HEALTH_CHECK_INTERVAL, 1_000L);
  }

  @Test
  @Timeout(240)
  void aLeaderHoldingAQuarantineHandsLeadershipOffAndResyncsFromTheNextLeader() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final String dbName = getDatabaseName();
    final String type = "Issue8483Record";

    final Database leaderDb = getServerDatabase(leaderIndex, dbName);
    leaderDb.transaction(() -> {
      leaderDb.getSchema().createVertexType(type);
      for (int i = 0; i < 10; i++)
        leaderDb.newVertex(type).set("id", i).save();
    });
    assertClusterConsistency();

    final ArcadeStateMachine exLeaderMachine = getRaftPlugin(leaderIndex).getRaftHAServer().getStateMachine();

    // What a WAL version gap on the leader's apply thread records, or a quarantine restored from disk on a node that
    // then won the election: nothing in this term raised it, so the health tick is what has to act on it.
    exLeaderMachine.markStateDiverged(dbName, DivergenceCause.WAL_VERSION_GAP);

    assertThat(await(() -> !getRaftPlugin(leaderIndex).isLeader() && findLeaderIndex() >= 0))
        .as("the quarantined leader must hand leadership to a healthy peer").isTrue();
    final int newLeaderIndex = findLeaderIndex();
    assertThat(newLeaderIndex).isNotEqualTo(leaderIndex);

    assertThat(await(() -> !exLeaderMachine.isDatabaseDiverged(dbName)))
        .as("once a follower, the ex-leader must resync the quarantined database from the new leader").isTrue();

    final Database newLeaderDb = getServerDatabase(newLeaderIndex, dbName);
    newLeaderDb.transaction(() -> newLeaderDb.newVertex(type).set("id", 10).save());
    assertThat(awaitCountOn(leaderIndex, type, 11)).as("the ex-leader replicates again").isEqualTo(11);
  }

  private static boolean await(final BooleanSupplier condition) throws InterruptedException {
    final long deadline = System.currentTimeMillis() + AWAIT_MS;
    while (System.currentTimeMillis() < deadline) {
      if (condition.getAsBoolean())
        return true;
      Thread.sleep(200);
    }
    return condition.getAsBoolean();
  }
}
