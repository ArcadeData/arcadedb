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
package com.arcadedb.server.ha;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.log.LogManager;
import com.arcadedb.network.binary.QuorumNotReachedException;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests that writes fail with QuorumNotReachedException when 2 out of 3 servers are stopped
 * and MAJORITY quorum cannot be reached.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ReplicationServerQuorumMajority2ServersOutIT extends BaseGraphServerTest {

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.HA_QUORUM.setValue("Majority");
    // Short quorum timeout so writes fail fast when quorum is lost
    GlobalConfiguration.HA_QUORUM_TIMEOUT.setValue(2000L);
  }

  @Test
  void quorumLostAfterStoppingTwoServers() {
    // Write some data first to confirm cluster is healthy
    final ArcadeDBServer leader = getLeaderServer();
    assertThat(leader).isNotNull();

    final Database db = leader.getDatabase(getDatabaseName());
    for (int i = 0; i < 5; i++) {
      final int idx = i;
      db.transaction(() -> db.newVertex(VERTEX1_TYPE_NAME).set("id", 10000L + idx).set("name", "pre-stop").save());
    }

    LogManager.instance().log(this, Level.INFO, "TEST: Cluster healthy, stopping 2 non-leader servers...");

    // Stop both followers (keep the leader running)
    for (int i = 0; i < getServerCount(); i++) {
      final ArcadeDBServer s = getServer(i);
      if (s != leader && s.isStarted()) {
        LogManager.instance().log(this, Level.INFO, "TEST: Stopping server %s...", s.getServerName());
        s.stop();
      }
    }

    LogManager.instance().log(this, Level.INFO, "TEST: Both followers stopped. Next write should fail on quorum...");

    // Now writes should fail because MAJORITY quorum (2/3) is unreachable.
    // The failure can manifest as:
    // - QuorumNotReachedException: Ratis can't replicate to majority
    // - ServerIsNotTheLeaderException: old leader lost leadership (no election possible with 1/3)
    assertThatThrownBy(() -> {
      for (int i = 0; i < 3; i++) {
        final int idx = i;
        db.transaction(() ->
            db.newVertex(VERTEX1_TYPE_NAME).set("id", System.nanoTime()).set("name", "should-fail-" + idx).save()
        );
      }
    }).satisfiesAnyOf(
        e -> assertThat(e).isInstanceOf(QuorumNotReachedException.class),
        e -> assertThat(e).isInstanceOf(ServerIsNotTheLeaderException.class),
        e -> assertThat(e).hasCauseInstanceOf(QuorumNotReachedException.class),
        e -> assertThat(e).hasCauseInstanceOf(ServerIsNotTheLeaderException.class)
    );

    LogManager.instance().log(this, Level.INFO, "TEST: QuorumNotReachedException received as expected.");
  }

  @Override
  protected int[] getServerToCheck() {
    // Skip database comparison: with "leader commits first" design (ReplicatedDatabase.commit2ndPhase
    // runs before Ratis replication), the leader may have locally committed writes that failed to
    // replicate after quorum was lost. Additionally, followers stopped mid-replication may not have
    // applied all pre-stop writes. So databases are expected to diverge in this test.
    return new int[] {};
  }
}
