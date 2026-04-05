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
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.utility.CodeUtils;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that the Ratis cluster survives multiple leader changes.
 * Stops the current leader, verifies a new leader is elected, writes data,
 * restarts the old leader, repeats 3 times.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ReplicationServerLeaderChanges3TimesIT extends BaseGraphServerTest {

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.HA_QUORUM.setValue("Majority");
  }

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void testLeaderChanges3Times() throws Exception {
    for (int cycle = 0; cycle < 3; cycle++) {
      testLog("=== Cycle %d: finding and stopping leader ===", cycle);

      // Find the current leader
      final ArcadeDBServer leader = getLeaderServer();
      assertThat(leader).as("No leader found in cycle " + cycle).isNotNull();
      final String leaderName = leader.getServerName();
      testLog("Leader is %s, stopping it...", leaderName);

      // Stop the leader
      leader.stop();
      testLog("Leader %s stopped", leaderName);

      // Wait for a new leader to be elected
      Awaitility.await()
          .atMost(15, TimeUnit.SECONDS)
          .pollInterval(500, TimeUnit.MILLISECONDS)
          .until(() -> {
            for (final ArcadeDBServer s : getServers())
              if (s != null && s.isStarted() && s.getHA() != null && s.getHA().isLeader())
                return true;
            return false;
          });

      final ArcadeDBServer newLeader = getLeaderServer();
      assertThat(newLeader).as("No new leader elected after stopping " + leaderName).isNotNull();
      testLog("New leader: %s", newLeader.getServerName());
      assertThat(newLeader.getServerName()).isNotEqualTo(leaderName);

      // Write some data on the new leader
      final int currentCycle = cycle;
      final var db = newLeader.getDatabase(getDatabaseName());
      db.transaction(() -> {
        db.newVertex(VERTEX1_TYPE_NAME).set("id", (long) (1000 + currentCycle)).set("name", "cycle-" + currentCycle).save();
      });
      testLog("Wrote vertex for cycle %d on new leader %s", cycle, newLeader.getServerName());

      // Verify the data is readable on surviving servers
      for (final ArcadeDBServer s : getServers()) {
        if (s != null && s.isStarted()) {
          CodeUtils.sleep(2000);
          final var sdb = s.getDatabase(getDatabaseName());
          final long count = sdb.query("sql", "SELECT count(*) as cnt FROM " + VERTEX1_TYPE_NAME)
              .nextIfAvailable().getProperty("cnt", 0L);
          testLog("Server %s has %d vertices after cycle %d", s.getServerName(), count, cycle);
        }
      }

      // Restart the stopped leader
      testLog("Restarting old leader %s...", leaderName);
      final int leaderIndex = getServerNumber(leaderName);
      if (leaderIndex >= 0) {
        final var config = leader.getConfiguration();
        final var newServer = new ArcadeDBServer(config);
        getServers()[leaderIndex] = newServer;
        newServer.start();
        testLog("Old leader %s restarted", leaderName);

        // Wait for the restarted server to rejoin
        CodeUtils.sleep(5000);
      }
    }

    testLog("=== All 3 leader change cycles completed successfully ===");
  }

  // getServers() and getServerNumber() are inherited from BaseGraphServerTest
}
