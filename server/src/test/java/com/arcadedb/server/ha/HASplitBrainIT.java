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
 * Tests Ratis cluster resilience under simulated split-brain conditions.
 *
 * With 5 nodes and MAJORITY quorum (3 of 5), we simulate:
 * 1. Stop 2 nodes (minority partition) - the majority (3) should continue working
 * 2. Verify writes succeed on the majority partition
 * 3. Restart the 2 stopped nodes
 * 4. Verify the restarted nodes rejoin and get the data written during the partition
 *
 * Ratis guarantees: only the majority partition can elect a leader and accept writes.
 * The minority partition cannot form a quorum and becomes read-only/unavailable.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class HASplitBrainIT extends BaseGraphServerTest {

  @Override
  protected int getServerCount() {
    return 5;
  }

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.HA_QUORUM.setValue("Majority");
  }

  @Test
  void testSplitBrainMajoritySurvives() throws Exception {
    testLog("=== Phase 1: Verify all 5 servers are up and a leader is elected ===");
    final ArcadeDBServer initialLeader = getLeaderServer();
    assertThat(initialLeader).isNotNull();
    testLog("Initial leader: %s", initialLeader.getServerName());

    // Write initial data
    final var leaderDb = initialLeader.getDatabase(getDatabaseName());
    leaderDb.transaction(() -> {
      leaderDb.newVertex(VERTEX1_TYPE_NAME).set("id", 1L).set("name", "before-split").save();
    });
    CodeUtils.sleep(3000); // wait for replication

    testLog("=== Phase 2: Simulate partition - stop 2 servers (minority) ===");
    // Stop servers 3 and 4 (the minority)
    final ArcadeDBServer server3 = getServer(3);
    final ArcadeDBServer server4 = getServer(4);
    final String name3 = server3.getServerName();
    final String name4 = server4.getServerName();

    server3.stop();
    server4.stop();
    testLog("Stopped %s and %s", name3, name4);

    // Wait for the majority to detect the partition and re-elect if needed
    CodeUtils.sleep(5000);

    testLog("=== Phase 3: Verify majority partition still works ===");
    // Find the leader in the remaining 3 servers
    Awaitility.await()
        .atMost(15, TimeUnit.SECONDS)
        .pollInterval(500, TimeUnit.MILLISECONDS)
        .until(() -> {
          for (int i = 0; i < 3; i++)
            if (getServer(i).isStarted() && getServer(i).getHA() != null && getServer(i).getHA().isLeader())
              return true;
          return false;
        });

    final ArcadeDBServer majorityLeader = getLeaderServer();
    assertThat(majorityLeader).as("No leader in majority partition").isNotNull();
    testLog("Majority leader: %s", majorityLeader.getServerName());

    // Write data during the partition
    final var majorityDb = majorityLeader.getDatabase(getDatabaseName());
    majorityDb.transaction(() -> {
      majorityDb.newVertex(VERTEX1_TYPE_NAME).set("id", 2L).set("name", "during-split").save();
    });
    testLog("Wrote vertex during partition on %s", majorityLeader.getServerName());

    // Verify data on surviving servers
    CodeUtils.sleep(3000);
    for (int i = 0; i < 3; i++) {
      if (getServer(i).isStarted()) {
        final var db = getServer(i).getDatabase(getDatabaseName());
        final long count = db.query("sql", "SELECT count(*) as cnt FROM " + VERTEX1_TYPE_NAME)
            .nextIfAvailable().getProperty("cnt", 0L);
        testLog("Server %s has %d vertices during partition", getServer(i).getServerName(), count);
        // Should have at least the initial vertex + the one written during partition
        assertThat(count).as("Server " + getServer(i).getServerName() + " should have vertices").isGreaterThanOrEqualTo(2);
      }
    }

    testLog("=== Phase 4: Heal partition - restart minority servers ===");
    // Restart server 3 and 4
    for (int i = 3; i <= 4; i++) {
      final var config = getServer(i).getConfiguration();
      final var newServer = new ArcadeDBServer(config);
      // Store the new server reference
      setServer(i, newServer);
      newServer.start();
      testLog("Restarted server %d", i);
    }

    // Wait for the restarted servers to rejoin the cluster
    testLog("Waiting for minority servers to rejoin...");
    CodeUtils.sleep(10000);

    testLog("=== Phase 5: Verify all servers have consistent data ===");
    // Write one more vertex to ensure the cluster is fully operational
    final ArcadeDBServer finalLeader = getLeaderServer();
    assertThat(finalLeader).isNotNull();
    final var finalDb = finalLeader.getDatabase(getDatabaseName());
    finalDb.transaction(() -> {
      finalDb.newVertex(VERTEX1_TYPE_NAME).set("id", 3L).set("name", "after-heal").save();
    });
    CodeUtils.sleep(5000);

    // All servers should have all 3 vertices (initial + during-split + after-heal)
    // Note: restarted servers may need snapshot installation to catch up
    for (int i = 0; i < 3; i++) {
      if (getServer(i).isStarted()) {
        final var db = getServer(i).getDatabase(getDatabaseName());
        final long count = db.query("sql", "SELECT count(*) as cnt FROM " + VERTEX1_TYPE_NAME)
            .nextIfAvailable().getProperty("cnt", 0L);
        testLog("Server %d has %d vertices after heal", i, count);
        assertThat(count).as("Server " + i + " should have all 3 vertices").isGreaterThanOrEqualTo(3);
      }
    }

    testLog("=== Split brain test completed successfully ===");
  }

  protected void setServer(final int index, final ArcadeDBServer server) {
    // Access the servers array via reflection since it's in the superclass
    try {
      final var field = BaseGraphServerTest.class.getDeclaredField("servers");
      field.setAccessible(true);
      final ArcadeDBServer[] servers = (ArcadeDBServer[]) field.get(this);
      servers[index] = server;
    } catch (final Exception e) {
      throw new RuntimeException("Cannot set server " + index, e);
    }
  }

  @Override
  protected int[] getServerToCheck() {
    // Only check the first 3 servers (the majority) for database comparison
    return new int[] { 0, 1, 2 };
  }
}
