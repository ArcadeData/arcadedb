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
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.utility.CodeUtils;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that writes continue after the leader goes down and a new leader is elected.
 * Uses the server Java API (not RemoteDatabase) to avoid client-side failover issues.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ReplicationServerLeaderDownIT extends BaseGraphServerTest {

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
  void testLeaderDownDuringWrites() {
    // Phase 1: Write 10 vertices on the leader
    ArcadeDBServer leader = getLeaderServer();
    assertThat(leader).isNotNull();
    testLog("Initial leader: %s", leader.getServerName());

    for (int i = 0; i < 10; i++) {
      final int idx = i;
      leader.getDatabase(getDatabaseName()).transaction(() -> {
        leader.getDatabase(getDatabaseName()).newVertex(VERTEX1_TYPE_NAME)
            .set("id", (long) (10000 + idx)).set("name", "before-stop").save();
      });
    }
    testLog("Phase 1: 10 vertices written on leader");
    CodeUtils.sleep(3000);

    // Phase 2: Stop the leader
    final String leaderName = leader.getServerName();
    testLog("Stopping leader: %s", leaderName);
    leader.stop();

    // Phase 3: Wait for new leader election
    testLog("Waiting for new leader election...");
    Awaitility.await()
        .atMost(15, TimeUnit.SECONDS)
        .pollInterval(500, TimeUnit.MILLISECONDS)
        .until(() -> {
          for (int i = 0; i < getServerCount(); i++) {
            final ArcadeDBServer s = getServer(i);
            if (s != null && s.isStarted() && s.getHA() != null && s.getHA().isLeader())
              return true;
          }
          return false;
        });

    final ArcadeDBServer newLeader = getLeaderServer();
    assertThat(newLeader).isNotNull();
    assertThat(newLeader.getServerName()).isNotEqualTo(leaderName);
    testLog("New leader elected: %s", newLeader.getServerName());

    // Phase 4: Write 10 more vertices on the new leader
    final AtomicInteger successCount = new AtomicInteger();
    for (int i = 0; i < 10; i++) {
      final int idx = i;
      try {
        newLeader.getDatabase(getDatabaseName()).transaction(() -> {
          newLeader.getDatabase(getDatabaseName()).newVertex(VERTEX1_TYPE_NAME)
              .set("id", (long) (20000 + idx)).set("name", "after-stop").save();
        });
        successCount.incrementAndGet();
      } catch (final Exception e) {
        testLog("Write failed after leader change: %s", e.getMessage());
      }
    }

    testLog("Phase 4: %d/10 vertices written on new leader", successCount.get());
    assertThat(successCount.get()).as("Expected at least 8 writes on new leader").isGreaterThanOrEqualTo(8);

    // Phase 5: Verify data on surviving servers
    CodeUtils.sleep(3000);
    for (int i = 0; i < getServerCount(); i++) {
      final ArcadeDBServer s = getServer(i);
      if (s != null && s.isStarted()) {
        final long count = s.getDatabase(getDatabaseName())
            .query("sql", "SELECT count(*) as cnt FROM " + VERTEX1_TYPE_NAME)
            .nextIfAvailable().getProperty("cnt", 0L);
        testLog("Server %s has %d vertices", s.getServerName(), count);
      }
    }
  }

  @Override
  public void endTest() {
    // Don't restart the stopped server - just stop all and clean up
    try {
      stopServers();
    } finally {
      GlobalConfiguration.resetAll();
      if (dropDatabasesAtTheEnd())
        deleteDatabaseFolders();
    }
  }

  @Override
  protected int[] getServerToCheck() {
    // Only check surviving servers (server 0 was stopped and may not have caught up)
    return new int[] { 1, 2 };
  }

  @Override
  protected boolean dropDatabasesAtTheEnd() {
    return true;
  }
}
