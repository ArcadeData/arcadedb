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
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.utility.CodeUtils;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that the RemoteDatabase client correctly fails over to the new leader
 * after the current leader goes down.
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
    // Phase 1: Write via server-side API on leader
    ArcadeDBServer leader = getLeaderServer();
    assertThat(leader).isNotNull();
    testLog("Initial leader: %s", leader.getServerName());

    for (int i = 0; i < 10; i++) {
      final int idx = i;
      leader.getDatabase(getDatabaseName()).transaction(() ->
          leader.getDatabase(getDatabaseName()).newVertex(VERTEX1_TYPE_NAME)
              .set("id", (long) (10000 + idx)).set("name", "before-stop").save()
      );
    }
    CodeUtils.sleep(3000);
    testLog("Phase 1: 10 vertices written on leader");

    // Phase 2: Create a RemoteDatabase client connected to a FOLLOWER
    // The client should discover the cluster topology and be able to failover
    int followerPort = -1;
    for (int i = 0; i < getServerCount(); i++) {
      final ArcadeDBServer s = getServer(i);
      if (s != null && s.isStarted() && s.getHA() != null && !s.getHA().isLeader()) {
        followerPort = s.getHttpServer().getPort();
        break;
      }
    }
    assertThat(followerPort).isGreaterThan(0);
    testLog("RemoteDatabase connected to follower on port %d", followerPort);

    final RemoteDatabase db = new RemoteDatabase("127.0.0.1", followerPort, getDatabaseName(), "root", DEFAULT_PASSWORD_FOR_TESTS);

    // Phase 3: Write via RemoteDatabase (goes through HTTP proxy to leader)
    for (int i = 0; i < 5; i++) {
      db.command("SQL", "INSERT INTO " + VERTEX1_TYPE_NAME + " SET id = ?, name = ?", (long) (20000 + i), "via-remote-before");
    }
    testLog("Phase 3: 5 vertices written via RemoteDatabase through follower");

    // Phase 4: Stop the leader
    final String leaderName = leader.getServerName();
    testLog("Stopping leader: %s", leaderName);
    leader.stop();

    // Phase 5: Wait for new leader election
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
    testLog("New leader elected");

    // Phase 6: Write via RemoteDatabase - the client should failover to the new leader
    final AtomicInteger successes = new AtomicInteger();
    for (int i = 0; i < 10; i++) {
      try {
        db.command("SQL", "INSERT INTO " + VERTEX1_TYPE_NAME + " SET id = ?, name = ?", (long) (30000 + i), "via-remote-after");
        successes.incrementAndGet();
      } catch (final Exception e) {
        testLog("Write %d after leader change failed: %s", i, e.getMessage());
      }
    }

    testLog("Phase 6: %d/10 writes via RemoteDatabase after leader change", successes.get());
    assertThat(successes.get()).as("RemoteDatabase should failover to new leader").isGreaterThanOrEqualTo(5);

    db.close();
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
    return new int[] { 1, 2 };
  }
}
