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
package com.arcadedb.server.ha;

import com.arcadedb.database.Database;
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.ReplicationCallback;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that a follower receives the REPLICA_HOT_RESYNC callback after catching up
 * via Raft log replay (not snapshot). Stops a follower, writes data on the leader,
 * restarts the follower, and verifies it catches up and fires the callback.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ReplicationServerReplicaHotResyncIT extends BaseGraphServerTest {
  private final CountDownLatch hotResyncLatch  = new CountDownLatch(1);
  private final CountDownLatch fullResyncLatch = new CountDownLatch(1);

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected void onBeforeStarting(final ArcadeDBServer server) {
    server.registerTestEventListener((type, object, s) -> {
      if (type == ReplicationCallback.TYPE.REPLICA_HOT_RESYNC) {
        LogManager.instance().log(this, Level.INFO, "TEST: Received REPLICA_HOT_RESYNC on %s", s.getServerName());
        hotResyncLatch.countDown();
      } else if (type == ReplicationCallback.TYPE.REPLICA_FULL_RESYNC) {
        LogManager.instance().log(this, Level.INFO, "TEST: Received REPLICA_FULL_RESYNC on %s", s.getServerName());
        fullResyncLatch.countDown();
      }
    });
  }

  @Test
  void hotResyncAfterFollowerRestart() throws Exception {
    final ArcadeDBServer leader = getLeaderServer();
    assertThat(leader).isNotNull();

    // Find a follower (not the leader)
    int followerIndex = -1;
    for (int i = 0; i < getServerCount(); i++)
      if (getServer(i) != leader) {
        followerIndex = i;
        break;
      }
    assertThat(followerIndex).isGreaterThanOrEqualTo(0);

    // Write some data to establish a baseline
    final Database leaderDb = leader.getDatabase(getDatabaseName());
    for (int i = 0; i < 5; i++) {
      final int idx = i;
      leaderDb.transaction(() -> leaderDb.newVertex(VERTEX1_TYPE_NAME).set("id", 20000L + idx).set("name", "pre-stop").save());
    }
    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    // Stop the follower
    LogManager.instance().log(this, Level.INFO, "TEST: Stopping follower %d...", followerIndex);
    final ArcadeDBServer follower = getServer(followerIndex);
    follower.stop();

    // Write more data while follower is down (these will be replayed via Raft log)
    for (int i = 0; i < 20; i++) {
      final int idx = i;
      leaderDb.transaction(() -> leaderDb.newVertex(VERTEX1_TYPE_NAME).set("id", 30000L + idx).set("name", "during-stop").save());
    }

    // Restart the follower - it should catch up via Raft log replay (hot resync)
    LogManager.instance().log(this, Level.INFO, "TEST: Restarting follower %d...", followerIndex);
    follower.start();

    // Wait for hot resync callback
    assertThat(hotResyncLatch.await(30, TimeUnit.SECONDS))
        .as("REPLICA_HOT_RESYNC should have been received")
        .isTrue();

    // Full resync should NOT have been triggered (follower caught up via log, not snapshot)
    assertThat(fullResyncLatch.await(2, TimeUnit.SECONDS))
        .as("REPLICA_FULL_RESYNC should NOT have been received")
        .isFalse();

    // Verify data is consistent
    Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> {
      final Database followerDb = follower.getDatabase(getDatabaseName());
      return followerDb.countType(VERTEX1_TYPE_NAME, true) >= 1 + 5 + 20;
    });
  }

  @Override
  protected int[] getServerToCheck() {
    return new int[] {};
  }
}
