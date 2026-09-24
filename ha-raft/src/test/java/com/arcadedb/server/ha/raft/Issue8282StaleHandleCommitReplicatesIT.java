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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerDatabase;
import com.arcadedb.utility.CodeUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #8282: a {@link ServerDatabase} resolved on a starting node before its Raft plugin wraps
 * the databases holds the plain {@code LocalDatabase}. The Postgres executor resolves its handle once per connection
 * and keeps it, as do the MongoDB, Bolt and gRPC ones, so a connection opened in that window used to commit every
 * later transaction through {@code LocalDatabase.commit()}: applied on that node, never sent through Raft.
 * <p>
 * The handle is resolved while the restarting follower's plugin is held before the wrap
 * ({@link RaftHAPlugin#TEST_BEFORE_START_HOOK}); the transaction begins and commits after the node is up, which is the
 * case the write refusal of #8270 does not cover. The contract: that write is on the leader and on every node.
 */
@Tag("slow")
class Issue8282StaleHandleCommitReplicatesIT extends BaseRaftHATest {

  private static final String TYPE_NAME = "StaleHandleProbe";

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected boolean persistentRaftStorage() {
    return true;
  }

  @Override
  protected void populateDatabase() {
    // The schema is created in the test, through the leader.
  }

  @Test
  void aCommitOnAHandleResolvedBeforeTheWrapReachesTheCluster() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    leaderDb.command("sql", "CREATE DOCUMENT TYPE " + TYPE_NAME);
    leaderDb.command("sql", "CREATE PROPERTY " + TYPE_NAME + ".name STRING");
    waitForAllServers();

    final int restarted = leaderIndex == getServerCount() - 1 ? getServerCount() - 2 : getServerCount() - 1;
    final ArcadeDBServer server = getServer(restarted);

    server.stop();
    while (server.getStatus() == ArcadeDBServer.STATUS.SHUTTING_DOWN)
      CodeUtils.sleep(100);

    final AtomicReference<ServerDatabase> staleHandle = new AtomicReference<>();
    RaftHAPlugin.TEST_BEFORE_START_HOOK = starting -> {
      if (starting == server)
        staleHandle.set(starting.getDatabase(getDatabaseName()));
    };
    try {
      server.start();
    } finally {
      RaftHAPlugin.TEST_BEFORE_START_HOOK = null;
    }

    final ServerDatabase handle = staleHandle.get();
    assertThat(handle).as("the handle must have been resolved while node %d was held before the wrap", restarted).isNotNull();
    assertThat(handle).as("the registry must hold a different, wrapped handle by now")
        .isNotSameAs(server.getDatabase(getDatabaseName()));
    waitForAllServers();

    // What a Postgres connection opened in the window does next, on its own thread, for its whole lifetime.
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final Thread connection = new Thread(() -> {
      try {
        DatabaseContext.INSTANCE.init(handle);
        handle.begin();
        handle.newDocument(TYPE_NAME).set("name", "stale-handle").save();
        handle.commit();
      } catch (final Throwable t) {
        failure.set(t);
      }
    }, "issue8282-connection");
    connection.start();
    connection.join(30_000);
    assertThat(failure.get()).as("the commit on the stale handle").isNull();

    waitForAllServers();

    final Database leader = getServerDatabase(findLeaderIndex(), getDatabaseName());
    assertThat(leader.query("sql", "SELECT FROM " + TYPE_NAME + " WHERE name = 'stale-handle'").hasNext())
        .as("a write committed on node %d through a handle resolved before the wrap must reach the leader", restarted)
        .isTrue();

    for (int i = 0; i < getServerCount(); i++)
      assertThat(getServerDatabase(i, getDatabaseName()).countType(TYPE_NAME, true))
          .as("count of node %d", i).isEqualTo(1L);

    assertClusterConsistency();
  }
}
