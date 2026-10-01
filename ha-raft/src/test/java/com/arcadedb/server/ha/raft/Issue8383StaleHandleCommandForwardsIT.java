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
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerDatabase;
import com.arcadedb.utility.CodeUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #8383: the #8282 fix routed only {@code commit()} of a stale {@link ServerDatabase} through
 * the database's current Raft wrapper. A handle resolved on a starting follower before its Raft plugin wraps the
 * databases - which a Postgres, MongoDB, Bolt or gRPC connection keeps for its whole lifetime - still ran
 * {@code command()} on the plain {@code LocalDatabase}, so it never reached {@code RaftReplicatedDatabase.command()},
 * the one place where a follower forwards a write or a DDL statement to the leader instead of executing it on its own
 * state (issue #4039).
 * <p>
 * A DDL statement makes the difference observable: forwarded, the leader runs it and every node gets the new type;
 * executed on the follower, it gets as far as the schema write and is refused there ("Changes to the schema must be
 * executed on the leader server"), so the connection sees an error for a statement any fresh handle forwards.
 */
@Tag("slow")
class Issue8383StaleHandleCommandForwardsIT extends BaseRaftHATest {

  private static final String TYPE_NAME = "StaleHandleForwardProbe";

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
    // The schema is created in the test, through the stale handle.
  }

  @Test
  void aCommandOnAHandleResolvedBeforeTheWrapIsForwardedToTheLeader() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);

    // A first write through the leader concludes the cluster's first-formation bootstrap, which otherwise refuses a
    // forwarded statement with a NeedRetryException.
    getServerDatabase(leaderIndex, getDatabaseName()).command("sql", "CREATE DOCUMENT TYPE " + TYPE_NAME + "Warmup");
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

    // The restarted node can win an election on its way back; hand leadership off, since what is tested is a follower.
    for (int attempt = 0; attempt < 5 && findLeaderIndex() == restarted; attempt++) {
      getRaftPlugin(restarted).getRaftHAServer().transferLeadership(10_000);
      waitForAllServers();
    }
    final int currentLeader = findLeaderIndex();
    assertThat(currentLeader).as("node %d must still be a follower for the forward to be what is tested", restarted)
        .isNotEqualTo(restarted);

    // What a Postgres connection opened in the window does next, on its own thread, for its whole lifetime.
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final Thread connection = new Thread(() -> {
      try {
        DatabaseContext.INSTANCE.init(handle);
        runRetrying(() -> handle.command("sql", "CREATE DOCUMENT TYPE " + TYPE_NAME).close());
        runRetrying(() -> handle.command("sql", "INSERT INTO " + TYPE_NAME + " SET name = 'stale-handle'").close());
      } catch (final Throwable t) {
        failure.set(t);
      }
    }, "issue8383-connection");
    connection.setDaemon(true);
    connection.start();
    connection.join(30_000);
    assertThat(connection.isAlive()).as("the commands on the stale handle must complete").isFalse();
    assertThat(failure.get()).as("the commands on the stale handle").isNull();

    waitForAllServers();

    final Database leader = getServerDatabase(findLeaderIndex(), getDatabaseName());
    assertThat(leader.getSchema().existsType(TYPE_NAME))
        .as("a DDL statement issued on follower %d through a handle resolved before the wrap must be forwarded to the leader",
            restarted)
        .isTrue();

    for (int i = 0; i < getServerCount(); i++) {
      final Database db = getServerDatabase(i, getDatabaseName());
      assertThat(db.getSchema().existsType(TYPE_NAME)).as("type on node %d", i).isTrue();
      assertThat(db.countType(TYPE_NAME, true)).as("count of node %d", i).isEqualTo(1L);
    }

    assertClusterConsistency();
  }

  /** What a client does with a NeedRetryException the restarted node raises while it is still catching up. */
  private static void runRetrying(final Runnable statement) {
    for (int attempt = 1; ; attempt++) {
      try {
        statement.run();
        return;
      } catch (final NeedRetryException e) {
        if (attempt >= 60)
          throw e;
        CodeUtils.sleep(500);
      }
    }
  }
}
