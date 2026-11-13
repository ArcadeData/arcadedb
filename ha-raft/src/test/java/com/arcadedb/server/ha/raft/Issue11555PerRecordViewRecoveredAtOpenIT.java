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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.MaterializedViewImpl;
import com.arcadedb.schema.MaterializedViewRefreshMode;
import com.arcadedb.schema.MaterializedViewStatus;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerDatabase;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11555: the node that becomes the leader recovers the per-record materialized views of the databases open on it when it
 * takes the leadership (issue #11550). A database it opens only later - not loaded at startup, or closed and opened again - was
 * never recovered: the open-time recovery is skipped on every HA node, and the leadership hook had already run. The open on the
 * ready leader now makes that recovery.
 */
@Tag("slow")
class Issue11555PerRecordViewRecoveredAtOpenIT extends BaseRaftHATest {
  private static final String QUERY = "SELECT name, age FROM RaftPerson WHERE age >= 18";

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "majority");
  }

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected void populateDatabase() {
  }

  @Test
  void aDatabaseTheNewLeaderOpensAfterTheLeaderChangeIsRecoveredAtItsOpen() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final Database leader = getServerDatabase(leaderIndex, getDatabaseName());

    leader.getSchema().createDocumentType("RaftPerson");
    leader.transaction(() -> {
      for (int i = 0; i < 20; i++)
        leader.newDocument("RaftPerson").set("name", "p" + i).set("age", 10 + i).save();
    });
    leader.getSchema().buildMaterializedView().withName("RaftAdults").withQuery(QUERY)
        .withRefreshMode(MaterializedViewRefreshMode.INCREMENTAL).create();
    final MaterializedViewImpl leaderView = (MaterializedViewImpl) leader.getSchema().getMaterializedView("RaftAdults");
    assertThat(leaderView.isPerRecord()).isTrue();
    waitForAllServers();

    // The leader owns the refresh, as a pass still running would: the commit replicates, its RIDs wait in the leader's pending
    // delta for a further pass that the stop below takes down
    assertThat(leaderView.tryBeginRefresh()).isTrue();
    leader.transaction(() -> {
      leader.command("sql", "UPDATE RaftPerson SET age = 99 WHERE name = 'p3'");
      leader.command("sql", "DELETE FROM RaftPerson WHERE name = 'p15'");
    });
    waitForAllServers();
    for (int i = 0; i < getServerCount(); i++) {
      final int server = i;
      final List<String> rows = withResyncRetry(server, db -> rows(db, "SELECT name, age FROM RaftAdults"));
      assertThat(rows).as("server %d: the pass of the commit has not run", server).doesNotContain("p3/99").contains("p15/25");
    }

    // Not open on any survivor when the leadership changes: the leadership hook finds nothing to recover
    for (int i = 0; i < getServerCount(); i++)
      if (i != leaderIndex)
        closeAndDeregister(i);

    getServer(leaderIndex).stop();
    final int newLeaderIndex = waitForNewLeader(leaderIndex);
    assertThat(newLeaderIndex).as("a new leader must be elected").isGreaterThanOrEqualTo(0).isNotEqualTo(leaderIndex);

    // The leadership hook has run, over no open database, before the database is opened
    final RaftHAServer newLeaderRaft = getRaftPlugin(newLeaderIndex).getRaftHAServer();
    Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS).until(newLeaderRaft::isLeaderReady);
    awaitLifecycleExecutorIdle(newLeaderRaft);
    assertThat(getServer(newLeaderIndex).getDatabaseNames()).as("nothing opened the database on the new leader")
        .doesNotContain(getDatabaseName());

    // The other survivor first, so the rows the recovery rewrites replicate to an open copy; it is not the leader: no recovery
    for (int i = 0; i < getServerCount(); i++)
      if (i != leaderIndex && i != newLeaderIndex)
        getServer(i).getDatabaseForOperator(getDatabaseName());
    getServer(newLeaderIndex).getDatabaseForOperator(getDatabaseName());

    // No REFRESH: the open on the ready leader handed the view its recovery full pass
    Awaitility.await().atMost(60, TimeUnit.SECONDS).pollInterval(200, TimeUnit.MILLISECONDS).until(() -> {
      final List<String> rows = withResyncRetry(newLeaderIndex, db -> rows(db, "SELECT name, age FROM RaftAdults"));
      final String status = withResyncRetry(newLeaderIndex, db -> db.getSchema().getMaterializedView("RaftAdults").getStatus());
      return MaterializedViewStatus.VALID.name().equals(status) && rows.contains("p3/99") && !rows.contains("p15/25");
    });

    waitForReplicationIsCompleted(newLeaderIndex);
    for (int i = 0; i < getServerCount(); i++) {
      if (i == leaderIndex)
        continue;
      final int server = i;
      Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(200, TimeUnit.MILLISECONDS).untilAsserted(() -> {
        final List<String> view = withResyncRetry(server, db -> rows(db, "SELECT name, age FROM RaftAdults"));
        final List<String> query = withResyncRetry(server, db -> rows(db, QUERY));
        assertThat(view).as("server %d: the view equals its query", server).containsExactlyInAnyOrderElementsOf(query);
      });
    }
  }

  private static List<String> rows(final Database db, final String sql) {
    final List<String> rows = new ArrayList<>();
    try (final ResultSet rs = db.query("sql", sql)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        rows.add(row.getProperty("name") + "/" + row.getProperty("age"));
      }
    }
    return rows;
  }

  /** As a database not loaded at startup is: not open and not registered, opened by the next request that names it. */
  private void closeAndDeregister(final int serverIndex) {
    final ArcadeDBServer server = getServer(serverIndex);
    final ServerDatabase database = server.getDatabaseForOperator(getDatabaseName());
    database.getEmbedded().close();
    server.removeDatabase(getDatabaseName());
  }

  /** Waits for the leadership hook queued on the state machine's single-threaded lifecycle executor to have run. */
  private static void awaitLifecycleExecutorIdle(final RaftHAServer raft) throws Exception {
    final Field field = ArcadeStateMachine.class.getDeclaredField("lifecycleExecutor");
    field.setAccessible(true);
    final ThreadPoolExecutor executor = (ThreadPoolExecutor) field.get(raft.getStateMachine());
    final CountDownLatch drained = new CountDownLatch(1);
    executor.submit(drained::countDown);
    assertThat(drained.await(30, TimeUnit.SECONDS)).as("the lifecycle executor drains").isTrue();
  }

  @Override
  protected void checkDatabasesAreIdentical() {
    // One server is stopped on purpose; the assertions above are about the survivors
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
