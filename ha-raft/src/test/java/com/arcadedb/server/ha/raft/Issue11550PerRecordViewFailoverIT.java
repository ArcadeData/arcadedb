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
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11550: a per-record materialized view's pending delta lives in the leader's memory only. A leader stopped with a delta
 * still pending took it down with it, and the commit is in every survivor's source type but in no view. The node that becomes
 * leader hands every per-record view a recovery full pass, which converges the view and replicates the rows it rewrote.
 */
@Tag("slow")
class Issue11550PerRecordViewFailoverIT extends BaseRaftHATest {
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
  void theNewLeaderConvergesAViewWhosePendingDeltaDiedWithTheOldLeader() {
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

    getServer(leaderIndex).stop();
    final int newLeaderIndex = waitForNewLeader(leaderIndex);
    assertThat(newLeaderIndex).as("a new leader must be elected").isGreaterThanOrEqualTo(0).isNotEqualTo(leaderIndex);

    // No REFRESH: the leadership acquisition handed the view its recovery full pass
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
