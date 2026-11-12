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
import com.arcadedb.event.BeforeRecordCreateListener;
import com.arcadedb.event.BeforeRecordUpdateListener;
import com.arcadedb.index.Index;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.MaterializedViewImpl;
import com.arcadedb.schema.MaterializedViewRefreshMode;
import com.arcadedb.utility.FileUtils;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Verifies that materialized view creation, querying, and deletion replicate correctly across a
 * 3-node Raft cluster. Schema changes (view creation and deletion) are issued on the leader and
 * propagated to all followers via the Raft log.
 */
class RaftReplicationMaterializedViewIT extends BaseRaftHATest {

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void materializedViewReplicates() throws Exception {
    // Find the leader - schema changes must be issued on the leader for Raft replication
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("Expected to find a Raft leader").isGreaterThanOrEqualTo(0);

    // Every database handle below is resolved fresh inside withResyncRetry() rather than cached
    // once up front: a bootstrap-mismatch snapshot reinstall can still be in flight right after
    // cluster startup (observed in #5668, CI run 30676355936 - a DatabaseIsClosedException out of
    // Database.getSchema() a few milliseconds into the test), and a handle resolved before that
    // point is exactly the stale-handle shape #5977 already documents on BaseRaftHATest.

    // Create source type on leader
    withResyncRetry(leaderIndex, db -> {
      db.getSchema().createDocumentType("RaftMetric");
      return null;
    });

    // Wait for schema replication before checking all servers
    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    for (int i = 0; i < getServerCount(); i++) {
      final boolean exists = withResyncRetry(i, db -> db.getSchema().existsType("RaftMetric"));
      assertThat(exists).as("All servers should have RaftMetric type").isTrue();
    }

    // Insert data on leader
    withResyncRetry(leaderIndex, db -> {
      db.transaction(() -> {
        db.newDocument("RaftMetric").set("name", "cpu").set("value", 80).save();
        db.newDocument("RaftMetric").set("name", "mem").set("value", 60).save();
      });
      return null;
    });

    // Wait for data replication
    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    // Create materialized view on leader
    withResyncRetry(leaderIndex, db -> db.getSchema().buildMaterializedView()
        .withName("RaftHighMetrics")
        .withQuery("SELECT name, value FROM RaftMetric WHERE value > 70")
        .create());

    // Wait for materialized view schema to replicate
    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    // Verify view exists on all servers
    for (int i = 0; i < getServerCount(); i++) {
      final boolean exists = withResyncRetry(i, db -> db.getSchema().existsMaterializedView("RaftHighMetrics"));
      assertThat(exists).as("All servers should have RaftHighMetrics materialized view").isTrue();
    }

    // Verify schema file contains the view definition on all servers
    for (int i = 0; i < getServerCount(); i++) {
      final String content = withResyncRetry(i, RaftReplicationMaterializedViewIT::readSchemaFile);
      assertThat(content).contains("RaftHighMetrics");
      assertThat(content).contains("materializedViews");
    }

    // Query view on a replica
    final int replicaIndex = (leaderIndex + 1) % getServerCount();
    final long viewRowCount = withResyncRetry(replicaIndex, db -> {
      try (final var rs = db.query("sql", "SELECT FROM RaftHighMetrics")) {
        return rs.stream().count();
      }
    });
    assertThat(viewRowCount).isEqualTo(1L);

    // Drop the view on leader
    withResyncRetry(leaderIndex, db -> {
      db.getSchema().dropMaterializedView("RaftHighMetrics");
      return null;
    });

    // Wait for drop replication
    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    // Verify view is gone on all servers
    for (int i = 0; i < getServerCount(); i++) {
      final boolean exists = withResyncRetry(i, db -> db.getSchema().existsMaterializedView("RaftHighMetrics"));
      assertThat(exists).as("All servers should not have RaftHighMetrics after drop").isFalse();
    }

    for (int i = 0; i < getServerCount(); i++)
      assertThat(withResyncRetry(i, RaftReplicationMaterializedViewIT::readSchemaFile)).doesNotContain("RaftHighMetrics");
  }

  /**
   * An ALTER ... REFRESH INCREMENTAL whose upgrade pass fails on the leader (issue #11475) still ships its schema change: the
   * frame returns normally, so every follower gets the source link, its UNIQUE index and the per-record flag the leader already
   * has, and the recovery pass a later commit gets made on the leader replicates to all of them.
   */
  @Test
  void failedUpgradePassStillReplicatesTheSchemaChange() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("Expected to find a Raft leader").isGreaterThanOrEqualTo(0);
    final String query = "SELECT name, age FROM RaftPerson WHERE age >= 18";

    withResyncRetry(leaderIndex, db -> {
      db.getSchema().createDocumentType("RaftPerson");
      db.transaction(() -> {
        for (int i = 0; i < 20; i++)
          db.newDocument("RaftPerson").set("name", "p" + i).set("age", 10 + i).save();
      });
      db.getSchema().buildMaterializedView().withName("RaftAdults").withQuery(query)
          .withRefreshMode(MaterializedViewRefreshMode.MANUAL).create();
      return null;
    });
    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    final Exception failure = withResyncRetry(leaderIndex, db -> {
      final DocumentType backing = db.getSchema().getType("RaftAdults");
      final BeforeRecordCreateListener boomOnCreate = record -> {
        throw new IllegalStateException("simulated failure of the upgrade pass");
      };
      final BeforeRecordUpdateListener boomOnUpdate = record -> {
        throw new IllegalStateException("simulated failure of the upgrade pass");
      };
      backing.getEvents().registerListener(boomOnCreate);
      backing.getEvents().registerListener(boomOnUpdate);
      try {
        db.getSchema().alterMaterializedView("RaftAdults", MaterializedViewRefreshMode.INCREMENTAL, 0);
        return null;
      } catch (final Exception e) {
        return e;
      } finally {
        backing.getEvents().unregisterListener(boomOnCreate);
        backing.getEvents().unregisterListener(boomOnUpdate);
      }
    });
    assertThat(failure).as("the ALTER reports the failed upgrade pass").isNotNull();
    assertThat(failure.getMessage()).contains("was switched to REFRESH INCREMENTAL");

    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);
    for (int i = 0; i < getServerCount(); i++) {
      final int server = i;
      final boolean perRecord = withResyncRetry(server,
          db -> ((MaterializedViewImpl) db.getSchema().getMaterializedView("RaftAdults")).isPerRecord());
      assertThat(perRecord).as("server %d has the per-record flag", server).isTrue();
      final boolean indexed = withResyncRetry(server, RaftReplicationMaterializedViewIT::hasSourceIndex);
      assertThat(indexed).as("server %d has the UNIQUE index on the source link", server).isTrue();
    }

    // A commit on the leader gets the recovery full pass made, and that pass replicates like any other
    withResyncRetry(leaderIndex, db -> {
      db.transaction(() -> db.newDocument("RaftPerson").set("name", "late").set("age", 40).save());
      return null;
    });
    for (int i = 0; i < getServerCount(); i++) {
      final int server = i;
      Awaitility.await("server " + server + " holds the recovered view").atMost(60, TimeUnit.SECONDS)
          .pollInterval(200, TimeUnit.MILLISECONDS)
          .untilAsserted(() -> {
            final long linked = withResyncRetry(server, RaftReplicationMaterializedViewIT::linkedRowCount);
            final long expected = withResyncRetry(server, db -> count(db, "SELECT count(*) AS c FROM (" + query + ")"));
            assertThat(linked).isEqualTo(expected);
          });
    }
  }

  private static boolean hasSourceIndex(final Database database) {
    for (final Index index : database.getSchema().getType("RaftAdults").getAllIndexes(false))
      if (index.isUnique() && index.getPropertyNames().equals(List.of("_sourceRID")))
        return true;
    return false;
  }

  /** The view rows that carry the source link; -1 when some row lacks it, so a half-recovered view never matches. */
  private static long linkedRowCount(final Database database) {
    long linked = 0;
    try (final ResultSet rs = database.query("sql", "SELECT _sourceRID FROM RaftAdults")) {
      while (rs.hasNext()) {
        if (rs.next().getProperty("_sourceRID") == null)
          return -1;
        ++linked;
      }
    }
    return linked;
  }

  private static long count(final Database database, final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  private static String readSchemaFile(final Database database) {
    try {
      return FileUtils.readFileAsString(database.getSchema().getEmbedded().getConfigurationFile());
    } catch (final IOException e) {
      fail("Cannot read schema file for " + database.getDatabasePath(), e);
      return null;
    }
  }
}
