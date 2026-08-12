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
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;

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

  private static String readSchemaFile(final Database database) {
    try {
      return FileUtils.readFileAsString(database.getSchema().getEmbedded().getConfigurationFile());
    } catch (final IOException e) {
      fail("Cannot read schema file for " + database.getDatabasePath(), e);
      return null;
    }
  }
}
