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
    int leaderIndexTmp = -1;
    for (int i = 0; i < getServerCount(); i++) {
      final RaftHAPlugin plugin = getRaftPlugin(i);
      if (plugin != null && plugin.isLeader()) {
        leaderIndexTmp = i;
        break;
      }
    }
    assertThat(leaderIndexTmp).as("Expected to find a Raft leader").isGreaterThanOrEqualTo(0);
    final int leaderIndex = leaderIndexTmp;

    final Database[] databases = new Database[getServerCount()];
    for (int i = 0; i < getServerCount(); i++) {
      databases[i] = getServer(i).getDatabase(getDatabaseName());
      if (databases[i].isTransactionActive())
        databases[i].commit();
    }

    // Create source type and insert data on leader
    databases[leaderIndex].getSchema().createDocumentType("RaftMetric");

    // Wait for schema replication before checking all servers
    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    for (final Database db : databases)
      assertThat(db.getSchema().existsType("RaftMetric"))
          .as("All servers should have RaftMetric type").isTrue();

    databases[leaderIndex].transaction(() -> {
      databases[leaderIndex].newDocument("RaftMetric").set("name", "cpu").set("value", 80).save();
      databases[leaderIndex].newDocument("RaftMetric").set("name", "mem").set("value", 60).save();
    });

    // Wait for data replication
    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    // Create materialized view on leader
    databases[leaderIndex].getSchema().buildMaterializedView()
        .withName("RaftHighMetrics")
        .withQuery("SELECT name, value FROM RaftMetric WHERE value > 70")
        .create();

    // Wait for materialized view schema to replicate
    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    // Verify view exists on all servers
    for (final Database db : databases)
      assertThat(db.getSchema().existsMaterializedView("RaftHighMetrics"))
          .as("All servers should have RaftHighMetrics materialized view").isTrue();

    // Verify schema file contains the view definition on all servers
    for (final Database db : databases) {
      final String content = readSchemaFile(db);
      assertThat(content).contains("RaftHighMetrics");
      assertThat(content).contains("materializedViews");
    }

    // Query view on a replica
    final int replicaIndex = (leaderIndex + 1) % getServerCount();
    try (final var rs = databases[replicaIndex].query("sql", "SELECT FROM RaftHighMetrics")) {
      assertThat(rs.stream().count()).isEqualTo(1L);
    }

    // Drop the view on leader
    databases[leaderIndex].getSchema().dropMaterializedView("RaftHighMetrics");

    // Wait for drop replication
    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    // Verify view is gone on all servers
    for (final Database db : databases)
      assertThat(db.getSchema().existsMaterializedView("RaftHighMetrics"))
          .as("All servers should not have RaftHighMetrics after drop").isFalse();

    for (final Database db : databases)
      assertThat(readSchemaFile(db)).doesNotContain("RaftHighMetrics");
  }

  private String readSchemaFile(final Database database) {
    try {
      return FileUtils.readFileAsString(database.getSchema().getEmbedded().getConfigurationFile());
    } catch (final IOException e) {
      fail("Cannot read schema file for " + database.getDatabasePath(), e);
      return null;
    }
  }
}
