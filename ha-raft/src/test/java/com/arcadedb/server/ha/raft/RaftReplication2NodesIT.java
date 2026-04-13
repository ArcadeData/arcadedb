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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.ServerDatabase;
import com.arcadedb.server.ha.raft.ReplicatedDatabase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test: 2-node cluster with no-quorum replication.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("IntegrationTest")
class RaftReplication2NodesIT extends BaseGraphServerTest {

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    config.setValue(GlobalConfiguration.HA_QUORUM, "all");
  }

  @Override
  protected int getServerCount() {
    return 2;
  }

  @Test
  void basicReplicationBetween2Nodes() {
    final int leaderIndex = getLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final var db = getServerDatabase(leaderIndex, getDatabaseName());

    // Verify that the database is wrapped with ReplicatedDatabase
    assertThat(db).isInstanceOf(ServerDatabase.class);
    final DatabaseInternal wrapped = ((ServerDatabase) db).getWrappedDatabaseInstance();
    assertThat(wrapped).isInstanceOf(ReplicatedDatabase.class);

    // Use "RaftPerson" to avoid conflict with "Person" document type created by BaseGraphServerTest
    db.transaction(() -> {
      if (!db.getSchema().existsType("RaftPerson"))
        db.getSchema().createVertexType("RaftPerson");
    });

    db.transaction(() -> {
      for (int i = 0; i < 100; i++) {
        final MutableVertex v = db.newVertex("RaftPerson");
        v.set("name", "person-" + i);
        v.set("idx", i);
        v.save();
      }
    });

    waitForReplicationConvergence();

    // Verify replication on the other server
    final int replicaIndex = leaderIndex == 0 ? 1 : 0;
    final var replicaDb = getServerDatabase(replicaIndex, getDatabaseName());
    final long count = replicaDb.countType("RaftPerson", true);
    assertThat(count).isEqualTo(100);
  }
}
