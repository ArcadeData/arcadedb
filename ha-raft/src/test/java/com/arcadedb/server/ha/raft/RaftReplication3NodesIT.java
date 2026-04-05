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
package com.arcadedb.server.ha.raft;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.server.ServerDatabase;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test: 3-node cluster with majority quorum replication.
 */
class RaftReplication3NodesIT extends BaseRaftHATest {

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "majority");
  }

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void basicReplicationWith3Nodes() {
    // Find the leader server
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final var db = getServerDatabase(leaderIndex, getDatabaseName());

    // Verify that the database is wrapped with RaftReplicatedDatabase, not the legacy ReplicatedDatabase
    assertThat(db).isInstanceOf(ServerDatabase.class);
    final DatabaseInternal wrapped = ((ServerDatabase) db).getWrappedDatabaseInstance();
    assertThat(wrapped).isInstanceOf(RaftReplicatedDatabase.class);

    // Use "RaftProduct" to avoid conflict with base test types
    db.transaction(() -> {
      if (!db.getSchema().existsType("RaftProduct"))
        db.getSchema().createVertexType("RaftProduct");
    });

    db.transaction(() -> {
      for (int i = 0; i < 500; i++) {
        final MutableVertex v = db.newVertex("RaftProduct");
        v.set("name", "product-" + i);
        v.set("idx", i);
        v.save();
      }
    });

    assertClusterConsistency();

    // Verify replication on all 3 nodes
    for (int i = 0; i < getServerCount(); i++) {
      final var nodeDb = getServerDatabase(i, getDatabaseName());
      final long count = nodeDb.countType("RaftProduct", true);
      assertThat(count).as("Server " + i + " should have 500 RaftProduct records").isEqualTo(500);
    }
  }

  @Test
  void schemaReplicationWith3Nodes() {
    // Find the leader server
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final var db = getServerDatabase(leaderIndex, getDatabaseName());

    // Use "RaftCustomer" to avoid conflict with base test types
    db.transaction(() -> {
      if (!db.getSchema().existsType("RaftCustomer")) {
        final var customerType = db.getSchema().createVertexType("RaftCustomer");
        customerType.createProperty("email", Type.STRING);
        customerType.createProperty("age", Type.INTEGER);
        customerType.createProperty("active", Type.BOOLEAN);
      }
    });

    assertClusterConsistency();

    // Verify schema exists on all 3 nodes
    for (int i = 0; i < getServerCount(); i++) {
      final var nodeDb = getServerDatabase(i, getDatabaseName());
      final Schema schema = nodeDb.getSchema();

      assertThat(schema.existsType("RaftCustomer"))
        .as("Server " + i + " should have RaftCustomer type")
        .isTrue();

      final var customerType = schema.getType("RaftCustomer");
      assertThat(customerType.existsProperty("email"))
        .as("Server " + i + " should have email property")
        .isTrue();
      assertThat(customerType.existsProperty("age"))
        .as("Server " + i + " should have age property")
        .isTrue();
      assertThat(customerType.existsProperty("active"))
        .as("Server " + i + " should have active property")
        .isTrue();
    }
  }
}
