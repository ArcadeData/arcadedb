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
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test: 2-node cluster with none quorum.
 * Tests schema and index replication: vertex types, edge types, properties, and indexes
 * are replicated correctly to all nodes.
 */
class RaftSchemaReplicationIT extends BaseRaftHATest {

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "none");
  }

  @Override
  protected int getServerCount() {
    return 2;
  }

  @Test
  void vertexTypeWithPropertiesIsReplicated() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int replicaIndex = leaderIndex == 0 ? 1 : 0;

    final var leaderDb = getServerDatabase(leaderIndex, getDatabaseName());

    // Create a vertex type with multiple properties
    leaderDb.transaction(() -> {
      final var type = leaderDb.getSchema().createVertexType("RaftEmployee");
      type.createProperty("firstName", Type.STRING);
      type.createProperty("lastName", Type.STRING);
      type.createProperty("age", Type.INTEGER);
      type.createProperty("salary", Type.DOUBLE);
      type.createProperty("active", Type.BOOLEAN);
      type.createProperty("hireDate", Type.DATE);
    });

    assertClusterConsistency();

    // Verify on replica
    final var replicaDb = getServerDatabase(replicaIndex, getDatabaseName());
    final Schema replicaSchema = replicaDb.getSchema();

    assertThat(replicaSchema.existsType("RaftEmployee")).as("Replica should have RaftEmployee type").isTrue();

    final var employeeType = replicaSchema.getType("RaftEmployee");
    assertThat(employeeType.existsProperty("firstName")).isTrue();
    assertThat(employeeType.existsProperty("lastName")).isTrue();
    assertThat(employeeType.existsProperty("age")).isTrue();
    assertThat(employeeType.existsProperty("salary")).isTrue();
    assertThat(employeeType.existsProperty("active")).isTrue();
    assertThat(employeeType.existsProperty("hireDate")).isTrue();

    assertThat(employeeType.getProperty("firstName").getType()).isEqualTo(Type.STRING);
    assertThat(employeeType.getProperty("age").getType()).isEqualTo(Type.INTEGER);
    assertThat(employeeType.getProperty("salary").getType()).isEqualTo(Type.DOUBLE);
    assertThat(employeeType.getProperty("active").getType()).isEqualTo(Type.BOOLEAN);
  }

  @Test
  void edgeTypeIsReplicated() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int replicaIndex = leaderIndex == 0 ? 1 : 0;

    final var leaderDb = getServerDatabase(leaderIndex, getDatabaseName());

    // Create vertex and edge types
    leaderDb.transaction(() -> {
      final var personType = leaderDb.getSchema().createVertexType("RaftAuthor");
      personType.createProperty("name", Type.STRING);

      final var bookType = leaderDb.getSchema().createVertexType("RaftBook");
      bookType.createProperty("title", Type.STRING);
      bookType.createProperty("year", Type.INTEGER);

      final var wroteType = leaderDb.getSchema().createEdgeType("RaftWrote");
      wroteType.createProperty("role", Type.STRING);
    });

    // Insert data using the types
    leaderDb.transaction(() -> {
      final MutableVertex author = leaderDb.newVertex("RaftAuthor");
      author.set("name", "Jane Austen");
      author.save();

      final MutableVertex book = leaderDb.newVertex("RaftBook");
      book.set("title", "Pride and Prejudice");
      book.set("year", 1813);
      book.save();

      author.newEdge("RaftWrote", book, "role", "author");
    });

    assertClusterConsistency();

    // Verify types on replica
    final var replicaDb = getServerDatabase(replicaIndex, getDatabaseName());
    final Schema replicaSchema = replicaDb.getSchema();

    assertThat(replicaSchema.existsType("RaftAuthor")).as("Replica should have RaftAuthor type").isTrue();
    assertThat(replicaSchema.existsType("RaftBook")).as("Replica should have RaftBook type").isTrue();
    assertThat(replicaSchema.existsType("RaftWrote")).as("Replica should have RaftWrote type").isTrue();

    assertThat(replicaSchema.getType("RaftWrote").existsProperty("role")).isTrue();

    // Verify data on replica
    assertThat(replicaDb.countType("RaftAuthor", true)).isEqualTo(1);
    assertThat(replicaDb.countType("RaftBook", true)).isEqualTo(1);
    assertThat(replicaDb.countType("RaftWrote", true)).isEqualTo(1);
  }

  @Test
  void indexSchemaIsReplicated() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int replicaIndex = leaderIndex == 0 ? 1 : 0;

    final var leaderDb = getServerDatabase(leaderIndex, getDatabaseName());

    // Create type with properties
    leaderDb.transaction(() -> {
      final var type = leaderDb.getSchema().createVertexType("RaftIndexed");
      type.createProperty("code", Type.STRING);
      type.createProperty("value", Type.INTEGER);
    });

    // Create index in a separate transaction
    leaderDb.transaction(() -> {
      leaderDb.getSchema().getType("RaftIndexed")
          .createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "code");
    });

    assertClusterConsistency();

    // Verify index schema exists on replica
    final var replicaDb = getServerDatabase(replicaIndex, getDatabaseName());
    final Schema replicaSchema = replicaDb.getSchema();

    assertThat(replicaSchema.existsType("RaftIndexed")).as("Replica should have RaftIndexed type").isTrue();

    final var indexedType = replicaSchema.getType("RaftIndexed");
    assertThat(indexedType.existsProperty("code")).isTrue();
    assertThat(indexedType.existsProperty("value")).isTrue();

    // Verify the index exists by checking indexes on the type
    final var indexes = indexedType.getAllIndexes(true);
    assertThat(indexes).as("RaftIndexed should have at least one index on replica").isNotEmpty();

    // Note: Data insertion with indexes and subsequent byte-level comparison
    // is tested by the existing replication tests. This test focuses on verifying
    // that the index schema definition is replicated correctly.
  }

  @Test
  void multipleTypesInSingleTransaction() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int replicaIndex = leaderIndex == 0 ? 1 : 0;

    final var leaderDb = getServerDatabase(leaderIndex, getDatabaseName());

    // Create multiple types in a single transaction
    leaderDb.transaction(() -> {
      final var type1 = leaderDb.getSchema().createVertexType("RaftCity");
      type1.createProperty("name", Type.STRING);
      type1.createProperty("population", Type.LONG);

      final var type2 = leaderDb.getSchema().createVertexType("RaftCountry");
      type2.createProperty("name", Type.STRING);
      type2.createProperty("code", Type.STRING);

      final var edgeType = leaderDb.getSchema().createEdgeType("RaftLocatedIn");
    });

    assertClusterConsistency();

    // Verify all types on replica
    final var replicaDb = getServerDatabase(replicaIndex, getDatabaseName());
    final Schema replicaSchema = replicaDb.getSchema();

    assertThat(replicaSchema.existsType("RaftCity")).isTrue();
    assertThat(replicaSchema.existsType("RaftCountry")).isTrue();
    assertThat(replicaSchema.existsType("RaftLocatedIn")).isTrue();

    assertThat(replicaSchema.getType("RaftCity").existsProperty("name")).isTrue();
    assertThat(replicaSchema.getType("RaftCity").existsProperty("population")).isTrue();
    assertThat(replicaSchema.getType("RaftCountry").existsProperty("name")).isTrue();
    assertThat(replicaSchema.getType("RaftCountry").existsProperty("code")).isTrue();
  }
}
