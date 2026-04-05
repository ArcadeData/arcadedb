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
import com.arcadedb.index.Index;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.TypeLSMVectorIndexBuilder;
import com.arcadedb.schema.VertexType;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

class RaftIndexCompactionReplicationIT extends BaseRaftHATest {

  private static final int TOTAL_RECORDS = 5_000;
  private static final int TX_CHUNK      = 500;

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM_TIMEOUT, 30_000L);
  }

  @Override
  protected void populateDatabase() {
  }

  /**
   * Tests that LSM Tree index compaction does not corrupt data on the leader.
   * Cross-server compaction replication is not yet implemented in Raft HA
   * (no COMPACT log entry type in RaftLogEntryType), so follower consistency
   * after compaction is not verified here.
   */
  @Disabled("Index compaction is not replicated via Raft log entries - RaftLogEntryType has no COMPACT entry type")
  @Test
  void lsmTreeCompactionReplication() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final Database database = getServerDatabase(leaderIndex, getDatabaseName());

    final VertexType v = database.getSchema().buildVertexType().withName("RaftPerson").withTotalBuckets(3).create();
    v.createProperty("id", Long.class);
    v.createProperty("uuid", String.class);

    final String indexName = "RaftPerson[id]";
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "RaftPerson", "id");

    database.transaction(() -> insertPersonRecords(database));

    final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName(indexName);
    index.compact();

    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    testEachServer((serverIndex) -> {
      final Database serverDb = getServerDatabase(serverIndex, getDatabaseName());
      final Index serverIdx = serverDb.getSchema().getIndexByName(indexName);
      assertThat(serverIdx.countEntries())
          .as("Index on server %d should have %d entries", serverIndex, TOTAL_RECORDS)
          .isEqualTo(TOTAL_RECORDS);

      for (int i = 0; i < 10; i++) {
        final long value = i * 100L;
        assertThat(serverIdx.get(new Object[] { value }).hasNext() || value >= TOTAL_RECORDS)
            .as("Index on server %d should be queryable", serverIndex).isTrue();
      }
    });
  }

  /**
   * Tests that LSM Vector indexes are created and replicated to all replicas.
   * Vector index entry counts must match across all servers after replication completes.
   */
  @Disabled("Fails with TransactionException 'Error sending schema changes via Raft' caused by AlreadyClosedException on the Raft client when creating the vertex type; schema change replication for LSMVectorIndexBuilder not yet supported")
  @Test
  void lsmVectorReplication() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final Database database = getServerDatabase(leaderIndex, getDatabaseName());

    final VertexType v = database.getSchema().buildVertexType().withName("RaftEmbedding").withTotalBuckets(1).create();
    v.createProperty("vector", float[].class);

    final TypeLSMVectorIndexBuilder builder = database.getSchema()
        .buildTypeIndex("RaftEmbedding", new String[] { "vector" })
        .withLSMVectorType();
    builder.withDimensions(10);
    final TypeIndex vectorIndex = builder.create();

    assertThat(vectorIndex).isNotNull();

    database.transaction(() -> {
      for (int i = 0; i < TOTAL_RECORDS; i++) {
        final float[] vector = new float[10];
        for (int j = 0; j < vector.length; j++)
          vector[j] = (i + j) % 100f;
        database.newVertex("RaftEmbedding").set("vector", vector).save();
        if (i % TX_CHUNK == 0) {
          database.commit();
          database.begin();
        }
      }
    });

    final long entriesOnLeader = vectorIndex.countEntries();
    assertThat(entriesOnLeader).isGreaterThan(0);

    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    final String actualIndexName = vectorIndex.getName();
    testEachServer((serverIndex) -> {
      final Database serverDb = getServerDatabase(serverIndex, getDatabaseName());
      final Index serverVectorIndex = serverDb.getSchema().getIndexByName(actualIndexName);
      assertThat(serverVectorIndex).as("Vector index should be replicated to server %d", serverIndex).isNotNull();
      assertThat(serverVectorIndex.countEntries()).isEqualTo(entriesOnLeader);
    });
  }

  /**
   * Tests that LSM Vector index compaction does not crash.
   * Cross-server replication of compaction is not yet implemented in Raft HA.
   */
  @Disabled("Index compaction is not replicated via Raft log entries - RaftLogEntryType has no COMPACT entry type")
  @Test
  void lsmVectorCompactionReplication() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final Database database = getServerDatabase(leaderIndex, getDatabaseName());

    final VertexType v = database.getSchema().buildVertexType().withName("RaftEmbedding").withTotalBuckets(1).create();
    v.createProperty("vector", float[].class);

    final TypeLSMVectorIndexBuilder builder = database.getSchema()
        .buildTypeIndex("RaftEmbedding", new String[] { "vector" })
        .withLSMVectorType();
    builder.withDimensions(10);
    final TypeIndex vectorIndex = builder.create();

    database.transaction(() -> {
      for (int i = 0; i < TOTAL_RECORDS; i++) {
        final float[] vector = new float[10];
        for (int j = 0; j < vector.length; j++)
          vector[j] = (i + j) % 100f;
        database.newVertex("RaftEmbedding").set("vector", vector).save();
        if (i % TX_CHUNK == 0) {
          database.commit();
          database.begin();
        }
      }
    });

    final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName(vectorIndex.getName());
    index.scheduleCompaction();
    index.compact();

    final long entriesOnLeader = vectorIndex.countEntries();
    assertThat(entriesOnLeader).isGreaterThan(0);

    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    final String actualIndexName = vectorIndex.getName();
    testEachServer((serverIndex) -> {
      final Database serverDb = getServerDatabase(serverIndex, getDatabaseName());
      final Index serverVectorIndex = serverDb.getSchema().getIndexByName(actualIndexName);
      assertThat(serverVectorIndex).as("Vector index should be replicated to server %d", serverIndex).isNotNull();
    });
  }

  /**
   * Tests that index compaction does not corrupt data and subsequent writes are replicated.
   * Cross-server replication of the compacted index state is not yet implemented in Raft HA.
   */
  @Disabled("Index compaction is not replicated via Raft log entries - RaftLogEntryType has no COMPACT entry type")
  @Test
  void compactionReplicationWithConcurrentWrites() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final Database database = getServerDatabase(leaderIndex, getDatabaseName());

    final VertexType v = database.getSchema().buildVertexType().withName("RaftItem").withTotalBuckets(3).create();
    v.createProperty("itemId", Long.class);
    v.createProperty("value", String.class);

    final String indexName = "RaftItem[itemId]";
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "RaftItem", "itemId");

    database.transaction(() -> {
      for (int i = 0; i < 1000; i++)
        database.newVertex("RaftItem").set("itemId", (long) i, "value", "initial-" + i).save();
    });

    final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName(indexName);
    index.compact();

    database.transaction(() -> {
      for (int i = 1000; i < 2000; i++)
        database.newVertex("RaftItem").set("itemId", (long) i, "value", "post-compact-" + i).save();
    });

    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    testEachServer((serverIndex) -> {
      final Database serverDb = getServerDatabase(serverIndex, getDatabaseName());
      final Index serverIdx = serverDb.getSchema().getIndexByName(indexName);
      assertThat(serverIdx.countEntries()).as("Server %d index should have 2000 entries", serverIndex).isEqualTo(2000);
    });
  }

  private void insertPersonRecords(final Database database) {
    for (int i = 0; i < TOTAL_RECORDS; i++) {
      database.newVertex("RaftPerson").set("id", (long) i, "uuid", UUID.randomUUID().toString()).save();
      if (i % TX_CHUNK == 0) {
        database.commit();
        database.begin();
      }
    }
  }
}
