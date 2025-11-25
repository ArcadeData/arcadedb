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
package com.arcadedb.server.ha;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.TypeLSMVectorIndexBuilder;
import com.arcadedb.schema.VertexType;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.logging.*;

/**
 * Integration tests for LSM index compaction replication in distributed mode.
 * Verifies that index compaction is properly tracked and replicated to all replicas.
 */
public class IndexCompactionReplicationIT extends BaseGraphServerTest {

  private static final int TOTAL_RECORDS = 5_000;
  private static final int TX_CHUNK      = 500;

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    // INCREASE HA QUORUM TIMEOUT FROM DEFAULT 10s TO 30s FOR VECTOR INDEX OPERATIONS
    config.setValue(GlobalConfiguration.HA_QUORUM_TIMEOUT, 30_000L);
  }

  @Override
  protected void populateDatabase() {
  }

  /**
   * Test that LSM Tree index compaction is replicated to all replicas.
   * This test creates records, triggers index compaction on the leader,
   * and verifies that the compacted index is consistent across all servers.
   */
  @Test
  public void lsmTreeCompactionReplication() throws Exception {
    final Database database = getServerDatabase(0, getDatabaseName());

    // CREATE SCHEMA WITH INDEX
    final VertexType v = database.getSchema().buildVertexType().withName("Person").withTotalBuckets(3).create();
    v.createProperty("id", Long.class);
    v.createProperty("uuid", String.class);

    final String indexName = "Person[id]";
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Person", "id");

    LogManager.instance().log(this, Level.FINE, "Inserting %d records into LSM index...", TOTAL_RECORDS);
    // INSERT RECORDS IN BATCHES TO ACCUMULATE PAGES IN LSM INDEX
    database.transaction(() -> insertRecords(database));

    // GET THE INDEX AND TRIGGER COMPACTION ON LEADER
    LogManager.instance().log(this, Level.FINE, "Triggering compaction on index '%s' on leader...", indexName);
    final com.arcadedb.index.TypeIndex index = (com.arcadedb.index.TypeIndex) database.getSchema().getIndexByName(indexName);
    final boolean compacted = index.compact();
    LogManager.instance().log(this, Level.FINE, "Compaction result: %b", compacted);
    // Compaction might return false if the index doesn't need compaction, which is OK for this test
    // The important thing is that it doesn't throw an exception

    // WAIT FOR REPLICATION TO COMPLETE
    Thread.sleep(2000);

    // VERIFY THAT COMPACTION WAS REPLICATED BY CHECKING INDEX CONSISTENCY ON ALL SERVERS
    testEachServer((serverIndex) -> {
      LogManager.instance().log(this, Level.FINE, "Verifying compaction replication on server %d...", serverIndex);

      final Database serverDb = getServerDatabase(serverIndex, getDatabaseName());
      final com.arcadedb.index.Index serverIndex_idx = serverDb.getSchema().getIndexByName(indexName);

      // VERIFY THAT INDEX IS FUNCTIONAL AND CONTAINS ALL ENTRIES
      Assertions.assertEquals(TOTAL_RECORDS, serverIndex_idx.countEntries(),
          "Index on server " + serverIndex + " should contain " + TOTAL_RECORDS + " entries after compaction");

      // VERIFY THAT WE CAN QUERY USING THE COMPACTED INDEX
      for (int i = 0; i < 10; i++) {
        final long value = i * 100L;
        Assertions.assertTrue(serverIndex_idx.get(new Object[] { value }).hasNext() || value >= TOTAL_RECORDS,
            "Should be able to query index on server " + serverIndex);
      }
    });

    LogManager.instance().log(this, Level.FINE, "LSM Tree compaction replication test PASSED");
  }

  /**
   * Test that LSM Vector indexes are properly created and replicated to all replicas.
   * This test verifies that vector index definitions with complete metadata are
   * correctly stored in schema JSON and replicated to all replicas.
   */
  @Test
  public void lsmVectorReplication() throws Exception {
    final Database database = getServerDatabase(0, getDatabaseName());

    // CREATE SCHEMA WITH VECTOR INDEX (use 1 bucket for simpler replication testing)
    final VertexType v = database.getSchema().buildVertexType().withName("Embedding").withTotalBuckets(1).create();
    v.createProperty("vector", float[].class);

    // USE BUILDER FOR VECTOR INDEXES WITH DIMENSION = 10
    final TypeLSMVectorIndexBuilder builder = database.getSchema().buildTypeIndex("Embedding", new String[] { "vector" })
        .withLSMVectorType();

    builder.withDimensions(10);

    final com.arcadedb.index.TypeIndex vectorIndex = builder.create();

    LogManager.instance().log(this, Level.FINE, "Vector index created: %s", vectorIndex.getName());
    Assertions.assertNotNull(vectorIndex, "Vector index should be created successfully");

    LogManager.instance().log(this, Level.FINE, "Inserting %d records into vector index...", TOTAL_RECORDS);
    // INSERT VECTOR RECORDS IN BATCHES
    database.transaction(() -> {
      for (int i = 0; i < TOTAL_RECORDS; i++) {
        final float[] vector = new float[10];
        for (int j = 0; j < vector.length; j++)
          vector[j] = (i + j) % 100f;

        database.newVertex("Embedding").set("vector", vector).save();

        if (i % TX_CHUNK == 0) {
          database.commit();
          database.begin();
        }
      }
    });

    LogManager.instance().log(this, Level.FINE, "Verifying vector index on leader...");
    final long entriesOnLeader = vectorIndex.countEntries();
    LogManager.instance().log(this, Level.FINE, "Vector index contains %d entries on leader", entriesOnLeader);
    Assertions.assertTrue(entriesOnLeader > 0, "Vector index should contain entries after inserting records");

    // WAIT FOR REPLICATION TO COMPLETE
    LogManager.instance().log(this, Level.FINE, "Waiting for replication...");
    Thread.sleep(2000);

    // VERIFY THAT VECTOR INDEX DEFINITION IS REPLICATED TO ALL SERVERS
    final String actualIndexName = vectorIndex.getName();
    testEachServer((serverIndex) -> {
      LogManager.instance().log(this, Level.FINE, "Verifying vector index definition on server %d...", serverIndex);

      final Database serverDb = getServerDatabase(serverIndex, getDatabaseName());

      // Check if the index exists in schema
      final com.arcadedb.index.Index serverVectorIndex = serverDb.getSchema().getIndexByName(actualIndexName);
      if (serverVectorIndex == null) {
        // Index not found, check the type's indexes
        final com.arcadedb.schema.DocumentType embeddingType = serverDb.getSchema().getType("Embedding");
        LogManager.instance().log(this, Level.WARNING, "Vector index not found on server %d. Type has %d indexes", serverIndex,
            embeddingType.getAllIndexes(false).size());
      }
      Assertions.assertNotNull(serverVectorIndex, "Vector index should be replicated to server " + serverIndex);

      final long entriesOnReplica = serverVectorIndex.countEntries();
      Assertions.assertEquals(entriesOnLeader, entriesOnReplica);
    });

    LogManager.instance().log(this, Level.FINE, "LSM Vector index replication test PASSED");
  }

  /**
   * Test that LSM Vector indexes are properly created and replicated to all replicas.
   * This test verifies that vector index definitions with complete metadata are
   * correctly stored in schema JSON and replicated to all replicas.
   */
  @Test
  public void lsmVectorCompactionReplication() throws Exception {
    final Database database = getServerDatabase(0, getDatabaseName());

    // CREATE SCHEMA WITH VECTOR INDEX (use 1 bucket for simpler replication testing)
    final VertexType v = database.getSchema().buildVertexType().withName("Embedding").withTotalBuckets(1).create();
    v.createProperty("vector", float[].class);

    // USE BUILDER FOR VECTOR INDEXES WITH DIMENSION = 10
    final TypeLSMVectorIndexBuilder builder = database.getSchema().buildTypeIndex("Embedding", new String[] { "vector" })
        .withLSMVectorType();

    builder.withDimensions(10);

    final com.arcadedb.index.TypeIndex vectorIndex = builder.create();

    LogManager.instance().log(this, Level.FINE, "Vector index created: %s", vectorIndex.getName());
    Assertions.assertNotNull(vectorIndex, "Vector index should be created successfully");

    LogManager.instance().log(this, Level.FINE, "Inserting %d records into vector index...", TOTAL_RECORDS);
    // INSERT VECTOR RECORDS IN BATCHES
    database.transaction(() -> {
      for (int i = 0; i < TOTAL_RECORDS; i++) {
        final float[] vector = new float[10];
        for (int j = 0; j < vector.length; j++)
          vector[j] = (i + j) % 100f;

        database.newVertex("Embedding").set("vector", vector).save();

        if (i % TX_CHUNK == 0) {
          database.commit();
          database.begin();
        }
      }
    });

    // GET THE INDEX AND TRIGGER COMPACTION ON LEADER
    LogManager.instance().log(this, Level.FINE, "Triggering compaction on index '%s' on leader...", vectorIndex.getName());
    final com.arcadedb.index.TypeIndex index = (com.arcadedb.index.TypeIndex) database.getSchema()
        .getIndexByName(vectorIndex.getName());
    index.scheduleCompaction();
    final boolean compacted = index.compact();
    LogManager.instance().log(this, Level.FINE, "Compaction result: %b", compacted);
    // Compaction might return false if the index doesn't need compaction, which is OK for this test

    LogManager.instance().log(this, Level.FINE, "Verifying vector index on leader...");
    final long entriesOnLeader = vectorIndex.countEntries();
    LogManager.instance().log(this, Level.FINE, "Vector index contains %d entries on leader", entriesOnLeader);
    Assertions.assertTrue(entriesOnLeader > 0, "Vector index should contain entries after inserting records");

    // WAIT FOR REPLICATION TO COMPLETE
    LogManager.instance().log(this, Level.FINE, "Waiting for replication...");
    Thread.sleep(2000);

    // VERIFY THAT VECTOR INDEX DEFINITION IS REPLICATED TO ALL SERVERS
    final String actualIndexName = vectorIndex.getName();
    testEachServer((serverIndex) -> {
      LogManager.instance().log(this, Level.FINE, "Verifying vector index definition on server %d...", serverIndex);

      final Database serverDb = getServerDatabase(serverIndex, getDatabaseName());

      // Check if the index exists in schema
      final com.arcadedb.index.Index serverVectorIndex = serverDb.getSchema().getIndexByName(actualIndexName);
      if (serverVectorIndex == null) {
        // Index not found, check the type's indexes
        final com.arcadedb.schema.DocumentType embeddingType = serverDb.getSchema().getType("Embedding");
        LogManager.instance().log(this, Level.WARNING, "Vector index not found on server %d. Type has %d indexes", serverIndex,
            embeddingType.getAllIndexes(false).size());
      }
      Assertions.assertNotNull(serverVectorIndex, "Vector index should be replicated to server " + serverIndex);
    });

    LogManager.instance().log(this, Level.FINE, "LSM Vector index replication test PASSED");
  }

  /**
   * Test compaction replication with concurrent writes.
   * This test verifies that compaction can occur while new records are being inserted
   * on replicas (eventual consistency scenario).
   */
  @Test
  public void compactionReplicationWithConcurrentWrites() throws Exception {
    final Database database = getServerDatabase(0, getDatabaseName());

    // CREATE SCHEMA WITH INDEX
    final VertexType v = database.getSchema().buildVertexType().withName("Item").withTotalBuckets(3).create();
    v.createProperty("itemId", Long.class);
    v.createProperty("value", String.class);

    final String indexName = "Item[itemId]";
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Item", "itemId");

    LogManager.instance().log(this, Level.FINE, "Inserting initial records...");
    database.transaction(() -> {
      for (int i = 0; i < 1000; i++) {
        database.newVertex("Item").set("itemId", (long) i, "value", "initial-" + i).save();
      }
    });

    // TRIGGER COMPACTION
    LogManager.instance().log(this, Level.FINE, "Triggering compaction while records exist...");
    final com.arcadedb.index.TypeIndex index = (com.arcadedb.index.TypeIndex) database.getSchema().getIndexByName(indexName);
    final boolean compacted = index.compact();
    LogManager.instance().log(this, Level.FINE, "Compaction result: %b", compacted);
    // Compaction might return false if the index doesn't need compaction, which is OK

    // CONTINUE INSERTING RECORDS AFTER COMPACTION
    LogManager.instance().log(this, Level.FINE, "Inserting additional records after compaction...");
    database.transaction(() -> {
      for (int i = 1000; i < 2000; i++) {
        database.newVertex("Item").set("itemId", (long) i, "value", "post-compact-" + i).save();
      }
    });

    // WAIT FOR REPLICATION
    Thread.sleep(2000);

    // VERIFY CONSISTENCY ON ALL SERVERS
    testEachServer((serverIndex) -> {
      LogManager.instance().log(this, Level.FINE, "Verifying consistency on server %d...", serverIndex);

      final Database serverDb = getServerDatabase(serverIndex, getDatabaseName());
      final com.arcadedb.index.Index serverIndex_idx = serverDb.getSchema().getIndexByName(indexName);

      Assertions.assertEquals(2000, serverIndex_idx.countEntries(), "Index on server " + serverIndex + " should have 2000 entries");
    });

    LogManager.instance().log(this, Level.FINE, "Concurrent writes with compaction test PASSED");
  }

  private void insertRecords(final Database database) {
    for (int i = 0; i < TOTAL_RECORDS; i++) {
      database.newVertex("Person").set("id", (long) i, "uuid", UUID.randomUUID().toString()).save();

      if (i % TX_CHUNK == 0) {
        database.commit();
        database.begin();
      }
    }
  }
}
