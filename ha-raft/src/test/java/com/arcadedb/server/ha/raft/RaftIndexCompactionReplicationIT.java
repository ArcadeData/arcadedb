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
import com.arcadedb.engine.FileManager;
import com.arcadedb.index.Index;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.TypeLSMVectorIndexBuilder;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

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
   * Tests that LSM Tree index compaction is replicated to all follower nodes.
   * After the leader compacts, all followers must have matching index entry counts
   * and be able to query the compacted index.
   */
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
    index.scheduleCompaction();
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
      assertThat(serverVectorIndex.countEntries())
          .as("Server %d vector index entry count must match leader after compaction", serverIndex)
          .isEqualTo(entriesOnLeader);
    });
  }

  /**
   * Tests that index compaction is replicated and sequential writes after compaction
   * are also correctly replicated to all follower nodes.
   */
  @Test
  void compactionReplicationWithSequentialWrites() throws Exception {
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
    index.scheduleCompaction();
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

  /**
   * Regression test for WALVersionGapException caused by concurrent writes during compaction.
   * <p>
   * When compaction calls startRecordingChanges(), concurrent user-transaction threads must still
   * replicate their WAL as TX_ENTRY instead of silently buffering it to the per-thread
   * schemaWalBuffer.  Before the fix, ALL threads saw getRecordedChanges() != null and buffered;
   * the buffered WAL from non-compaction threads was discarded, leaving followers N versions
   * behind and triggering WALVersionGapException on the next TX_ENTRY touching those pages.
   */
  @Tag("slow")
  @Test
  void compactionReplicationWithConcurrentWrites() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final Database database = getServerDatabase(leaderIndex, getDatabaseName());

    final VertexType v = database.getSchema().buildVertexType().withName("RaftConcurrent").withTotalBuckets(3).create();
    v.createProperty("itemId", Long.class);
    v.createProperty("value", String.class);

    final String indexName = "RaftConcurrent[itemId]";
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "RaftConcurrent", "itemId");

    // Seed enough data to guarantee compaction actually merges segments
    database.transaction(() -> {
      for (int i = 0; i < 500; i++)
        database.newVertex("RaftConcurrent").set("itemId", (long) i, "value", "seed-" + i).save();
    });

    final AtomicLong nextId = new AtomicLong(500);
    final CountDownLatch compactionStarted = new CountDownLatch(1);
    final int writerCount = 3;
    final int writesPerThread = 100;
    final ExecutorService pool = Executors.newFixedThreadPool(writerCount + 1);
    final List<Future<?>> futures = new ArrayList<>();

    // Writer threads: insert records that overlap in time with compaction
    for (int t = 0; t < writerCount; t++) {
      futures.add(pool.submit(() -> {
        try {
          compactionStarted.await(10, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          return null;
        }
        for (int i = 0; i < writesPerThread; i++) {
          final long id = nextId.getAndIncrement();
          database.transaction(() ->
              database.newVertex("RaftConcurrent").set("itemId", id, "value", "concurrent-" + id).save());
        }
        return null;
      }));
    }

    // Compaction thread: signal writers then compact
    final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName(indexName);
    futures.add(pool.submit(() -> {
      index.scheduleCompaction();
      compactionStarted.countDown();
      try {
        index.compact();
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
      return null;
    }));

    pool.shutdown();
    assertThat(pool.awaitTermination(60, TimeUnit.SECONDS)).isTrue();
    for (final Future<?> f : futures)
      f.get();

    // Post-compaction writes to verify pages touched by compaction are still writable
    database.transaction(() -> {
      for (int i = 0; i < 100; i++) {
        final long id = nextId.getAndIncrement();
        database.newVertex("RaftConcurrent").set("itemId", id, "value", "post-" + id).save();
      }
    });

    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    // nextId now equals total inserted records (seed + concurrent + post-compact)
    final long totalExpected = nextId.get();
    testEachServer((serverIndex) -> {
      final Database serverDb = getServerDatabase(serverIndex, getDatabaseName());
      final Index serverIdx = serverDb.getSchema().getIndexByName(indexName);
      assertThat(serverIdx.countEntries())
          .as("Server %d index entry count should match leader after concurrent compaction", serverIndex)
          .isEqualTo(totalExpected);
    });
  }

  /**
   * Regression test for issue #4063: when another recordFileChanges session is active on the
   * leader, {@link com.arcadedb.server.ha.raft.RaftReplicatedDatabase#runWithCompactionReplication}
   * must not invoke the compaction callback. The pre-fix "local-only fallback" silently ran the
   * compaction without replicating its files/WAL/schema, which renamed the leader's mutable
   * index file but left followers with the old name (or no file at all) - eventually surfacing
   * as a "Cannot find index ..." warning when the follower reloaded its schema.
   * <p>
   * The post-fix behaviour is to defer: return {@code false} immediately and let the async
   * scheduler retry once the contending recording session has released the file manager. This
   * test enforces the contract by holding a recording session and verifying the compaction
   * callable is never invoked.
   */
  @Test
  void compactionDefersWhenRecordingSessionActive() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final com.arcadedb.database.DatabaseInternal leaderDb =
        (com.arcadedb.database.DatabaseInternal) getServerDatabase(leaderIndex, getDatabaseName());

    leaderDb.getSchema().buildVertexType().withName("RaftDeferred").withTotalBuckets(1).create();

    // Settle any in-flight async tasks, then poll for an unowned recording session.
    leaderDb.async().waitCompletion();
    final FileManager fm = leaderDb.getEmbedded().getFileManager();
    final long acquireDeadline = System.currentTimeMillis() + 5_000;
    boolean started = false;
    while (System.currentTimeMillis() < acquireDeadline) {
      if (fm.startRecordingChanges()) {
        started = true;
        break;
      }
      Thread.sleep(50);
    }
    assertThat(started).as("Test must own the recording session for the assertion to be meaningful").isTrue();

    final java.util.concurrent.atomic.AtomicBoolean callbackInvoked = new java.util.concurrent.atomic.AtomicBoolean(false);
    final boolean returnedValue;
    try {
      // The production call site is LSMTreeIndex.compact() which goes through
      // getWrappedDatabaseInstance() to reach the RaftReplicatedDatabase override; using the
      // ServerDatabase wrapper directly would land on the no-op default in DatabaseInternal.
      returnedValue = leaderDb.getWrappedDatabaseInstance().runWithCompactionReplication(() -> {
        callbackInvoked.set(true);
        return true;
      });
    } finally {
      fm.stopRecordingChanges();
    }

    assertThat(callbackInvoked.get())
        .as("runWithCompactionReplication must NOT invoke the compaction callback when a recording session is in progress; "
            + "running it locally would diverge the leader from followers and ultimately log 'Cannot find index ...' warnings")
        .isFalse();
    assertThat(returnedValue)
        .as("runWithCompactionReplication must return false (deferred) so the index status resets to AVAILABLE and the next "
            + "onAfterCommit reschedules compaction once the contending session has released the file manager")
        .isFalse();
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
