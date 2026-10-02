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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.graph.GraphBatch;
import com.arcadedb.index.Index;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.vector.LSMVectorIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.TypeLSMVectorIndexBuilder;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8291: {@code RaftReplicatedDatabase.commit()} replicates a transaction as the WAL buffer phase 1 builds, and
 * phase 1 builds none for a transaction whose WAL is off. Both arms of the commit dereferenced that missing buffer, so a
 * WAL-less transaction - an {@code LSM_VECTOR} index build owning its transaction, any session that called
 * {@code setUseWAL(false)} - failed with "Error on commit distributed transaction (phase 1)" on an HA node. Under
 * replication the buffer IS the transaction, so the wrapper now writes it whatever the transaction asked for.
 */
class Issue8291WalDisabledTransactionHATest extends BaseRaftHATest {

  private static final int RECORDS    = 200;
  private static final int DIMENSIONS = 8;

  // THE BUILD ESTIMATES dimensions * 4 + 32 BYTES PER VECTOR: 1_100 OF 256 DIMENSIONS IS ~1.1MB, PAST THE 1MB CHUNK
  // SET BELOW, SO A BUILD OVER THEM COMMITS ONE CHUNK MID-SCAN
  private static final int CHUNKED_RECORDS    = 1_100;
  private static final int CHUNKED_DIMENSIONS = 256;

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected void populateDatabase() {
  }

  @BeforeEach
  void countWalVersionGaps() {
    ArcadeStateMachine.TEST_WAL_GAP_COUNTER = new AtomicInteger();
    // A DATABASE READS THE CHUNK SIZE FROM ITS OWN CONFIGURATION, WHICH FALLS BACK TO THE GLOBAL ONE AND NOT TO THE
    // SERVER'S, SO IT IS SET HERE RATHER THAN IN onServerConfiguration()
    GlobalConfiguration.INDEX_BUILD_CHUNK_SIZE_MB.setValue(1L);
  }

  @AfterEach
  void stopCountingWalVersionGaps() {
    ArcadeStateMachine.TEST_WAL_GAP_COUNTER = null;
    GlobalConfiguration.INDEX_BUILD_CHUNK_SIZE_MB.reset();
  }

  /**
   * The path the issue names: {@code CREATE INDEX} over more than one chunk of vectors. The DDL opens a transaction,
   * the build turns the WAL off for it and, at the chunk boundary, commits it through
   * {@code getWrappedDatabaseInstance().commit()} - on an HA node the schema arm of {@code RaftReplicatedDatabase},
   * since the DDL holds a {@code recordFileChanges} frame. Before the fix that commit failed, the build logged the
   * failure once per remaining record and carried on, and the DDL returned normally: an index with the first chunk on
   * the leader and nothing on the replicas.
   */
  @Test
  void vectorIndexCreatedOverMoreThanOneChunkReplicates() throws Exception {
    final Database leader = leaderDatabase();
    createVectorTypeWithRecords(leader, "Issue8291Chunked", CHUNKED_RECORDS, CHUNKED_DIMENSIONS);

    final TypeIndex index = buildVectorIndex(leader, "Issue8291Chunked", CHUNKED_DIMENSIONS);
    assertThat(index.countEntries()).isEqualTo(CHUNKED_RECORDS);
    assertVectorIndexOnEveryServer(index.getName(), CHUNKED_RECORDS);

    // A page the build committed on the leader and not on a replica fails only when the NEXT write touches it, as a
    // WAL version gap: one more vector into the same bucket and index, then the gap counter
    final float[] extra = new float[CHUNKED_DIMENSIONS];
    Arrays.fill(extra, 0.5f);
    leader.transaction(() -> leader.newVertex("Issue8291Chunked").set("vector", extra).save());
    assertVectorIndexOnEveryServer(index.getName(), CHUNKED_RECORDS + 1);
  }

  /**
   * {@code LSMVectorIndex.build()} owning its transaction, which it opens with
   * {@code getWrappedDatabaseInstance().begin()}, turns the WAL off for, and commits - per chunk and at the end -
   * through {@code getWrappedDatabaseInstance().commit()}: the ordinary arm of {@code RaftReplicatedDatabase}. The
   * index is created over an empty type, so that its own {@code CREATE INDEX} - the test above - cannot fail first.
   */
  @Test
  void vectorIndexBuildOwningItsTransactionReplicates() throws Exception {
    final Database leader = leaderDatabase();
    createVectorTypeWithRecords(leader, "Issue8291Direct", 0, CHUNKED_DIMENSIONS);
    final TypeIndex index = buildVectorIndex(leader, "Issue8291Direct", CHUNKED_DIMENSIONS);
    createRecords(leader, "Issue8291Direct", CHUNKED_RECORDS, CHUNKED_DIMENSIONS);
    assertThat(index.countEntries()).isEqualTo(CHUNKED_RECORDS);

    final LSMVectorIndex bucketIndex = (LSMVectorIndex) index.getIndexesOnBuckets()[0];
    assertThat(leader.isTransactionActive()).as("the build must own its transaction").isFalse();
    assertThat(bucketIndex.build(null, null)).isEqualTo(CHUNKED_RECORDS);

    // A second build over a populated index appends to it rather than replacing it, so the replicas are held to what
    // the leader ended up with, not to the record count
    assertVectorIndexOnEveryServer(index.getName(), index.countEntries());

    final float[] extra = new float[CHUNKED_DIMENSIONS];
    Arrays.fill(extra, 0.5f);
    leader.transaction(() -> leader.newVertex("Issue8291Direct").set("vector", extra).save());
    assertVectorIndexOnEveryServer(index.getName(), index.countEntries());
  }

  /**
   * A guard rather than a reproduction: {@code CREATE INDEX} over records that fit one chunk builds inside the DDL's
   * own transaction without committing it, and the WAL override is gone again by the time the DDL commits.
   */
  @Test
  void vectorIndexCreatedOverExistingRecordsReplicates() throws Exception {
    final Database leader = leaderDatabase();
    createVectorTypeWithRecords(leader, "Issue8291Built", RECORDS, DIMENSIONS);

    final TypeIndex index = buildVectorIndex(leader, "Issue8291Built", DIMENSIONS);
    assertThat(index.countEntries()).isEqualTo(RECORDS);

    assertVectorIndexOnEveryServer(index.getName(), RECORDS);
  }

  /**
   * A guard, for the same reason: {@code REBUILD INDEX} of an index that fits one chunk recreates it through the DDL
   * path.
   */
  @Test
  void vectorIndexRebuildReplicates() throws Exception {
    final Database leader = leaderDatabase();
    createVectorTypeWithRecords(leader, "Issue8291Rebuilt", RECORDS, DIMENSIONS);
    final TypeIndex index = buildVectorIndex(leader, "Issue8291Rebuilt", DIMENSIONS);

    try (final ResultSet rs = leader.command("sql", "REBUILD INDEX `" + index.getName() + "`")) {
      assertThat(rs.hasNext()).isTrue();
    }

    assertVectorIndexOnEveryServer(index.getName(), RECORDS);
  }

  /**
   * The ordinary arm, from the leader: a plain user transaction with the WAL turned off for it.
   */
  @Test
  void userTransactionWithWalOffReplicatesFromTheLeader() throws Exception {
    final Database leader = leaderDatabase();
    leader.getSchema().createDocumentType("Issue8291LeaderDoc");

    // THE SAME PER-TRANSACTION OVERRIDE THE VECTOR INDEX BUILD SETS: IT DIES WITH THE TRANSACTION, NOTHING TO RESTORE
    leader.begin();
    ((DatabaseInternal) leader).getTransaction().setUseWALForThisTransaction(false);
    for (int i = 0; i < RECORDS; i++)
      leader.newDocument("Issue8291LeaderDoc").set("id", i).save();
    leader.commit();

    assertCountOnEveryServer("Issue8291LeaderDoc", RECORDS);
  }

  /**
   * The ordinary arm on a replica, with the WAL off for the whole database rather than for one transaction.
   */
  @Test
  void databaseWithWalOffReplicatesFromAReplica() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);
    leaderDatabase().getSchema().createDocumentType("Issue8291ReplicaDoc");
    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    final int replicaIndex = (leaderIndex + 1) % getServerCount();
    final Database replica = getServerDatabase(replicaIndex, getDatabaseName());
    replica.setUseWAL(false);
    try {
      replica.transaction(() -> {
        for (int i = 0; i < RECORDS; i++)
          replica.newDocument("Issue8291ReplicaDoc").set("id", i).save();
      });
    } finally {
      replica.setUseWAL(true);
    }

    assertCountOnEveryServer("Issue8291ReplicaDoc", RECORDS);
  }

  /**
   * The schema-commit arm: a WAL-less commit on the thread of an open {@code recordFileChanges} frame, whose WAL is
   * buffered into the {@code SCHEMA_ENTRY} the frame ships on its way out.
   */
  @Test
  void walLessCommitInsideRecordFileChangesReplicates() throws Exception {
    final Database leader = leaderDatabase();
    leader.getSchema().createDocumentType("Issue8291SchemaArmDoc");
    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    final DatabaseInternal wrapped = ((DatabaseInternal) leader).getWrappedDatabaseInstance();
    wrapped.recordFileChanges(() -> {
      wrapped.begin();
      wrapped.getTransaction().setUseWALForThisTransaction(false);
      for (int i = 0; i < RECORDS; i++)
        wrapped.newDocument("Issue8291SchemaArmDoc").set("id", i).save();
      wrapped.commit();
      return null;
    });

    assertCountOnEveryServer("Issue8291SchemaArmDoc", RECORDS);
  }

  /**
   * A guard rather than a reproduction: {@code GraphBatch.withWAL(false)}, the documented bulk-load setting, commits
   * through the outermost wrapper, and its builder already forces the WAL back on for a replicated database (issue
   * #4076). Pinned so that the batch keeps replicating whichever of the two places ends up owning the rule.
   */
  @Test
  void graphBatchWithWalOffReplicates() throws Exception {
    final Database leader = leaderDatabase();
    leader.getSchema().createVertexType("Issue8291BatchV").createProperty("id", Integer.class);
    leader.getSchema().createEdgeType("Issue8291BatchE");
    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    try (final GraphBatch batch = leader.batch().withWAL(false).withLightEdges(false).withParallelFlush(false).build()) {
      RID previous = null;
      for (int i = 0; i < RECORDS; i++) {
        final RID current = batch.newVertex("Issue8291BatchV").set("id", i).save().getIdentity();
        if (previous != null)
          batch.newEdge(previous, "Issue8291BatchE", current);
        previous = current;
      }
    }

    assertCountOnEveryServer("Issue8291BatchV", RECORDS);
    testEachServer(serverIndex -> assertThat(
        getServerDatabase(serverIndex, getDatabaseName()).countType("Issue8291BatchE", false))
        .as("edges on server %d", serverIndex).isEqualTo(RECORDS - 1));
  }

  private Database leaderDatabase() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    return getServerDatabase(leaderIndex, getDatabaseName());
  }

  private void createVectorTypeWithRecords(final Database leader, final String typeName, final int records,
      final int dimensions) {
    final VertexType type = leader.getSchema().buildVertexType().withName(typeName).withTotalBuckets(1).create();
    type.createProperty("vector", float[].class);
    createRecords(leader, typeName, records, dimensions);
  }

  private void createRecords(final Database leader, final String typeName, final int records, final int dimensions) {
    leader.transaction(() -> {
      for (int i = 0; i < records; i++) {
        final float[] vector = new float[dimensions];
        for (int j = 0; j < dimensions; j++)
          vector[j] = (i * 31 + j * 7) % 101 / 101f;
        leader.newVertex(typeName).set("vector", vector).save();
      }
    });
  }

  private TypeIndex buildVectorIndex(final Database leader, final String typeName, final int dimensions) {
    final TypeLSMVectorIndexBuilder builder = leader.getSchema().buildTypeIndex(typeName, new String[] { "vector" })
        .withLSMVectorType();
    builder.withDimensions(dimensions);
    return builder.create();
  }

  private void assertVectorIndexOnEveryServer(final String indexName, final long expected) throws Exception {
    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    testEachServer(serverIndex -> {
      final Index index = getServerDatabase(serverIndex, getDatabaseName()).getSchema().getIndexByName(indexName);
      assertThat(index).as("vector index on server %d", serverIndex).isNotNull();
      assertThat(index.countEntries()).as("vector index entries on server %d", serverIndex).isEqualTo(expected);
    });
    assertNoWalVersionGap();
  }

  private void assertCountOnEveryServer(final String typeName, final long expected) throws Exception {
    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    testEachServer(serverIndex -> assertThat(countOn(serverIndex, typeName)).as("%s on server %d", typeName, serverIndex)
        .isEqualTo(expected));
    assertNoWalVersionGap();
  }

  private static void assertNoWalVersionGap() {
    assertThat(ArcadeStateMachine.TEST_WAL_GAP_COUNTER.get()).as("WAL version gaps on the replicas").isZero();
  }
}
