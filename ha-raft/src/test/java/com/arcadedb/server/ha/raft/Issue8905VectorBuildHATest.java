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
import com.arcadedb.index.vector.LSMVectorIndex;
import com.arcadedb.schema.TypeLSMVectorIndexBuilder;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8905, finding 2, end to end on a 3-node cluster: one chunk of {@code LSMVectorIndex.build()} is one replicated
 * entry on the ordinary arm, and its size ({@code arcadedb.index.buildChunkSizeMB}, 50MB of ESTIMATED vector bytes) was
 * not bounded by the replicated entry cap. The chunk is now clamped below the cap when the build replicates. Finding 1
 * is covered by {@code Issue8905BuildWithUnloadedGraphHATest}, the engine side by
 * {@code Issue8905VectorBuildCommitsReachTheWrapperTest}.
 */
class Issue8905VectorBuildHATest extends BaseRaftHATest {

  // A 256KB ENTRY CAP, TWO ORDERS OF MAGNITUDE BELOW THE 32MB DEFAULT, WITH THE WRITE BUFFER KEPT ABOVE IT (THE SERVER
  // REFUSES TO START OTHERWISE: IT MUST STAY >= appendBufferSize + 8 BYTES)
  private static final String APPEND_BUFFER_SIZE = "256KB";
  private static final String WRITE_BUFFER_SIZE  = "512KB";

  // 60_000 VECTORS OF 2 DIMENSIONS: ~2.3MB ESTIMATED, ONE CHUNK AT THE 50MB DEFAULT, AND THEIR INDEX PAGES ARE WELL PAST
  // THE 256KB CAP. RECORDS ARE CREATED IN BATCHES THAT EACH FIT IT
  private static final int CAPPED_RECORDS    = 60_000;
  private static final int CAPPED_DIMENSIONS = 2;
  private static final int RECORDS_PER_TX    = 2_000;

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected void populateDatabase() {
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_APPEND_BUFFER_SIZE, APPEND_BUFFER_SIZE);
    config.setValue(GlobalConfiguration.HA_WRITE_BUFFER_SIZE, WRITE_BUFFER_SIZE);
  }

  @BeforeEach
  void countWalVersionGaps() {
    // THE CAP IS SET ONLY IN THE SERVER CONFIGURATION (onServerConfiguration), NEVER AS A GLOBAL: THE BUILD MUST FIND
    // IT THROUGH THE REPLICATION CONFIGURATION, NOT THROUGH THE DATABASE'S OWN, WHICH NEVER SEES IT (ISSUE #9430)
    ArcadeStateMachine.TEST_WAL_GAP_COUNTER = new AtomicInteger();
  }

  @AfterEach
  void stopCountingWalVersionGaps() {
    ArcadeStateMachine.TEST_WAL_GAP_COUNTER = null;
  }

  /**
   * Finding 2. A build owning its transaction over more vectors than one replicated entry can carry, at the default
   * chunk size. Before the fix the whole build was one chunk, so one entry, refused with
   * {@code ReplicatedEntryTooLargeException}.
   */
  @Test
  void aBuildLargerThanTheReplicatedEntryCapReplicatesInChunks() throws Exception {
    final Database leader = leaderDatabase();
    createVectorType(leader, "Issue8905Capped");
    final TypeIndex index = buildVectorIndex(leader, "Issue8905Capped", CAPPED_DIMENSIONS, false);
    createRecords(leader, "Issue8905Capped", CAPPED_RECORDS, CAPPED_DIMENSIONS);
    assertThat(index.countEntries()).isEqualTo(CAPPED_RECORDS);

    final LSMVectorIndex bucketIndex = (LSMVectorIndex) index.getIndexesOnBuckets()[0];
    assertThat(leader.isTransactionActive()).as("the build must own its transaction").isFalse();
    assertThat(bucketIndex.build(null, null)).isEqualTo(CAPPED_RECORDS);

    assertVectorIndexOnEveryServer(index.getName(), index.countEntries());

    leader.transaction(() -> leader.newVertex("Issue8905Capped").set("vector", new float[] { 0.5f, 0.5f }).save());
    assertVectorIndexOnEveryServer(index.getName(), index.countEntries());
  }

  private Database leaderDatabase() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    return getServerDatabase(leaderIndex, getDatabaseName());
  }

  private void createVectorType(final Database leader, final String typeName) {
    final VertexType type = leader.getSchema().buildVertexType().withName(typeName).withTotalBuckets(1).create();
    type.createProperty("vector", float[].class);
  }

  private void createRecords(final Database leader, final String typeName, final int records, final int dimensions) {
    for (int start = 0; start < records; start += RECORDS_PER_TX) {
      final int from = start;
      final int to = Math.min(records, start + RECORDS_PER_TX);
      leader.transaction(() -> {
        for (int i = from; i < to; i++) {
          final float[] vector = new float[dimensions];
          for (int j = 0; j < dimensions; j++)
            vector[j] = ((i * 31 + j * 7) % 1_009 + 1) / 1_009f;
          leader.newVertex(typeName).set("vector", vector).save();
        }
      });
    }
  }

  private TypeIndex buildVectorIndex(final Database leader, final String typeName, final int dimensions,
      final boolean storeVectorsInGraph) {
    final TypeLSMVectorIndexBuilder builder = leader.getSchema().buildTypeIndex(typeName, new String[] { "vector" })
        .withLSMVectorType();
    builder.withDimensions(dimensions);
    builder.withStoreVectorsInGraph(storeVectorsInGraph);
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
    assertThat(ArcadeStateMachine.TEST_WAL_GAP_COUNTER.get()).as("WAL version gaps on the replicas").isZero();
  }
}
