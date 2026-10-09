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
import com.arcadedb.index.Index;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.vector.LSMVectorIndex;
import com.arcadedb.schema.TypeLSMVectorIndexBuilder;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8905, finding 1, end to end on a 3-node cluster: PHASE 3 of {@code LSMVectorIndex.build()}
 * ({@code buildGraphWithChunking}) committed its graph persist chunks on the inner database, so they - and the
 * bulk-loaded vector pages the build's transaction still held - stayed on the node that ran the build. Before the fix
 * the build below reached PHASE 3 on the replica and failed its final, replicated commit with a concurrent
 * modification on a graph page the replica had committed alone ("validated against version 1 but the cluster is at
 * version 0"). Routing those commits through the replicated wrapper fails the same way ("validated against version 2
 * but the cluster is at version 0"), because the graph file is node-local: every node persists its own graph (#8292).
 * PHASE 3 is therefore skipped on a replicated database, and the replica builds its graph on first use.
 * <p>
 * The cluster runs at the default replicated entry cap on purpose: below it the build's bulk load commits in chunks
 * clamped under the cap (finding 2, {@code Issue8905VectorBuildHATest}), a chunk commit loads the graph, and PHASE 3 is
 * not reached at all. The engine-side routing is pinned by {@code Issue8905VectorBuildCommitsReachTheWrapperTest}.
 */
class Issue8905BuildWithUnloadedGraphHATest extends BaseRaftHATest {

  // 20_000 VECTORS OF 2 DIMENSIONS: ~780KB ESTIMATED, UNDER THE 1MB CHUNK, SO THE BULK LOAD COMMITS NO CHUNK AND THE
  // GRAPH STAYS UNLOADED UNTIL PHASE 3. THE GRAPH, WITH ITS VECTORS INLINE, IS WELL PAST 1MB
  private static final int GRAPH_RECORDS    = 20_000;
  private static final int GRAPH_DIMENSIONS = 2;

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected boolean persistentRaftStorage() {
    // THE GRAPH TEST RESTARTS A REPLICA, WHICH HAS TO REJOIN THE SAME GROUP
    return true;
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
   * A replica restarted from disk holds the index with its graph not loaded; a build there, whose bulk load fits one
   * chunk, was the path into PHASE 3, whose graph persist crosses a chunk boundary.
   */
  @Test
  void aBuildOnAReplicaWithItsGraphUnloadedReplicatesEveryVector() throws Exception {
    final Database leader = leaderDatabase();
    createVectorType(leader, "Issue8905Graph");
    final TypeIndex index = buildVectorIndex(leader, "Issue8905Graph", GRAPH_DIMENSIONS, true);
    createRecords(leader, "Issue8905Graph", GRAPH_RECORDS, GRAPH_DIMENSIONS);
    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    final int replicaIndex = (findLeaderIndex() + 1) % getServerCount();
    restartServer(replicaIndex);

    final Database replica = getServerDatabase(replicaIndex, getDatabaseName());
    final LSMVectorIndex bucketIndex =
        (LSMVectorIndex) ((TypeIndex) replica.getSchema().getIndexByName(index.getName())).getIndexesOnBuckets()[0];

    final AtomicBoolean graphBuilt = new AtomicBoolean();
    bucketIndex.build(null, (phase, processed, total, insertsInProgress) -> graphBuilt.set(true));
    assertThat(graphBuilt.get()).as("PHASE 3 must not run on a replicated database").isFalse();

    // ONE ENTRY PER RECORD: A BUILD OVER A POPULATED INDEX REPLACES THE RECORDS' VECTORS (ISSUE #9506)
    assertVectorIndexOnEveryServer(index.getName(), GRAPH_RECORDS);
    // NO SEARCH AND NO FURTHER WRITE HERE, UNLIKE Issue8291WalDisabledTransactionHATest: what a node makes of the index
    // afterwards is not this issue's. An insert on the leader no longer reuses a vector id the replica allocated (issue
    // #9428, Issue9428FollowerVectorInsertHATest). That this build commits nothing outside the wrapper - the defect - is
    // pinned by Issue8905VectorBuildCommitsReachTheWrapperTest, which also searches the index after a build that skipped
    // PHASE 3.
    // Before the fix THIS build already failed, above.
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
    leader.transaction(() -> {
      for (int i = 0; i < records; i++) {
        final float[] vector = new float[dimensions];
        for (int j = 0; j < dimensions; j++)
          vector[j] = ((i * 31 + j * 7) % 1_009 + 1) / 1_009f;
        leader.newVertex(typeName).set("vector", vector).save();
      }
    });
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
