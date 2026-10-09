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
import com.arcadedb.database.RID;
import com.arcadedb.index.Index;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.vector.LSMVectorIndex;
import com.arcadedb.schema.TypeLSMVectorIndexBuilder;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9506, end to end on a 3-node cluster: a build on a follower over a populated LSM_VECTOR index appended a second
 * entry per record, and the nodes then disagreed on {@code countEntries()} - the node that searched kept one entry per
 * record, and a delete tombstoned two ids on a node holding the duplicates and one on a node that had dropped them. The
 * build now replaces each record's vector, so every node agrees after a search and after a delete.
 */
class Issue9506BuildOverPopulatedIndexHATest extends BaseRaftHATest {

  private static final String TYPE_NAME  = "Probe";
  private static final int    DIMENSIONS = 2;
  private static final int    BATCH      = 10;

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected void populateDatabase() {
  }

  @Test
  void everyNodeAgreesOnTheEntriesAfterAFollowerBuildASearchAndADelete() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final Database leader = getServerDatabase(leaderIndex, getDatabaseName());
    final TypeIndex index = createVectorIndex(leader);

    final List<RID> rids = new ArrayList<>();
    leader.transaction(() -> {
      for (int i = 0; i < BATCH; i++)
        rids.add(leader.newVertex(TYPE_NAME).set("vector", new float[] { (i + 1) / 11f, (BATCH - i) / 11f }).save()
            .getIdentity());
    });
    assertIndexOnEveryServer(index.getName(), BATCH);

    final int followerIndex = (leaderIndex + 1) % getServerCount();
    final Database follower = getServerDatabase(followerIndex, getDatabaseName());
    final LSMVectorIndex followerBucketIndex =
        (LSMVectorIndex) ((TypeIndex) follower.getSchema().getIndexByName(index.getName())).getIndexesOnBuckets()[0];
    followerBucketIndex.build(null, null);
    assertIndexOnEveryServer(index.getName(), BATCH);

    // A search on ONE node builds that node's graph from its pages, keyed by RID
    assertThat(followerBucketIndex.findNeighborsFromVector(new float[] { 0.5f, 0.5f }, BATCH * 3)).hasSize(BATCH);
    assertIndexOnEveryServer(index.getName(), BATCH);

    leader.transaction(() -> rids.getFirst().asVertex().delete());
    assertIndexOnEveryServer(index.getName(), BATCH - 1);
  }

  private static TypeIndex createVectorIndex(final Database leader) {
    final VertexType type = leader.getSchema().buildVertexType().withName(TYPE_NAME).withTotalBuckets(1).create();
    type.createProperty("vector", float[].class);
    final TypeLSMVectorIndexBuilder builder = leader.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "vector" })
        .withLSMVectorType();
    builder.withDimensions(DIMENSIONS);
    return builder.create();
  }

  private void assertIndexOnEveryServer(final String indexName, final long expected) throws Exception {
    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    testEachServer(serverIndex -> {
      final Database database = getServerDatabase(serverIndex, getDatabaseName());
      assertThat(database.countType(TYPE_NAME, false)).as("records on server %d", serverIndex).isEqualTo(expected);
      final Index index = database.getSchema().getIndexByName(indexName);
      assertThat(index.countEntries()).as("vector index entries on server %d", serverIndex).isEqualTo(expected);
    });
  }
}
