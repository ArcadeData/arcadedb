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
import com.arcadedb.index.Index;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.vector.LSMVectorIndex;
import com.arcadedb.schema.TypeLSMVectorIndexBuilder;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9428: on a 3-node cluster, a transaction committed on a FOLLOWER that inserts vertices into a type with an
 * LSM_VECTOR index replicated the records but not their index entries, on every node including the one that wrote
 * them. The follower minted vector ids its own allocator had not moved past: a replicated page added its entries to
 * the location index without advancing the allocator, so the follower's ids superseded the leader's. The same gap left
 * the leader behind ids a build on a follower allocated, and its next insert superseded one of them.
 */
class Issue9428FollowerVectorInsertHATest extends BaseRaftHATest {

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
  void vectorsInsertedOnAFollowerReachTheIndexOnEveryNode() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final Database leader = getServerDatabase(leaderIndex, getDatabaseName());

    final TypeIndex index = createVectorIndex(leader);

    insert(leader, 0, BATCH);
    assertIndexOnEveryServer(index.getName(), BATCH);

    final int followerIndex = (leaderIndex + 1) % getServerCount();
    final Database follower = getServerDatabase(followerIndex, getDatabaseName());
    insert(follower, BATCH, BATCH);
    assertIndexOnEveryServer(index.getName(), 2L * BATCH);

    insert(leader, 2 * BATCH, 1);
    assertIndexOnEveryServer(index.getName(), 2L * BATCH + 1);
  }

  /**
   * A build run on a follower allocates vector ids too, and the leader learns of them only from the replicated pages.
   * A build over a populated index appends a second entry per record, which is why the expected count doubles.
   */
  @Test
  void anInsertOnTheLeaderAfterABuildOnAFollowerGetsAFreshVectorId() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final Database leader = getServerDatabase(leaderIndex, getDatabaseName());
    final TypeIndex index = createVectorIndex(leader);

    insert(leader, 0, BATCH);
    assertIndexOnEveryServer(index.getName(), BATCH, BATCH);

    final int followerIndex = (leaderIndex + 1) % getServerCount();
    final Database follower = getServerDatabase(followerIndex, getDatabaseName());
    final LSMVectorIndex bucketIndex =
        (LSMVectorIndex) ((TypeIndex) follower.getSchema().getIndexByName(index.getName())).getIndexesOnBuckets()[0];
    bucketIndex.build(null, null);
    // TODO(#9506) TWICE THE RECORDS: THE BUILD APPENDS A SECOND ENTRY PER RECORD (ISSUE #9506). WHEN THAT IS FIXED THESE TWO
    // EXPECTATIONS BECOME BATCH AND BATCH + 1, AND THE INSERT BELOW STILL HAS TO GET AN ID OF ITS OWN
    assertIndexOnEveryServer(index.getName(), BATCH, 2L * BATCH);

    insert(leader, BATCH, 1);
    assertIndexOnEveryServer(index.getName(), BATCH + 1, 2L * BATCH + 1);
  }

  private static TypeIndex createVectorIndex(final Database leader) {
    final VertexType type = leader.getSchema().buildVertexType().withName(TYPE_NAME).withTotalBuckets(1).create();
    type.createProperty("vector", float[].class);
    final TypeLSMVectorIndexBuilder builder = leader.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "vector" })
        .withLSMVectorType();
    builder.withDimensions(DIMENSIONS);
    return builder.create();
  }

  private static void insert(final Database database, final int from, final int count) {
    database.transaction(() -> {
      for (int i = from; i < from + count; i++)
        database.newVertex(TYPE_NAME).set("vector", new float[] { (i % 97 + 1) / 97f, (i * 7 % 89 + 1) / 89f }).save();
    });
  }

  private void assertIndexOnEveryServer(final String indexName, final long expected) throws Exception {
    assertIndexOnEveryServer(indexName, expected, expected);
  }

  private void assertIndexOnEveryServer(final String indexName, final long records, final long entries) throws Exception {
    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    testEachServer(serverIndex -> {
      final Database database = getServerDatabase(serverIndex, getDatabaseName());
      assertThat(database.countType(TYPE_NAME, false)).as("records on server %d", serverIndex).isEqualTo(records);
      final Index index = database.getSchema().getIndexByName(indexName);
      assertThat(index.countEntries()).as("vector index entries on server %d", serverIndex).isEqualTo(entries);
    });
  }
}
