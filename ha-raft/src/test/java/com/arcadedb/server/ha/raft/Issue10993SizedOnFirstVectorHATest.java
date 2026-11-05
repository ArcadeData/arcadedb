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
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.vector.LSMVectorIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10993: an LSM_VECTOR index created with {@code sizeOnFirstVector} (openCypher {@code CREATE VECTOR INDEX} without
 * {@code vector.dimensions} over a property that holds no vector yet) fixes its dimension on the first put. On a cluster the
 * leader marks the schema dirty and saves it at the commit; a follower learns the size from the schema the leader ships or,
 * failing that, from the vectors it holds ({@code LSMVectorIndex.deriveDimensionsFromData}) before its first search.
 * <p>
 * The first vector is written through the leader in one test and through a follower in the other, and every node is asked
 * for the dimensions of its index and searched.
 */
class Issue10993SizedOnFirstVectorHATest extends BaseRaftHATest {
  static final String TYPE_NAME  = "Doc";
  static final String INDEX_NAME = "emb";
  static final int    DIMENSIONS = 16;
  static final int    VECTORS    = 40;

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected void populateDatabase() {
  }

  @Test
  void aFirstVectorWrittenThroughTheLeaderSizesTheIndexOnEveryNode() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final Database leader = getServerDatabase(leaderIndex, getDatabaseName());
    createUnsizedIndex(leader);
    waitForAllServers();

    // the index reached every node still waiting for its first vector, and an empty one answers an empty result
    for (int server = 0; server < getServerCount(); server++) {
      assertThat(dimensionsOn(server)).as("dimensions on server %d before any vector", server).containsOnly(0);
      final RID found = withResyncRetry(server, db -> nearest(db, embedding(0)));
      assertThat(found).as("search on server %d", server).isNull();
    }

    insert(leader, 0, 1);
    waitForAllServers();
    assertEveryNodeIsSizedAndSearches(1);

    insert(leader, 1, VECTORS - 1);
    waitForAllServers();
    assertEveryNodeIsSizedAndSearches(VECTORS);
  }

  @Test
  void aFirstVectorWrittenThroughAFollowerSizesTheIndexOnEveryNode() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    createUnsizedIndex(getServerDatabase(leaderIndex, getDatabaseName()));
    waitForAllServers();

    final int followerIndex = (leaderIndex + 1) % getServerCount();
    insert(getServerDatabase(followerIndex, getDatabaseName()), 0, 1);
    waitForAllServers();
    assertEveryNodeIsSizedAndSearches(1);

    // the other follower and the leader write vectors of the decided length only
    insert(getServerDatabase(leaderIndex, getDatabaseName()), 1, VECTORS - 1);
    waitForAllServers();
    assertEveryNodeIsSizedAndSearches(VECTORS);
  }

  static void createUnsizedIndex(final Database leader) {
    leader.command("sql", "CREATE VERTEX TYPE " + TYPE_NAME + " BUCKETS 1");
    leader.command("sql", "CREATE PROPERTY " + TYPE_NAME + ".id INTEGER");
    leader.command("sql", "CREATE PROPERTY " + TYPE_NAME + ".embedding ARRAY_OF_FLOATS");
    leader.command("opencypher", "CREATE VECTOR INDEX " + INDEX_NAME + " FOR (n:" + TYPE_NAME + ") ON n.embedding OPTIONS "
        + "{indexConfig: {`vector.similarity_function`: 'euclidean'}}");
  }

  static void insert(final Database database, final int from, final int count) {
    database.transaction(() -> {
      for (int i = from; i < from + count; i++)
        database.newVertex(TYPE_NAME).set("id", i, "embedding", embedding(i)).save();
    });
  }

  /** Every node finds each record by its own embedding, and reports the decided dimensions once it was searched. */
  void assertEveryNodeIsSizedAndSearches(final int records) {
    for (int server = 0; server < getServerCount(); server++) {
      final int serverIndex = server;
      for (int id = 0; id < records; id++) {
        final int record = id;
        final RID expected = withResyncRetry(serverIndex, db -> recordOf(db, record));
        final RID found = withResyncRetry(serverIndex, db -> nearest(db, embedding(record)));
        assertThat(found).as("server %d must find record %d", serverIndex, record).isEqualTo(expected);
      }
      assertThat(dimensionsOn(serverIndex)).as("dimensions on server %d", serverIndex).containsOnly(DIMENSIONS);
    }
  }

  /** The dimensions of every bucket index behind the type index, on the given node. */
  List<Integer> dimensionsOn(final int serverIndex) {
    return withResyncRetry(serverIndex, db -> {
      final TypeIndex typeIndex = (TypeIndex) db.getSchema().getIndexByName(INDEX_NAME);
      return Arrays.stream(typeIndex.getIndexesOnBuckets()).map(i -> ((LSMVectorIndex) i).getDimensions()).toList();
    });
  }

  static RID nearest(final Database db, final float[] query) {
    final TypeIndex typeIndex = (TypeIndex) db.getSchema().getIndexByName(INDEX_NAME);
    for (final IndexInternal bucketIndex : typeIndex.getIndexesOnBuckets()) {
      final List<Pair<RID, Float>> answer = ((LSMVectorIndex) bucketIndex).findNeighborsFromVector(query, 1);
      if (!answer.isEmpty())
        return answer.get(0).getFirst();
    }
    return null;
  }

  static RID recordOf(final Database db, final int id) {
    try (final ResultSet rs = db.query("sql", "SELECT @rid AS rid FROM " + TYPE_NAME + " WHERE id = ?", id)) {
      return rs.next().getProperty("rid");
    }
  }

  static float[] embedding(final int id) {
    final Random random = new Random(0x10993L * 31 + id);
    final float[] vector = new float[DIMENSIONS];
    for (int d = 0; d < DIMENSIONS; d++)
      vector[d] = (float) random.nextGaussian();
    return vector;
  }
}
