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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.vector.LSMVectorIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.TypeLSMVectorIndexBuilder;
import com.arcadedb.schema.VertexType;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9857: the first vector search on a node builds that node's in-memory graph, and a node that receives its vector
 * index pages through replication used to keep searching that graph forever. A replicated page updated the location index
 * only: unlike a local {@code put()}/{@code remove()} it queued no delta entry, counted no mutation and left the graph
 * IMMUTABLE, so the vectors replicated after the first search were invisible to every search on that node, and a follower
 * promoted to leader kept the gap. The same path applies a follower's own commit on the leader (issue #9428), so a vector
 * inserted on a follower was invisible to the leader's searches too.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9857ReplicatedVectorsReachSearchHATest extends BaseRaftHATest {
  private static final String TYPE_NAME  = "Chunk";
  private static final int    DIMENSIONS = 16;
  private static final int    FIRST      = 200;
  private static final int    SECOND     = 100;

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected void populateDatabase() {
  }

  @Test
  void everyNodeFindsTheVectorsReplicatedAfterItsFirstSearch() throws Exception {
    final Database leader = leaderDatabase();
    final String indexName = createVectorIndex(leader);

    insert(leader, 0, FIRST);
    waitForAllServers();
    // The first search builds each node's graph
    assertEveryNodeFindsByOwnEmbedding(indexName, 0, FIRST);

    insert(leader, FIRST, SECOND);
    waitForAllServers();
    assertEveryNodeFindsByOwnEmbedding(indexName, FIRST, SECOND);
  }

  @Test
  void everyNodeSeesTheUpdatesAndDeletesReplicatedAfterItsFirstSearch() throws Exception {
    final Database leader = leaderDatabase();
    final String indexName = createVectorIndex(leader);

    insert(leader, 0, FIRST);
    waitForAllServers();
    assertEveryNodeFindsByOwnEmbedding(indexName, 0, FIRST);

    // Re-embed 0..19 and delete 20..39
    leader.transaction(() -> {
      for (int i = 0; i < 20; i++) {
        final MutableDocument doc = recordOf(leader, i).asDocument().modify();
        doc.set("embedding", movedEmbedding(i)).save();
      }
      for (int i = 20; i < 40; i++)
        recordOf(leader, i).asVertex().delete();
    });
    waitForAllServers();

    for (int server = 0; server < getServerCount(); server++) {
      final int serverIndex = server;
      for (int i = 0; i < 20; i++) {
        final int id = i;
        final RID expected = withResyncRetry(serverIndex, db -> recordOf(db, id));
        assertThat(nearest(serverIndex, indexName, movedEmbedding(id))).as("server %d must find record %d by its new embedding",
            serverIndex, id).isEqualTo(expected);
        assertThat(nearest(serverIndex, indexName, embedding(id))).as(
            "server %d must not answer the old embedding of record %d with the record itself", serverIndex, id).isNotEqualTo(expected);
      }

      final long deletedLeft = withResyncRetry(serverIndex, db -> {
        try (final ResultSet rs = db.query("sql", "SELECT count(id) AS n FROM " + TYPE_NAME + " WHERE id >= 20 AND id < 40")) {
          return ((Number) rs.next().getProperty("n")).longValue();
        }
      });
      assertThat(deletedLeft).as("deleted records left on server %d", serverIndex).isZero();
      for (int i = 20; i < 40; i++) {
        final int id = i;
        final List<Pair<RID, Float>> answer = withResyncRetry(serverIndex,
            db -> vectorIndex(db, indexName).findNeighborsFromVector(embedding(id), 5));
        for (final Pair<RID, Float> neighbor : answer) {
          final boolean exists = withResyncRetry(serverIndex, db -> db.existsRecord(neighbor.getFirst()));
          assertThat(exists).as("server %d answered the embedding of deleted record %d with a record that does not exist",
              serverIndex, id).isTrue();
        }
      }
    }
  }

  @Test
  void theLeaderFindsAVectorInsertedOnAFollowerAfterItsFirstSearch() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final Database leader = getServerDatabase(leaderIndex, getDatabaseName());
    final String indexName = createVectorIndex(leader);

    insert(leader, 0, FIRST);
    waitForAllServers();
    assertEveryNodeFindsByOwnEmbedding(indexName, 0, FIRST);

    final int followerIndex = (leaderIndex + 1) % getServerCount();
    insert(getServerDatabase(followerIndex, getDatabaseName()), FIRST, SECOND);
    waitForAllServers();
    assertEveryNodeFindsByOwnEmbedding(indexName, FIRST, SECOND);
  }

  private Database leaderDatabase() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    return getServerDatabase(leaderIndex, getDatabaseName());
  }

  private static String createVectorIndex(final Database leader) {
    final VertexType type = leader.getSchema().buildVertexType().withName(TYPE_NAME).withTotalBuckets(1).create();
    type.createProperty("id", Integer.class);
    type.createProperty("embedding", float[].class);
    final TypeLSMVectorIndexBuilder builder = leader.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "embedding" })
        .withLSMVectorType();
    builder.withDimensions(DIMENSIONS);
    return builder.create().getName();
  }

  private static void insert(final Database database, final int from, final int count) {
    database.transaction(() -> {
      for (int i = from; i < from + count; i++)
        database.newVertex(TYPE_NAME).set("id", i, "embedding", embedding(i)).save();
    });
  }

  /** Every node answers each record's own embedding with that record. */
  private void assertEveryNodeFindsByOwnEmbedding(final String indexName, final int from, final int count) {
    for (int server = 0; server < getServerCount(); server++) {
      final int serverIndex = server;
      for (int i = from; i < from + count; i++) {
        final int id = i;
        final RID expected = withResyncRetry(serverIndex, db -> recordOf(db, id));
        assertThat(nearest(serverIndex, indexName, embedding(id))).as("server %d must find record %d by its own embedding",
            serverIndex, id).isEqualTo(expected);
      }
    }
  }

  private RID nearest(final int serverIndex, final String indexName, final float[] query) {
    return withResyncRetry(serverIndex, db -> {
      final List<Pair<RID, Float>> answer = vectorIndex(db, indexName).findNeighborsFromVector(query, 1);
      return answer.isEmpty() ? null : answer.getFirst().getFirst();
    });
  }

  private static RID recordOf(final Database db, final int id) {
    try (final ResultSet rs = db.query("sql", "SELECT @rid AS rid FROM " + TYPE_NAME + " WHERE id = ?", id)) {
      return rs.next().getProperty("rid");
    }
  }

  private static LSMVectorIndex vectorIndex(final Database db, final String indexName) {
    return (LSMVectorIndex) ((TypeIndex) db.getSchema().getIndexByName(indexName)).getIndexesOnBuckets()[0];
  }

  private static float[] embedding(final int id) {
    final Random random = new Random(0x9857L * 31 + id);
    final float[] vector = new float[DIMENSIONS];
    for (int d = 0; d < DIMENSIONS; d++)
      vector[d] = (float) random.nextGaussian();
    return vector;
  }

  /** The embedding a re-embedded record gets: unrelated to its first one. */
  private static float[] movedEmbedding(final int id) {
    return embedding(id + 1_000_000);
  }
}
