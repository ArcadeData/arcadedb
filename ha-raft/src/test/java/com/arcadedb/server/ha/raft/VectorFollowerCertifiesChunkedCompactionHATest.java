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
import com.arcadedb.database.RID;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.vector.LSMVectorIndex;
import com.arcadedb.index.vector.VectorQuantizationType;
import com.arcadedb.schema.TypeLSMVectorIndexBuilder;
import com.arcadedb.schema.VertexType;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #13484, for a compaction too large for one Raft entry, which is the size of the production report. Such a compaction
 * ships as several entries: the first creates the files, the last publishes the change, and only the first names the files it
 * creates. The follower must still certify the graph the compaction shipped when the last one is applied, or the first deletion
 * sends the compacted piece to a rebuild from scratch on every follower. The entry cap is lowered here so a few thousand vectors
 * are enough to make the compaction ship in chunks.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class VectorFollowerCertifiesChunkedCompactionHATest extends BaseRaftHATest {
  // A 256KB entry cap, with the write buffer kept above it (the server refuses to start otherwise)
  private static final String APPEND_BUFFER  = "256KB";
  private static final String WRITE_BUFFER   = "512KB";
  private static final String TYPE           = "Chunk";
  private static final int    RECORDS        = 2_400;
  private static final int    DIMENSIONS     = 64;
  private static final int    RECORDS_PER_TX = 200;

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
    config.setValue(GlobalConfiguration.HA_APPEND_BUFFER_SIZE, APPEND_BUFFER);
    config.setValue(GlobalConfiguration.HA_WRITE_BUFFER_SIZE, WRITE_BUFFER);
  }

  @Test
  void aFollowerCertifiesTheGraphOfACompactionShippedInSeveralEntries() throws Exception {
    final int leaderIndex = requireLeaderIndex();
    final Database leader = getServerDatabase(leaderIndex, getDatabaseName());
    final String indexName = createVectorType(leader);

    final Random random = new Random(13484);
    insert(leader, random, RECORDS);
    // Tombstones for the compaction to fold away
    leader.transaction(() -> leader.command("sql", "DELETE FROM " + TYPE + " LIMIT 400"));
    waitForAll();

    // Nothing else writes meanwhile, so the entries the leader appends across the compaction are the compaction's own
    final long appliedBefore = getRaftPlugin(leaderIndex).getRaftHAServer().getLastAppliedIndex();
    final TypeIndex leaderIndexOfType = typeIndex(leaderIndex, indexName);
    leaderIndexOfType.scheduleCompaction();
    leaderIndexOfType.compact();
    waitForAll();
    final long compactionEntries = getRaftPlugin(leaderIndex).getRaftHAServer().getLastAppliedIndex() - appliedBefore;

    final LSMVectorIndex leaderPiece = piece(leaderIndex, indexName);
    assertThat(leaderPiece.getStats().get("compactionCount")).as("compactions of the piece on the leader").isEqualTo(1L);
    assertThat(compactionEntries).as("Raft entries the compaction shipped in: more than one, so only the first named its files")
        .isGreaterThan(1L);

    // Deletions on top of the graph the compaction shipped, before any follower searched it
    leader.transaction(() -> leader.command("sql", "DELETE FROM " + TYPE + " LIMIT 50"));
    waitForAll();

    final Vertex probe = leader.query("sql", "SELECT FROM " + TYPE + " LIMIT 1").next().getVertex().orElseThrow();
    final float[] query = (float[]) probe.get("embedding");
    final Map<String, Long> leaderStats = leaderPiece.getStats();
    testEachServer(serverIndex -> {
      if (serverIndex == leaderIndex)
        return;
      final LSMVectorIndex piece = piece(serverIndex, indexName);
      final Map<String, Long> stats = piece.getStats();
      assertThat(stats.get("totalVectors")).as("live vectors on follower %d", serverIndex).isEqualTo(leaderStats.get("totalVectors"));
      assertThat(stats.get("deletedVectors")).as("deleted vectors on follower %d", serverIndex).isEqualTo(50L);

      final long before = stats.get("graphRebuildCount");
      final List<Pair<RID, Float>> neighbours = piece.findNeighborsFromVector(query, 10);
      assertThat(piece.getStats().get("graphRebuildCount") - before)
          .as("graph rebuilds of the piece compacted in several entries on follower %d", serverIndex).isZero();
      assertThat(neighbours.getFirst().getFirst()).as("nearest neighbour of a stored vector on follower %d", serverIndex)
          .isEqualTo(probe.getIdentity());
    });
  }

  private String createVectorType(final Database leader) {
    final VertexType type = leader.getSchema().buildVertexType().withName(TYPE).withTotalBuckets(1).create();
    type.createProperty("embedding", float[].class);
    final TypeLSMVectorIndexBuilder builder = leader.getSchema().buildTypeIndex(TYPE, new String[] { "embedding" })
        .withLSMVectorType();
    builder.withDimensions(DIMENSIONS);
    builder.withSimilarity("COSINE");
    builder.withQuantization(VectorQuantizationType.INT8);
    builder.withMutationsBeforeRebuild(5000);
    return builder.create().getName();
  }

  private void insert(final Database leader, final Random random, final int records) {
    for (int from = 0; from < records; from += RECORDS_PER_TX) {
      final int start = from;
      final int end = Math.min(records, from + RECORDS_PER_TX);
      leader.transaction(() -> {
        for (int i = start; i < end; i++)
          leader.newVertex(TYPE).set("embedding", randomVector(random)).save();
      });
    }
  }

  private void waitForAll() {
    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);
  }

  private TypeIndex typeIndex(final int serverIndex, final String indexName) {
    return (TypeIndex) getServerDatabase(serverIndex, getDatabaseName()).getSchema().getIndexByName(indexName);
  }

  private LSMVectorIndex piece(final int serverIndex, final String indexName) {
    return (LSMVectorIndex) typeIndex(serverIndex, indexName).getIndexesOnBuckets()[0];
  }

  private static float[] randomVector(final Random random) {
    final float[] vector = new float[DIMENSIONS];
    for (int i = 0; i < DIMENSIONS; i++)
      vector[i] = random.nextFloat() * 2 - 1;
    return vector;
  }
}
