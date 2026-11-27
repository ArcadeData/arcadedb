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
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.vector.LSMVectorIndex;
import com.arcadedb.index.vector.VectorQuantizationType;
import com.arcadedb.schema.TypeLSMVectorIndexBuilder;
import com.arcadedb.schema.VertexType;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #13484: a vector compaction on the leader ships the graph it rebuilt to the followers, but not the manifest and the
 * ordinal map that certify it (plain files next to the graph). A follower could reuse those pages only while nothing was
 * deleted, so the first deletion that reached it before its first search - and every schema entry that retires a file makes a
 * follower reload every index, so that search comes after a reload - sent every compacted piece to a rebuild from scratch on
 * every follower, which the leader never pays. The follower now certifies the shipped graph when it applies the compaction.
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
class VectorFollowerReusesGraphAfterFileRetirementHATest extends BaseRaftHATest {
  private static final String TYPE       = "Chunk";
  private static final String CHURN      = "Churn";
  private static final int    RECORDS    = 1_200;
  private static final int    DIMENSIONS = 16;
  private static final int    BATCH      = 300;

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected void populateDatabase() {
  }

  @AfterEach
  void resetCompactionThreshold() {
    GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.reset();
  }

  /**
   * The leader compacts the index, some of its vectors are deleted, and another index retires its files: each follower reloads
   * every index and its first search must reuse the graph the compaction shipped, on that reload and on the following ones.
   */
  @Test
  void aFollowerReusesTheGraphACompactionShippedAfterDeletionsAndReloads() throws Exception {
    final int leaderIndex = requireLeaderIndex();
    final Database leader = getServerDatabase(leaderIndex, getDatabaseName());
    final String indexName = createVectorType(leader, TYPE, 1, DIMENSIONS);
    final String churnIndex = createVectorType(leader, CHURN, 1, DIMENSIONS);

    final Random random = new Random(13484);
    insert(leader, TYPE, random, RECORDS);
    insert(leader, CHURN, random, RECORDS);
    waitForAll();
    searchEverywhere(indexName, random);

    // Tombstones for the compaction to fold away, then the compaction, then deletions on top of the graph it shipped
    leader.transaction(() -> leader.command("sql", "DELETE FROM " + TYPE + " LIMIT 300"));
    insert(leader, TYPE, random, RECORDS);
    compactOnLeader(indexName);
    waitForAll();
    final Map<String, Long> leaderStats = piece(leaderIndex, indexName).getStats();
    testEachServer(serverIndex -> {
      final Map<String, Long> stats = piece(serverIndex, indexName).getStats();
      assertThat(stats.get("totalVectors")).as("live vectors on server %d after the compaction", serverIndex)
          .isEqualTo(leaderStats.get("totalVectors"));
      assertThat(stats.get("deletedVectors")).as("deleted vectors on server %d after the compaction", serverIndex)
          .isEqualTo(leaderStats.get("deletedVectors"));
    });
    leader.transaction(() -> leader.command("sql", "DELETE FROM " + TYPE + " LIMIT 100"));

    final long[] followerRebuildsPerRound = new long[4];
    for (int round = 0; round < followerRebuildsPerRound.length; round++) {
      leader.transaction(() -> leader.command("sql", "DELETE FROM " + TYPE + " LIMIT 20"));
      // Another index retires its files: every follower reloads the whole schema
      leader.transaction(() -> leader.command("sql", "DELETE FROM " + CHURN + " LIMIT 300"));
      insert(leader, CHURN, random, 300);
      compactOnLeader(churnIndex);
      waitForAll();

      // A vector the graph holds: the reused graph has to answer with its own record, which a graph whose ordinals were
      // paired with the wrong records would not
      final Vertex probe = leader.query("sql", "SELECT FROM " + TYPE + " LIMIT 1").next().getVertex().orElseThrow();
      final float[] query = (float[]) probe.get("embedding");
      final int r = round;
      testEachServer(serverIndex -> {
        if (serverIndex == leaderIndex)
          return;
        final LSMVectorIndex piece = piece(serverIndex, indexName);
        assertThat(piece.getStats().get("deletedVectors")).as("deleted vectors on follower %d", serverIndex).isPositive();
        final long before = piece.getStats().get("graphRebuildCount");
        final List<Pair<RID, Float>> neighbours = piece.findNeighborsFromVector(query, 10);
        followerRebuildsPerRound[r] += piece.getStats().get("graphRebuildCount") - before;
        assertThat(neighbours).as("neighbours on follower %d", serverIndex).isNotEmpty();
        assertThat(neighbours.getFirst().getFirst()).as("nearest neighbour of a stored vector on follower %d", serverIndex)
            .isEqualTo(probe.getIdentity());
      });
    }
    assertThat(followerRebuildsPerRound).as("graph rebuilds on the followers per reload %s",
        Arrays.toString(followerRebuildsPerRound)).containsOnly(0L);
  }

  /**
   * The production shape of the report: a piece the leader never compacted, holding deleted vectors, and a follower reloading
   * every index each time another index retires its files. The follower built and persisted that graph itself, with its
   * manifest and ordinal map, so every later reload must reuse it through that map, whatever was deleted since, instead of
   * rebuilding it once per reload.
   */
  @Test
  void aFollowerReusesTheGraphItBuiltItselfAcrossReloadsWithDeletionsOnTop() throws Exception {
    final int leaderIndex = requireLeaderIndex();
    final Database leader = getServerDatabase(leaderIndex, getDatabaseName());
    final String indexName = createVectorType(leader, TYPE, 1, DIMENSIONS);
    final String churnIndex = createVectorType(leader, CHURN, 1, DIMENSIONS);

    final Random random = new Random(134841);
    insert(leader, TYPE, random, RECORDS);
    insert(leader, CHURN, random, RECORDS);
    waitForAll();
    // Every node builds its graph over the live set and persists it next to its manifest
    searchEverywhere(indexName, random);
    leader.transaction(() -> leader.command("sql", "DELETE FROM " + TYPE + " LIMIT 100"));

    final long[] followerRebuildsPerRound = new long[3];
    for (int round = 0; round < followerRebuildsPerRound.length; round++) {
      leader.transaction(() -> leader.command("sql", "DELETE FROM " + TYPE + " LIMIT 20"));
      // Another index retires its files: every follower reloads the whole schema, and this index comes back cold
      leader.transaction(() -> leader.command("sql", "DELETE FROM " + CHURN + " LIMIT 300"));
      insert(leader, CHURN, random, 300);
      compactOnLeader(churnIndex);
      waitForAll();

      final Vertex probe = leader.query("sql", "SELECT FROM " + TYPE + " LIMIT 1").next().getVertex().orElseThrow();
      final float[] query = (float[]) probe.get("embedding");
      final int r = round;
      testEachServer(serverIndex -> {
        if (serverIndex == leaderIndex)
          return;
        final LSMVectorIndex piece = piece(serverIndex, indexName);
        assertThat(piece.getStats().get("compactionCount")).as("compactions of the piece on follower %d", serverIndex).isZero();
        assertThat(piece.getStats().get("deletedVectors")).as("deleted vectors on follower %d", serverIndex).isPositive();
        final long before = piece.getStats().get("graphRebuildCount");
        final List<Pair<RID, Float>> neighbours = piece.findNeighborsFromVector(query, 10);
        followerRebuildsPerRound[r] += piece.getStats().get("graphRebuildCount") - before;
        assertThat(neighbours.getFirst().getFirst()).as("nearest neighbour of a stored vector on follower %d", serverIndex)
            .isEqualTo(probe.getIdentity());
      });
    }
    assertThat(followerRebuildsPerRound).as("graph rebuilds on the followers per reload %s",
        Arrays.toString(followerRebuildsPerRound)).containsOnly(0L);
  }

  /**
   * The shape of the report: four pieces, every vector deleted and replaced through openCypher while the leader compacts the
   * pieces that cross the bloat factor on its own. A follower must count what the leader counts, and search the compacted
   * pieces without rebuilding them.
   */
  @Test
  @Tag("slow")
  void aFollowerSearchesThePiecesTheLeaderCompactedDuringAReIngestWithoutRebuildingThem() throws Exception {
    GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.setValue(2);
    final int dimensions = 512;
    final int records = 4_800;
    final int leaderIndex = requireLeaderIndex();
    final Database leader = getServerDatabase(leaderIndex, getDatabaseName());
    final String indexName = createVectorType(leader, TYPE, 4, dimensions);
    leader.getSchema().getType(TYPE).createProperty("id", Integer.class);

    final Random random = new Random(1);
    for (int from = 0; from < records; from += 200) {
      final int start = from;
      leader.transaction(() -> {
        for (int i = start; i < start + 200; i++)
          leader.newVertex(TYPE).set("id", i, "embedding", randomVector(random, dimensions)).save();
      });
    }
    for (int pass = 0; pass < 3; pass++)
      for (int from = 0; from < records; from += 200) {
        final int start = from;
        leader.transaction(() -> {
          leader.command("opencypher", "MATCH (d:" + TYPE + ") WHERE d.id >= $from AND d.id < $to DETACH DELETE d",
              Map.of("from", start, "to", start + 200));
          for (int i = start; i < start + 200; i++)
            leader.newVertex(TYPE).set("id", i, "embedding", randomVector(random, dimensions)).save();
        });
      }
    ((DatabaseInternal) leader).async().waitCompletion();
    waitForAll();

    long compactions = 0;
    for (final IndexInternal piece : typeIndex(leaderIndex, indexName).getIndexesOnBuckets())
      compactions += piece.getStats().get("compactionCount");
    assertThat(compactions).as("pieces the leader compacted on its own").isPositive();

    final float[] query = randomVector(random, dimensions);
    final IndexInternal[] leaderPieces = typeIndex(leaderIndex, indexName).getIndexesOnBuckets();
    testEachServer(serverIndex -> {
      if (serverIndex == leaderIndex)
        return;
      final IndexInternal[] pieces = typeIndex(serverIndex, indexName).getIndexesOnBuckets();
      for (int i = 0; i < pieces.length; i++) {
        final LSMVectorIndex piece = (LSMVectorIndex) pieces[i];
        final Map<String, Long> leaderStats = leaderPieces[i].getStats();
        final Map<String, Long> stats = piece.getStats();
        assertThat(stats.get("totalVectors")).as("live vectors of %s on follower %d", piece.getName(), serverIndex)
            .isEqualTo(leaderStats.get("totalVectors"));
        assertThat(stats.get("deletedVectors")).as("deleted vectors of %s on follower %d", piece.getName(), serverIndex)
            .isEqualTo(leaderStats.get("deletedVectors"));
        if (leaderStats.get("compactionCount") == 0)
          continue;
        final long before = stats.get("graphRebuildCount");
        piece.findNeighborsFromVector(query, 10);
        assertThat(piece.getStats().get("graphRebuildCount") - before)
            .as("graph rebuilds of the compacted %s on follower %d", piece.getName(), serverIndex).isZero();
      }
    });
  }

  private String createVectorType(final Database leader, final String typeName, final int buckets, final int dimensions) {
    final VertexType type = leader.getSchema().buildVertexType().withName(typeName).withTotalBuckets(buckets).create();
    type.createProperty("embedding", float[].class);
    final TypeLSMVectorIndexBuilder builder = leader.getSchema().buildTypeIndex(typeName, new String[] { "embedding" })
        .withLSMVectorType();
    builder.withDimensions(dimensions);
    builder.withSimilarity("COSINE");
    builder.withQuantization(VectorQuantizationType.INT8);
    builder.withMutationsBeforeRebuild(5000);
    return builder.create().getName();
  }

  private void compactOnLeader(final String indexName) throws Exception {
    final TypeIndex index = typeIndex(requireLeaderIndex(), indexName);
    index.scheduleCompaction();
    index.compact();
  }

  private void insert(final Database leader, final String typeName, final Random random, final int records) {
    for (int from = 0; from < records; from += BATCH) {
      final int start = from;
      final int end = Math.min(records, from + BATCH);
      leader.transaction(() -> {
        for (int i = start; i < end; i++)
          leader.newVertex(typeName).set("embedding", randomVector(random, DIMENSIONS)).save();
      });
    }
  }

  private void waitForAll() {
    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);
  }

  private void searchEverywhere(final String indexName, final Random random) throws Exception {
    final float[] query = randomVector(random, DIMENSIONS);
    testEachServer(serverIndex -> assertThat(piece(serverIndex, indexName).findNeighborsFromVector(query, 10))
        .as("neighbours on server %d", serverIndex).isNotEmpty());
  }

  private TypeIndex typeIndex(final int serverIndex, final String indexName) {
    return (TypeIndex) getServerDatabase(serverIndex, getDatabaseName()).getSchema().getIndexByName(indexName);
  }

  private LSMVectorIndex piece(final int serverIndex, final String indexName) {
    return (LSMVectorIndex) typeIndex(serverIndex, indexName).getIndexesOnBuckets()[0];
  }

  private static float[] randomVector(final Random random, final int dimensions) {
    final float[] vector = new float[dimensions];
    for (int i = 0; i < dimensions; i++)
      vector[i] = random.nextFloat() * 2 - 1;
    return vector;
  }
}
