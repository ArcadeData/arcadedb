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
package com.arcadedb.function.sql.vector;

import com.arcadedb.TestHelper;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@code vector.fuse} - server-side hybrid retrieval fusion (issue #4066).
 * <p>
 * Verifies fusion across dense + sparse + plain SQL sources using each strategy
 * (RRF, DBSF, LINEAR), weighted variants, and post-fusion {@code groupBy} / {@code groupSize}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SQLFunctionVectorFuseTest extends TestHelper {

  private static final String TYPE_NAME      = "FuseDoc";
  private static final String DENSE_IDX      = "FuseDoc[dense]";
  private static final String SPARSE_IDX     = "FuseDoc[tokens,weights]";

  private void buildSchema() {
    database.transaction(() -> {
      final DocumentType t = database.getSchema().createDocumentType(TYPE_NAME);
      t.createProperty("dense", Type.ARRAY_OF_FLOATS);
      t.createProperty("tokens", Type.ARRAY_OF_INTEGERS);
      t.createProperty("weights", Type.ARRAY_OF_FLOATS);
      t.createProperty("source", Type.STRING);

      database.getSchema()
          .buildTypeIndex(TYPE_NAME, new String[] { "dense" })
          .withLSMVectorType()
          .withDimensions(3)
          .withSimilarity("COSINE")
          .create();

      database.getSchema()
          .buildTypeIndex(TYPE_NAME, new String[] { "tokens", "weights" })
          .withSparseVectorType()
          .withDimensions(50)
          .create();
    });
  }

  /**
   * Inserts a 5-document fixture where:
   * <ul>
   *   <li>doc A wins on dense ranking and wins on sparse ranking (first across both)</li>
   *   <li>doc B wins on dense, mid on sparse</li>
   *   <li>doc C is mid on both</li>
   *   <li>doc D mid on dense, wins on sparse</li>
   *   <li>doc E loses both</li>
   * </ul>
   * The exact RID returned by each insert is captured for assertions.
   */
  private RID[] seedFixture() {
    final RID[] rids = new RID[5];
    database.transaction(() -> {
      // Dense vectors: query [1,0,0] picks A first, B second, then C, D, E.
      // Sparse vectors: query at dim 1 picks A first, D second, then B, C, E.
      rids[0] = newDoc("A", new float[] { 1.0f, 0.0f, 0.0f }, new int[] { 1, 5 }, new float[] { 0.9f, 0.5f }, "g1");
      rids[1] = newDoc("B", new float[] { 0.95f, 0.1f, 0.05f }, new int[] { 1, 6 }, new float[] { 0.4f, 0.5f }, "g1");
      rids[2] = newDoc("C", new float[] { 0.5f, 0.5f, 0.0f }, new int[] { 1, 7 }, new float[] { 0.3f, 0.5f }, "g2");
      rids[3] = newDoc("D", new float[] { 0.4f, 0.6f, 0.1f }, new int[] { 1, 8 }, new float[] { 0.7f, 0.5f }, "g2");
      rids[4] = newDoc("E", new float[] { 0.1f, 0.2f, 0.97f }, new int[] { 9 }, new float[] { 0.5f }, "g3");
    });
    return rids;
  }

  private RID newDoc(final String src, final float[] dense, final int[] tokens, final float[] weights, final String group) {
    final MutableDocument d = database.newDocument(TYPE_NAME);
    d.set("source", src);
    d.set("dense", dense);
    d.set("tokens", tokens);
    d.set("weights", weights);
    d.set("group", group);
    d.save();
    return d.getIdentity();
  }

  @Test
  void requireAtLeastTwoSources() {
    buildSchema();
    seedFixture();

    assertThatThrownBy(() -> {
      try (ResultSet rs = database.query("sql",
          "SELECT expand(`vector.fuse`(`vector.neighbors`(?, ?, ?), { fusion: 'RRF' }))",
          DENSE_IDX, new float[] { 1.0f, 0.0f, 0.0f }, 3)) {
        while (rs.hasNext()) rs.next();
      }
    }).hasMessageContaining("at least 2");
  }

  @Test
  void rrfTwoWayDenseAndSparse() {
    buildSchema();
    final RID[] rids = seedFixture();

    final ResultSet rs = database.query("sql", """
        SELECT expand(`vector.fuse`(
            `vector.neighbors`(?, ?, ?),
            `vector.sparseNeighbors`(?, ?, ?, ?),
            { fusion: 'RRF' }
        ))""",
        DENSE_IDX, new float[] { 1.0f, 0.0f, 0.0f }, 5,
        SPARSE_IDX, new int[] { 1 }, new float[] { 1.0f }, 5);

    final List<RID> ranked = new ArrayList<>();
    while (rs.hasNext())
      ranked.add((RID) rs.next().getProperty("@rid"));

    // Doc A is first in both rankings (dense rank 1, sparse rank 1) so RRF must surface it first.
    assertThat(ranked).as("fused results").contains(rids[0]);
    assertThat(ranked.get(0)).as("rank 0").isEqualTo(rids[0]);
    // Doc E is absent from sparse (no shared dim with query) and last on dense, so it must rank below
    // anything that appears in both lists.
    if (ranked.contains(rids[4]))
      assertThat(ranked.indexOf(rids[4])).isGreaterThan(0);
  }

  @Test
  void rrfWeightsTiltTheRanking() {
    buildSchema();
    final RID[] rids = seedFixture();

    // With dense weighted 10x sparse, B (dense rank 2, sparse rank 3) must beat D (dense rank 4,
    // sparse rank 2): under unweighted RRF, B = 1/62 + 1/63, D = 1/64 + 1/62; weighting dense up
    // breaks the tie strongly in B's favour.
    final ResultSet rs = database.query("sql", """
        SELECT expand(`vector.fuse`(
            `vector.neighbors`(?, ?, ?),
            `vector.sparseNeighbors`(?, ?, ?, ?),
            { fusion: 'RRF', weights: [10.0, 1.0] }
        ))""",
        DENSE_IDX, new float[] { 1.0f, 0.0f, 0.0f }, 5,
        SPARSE_IDX, new int[] { 1 }, new float[] { 1.0f }, 5);

    final List<RID> ranked = new ArrayList<>();
    while (rs.hasNext())
      ranked.add((RID) rs.next().getProperty("@rid"));

    assertThat(ranked.indexOf(rids[1])).as("B (dense-favoured) rank")
        .isLessThan(ranked.indexOf(rids[3]));
  }

  @Test
  void linearStrategyUsesScores() {
    buildSchema();
    final RID[] rids = seedFixture();

    final ResultSet rs = database.query("sql", """
        SELECT expand(`vector.fuse`(
            `vector.neighbors`(?, ?, ?),
            `vector.sparseNeighbors`(?, ?, ?, ?),
            { fusion: 'LINEAR', weights: [1.0, 1.0] }
        ))""",
        DENSE_IDX, new float[] { 1.0f, 0.0f, 0.0f }, 5,
        SPARSE_IDX, new int[] { 1 }, new float[] { 1.0f }, 5);

    final List<RID>   ranked = new ArrayList<>();
    final List<Float> scores = new ArrayList<>();
    while (rs.hasNext()) {
      final Result row = rs.next();
      ranked.add((RID) row.getProperty("@rid"));
      scores.add(((Number) row.getProperty("score")).floatValue());
    }

    assertThat(ranked.get(0)).as("LINEAR top-1").isEqualTo(rids[0]);
    // Scores must be in non-increasing order.
    for (int i = 1; i < scores.size(); i++)
      assertThat(scores.get(i)).isLessThanOrEqualTo(scores.get(i - 1));
  }

  @Test
  void dbsfStrategyNormalizesAcrossSources() {
    buildSchema();
    final RID[] rids = seedFixture();

    final ResultSet rs = database.query("sql", """
        SELECT expand(`vector.fuse`(
            `vector.neighbors`(?, ?, ?),
            `vector.sparseNeighbors`(?, ?, ?, ?),
            { fusion: 'DBSF' }
        ))""",
        DENSE_IDX, new float[] { 1.0f, 0.0f, 0.0f }, 5,
        SPARSE_IDX, new int[] { 1 }, new float[] { 1.0f }, 5);

    final List<RID> ranked = new ArrayList<>();
    while (rs.hasNext())
      ranked.add((RID) rs.next().getProperty("@rid"));

    // DBSF must surface doc A (top of both distributions after normalization).
    assertThat(ranked.get(0)).isEqualTo(rids[0]);
  }

  @Test
  void groupByCollapsesAfterFusion() {
    buildSchema();
    final RID[] rids = seedFixture();

    // Without groupBy we'd expect A, B, ... With groupBy=group and groupSize=1, only one
    // representative per group survives. A is in g1, D is in g2, E is in g3 → 3 distinct groups.
    final ResultSet rs = database.query("sql", """
        SELECT expand(`vector.fuse`(
            `vector.neighbors`(?, ?, ?),
            `vector.sparseNeighbors`(?, ?, ?, ?),
            { fusion: 'RRF', groupBy: 'group', groupSize: 1 }
        ))""",
        DENSE_IDX, new float[] { 1.0f, 0.0f, 0.0f }, 5,
        SPARSE_IDX, new int[] { 1 }, new float[] { 1.0f }, 5);

    final List<RID>     ranked = new ArrayList<>();
    final HashSet<String> groups = new HashSet<>();
    while (rs.hasNext()) {
      final Result row = rs.next();
      ranked.add((RID) row.getProperty("@rid"));
      groups.add((String) row.getProperty("group"));
    }

    assertThat(groups.size()).as("each group appears at most once").isEqualTo(ranked.size());
    assertThat(ranked.get(0)).as("best member of best group is first").isEqualTo(rids[0]);
  }

  @Test
  void rrfFallbackOnScorelessSources() {
    // RRF only needs ordered RIDs; it must work even when sources do not expose a score column.
    buildSchema();
    final RID[] rids = seedFixture();

    final ResultSet rs = database.query("sql", """
        SELECT expand(`vector.fuse`(
            (SELECT @rid FROM FuseDoc ORDER BY source ASC),
            (SELECT @rid FROM FuseDoc ORDER BY source DESC),
            { fusion: 'RRF' }
        ))""");

    final List<RID> ranked = new ArrayList<>();
    while (rs.hasNext())
      ranked.add((RID) rs.next().getProperty("@rid"));

    // Each doc appears in both sources (one ascending, one descending). RRF on perfectly
    // mirrored rankings yields identical fused scores for every RID; we just need 5 results.
    assertThat(ranked).hasSize(5);
    assertThat(new HashSet<>(ranked)).containsExactlyInAnyOrder(rids[0], rids[1], rids[2], rids[3], rids[4]);
  }

  @Test
  void threeWayFusionAcceptsMixedSources() {
    buildSchema();
    final RID[] rids = seedFixture();

    // Three sources of different shapes:
    //   1. dense `vector.neighbors`
    //   2. sparse `vector.sparseNeighbors`
    //   3. plain SQL ordered by a payload field (no score column)
    final ResultSet rs = database.query("sql", """
        SELECT expand(`vector.fuse`(
            `vector.neighbors`(?, ?, ?),
            `vector.sparseNeighbors`(?, ?, ?, ?),
            (SELECT @rid FROM FuseDoc ORDER BY source ASC),
            { fusion: 'RRF' }
        ))""",
        DENSE_IDX, new float[] { 1.0f, 0.0f, 0.0f }, 5,
        SPARSE_IDX, new int[] { 1 }, new float[] { 1.0f }, 5);

    final List<RID> ranked = new ArrayList<>();
    while (rs.hasNext())
      ranked.add((RID) rs.next().getProperty("@rid"));

    // A wins under RRF: rank 1 in dense, rank 1 in sparse, rank 1 in plain SQL (alphabetical).
    assertThat(ranked.get(0)).as("three-way RRF top-1").isEqualTo(rids[0]);
    assertThat(ranked).hasSize(5);
  }

  @Test
  void limitOptionCapsResults() {
    buildSchema();
    final RID[] rids = seedFixture();

    final ResultSet rs = database.query("sql", """
        SELECT expand(`vector.fuse`(
            `vector.neighbors`(?, ?, ?),
            `vector.sparseNeighbors`(?, ?, ?, ?),
            { fusion: 'RRF', limit: 2 }
        ))""",
        DENSE_IDX, new float[] { 1.0f, 0.0f, 0.0f }, 5,
        SPARSE_IDX, new int[] { 1 }, new float[] { 1.0f }, 5);

    final List<RID> ranked = new ArrayList<>();
    while (rs.hasNext())
      ranked.add((RID) rs.next().getProperty("@rid"));

    assertThat(ranked).hasSizeLessThanOrEqualTo(2);
    assertThat(ranked.get(0)).isEqualTo(rids[0]);
  }
}
