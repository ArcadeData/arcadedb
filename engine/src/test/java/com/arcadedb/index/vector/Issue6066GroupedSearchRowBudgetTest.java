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
package com.arcadedb.index.vector;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.TypeLSMVectorIndexBuilder;
import com.arcadedb.utility.Pair;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6066: {@code findNeighborsFromVectorGrouped} derived its row budget with a plain {@code limit * groupSize}
 * int multiplication and then used it as an eager {@code ArrayList} capacity and as the floor on the beam width.
 * Neither factor is bounded anywhere on the way in - the method is public API and the SQL layer's own cap only
 * applies to the {@code vector.neighbors} entry point - so two individually reasonable values could produce a budget
 * that either wrapped through {@link Integer#MAX_VALUE} or was simply larger than any allocation can be:
 * <ul>
 *   <li>a product that overflows to a negative int failed the whole search with an
 *       {@code IllegalArgumentException: Illegal Capacity} out of {@code new ArrayList<>(maxRows)}, wrapped in an
 *       {@code IndexException} whose message says nothing about the real cause;</li>
 *   <li>a product that stays positive but is huge asked for a multi-GB backing array and died with an
 *       {@code OutOfMemoryError} before a single candidate was scored.</li>
 * </ul>
 * The budget is now computed in {@code long} and clamped to the number of candidates the index can actually address,
 * which is the same treatment {@code findNeighborsFromVector}'s {@code k} received for issue #5924. The clamp cannot
 * lose a row: no grouped search can return more rows than the index holds vectors, and a beam wider than the graph
 * has nothing extra to look at.
 */
class Issue6066GroupedSearchRowBudgetTest extends TestHelper {

  private static final int    DIMENSIONS  = 16;
  private static final int    CLUSTERS    = 6;
  private static final int    PER_CLUSTER = 10;
  private static final int    VERTICES    = CLUSTERS * PER_CLUSTER;
  private static final double CLUSTER_GAP = 0.12;
  private static final double JITTER      = 0.0001;

  @BeforeEach
  void freezeTheGraph() {
    GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD.setValue(1_000_000);
    GlobalConfiguration.VECTOR_INDEX_REBUILD_GRAPH_RATIO.setValue(0f);
    GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS.setValue(0);
  }

  @AfterEach
  void thawTheGraph() {
    GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD.reset();
    GlobalConfiguration.VECTOR_INDEX_REBUILD_GRAPH_RATIO.reset();
    GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS.reset();
  }

  /**
   * {@code 50000 * 50000} is 2.5e9, which two's-complement-wraps to a negative int. The search used to die on the
   * result buffer's capacity; it now has to answer, and answer with exactly what the same request answers when the
   * budget is expressed in a width that holds it.
   */
  @Test
  void aRowBudgetThatOverflowsIntStillAnswers() {
    createSchema();
    insertVertices();

    assertThat((long) 50_000 * 50_000).as("the fixture has to actually overflow an int to be a regression test")
        .isGreaterThan(Integer.MAX_VALUE);
    assertThat(50_000 * 50_000).as("and the unguarded product is the negative capacity that used to throw")
        .isNegative();

    final List<Pair<RID, Float>> neighbors = grouped(centerOf(1), 50_000, 50_000);

    assertThat(neighbors).as("a budget larger than the index is not a reason to fail the search").hasSize(VERTICES);
    assertRankOrdered(neighbors);
  }

  /**
   * The same defect without the wrap: {@code 200000000 * 10} is 2e9, a perfectly valid positive int that is
   * nonetheless an 8 GB {@code Object[]} the result buffer used to ask for before a single candidate was scored - an
   * {@code OutOfMemoryError} on any ordinary heap, and several seconds of zeroing on a heap large enough to serve it.
   * After the clamp the buffer is sized by the index, so the request costs nothing whatever the heap is.
   */
  @Test
  void aRowBudgetTooLargeToAllocateStillAnswers() {
    createSchema();
    insertVertices();

    assertThat(200_000_000 * 10).as("this product does not wrap - it is simply bigger than any allocation")
        .isPositive();

    final List<Pair<RID, Float>> neighbors = grouped(centerOf(1), 200_000_000, 10);

    assertThat(neighbors).as("a budget the heap cannot hold is still only as many rows as the index has")
        .hasSize(VERTICES);
    assertRankOrdered(neighbors);
  }

  /**
   * The silent half of the defect, and the one the issue calls out as the more dangerous: a product that wraps to a
   * small positive throws nothing at all, it just under-drives the beam. {@code Integer.MAX_VALUE} squared wraps to
   * exactly {@code 1}, and {@code effectiveEfSearch} is {@code max(maxRows, efSearch)} - so with the caller's own
   * {@code efSearch} at 1 the whole search used to run on a one-candidate beam and come back with a fraction of the
   * rows it was asked for, in silence. {@code Integer.MAX_VALUE} is also precisely what a saturating narrowing at a
   * query entry point produces, which is how a request this shape arrives in practice.
   */
  @Test
  void aRowBudgetThatWrapsToOneDoesNotStarveTheBeam() {
    createSchema();
    insertVertices();

    assertThat(Integer.MAX_VALUE * Integer.MAX_VALUE).as("the unguarded product of two saturated factors is 1")
        .isEqualTo(1);

    final List<Pair<RID, Float>> neighbors = grouped(centerOf(1), Integer.MAX_VALUE, Integer.MAX_VALUE, 1);

    assertThat(neighbors).as("the beam floor is the row budget, so a wrapped budget must not shrink the answer")
        .hasSize(VERTICES);
    assertRankOrdered(neighbors);
  }

  /**
   * The clamp must not touch a budget that fits: a request for fewer rows than the index holds still gets exactly the
   * group cap it asked for, so the fix is invisible to every realistic query.
   */
  @Test
  void aBudgetThatFitsIsUnchanged() {
    createSchema();
    insertVertices();

    final List<Pair<RID, Float>> neighbors = grouped(centerOf(1), 3, 2);

    assertThat(neighbors).as("three groups of two is six rows, and the clamp has no business shrinking it")
        .hasSize(6);
    assertThat(clustersOf(neighbors)).as("the group the query sits in cannot be absent from its own answer")
        .contains(1);
    assertRankOrdered(neighbors);
  }

  // ------------------------------------------------------------------------------------------------- helpers

  private void assertRankOrdered(final List<Pair<RID, Float>> neighbors) {
    final List<RID> rids = new ArrayList<>(neighbors.size());
    for (final Pair<RID, Float> neighbor : neighbors)
      rids.add(neighbor.getFirst());
    assertThat(rids).as("the same row must not be admitted twice").doesNotHaveDuplicates();

    for (int i = 1; i < neighbors.size(); i++)
      assertThat(neighbors.get(i).getSecond()).as("row %d is nearer than row %d, so the answer is not sorted", i, i - 1)
          .isGreaterThanOrEqualTo(neighbors.get(i - 1).getSecond());
  }

  private List<Pair<RID, Float>> grouped(final int vertex, final int limit, final int groupSize) {
    return grouped(vertex, limit, groupSize, -1);
  }

  private List<Pair<RID, Float>> grouped(final int vertex, final int limit, final int groupSize, final int efSearch) {
    // The resolver receives the raw (unbound) RID the location map holds, exactly as the SQL function's does.
    return vectorIndex().findNeighborsFromVectorGrouped(embedding(vertex), limit, groupSize, efSearch, null,
        rid -> ((Document) database.lookupByRID(rid, true)).getInteger("cluster"));
  }

  private List<Integer> clustersOf(final List<Pair<RID, Float>> neighbors) {
    final List<Integer> clusters = new ArrayList<>(neighbors.size());
    for (final Pair<RID, Float> neighbor : neighbors)
      clusters.add(neighbor.getFirst().asVertex().getInteger("cluster"));
    return clusters;
  }

  private LSMVectorIndex vectorIndex() {
    return (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName("Doc[embedding]")).getIndexesOnBuckets()[0];
  }

  private void createSchema() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.id STRING");
      database.command("sql", "CREATE PROPERTY Doc.cluster INTEGER");
      database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON Doc (id) UNIQUE");

      final TypeLSMVectorIndexBuilder builder = (TypeLSMVectorIndexBuilder) database.getSchema()
          .buildTypeIndex("Doc", new String[] { "embedding" }).withLSMVectorType();
      builder.withDimensions(DIMENSIONS).withQuantization(VectorQuantizationType.INT8).withSimilarity("COSINE")
          .create();
    });
  }

  private void insertVertices() {
    database.transaction(() -> {
      for (int i = 0; i < VERTICES; i++)
        database.command("sql", "INSERT INTO Doc SET id = ?, cluster = ?, embedding = ?", "doc" + i, i / PER_CLUSTER,
            embedding(i));
    });
    // Force the first graph build outside the assertions, so a search under test measures the search only.
    vectorIndex().findNeighborsFromVector(embedding(centerOf(0)), 1);
  }

  private static int centerOf(final int cluster) {
    return cluster * PER_CLUSTER + PER_CLUSTER / 2;
  }

  /**
   * Points on an arc, embedded with damped harmonics so cosine similarity decreases strictly with the angular gap -
   * the same construction as {@code Issue5761GroupedSearchGroupChoiceTest}, so which cluster is nearest is decidable
   * on paper rather than an artefact of quantization noise.
   */
  private float[] embedding(final int vertex) {
    final float[] v = new float[DIMENSIONS];
    final double theta = (vertex / PER_CLUSTER) * CLUSTER_GAP + (vertex % PER_CLUSTER - PER_CLUSTER / 2.0) * JITTER;
    for (int m = 1; m <= DIMENSIONS / 2; m++) {
      v[(m - 1) * 2] = (float) (Math.cos(m * theta) / m);
      v[(m - 1) * 2 + 1] = (float) (Math.sin(m * theta) / m);
    }
    return v;
  }
}
