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
import com.arcadedb.database.RID;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.TypeLSMVectorIndexBuilder;
import com.arcadedb.utility.Pair;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6514: extends the issue #6502 RID allow-list pre-filter plan to
 * {@code findNeighborsFromVectorApproximate}, the zero-disk-I/O PQ search path. When PQ is available this path runs
 * its own {@code Bits}-filtered HNSW walk scored via {@code pqVectors.precomputedScoreFunctionFor(...)} - same
 * pathology as #6502, no pre-filter equivalent before this fix.
 * <p>
 * The fix scores an allow-list narrower than {@link GlobalConfiguration#VECTOR_INDEX_PREFILTER_APPROXIMATE_MAX_SELECTIVITY}
 * directly against the PQ score function (via {@code LSMVectorIndex#preFilterApproximate}), never touching a raw
 * vector, so it stays zero-disk-I/O like the graph-walk path it replaces. Since the graph walk's own final score for
 * a node is that same PQ function (the reranker forwards to it - see {@code findNeighborsFromVectorApproximate}),
 * the two plans must agree exactly on both ranking and score for any candidate they can both reach.
 * <ul>
 *   <li>{@link #aNarrowAllowListTakesThePreFilterPlanAndMatchesTheGraphAnswer} - the plan switches on below the
 *       threshold and answers exactly what the graph walk (forced by disabling the plan) answers.</li>
 *   <li>{@link #aWideAllowListStaysOnTheGraphWalk} - above the threshold the graph walk is still used.</li>
 *   <li>{@link #disablingThePlanViaZeroSelectivityAlwaysUsesTheGraphWalk} - the escape hatch still works.</li>
 *   <li>{@link #thePreFilterPlanSeesVectorsStillInTheDeltaBuffer} - a delta-buffered allowed vector is merged in.</li>
 *   <li>{@link #theApproximateThresholdIsIndependentOfTheSharedOne} - this path's threshold does not alias the
 *       plain/groupBy paths' setting in either direction.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6514ApproximateVectorPrefilterTest extends TestHelper {

  private static final int DIMENSIONS = 8;
  private static final int VERTICES   = 200;
  private static final int K          = 5;

  @AfterEach
  void resetConfig() {
    GlobalConfiguration.VECTOR_INDEX_PREFILTER_APPROXIMATE_MAX_SELECTIVITY.reset();
  }

  @Test
  void aNarrowAllowListTakesThePreFilterPlanAndMatchesTheGraphAnswer() {
    createSchemaAndData();

    final LSMVectorIndex index = vectorIndex();
    assertThat(index.isPQSearchAvailable()).as("the fixture must actually exercise the PQ path").isTrue();

    final float[] query = embedding(7);
    // 6 of 200 is 3%, well under the approximate path's 5% default threshold.
    final Set<RID> allowed = ridsOf(3, 42, 91, 150, 175, 199);

    final long preFilterBefore = index.getStats().getOrDefault("preFilterSearches", 0L);
    final List<Pair<RID, Float>> preFiltered = index.findNeighborsFromVectorApproximate(query, K, allowed);
    assertThat(index.getStats().get("preFilterSearches")).as("the narrow allow-list must take the pre-filter plan")
        .isEqualTo(preFilterBefore + 1);
    assertThat(preFiltered).hasSize(K);

    GlobalConfiguration.VECTOR_INDEX_PREFILTER_APPROXIMATE_MAX_SELECTIVITY.setValue(0f);
    try {
      final List<Pair<RID, Float>> graphWalked = index.findNeighborsFromVectorApproximate(query, K, allowed);
      assertThat(ridsIn(preFiltered)).as("the pre-filter plan must answer exactly what the graph walk answers")
          .isEqualTo(ridsIn(graphWalked));
      assertThat(distancesIn(preFiltered)).isEqualTo(distancesIn(graphWalked));
    } finally {
      GlobalConfiguration.VECTOR_INDEX_PREFILTER_APPROXIMATE_MAX_SELECTIVITY.reset();
    }
  }

  @Test
  void aWideAllowListStaysOnTheGraphWalk() {
    createSchemaAndData();

    final LSMVectorIndex index = vectorIndex();
    final float[] query = embedding(7);

    // 160 of 200 is 80%, well over the approximate path's 5% default threshold.
    final Set<RID> allowed = new HashSet<>();
    for (int i = 0; i < 160; i++)
      allowed.add(ridOf("doc" + i));

    final long preFilterBefore = index.getStats().getOrDefault("preFilterSearches", 0L);
    final List<Pair<RID, Float>> results = index.findNeighborsFromVectorApproximate(query, K, allowed);
    assertThat(index.getStats().get("preFilterSearches"))
        .as("a wide allow-list must not take the pre-filter plan").isEqualTo(preFilterBefore);
    assertThat(results).as("the graph walk must still answer the query").hasSize(K);
    for (final Pair<RID, Float> r : results)
      assertThat(allowed).as("and every result must respect the allow-list").contains(r.getFirst());
  }

  @Test
  void disablingThePlanViaZeroSelectivityAlwaysUsesTheGraphWalk() {
    createSchemaAndData();
    GlobalConfiguration.VECTOR_INDEX_PREFILTER_APPROXIMATE_MAX_SELECTIVITY.setValue(0f);
    try {
      final LSMVectorIndex index = vectorIndex();
      final float[] query = embedding(7);
      final Set<RID> allowed = ridsOf(3, 42, 91);

      final long preFilterBefore = index.getStats().getOrDefault("preFilterSearches", 0L);
      final List<Pair<RID, Float>> results = index.findNeighborsFromVectorApproximate(query, K, allowed);
      assertThat(index.getStats().get("preFilterSearches"))
          .as("selectivity 0 must disable the pre-filter plan even for a very narrow allow-list")
          .isEqualTo(preFilterBefore);
      assertThat(results).hasSize(3);
    } finally {
      GlobalConfiguration.VECTOR_INDEX_PREFILTER_APPROXIMATE_MAX_SELECTIVITY.reset();
    }
  }

  @Test
  void thePreFilterPlanSeesVectorsStillInTheDeltaBuffer() {
    createSchemaAndData();

    database.transaction(() -> database.command("sql", "INSERT INTO Doc SET id = ?, embedding = ?", "afterTheBuild",
        embedding(500)));
    final RID inDelta = ridOf("afterTheBuild");

    final LSMVectorIndex index = vectorIndex();
    final float[] query = embedding(500);
    final Set<RID> allowed = new HashSet<>();
    allowed.add(inDelta);
    allowed.add(ridOf("doc3"));

    final List<Pair<RID, Float>> results = index.findNeighborsFromVectorApproximate(query, K, allowed);
    assertThat(ridsIn(results)).as("the pre-filter plan must merge in a delta-buffered allowed vector too")
        .contains(inDelta);
  }

  /**
   * The approximate path's threshold (measured at ~6-7% selectivity by
   * {@link Issue6514ApproximatePrefilterBenchmark}, defaulted conservatively to 5%) is a separate setting from
   * {@link GlobalConfiguration#VECTOR_INDEX_PREFILTER_MAX_SELECTIVITY}, not an alias for it: disabling the shared
   * plain/groupBy setting must not disable this path's plan, and vice versa.
   */
  @Test
  void theApproximateThresholdIsIndependentOfTheSharedOne() {
    createSchemaAndData();
    final LSMVectorIndex index = vectorIndex();
    final float[] query = embedding(7);
    final Set<RID> allowed = ridsOf(3, 42, 91); // 1.5% of 200, under both thresholds

    GlobalConfiguration.VECTOR_INDEX_PREFILTER_MAX_SELECTIVITY.setValue(0f);
    try {
      final long preFilterBefore = index.getStats().getOrDefault("preFilterSearches", 0L);
      index.findNeighborsFromVectorApproximate(query, K, allowed);
      assertThat(index.getStats().get("preFilterSearches"))
          .as("disabling the shared plain/groupBy setting must not disable the approximate path's own plan")
          .isEqualTo(preFilterBefore + 1);
    } finally {
      GlobalConfiguration.VECTOR_INDEX_PREFILTER_MAX_SELECTIVITY.reset();
    }

    GlobalConfiguration.VECTOR_INDEX_PREFILTER_APPROXIMATE_MAX_SELECTIVITY.setValue(0f);
    try {
      final long preFilterBefore = index.getStats().getOrDefault("preFilterSearches", 0L);
      index.findNeighborsFromVectorApproximate(query, K, allowed);
      assertThat(index.getStats().get("preFilterSearches"))
          .as("disabling the approximate path's own setting must actually disable its plan")
          .isEqualTo(preFilterBefore);
    } finally {
      GlobalConfiguration.VECTOR_INDEX_PREFILTER_APPROXIMATE_MAX_SELECTIVITY.reset();
    }
  }

  // ------------------------------------------------------------------------------------------------- helpers

  private void createSchemaAndData() {
    // No rebuild may run mid-test, or ordinal/live counts used by the selectivity check would move under the test.
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 1_000_000);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, -1);

    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.id STRING");
      database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON Doc (id) UNIQUE");

      final TypeLSMVectorIndexBuilder builder = (TypeLSMVectorIndexBuilder) database.getSchema()
          .buildTypeIndex("Doc", new String[] { "embedding" }).withLSMVectorType();
      // A low cluster count so PQ can actually train against a VERTICES-sized fixture (default is 256 clusters).
      builder.withDimensions(DIMENSIONS).withQuantization(VectorQuantizationType.PRODUCT).withPQClusters(16)
          .withSimilarity("EUCLIDEAN").create();
    });

    database.transaction(() -> {
      for (int i = 0; i < VERTICES; i++)
        database.command("sql", "INSERT INTO Doc SET id = ?, embedding = ?", "doc" + i, embedding(i));
    });

    vectorIndex().buildVectorGraphNow();
  }

  private Set<RID> ridsOf(final int... ids) {
    final Set<RID> rids = new HashSet<>();
    for (final int id : ids)
      rids.add(ridOf("doc" + id));
    return rids;
  }

  private RID ridOf(final String id) {
    try (final ResultSet rs = database.query("sql", "SELECT FROM Doc WHERE id = ?", id)) {
      return rs.next().getIdentity().orElseThrow();
    }
  }

  private static List<RID> ridsIn(final List<Pair<RID, Float>> results) {
    final List<RID> rids = new ArrayList<>(results.size());
    for (final Pair<RID, Float> r : results)
      rids.add(r.getFirst());
    return rids;
  }

  private static List<Float> distancesIn(final List<Pair<RID, Float>> results) {
    final List<Float> distances = new ArrayList<>(results.size());
    for (final Pair<RID, Float> r : results)
      distances.add(r.getSecond());
    return distances;
  }

  private LSMVectorIndex vectorIndex() {
    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Doc[embedding]");
    return (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];
  }

  /** Deterministic, distinct, non-zero vectors, well spread so no two of them tie on distance. */
  private static float[] embedding(final int i) {
    final float[] vector = new float[DIMENSIONS];
    for (int j = 0; j < DIMENSIONS; j++)
      vector[j] = (float) Math.sin(i * 0.37 + j * 0.91) + 2.0f + i * 0.013f;
    return vector;
  }
}
