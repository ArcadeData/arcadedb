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
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.TypeLSMVectorIndexBuilder;
import com.arcadedb.utility.Pair;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6514: extends the issue #6502 RID allow-list pre-filter plan to {@code findNeighborsFromVectorGrouped}.
 * The graph-walk path there has the same pathology as the plain k-NN path fixed by #6502 - a {@code Bits} filter
 * that only rejects a node once it is popped from the beam, so a selective allow-list makes the beam admit almost
 * nothing and the walk keeps expanding - compounded by the {@code resume()} loop potentially running multiple
 * passes to fill groups that barely exist under the filter.
 * <p>
 * The fix scores an allow-list narrower than {@link GlobalConfiguration#VECTOR_INDEX_PREFILTER_MAX_SELECTIVITY}
 * directly (via {@code LSMVectorIndex#preFilterGrouped}) instead of walking the graph, applying the same
 * {@code GroupAdmissionState} cap the graph-walk path applies to its own output. The tests below pin:
 * <ul>
 *   <li>{@link #aNarrowAllowListTakesThePreFilterPlanAndMatchesTheGraphAnswer} - the plan switches on below the
 *       threshold and answers exactly what the graph walk (forced by disabling the plan) answers.</li>
 *   <li>{@link #aWideAllowListStaysOnTheGraphWalk} - above the threshold the graph walk is still used.</li>
 *   <li>{@link #thePreFilterPlanNeverExceedsTheGroupCap} - every group in the pre-filtered answer respects
 *       {@code groupSize}, and the number of distinct groups never exceeds {@code limit}.</li>
 *   <li>{@link #thePreFilterPlanReportsShortfallWhenTheAllowListCannotFillAllGroups} - an allow-list too narrow to
 *       open {@code limit} distinct groups is still answered correctly, and the shortfall counter reflects it.</li>
 *   <li>{@link #disablingThePlanViaZeroSelectivityAlwaysUsesTheGraphWalk} - the escape hatch still works for the
 *       grouped path.</li>
 *   <li>{@link #thePreFilterPlanDoesNotOverflowOnAHugeLimitTimesGroupSize} - {@code limit * groupSize} cannot
 *       reintroduce the issue #6066 overflow inside the pre-filter plan's own row budget.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6514GroupedVectorPrefilterTest extends TestHelper {

  private static final int    DIMENSIONS  = 16;
  private static final int    CLUSTERS    = 10;
  private static final int    PER_CLUSTER = 10;
  private static final int    VERTICES    = CLUSTERS * PER_CLUSTER;
  private static final double CLUSTER_GAP = 0.12;
  private static final double JITTER      = 0.0001;

  @BeforeEach
  void freezeTheGraph() {
    // No rebuild may run mid-test, or the ordinal/live counts the selectivity check reads would move under the test.
    GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD.setValue(1_000_000);
    GlobalConfiguration.VECTOR_INDEX_REBUILD_GRAPH_RATIO.setValue(0f);
    GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS.setValue(0);
  }

  @AfterEach
  void thawTheGraph() {
    GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD.reset();
    GlobalConfiguration.VECTOR_INDEX_REBUILD_GRAPH_RATIO.reset();
    GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS.reset();
    GlobalConfiguration.VECTOR_INDEX_PREFILTER_MAX_SELECTIVITY.reset();
  }

  @Test
  void aNarrowAllowListTakesThePreFilterPlanAndMatchesTheGraphAnswer() {
    createSchemaAndData();

    final LSMVectorIndex index = vectorIndex();
    final float[] query = embedding(centerOf(3));
    // 8 of 100 is 8%, well under the 20% default threshold, spread across 4 different clusters so the group cap
    // actually has something to do.
    final Set<RID> allowed = ridsOf(30, 31, 45, 46, 60, 61, 75, 76);

    final long preFilterBefore = index.getStats().getOrDefault("preFilterSearches", 0L);
    final List<Pair<RID, Float>> preFiltered = grouped(query, 4, 2, allowed);
    assertThat(index.getStats().get("preFilterSearches")).as("the narrow allow-list must take the pre-filter plan")
        .isEqualTo(preFilterBefore + 1);

    GlobalConfiguration.VECTOR_INDEX_PREFILTER_MAX_SELECTIVITY.setValue(0f);
    try {
      final List<Pair<RID, Float>> graphWalked = grouped(query, 4, 2, allowed);
      assertThat(ridsIn(preFiltered)).as("the pre-filter plan must answer exactly what the graph walk answers")
          .isEqualTo(ridsIn(graphWalked));
      assertThat(distancesIn(preFiltered)).isEqualTo(distancesIn(graphWalked));
    } finally {
      GlobalConfiguration.VECTOR_INDEX_PREFILTER_MAX_SELECTIVITY.reset();
    }
  }

  @Test
  void aWideAllowListStaysOnTheGraphWalk() {
    createSchemaAndData();

    final LSMVectorIndex index = vectorIndex();
    final float[] query = embedding(centerOf(3));

    // 90 of 100 is 90%, well over the 20% default threshold.
    final Set<RID> allowed = new HashSet<>();
    for (int i = 0; i < 90; i++)
      allowed.add(ridOf("doc" + i));

    final long preFilterBefore = index.getStats().getOrDefault("preFilterSearches", 0L);
    final List<Pair<RID, Float>> results = grouped(query, 4, 2, allowed);
    assertThat(index.getStats().get("preFilterSearches"))
        .as("a wide allow-list must not take the pre-filter plan").isEqualTo(preFilterBefore);
    assertThat(results).as("the graph walk must still answer the query").isNotEmpty();
    for (final Pair<RID, Float> r : results)
      assertThat(allowed).as("and every result must respect the allow-list").contains(r.getFirst());
  }

  @Test
  void thePreFilterPlanNeverExceedsTheGroupCap() {
    createSchemaAndData();

    final float[] query = embedding(centerOf(0));
    // 2 members from each of 3 different clusters (6 of 100 = 6%, well under the 20% threshold), so the group cap
    // has multiple candidate groups to choose from and each is exactly full under groupSize=2.
    final Set<RID> allowed = new HashSet<>();
    for (int cluster = 0; cluster < 3; cluster++)
      for (int i = 0; i < 2; i++)
        allowed.add(ridOf("doc" + (cluster * PER_CLUSTER + i)));
    assertThat(allowed.size()).as("must stay under the 20% threshold").isLessThan((int) (VERTICES * 0.2));

    final int limit = 3;
    final int groupSize = 2;
    final List<Pair<RID, Float>> results = grouped(query, limit, groupSize, allowed);

    final Map<Integer, Integer> perGroup = new java.util.HashMap<>();
    for (final Pair<RID, Float> r : results) {
      final int cluster = r.getFirst().asDocument().getInteger("cluster");
      perGroup.merge(cluster, 1, Integer::sum);
    }
    assertThat(perGroup.keySet().size()).as("distinct groups must not exceed limit").isLessThanOrEqualTo(limit);
    for (final int count : perGroup.values())
      assertThat(count).as("each group must not exceed groupSize").isLessThanOrEqualTo(groupSize);
  }

  @Test
  void thePreFilterPlanReportsShortfallWhenTheAllowListCannotFillAllGroups() {
    createSchemaAndData();

    final LSMVectorIndex index = vectorIndex();
    final float[] query = embedding(centerOf(0));
    // Only cluster 0's members are allowed: at most 1 distinct group can ever be opened, however high the limit.
    final Set<RID> allowed = new HashSet<>();
    for (int i = 0; i < PER_CLUSTER; i++)
      allowed.add(ridOf("doc" + i));

    // Issue #6559 item 1: the pre-filter plan has no efSearch/candidate-budget concept at all - every allow-listed
    // candidate is scored up front - so this shortfall is never the "raise efSearch" case
    // groupedSearchesShortOfLimit means on the graph-walk plan. It must count under groupedSearchesGroupsUnavailable
    // instead.
    final long unavailableBefore = index.getStats().getOrDefault("groupedSearchesGroupsUnavailable", 0L);
    final long shortfallBefore = index.getStats().getOrDefault("groupedSearchesShortOfLimit", 0L);
    final List<Pair<RID, Float>> results = grouped(query, 5, 3, allowed);

    assertThat(index.getStats().get("groupedSearchesGroupsUnavailable"))
        .as("an allow-list confined to one cluster cannot open 5 groups").isEqualTo(unavailableBefore + 1);
    assertThat(index.getStats().get("groupedSearchesShortOfLimit"))
        .as("the pre-filter plan cannot hit a candidate-budget shortfall, so this counter must stay untouched")
        .isEqualTo(shortfallBefore);
    for (final Pair<RID, Float> r : results)
      assertThat(r.getFirst().asDocument().getInteger("cluster")).isEqualTo(0);
    assertThat(results.size()).as("groupSize still caps the one group that could be opened").isLessThanOrEqualTo(3);
  }

  /**
   * Regression guard for the issue #6066 overflow re-appearing inside the pre-filter plan: {@code limit * groupSize}
   * computed as a plain {@code int} inside {@code preFilterGrouped} would wrap to a negative capacity for the same
   * reason it did in the graph-walk path before #6066, this time feeding {@code new ArrayList<>(...)}. The fix reuses
   * the caller's already {@code long}-clamped {@code maxRows} instead of recomputing the product.
   */
  @Test
  void thePreFilterPlanDoesNotOverflowOnAHugeLimitTimesGroupSize() {
    createSchemaAndData();

    assertThat(50_000 * 50_000).as("the fixture has to actually overflow an int to be a regression test").isNegative();

    final float[] query = embedding(centerOf(3));
    final Set<RID> allowed = ridsOf(30, 31, 45, 46); // 4 of 100, well under the 20% threshold

    final List<Pair<RID, Float>> results = grouped(query, 50_000, 50_000, allowed);
    assertThat(results).as("a budget larger than the allow-list is not a reason to fail the search")
        .hasSize(allowed.size());
  }

  @Test
  void disablingThePlanViaZeroSelectivityAlwaysUsesTheGraphWalk() {
    createSchemaAndData();
    GlobalConfiguration.VECTOR_INDEX_PREFILTER_MAX_SELECTIVITY.setValue(0f);
    try {
      final LSMVectorIndex index = vectorIndex();
      final float[] query = embedding(centerOf(3));
      final Set<RID> allowed = ridsOf(30, 31, 45, 46);

      final long preFilterBefore = index.getStats().getOrDefault("preFilterSearches", 0L);
      final List<Pair<RID, Float>> results = grouped(query, 4, 2, allowed);
      assertThat(index.getStats().get("preFilterSearches"))
          .as("selectivity 0 must disable the pre-filter plan even for a very narrow allow-list")
          .isEqualTo(preFilterBefore);
      assertThat(results).isNotEmpty();
    } finally {
      GlobalConfiguration.VECTOR_INDEX_PREFILTER_MAX_SELECTIVITY.reset();
    }
  }

  // ------------------------------------------------------------------------------------------------- helpers

  private List<Pair<RID, Float>> grouped(final float[] query, final int limit, final int groupSize, final Set<RID> allowed) {
    final Function<RID, Object> groupKeyResolver = rid -> ((Document) database.lookupByRID(rid, true)).getInteger("cluster");
    return vectorIndex().findNeighborsFromVectorGrouped(query, limit, groupSize, -1, allowed, groupKeyResolver);
  }

  private void createSchemaAndData() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.id STRING");
      database.command("sql", "CREATE PROPERTY Doc.cluster INTEGER");
      database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON Doc (id) UNIQUE");

      final TypeLSMVectorIndexBuilder builder = (TypeLSMVectorIndexBuilder) database.getSchema()
          .buildTypeIndex("Doc", new String[] { "embedding" }).withLSMVectorType();
      builder.withDimensions(DIMENSIONS).withQuantization(VectorQuantizationType.NONE).withSimilarity("COSINE")
          .create();
    });

    database.transaction(() -> {
      for (int i = 0; i < VERTICES; i++)
        database.command("sql", "INSERT INTO Doc SET id = ?, cluster = ?, embedding = ?", "doc" + i, i / PER_CLUSTER,
            embedding(i));
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

  private static int centerOf(final int cluster) {
    return cluster * PER_CLUSTER + PER_CLUSTER / 2;
  }

  /**
   * Points on an arc, embedded with damped harmonics so cosine similarity decreases strictly with the angular gap -
   * the same construction as {@code Issue5761GroupedSearchGroupChoiceTest} / {@code Issue6066GroupedSearchRowBudgetTest},
   * so which cluster is nearest a query is decidable on paper rather than an artefact of quantization noise.
   */
  private static float[] embedding(final int vertex) {
    final float[] v = new float[DIMENSIONS];
    final double theta = (vertex / PER_CLUSTER) * CLUSTER_GAP + (vertex % PER_CLUSTER - PER_CLUSTER / 2.0) * JITTER;
    for (int m = 1; m <= DIMENSIONS / 2; m++) {
      v[(m - 1) * 2] = (float) (Math.cos(m * theta) / m);
      v[(m - 1) * 2 + 1] = (float) (Math.sin(m * theta) / m);
    }
    return v;
  }
}
