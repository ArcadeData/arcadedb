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

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6502: a RID allow-list was applied only as an admission test inside the HNSW traversal, plus a post-filter
 * on the output. There was no pre-filter plan, so a query got monotonically <em>slower</em> the more selective its
 * filter was - at the limit, asking for the 5 nearest among 5 candidates cost more than asking for the 10 nearest
 * among 20,000, because the Bits filter only rejects a node once the walk has already popped it from the beam and
 * cannot make the walk itself shrink.
 * <p>
 * The fix resolves a narrow allow-list to its ordinals and scores them directly - the same walk
 * {@code collectAllowedOrdinals}/{@code scoreOrdinal}/{@code bruteForceScan} already used as the issue #3722
 * shortfall fallback - as the primary plan, whenever the allow-list covers no more than
 * {@link GlobalConfiguration#VECTOR_INDEX_PREFILTER_MAX_SELECTIVITY} of the index's live vectors. The tests below
 * pin:
 * <ul>
 *   <li>{@link #aNarrowAllowListTakesThePreFilterPlanAndMatchesTheGraphAnswer} - the plan switches on below the
 *       threshold, and its answer is identical to what the graph walk (forced by disabling the plan) returns.</li>
 *   <li>{@link #aWideAllowListStaysOnTheGraphWalk} - above the threshold the graph walk is still used, since
 *       resolving a wide allow-list up front would cost more than the walk it would replace.</li>
 *   <li>{@link #disablingThePlanViaZeroSelectivityAlwaysUsesTheGraphWalk} - the escape hatch: selectivity 0 restores
 *       the pre-issue-#6502 behaviour exactly.</li>
 *   <li>{@link #thePreFilterPlanNeverTriggersTheShortfallWarningOrItsScan} - the plan answers directly, so it must
 *       not also trip the issue #3722 shortfall fallback it happens to reuse the scan of.</li>
 *   <li>{@link #thePreFilterPlanSeesVectorsStillInTheDeltaBuffer} - an allowed RID ingested after the last graph
 *       build is only in the delta buffer, and the plan has to merge it in like the graph path does.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6502VectorPrefilterTest extends TestHelper {

  private static final int DIMENSIONS = 8;
  private static final int VERTICES   = 100;
  private static final int K          = 5;

  @Test
  void aNarrowAllowListTakesThePreFilterPlanAndMatchesTheGraphAnswer() {
    createSchemaAndData();

    final LSMVectorIndex index = vectorIndex();
    final float[] query = embedding(7);
    // 3 of 100 is 3%, well under the 20% default threshold.
    final Set<RID> allowed = ridsOf(3, 42, 91);

    final long preFilterBefore = index.getStats().getOrDefault("preFilterSearches", 0L);
    final List<Pair<RID, Float>> preFiltered = index.findNeighborsFromVector(query, K, -1, allowed);
    assertThat(index.getStats().get("preFilterSearches")).as("the narrow allow-list must take the pre-filter plan")
        .isEqualTo(preFilterBefore + 1);

    // Force the graph walk for the same query by disabling the plan, and compare answers.
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_PREFILTER_MAX_SELECTIVITY, 0f);
    try {
      final List<Pair<RID, Float>> graphWalked = index.findNeighborsFromVector(query, K, -1, allowed);
      assertThat(ridsIn(preFiltered)).as("the pre-filter plan must answer exactly what the graph walk answers")
          .isEqualTo(ridsIn(graphWalked));
      assertThat(distancesIn(preFiltered)).isEqualTo(distancesIn(graphWalked));
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_PREFILTER_MAX_SELECTIVITY,
          GlobalConfiguration.VECTOR_INDEX_PREFILTER_MAX_SELECTIVITY.getDefValue());
    }
  }

  @Test
  void aWideAllowListStaysOnTheGraphWalk() {
    createSchemaAndData();

    final LSMVectorIndex index = vectorIndex();
    final float[] query = embedding(7);

    // 80 of 100 is 80%, well over the 20% default threshold.
    final Set<RID> allowed = new HashSet<>();
    for (int i = 0; i < 80; i++)
      allowed.add(ridOf("doc" + i));

    final long preFilterBefore = index.getStats().getOrDefault("preFilterSearches", 0L);
    final List<Pair<RID, Float>> results = index.findNeighborsFromVector(query, K, -1, allowed);
    assertThat(index.getStats().get("preFilterSearches"))
        .as("a wide allow-list must not take the pre-filter plan").isEqualTo(preFilterBefore);
    assertThat(results).as("the graph walk must still answer the query").hasSize(K);
    for (final Pair<RID, Float> r : results)
      assertThat(allowed).as("and every result must respect the allow-list").contains(r.getFirst());
  }

  @Test
  void disablingThePlanViaZeroSelectivityAlwaysUsesTheGraphWalk() {
    createSchemaAndData();
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_PREFILTER_MAX_SELECTIVITY, 0f);
    try {
      final LSMVectorIndex index = vectorIndex();
      final float[] query = embedding(7);
      final Set<RID> allowed = ridsOf(3, 42, 91);

      final long preFilterBefore = index.getStats().getOrDefault("preFilterSearches", 0L);
      final List<Pair<RID, Float>> results = index.findNeighborsFromVector(query, K, -1, allowed);
      assertThat(index.getStats().get("preFilterSearches"))
          .as("selectivity 0 must disable the pre-filter plan even for a very narrow allow-list")
          .isEqualTo(preFilterBefore);
      assertThat(results).hasSize(3);
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_PREFILTER_MAX_SELECTIVITY,
          GlobalConfiguration.VECTOR_INDEX_PREFILTER_MAX_SELECTIVITY.getDefValue());
    }
  }

  @Test
  void thePreFilterPlanNeverTriggersTheShortfallWarningOrItsScan() {
    createSchemaAndData();

    final LSMVectorIndex index = vectorIndex();
    final float[] query = embedding(7);
    final Set<RID> allowed = ridsOf(3, 42, 91);

    final Map<String, Long> before = index.getStats();
    final List<Pair<RID, Float>> results = index.findNeighborsFromVector(query, K, -1, allowed);
    final Map<String, Long> after = index.getStats();

    assertThat(results).as("the query is still answered").hasSize(3);
    assertThat(after.get("preFilterSearches")).isEqualTo(before.getOrDefault("preFilterSearches", 0L) + 1);
    assertThat(after.get("bruteForceScans"))
        .as("the pre-filter plan answers directly and must not also trip the issue #3722 shortfall fallback")
        .isEqualTo(before.getOrDefault("bruteForceScans", 0L));
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

    final List<Pair<RID, Float>> results = index.findNeighborsFromVector(query, K, -1, allowed);
    assertThat(ridsIn(results)).as("the pre-filter plan must merge in a delta-buffered allowed vector too")
        .contains(inDelta);
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
      builder.withDimensions(DIMENSIONS).withQuantization(VectorQuantizationType.NONE).withSimilarity("EUCLIDEAN")
          .create();
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
