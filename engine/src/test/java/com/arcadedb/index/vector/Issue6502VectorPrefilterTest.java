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
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.TypeLSMVectorIndexBuilder;
import com.arcadedb.utility.Pair;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Level;

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
 *   <li>{@link #shortfallIsAllowListDrivenClassifiesBothDirections} - direct coverage of the predicate that decides
 *       whether the issue #3722 shortfall log line is the expected FINE case or the WARNING one, since a logic
 *       inversion there would otherwise pass the rest of the suite silently.</li>
 *   <li>{@link #aNarrowAllowListWithThePlanDisabledLogsAtFineNotWarning} - the end-to-end path into that predicate:
 *       selectivity 0 forces a narrow allow-list past the pre-filter plan and into the shortfall log, which must
 *       come out at FINE, not the graph-degradation WARNING.</li>
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

  /**
   * Direct coverage of {@link LSMVectorIndex#shortfallIsAllowListDriven}, the predicate that decides whether the
   * issue #3722 shortfall log line comes out at FINE (the allow-list itself made the shortfall unavoidable) or
   * WARNING (the graph may genuinely need attention). Exercised without a real degraded-graph fixture: JVector's
   * graph builder is not guaranteed to produce the same structure run to run, so pinning a specific log level on a
   * "real shortfall" fixture would risk flaking. The predicate is pure and takes no graph state, so testing it
   * directly is both simpler and deterministic - and it is exactly what would catch a logic inversion silently
   * swapping the two branches.
   */
  @Test
  void shortfallIsAllowListDrivenClassifiesBothDirections() {
    final RID a = new RID(1, 0);
    final RID b = new RID(1, 1);
    final RID c = new RID(1, 2);

    assertThat(LSMVectorIndex.shortfallIsAllowListDriven(Set.of(a, b), 5))
        .as("an allow-list narrower than expectedResults is what the pre-filter plan could not fully answer")
        .isTrue();
    assertThat(LSMVectorIndex.shortfallIsAllowListDriven(null, 5))
        .as("no allow-list at all is the original issue #3722 degraded-graph case").isFalse();
    assertThat(LSMVectorIndex.shortfallIsAllowListDriven(Set.of(), 5))
        .as("an empty allow-list means \"no filter\", not \"filter to nothing\"").isFalse();
    assertThat(LSMVectorIndex.shortfallIsAllowListDriven(Set.of(a, b, c), 3))
        .as("an allow-list at least as wide as expectedResults did not force the shortfall by itself").isFalse();
    assertThat(LSMVectorIndex.shortfallIsAllowListDriven(Set.of(a, b, c), 2))
        .as("wider than expectedResults is the same case, comfortably so").isFalse();
  }

  /**
   * End-to-end path into the predicate above: disabling the pre-filter plan is the only way to make a narrow
   * allow-list reach the shortfall log at all (the plan would otherwise answer it directly first), so this pins
   * that the log level it lands on is FINE, and that the graph-degradation WARNING text does not also fire.
   */
  @Test
  void aNarrowAllowListWithThePlanDisabledLogsAtFineNotWarning() {
    createSchemaAndData();
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_PREFILTER_MAX_SELECTIVITY, 0f);

    final LSMVectorIndex index = vectorIndex();
    final float[] query = embedding(7);
    final Set<RID> allowed = ridsOf(3, 42, 91); // 3 of 100, narrower than K=5

    final List<String> captured = new CopyOnWriteArrayList<>();
    final Logger originalLogger = LogManager.instance().getLogger();
    LogManager.instance().setLogger(new LevelInclusiveCapturingLogger(captured, originalLogger));
    try {
      final List<Pair<RID, Float>> results = index.findNeighborsFromVector(query, K, -1, allowed);

      assertThat(results).hasSize(3);
      assertThat(captured)
          .as("a shortfall forced purely by allow-list width must log at FINE, saying so")
          .anyMatch(m -> m != null && m.contains("allow-list has only 3 entries"));
      assertThat(captured)
          .as("and must NOT also log the graph-degradation WARNING - only one of the two may fire")
          .noneMatch(m -> m != null && m.contains("graph may need rebuilding"));
    } finally {
      LogManager.instance().setLogger(originalLogger);
      database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_PREFILTER_MAX_SELECTIVITY,
          GlobalConfiguration.VECTOR_INDEX_PREFILTER_MAX_SELECTIVITY.getDefValue());
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

  /**
   * Captures every message regardless of level while still forwarding to the production logger, unlike
   * {@code LSMVectorIndexBruteForceScanTest}'s {@code CapturingLogger}, which drops anything below WARNING and so
   * cannot observe the FINE-level branch of the issue #6502 shortfall log.
   */
  private static final class LevelInclusiveCapturingLogger implements Logger {
    private final List<String> captured;
    private final Logger       delegate;

    LevelInclusiveCapturingLogger(final List<String> captured, final Logger delegate) {
      this.captured = captured;
      this.delegate = delegate;
    }

    private void capture(final String message, final Object... args) {
      if (message == null)
        return;
      String formatted = message;
      if (args != null && args.length > 0) {
        try {
          formatted = message.formatted(args);
        } catch (final Exception ignored) {
          // Fall back to the raw template; good enough for substring matching.
        }
      }
      captured.add(formatted);
    }

    @Override
    public void log(final Object requester, final Level level, final String message, final Throwable exception,
        final String context, final Object arg1, final Object arg2, final Object arg3, final Object arg4, final Object arg5,
        final Object arg6, final Object arg7, final Object arg8, final Object arg9, final Object arg10, final Object arg11,
        final Object arg12, final Object arg13, final Object arg14, final Object arg15, final Object arg16, final Object arg17) {
      capture(message, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15, arg16,
          arg17);
      delegate.log(requester, level, message, exception, context, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10,
          arg11, arg12, arg13, arg14, arg15, arg16, arg17);
    }

    @Override
    public void log(final Object requester, final Level level, final String message, final Throwable exception,
        final String context, final Object... args) {
      capture(message, args);
      delegate.log(requester, level, message, exception, context, args);
    }

    @Override
    public void flush() {
      delegate.flush();
    }
  }
}
