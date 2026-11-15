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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.function.sql.vector.RecordPredicateRidSet;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.TypeLSMVectorIndexBuilder;
import com.arcadedb.utility.Pair;
import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Search with a metadata prefilter (allow list, selective walk, approximate and grouped). Each nested class keeps the fixture of the regression test it was folded from.
 */
class VectorPrefilterTest {
  /**
   * Regression for #6502: vector prefilter regression.
   * <p>
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
  @Nested
  class VectorPrefilterRegression extends TestHelper {

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
     * The union-and-truncate case flagged in code review: {@code mergeWithDeltaScan} runs first against an empty
     * {@code results}, then {@code bruteForceScan} adds the persisted candidates and re-sorts the combined list before
     * trimming to {@code k}. With an allow-list wider than {@code k}, split across both the delta buffer and the
     * persisted graph, the closest {@code k} overall - not the closest {@code k} of either source considered alone -
     * has to survive the truncation, regardless of which side of that split they came from.
     */
    @Test
    void thePreFilterPlanTruncatesToKAcrossBothDeltaAndPersistedCandidates() {
      createSchemaAndData();

      // Five more documents landing in the delta buffer (inserted after the graph build), interleaved with five
      // already-persisted ones so the allow-list straddles both sources.
      database.transaction(() -> {
        for (int i = 0; i < 5; i++)
          database.command("sql", "INSERT INTO Doc SET id = ?, embedding = ?", "delta" + i, embedding(200 + i));
      });

      final LSMVectorIndex index = vectorIndex();
      final float[] query = embedding(202); // near the middle of the delta cluster

      final Set<RID> allowed = new HashSet<>();
      for (int i = 0; i < 5; i++)
        allowed.add(ridOf("delta" + i));
      for (final int i : new int[] { 10, 25, 40, 60, 80 }) // persisted, spread across the index
        allowed.add(ridOf("doc" + i));
      assertThat(allowed).hasSize(10);

      final int k = 3;
      final List<Pair<RID, Float>> preFiltered = index.findNeighborsFromVector(query, k, -1, allowed);
      assertThat(preFiltered).hasSize(k);

      // Reference: an unfiltered search wide enough to rank the whole live set (persisted + delta), then keep only
      // the allowed RIDs and truncate by hand - the same reference-by-construction approach used elsewhere in this
      // suite, extended to a query the delta buffer also has to answer through.
      final List<Pair<RID, Float>> everything = index.findNeighborsFromVector(query, VERTICES + 10, -1, null);
      final List<Pair<RID, Float>> reference = new ArrayList<>();
      for (final Pair<RID, Float> candidate : everything)
        if (allowed.contains(candidate.getFirst()) && reference.size() < k)
          reference.add(candidate);

      assertThat(reference).as("the fixture has to leave something for the comparison to be about").hasSize(k);
      assertThat(ridsIn(preFiltered))
          .as("the closest k overall must survive truncation, not the closest k of either source alone")
          .isEqualTo(ridsIn(reference));
      assertThat(distancesIn(preFiltered)).isEqualTo(distancesIn(reference));
    }

    /**
     * An operator-supplied selectivity above 1.0 is nonsensical (the setting is documented as a fraction of the
     * index) but not rejected outright; it must not be taken literally either, or a value like {@code 5.0} would
     * route every allow-list query - however wide - through the ordinal-resolution walk instead of the graph, which
     * is the same pathology this plan exists to fix on the other side of the crossover.
     */
    @Test
    void aSelectivityAboveOneIsClampedNotTakenLiterally() {
      createSchemaAndData();
      database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_PREFILTER_MAX_SELECTIVITY, 5.0f);
      try {
        final LSMVectorIndex index = vectorIndex();
        final float[] query = embedding(7);

        // Every real RID (100% of the index) plus enough RIDs from a bucket this index knows nothing about to push
        // the allow-list past availableVectors (100) but comfortably under an unclamped 5.0x threshold (500). Only a
        // literal (unclamped) selectivity would route this to the pre-filter plan.
        final Set<RID> allWithExtra = new HashSet<>();
        for (int i = 0; i < VERTICES; i++)
          allWithExtra.add(ridOf("doc" + i));
        for (int i = 0; i < 50; i++)
          allWithExtra.add(new RID(9999, i));
        assertThat(allWithExtra.size()).as("must exceed availableVectors to distinguish clamped from unclamped")
            .isGreaterThan(VERTICES);

        final long preFilterBefore = index.getStats().getOrDefault("preFilterSearches", 0L);
        final List<Pair<RID, Float>> results = index.findNeighborsFromVector(query, K, -1, allWithExtra);
        assertThat(index.getStats().get("preFilterSearches"))
            .as("a selectivity above 1.0 must be clamped to 1.0, not taken as \"prefer the pre-filter plan always\"")
            .isEqualTo(preFilterBefore);
        assertThat(results).hasSize(K);
      } finally {
        database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_PREFILTER_MAX_SELECTIVITY,
            GlobalConfiguration.VECTOR_INDEX_PREFILTER_MAX_SELECTIVITY.getDefValue());
      }
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

  /**
   * Regression for #6514: approximate vector prefilter.
   * <p>
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
  @Nested
  class ApproximateVectorPrefilter extends TestHelper {

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
     * {@link VectorPrefilterBenchmark.ApproximatePrefilter}, defaulted conservatively to 5%) is a separate setting from
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

  /**
   * Regression for #6514: grouped vector prefilter.
   * <p>
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
  @Nested
  class GroupedVectorPrefilter extends TestHelper {

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
     * the same construction as {@code GroupedVectorSearchTest.GroupedSearchGroupChoice} / {@code GroupedVectorSearchTest.GroupedSearchRowBudget},
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

  /**
   * Regression for #10782: predicate shortfall.
   * <p>
   * Issue #10782: a predicate allow-list reports an unbounded size, since it decides its members one candidate at a time. By
   * size alone the shortfall fallback read every selective predicate as a graph that needs rebuilding, and logged a WARNING
   * saying so on each such search of {@code vector.neighbors} with a {@code where} option (#10169), and now of the Cypher
   * {@code SEARCH} clause's {@code WHERE}.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class PredicateShortfall extends TestHelper {

    @Test
    void aPredicateShortfallIsThePredicatesNotTheGraphs() {
      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(database);
      final Set<RID> predicate = RecordPredicateRidSet.matching(record -> true, context);

      assertThat(predicate).isInstanceOf(PredicateRidSet.class);
      assertThat(predicate.size()).as("the reason size cannot decide it").isEqualTo(Integer.MAX_VALUE);
      assertThat(LSMVectorIndex.shortfallIsAllowListDriven(predicate, 5)).isTrue();
    }
  }

  /**
   * Regression for #10921: selective walk.
   * <p>
   * Issue #10921: the walk restricted by a predicate an index can list is given up for the listing once it proves the predicate
   * selective, and the plan then scores what the index lists. These tests drive the index with a predicate of their own, so
   * they can tell the two outcomes apart: the listing taken, and the listing refused (more candidates than the walk's cost
   * allows), where the walk has to answer after all.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class SelectiveWalk extends TestHelper {
    private static final int DIMENSIONS = 8;
    private static final int VERTICES   = 1_000;
    private static final int K          = 3;

    @Test
    void aSelectivePredicateGivesTheWalkUpForTheListing() {
      createSchemaAndData();
      final LSMVectorIndex index = vectorIndex();
      final Set<RID> members = ridsOf(3, 420, 917);

      final ListedPredicate predicate = new ListedPredicate(members, true);
      final long plansBefore = index.getStats().getOrDefault("preFilterSearches", 0L);
      final List<Pair<RID, Float>> results = index.findNeighborsFromVector(embedding(7), K, -1, predicate);

      assertThat(predicate.listingCaps).as("the walk was given up for the listing").hasSize(1);
      assertThat(predicate.listingCaps.getFirst()).isGreaterThanOrEqualTo(members.size());
      assertThat(index.getStats().get("preFilterSearches")).isEqualTo(plansBefore + 1);
      assertThat(predicate.tested).as("a few dozen candidates walked, not the graph").isLessThan(VERTICES / 4);
      assertThat(ridsIn(results)).containsExactlyElementsOf(ridsIn(index.findNeighborsFromVector(embedding(7), K, -1, members)));
    }

    @Test
    void aRefusedListingLeavesTheAnswerToTheWalk() {
      createSchemaAndData();
      final LSMVectorIndex index = vectorIndex();
      final Set<RID> members = ridsOf(3, 420, 917);

      // The index cannot list the candidates under the cap the walk's cost allows: the walk answers, unbounded
      final ListedPredicate predicate = new ListedPredicate(members, false);
      final long plansBefore = index.getStats().getOrDefault("preFilterSearches", 0L);
      final List<Pair<RID, Float>> results = index.findNeighborsFromVector(embedding(7), K, -1, predicate);

      assertThat(predicate.listingCaps).hasSize(1);
      assertThat(index.getStats().get("preFilterSearches")).isEqualTo(plansBefore);
      assertThat(ridsIn(results)).containsExactlyInAnyOrderElementsOf(members);
    }

    @Test
    void aBroadPredicateIsWalkedWithoutAskingForTheListing() {
      createSchemaAndData();
      final LSMVectorIndex index = vectorIndex();
      final Set<RID> members = new HashSet<>();
      for (int i = 0; i < VERTICES; i += 2)
        members.add(ridOf("doc" + i));

      final ListedPredicate predicate = new ListedPredicate(members, true);
      final List<Pair<RID, Float>> results = index.findNeighborsFromVector(embedding(7), K, -1, predicate);

      assertThat(predicate.listingCaps).as("half the records: the walk is the plan").isEmpty();
      assertThat(results).hasSize(K);
      for (final Pair<RID, Float> r : results)
        assertThat(members).contains(r.getFirst());
    }

    @Test
    void aDisabledPreFilterNeverGivesTheWalkUp() {
      createSchemaAndData();
      database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_PREFILTER_MAX_SELECTIVITY, 0f);
      try {
        final LSMVectorIndex index = vectorIndex();
        final Set<RID> members = ridsOf(3, 420, 917);
        final ListedPredicate predicate = new ListedPredicate(members, true);
        final List<Pair<RID, Float>> results = index.findNeighborsFromVector(embedding(7), K, -1, predicate);

        assertThat(predicate.listingCaps).isEmpty();
        assertThat(ridsIn(results)).containsExactlyInAnyOrderElementsOf(members);
      } finally {
        database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_PREFILTER_MAX_SELECTIVITY,
            GlobalConfiguration.VECTOR_INDEX_PREFILTER_MAX_SELECTIVITY.getDefValue());
      }
    }

    /**
     * A predicate whose members an index can list: {@code lists} false makes every listing come back over the cap.
     */
    private static final class ListedPredicate extends AbstractSet<RID> implements PredicateRidSet {
      private final Set<RID>      members;
      private final boolean       lists;
      private final List<Integer> listingCaps = new ArrayList<>();
      private       int           tested;

      ListedPredicate(final Set<RID> members, final boolean lists) {
        this.members = members;
        this.lists = lists;
      }

      @Override
      public boolean contains(final Object o) {
        ++tested;
        return members.contains(o);
      }

      @Override
      public boolean isEmpty() {
        return false;
      }

      @Override
      public int size() {
        return Integer.MAX_VALUE;
      }

      @Override
      public Iterator<RID> iterator() {
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean listsCandidates(final int bucketId) {
        return true;
      }

      @Override
      public Candidates listCandidates(final int bucketId, final int maxCandidates) {
        listingCaps.add(maxCandidates);
        return lists && members.size() <= maxCandidates ? new Candidates(members, true) : null;
      }
    }

    private void createSchemaAndData() {
      // No rebuild may run mid-test, or the live counts the plan is measured against would move under the test
      database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 1_000_000);
      database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, -1);

      database.transaction(() -> {
        database.command("sql", "CREATE VERTEX TYPE Doc BUCKETS 1");
        database.command("sql", "CREATE PROPERTY Doc.id STRING");
        database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
        database.command("sql", "CREATE INDEX ON Doc (id) UNIQUE");

        final TypeLSMVectorIndexBuilder builder = (TypeLSMVectorIndexBuilder) database.getSchema()
            .buildTypeIndex("Doc", new String[] { "embedding" }).withLSMVectorType();
        builder.withDimensions(DIMENSIONS).withQuantization(VectorQuantizationType.NONE).withSimilarity("EUCLIDEAN").create();
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
      return database.query("sql", "SELECT @rid AS rid FROM Doc WHERE id = ?", id).next().getProperty("rid");
    }

    private static List<RID> ridsIn(final List<Pair<RID, Float>> results) {
      final List<RID> rids = new ArrayList<>(results.size());
      for (final Pair<RID, Float> r : results)
        rids.add(r.getFirst());
      return rids;
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

  /**
   * Regression for #5748: allow list brute force scan.
   * <p>
   * Issue #5748: the brute-force fallback used to walk every ordinal in the index and test each one for allow-list
   * membership. An allow-list narrower than {@code k} can never satisfy the caller's expected-result threshold, so the
   * fallback fires on every single filtered query and each one pays a full index scan. The scan now resolves the
   * allow-list to its ordinals and reads only those vectors.
   * <p>
   * The optimization is only allowed to be faster, never different, so the tests here compare the two paths on the same
   * inputs rather than checking the filtered answer in isolation:
   * <ul>
   *   <li>{@link #theAllowListWalkReturnsExactlyWhatTheFullScanWouldHaveReturned} is the parity assertion, RID for RID,
   *       distance for distance, in the same order and with the same truncation at k.</li>
   *   <li>{@link #theAllowListWalkTouchesOnlyTheAllowedEntries} is the win itself, counted rather than timed: the same
   *       query resolves one location per allowed RID instead of one per ordinal in the index.</li>
   *   <li>{@link #anAllowedRidWhoseVectorCannotBeReadIsNotScored} pins the guard the rebase nearly lost. A location can
   *       say "live" while the read still fails, and {@code getVector} answers a placeholder rather than null in that
   *       case, so a {@code vec == null} check alone lets the placeholder be scored as if it were a real vector
   *       (issue #5558).</li>
   *   <li>{@link #anAllowListWiderThanTheIndexStillFilters} covers the crossover: past a certain width the plain scan is
   *       the cheaper way to answer, and taking it must not drop the filter.</li>
   *   <li>{@link #anAllowedRidWithNoLiveOrdinalIsSkippedOnBothPaths} covers the entries the walk cannot resolve at all,
   *       which is where an equivalence between "every ordinal, filtered" and "the allow-list, resolved" can quietly
   *       stop holding.</li>
   * </ul>
   *
   * @author Roberto Franchini (r.franchini@arcadedata.com)
   */
  @Nested
  class AllowListBruteForceScan extends TestHelper {

    private static final VectorTypeSupport VTS        = VectorizationProvider.getInstance().getVectorTypeSupport();
    private static final int               DIMENSIONS = 8;
    private static final int               VERTICES   = 40;
    private static final int               K          = 10;

    @Test
    void theAllowListWalkReturnsExactlyWhatTheFullScanWouldHaveReturned() throws Exception {
      createSchemaAndData();

      final LSMVectorIndex index = vectorIndex();
      final int[] ordinalMap = index.getOrdinalToVectorIdForTest();
      final float[] query = embedding(7);

      // Three RIDs scattered across the ordinal map so the answer is not the first few ordinals by accident.
      final Set<RID> allowed = ridsOf(3, 19, 31);

      final List<Pair<RID, Float>> filtered = scan(index, query, K, allowed, ordinalMap);

      // The reference: scan everything with no allow-list at all, then apply the filter and the truncation by hand.
      // Anything the walk gets wrong - a missing RID, a RID resolved to the wrong ordinal, a different tie break -
      // shows up as a difference against this list.
      final List<Pair<RID, Float>> everything = scan(index, query, ordinalMap.length, null, ordinalMap);
      final List<Pair<RID, Float>> reference = new ArrayList<>();
      for (final Pair<RID, Float> candidate : everything)
        if (allowed.contains(candidate.getFirst()) && reference.size() < K)
          reference.add(candidate);

      assertThat(reference).as("the fixture has to leave something for the comparison to be about").hasSize(3);
      assertThat(ridsIn(filtered)).as("the allow-list walk must return the same RIDs, in the same order")
          .isEqualTo(ridsIn(reference));
      assertThat(distancesIn(filtered)).as("and the same distances, computed from the same vectors")
          .isEqualTo(distancesIn(reference));
    }

    @Test
    void theAllowListWalkTruncatesAtKLikeTheFullScan() throws Exception {
      createSchemaAndData();

      final LSMVectorIndex index = vectorIndex();
      final int[] ordinalMap = index.getOrdinalToVectorIdForTest();
      final float[] query = embedding(0);

      // More allowed RIDs than k, so truncation is what decides the answer rather than availability.
      final Set<RID> allowed = ridsOf(1, 5, 9, 13, 17, 21, 25, 29, 33, 37, 2, 6);
      final int k = 4;

      final List<Pair<RID, Float>> filtered = scan(index, query, k, allowed, ordinalMap);

      final List<Pair<RID, Float>> everything = scan(index, query, ordinalMap.length, null, ordinalMap);
      final List<Pair<RID, Float>> reference = new ArrayList<>();
      for (final Pair<RID, Float> candidate : everything)
        if (allowed.contains(candidate.getFirst()) && reference.size() < k)
          reference.add(candidate);

      assertThat(filtered).as("k caps the filtered answer exactly as it caps the full scan").hasSize(k);
      assertThat(ridsIn(filtered)).isEqualTo(ridsIn(reference));
      assertThat(distancesIn(filtered)).isEqualTo(distancesIn(reference));
    }

    /**
     * Two allowed vectors at the same distance, and only room for one. Which one survives the truncation is decided by
     * the order the two were scored in, so the walk has to score in ordinal order like the full scan does - the
     * allow-list's own iteration order is not the answer. The allow-list here is deliberately ordered against the
     * ordinals to make the difference visible rather than incidental.
     */
    @Test
    void tiedDistancesTruncateToTheSameRidAsTheFullScan() throws Exception {
      createSchemaAndData();
      // Two more documents sharing one embedding, inserted in a known order so the earlier one holds the lower ordinal.
      database.transaction(() -> {
        database.command("sql", "INSERT INTO Doc SET id = ?, embedding = ?", "twinA", embedding(500));
        database.command("sql", "INSERT INTO Doc SET id = ?, embedding = ?", "twinB", embedding(500));
        // A trailing document, because the most recent insert stays in the delta buffer and out of the ordinal map,
        // and both twins have to be in the map for the scan to see the tie at all.
        database.command("sql", "INSERT INTO Doc SET id = ?, embedding = ?", "tail", embedding(501));
      });
      vectorIndex().buildVectorGraphNow();

      final LSMVectorIndex index = vectorIndex();
      final int[] ordinalMap = index.getOrdinalToVectorIdForTest();
      final float[] query = embedding(500);

      final Set<RID> allowed = new LinkedHashSet<>();
      allowed.add(ridOf("twinB"));
      allowed.add(ridOf("twinA"));

      final List<Pair<RID, Float>> filtered = scan(index, query, 1, allowed, ordinalMap);
      final List<Pair<RID, Float>> everything = scan(index, query, ordinalMap.length, null, ordinalMap);

      assertThat(everything.get(0).getSecond()).as("the fixture must really produce a tie")
          .isEqualTo(everything.get(1).getSecond());

      final List<Pair<RID, Float>> reference = new ArrayList<>();
      for (final Pair<RID, Float> candidate : everything)
        if (allowed.contains(candidate.getFirst()) && reference.isEmpty())
          reference.add(candidate);

      assertThat(ridsIn(filtered)).as("a tie must break the same way on both paths").isEqualTo(ridsIn(reference));
    }

    /**
     * The win itself, counted rather than timed so it cannot flake on a loaded runner.
     * <p>
     * What the full scan spends on a filtered query is one location lookup per ordinal - a boxed key into a map the
     * size of the index, for every vector it is about to reject - and the vector reads were already limited to the
     * allowed RIDs, because the membership test comes before the read on both paths. So the lookup count is the
     * measure of this change, and it goes from "the whole index" to "the allow-list".
     */
    @Test
    void theAllowListWalkTouchesOnlyTheAllowedEntries() throws Exception {
      createSchemaAndData();

      final LSMVectorIndex index = vectorIndex();
      final int[] ordinalMap = index.getOrdinalToVectorIdForTest();
      final float[] query = embedding(7);
      final Set<RID> allowed = ridsOf(3, 19, 31);

      final VectorLocationIndex original = index.getVectorIndex();
      final CountingLocationIndex locations = installLocationIndex(index, new CountingLocationIndex(original));
      final List<Pair<RID, Float>> filtered;
      final int filteredLookups;
      final int fullLookups;
      try {
        locations.lookups = 0;
        filtered = scan(index, query, K, allowed, ordinalMap);
        filteredLookups = locations.lookups;

        locations.lookups = 0;
        scan(index, query, ordinalMap.length, null, ordinalMap);
        fullLookups = locations.lookups;
      } finally {
        // Put the real map back before teardown: the counting one only forwards reads, it holds no entries of its own.
        installLocationIndex(index, original);
      }

      assertThat(filtered).as("and it still answers the query").hasSize(allowed.size());
      // Three lookups per resolved ordinal since issue #5588: the scan asks for the RID to apply the membership
      // test, and the vector read asks for the packed offset and then, when it has to fall back to the document,
      // for the RID again. It was two when one call answered the whole tuple. What the test pins is unchanged -
      // the cost follows the allow-list and not the size of the index.
      assertThat(filteredLookups)
          .as("the filtered scan resolves the allow-list and nothing else, so its cost follows the allow-list")
          .isLessThanOrEqualTo(allowed.size() * 3);
      assertThat(fullLookups).as("while the unfiltered scan costs one lookup per ordinal, which is the baseline")
          .isGreaterThanOrEqualTo(ordinalMap.length);
      assertThat(filteredLookups).as("and the gap is what issue #5748 is about").isLessThan(fullLookups / 4);
    }

    /**
     * A location that says "live" is not a promise that the vector can be read: a document whose vector property was
     * removed, has the wrong type or reads back as all zeros comes out of {@code getVector} as the deleted placeholder,
     * which is a well-formed vector of the right shape and scores like one. The scan has to recognise it.
     * <p>
     * The fixture makes every read fail at once by pointing the vector values at a property the documents do not have,
     * which is the same code path as a single unreadable document and needs no mutation of the index behind its own
     * back. With the guard removed from the allow-list walk this returns three results carrying a plausible distance.
     */
    @Test
    void anAllowedRidWhoseVectorCannotBeReadIsNotScored() throws Exception {
      createSchemaAndData();

      final LSMVectorIndex index = vectorIndex();
      final int[] ordinalMap = index.getOrdinalToVectorIdForTest();
      final float[] query = embedding(7);
      final Set<RID> allowed = ridsOf(3, 19, 31);

      final RandomAccessVectorValues unreadable = ArcadePageVectorValues.forSearch((DatabaseInternal) database,
          DIMENSIONS, "thereIsNoSuchProperty", index.getVectorIndex(), ordinalMap, index);

      assertThat(unreadable.getVector(0)).as("the fixture must really produce the placeholder, not a vector")
          .matches(v -> ((ArcadePageVectorValues) unreadable).isDeletedSentinel(v));

      final List<Pair<RID, Float>> filtered = new ArrayList<>();
      invokeBruteForceScan(index, query, K, allowed, filtered, unreadable, ordinalMap);
      assertThat(filtered).as("an allowed RID whose vector cannot be read must not be scored").isEmpty();

      final List<Pair<RID, Float>> full = new ArrayList<>();
      invokeBruteForceScan(index, query, K, null, full, unreadable, ordinalMap);
      assertThat(full).as("and the full scan has to agree, which is what makes the two paths one behaviour").isEmpty();
    }

    /**
     * Past the crossover the allow-list is wider than the index and resolving every entry costs more than walking the
     * ordinals, so the scan takes the plain loop. Taking it must not lose the filter: the answer is still only the
     * allowed RIDs.
     */
    @Test
    void anAllowListWiderThanTheIndexStillFilters() throws Exception {
      createSchemaAndData();

      final LSMVectorIndex index = vectorIndex();
      final int[] ordinalMap = index.getOrdinalToVectorIdForTest();
      final float[] query = embedding(7);

      final Set<RID> real = ridsOf(3, 19, 31);
      final Set<RID> wide = new LinkedHashSet<>(real);
      // RIDs of a bucket this index knows nothing about, purely to push the allow-list past the ordinal count.
      for (int i = 0; wide.size() <= ordinalMap.length; i++)
        wide.add(new RID(9999, i));
      assertThat(wide.size()).as("the allow-list has to be wide enough to take the other branch")
          .isGreaterThan(ordinalMap.length);

      final List<Pair<RID, Float>> filtered = scan(index, query, K, wide, ordinalMap);

      assertThat(ridsIn(filtered)).as("a wide allow-list filters exactly like a narrow one")
          .isEqualTo(ridsIn(scan(index, query, K, real, ordinalMap)));
    }

    /**
     * The allow-list to ordinal mapping has to be total: an entry it cannot resolve must contribute nothing, not a
     * guessed ordinal. Three kinds of unresolvable entry are in the allow-list here - a RID whose vector was tombstoned,
     * which keeps its ordinal in the map but loses its location; a RID ingested after the last graph build, which is
     * only in the delta buffer and has no ordinal at all; and a RID this index never saw. The answer still has to be
     * what the full scan would have filtered down to.
     */
    @Test
    void anAllowedRidWithNoLiveOrdinalIsSkippedOnBothPaths() throws Exception {
      createSchemaAndData();

      final RID tombstoned = ridOf("doc5");
      database.transaction(() -> database.command("sql", "DELETE FROM Doc WHERE id = ?", "doc5"));
      database.transaction(
          () -> database.command("sql", "INSERT INTO Doc SET id = ?, embedding = ?", "afterTheBuild", embedding(600)));

      final LSMVectorIndex index = vectorIndex();
      final int[] ordinalMap = index.getOrdinalToVectorIdForTest();
      final float[] query = embedding(7);

      final Set<RID> allowed = new LinkedHashSet<>(ridsOf(3, 19, 31));
      allowed.add(tombstoned);
      final RID onlyInTheDelta = ridOf("afterTheBuild");
      allowed.add(onlyInTheDelta);
      allowed.add(new RID(9999, 0));
      assertThat(allowed.size()).as("the fixture has to stay on the allow-list walk").isLessThan(ordinalMap.length);

      final List<Pair<RID, Float>> filtered = scan(index, query, K, allowed, ordinalMap);

      final List<Pair<RID, Float>> everything = scan(index, query, ordinalMap.length, null, ordinalMap);
      final List<Pair<RID, Float>> reference = new ArrayList<>();
      for (final Pair<RID, Float> candidate : everything)
        if (allowed.contains(candidate.getFirst()) && reference.size() < K)
          reference.add(candidate);

      assertThat(reference).as("only the three live indexed RIDs can be answered at all").hasSize(3);
      assertThat(ridsIn(filtered)).as("an unresolvable allow-list entry contributes nothing, on either path")
          .isEqualTo(ridsIn(reference));
      assertThat(ridsIn(filtered)).doesNotContain(tombstoned, onlyInTheDelta);
    }

    /**
     * The reverse lookup is a binary search, so the ordinal map has to be sorted. Every producer sorts it because the
     * ordinals have to line up with the order the graph was persisted in; this pins that invariant where the search
     * depends on it, since breaking it would not throw, it would silently return fewer neighbors.
     */
    @Test
    void theOrdinalMapIsSortedAscending() {
      createSchemaAndData();

      final int[] ordinalMap = vectorIndex().getOrdinalToVectorIdForTest();
      assertThat(ordinalMap).as("the fixture must have built a graph").isNotEmpty();
      assertThat(ordinalMap).isSorted();
    }

    // ------------------------------------------------------------------------------------------------- helpers

    private void createSchemaAndData() {
      // No rebuild may run mid-test: the ordinal map has to stay the one the assertions captured. Both settings are
      // database-scoped, so they are set on this database rather than globally - the inactivity timer is process-wide
      // and disarming it here would follow the JVM into every later test in the module.
      database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 1_000_000);
      database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, -1);

      database.transaction(() -> {
        database.command("sql", "CREATE VERTEX TYPE Doc");
        database.command("sql", "CREATE PROPERTY Doc.id STRING");
        database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
        database.command("sql", "CREATE INDEX ON Doc (id) UNIQUE");

        final TypeLSMVectorIndexBuilder builder = (TypeLSMVectorIndexBuilder) database.getSchema()
            .buildTypeIndex("Doc", new String[] { "embedding" }).withLSMVectorType();
        // No quantization: the vectors are read back from the documents, which is what lets the unreadable-vector test
        // fail the read by naming a property that does not exist.
        builder.withDimensions(DIMENSIONS).withQuantization(VectorQuantizationType.NONE).withSimilarity("EUCLIDEAN")
            .create();
      });

      database.transaction(() -> {
        for (int i = 0; i < VERTICES; i++)
          database.command("sql", "INSERT INTO Doc SET id = ?, embedding = ?", "doc" + i, embedding(i));
      });

      vectorIndex().buildVectorGraphNow();
    }

    private List<Pair<RID, Float>> scan(final LSMVectorIndex index, final float[] query, final int k,
        final Set<RID> allowedRIDs, final int[] ordinalMap) throws Exception {
      final List<Pair<RID, Float>> results = new ArrayList<>();
      invokeBruteForceScan(index, query, k, allowedRIDs, results, vectorValues(ordinalMap), ordinalMap);
      return results;
    }

    private void invokeBruteForceScan(final LSMVectorIndex index, final float[] query, final int k,
        final Set<RID> allowedRIDs, final List<Pair<RID, Float>> results, final RandomAccessVectorValues vectors,
        final int[] ordinalMap) throws Exception {
      final Method bruteForceScan = LSMVectorIndex.class.getDeclaredMethod("bruteForceScan", VectorFloat.class, int.class,
          Set.class, List.class, RandomAccessVectorValues.class, int[].class);
      bruteForceScan.setAccessible(true);
      bruteForceScan.invoke(index, VTS.createFloatVector(query), k, allowedRIDs, results, vectors, ordinalMap);
    }

    /**
     * Replaces the index's location map with one that counts its lookups. The instance is only swapped out by a
     * compaction, which this fixture never triggers, so the counter installed here is the one the scan will use.
     */
    private <T extends VectorLocationIndex> T installLocationIndex(final LSMVectorIndex index, final T locations)
        throws Exception {
      final Field field = LSMVectorIndex.class.getDeclaredField("residentLocations");
      field.setAccessible(true);
      field.set(index, locations);
      return locations;
    }

    /**
     * The page-backed reader the index itself would use. It has to be that class and not a decorator around it: the
     * scan recognises {@link ArcadePageVectorValues} to apply its deleted-placeholder guard, and a wrapper would
     * silently turn the guard off and move the test onto a different code path.
     */
    private ArcadePageVectorValues vectorValues(final int[] ordinalMap) {
      return ArcadePageVectorValues.forSearch((DatabaseInternal) database, DIMENSIONS, "embedding",
          vectorIndex().getVectorIndex(), ordinalMap, vectorIndex());
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
     * Counts the location lookups the scan performs, delegating the answers to the real index.
     * <p>
     * Every way of asking for one entry's location counts, not just {@code getLocation}: since issue #5588 the scan
     * asks for the RID and the vector reader asks for the packed offset, where both used to ask for the whole tuple.
     * <p>
     * It has to extend {@link VectorLocationIndex} rather than wrap it, because the field it is installed into is
     * typed as one. That makes it a subclass whose OWN inherited state is permanently empty, so any method left
     * un-overridden answers about nothing rather than about the delegate - a silent wrong answer, not a failure.
     * Everything {@code LSMVectorIndex} reads is therefore forwarded below, not just the methods this test counts:
     * a read that is not forwarded would report an empty index and the scan under test would find no vectors at all.
     */
    private static final class CountingLocationIndex extends VectorLocationIndex {
      private final VectorLocationIndex delegate;
      private       int                 lookups;

      CountingLocationIndex(final VectorLocationIndex delegate) {
        this.delegate = delegate;
      }

      // --- forwarded but NOT counted: the scan's cost is measured in per-entry location lookups, and these are
      // either O(1) bookkeeping or whole-index walks that no per-ordinal bound should charge to.

      @Override
      public int size() {
        return delegate.size();
      }

      @Override
      public long getActiveCount() {
        return delegate.getActiveCount();
      }

      @Override
      public IntStream getAllVectorIds() {
        return delegate.getAllVectorIds();
      }

      @Override
      public IntStream getActiveVectorIds() {
        return delegate.getActiveVectorIds();
      }

      @Override
      public boolean isDeleted(final int vectorId) {
        return delegate.isDeleted(vectorId);
      }

      @Override
      public int getDeletedCount() {
        return delegate.getDeletedCount();
      }

      @Override
      public int getNextId() {
        return delegate.getNextId();
      }

      @Override
      public int getMaxVectorId() {
        return delegate.getMaxVectorId();
      }

      @Override
      public long estimatedRetainedBytes() {
        return delegate.estimatedRetainedBytes();
      }

      @Override
      public VectorLocation getLocation(final int vectorId) {
        lookups++;
        return delegate.getLocation(vectorId);
      }

      @Override
      public RID getRid(final int vectorId) {
        lookups++;
        return delegate.getRid(vectorId);
      }

      @Override
      public long getOffsetAndFlag(final int vectorId) {
        lookups++;
        return delegate.getOffsetAndFlag(vectorId);
      }

      @Override
      public boolean isLive(final int vectorId) {
        lookups++;
        return delegate.isLive(vectorId);
      }

      @Override
      public boolean isLocationOf(final int vectorId, final RID rid) {
        lookups++;
        return delegate.isLocationOf(vectorId, rid);
      }

      @Override
      public int[] getVectorIdsForRid(final RID rid) {
        return delegate.getVectorIdsForRid(rid);
      }
    }
  }
}
