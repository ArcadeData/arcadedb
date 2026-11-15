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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.Pair;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The delta buffer and what a query pays for it. Each nested class keeps the fixture of the regression test it was folded from.
 */
class VectorDeltaScanTest {

  /**
   * Regression for #6797: rebuild triggers denominated in what a query pays for the delta buffer.
   * <p>
   * Issue #6797: every rebuild trigger an {@code LSM_VECTOR} index had was denominated in mutations, and no number
   * of mutations bounds what a query pays for them.
   * <p>
   * Vectors ingested since the last rebuild are answered by a linear scan of the in-memory delta buffer, so that
   * term of a query is {@code O(buffer)} while the HNSW walk it supplements is {@code O(log corpus)}.
   * {@code rebuildGraphRatio} sizes the buffer as a fixed fraction of the corpus - asymptotically the wrong shape,
   * since a fifth of the corpus scanned linearly dominates a logarithmic walk at every scale - and
   * {@code maxPendingMutations} caps that at a fixed count, so from 250,000 vectors upward (0.2 x 250,000 = 50,000)
   * the threshold stops scaling altogether and the same absolute scan cost lands on an index of any size. Measured
   * at that cap on a 200,000-vector index, the scan was roughly four fifths of query time.
   * <p>
   * The fix adds a trigger denominated in the quantity that actually matters, and measured rather than assumed:
   * {@code maxDeltaScanRatio} bounds the buffer against how many nodes this index's graph walks are observed to
   * visit. Because it is evaluated on the search path against a cost only searches produce, an ingest-only workload
   * never reaches it and keeps the geometric amortization {@code rebuildGraphRatio} exists to provide - only a
   * workload actually paying the scan pays for the rebuilds that remove it.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  @Tag("vector")
  class DeltaScanScaling extends TestHelper {

    private static final int EMBEDDING_DIM = 32;
    // Must be >= LSMVectorIndex.ASYNC_REBUILD_MIN_GRAPH_SIZE (1000) so a rebuild goes through the async, threshold
    // -gated path rather than the unconditional synchronous one small graphs take.
    private static final int BASE_VECTORS   = 1100;

    private static final Duration REBUILD_SETTLE_TIMEOUT =
        Duration.ofMillis(GlobalConfiguration.VECTOR_INDEX_REBUILD_PERMIT_TIMEOUT_MS.getValueAsLong() + 60_000L);

    /**
     * The defect itself, as a pure function of the four inputs the engine feeds it. The table is the one reported in
     * the issue: the ratio-derived term is the right shape, but the ceiling on it is a constant, so past
     * {@code maxPendingMutations / rebuildGraphRatio} vectors the threshold is the same on a million-vector index
     * as on a ten-million-vector one.
     */
    @Test
    void rebuildThresholdStopsScalingOnceTheFixedCeilingBinds() {
      final int absolute = GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD.getDefValue() instanceof Integer i ?
          i : 100;
      final float ratio = 0.2f;
      final int maxPending = 50_000;

      assertThat(LSMVectorIndex.computeRebuildThreshold(absolute, 10_000, ratio, maxPending))
          .as("below the ceiling the threshold tracks the graph").isEqualTo(2_000);
      assertThat(LSMVectorIndex.computeRebuildThreshold(absolute, 100_000, ratio, maxPending))
          .isEqualTo(20_000);
      assertThat(LSMVectorIndex.computeRebuildThreshold(absolute, 250_000, ratio, maxPending))
          .as("the ceiling binds exactly here").isEqualTo(50_000);

      // From here on the graph grows by 40x and the permitted buffer does not move at all.
      assertThat(LSMVectorIndex.computeRebuildThreshold(absolute, 1_000_000, ratio, maxPending))
          .isEqualTo(50_000);
      assertThat(LSMVectorIndex.computeRebuildThreshold(absolute, 10_000_000, ratio, maxPending))
          .as("issue #6797: 0.2 x 10M is 2,000,000, but the fixed ceiling pins it at the same 50,000 a 250,000"
              + "-vector index gets").isEqualTo(50_000);

      // The two escape hatches the arithmetic already had, pinned so the extraction cannot have changed them.
      assertThat(LSMVectorIndex.computeRebuildThreshold(absolute, 10_000_000, 0f, maxPending))
          .as("ratio 0 disables scaling and leaves the absolute floor").isEqualTo(absolute);
      assertThat(LSMVectorIndex.computeRebuildThreshold(absolute, 10_000_000, ratio, 0))
          .as("maxPending 0 removes the ceiling").isEqualTo(2_000_000);
      assertThat(LSMVectorIndex.computeRebuildThreshold(120_000, 250_000, ratio, maxPending))
          .as("the absolute floor is applied last, so an explicit value above the ceiling still wins")
          .isEqualTo(120_000);
    }

    /**
     * The damping term. A budget denominated in the cost of one query knows nothing about what draining the buffer
     * costs, so on a large index under steady ingest it would be exceeded again seconds after a rebuild that took
     * minutes, and would ask for another - a background thread turned permanently busy, which would be a worse
     * problem than the one being fixed. The guard is the amortized argument in the currency both sides can be
     * counted in, so it is asserted as arithmetic rather than as a latency.
     */
    @Test
    void aRebuildIsOnlyWorthTriggeringOnceTheScansHavePaidForIt() {
      final long rebuildWork = 1_000_000L;

      assertThat(LSMVectorIndex.deltaScanPaysForARebuild(0L, rebuildWork))
          .as("no query has scanned anything - nothing has been lost to the buffer yet").isFalse();
      assertThat(LSMVectorIndex.deltaScanPaysForARebuild(rebuildWork - 1, rebuildWork))
          .as("still cheaper to keep scanning than to rebuild").isFalse();
      assertThat(LSMVectorIndex.deltaScanPaysForARebuild(rebuildWork, rebuildWork))
          .as("the scans have now cost what the rebuild will, which is the 2-competitive bound this gives: the "
              + "extra rebuild CPU the policy adds can never exceed the query CPU it removes").isTrue();
      assertThat(LSMVectorIndex.deltaScanPaysForARebuild(rebuildWork * 100, rebuildWork)).isTrue();

      // A graph that costs nothing to rebuild is always worth rebuilding; the caller keeps the "is there a graph
      // at all" question to itself, because a rebuild that has no graph to walk is not this policy's decision.
      assertThat(LSMVectorIndex.deltaScanPaysForARebuild(5L, 0L)).isTrue();
    }

    /**
     * The fix, end to end: with every mutation-count trigger placed out of reach, a buffer that outgrows the
     * measured graph-walk cost must still be drained.
     */
    @Test
    void deltaScanOutgrowingTheGraphWalkTriggersARebuild() {
      disableEveryCountBasedTrigger();
      database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MAX_DELTA_SCAN_RATIO, 1.0f);

      final Random random = new Random(6797);
      final LSMVectorIndex index = createSettledIndex(random);

      // One query to measure the walk. Until a search has actually walked this graph there is no latency to protect
      // and the policy deliberately does nothing.
      index.findNeighborsFromVector(randomUnitVector(random), 10);

      final long walkCost = index.getStats().get("graphWalkVisitedAvg");
      assertThat(walkCost).as("a graph walk over %d vectors must have visited something", BASE_VECTORS)
          .isGreaterThan(0L);

      final long budget = index.getStats().get("deltaScanBudget");
      assertThat(budget).as("the budget must be a real number, not the disabled sentinel")
          .isLessThan(Long.MAX_VALUE);
      assertThat(budget).as("at ratio 1.0 the budget IS the measured walk cost, not the absolute floor - if it were "
              + "the floor, this test would be pinning mutationsBeforeRebuild rather than the new policy")
          .isEqualTo(walkCost)
          .isGreaterThan(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD.getValueAsInteger());

      // Fill the buffer past the budget. Nothing here can reach a mutation-count trigger: the effective count
      // threshold is over a hundred thousand and the inactivity timer is off.
      final int overBudget = (int) Math.min(budget + 50, 5_000);
      insertVectors(random, overBudget);

      assertThat(index.getStats().get("mutationsSinceRebuild"))
          .as("well below the count threshold of %d - only the scan budget can fire here",
              index.getStats().get("effectiveMutationsThreshold"))
          .isLessThan(index.getStats().get("effectiveMutationsThreshold"));
      assertThat(index.getStats().get("deltaVectorsCount"))
          .as("the buffer is over the measured budget of %d", budget)
          .isGreaterThanOrEqualTo(budget);

      // ...but not yet enough scanning to have paid for the rebuild: the buffer being over budget is only half
      // the trigger. This is what stops the policy asking for a rebuild the instant the buffer refills.
      assertThat(index.getStats().get("deltaScanWorkSinceRebuild"))
          .as("no query has scanned the new buffer yet")
          .isLessThan(index.getStats().get("deltaScanWorkTarget"));

      // The trigger lives on the search path, so searches are what must notice - and enough of them that the scan
      // they have collectively paid for exceeds what the rebuild will cost.
      index.findNeighborsFromVector(randomUnitVector(random), 10);

      Awaitility.await("a delta buffer that outgrew the graph walk is drained by a rebuild (issue #6797)")
          .atMost(REBUILD_SETTLE_TIMEOUT)
          .pollInterval(Duration.ofMillis(50))
          .untilAsserted(() -> {
            index.findNeighborsFromVector(randomUnitVector(random), 10);
            assertThat(PendingDeltaVectors.of(index)).isZero();
          });
    }

    /**
     * The other half of the claim the fix makes: the inactivity timer honours the same condition, because it would
     * be odd for a search to judge the scan too expensive and the timer, looking at the same buffer with the
     * machine idle, to decline.
     * <p>
     * The timer's decision is asked directly rather than driven through a real timer. Not for speed: the search
     * path evaluates the identical condition on every query and would usually act on it first, so a test that
     * waited for a timeout would be racing the thing it is trying to isolate, and would pass or fail on which one
     * won. Arranging the state under {@code maxDeltaScanRatio = 0} - where the scan work still accumulates but
     * nothing can trigger on it - and only then turning the policy on isolates the question completely.
     */
    @Test
    void theInactivityTimerHonoursTheSameScanBudget() {
      disableEveryCountBasedTrigger();
      database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MAX_DELTA_SCAN_RATIO, 1.0f);

      final Random random = new Random(6797);
      final LSMVectorIndex index = createSettledIndex(random);
      index.findNeighborsFromVector(randomUnitVector(random), 10);

      final long budget = index.getStats().get("deltaScanBudget");
      // deltaScanWorkTarget, not estimatedRebuildWork: the latter is the graph-only cost estimate, the former is
      // that estimate scaled by the ratio and is what the trigger actually compares against.
      final long rebuildWork = index.getStats().get("deltaScanWorkTarget");
      assertThat(budget).isLessThan(Long.MAX_VALUE);
      assertThat(rebuildWork).isPositive();
      assertThat(index.getStats().get("estimatedRebuildWork"))
          .as("the cost estimate must answer for the graph alone - at ratio 1.0 the target equals it, and it must "
              + "not have been scaled twice")
          .isEqualTo(rebuildWork);

      // Policy off while the state is arranged: the scan is charged to the amortization counter regardless of the
      // ratio, but with the ratio at 0 nothing - search path or timer - can act on what accumulates.
      database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MAX_DELTA_SCAN_RATIO, 0f);

      insertVectors(random, (int) Math.min(budget + 50, 5_000));
      for (int i = 0; i < 100_000 && index.deltaScanWorkForTest() < rebuildWork; i++)
        index.findNeighborsFromVector(randomUnitVector(random), 10);

      assertThat(index.deltaScanWorkForTest())
          .as("the scans must have paid for a rebuild, or there is nothing for the timer to decide")
          .isGreaterThanOrEqualTo(rebuildWork);
      assertThat(index.getStats().get("deltaVectorsCount"))
          .as("with the policy off nothing may have drained the buffer").isGreaterThanOrEqualTo(budget);

      final long pending = index.getStats().get("mutationsSinceRebuild");
      final long countThreshold = index.getStats().get("effectiveMutationsThreshold");
      assertThat(pending)
          .as("the count-based reasons the timer rebuilds must all be out of reach, so only the scan budget is "
              + "left to explain a true answer")
          .isLessThan(Math.max(countThreshold / 10, 1));

      assertThat(index.inactivityRebuildIsWorthIt())
          .as("policy off: the timer sees the pre-#6797 picture and declines, exactly as before").isFalse();

      database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MAX_DELTA_SCAN_RATIO, 1.0f);

      assertThat(index.inactivityRebuildIsWorthIt())
          .as("policy on: the same buffer, on an idle machine, is worth the rebuild the search path would also "
              + "have asked for (issue #6797)").isTrue();
    }

    /**
     * The approximate search path scans the delta buffer too - {@code mergeWithDeltaScanApproximate} delegates the
     * scan itself to {@code mergeWithDeltaScan} - but it is the one search path that never called
     * {@code rebuildGraphBeforeSearch()}, so no query on it has ever triggered a rebuild of any kind. Leaving it
     * out would have made the mode sold on microsecond latency the one mode where the linear scan still grows
     * without limit.
     */
    @Test
    void theApproximateSearchPathIsBoundedToo() {
      disableEveryCountBasedTrigger();
      database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MAX_DELTA_SCAN_RATIO, 1.0f);

      final Random random = new Random(6797);
      final LSMVectorIndex index = createSettledIndex(random, true);

      // Every query below goes through the PQ path, never the exact one - if PQ were unavailable the method falls
      // back to findNeighborsFromVector and this test would silently be re-testing the path already covered.
      assertThat(index.isPQSearchAvailable())
          .as("this fixture must exercise the approximate path, not fall back to the exact one").isTrue();

      index.findNeighborsFromVectorApproximate(randomUnitVector(random), 10);
      final long budget = index.getStats().get("deltaScanBudget");
      assertThat(budget).as("the approximate path must feed the walk measurement as well")
          .isLessThan(Long.MAX_VALUE);

      insertVectors(random, (int) Math.min(budget + 50, 5_000));
      assertThat(index.getStats().get("deltaVectorsCount")).isGreaterThanOrEqualTo(budget);

      Awaitility.await("approximate queries alone must be able to drain a buffer they are paying to scan")
          .atMost(REBUILD_SETTLE_TIMEOUT)
          .pollInterval(Duration.ofMillis(50))
          .untilAsserted(() -> {
            index.findNeighborsFromVectorApproximate(randomUnitVector(random), 10);
            assertThat(PendingDeltaVectors.of(index)).isZero();
          });
    }

    /**
     * The grouped path completes the set. It has its own delta scan - {@code scoreDeltaCandidates}, which scores the
     * whole buffer rather than a top-k slice of it, so it is if anything the costlier of the two - and its own
     * charge to the amortization counter. Since the value this fix claims is that every search path pays for what
     * it scans, each of the three paths that scans is asserted end to end rather than two of them plus an
     * assumption about the third.
     */
    @Test
    void theGroupedSearchPathIsBoundedToo() {
      disableEveryCountBasedTrigger();
      database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MAX_DELTA_SCAN_RATIO, 1.0f);

      final Random random = new Random(6797);
      final LSMVectorIndex index = createSettledIndex(random);

      // One group per row, so the cap fills within the first beam and the walk stops there. Grouping by something
      // coarse - a bucket id, say - is a trap this test fell into first: with only a handful of distinct keys the
      // cap can never fill, the walk resumes until it exhausts its candidate budget, and the measured cost climbs
      // to most of the graph. The buffer then never exceeds it and no rebuild is triggered - which is the policy
      // being RIGHT, not a gap: a scan of a thousand vectors does not dominate a walk that visited a thousand nodes.
      final Function<RID, Object> groupKey = rid -> rid;
      for (int i = 0; i < 3; i++)
        assertThat(index.findNeighborsFromVectorGrouped(randomUnitVector(random), 5, 1, -1, null, groupKey))
            .isNotEmpty();

      final long budget = index.getStats().get("deltaScanBudget");
      assertThat(budget).as("a grouped walk must feed the measurement the budget is derived from")
          .isLessThan(Long.MAX_VALUE);

      // Comfortably over, not marginally: the budget tracks an EWMA that keeps moving as the loop below queries,
      // and a buffer that only just cleared it could be overtaken by its own denominator.
      insertVectors(random, (int) Math.min(budget * 2 + 200, 5_000));
      assertThat(index.getStats().get("deltaVectorsCount")).isGreaterThanOrEqualTo(budget);

      Awaitility.await("grouped queries alone must be able to drain a buffer they are paying to scan")
          .atMost(REBUILD_SETTLE_TIMEOUT)
          .pollInterval(Duration.ofMillis(50))
          .untilAsserted(() -> {
            index.findNeighborsFromVectorGrouped(randomUnitVector(random), 5, 1, -1, null, groupKey);
            assertThat(PendingDeltaVectors.of(index)).isZero();
          });
    }

    /**
     * The control, and the opt-out: with {@code maxDeltaScanRatio} at 0 the engine behaves exactly as it did before
     * this issue - the count thresholds are the only trigger, and a buffer well past any measured walk cost is
     * scanned query after query without anything draining it.
     */
    @Test
    void ratioZeroLeavesTheCountThresholdsAsTheOnlyTrigger() throws Exception {
      disableEveryCountBasedTrigger();
      database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MAX_DELTA_SCAN_RATIO, 0f);

      final Random random = new Random(6797);
      final LSMVectorIndex index = createSettledIndex(random);

      index.findNeighborsFromVector(randomUnitVector(random), 10);
      assertThat(index.getStats().get("deltaScanBudget"))
          .as("ratio 0 reports the disabled sentinel").isEqualTo(Long.MAX_VALUE);

      // No rebuild may still be settling from the fixture before the negative assertion below starts: this test
      // proves a buffer is NOT drained, so a stray rebuild finishing during the wait would fail it for a reason
      // that has nothing to do with the policy. Bounded in seconds, deliberately, and NOT by
      // REBUILD_SETTLE_TIMEOUT: this waits on an in-process flag that the fixture's synchronous build has already
      // cleared, so ten seconds is generous. Sizing it to the rebuild-permit timeout instead would mean that on the
      // one occasion the flag is genuinely stuck - another class in the same JVM still holding the semaphore - this
      // test blocks the whole vector lane for ten minutes before saying so.
      Awaitility.await("the fixture's rebuild has fully finished, not just published its buffer")
          .atMost(Duration.ofSeconds(10))
          .pollInterval(Duration.ofMillis(50))
          .untilAsserted(() -> assertThat(index.getStats().get("asyncRebuildInProgress")).isZero());

      final long generationBefore = index.getStats().get("rebuildSnapshotGeneration");
      final int buffered = 600;
      insertVectors(random, buffered);

      for (int i = 0; i < 5; i++)
        index.findNeighborsFromVector(randomUnitVector(random), 10);

      // Long enough for an async rebuild to have started and drained the buffer had anything triggered one.
      Thread.sleep(1_000);

      assertThat(PendingDeltaVectors.of(index))
          .as("with the policy off, nothing bounds the scan - which is precisely the behaviour issue #6797 reports")
          .isEqualTo((long) buffered);
      // The buffer count alone would also read unchanged if a rebuild had run and been immediately refilled, which
      // cannot happen here but would make this test pass for the wrong reason if it could. The generation counter
      // says no rebuild ran at all.
      assertThat(index.getStats().get("rebuildSnapshotGeneration"))
          .as("no rebuild of any kind may have run").isEqualTo(generationBefore);
    }

    /**
     * The scan's own answer must not have changed. Pruning against the k-th best distance before the duplicate and
     * tombstone checks rather than after admits exactly the same rows - the heap only advances when a row is added,
     * and neither check can make a row admissible - and this pins that against distances computed by hand.
     */
    @Test
    void deltaScanStillRanksExactly() {
      disableEveryCountBasedTrigger();
      database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MAX_DELTA_SCAN_RATIO, 0f);

      final Random random = new Random(6797);
      final LSMVectorIndex index = createSettledIndex(random);
      index.findNeighborsFromVector(randomUnitVector(random), 10);

      // Buffered vectors placed strictly nearer the query than anything the base corpus holds: the base vectors are
      // random unit vectors, these sit on the first axis a hair away from the query. So the answer is decided
      // entirely by the delta scan and can be checked against distances computed here.
      final float[] query = new float[EMBEDDING_DIM];
      query[0] = 1f;

      final List<float[]> planted = new ArrayList<>();
      for (int i = 0; i < 12; i++) {
        final float[] vector = new float[EMBEDDING_DIM];
        vector[0] = 1f;
        vector[1] = 0.001f * (i + 1);
        planted.add(vector);
      }
      database.transaction(() -> {
        for (final float[] vector : planted)
          database.command("sql", "INSERT INTO Embedding SET vector = ?", (Object) vector);
      });

      assertThat(PendingDeltaVectors.of(index)).isEqualTo((long) planted.size());

      final int k = 5;
      final List<Pair<RID, Float>> results = index.findNeighborsFromVector(query, k);
      assertThat(results).hasSize(k);

      // Ascending by distance, and the k nearest planted vectors in the order their euclidean distances dictate.
      for (int i = 1; i < results.size(); i++)
        assertThat(results.get(i).getSecond()).isGreaterThanOrEqualTo(results.get(i - 1).getSecond());

      for (int i = 0; i < k; i++) {
        final float[] stored = (float[]) results.get(i).getFirst().asVertex().get("vector");
        assertThat(stored[1]).as("row %d must be the %d-th nearest planted vector", i, i)
            .isEqualTo(0.001f * (i + 1));
      }
    }

    /**
     * The one state in which the delta merge is offered a RID the graph walk already returned - a rebuild leaves a
     * node unreachable and re-queues its vector into the buffer while the node itself survives in the graph. The
     * duplicate check that catches it moved below the distance prune, so pin that it still catches it.
     */
    @Test
    void aRidInBothTheGraphAndTheBufferIsReturnedOnce() {
      disableEveryCountBasedTrigger();
      database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MAX_DELTA_SCAN_RATIO, 0f);

      final Random random = new Random(6797);
      final LSMVectorIndex index = createSettledIndex(random);
      final float[] query = randomUnitVector(random);

      final List<Pair<RID, Float>> before = index.findNeighborsFromVector(query, 10);
      assertThat(before).isNotEmpty();

      // Put the nearest row into the delta buffer as well, without removing its graph node.
      final RID duplicated = before.getFirst().getFirst();
      assertThat(index.requeueIntoDeltaBufferForTest(duplicated))
          .as("the row must still carry a live graph node").isNotEqualTo(-1);

      final List<Pair<RID, Float>> after = index.findNeighborsFromVector(query, 10);
      final Set<RID> distinct = new HashSet<>();
      for (final Pair<RID, Float> row : after)
        assertThat(distinct.add(row.getFirst())).as("%s returned twice", row.getFirst()).isTrue();
      assertThat(after).hasSameSizeAs(before);
    }

    /**
     * Puts every mutation-count trigger out of reach so only the scan budget can fire. The count threshold is
     * {@code max(absolute, min(ratio x graphSize, maxPending))}, so an enormous ratio with the ceiling removed
     * leaves it at six figures on a 1100-vector graph, while the absolute floor - which is also the floor the scan
     * budget is held to - stays at its default.
     */
    private void disableEveryCountBasedTrigger() {
      database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_REBUILD_GRAPH_RATIO, 100f);
      database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MAX_PENDING_MUTATIONS, 0);
      database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, 0);
    }

    private LSMVectorIndex createSettledIndex(final Random random) {
      return createSettledIndex(random, false);
    }

    private LSMVectorIndex createSettledIndex(final Random random, final boolean productQuantization) {
      database.transaction(() -> {
        database.getSchema().createVertexType("Embedding");
        database.getSchema().getType("Embedding").createProperty("vector", Type.ARRAY_OF_FLOATS);
        database.command("sql", productQuantization ? """
            CREATE INDEX ON Embedding (vector) LSM_VECTOR
            METADATA {
                "dimensions": %d,
                "similarity": "EUCLIDEAN",
                "quantization": "PRODUCT",
                "pqClusters": 32
            }""".formatted(EMBEDDING_DIM) : """
            CREATE INDEX ON Embedding (vector) LSM_VECTOR
            METADATA {
                "dimensions": %d,
                "similarity": "EUCLIDEAN"
            }""".formatted(EMBEDDING_DIM));
      });

      insertVectors(random, BASE_VECTORS);

      final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Embedding[vector]");
      final LSMVectorIndex index = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];

      // The first search on an empty graph builds it synchronously whatever the thresholds say, which is what
      // settles the buffer and gets the graph past ASYNC_REBUILD_MIN_GRAPH_SIZE.
      assertThat(index.findNeighborsFromVector(randomUnitVector(random), 10)).isNotEmpty();
      Awaitility.await("the initial synchronous build settles the buffer")
          .atMost(REBUILD_SETTLE_TIMEOUT)
          .pollInterval(Duration.ofMillis(50))
          .untilAsserted(() -> assertThat(PendingDeltaVectors.of(index)).isZero());
      assertThat(index.getStats().get("graphNodeCount")).isGreaterThanOrEqualTo(1000L);
      return index;
    }

    private void insertVectors(final Random random, final int count) {
      database.transaction(() -> {
        for (int i = 0; i < count; i++)
          database.command("sql", "INSERT INTO Embedding SET vector = ?", (Object) randomUnitVector(random));
      });
    }

    private float[] randomUnitVector(final Random random) {
      final float[] vector = new float[EMBEDDING_DIM];
      float norm = 0;
      for (int i = 0; i < EMBEDDING_DIM; i++) {
        vector[i] = random.nextFloat() * 2 - 1;
        norm += vector[i] * vector[i];
      }
      norm = (float) Math.sqrt(norm);
      if (norm > 0)
        for (int i = 0; i < EMBEDDING_DIM; i++)
          vector[i] /= norm;
      return vector;
    }
  }

  /**
   * Regression for #7357: the delta buffer heap is bounded.
   * <p>
   * Regression test for issue #7357: the delta buffer held a full copy of every vector written since the last graph
   * rebuild, on the heap, with nothing bounding it.
   * <p>
   * Every write appends an entry to that buffer so the vector is searchable before it reaches the HNSW graph, and the
   * entry carried the whole vector. Only a rebuild drains the buffer, and a rebuild is triggered by the index going
   * quiet - so an ingest that never goes quiet grows a second complete copy of the corpus in RAM. At the reporter's
   * scale, 4.2M records of 768-dimension embeddings, that copy is 12.9 GB and it was two thirds of what killed a
   * {@code -Xmx16g} load. It is the same second copy issue #3144 removed from the graph build's vector reader, on the
   * one path that still had it.
   * <p>
   * The vector is persisted before the entry is queued, so the payload in the entry is a cache and nothing more. Past
   * the budget the entry keeps only its id and RID, and the delta scan reads the vector back from the pages.
   * <p>
   * Three things are pinned:
   * <ul>
   *   <li>the number of payloads held on the heap stops at the budget while the buffer itself keeps growing;</li>
   *   <li>searches still find the buffered rows, and rank them the same way, once the payloads are gone - which is
   *       the whole correctness question, because a bound that quietly dropped rows from results would satisfy the
   *       first assertion perfectly;</li>
   *   <li>{@code deltaCacheSize = -1} still keeps every payload, so the bound is opt-out rather than imposed.</li>
   * </ul>
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class DeltaBufferHeapBound {
    private static final String DB_ROOT    = "target/test-databases/DeltaBufferHeapBound";
    private static final int    DIMENSIONS = 16;
    /** Kept under {@code ASYNC_REBUILD_MIN_GRAPH_SIZE} so no rebuild drains the buffer behind the assertions. */
    private static final int    RECORDS    = 400;
    private static final int    BUDGET     = 50;

    /**
     * Fixture for the re-queue path: above {@code ASYNC_REBUILD_MIN_GRAPH_SIZE} so the persisted graph is treated as
     * a large one and the stale-prefix reuse is reachable at all.
     */
    private static final int PERSISTED_VECTORS = 1_200;
    /** Records written after the persisted graph, i.e. the gap the reuse re-queues. Must exceed {@link #BUDGET}. */
    private static final int GAP_VECTORS       = 200;

    /** Fixture for the orphan re-queue: a graph small enough to build quickly, a budget well under it. */
    private static final int ORPHAN_RECORDS = 120;
    private static final int ORPHAN_BUDGET  = 10;

    private String dbPath;

    @BeforeEach
    void setUp(final TestInfo testInfo) {
      dbPath = DB_ROOT + "-" + testInfo.getTestMethod().orElseThrow().getName();
      FileUtils.deleteRecursively(new File(dbPath));
    }

    @AfterEach
    void tearDown() {
      FileUtils.deleteRecursively(new File(dbPath));
    }

    @Test
    void theBufferStopsHoldingPayloadsAtTheBudgetAndStillAnswersSearches() {
      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.create();
        try {
          createSchema(db, BUDGET);
          insert(db, 0, RECORDS);

          final LSMVectorIndex index = vectorIndex(db);

          assertThat(index.getStats().get("deltaVectorsCount"))
              .as("precondition: nothing has drained the buffer, so it holds every written vector")
              .isEqualTo((long) RECORDS);
          assertThat(index.getStats().get("deltaResidentVectorsCapacity"))
              .as("precondition: the explicit budget is the one in force")
              .isEqualTo((long) BUDGET);

          assertThat(index.getStats().get("deltaResidentVectors"))
              .as("the buffer's heap footprint stops at the budget even though it keeps accepting entries "
                  + "(issue #7357)")
              .isEqualTo((long) BUDGET);

          // The assertion the bound is actually for. Every one of these rows is served ONLY by the delta scan -
          // there is no graph yet - and all but the first BUDGET of them now carry no vector at all, so a scan that
          // could not read them back would return the wrong rows rather than fail.
          for (final int probe : new int[] { 3, BUDGET, BUDGET + 1, RECORDS / 2, RECORDS - 1 }) {
            final List<Pair<RID, Float>> hits = search(db, embedding(probe), 1);
            assertThat(hits)
                .as("a search for the exact vector of record %d must find it", probe)
                .isNotEmpty();
            assertThat(idOf(db, hits.getFirst().getFirst()))
                .as("record %d is the nearest neighbour of its own vector, payload on the heap or not", probe)
                .isEqualTo(probe);
          }
        } finally {
          db.drop();
        }
      }
    }

    /**
     * The opt-out. {@code deltaCacheSize = -1} is the behaviour before this fix, kept reachable for an index whose
     * ingest is bounded by something else and that would rather not pay a page read per scored entry.
     */
    @Test
    void aNegativeBudgetKeepsEveryPayloadOnTheHeap() {
      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.create();
        try {
          createSchema(db, -1);
          insert(db, 0, RECORDS);

          final LSMVectorIndex index = vectorIndex(db);
          assertThat(index.getStats().get("deltaVectorsCount")).isEqualTo((long) RECORDS);
          assertThat(index.getStats().get("deltaResidentVectors"))
              .as("an explicitly negative budget declines nothing")
              .isEqualTo((long) RECORDS);
        } finally {
          db.drop();
        }
      }
    }

    /**
     * Deleting the records the buffer holds must give their heap budget back, or an index that churns would decline
     * payloads for room it has.
     */
    @Test
    void deletingBufferedRecordsGivesTheirBudgetBack() {
      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.create();
        try {
          createSchema(db, BUDGET);
          insert(db, 0, RECORDS);

          final LSMVectorIndex index = vectorIndex(db);
          assertThat(index.getStats().get("deltaResidentVectors")).isEqualTo((long) BUDGET);

          // Every row, not a prefix of them: which entries ended up holding a payload follows the order the
          // transaction applies its index writes in, which is not the order the documents were created in.
          db.transaction(() -> db.command("sql", "DELETE FROM Doc"));

          assertThat(index.getStats().get("deltaVectorsCount"))
              .as("precondition: the deletions really did empty the buffer")
              .isZero();
          assertThat(index.getStats().get("deltaResidentVectors"))
              .as("the payloads of the deleted rows are no longer charged to the buffer")
              .isZero();
        } finally {
          db.drop();
        }
      }
    }

    /**
     * The re-queue path, which is where the bound was first walked around (PR #7360 review).
     * <p>
     * {@code readDeltaEntriesFor()} builds delta entries carrying full payloads for the vectors a stale persisted
     * prefix does not cover, and hands them to the buffer. Those entries never write anything - they are read back
     * off the pages - so they did not pass through the write path's gate, and the budget did not apply to them. The
     * quantity involved is the worst possible one to exempt: the gap is "everything written since the persisted
     * graph was built", which for a session reopened after an interrupted load is the entire load. That is precisely
     * run 1 of the report, the one that OOM'd and left the database fenced for recovery.
     * <p>
     * The fix moves the budget to the door every entry goes through, so this pins the outcome rather than the
     * mechanism: after a reuse re-queues a gap four times the budget, the buffer holds all of it and the heap holds
     * only what the budget allows.
     */
    @Test
    @Tag("vector")
    void aReQueuedGapHonoursTheSameBudgetAsAWrite() {
      buildStalePersistedGraphFixture();

      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.open();
        try {
          db.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_DELTA_CACHE_SIZE, BUDGET);
          // The reuse only folds its gap into a rebuild once the gap crosses the ratio-scaled threshold; disabling
          // the scaling leaves the absolute floor, which is what this fixture is sized against (issue #7183).
          db.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_REBUILD_GRAPH_RATIO, 0f);

          final LSMVectorIndex index = vectorIndex(db);
          assertThat(index.getStats().get("deltaVectorsCount"))
              .as("precondition: this session has buffered nothing of its own")
              .isZero();

          // The rebuild the reuse dispatches would drain the buffer out from under the assertions, so the sole
          // JVM-wide rebuild permit is held for the observation window. No search path takes a permit, so the query
          // below still runs.
          LSMVectorIndex.acquireAllRebuildPermitsForTest();
          try {
            // The search is what triggers the reuse, and with it the gap re-queue this test is about.
            index.findNeighborsFromVector(embedding(0), 5);

            assertThat(index.getStats().get("stalePrefixGraphReuses"))
                .as("precondition: the reuse must actually have run, or nothing re-queued a gap and this test "
                    + "asserts nothing")
                .isEqualTo(1L);
            assertThat(PendingDeltaVectors.of(index))
                .as("precondition: the whole gap is buffered - the bound drops payloads, never entries")
                .isEqualTo((long) GAP_VECTORS);

            assertThat(index.getStats().get("deltaResidentVectors"))
                .as("a re-queued gap is charged the same heap budget as a write: %d entries, at most %d payloads",
                    GAP_VECTORS, BUDGET)
                .isLessThanOrEqualTo((long) BUDGET);
          } finally {
            LSMVectorIndex.releaseAllRebuildPermitsForTest();
          }
        } finally {
          db.drop();
        }
      }
    }

    /**
     * Session 1 builds and persists a graph over {@value #PERSISTED_VECTORS} records and closes cleanly; session 2
     * writes {@value #GAP_VECTORS} more and is killed rather than closed, so the persisted graph and its manifest
     * still describe only the first batch while the live vector set has moved on. The same fixture shape
     * {@code VectorGraphStalenessTest.WriteBeforeSearchPrefixReuse} uses, at a size that keeps this test cheap.
     */
    private void buildStalePersistedGraphFixture() {
      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.create();
        try {
          createSchema(db, -1);
          insert(db, 0, PERSISTED_VECTORS);
          vectorIndex(db).buildVectorGraphNow();
          assertThat(vectorIndex(db).getStats().get("graphState"))
              .as("precondition: the graph must be built and IMMUTABLE before the close that persists it")
              .isEqualTo(1L); // GraphState.IMMUTABLE
        } finally {
          if (db.isOpen())
            db.close();
        }
      }

      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.open();
        try {
          insert(db, PERSISTED_VECTORS, PERSISTED_VECTORS + GAP_VECTORS);
          ((DatabaseInternal) db).kill();
          db.close();
        } finally {
          if (db.isOpen())
            db.close();
        }
      }
    }

    /**
     * The orphan re-queue, the third producer of buffered payloads (PR #7360 review).
     * <p>
     * A build re-queues the vectors of every node it left unreachable, carrying their payloads, and that append used
     * to go around the budget. It is the worst of the three to leave uncapped, because a Vamana build orphans a
     * FRESH set each time and the count does not converge downwards (issue #7190), so a corpus that keeps
     * re-triggering rebuilds would keep adding full payloads to a buffer nothing else is allowed to grow.
     * <p>
     * Driven through {@code requeueIntoDeltaBufferForTest()}, which puts the index into exactly that state, because
     * whether a build orphans a node is decided between the data and JVector's diversity heuristic and cannot be
     * demanded from the public API - which is why that hook exists at all. What is asserted is the property the fix
     * establishes: a re-queue is charged the budget like any other arrival, whoever produced it.
     */
    @Test
    @Tag("vector")
    void aReQueuedOrphanHonoursTheSameBudgetAsAWrite() {
      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.create();
        try {
          createSchema(db, ORPHAN_BUDGET);
          insert(db, 0, ORPHAN_RECORDS);

          final LSMVectorIndex index = vectorIndex(db);
          index.buildVectorGraphNow();

          // The build itself may orphan nodes and re-queue them, payloads included (issues #7190, #8200): those are the
          // baseline the forced re-queue below adds to, not a sign the build left writes behind.
          final Map<String, Long> built = index.getStats();
          final long buildOrphans = built.get("unreachableGraphNodes");
          assertThat(PendingDeltaVectors.of(built))
              .as("precondition: the build drained the buffer, so what follows is only the re-queue")
              .isZero();
          assertThat(built.get("deltaResidentVectors"))
              .as("precondition: and gave every payload back but those of the nodes it orphaned")
              .isLessThanOrEqualTo(buildOrphans);

          int requeued = 0;
          for (int id = 0; id < ORPHAN_RECORDS && requeued < ORPHAN_BUDGET * 3; id++) {
            if (index.requeueIntoDeltaBufferForTest(ridOf(db, id)) >= 0)
              requeued++;
          }

          assertThat(requeued)
              .as("precondition: enough orphans re-queued to exceed the budget of %d", ORPHAN_BUDGET)
              .isGreaterThan(ORPHAN_BUDGET);
          assertThat(index.getStats().get("deltaVectorsCount"))
              .as("every re-queued orphan is buffered - the bound drops payloads, never entries")
              .isEqualTo(buildOrphans + requeued);
          assertThat(index.getStats().get("deltaResidentVectors"))
              .as("but the heap holds at most the budget, the same as for a write (issue #7357)")
              .isLessThanOrEqualTo((long) ORPHAN_BUDGET);
        } finally {
          db.drop();
        }
      }
    }

    /**
     * The auto-sized branch, which is what every installation that never touches either new setting gets, and the
     * only one of the three whose answer a test cannot otherwise predict (PR #7360 review).
     * <p>
     * The heap figures are pinned rather than read off the live JVM - the same seam
     * {@code VectorHeapBudget.buildCacheBudgetBytes(percent, maxHeap, availableHeap)} already provides for the
     * graph-build cache - because a budget derived from whatever a CI runner happens to have free is not an
     * assertion about anything. Three readings, one per arm of the arithmetic:
     * <ul>
     *   <li>a roomy heap, where the ceiling percentage is what binds;</li>
     *   <li>a heap an online rebuild has nearly filled, where the 90%-of-available cap binds instead and the buffer
     *       is told to stop growing - which is exactly when it must be (issue #6503's lesson applied here);</li>
     *   <li>the reporter's own shape, where the answer has to be far below the corpus or the bound does nothing.</li>
     * </ul>
     */
    @Test
    void theAutoSizedBudgetIsAShareOfTheCeilingCappedByWhatIsFree() {
      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.create();
        try {
          createSchema(db, 0); // 0 = auto-size, the default
          final LSMVectorIndex index = vectorIndex(db);

          final long bytesPerVector = VectorHeapBudget.bytesPerCachedVector(DIMENSIONS);
          assertThat(bytesPerVector)
              .as("precondition: the per-entry cost the budget divides by")
              .isEqualTo((long) DIMENSIONS * Float.BYTES + 64);

          final long maxHeap = 16L * 1024 * 1024 * 1024;

          // 1. Roomy heap: 10% of the 16 GB ceiling, since 90% of 12 GB free is far more than that.
          final long roomy = index.computeDeltaPayloadCapacity(maxHeap, 12L * 1024 * 1024 * 1024);
          assertThat(roomy)
              .as("the ceiling share binds while there is headroom")
              .isEqualTo((int) (maxHeap / 100 * 10 / bytesPerVector));

          // 2. A rebuild holding the old graph has left 512 MB. The available cap has to bind now, or the buffer
          // would keep growing into heap the rebuild is about to need.
          final long tight = index.computeDeltaPayloadCapacity(maxHeap, 512L * 1024 * 1024);
          assertThat(tight)
              .as("90%% of what is actually free binds when the heap is tight, not the ceiling share")
              .isEqualTo((int) (512L * 1024 * 1024 / 100 * 90 / bytesPerVector));
          assertThat(tight)
              .as("and it is a real reduction, not a formality")
              .isLessThan(roomy);

          // 3. The reporter's shape. At 768 dimensions the whole 4.2M-record buffer is 12.9 GB, so an auto-sized
          // budget on a 16 GB heap has to come out far below that or the bound accomplishes nothing.
          final long reporterBytesPerVector = VectorHeapBudget.bytesPerCachedVector(768);
          final long reporterCapacity = maxHeap / 100 * 10 / reporterBytesPerVector;
          assertThat(reporterCapacity * reporterBytesPerVector)
              .as("the default budget on the reported heap is a fraction of the 12.9 GB the buffer used to hold")
              .isLessThan(2L * 1024 * 1024 * 1024);
          assertThat(reporterCapacity)
              .as("and it still keeps a useful number of payloads resident")
              .isGreaterThan(100_000L);
        } finally {
          db.drop();
        }
      }
    }

    private static void createSchema(final Database db, final int deltaCacheSize) {
      db.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_DELTA_CACHE_SIZE, deltaCacheSize);
      // No rebuild may drain the buffer while the assertions look at it: the inactivity timer is off and the
      // mutation threshold is out of reach.
      db.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, 0);
      db.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, Integer.MAX_VALUE);

      db.transaction(() -> {
        if (db.getSchema().existsType("Doc"))
          return;
        final var type = db.getSchema().createDocumentType("Doc");
        type.createProperty("id", Type.INTEGER);
        type.createProperty("vector", Type.ARRAY_OF_FLOATS);
        db.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
            + ", \"similarity\": \"EUCLIDEAN\" }");
      });
    }

    private static void insert(final Database db, final int fromInclusive, final int toExclusive) {
      db.begin();
      for (int i = fromInclusive; i < toExclusive; i++)
        db.newDocument("Doc").set("id", i).set("vector", embedding(i)).save();
      db.commit();
    }

    private static List<Pair<RID, Float>> search(final Database db, final float[] query, final int k) {
      final List<Pair<RID, Float>> hits = new ArrayList<>();
      db.transaction(() -> hits.addAll(vectorIndex(db).findNeighborsFromVector(query, k)));
      return hits;
    }

    private static RID ridOf(final Database db, final int id) {
      final RID[] rid = new RID[1];
      db.transaction(() -> {
        try (final var rs = db.query("sql", "SELECT @rid FROM Doc WHERE id = ?", id)) {
          rid[0] = rs.hasNext() ? rs.next().getProperty("@rid") : null;
        }
      });
      return rid[0];
    }

    private static int idOf(final Database db, final RID rid) {
      return db.lookupByRID(rid, true).asDocument().getInteger("id");
    }

    /** Deterministic per-id embedding, so a fixture is reproducible run to run. */
    private static float[] embedding(final int id) {
      final Random random = new Random(0x7357L * 31 + id);
      final float[] vector = new float[DIMENSIONS];
      for (int d = 0; d < DIMENSIONS; d++)
        vector[d] = random.nextFloat();
      return vector;
    }

    private static LSMVectorIndex vectorIndex(final Database db) {
      return (LSMVectorIndex) db.getSchema().getType("Doc")
          .getPolymorphicIndexByProperties("vector").getIndexesOnBuckets()[0];
    }
  }

  /**
   * Regression for #7742: a rebuild absorbs the writes that land while it runs.
   * <p>
   * Regression test for issue #7742: a graph rebuild running alongside ordinary ingestion stopped absorbing the
   * vectors that landed while it ran, and the delta buffer they sit in was then never drained.
   * <p>
   * Two independent defects on {@code buildGraphFromScratchExclusively}, both of which need a writer racing the
   * build to show up at all - which is why the symptom was machine-dependent and looked like a flaky wait:
   * <ol>
   *   <li>the build indexed the delta buffer by walking it with an iterator and no lock, while {@code put()} appends
   *       to that very list under the write lock. A concurrent insert therefore threw
   *       {@code ConcurrentModificationException} out of the build. The async rebuild catches it, logs it, and
   *       leaves {@code completed} false - so it neither chains into the next rebuild nor pays off the mutations it
   *       had snapshotted, and an index under sustained ingestion stops absorbing anything at all;</li>
   *   <li>the build read its two snapshots - the next vector id, which decides what the buffer keeps, and the
   *       mutation counter, which decides what the rebuild pays off - as two separate unlocked reads. A writer
   *       landing between them makes the pair describe different instants: the entry is kept in the buffer AND
   *       counted as paid for. The chained rebuild compares that counter with its threshold and declines, and
   *       {@code cancelInactivityRebuildTimer()} cancels the one mechanism that would still have picked it up. The
   *       vector stays buffered for the rest of the session and every query scans it.</li>
   * </ol>
   * The invariant both fixes restore is the one asserted below: <b>a pending vector is always accounted for</b> -
   * anything still in the buffer that the build did not put in the graph is matched by a mutation the counter still
   * owes, so something is always coming for it.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  @Tag("vector")
  class RebuildAbsorbsWritesThatRaceIt {
    private static final String DB_PATH    = "./target/databases/RebuildAbsorbsWritesThatRaceIt";
    private static final int    DIMENSIONS = 16;
    /**
     * Rows seeded before the first build, none of which the graph has absorbed yet - so they are all still in the
     * delta buffer when the build below walks it. That walk is the window a concurrent insert has to land in, and
     * its width IS the buffer size: with a few hundred rows the race is won maybe one run in three, which is a test
     * that reports the defect as a flake. With this many it is won every time.
     */
    private static final int    SEED_ROWS   = 15_000;
    private static final int    ROUNDS      = 3;
    /**
     * Total rows the racing writer inserts across the whole test.
     * <p>
     * Capped, but NOT throttled. What has to happen for either defect to show is a write landing inside a build's
     * own window, and that window is milliseconds on an index this size - a writer that pauses between batches
     * mostly misses it and the test passes on the broken engine. The cap is what keeps the run bounded instead: the
     * rebuild is O(index size), so a writer left running for the whole test would make each round dearer than the
     * last.
     */
    private static final int    WRITER_ROWS = 4_000;

    @AfterEach
    void cleanUp() {
      FileUtils.deleteRecursively(new File(DB_PATH));
    }

    @Test
    void aRebuildRacingAnIngestNeitherDiesNorStrandsAPendingVector() throws Exception {
      FileUtils.deleteRecursively(new File(DB_PATH));

      try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
        final Database db = factory.create();
        final AtomicBoolean stop = new AtomicBoolean();
        final AtomicReference<Throwable> writerFailure = new AtomicReference<>();
        Thread writer = null;
        try {
          // The inactivity timer is off so the ONLY thing that can absorb a pending vector is a rebuild. That is
          // what makes the accounting invariant below observable: with the timer on, a stranded vector is picked up
          // eventually and the defect hides.
          db.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 100);
          db.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, 0);
          db.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_REBUILD_GRAPH_RATIO, 0f);

          db.transaction(() -> {
            final DocumentType t = db.getSchema().createDocumentType("Doc");
            t.createProperty("embedding", Type.ARRAY_OF_FLOATS);
          });
          db.command("sql", "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA "
              + "{ \"dimensions\": " + DIMENSIONS + ", \"similarity\": \"COSINE\" }");

          final LSMVectorIndex idx = (LSMVectorIndex) ((TypeIndex) db.getSchema().getIndexByName("Doc[embedding]"))
              .getIndexesOnBuckets()[0];

          final Random seedRandom = new Random(7742);
          db.transaction(() -> {
            for (int i = 0; i < SEED_ROWS; i++)
              db.newDocument("Doc").set("embedding", randomVector(seedRandom)).save();
          });

          writer = new Thread(() -> {
            final Random random = new Random(42);
            try {
              for (int written = 0; written < WRITER_ROWS && !stop.get(); written += 20)
                db.transaction(() -> {
                  for (int i = 0; i < 20; i++)
                    db.newDocument("Doc").set("embedding", randomVector(random)).save();
                });
            } catch (final Throwable t) {
              writerFailure.set(t);
            }
          }, "issue-7742-writer");
          writer.setDaemon(true);
          writer.start();

          for (int round = 0; round < ROUNDS; round++) {
            // Before the fix this threw ConcurrentModificationException out of the build, usually on the first round.
            idx.buildVectorGraphNow();

            final Map<String, Long> stats = idx.getStats();
            // Vectors this build orphaned are re-queued into the buffer on purpose (issue #7190) and are already in
            // the graph, so they are not pending work and no counter owes anything for them.
            final long pending = PendingDeltaVectors.of(stats);
            if (pending > 0)
              assertThat(stats.get("mutationsSinceRebuild"))
                  .as("round %d left %d vector(s) buffered: the mutation counter must still owe them, or nothing "
                      + "will ever come to absorb them", round, pending)
                  .isGreaterThan(0L);
          }

          stop.set(true);
          writer.join(30_000);
          assertThat(writer.isAlive())
              .as("the writer must have stopped, or the final build below still races it and its assertion would "
                  + "fail for a reason its message denies")
              .isFalse();
          assertThat(writerFailure.get()).as("the ingest must survive every rebuild that ran alongside it").isNull();

          // One last build with nothing writing: it must absorb everything that is genuinely pending.
          idx.buildVectorGraphNow();
          final Map<String, Long> stats = idx.getStats();
          assertThat(PendingDeltaVectors.of(stats))
              .as("a build with no writer racing it must leave nothing pending behind")
              .isZero();
        } finally {
          stop.set(true);
          if (writer != null)
            writer.join(30_000);
          db.drop();
        }
      }
    }

    private static float[] randomVector(final Random random) {
      final float[] v = new float[DIMENSIONS];
      for (int i = 0; i < DIMENSIONS; i++)
        v[i] = random.nextFloat() * 2 - 1;
      return v;
    }
  }
}
