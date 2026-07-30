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
package com.arcadedb.index.sparsevector;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.sparsevector.SegmentFormat.WeightQuantization;
import com.arcadedb.schema.LocalSchema;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Correctness of the intra-query parallel top-K introduced in issue #4085: a query may be split into
 * RID ranges scored independently and merged, and the merged answer has to be the one the serial
 * traversal would have produced - <b>the same list, not merely an equally good one</b>.
 * <p>
 * That distinction is the whole point of the test. Splitting is exact by construction for distinct
 * scores: ranges partition the RID space, so a document is scored once and its score does not depend
 * on which range held it - to within float rounding, since the order a document's terms are summed
 * in follows the pruning split, which the two shapes reach differently (see
 * {@link #assertSameResult}). Ties are where a partitioned traversal can legitimately diverge, because
 * each range keeps its own top-K and the two shapes see candidates in different orders. Both the
 * per-range heap and the final merge therefore rank on "score descending, then RID ascending", and
 * the tie test below fails without that.
 * <p>
 * <b>Where the claim stops.</b> Rounding and the tie-break are not independent, so "identical list"
 * holds for distinct scores and for ties that survive rounding intact, not universally: two
 * documents that tie exactly in one shape can sit an ulp apart in the other, and at the k boundary
 * an ulp decides which is kept. The RID tie-break cannot reach that case, because it only orders
 * scores that are still equal after rounding. What holds without qualification is that the split
 * never returns a worse answer - same count, rank-for-rank indistinguishable scores, nothing below
 * the serial k-th - which is what
 * {@link #aPlateauOfNearTiesAtTheBoundaryStillReturnsAnEquallyGoodResult} pins.
 * <p>
 * <b>How often that corner bites in practice: not once, so far.</b> Measured over 1000 real Big-ANN
 * SPLADE queries at INT8, comparing returned id lists against the serial arm at both the adaptive
 * default and a forced 8-way split, the sets, the order and the counts came back identical every
 * time. The summation-order effect is real and visible in the arithmetic - the largest rank-for-rank
 * score difference was 9.0e-06 on scores of order 20, about 0.45 ppm - but that is some four orders
 * of magnitude short of flipping a decision at the k boundary on that weight distribution. Which is
 * why the weakened guarantee is still the one to state and this test is still what pins it: it
 * constructs the case deliberately instead of waiting to meet it, and nothing should be read into
 * the field result beyond "do not expect to see this on real data".
 * <p>
 * The other thing asserted here is that a fan-out never nests. A scoring-pool worker that split its
 * own query again would fill the pool with tasks waiting on tasks that nothing is left to run - a
 * deadlock, not a slowdown, and one the caller-runs rejection policy does not catch because the
 * bounded queue accepts long before it rejects.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ParallelRangeTopKTest extends TestHelper {

  private static final int DIMS  = 24;
  private static final int DOCS  = 30_000;
  /** Enough postings on the widest dim to cut into ranges, without paying to build a big corpus. */
  private static final int SMALL_DOCS = 6_000;
  private static final int K     = 10;
  /**
   * Ceiling on how many blocking tasks a saturation loop will submit. Purely a runaway guard: the
   * loop stops when the queue reports full, and this stops it looping forever if that never happens.
   */
  private static final int SATURATION_SUBMIT_LIMIT = 10_000;
  /**
   * How long a saturation task waits before giving up. The tests always release the latch in a
   * finally, so this only matters when something has already gone wrong - and then it turns a hung
   * CI job into a failed test, which is the difference between a diagnosis and an hour of nothing.
   */
  private static final int SATURATION_RELEASE_TIMEOUT_SECONDS = 60;
  /** Wide enough to cover float summation-order differences, far below any real score gap. */
  private static final float TIE_EPSILON = 1e-4f;

  @Test
  void rangePartitionedScoringReproducesTheSerialResult() throws Exception {
    final Map<RID, Map<Integer, Float>> corpus = buildCorpus(DOCS, 5388L, false);

    inTx(() -> {
      final PaginatedSegmentReader reader = buildSegment("seg-4085-ranges", 1L, corpus);
      final int[] queryDims = new int[DIMS];
      final float[] queryWeights = new float[DIMS];
      for (int d = 0; d < DIMS; d++) {
        queryDims[d] = d;
        queryWeights[d] = 1.0f;
      }

      final List<RidScore> serial = scoreRange(reader, queryDims, queryWeights, null, null);

      // Every split count, including ones that do not divide the corpus evenly.
      for (final int partitions : new int[] { 2, 3, 4, 8, 16 }) {
        final RID[] boundaries = evenBoundaries(reader, partitions);
        final List<List<RidScore>> ranges = new ArrayList<>();
        for (int i = 0; i <= boundaries.length; i++) {
          final RID start = i == 0 ? null : boundaries[i - 1];
          final RID end = i == boundaries.length ? null : boundaries[i];
          ranges.add(scoreRange(reader, queryDims, queryWeights, start, end));
        }
        assertSameResult(BmwScorer.mergeRanges(ranges, K), serial, "partitions=" + partitions);
      }
    });
  }

  @Test
  void tiedScoresResolveIdenticallyWhetherOrNotTheQueryIsSplit() throws Exception {
    // Every document carries the same weight on the same dims, so the whole corpus ties on score and
    // the top-K is decided purely by the tie-break. A partitioned run that ranked ties differently
    // would return a different set of RIDs here, not just a different order.
    final Map<RID, Map<Integer, Float>> corpus = buildCorpus(DOCS, 4085L, true);

    inTx(() -> {
      final PaginatedSegmentReader reader = buildSegment("seg-4085-ties", 1L, corpus);
      final int[] queryDims = new int[DIMS];
      final float[] queryWeights = new float[DIMS];
      for (int d = 0; d < DIMS; d++) {
        queryDims[d] = d;
        queryWeights[d] = 1.0f;
      }

      final List<RidScore> serial = scoreRange(reader, queryDims, queryWeights, null, null);
      // Ties must be won by the lowest RIDs, and the result ordered by RID ascending.
      RID previous = null;
      for (final RidScore rs : serial) {
        if (previous != null)
          assertThat(SparseSegmentBuilder.compareRid(previous, rs.rid())).isNegative();
        previous = rs.rid();
      }

      for (final int partitions : new int[] { 2, 4, 8 }) {
        final RID[] boundaries = evenBoundaries(reader, partitions);
        final List<List<RidScore>> ranges = new ArrayList<>();
        for (int i = 0; i <= boundaries.length; i++) {
          final RID start = i == 0 ? null : boundaries[i - 1];
          final RID end = i == boundaries.length ? null : boundaries[i];
          ranges.add(scoreRange(reader, queryDims, queryWeights, start, end));
        }
        assertSameResult(BmwScorer.mergeRanges(ranges, K), serial, "tied partitions=" + partitions);
      }
    });
  }

  /**
   * The one case where the split may legitimately keep a different document than the serial scan,
   * and therefore the case that says what the equivalence claim actually is.
   * <p>
   * A plateau of documents with mathematically equal scores straddles the k-th place, built from
   * weights that are not exactly representable so the order MaxScore sums a document's terms in can
   * move its total by an ulp. That order follows the essential/non-essential split, which a range
   * reaches at a different watermark than the whole scan, so two documents that tie exactly in one
   * shape can sit an ulp apart in the other - and at the k boundary an ulp decides which is kept.
   * The RID tie-break cannot save this: it only breaks ties between scores that are still equal
   * after rounding.
   * <p>
   * So the guarantee is not "identical RIDs in all cases". It is that the split never returns a
   * <i>worse</i> answer: the same number of documents, rank for rank indistinguishable in score,
   * and never one that ranks below the serial k-th. That is what is asserted here. The existing
   * tie test uses exactly-representable weights, which deliberately removes the rounding and pins
   * the tie-break on its own; this one puts the rounding back.
   */
  @Test
  void aPlateauOfNearTiesAtTheBoundaryStillReturnsAnEquallyGoodResult() throws Exception {
    final int previousPartitions = GlobalConfiguration.SPARSE_VECTOR_SCORING_MAX_PARTITIONS.getValueAsInteger();
    final long previousMin = GlobalConfiguration.SPARSE_VECTOR_SCORING_MIN_POSTINGS_FOR_PARTITIONING.getValueAsLong();
    try {
      GlobalConfiguration.SPARSE_VECTOR_SCORING_MIN_POSTINGS_FOR_PARTITIONING.setValue(1L);

      // Weights chosen so no partial sum is exact in binary floating point.
      final float[] awkward = { 0.1f, 0.3f, 0.7f, 0.05f, 0.15f, 0.35f, 0.45f, 0.55f, 0.65f, 0.85f, 0.95f, 0.25f };
      final TreeMap<RID, Map<Integer, Float>> corpus = new TreeMap<>(SparseSegmentBuilder::compareRid);
      // Background: enough postings for the split to engage, all scoring well below the plateau.
      for (int i = 0; i < SMALL_DOCS; i++) {
        final TreeMap<Integer, Float> doc = new TreeMap<>();
        for (int d = 0; d < 4; d++)
          doc.put(d, 0.01f);
        corpus.put(new RID(0, 1L + i), doc);
      }
      // The plateau: many more documents than k, every one carrying the identical heavy weights, so
      // their true scores are equal and only rounding can separate them.
      for (int i = 0; i < K * 5; i++) {
        final TreeMap<Integer, Float> doc = new TreeMap<>();
        for (int d = 0; d < awkward.length; d++)
          doc.put(d, awkward[d]);
        corpus.put(new RID(0, 1L + (long) SMALL_DOCS + i * 7L), doc);
      }

      final int[] queryDims = new int[awkward.length];
      final float[] queryWeights = new float[awkward.length];
      for (int d = 0; d < awkward.length; d++) {
        queryDims[d] = d;
        queryWeights[d] = 1.0f;
      }

      try (final PaginatedSparseVectorEngine engine = new PaginatedSparseVectorEngine((DatabaseInternal) database,
          "idx4085plateau", SegmentParameters.builder().weightQuantization(WeightQuantization.FP32).build())) {
        database.transaction(() -> {
          for (final var doc : corpus.entrySet())
            for (final var dw : doc.getValue().entrySet())
              engine.put(dw.getKey(), doc.getKey(), dw.getValue());
          engine.flush();
        });

        GlobalConfiguration.SPARSE_VECTOR_SCORING_MAX_PARTITIONS.setValue(1);
        final List<RidScore> serial = engine.topK(queryDims, queryWeights, K);
        assertThat(serial).hasSize(K);

        for (final int partitions : new int[] { 2, 4, 8 }) {
          GlobalConfiguration.SPARSE_VECTOR_SCORING_MAX_PARTITIONS.setValue(partitions);
          final List<RidScore> split = engine.topK(queryDims, queryWeights, K);

          assertThat(split).as("split=%d must return as many documents", partitions).hasSameSizeAs(serial);
          final float worstSerial = serial.getLast().score();
          for (int i = 0; i < serial.size(); i++) {
            assertThat(split.get(i).score()).as("split=%d rank %d must be indistinguishable in score", partitions, i)
                .isCloseTo(serial.get(i).score(), Offset.offset(TIE_EPSILON));
            assertThat(split.get(i).score()).as("split=%d rank %d must not rank below the serial k-th", partitions, i)
                .isGreaterThanOrEqualTo(worstSerial - TIE_EPSILON);
          }
        }
      }
    } finally {
      GlobalConfiguration.SPARSE_VECTOR_SCORING_MAX_PARTITIONS.setValue(previousPartitions);
      GlobalConfiguration.SPARSE_VECTOR_SCORING_MIN_POSTINGS_FOR_PARTITIONING.setValue(previousMin);
    }
  }

  @Test
  void emptyAndSingletonRangesAreHandled() throws Exception {
    final Map<RID, Map<Integer, Float>> corpus = buildCorpus(2_000, 99L, false);

    inTx(() -> {
      final PaginatedSegmentReader reader = buildSegment("seg-4085-edges", 1L, corpus);
      final int[] queryDims = { 0, 1, 2 };
      final float[] queryWeights = { 1.0f, 1.0f, 1.0f };

      // A range entirely past the last posting yields nothing rather than failing.
      assertThat(scoreRange(reader, queryDims, queryWeights, new RID(0, 9_000_000L), null)).isEmpty();
      // An empty half-open range [x, x) yields nothing.
      final RID x = new RID(0, 101L);
      assertThat(scoreRange(reader, queryDims, queryWeights, x, x)).isEmpty();
      // A range before the first posting still returns the head of the corpus.
      assertThat(scoreRange(reader, queryDims, queryWeights, new RID(0, 0L), null)).isNotEmpty();
    });
  }

  @Test
  void theEngineSplitsAQueryAndReturnsWhatTheSerialPathWould() throws Exception {
    final int previous = GlobalConfiguration.SPARSE_VECTOR_SCORING_MAX_PARTITIONS.getValueAsInteger();
    final long previousMin = GlobalConfiguration.SPARSE_VECTOR_SCORING_MIN_POSTINGS_FOR_PARTITIONING.getValueAsLong();
    try {
      final Map<RID, Map<Integer, Float>> corpus = buildCorpus(DOCS, 777L, false);
      final int[] queryDims = new int[DIMS];
      final float[] queryWeights = new float[DIMS];
      for (int d = 0; d < DIMS; d++) {
        queryDims[d] = d;
        queryWeights[d] = 1.0f;
      }

      try (final PaginatedSparseVectorEngine engine = new PaginatedSparseVectorEngine((DatabaseInternal) database,
          "idx4085", SegmentParameters.builder().weightQuantization(WeightQuantization.FP32).build())) {
        database.transaction(() -> {
          for (final var doc : corpus.entrySet())
            for (final var dw : doc.getValue().entrySet())
              engine.put(dw.getKey(), doc.getKey(), dw.getValue());
          engine.flush();
        });

        GlobalConfiguration.SPARSE_VECTOR_SCORING_MAX_PARTITIONS.setValue(1);
        final List<RidScore> serial = engine.topK(queryDims, queryWeights, K);
        assertThat(engine.partitionedQueryCount()).isZero();

        // Below the work threshold the query stays on the caller thread even with splitting enabled.
        GlobalConfiguration.SPARSE_VECTOR_SCORING_MAX_PARTITIONS.setValue(4);
        GlobalConfiguration.SPARSE_VECTOR_SCORING_MIN_POSTINGS_FOR_PARTITIONING.setValue(Long.MAX_VALUE);
        assertSameResult(engine.topK(queryDims, queryWeights, K), serial, "below work threshold");
        assertThat(engine.partitionedQueryCount()).isZero();

        GlobalConfiguration.SPARSE_VECTOR_SCORING_MIN_POSTINGS_FOR_PARTITIONING.setValue(1L);
        assertSameResult(engine.topK(queryDims, queryWeights, K), serial, "engine 4-way split");
        assertThat(engine.partitionedQueryCount()).isEqualTo(1L);

        // Repeated runs must be stable, and a larger k must hold too.
        assertSameResult(engine.topK(queryDims, queryWeights, K), serial, "engine 4-way split, repeat");
        GlobalConfiguration.SPARSE_VECTOR_SCORING_MAX_PARTITIONS.setValue(1);
        final List<RidScore> serial50 = engine.topK(queryDims, queryWeights, 50);
        GlobalConfiguration.SPARSE_VECTOR_SCORING_MAX_PARTITIONS.setValue(8);
        assertSameResult(engine.topK(queryDims, queryWeights, 50), serial50, "engine 8-way split, k=50");
      }
    } finally {
      GlobalConfiguration.SPARSE_VECTOR_SCORING_MAX_PARTITIONS.setValue(previous);
      GlobalConfiguration.SPARSE_VECTOR_SCORING_MIN_POSTINGS_FOR_PARTITIONING.setValue(previousMin);
    }
  }

  @Test
  void aQueryAlreadyRunningOnTheScoringPoolDoesNotSplitAgain() throws Exception {
    final int previous = GlobalConfiguration.SPARSE_VECTOR_SCORING_MAX_PARTITIONS.getValueAsInteger();
    final long previousMin = GlobalConfiguration.SPARSE_VECTOR_SCORING_MIN_POSTINGS_FOR_PARTITIONING.getValueAsLong();
    try {
      GlobalConfiguration.SPARSE_VECTOR_SCORING_MAX_PARTITIONS.setValue(8);
      GlobalConfiguration.SPARSE_VECTOR_SCORING_MIN_POSTINGS_FOR_PARTITIONING.setValue(1L);

      final Map<RID, Map<Integer, Float>> corpus = buildCorpus(SMALL_DOCS, 31L, false);
      final int[] queryDims = new int[DIMS];
      final float[] queryWeights = new float[DIMS];
      for (int d = 0; d < DIMS; d++) {
        queryDims[d] = d;
        queryWeights[d] = 1.0f;
      }

      try (final PaginatedSparseVectorEngine engine = new PaginatedSparseVectorEngine((DatabaseInternal) database,
          "idx4085nested", SegmentParameters.builder().weightQuantization(WeightQuantization.FP32).build())) {
        database.transaction(() -> {
          for (final var doc : corpus.entrySet())
            for (final var dw : doc.getValue().entrySet())
              engine.put(dw.getKey(), doc.getKey(), dw.getValue());
          engine.flush();
        });

        final List<RidScore> onCallerThread = engine.topK(queryDims, queryWeights, K);
        final long splitsAfterCallerRun = engine.partitionedQueryCount();
        assertThat(splitsAfterCallerRun).isEqualTo(1L);

        // Same query submitted the way the per-bucket fan-out submits it. It must complete - a nested
        // split would wedge the pool - and it must not have split again.
        final ExecutorService pool = SparseVectorScoringPool.getInstance().getExecutorService();
        final Future<List<RidScore>> f = pool.submit(() -> engine.topK(queryDims, queryWeights, K));
        assertSameResult(f.get(), onCallerThread, "submitted onto the scoring pool");
        assertThat(engine.partitionedQueryCount()).isEqualTo(splitsAfterCallerRun);
        assertThat(SparseVectorScoringPool.isPoolThread()).isFalse();
      }
    } finally {
      GlobalConfiguration.SPARSE_VECTOR_SCORING_MAX_PARTITIONS.setValue(previous);
      GlobalConfiguration.SPARSE_VECTOR_SCORING_MIN_POSTINGS_FOR_PARTITIONING.setValue(previousMin);
    }
  }

  @Test
  void claimedWorkersAreAlwaysHandedBack() throws Exception {
    final int previous = GlobalConfiguration.SPARSE_VECTOR_SCORING_MAX_PARTITIONS.getValueAsInteger();
    final long previousMin = GlobalConfiguration.SPARSE_VECTOR_SCORING_MIN_POSTINGS_FOR_PARTITIONING.getValueAsLong();
    try {
      GlobalConfiguration.SPARSE_VECTOR_SCORING_MAX_PARTITIONS.setValue(4);
      GlobalConfiguration.SPARSE_VECTOR_SCORING_MIN_POSTINGS_FOR_PARTITIONING.setValue(1L);

      final Map<RID, Map<Integer, Float>> corpus = buildCorpus(SMALL_DOCS, 5L, false);
      final int[] queryDims = new int[DIMS];
      final float[] queryWeights = new float[DIMS];
      for (int d = 0; d < DIMS; d++) {
        queryDims[d] = d;
        queryWeights[d] = 1.0f;
      }

      final SparseVectorScoringPool pool = SparseVectorScoringPool.getInstance();
      final int reservedBefore = pool.getReservedWorkers();

      try (final PaginatedSparseVectorEngine engine = new PaginatedSparseVectorEngine((DatabaseInternal) database,
          "idx4085reserve", SegmentParameters.builder().weightQuantization(WeightQuantization.FP32).build())) {
        database.transaction(() -> {
          for (final var doc : corpus.entrySet())
            for (final var dw : doc.getValue().entrySet())
              engine.put(dw.getKey(), doc.getKey(), dw.getValue());
          engine.flush();
        });

        for (int i = 0; i < 5; i++)
          engine.topK(queryDims, queryWeights, K);
        assertThat(engine.partitionedQueryCount()).isEqualTo(5L);
      }

      // A leak here would be silent and permanent: every later query would see the pool as busier
      // than it is and quietly stop splitting, with nothing failing to point at it.
      assertThat(pool.getReservedWorkers()).as("claimed workers must be released after every query")
          .isEqualTo(reservedBefore);
    } finally {
      GlobalConfiguration.SPARSE_VECTOR_SCORING_MAX_PARTITIONS.setValue(previous);
      GlobalConfiguration.SPARSE_VECTOR_SCORING_MIN_POSTINGS_FOR_PARTITIONING.setValue(previousMin);
    }
  }

  @Test
  void aFullyClaimedPoolKeepsTheQueryOnTheCallerThread() throws Exception {
    final int previous = GlobalConfiguration.SPARSE_VECTOR_SCORING_MAX_PARTITIONS.getValueAsInteger();
    final long previousMin = GlobalConfiguration.SPARSE_VECTOR_SCORING_MIN_POSTINGS_FOR_PARTITIONING.getValueAsLong();
    final SparseVectorScoringPool pool = SparseVectorScoringPool.getInstance();
    int hogged = 0;
    try {
      GlobalConfiguration.SPARSE_VECTOR_SCORING_MAX_PARTITIONS.setValue(0);  // adaptive
      GlobalConfiguration.SPARSE_VECTOR_SCORING_MIN_POSTINGS_FOR_PARTITIONING.setValue(1L);

      final Map<RID, Map<Integer, Float>> corpus = buildCorpus(SMALL_DOCS, 6L, false);
      final int[] queryDims = new int[DIMS];
      final float[] queryWeights = new float[DIMS];
      for (int d = 0; d < DIMS; d++) {
        queryDims[d] = d;
        queryWeights[d] = 1.0f;
      }

      try (final PaginatedSparseVectorEngine engine = new PaginatedSparseVectorEngine((DatabaseInternal) database,
          "idx4085busy", SegmentParameters.builder().weightQuantization(WeightQuantization.FP32).build())) {
        database.transaction(() -> {
          for (final var doc : corpus.entrySet())
            for (final var dw : doc.getValue().entrySet())
              engine.put(dw.getKey(), doc.getKey(), dw.getValue());
          engine.flush();
        });

        final List<RidScore> whenFree = engine.topK(queryDims, queryWeights, K);
        final long splitWhenFree = engine.partitionedQueryCount();

        // Stand in for a server already busy with other queries: claim every worker.
        hogged = pool.tryReserveWorkers(pool.getMaxParallelism());
        assertThat(hogged).isGreaterThan(0);

        final List<RidScore> whenBusy = engine.topK(queryDims, queryWeights, K);
        assertThat(engine.partitionedQueryCount())
            .as("with no capacity left the query must not split").isEqualTo(splitWhenFree);
        assertSameResult(whenBusy, whenFree, "serial fallback under a fully claimed pool");
      }
    } finally {
      pool.releaseWorkers(hogged);
      GlobalConfiguration.SPARSE_VECTOR_SCORING_MAX_PARTITIONS.setValue(previous);
      GlobalConfiguration.SPARSE_VECTOR_SCORING_MIN_POSTINGS_FOR_PARTITIONING.setValue(previousMin);
    }
  }

  @Test
  void concurrentCallersSuppressTheSplitEvenWhileThePoolLooksIdle() {
    final SparseVectorScoringPool pool = SparseVectorScoringPool.getInstance();
    final int ceiling = pool.getMaxParallelism();
    assertThat(pool.getInFlightQueries()).isZero();

    // Nobody else querying: capacity is grantable.
    assertThat(pool.tryReserveWorkers(1)).as("an idle pool must grant a worker").isEqualTo(1);
    pool.releaseWorkers(1);

    // Enough concurrent callers to keep the pool's worth of threads busy on their own. The pool's
    // own counters still read idle at this instant - the callers are not its threads - which is
    // exactly the blind spot this gate covers: sizing the split from pool activity alone measured a
    // third of queries splitting at 16 concurrent clients, and cost throughput to do it.
    final int callers = ceiling / 2 + 1;
    for (int i = 0; i < callers; i++)
      pool.queryStarted();
    try {
      assertThat(pool.getInFlightQueries()).isEqualTo(callers);
      assertThat(pool.tryReserveWorkers(1))
          .as("with callers alone able to saturate the box, a query must not claim workers").isZero();
    } finally {
      for (int i = 0; i < callers; i++)
        pool.queryFinished();
    }

    assertThat(pool.getInFlightQueries()).isZero();
    assertThat(pool.tryReserveWorkers(1)).as("capacity must come back once the callers drain").isEqualTo(1);
    pool.releaseWorkers(1);
  }

  @Test
  void aRangeThatNeverGetsAWorkerFailsTheQueryOnTheDeadline() throws Exception {
    final int previousPartitions = GlobalConfiguration.SPARSE_VECTOR_SCORING_MAX_PARTITIONS.getValueAsInteger();
    final long previousMin = GlobalConfiguration.SPARSE_VECTOR_SCORING_MIN_POSTINGS_FOR_PARTITIONING.getValueAsLong();
    final int previousTimeout = GlobalConfiguration.SPARSE_VECTOR_SCORING_TIMEOUT_SECONDS.getValueAsInteger();
    final CountDownLatch release = new CountDownLatch(1);
    final CountDownLatch occupied = new CountDownLatch(1);
    final List<Future<?>> hogs = new ArrayList<>();
    try {
      // Explicit split, so the load gate does not veto it while the pool is deliberately jammed.
      GlobalConfiguration.SPARSE_VECTOR_SCORING_MAX_PARTITIONS.setValue(2);
      GlobalConfiguration.SPARSE_VECTOR_SCORING_MIN_POSTINGS_FOR_PARTITIONING.setValue(1L);
      GlobalConfiguration.SPARSE_VECTOR_SCORING_TIMEOUT_SECONDS.setValue(1);

      final Map<RID, Map<Integer, Float>> corpus = buildCorpus(SMALL_DOCS, 8L, false);
      final int[] queryDims = new int[DIMS];
      final float[] queryWeights = new float[DIMS];
      for (int d = 0; d < DIMS; d++) {
        queryDims[d] = d;
        queryWeights[d] = 1.0f;
      }

      try (final PaginatedSparseVectorEngine engine = new PaginatedSparseVectorEngine((DatabaseInternal) database,
          "idx4085deadline", SegmentParameters.builder().weightQuantization(WeightQuantization.FP32).build())) {
        database.transaction(() -> {
          for (final var doc : corpus.entrySet())
            for (final var dw : doc.getValue().entrySet())
              engine.put(dw.getKey(), doc.getKey(), dw.getValue());
          engine.flush();
        });

        // Jam every worker so the query's range can be submitted but never started. Deterministic:
        // the deadline is what ends the query, not a race between it and the scoring.
        final SparseVectorScoringPool pool = SparseVectorScoringPool.getInstance();
        final ExecutorService executor = pool.getExecutorService();
        for (int i = 0; i < pool.getMaxParallelism(); i++) {
          hogs.add(executor.submit(() -> {
            occupied.countDown();
            // Same guard as the caller-runs test: never block the submitting thread, and never
            // block unboundedly, so a mis-sized saturation cannot wedge the run.
            if (SparseVectorScoringPool.isPoolThread())
              release.await(SATURATION_RELEASE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            return null;
          }));
        }
        assertThat(occupied.await(10, TimeUnit.SECONDS)).as("a worker must have picked up the block").isTrue();

        assertThatThrownBy(() -> engine.topK(queryDims, queryWeights, K))
            .isInstanceOf(IndexException.class)
            .hasMessageContaining("timed out")
            .hasMessageContaining(GlobalConfiguration.SPARSE_VECTOR_SCORING_TIMEOUT_SECONDS.getKey());

        // A failed fan-out must still hand its claim back, or splitting quietly dies for good.
        release.countDown();
        for (final Future<?> f : hogs)
          f.get(10, TimeUnit.SECONDS);
        assertThat(pool.getReservedWorkers()).as("a timed-out query must release its claim").isZero();
      }
    } finally {
      release.countDown();
      GlobalConfiguration.SPARSE_VECTOR_SCORING_MAX_PARTITIONS.setValue(previousPartitions);
      GlobalConfiguration.SPARSE_VECTOR_SCORING_MIN_POSTINGS_FOR_PARTITIONING.setValue(previousMin);
      GlobalConfiguration.SPARSE_VECTOR_SCORING_TIMEOUT_SECONDS.setValue(previousTimeout);
    }
  }

  @Test
  void aRangeForcedOntoTheCallerThreadLeavesTheCallersTransactionAlone() throws Exception {
    final int previousPartitions = GlobalConfiguration.SPARSE_VECTOR_SCORING_MAX_PARTITIONS.getValueAsInteger();
    final long previousMin = GlobalConfiguration.SPARSE_VECTOR_SCORING_MIN_POSTINGS_FOR_PARTITIONING.getValueAsLong();
    final CountDownLatch release = new CountDownLatch(1);
    try {
      GlobalConfiguration.SPARSE_VECTOR_SCORING_MAX_PARTITIONS.setValue(2);
      GlobalConfiguration.SPARSE_VECTOR_SCORING_MIN_POSTINGS_FOR_PARTITIONING.setValue(1L);

      final Map<RID, Map<Integer, Float>> corpus = buildCorpus(SMALL_DOCS, 12L, false);
      final int[] queryDims = new int[DIMS];
      final float[] queryWeights = new float[DIMS];
      for (int d = 0; d < DIMS; d++) {
        queryDims[d] = d;
        queryWeights[d] = 1.0f;
      }

      try (final PaginatedSparseVectorEngine engine = new PaginatedSparseVectorEngine((DatabaseInternal) database,
          "idx4085callerruns", SegmentParameters.builder().weightQuantization(WeightQuantization.FP32).build())) {
        database.transaction(() -> {
          for (final var doc : corpus.entrySet())
            for (final var dw : doc.getValue().entrySet())
              engine.put(dw.getKey(), doc.getKey(), dw.getValue());
          engine.flush();
        });

        final List<RidScore> expected = engine.topK(queryDims, queryWeights, K);

        // Saturate the pool - every worker busy AND every queue slot taken - so the next submission
        // is rejected and the pool's caller-runs policy executes it inline on the submitting thread.
        // That thread is the one inside the transaction below, which is the whole point: the range
        // task then runs somewhere that already has a database context.
        final SparseVectorScoringPool pool = SparseVectorScoringPool.getInstance();
        final ExecutorService executor = pool.getExecutorService();
        // Submit until the queue reports itself full rather than computing a count up front: the
        // count is read from a racing snapshot, and overshooting it by even one is not a slow test
        // but a permanently stuck one. An over-submitted task is rejected and the caller-runs policy
        // runs it HERE, on this thread, where awaiting a latch released in a finally this thread can
        // no longer reach hangs the JVM until CI times out. The guard below is what makes the task
        // safe to run anywhere; this loop is what stops it happening in the first place.
        int guard = 0;
        while (pool.getPoolStats().queueCapacityRemaining() > 0 && guard++ < SATURATION_SUBMIT_LIMIT)
          executor.submit(() -> {
            // Only block when actually on a worker. A task that finds itself on the submitting
            // thread was rejected, and blocking there is exactly the deadlock described above.
            if (SparseVectorScoringPool.isPoolThread())
              release.await(SATURATION_RELEASE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            return null;
          });
        assertThat(guard).as("the pool must actually be saturated for this test to mean anything")
            .isLessThan(SATURATION_SUBMIT_LIMIT);

        database.begin();
        try {
          // No uncommitted changes, so the query is still allowed to split; the range it submits is
          // rejected by the saturated pool and runs right here.
          final List<RidScore> got = engine.topK(queryDims, queryWeights, K);
          assertSameResult(got, expected, "range forced through caller-runs");

          // The real damage an unconditional DatabaseContext.init() does here: it takes the
          // "ROLLBACK PREVIOUS TXS" branch and silently rolls back the caller's transaction, then the
          // matching removeContext wipes the context the rest of the query still needs.
          assertThat(database.isTransactionActive())
              .as("the caller's transaction must survive a range that ran on its own thread").isTrue();
          assertThat(DatabaseContext.INSTANCE.getContextIfExists(((DatabaseInternal) database).getDatabasePath()))
              .as("the caller's database context must not be torn down under it").isNotNull();
        } finally {
          if (database.isTransactionActive())
            database.rollback();
        }
      }
    } finally {
      release.countDown();
      GlobalConfiguration.SPARSE_VECTOR_SCORING_MAX_PARTITIONS.setValue(previousPartitions);
      GlobalConfiguration.SPARSE_VECTOR_SCORING_MIN_POSTINGS_FOR_PARTITIONING.setValue(previousMin);
    }
  }

  @Test
  void anOuterTransactionsUncommittedChangesStillBlockTheSplit() throws Exception {
    final int previousPartitions = GlobalConfiguration.SPARSE_VECTOR_SCORING_MAX_PARTITIONS.getValueAsInteger();
    final long previousMin = GlobalConfiguration.SPARSE_VECTOR_SCORING_MIN_POSTINGS_FOR_PARTITIONING.getValueAsLong();
    try {
      GlobalConfiguration.SPARSE_VECTOR_SCORING_MAX_PARTITIONS.setValue(4);
      GlobalConfiguration.SPARSE_VECTOR_SCORING_MIN_POSTINGS_FOR_PARTITIONING.setValue(1L);

      final Map<RID, Map<Integer, Float>> corpus = buildCorpus(SMALL_DOCS, 21L, false);
      final int[] queryDims = new int[DIMS];
      final float[] queryWeights = new float[DIMS];
      for (int d = 0; d < DIMS; d++) {
        queryDims[d] = d;
        queryWeights[d] = 1.0f;
      }

      database.getSchema().createDocumentType("OuterTxProbe");

      try (final PaginatedSparseVectorEngine engine = new PaginatedSparseVectorEngine((DatabaseInternal) database,
          "idx4085nestedtx", SegmentParameters.builder().weightQuantization(WeightQuantization.FP32).build())) {
        database.transaction(() -> {
          for (final var doc : corpus.entrySet())
            for (final var dw : doc.getValue().entrySet())
              engine.put(dw.getKey(), doc.getKey(), dw.getValue());
          engine.flush();
        });

        final List<RidScore> expected = engine.topK(queryDims, queryWeights, K);
        final long splitsWhenClean = engine.partitionedQueryCount();
        assertThat(splitsWhenClean).as("a clean caller must split, or the rest proves nothing").isPositive();

        database.begin();
        try {
          // Dirty the OUTER transaction, then open a nested one that is itself clean. Asking only
          // the innermost transaction whether it has changes would answer "no" here and let the
          // query split, and a worker reading committed pages through its own context would not see
          // what the outer transaction has written.
          database.newDocument("OuterTxProbe").set("k", 1).save();
          database.begin();
          try {
            engine.topK(queryDims, queryWeights, K);
            assertThat(engine.partitionedQueryCount())
                .as("a nested-clean caller over a dirty outer transaction must not split")
                .isEqualTo(splitsWhenClean);
          } finally {
            database.rollback();
          }
        } finally {
          if (database.isTransactionActive())
            database.rollback();
        }

        // And once everything is rolled back it splits again, so the guard is not simply stuck on.
        assertSameResult(engine.topK(queryDims, queryWeights, K), expected, "after the transaction ended");
        assertThat(engine.partitionedQueryCount()).isGreaterThan(splitsWhenClean);
      }
    } finally {
      GlobalConfiguration.SPARSE_VECTOR_SCORING_MAX_PARTITIONS.setValue(previousPartitions);
      GlobalConfiguration.SPARSE_VECTOR_SCORING_MIN_POSTINGS_FOR_PARTITIONING.setValue(previousMin);
    }
  }

  /**
   * Equivalence with a live memtable under the query.
   * <p>
   * Every other engine-level test here flushes before querying, so the memtable leg of the merged
   * cursor is empty in all of them and the split has only ever been asserted against sealed
   * segments. It splits over a populated memtable too: the guard only refuses when the <i>caller</i>
   * holds uncommitted changes, and postings committed by an earlier transaction are not that.
   * <p>
   * There is a reason to expect this to hold - the ranges partition the RID space whatever the
   * postings are stored in, and the memtable's looser per-term ceiling can only prune more
   * conservatively, never wrongly - but "expected to hold" is what the rest of this class exists to
   * stop us relying on. The memtable is also the one source that reports no finite block boundary,
   * so it is the leg where the block-skip machinery behaves differently from a segment.
   */
  @Test
  void aQueryOverALiveMemtableSplitsAndStillMatchesTheSerialResult() throws Exception {
    final int previousPartitions = GlobalConfiguration.SPARSE_VECTOR_SCORING_MAX_PARTITIONS.getValueAsInteger();
    final long previousMin = GlobalConfiguration.SPARSE_VECTOR_SCORING_MIN_POSTINGS_FOR_PARTITIONING.getValueAsLong();
    try {
      GlobalConfiguration.SPARSE_VECTOR_SCORING_MIN_POSTINGS_FOR_PARTITIONING.setValue(1L);

      final Map<RID, Map<Integer, Float>> sealed = buildCorpus(SMALL_DOCS, 77L, false);
      // A second, disjoint slice of the RID space that stays in the memtable, interleaved with the
      // sealed one so ranges straddle both sources rather than each landing in a single leg.
      final TreeMap<RID, Map<Integer, Float>> live = new TreeMap<>(SparseSegmentBuilder::compareRid);
      final Random rnd = new Random(78L);
      for (int i = 0; i < SMALL_DOCS / 4; i++) {
        final TreeMap<Integer, Float> doc = new TreeMap<>();
        for (int d = 0; d < DIMS; d++)
          if (rnd.nextFloat() < 0.5f / (d + 1))
            doc.put(d, 0.90f + rnd.nextFloat() * 0.10f);
        if (!doc.isEmpty())
          live.put(new RID(0, 2L + i * 4L), doc);
      }

      final int[] queryDims = new int[DIMS];
      final float[] queryWeights = new float[DIMS];
      for (int d = 0; d < DIMS; d++) {
        queryDims[d] = d;
        queryWeights[d] = 1.0f;
      }

      try (final PaginatedSparseVectorEngine engine = new PaginatedSparseVectorEngine((DatabaseInternal) database,
          "idx4085memtable", SegmentParameters.builder().weightQuantization(WeightQuantization.FP32).build())) {
        database.transaction(() -> {
          for (final var doc : sealed.entrySet())
            for (final var dw : doc.getValue().entrySet())
              engine.put(dw.getKey(), doc.getKey(), dw.getValue());
          engine.flush();
        });
        // Committed in its own transaction and deliberately NOT flushed, so by query time these are
        // committed postings living in the memtable and the caller is clean.
        database.transaction(() -> {
          for (final var doc : live.entrySet())
            for (final var dw : doc.getValue().entrySet())
              engine.put(dw.getKey(), doc.getKey(), dw.getValue());
        });

        assertThat(engine.memtablePostings())
            .as("the memtable must actually be carrying postings, or this test proves nothing").isPositive();
        assertThat(engine.segmentCount()).as("and there must be a sealed segment to merge them against").isPositive();

        GlobalConfiguration.SPARSE_VECTOR_SCORING_MAX_PARTITIONS.setValue(1);
        final List<RidScore> serial = engine.topK(queryDims, queryWeights, K);
        assertThat(serial).isNotEmpty();
        final long splitsBefore = engine.partitionedQueryCount();

        for (final int partitions : new int[] { 2, 4, 8 }) {
          GlobalConfiguration.SPARSE_VECTOR_SCORING_MAX_PARTITIONS.setValue(partitions);
          assertSameResult(engine.topK(queryDims, queryWeights, K), serial, "live memtable, split=" + partitions);
        }
        assertThat(engine.partitionedQueryCount())
            .as("the query must really have split over the memtable, not quietly stayed serial")
            .isEqualTo(splitsBefore + 3);
      }
    } finally {
      GlobalConfiguration.SPARSE_VECTOR_SCORING_MAX_PARTITIONS.setValue(previousPartitions);
      GlobalConfiguration.SPARSE_VECTOR_SCORING_MIN_POSTINGS_FOR_PARTITIONING.setValue(previousMin);
    }
  }

  @Test
  void anExplicitClaimSaturatesAtThePoolCeilingButTheSplitStillHappens() throws Exception {
    final SparseVectorScoringPool pool = SparseVectorScoringPool.getInstance();
    final int ceiling = pool.getMaxParallelism();
    final int baseline = pool.getReservedWorkers();

    // The claim is capped: asking for far more than the pool holds records only what it can hold.
    final int granted = pool.reserveWorkers(ceiling * 10);
    try {
      assertThat(granted).as("an explicit claim must saturate at the pool ceiling").isEqualTo(ceiling - baseline);
      assertThat(pool.getReservedWorkers())
          .as("reserved workers must never exceed the ceiling, however many ranges were asked for")
          .isEqualTo(ceiling);
      // And with the pool fully claimed, a further claim records nothing rather than going negative
      // or overflowing past the ceiling.
      assertThat(pool.reserveWorkers(ceiling)).as("a fully claimed pool grants nothing").isZero();
      assertThat(pool.getReservedWorkers()).isEqualTo(ceiling);
    } finally {
      pool.releaseWorkers(granted);
    }
    assertThat(pool.getReservedWorkers()).as("the claim must return to baseline").isEqualTo(baseline);
  }

  @Test
  void anExplicitSplitDoesNotYieldToAFullyClaimedPool() throws Exception {
    final int previousPartitions = GlobalConfiguration.SPARSE_VECTOR_SCORING_MAX_PARTITIONS.getValueAsInteger();
    final long previousMin = GlobalConfiguration.SPARSE_VECTOR_SCORING_MIN_POSTINGS_FOR_PARTITIONING.getValueAsLong();
    final SparseVectorScoringPool pool = SparseVectorScoringPool.getInstance();
    int hogged = 0;
    try {
      GlobalConfiguration.SPARSE_VECTOR_SCORING_MAX_PARTITIONS.setValue(4);
      GlobalConfiguration.SPARSE_VECTOR_SCORING_MIN_POSTINGS_FOR_PARTITIONING.setValue(1L);

      final Map<RID, Map<Integer, Float>> corpus = buildCorpus(SMALL_DOCS, 91L, false);
      final int[] queryDims = new int[DIMS];
      final float[] queryWeights = new float[DIMS];
      for (int d = 0; d < DIMS; d++) {
        queryDims[d] = d;
        queryWeights[d] = 1.0f;
      }

      try (final PaginatedSparseVectorEngine engine = new PaginatedSparseVectorEngine((DatabaseInternal) database,
          "idx4085explicit", SegmentParameters.builder().weightQuantization(WeightQuantization.FP32).build())) {
        database.transaction(() -> {
          for (final var doc : corpus.entrySet())
            for (final var dw : doc.getValue().entrySet())
              engine.put(dw.getKey(), doc.getKey(), dw.getValue());
          engine.flush();
        });

        final List<RidScore> expected = engine.topK(queryDims, queryWeights, K);
        final long splitsBefore = engine.partitionedQueryCount();

        // Claim the entire pool on behalf of someone else. An adaptive query would go serial here;
        // an explicit one is an operator saying "do not throttle me", so it must still split.
        hogged = pool.tryReserveWorkers(pool.getMaxParallelism());
        assertThat(hogged).isPositive();
        final int reservedWhileHogged = pool.getReservedWorkers();

        assertSameResult(engine.topK(queryDims, queryWeights, K), expected, "explicit split, pool fully claimed");
        assertThat(engine.partitionedQueryCount())
            .as("an explicit split must not yield to a busy pool").isEqualTo(splitsBefore + 1);
        // ...and it must not have inflated the count on the way through, which is what would have
        // suppressed splitting for every other query on the box.
        assertThat(pool.getReservedWorkers())
            .as("an explicit split must not leave the ceiling exceeded").isEqualTo(reservedWhileHogged);
      }
    } finally {
      pool.releaseWorkers(hogged);
      GlobalConfiguration.SPARSE_VECTOR_SCORING_MAX_PARTITIONS.setValue(previousPartitions);
      GlobalConfiguration.SPARSE_VECTOR_SCORING_MIN_POSTINGS_FOR_PARTITIONING.setValue(previousMin);
    }
  }

  // ---------- helpers ----------

  /**
   * Same documents, in the same order, with the same scores to within float rounding.
   * <p>
   * The scores are compared with a tolerance rather than exactly, and that is not slack in the test -
   * it is the one thing partitioning genuinely changes. MaxScore sums a document's contributions in
   * an order that depends on where the essential/non-essential split sits, and the split moves with
   * the top-K watermark, which a range reaches differently from the whole scan. The same document
   * therefore gets the same terms added in a different order, and float addition is not associative:
   * the two arms can land one ulp apart. The document set and its ranking are unaffected.
   */
  private static void assertSameResult(final List<RidScore> got, final List<RidScore> expected, final String what) {
    assertThat(got).as("%s: result size", what).hasSameSizeAs(expected);
    for (int i = 0; i < expected.size(); i++) {
      assertThat(got.get(i).rid()).as("%s: rid at %d", what, i).isEqualTo(expected.get(i).rid());
      assertThat(got.get(i).score()).as("%s: score at %d", what, i)
          .isCloseTo(expected.get(i).score(), Offset.offset(1e-4f));
    }
  }

  private static List<RidScore> scoreRange(final PaginatedSegmentReader reader, final int[] queryDims,
      final float[] queryWeights, final RID start, final RID end) throws IOException {
    final DimCursor[] cursors = new DimCursor[queryDims.length];
    try {
      for (int i = 0; i < queryDims.length; i++) {
        final PaginatedSegmentDimCursor sc = reader.openCursor(queryDims[i]);
        cursors[i] = sc == null ? null : new DimCursor(queryDims[i], List.of(sc));
      }
      return BmwScorer.topK(queryDims, queryWeights, cursors, K, start, end);
    } finally {
      for (final DimCursor c : cursors)
        if (c != null)
          c.close();
    }
  }

  /** Cuts at evenly spaced block boundaries of dim 0, the same rule the engine's planner uses. */
  private static RID[] evenBoundaries(final PaginatedSegmentReader reader, final int partitions) throws IOException {
    final PaginatedDimMetadata md = reader.dimMetadata(0);
    final int blocks = md.blockCount();
    final RID[] out = new RID[partitions - 1];
    for (int i = 1; i < partitions; i++) {
      final int b = (int) ((long) blocks * i / partitions);
      out[i - 1] = new RID(md.blockFirstBucketId(b), md.blockFirstPosition(b));
    }
    return out;
  }

  private static Map<RID, Map<Integer, Float>> buildCorpus(final int docs, final long seed, final boolean allTied) {
    final Random rnd = new Random(seed);
    final TreeMap<RID, Map<Integer, Float>> out = new TreeMap<>();
    for (int i = 0; i < docs; i++) {
      final TreeMap<Integer, Float> doc = new TreeMap<>();
      if (allTied) {
        // Identical weight on identical dims: every document scores exactly the same.
        for (int d = 0; d < 4; d++)
          doc.put(d, 0.5f);
      } else {
        for (int d = 0; d < DIMS; d++)
          if (rnd.nextFloat() < 0.5f / (d + 1))
            doc.put(d, 0.90f + rnd.nextFloat() * 0.10f);
      }
      if (!doc.isEmpty())
        out.put(new RID(0, 1L + i), doc);
    }
    if (!allTied) {
      // A handful of genuine answers, so the top-K watermark rises and pruning actually engages -
      // which is what makes the partitioned arm prune differently from the serial one.
      for (int r = 0; r < 40; r++) {
        final Map<Integer, Float> doc = out.computeIfAbsent(new RID(0, 1L + rnd.nextInt(docs)), rid -> new TreeMap<>());
        for (int m = 0; m < 12; m++)
          doc.putIfAbsent(rnd.nextInt(DIMS), 0.90f + rnd.nextFloat() * 0.10f);
      }
    }
    return out;
  }

  @FunctionalInterface
  private interface CheckedRunnable {
    void run() throws Exception;
  }

  private void inTx(final CheckedRunnable r) {
    database.transaction(() -> {
      try {
        r.run();
      } catch (final RuntimeException e) {
        throw e;
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    });
  }

  /** FP32 so scores round-trip exactly and the two arms can be compared with {@code isEqualTo}. */
  private PaginatedSegmentReader buildSegment(final String name, final long segmentId,
      final Map<RID, Map<Integer, Float>> docs) throws IOException {
    final TreeMap<Integer, TreeMap<RID, Float>> byDim = new TreeMap<>();
    for (final var doc : docs.entrySet())
      for (final var dw : doc.getValue().entrySet())
        byDim.computeIfAbsent(dw.getKey(), k -> new TreeMap<>()).put(doc.getKey(), dw.getValue());

    final DatabaseInternal db = (DatabaseInternal) database;
    final SparseSegmentComponent c = new SparseSegmentComponent(db, name, db.getDatabasePath() + "/" + name,
        ComponentFile.MODE.READ_WRITE, SparseSegmentComponent.DEFAULT_PAGE_SIZE);
    ((LocalSchema) db.getSchema().getEmbedded()).registerFile(c);
    try (final SparseSegmentBuilder b = new SparseSegmentBuilder(c,
        SegmentParameters.builder().weightQuantization(WeightQuantization.FP32).build())) {
      b.setSegmentId(segmentId);
      for (final var dim : byDim.entrySet()) {
        b.startDim(dim.getKey());
        for (final var p : dim.getValue().entrySet())
          b.appendPosting(p.getKey(), p.getValue());
        b.endDim();
      }
      b.finish();
    }
    return new PaginatedSegmentReader(c);
  }
}
