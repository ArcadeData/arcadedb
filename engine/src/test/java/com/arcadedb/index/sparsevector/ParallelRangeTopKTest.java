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
            release.await();
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
