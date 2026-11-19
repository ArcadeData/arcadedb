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
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.sparsevector.SegmentFormat.WeightQuantization;
import com.arcadedb.index.vector.GroupAdmissionState;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.LocalSchema;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.logging.Level;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Regression tests for sparse vector traversal cost, windows, partitioning and grouped top-K. Each nested class keeps the fixture of the issue it covers.
 */
class SparseVectorTraversalTest {

  /**
   * Regression for #5467: the small-corpus tier, and the posting-traversal cost that the
   * reporter's last CPU profile left as the remainder.
   * <p>
   * The issue was filed because {@code LSM_SPARSE_VECTOR} top-K at the 100k tier had grown from
   * 1.45 ms to 8.94 ms p50 while the 1M tier improved, on the hypothesis that a fixed per-query
   * preparation cost was invisible at 1M and dominant at 100k. Two rounds of profiling disproved the
   * hypothesis - setup is under 0.1% of the query - and attacked what the profile actually showed:
   * the essential-term min-heap, the per-candidate block-max recomputation, and the multi-segment
   * merge on a settled index. The last profile (released 26.8.1.dev23) put what was left in posting
   * traversal: {@code advance} plus {@code decodeBlockIfNeeded} at 26.8% of query CPU, with scoring
   * down at 1.5%.
   * <p>
   * This class covers the third bullet of the original scope, which the first two rounds never
   * delivered: <b>a benchmark that reports a small-corpus tier</b>, so a per-query cost regression
   * cannot hide behind a healthy 1M number again. The corpus is shaped like the reporter's harness -
   * 100k documents, 59 query terms, roughly a million postings across them, near-uniform weights - so
   * the traversal it drives has the same shape as the one being measured upstream.
   * <p>
   * The assertions in the non-benchmark tests are on <b>work counters</b>, never on wall-clock: how
   * many times a cursor asked the page cache for a page, and how many posting weights it decoded.
   * Those are the two quantities this round changed, and unlike elapsed time they are deterministic
   * and CI-safe.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class SmallCorpusTraversalCost extends TestHelper {

    /** Production default: the arm the reporter measures. */
    private static final SegmentParameters INT8_PARAMS = SegmentParameters.builder()
        .weightQuantization(WeightQuantization.INT8)
        .build();

    /** FP32 so weights round-trip exactly and scores match brute force without quantization noise. */
    private static final SegmentParameters EXACT_PARAMS = SegmentParameters.builder()
        .weightQuantization(WeightQuantization.FP32)
        .build();

    /** Query terms. Real SPLADE queries carry 30-120; the reporter's harness averages 59. */
    private static final int   DIMS    = 59;
    /**
     * Document-frequency model, calibrated against the counters the reporter measures on real SPLADE at
     * this tier: roughly a million postings across the query's terms, of which MaxScore keeps most out
     * of the traversal, ~89k candidates, and ~2.8 essential postings consumed per candidate.
     * <p>
     * Learned-sparse expansion terms form a <b>plateau</b> - a handful of them appear in most documents
     * carrying small weights - while the query's original vocabulary terms are comparatively rare. A
     * Zipf curve cannot express that: it has no plateau, so tuning it to make the head fat enough makes
     * the tail fat too and every term stays essential, which is a different traversal regime from the
     * one being measured. Two bands with a gentle gradient inside each reproduce the counters.
     */
    private static final int   FAT_DIMS = 11;
    private static final float FAT_DF   = 0.79f;
    private static final float THIN_DF  = 0.051f;
    private static final int   K        = 10;
    /** Documents that genuinely answer the query, so the top-K watermark rises and MaxScore prunes. */
    private static final int   RELEVANT_DOCS    = 60;
    private static final int   RELEVANT_MATCHES = 30;

    /**
     * A cursor walking a dim sequentially must ask the page cache for each page <b>once</b>, not once
     * per block.
     * <p>
     * The builder packs a dim's blocks back to back, so well over a hundred default-sized blocks share
     * one 64 KiB page. Before this round every block decode called {@code readPage} again, and each call
     * allocated a {@code PageId} and an {@code ImmutablePage}, hashed into the page-cache map and
     * bumped the cache's global hit counter - an atomic that every parallel-range worker of #5518
     * contends on. The assertion is exact rather than a ratio: the cursor's page fetches must equal
     * the number of distinct pages its blocks actually live on.
     */
    @Test
    void aSequentialWalkFetchesEachPageExactlyOnce() throws Exception {
      final Map<RID, Map<Integer, Float>> docs = buildCorpus(100_000, 2);

      final List<PaginatedSegmentReader> readers = new ArrayList<>();
      inTx(() -> readers.add(buildSegment("seg-5467-pagefetch", 1L, docs, INT8_PARAMS)));

      inTx(() -> {
        try (final PaginatedSegmentDimCursor c = readers.getFirst().openCursor(0)) {
          final PaginatedDimMetadata meta = c.metadata();
          final Set<Integer> pages = new HashSet<>();
          for (int b = 0; b < meta.blockCount(); b++)
            pages.add(meta.blockPageNum(b));

          // A dense dim must actually span several pages and many blocks, or the assertion below is
          // vacuous - it would be satisfied by a one-block, one-page dim.
          assertThat(meta.blockCount()).as("blocks in the walked dim").isGreaterThan(50);
          assertThat(pages.size()).as("distinct pages the dim spans").isGreaterThan(1);

          long postings = 0;
          c.start();
          while (!c.isExhausted()) {
            postings++;
            if (!c.advance())
              break;
          }
          assertThat(postings).isEqualTo(meta.postingCount());
          assertThat(c.decodedBlockCount()).isEqualTo(meta.blockCount());
          assertThat(c.pageFetchCount())
              .as("fetched %d pages to decode %d blocks living on %d pages", c.pageFetchCount(), c.decodedBlockCount(),
                  pages.size())
              .isEqualTo(pages.size());
        }
      });
    }

    /**
     * A cursor that navigates but never scores must decode <b>no</b> weights, and one that scores every
     * posting must decode exactly one weight per posting - never more.
     * <p>
     * The weight section used to be decoded in full whenever a block was touched: 128 dequantizations
     * and 128 float plus 128 boolean stores for a block that a block-max skip walks straight past, or
     * that a non-essential probe enters to read a single posting. Weights are fixed-stride, so posting
     * {@code i}'s weight is one indexed read and there is no reason to pay for the other 127.
     */
    @Test
    void weightsAreDecodedOnlyForThePostingsThatAreRead() throws Exception {
      final Map<RID, Map<Integer, Float>> docs = buildCorpus(100_000, 2);

      final List<PaginatedSegmentReader> readers = new ArrayList<>();
      inTx(() -> readers.add(buildSegment("seg-5467-lazyweights", 1L, docs, INT8_PARAMS)));

      inTx(() -> {
        final PaginatedSegmentReader reader = readers.getFirst();
        try (final PaginatedSegmentDimCursor navigateOnly = reader.openCursor(0)) {
          navigateOnly.start();
          while (navigateOnly.advance())
            ;
          assertThat(navigateOnly.decodedBlockCount()).isGreaterThan(50L);
          assertThat(navigateOnly.resolvedWeightCount())
              .as("a pure navigation decoded %d weights across %d blocks", navigateOnly.resolvedWeightCount(),
                  navigateOnly.decodedBlockCount())
              .isZero();
        }

        try (final PaginatedSegmentDimCursor scoreEverything = reader.openCursor(0)) {
          long postings = 0;
          scoreEverything.start();
          while (!scoreEverything.isExhausted()) {
            postings++;
            // Both getters resolve the same posting; the second must not decode it again.
            assertThat(scoreEverything.isTombstone()).isFalse();
            assertThat(scoreEverything.currentWeight()).isNotNaN();
            if (!scoreEverything.advance())
              break;
          }
          assertThat(scoreEverything.resolvedWeightCount())
              .as("decoded %d weights for %d postings read", scoreEverything.resolvedWeightCount(), postings)
              .isEqualTo(postings);
        }
      });
    }

    /**
     * The same two properties, observed through a real Block-Max MaxScore traversal instead of a hand
     * walk: the query must decode far fewer weights than the blocks it touched contain, must fetch far
     * fewer pages than it decoded blocks, and must still return exactly the brute-force top-K.
     */
    @Test
    void aMaxScoreQueryPaysForNeitherUnreadWeightsNorRepeatedPageLookups() throws Exception {
      final Map<RID, Map<Integer, Float>> docs = buildCorpus(25_000, DIMS);

      final List<PaginatedSegmentReader> readers = new ArrayList<>();
      inTx(() -> readers.add(buildSegment("seg-5467-traversal", 1L, docs, EXACT_PARAMS)));

      final int[] queryDims = new int[DIMS];
      final float[] queryWeights = new float[DIMS];
      for (int d = 0; d < DIMS; d++) {
        queryDims[d] = d;
        queryWeights[d] = 1.0f;
      }

      inTx(() -> {
        final PaginatedSegmentReader reader = readers.getFirst();
        final PaginatedSegmentDimCursor[] sources = new PaginatedSegmentDimCursor[DIMS];
        final DimCursor[] cursors = new DimCursor[DIMS];
        try {
          for (int i = 0; i < DIMS; i++) {
            sources[i] = reader.openCursor(queryDims[i]);
            cursors[i] = new DimCursor(queryDims[i], List.of(sources[i]));
          }

          final List<RidScore> got = BmwScorer.topK(queryDims, queryWeights, cursors, K);

          long decodedBlocks = 0;
          long pageFetches = 0;
          long weights = 0;
          for (final PaginatedSegmentDimCursor c : sources) {
            decodedBlocks += c.decodedBlockCount();
            pageFetches += c.pageFetchCount();
            weights += c.resolvedWeightCount();
          }

          // Correctness first: the traversal must be untouched by any of this.
          final List<RidScore> expected = BruteForceScorer.topK(queryDims, queryWeights,
              readers.toArray(new PaginatedSegmentReader[0]), K);
          assertThat(got).hasSize(expected.size());
          for (int i = 0; i < got.size(); i++) {
            assertThat(got.get(i).rid()).isEqualTo(expected.get(i).rid());
            assertThat(got.get(i).score()).isCloseTo(expected.get(i).score(), Offset.offset(1e-4f));
          }

          assertThat(decodedBlocks).as("the query must decode a meaningful number of blocks").isGreaterThan(200L);

          final long postingsInDecodedBlocks = decodedBlocks * EXACT_PARAMS.blockSize();
          assertThat(weights)
              .as("decoded %d weights out of %d postings in the %d blocks touched", weights, postingsInDecodedBlocks,
                  decodedBlocks)
              .isLessThan(postingsInDecodedBlocks * 7 / 10);

          assertThat(pageFetches)
              .as("fetched %d pages to decode %d blocks", pageFetches, decodedBlocks)
              .isLessThan(decodedBlocks / 4);
        } finally {
          for (final DimCursor c : cursors)
            if (c != null)
              c.close();
        }
      });
    }

    /**
     * The small-corpus tier the original scope asked for and never got: wall-clock and work counters
     * for one learned-sparse query against a 100k-document corpus, in both the production INT8 arm and
     * the exact FP32 arm. Run with {@code -DexcludedGroups=} (the {@code benchmark} tag is excluded
     * from normal builds).
     * <p>
     * The corpus is 59 query terms over 100k documents with roughly a million postings between them
     * and weights drawn from a narrow band, which is the shape the reporter measures: per-block maxima
     * carry no information beyond the global maximum, so the term-level MaxScore split has to do all
     * the pruning and most of the corpus becomes a scored candidate. Latency at this tier is dominated
     * by per-posting and per-candidate work rather than by anything proportional to corpus size, which
     * is exactly what makes it the tier where a fixed-cost regression shows up first.
     * <p>
     * Measured on an M-series laptop before and after this round, arms interleaved across five rounds
     * so machine drift cancels (400 reps each, first fifth discarded):
     * <pre>
     * INT8  p50 7.76 -> 7.32 ms (-5.7%)   FP32  p50 7.52 -> 7.15 ms (-4.9%)
     * </pre>
     * The wall-clock is indicative - it was taken on a laptop with other work on it - but the work
     * counters underneath it are deterministic and are what the non-benchmark tests here assert on:
     * <pre>
     * page-cache fetches   2,559 -> 103    (-96%)
     * weights decoded    325,406 -> 140,077 (-57%)
     * heap comparisons   913,455 -> 695,016 (-24%)
     * payload bytes copied out of the page cache: all of them -> none
     * </pre>
     */
    @Tag("benchmark")
    @Test
    void smallCorpusTierQueryCost() throws Exception {
      final int docs = 100_000;
      final Map<RID, Map<Integer, Float>> corpus = buildCorpus(docs, DIMS);

      final List<PaginatedSegmentReader> readers = new ArrayList<>();
      inTx(() -> {
        readers.add(buildSegment("seg-5467-bench-int8", 1L, corpus, INT8_PARAMS));
        readers.add(buildSegment("seg-5467-bench-fp32", 2L, corpus, EXACT_PARAMS));
      });

      final int[] queryDims = new int[DIMS];
      final float[] queryWeights = new float[DIMS];
      for (int d = 0; d < DIMS; d++) {
        queryDims[d] = d;
        queryWeights[d] = 1.0f;
      }

      for (int r = 0; r < readers.size(); r++) {
        final String label = r == 0 ? "INT8 (production default)" : "FP32 (exact)";
        final PaginatedSegmentReader reader = readers.get(r);
        inTx(() -> benchmarkQuery(reader, label, docs, queryDims, queryWeights));
      }
    }

    private void benchmarkQuery(final PaginatedSegmentReader reader, final String label, final int docs, final int[] queryDims,
        final float[] queryWeights) {
      try {
        long totalBlocks = 0;
        long totalPostings = 0;
        for (int i = 0; i < DIMS; i++) {
          final PaginatedDimMetadata meta = reader.openCursor(queryDims[i]).metadata();
          totalBlocks += meta.blockCount();
          totalPostings += meta.postingCount();
        }

        // {@code -Dbench.reps=N} raises the rep count for a profiling run, where a 60-second sampling
        // window has to fall inside the measured loop. Clamped at 1 so the knob cannot turn the
        // statistics below into an out-of-bounds read on an empty sample array.
        final int reps = Math.max(1, Integer.getInteger("bench.reps", 400));
        final double[] samples = new double[reps];
        long decoded = 0;
        long payloadBytes = 0;
        long pageFetches = 0;
        long weights = 0;

        for (int rep = 0; rep < reps; rep++) {
          final PaginatedSegmentDimCursor[] sources = new PaginatedSegmentDimCursor[DIMS];
          final DimCursor[] cursors = new DimCursor[DIMS];
          for (int i = 0; i < DIMS; i++) {
            sources[i] = reader.openCursor(queryDims[i]);
            cursors[i] = new DimCursor(queryDims[i], List.of(sources[i]));
          }
          final long start = System.nanoTime();
          BmwScorer.topK(queryDims, queryWeights, cursors, K);
          samples[rep] = (System.nanoTime() - start) / 1_000_000.0;
          decoded = 0;
          payloadBytes = 0;
          pageFetches = 0;
          weights = 0;
          for (final PaginatedSegmentDimCursor c : sources) {
            decoded += c.decodedBlockCount();
            payloadBytes += c.decodedPayloadBytes();
            pageFetches += c.pageFetchCount();
            weights += c.resolvedWeightCount();
          }
          for (final DimCursor c : cursors)
            c.close();
        }

        // Discard the first 20% as warm-up before taking the statistics.
        final double[] tail = Arrays.copyOfRange(samples, reps / 5, reps);
        Arrays.sort(tail);
        System.out.printf("%n%s: docs=%,d terms=%d postings=%,d k=%d reps=%d (measured %d)%n", label, docs, DIMS,
            totalPostings, K, reps, tail.length);
        System.out.printf("  p50 %.2f ms  min %.2f ms  p90 %.2f ms%n", tail[tail.length / 2], tail[0],
            tail[(int) (tail.length * 0.9)]);
        System.out.printf("  %,d of %,d blocks decoded | %,d payload bytes | %,d page fetches | %,d weights decoded%n",
            decoded, totalBlocks, payloadBytes, pageFetches, weights);
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
    }

    // ---------- corpus ----------

    /**
     * Corpus shaped like a learned-sparse (SPLADE) collection restricted to the query's own terms: a
     * plateau of expansion terms carries most of the posting mass, the rest are rare, and every weight
     * sits in a narrow band so per-block maxima are no tighter than the global maximum. Dims outside the query are omitted on purpose - they cost build time and contribute
     * nothing to the traversal being measured.
     */
    private Map<RID, Map<Integer, Float>> buildCorpus(final int docs, final int dims) {
      final Random rnd = new Random(5467L);
      final float[] density = new float[dims];
      for (int d = 0; d < dims; d++)
        density[d] = d < FAT_DIMS ? FAT_DF * (1.0f - 0.03f * d) : THIN_DF * (1.0f - 0.008f * (d - FAT_DIMS));

      final TreeMap<RID, Map<Integer, Float>> out = new TreeMap<>();
      for (int i = 0; i < docs; i++) {
        final TreeMap<Integer, Float> doc = new TreeMap<>();
        for (int d = 0; d < dims; d++)
          if (rnd.nextFloat() < density[d])
            doc.put(d, 0.90f + rnd.nextFloat() * 0.10f);
        if (!doc.isEmpty())
          out.put(new RID(0, 1L + i), doc);
      }

      // A real query has real answers: a small set of documents matching many of its terms, which is
      // what lifts the top-K watermark far enough for MaxScore to prove terms non-essential. Without
      // them the watermark never clears a single term's ceiling and the traversal stays exhaustive -
      // a property of the synthetic corpus rather than of the workload.
      for (int r = 0; r < RELEVANT_DOCS; r++) {
        final Map<Integer, Float> doc = out.computeIfAbsent(new RID(0, 1L + rnd.nextInt(docs)), rid -> new TreeMap<>());
        for (int m = 0; m < RELEVANT_MATCHES && m < dims; m++)
          doc.putIfAbsent(rnd.nextInt(dims), 0.90f + rnd.nextFloat() * 0.10f);
      }
      return out;
    }

    // ---------- helpers ----------

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

    private SparseSegmentComponent newComponent(final String name) {
      final DatabaseInternal db = (DatabaseInternal) database;
      try {
        final SparseSegmentComponent c = new SparseSegmentComponent(db, name, db.getDatabasePath() + "/" + name,
            ComponentFile.MODE.READ_WRITE, SparseSegmentComponent.DEFAULT_PAGE_SIZE);
        ((LocalSchema) db.getSchema().getEmbedded()).registerFile(c);
        return c;
      } catch (final IOException e) {
        throw new RuntimeException("failed to create sparse segment component '" + name + "'", e);
      }
    }

    private PaginatedSegmentReader buildSegment(final String name, final long segmentId, final Map<RID, Map<Integer, Float>> docs,
        final SegmentParameters params) throws IOException {
      final TreeMap<Integer, TreeMap<RID, Float>> byDim = new TreeMap<>();
      for (final var doc : docs.entrySet())
        for (final var dw : doc.getValue().entrySet())
          byDim.computeIfAbsent(dw.getKey(), k -> new TreeMap<>()).put(doc.getKey(), dw.getValue());

      final SparseSegmentComponent c = newComponent(name);
      try (final SparseSegmentBuilder b = new SparseSegmentBuilder(c, params)) {
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

  /**
   * Regression for #9200: the top-K traversal scores a window of RIDs at a time (the shape of Lucene's {@code MaxScoreBulkScorer})
   * instead of keeping the essential cursors ordered per posting. It must answer exactly what the document-at-a-time
   * traversal answers, over segments, a live memtable and deletions, whatever the window size.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class WindowTraversal extends TestHelper {

    private static final int DIMS = 1500;

    @AfterEach
    void restoreWindow() {
      GlobalConfiguration.SPARSE_VECTOR_SCORING_WINDOW.reset();
      GlobalConfiguration.SPARSE_VECTOR_SCORING_MAX_PARTITIONS.reset();
      GlobalConfiguration.SPARSE_VECTOR_SCORING_MIN_POSTINGS_FOR_PARTITIONING.reset();
    }

    @Test
    void windowTraversalAnswersLikeDocumentAtATime() throws Exception {
      final LSMSparseVectorIndex index = buildCorpus(6_000);
      final Random random = new Random(9200);
      for (final int window : new int[] { 64, 1000, 16_384 }) {
        for (int q = 0; q < 60; q++) {
          final int width = 1 + random.nextInt(q % 3 == 0 ? 8 : 60);
          final int[] query = random.ints(0, DIMS).distinct().limit(width).toArray();
          final float[] weights = new float[query.length];
          for (int i = 0; i < weights.length; i++)
            weights[i] = (float) (0.1 + 2 * random.nextDouble());
          final int k = 1 + random.nextInt(25);

          GlobalConfiguration.SPARSE_VECTOR_SCORING_WINDOW.setValue(0);
          final List<RidScore> classic = index.topK(query, weights, k, null);
          GlobalConfiguration.SPARSE_VECTOR_SCORING_WINDOW.setValue(window);
          final List<RidScore> windowed = index.topK(query, weights, k, null);

          assertSameAnswer(classic, windowed, "window " + window + ", query " + q);
        }
      }
    }

    @Test
    void anOversizedWindowSettingIsClamped() throws Exception {
      final LSMSparseVectorIndex index = buildCorpus(600);
      final int[] query = { 1, 2, 3 };
      final float[] weights = { 1f, 1f, 1f };
      GlobalConfiguration.SPARSE_VECTOR_SCORING_WINDOW.setValue(0);
      final List<RidScore> classic = index.topK(query, weights, 5, null);
      GlobalConfiguration.SPARSE_VECTOR_SCORING_WINDOW.setValue(Integer.MAX_VALUE);
      final List<RidScore> clamped = index.topK(query, weights, 5, null);
      assertThat(clamped).isNotEmpty();
      assertSameAnswer(classic, clamped, "clamped window");
    }

    @Test
    void partitionedWindowTraversalAnswersLikeDocumentAtATime() throws Exception {
      final LSMSparseVectorIndex index = buildCorpus(6_000);
      final Random random = new Random(9201);
      GlobalConfiguration.SPARSE_VECTOR_SCORING_MIN_POSTINGS_FOR_PARTITIONING.setValue(1);
      GlobalConfiguration.SPARSE_VECTOR_SCORING_MAX_PARTITIONS.setValue(4);
      try {
        for (int q = 0; q < 40; q++) {
          final int[] query = random.ints(0, DIMS).distinct().limit(5 + random.nextInt(40)).toArray();
          final float[] weights = new float[query.length];
          for (int i = 0; i < weights.length; i++)
            weights[i] = (float) (0.1 + 2 * random.nextDouble());
          final int k = 1 + random.nextInt(25);

          GlobalConfiguration.SPARSE_VECTOR_SCORING_WINDOW.setValue(0);
          final List<RidScore> classic = index.topK(query, weights, k, null);
          GlobalConfiguration.SPARSE_VECTOR_SCORING_WINDOW.setValue(256);
          final List<RidScore> windowed = index.topK(query, weights, k, null);
          assertSameAnswer(classic, windowed, "partitioned query " + q);
        }
      } finally {
        GlobalConfiguration.SPARSE_VECTOR_SCORING_MIN_POSTINGS_FOR_PARTITIONING.reset();
        GlobalConfiguration.SPARSE_VECTOR_SCORING_MAX_PARTITIONS.reset();
      }
    }

    @Test
    @Tag("benchmark")
    void windowTraversalOnAWideCorpus() throws Exception {
      benchmark(30, 70);
    }

    @Test
    @Tag("benchmark")
    void windowTraversalOnSelectiveQueries() throws Exception {
      benchmark(1, 4);
    }

    private void benchmark(final int minTerms, final int extraTerms) throws Exception {
      final LSMSparseVectorIndex index = buildCorpus(60_000);
      final Random random = new Random(1);
      final int rounds = 6, queries = 150;
      final int[][] qs = new int[queries][];
      final float[][] ws = new float[queries][];
      for (int q = 0; q < queries; q++) {
        qs[q] = random.ints(0, DIMS).distinct().limit(minTerms + random.nextInt(extraTerms)).toArray();
        ws[q] = new float[qs[q].length];
        for (int i = 0; i < ws[q].length; i++)
          ws[q][i] = (float) (0.5 + random.nextDouble());
      }
      final long[] nanos = new long[2];
      for (int r = 0; r < rounds; r++)
        for (int mode = 0; mode < 2; mode++) {
          GlobalConfiguration.SPARSE_VECTOR_SCORING_WINDOW.setValue(mode == 0 ? 0 : 16_384);
          GlobalConfiguration.SPARSE_VECTOR_SCORING_MAX_PARTITIONS.setValue(1);
          final long t0 = System.nanoTime();
          for (int q = 0; q < queries; q++)
            index.topK(qs[q], ws[q], 10, null);
          if (r > 0)
            nanos[mode] += System.nanoTime() - t0;
        }
      GlobalConfiguration.SPARSE_VECTOR_SCORING_MAX_PARTITIONS.reset();
      LogManager.instance().log(this, Level.INFO, "Sparse top-10 on 60k docs, %d to %d terms: classic %.3f ms/query, window %.3f ms/query",
          minTerms, minTerms + extraTerms - 1, nanos[0] / 1e6 / ((rounds - 1) * queries), nanos[1] / 1e6 / ((rounds - 1) * queries));
      assertThat(nanos[0]).isPositive();
    }

    private static void assertSameAnswer(final List<RidScore> expected, final List<RidScore> actual, final String what) {
      assertThat(actual).as(what).hasSameSizeAs(expected);
      for (int i = 0; i < expected.size(); i++) {
        assertThat(actual.get(i).score()).as(what + " score at " + i).isCloseTo(expected.get(i).score(), within(1e-3f));
        // Documents that tie to float noise may swap places; anything clearly ahead of the K-th must be the same.
        final float kth = expected.get(expected.size() - 1).score();
        if (expected.get(i).score() > kth + 1e-3f && (i == 0 || expected.get(i - 1).score() > expected.get(i).score() + 1e-3f)
            && (i + 1 == expected.size() || expected.get(i + 1).score() < expected.get(i).score() - 1e-3f))
          assertThat(actual.get(i).rid()).as(what + " rid at " + i).isEqualTo(expected.get(i).rid());
      }
    }

    /** Several segments, a live memtable, and deletions (tombstones in the newer sources). */
    private LSMSparseVectorIndex buildCorpus(final int docs) throws Exception {
      database.command("sql", "CREATE DOCUMENT TYPE Win");
      database.command("sql", "CREATE PROPERTY Win.tokens ARRAY_OF_INTEGERS");
      database.command("sql", "CREATE PROPERTY Win.weights ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON Win (tokens, weights) LSM_SPARSE_VECTOR METADATA { dimensions: " + DIMS + " }");
      final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Win[tokens,weights]");
      final LSMSparseVectorIndex index = (LSMSparseVectorIndex) typeIndex.getIndexesOnBuckets()[0];

      final Random random = new Random(92);
      final List<RID> rids = new ArrayList<>();
      final int batch = docs / 3;
      for (int from = 0; from < docs; from += batch) {
        final int start = from;
        database.transaction(() -> {
          for (int i = start; i < Math.min(docs, start + batch); i++) {
            final int nnz = 8 + random.nextInt(40);
            final int[] tokens = new int[nnz];
            for (int j = 0; j < nnz; j++) {
              final double u = random.nextDouble();
              tokens[j] = (int) (DIMS * u * u); // skewed: a few dims are very frequent
            }
            final int[] distinct = Arrays.stream(tokens).distinct().sorted().toArray();
            final float[] weights = new float[distinct.length];
            for (int j = 0; j < weights.length; j++)
              weights[j] = (float) (0.05 + 3 * random.nextDouble());
            rids.add(database.newDocument("Win").set("tokens", distinct, "weights", weights).save().getIdentity());
          }
        });
        if (from + batch < docs)
          index.compact(); // seal this batch into a segment, keep the last one in the memtable
      }
      database.transaction(() -> {
        for (int i = 0; i < rids.size(); i += 11)
          rids.get(i).asDocument().delete();
      });
      return index;
    }
  }

  /**
   * Regression for #9482: a top-K whose posting mass sits just under the old 200,000 partitioning threshold ran
   * serially and set the p99, while a query a third of its size split across the whole pool would have
   * been faster. The default is now 50,000, and on the adaptive path the number of ranges follows the
   * posting mass (one range per half the threshold), so a query just past the threshold is split in two
   * instead of claiming every worker.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class PartitionThreshold extends TestHelper {

    private static final int K    = 10;
    /** Two query dims of 30,000 postings each: 60,000 in total, above the new default and below the old one. */
    private static final int DOCS = 30_000;

    @AfterEach
    void restoreConfiguration() {
      GlobalConfiguration.SPARSE_VECTOR_SCORING_MAX_PARTITIONS.reset();
      GlobalConfiguration.SPARSE_VECTOR_SCORING_MIN_POSTINGS_FOR_PARTITIONING.reset();
    }

    @Test
    void defaultThresholdIsFiftyThousandPostings() {
      assertThat(GlobalConfiguration.SPARSE_VECTOR_SCORING_MIN_POSTINGS_FOR_PARTITIONING.getDefValue()).isEqualTo(50_000L);
    }

    @Test
    void rangeCountFollowsThePostingMass() {
      // Exactly at the threshold: two ranges, whatever the pool could offer.
      assertThat(PaginatedSparseVectorEngine.rangesByMass(50_000L, 50_000L)).isEqualTo(2);
      assertThat(PaginatedSparseVectorEngine.rangesByMass(74_999L, 50_000L)).isEqualTo(2);
      assertThat(PaginatedSparseVectorEngine.rangesByMass(75_000L, 50_000L)).isEqualTo(3);
      assertThat(PaginatedSparseVectorEngine.rangesByMass(200_000L, 50_000L)).isEqualTo(8);
      // A threshold of 1 (the tests' way of forcing a split) must not cap anything.
      assertThat(PaginatedSparseVectorEngine.rangesByMass(1_000L, 1L)).isEqualTo(1_000);
      assertThat(PaginatedSparseVectorEngine.rangesByMass(Long.MAX_VALUE, 1L)).isEqualTo(Integer.MAX_VALUE);
      assertThat(PaginatedSparseVectorEngine.rangesByMass(0L, 0L)).isZero();
    }

    @Test
    void thePlannerCapsTheRangesByThePostingMass() throws Exception {
      final SparseVectorScoringPool pool = SparseVectorScoringPool.getInstance();
      // With fewer workers the pool, not the mass, is the limit and the cap cannot be told apart from it.
      assumeThat(pool.getMaxParallelism()).isGreaterThanOrEqualTo(4);

      try (final PaginatedSparseVectorEngine engine = openEngine("idx9482plan")) {
        final int[] queryDims = { 0, 1 };
        GlobalConfiguration.SPARSE_VECTOR_SCORING_MAX_PARTITIONS.reset();
        // 60,000 postings against a threshold of 60,000: exactly two ranges, not the pool's worth.
        GlobalConfiguration.SPARSE_VECTOR_SCORING_MIN_POSTINGS_FOR_PARTITIONING.setValue(2L * DOCS);

        final PaginatedSparseVectorEngine.PartitionPlan plan = engine.planPartitionBoundaries(queryDims, engine.segmentsForTest());
        assertThat(plan).isNotNull();
        try {
          assertThat(plan.boundaries()).hasSize(1);
        } finally {
          pool.releaseWorkers(plan.reservedWorkers());
        }

        // A threshold of 0 must not divide by zero or refuse to plan.
        GlobalConfiguration.SPARSE_VECTOR_SCORING_MIN_POSTINGS_FOR_PARTITIONING.setValue(0L);
        final PaginatedSparseVectorEngine.PartitionPlan zero = engine.planPartitionBoundaries(queryDims, engine.segmentsForTest());
        if (zero != null)
          pool.releaseWorkers(zero.reservedWorkers());
      }
    }

    @Test
    void aQueryBetweenTheNewAndTheOldThresholdIsSplitAndStaysExact() throws Exception {
      assumeThat(SparseVectorScoringPool.getInstance().getMaxParallelism()).isGreaterThanOrEqualTo(2);
      try (final PaginatedSparseVectorEngine engine = openEngine("idx9482split")) {
        final int[] queryDims = { 0, 1 };
        final float[] queryWeights = { 1.0f, 1.0f };

        GlobalConfiguration.SPARSE_VECTOR_SCORING_MAX_PARTITIONS.setValue(1);
        final List<RidScore> serial = engine.topK(queryDims, queryWeights, K);
        assertThat(serial).hasSize(K);

        GlobalConfiguration.SPARSE_VECTOR_SCORING_MAX_PARTITIONS.reset();
        GlobalConfiguration.SPARSE_VECTOR_SCORING_MIN_POSTINGS_FOR_PARTITIONING.reset();
        final long before = SparseVectorScoringPool.getInstance().getSplitQueryCount();
        final List<RidScore> adaptive = engine.topK(queryDims, queryWeights, K);

        assertThat(SparseVectorScoringPool.getInstance().getSplitQueryCount()).as("a 60,000-posting query must be split by default")
            .isGreaterThan(before);
        assertThat(adaptive).hasSameSizeAs(serial);
        for (int i = 0; i < serial.size(); i++) {
          assertThat(adaptive.get(i).rid()).isEqualTo(serial.get(i).rid());
          assertThat(adaptive.get(i).score()).isCloseTo(serial.get(i).score(), Offset.offset(1e-4f));
        }
      }
    }

    @Test
    void aQueryBelowTheThresholdStaysOnTheCallerThread() throws Exception {
      try (final PaginatedSparseVectorEngine engine = openEngine("idx9482serial")) {
        GlobalConfiguration.SPARSE_VECTOR_SCORING_MAX_PARTITIONS.reset();
        GlobalConfiguration.SPARSE_VECTOR_SCORING_MIN_POSTINGS_FOR_PARTITIONING.reset();
        final long before = SparseVectorScoringPool.getInstance().getSplitQueryCount();

        final List<RidScore> result = engine.topK(new int[] { 0 }, new float[] { 1.0f }, K);

        assertThat(result).hasSize(K);
        assertThat(SparseVectorScoringPool.getInstance().getSplitQueryCount()).as("a 30,000-posting query is under the threshold")
            .isEqualTo(before);
      }
    }

    private PaginatedSparseVectorEngine openEngine(final String name) {
      final PaginatedSparseVectorEngine engine = new PaginatedSparseVectorEngine((DatabaseInternal) database, name,
          SegmentParameters.builder().weightQuantization(WeightQuantization.FP32).build());
      final Random rnd = new Random(9482L);
      database.transaction(() -> {
        for (int i = 0; i < DOCS; i++) {
          final RID rid = new RID(0, 1L + i);
          engine.put(0, rid, 0.5f + rnd.nextFloat());
          engine.put(1, rid, 0.5f + rnd.nextFloat());
        }
        engine.flush();
      });
      return engine;
    }
  }

  /**
   * Regression for #6936: {@link BmwScorer#topKGrouped}'s group admission handed out its
   * {@code limit} distinct-group slots to whichever keys the ascending-RID DAAT traversal reached
   * first, and never revisited that choice once all slots were taken - the sparse index's own
   * separate collector repeating the defect #5761 fixed for the dense (HNSW) grouped search.
   * <p>
   * Reproduces the issue's own repro shape: low-RID documents from groups A, B and C score near
   * zero for the query, and a single much-later (higher-RID) document from group Z scores far above
   * everything else. Before the fix, groups A/B/C fill the 3 available slots first and group Z -
   * holding the single best-scoring document in the corpus - is rejected outright.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class SparseGroupedTopKGroupChoice extends TestHelper {

    private static final SegmentParameters EXACT_PARAMS = SegmentParameters.builder()
        .weightQuantization(WeightQuantization.FP32)
        .build();

    @Test
    void bestScoringGroupWinsEvenAtTheHighestRid() throws Exception {
      final Map<RID, Map<Integer, Float>> docs = new TreeMap<>();
      final Map<RID, String> groupOf = new HashMap<>();

      final int[]   queryDims    = { 0, 1, 2 };
      final float[] queryWeights = { 1.0f, 1.0f, 1.0f };

      // Groups A, B, C: two low-RID, near-zero-scoring documents each - they fill the 3 slots first.
      addDoc(docs, groupOf, new RID(0, 1L), "A", Map.of(0, 0.01f));
      addDoc(docs, groupOf, new RID(0, 2L), "A", Map.of(0, 0.01f));
      addDoc(docs, groupOf, new RID(0, 3L), "B", Map.of(1, 0.01f));
      addDoc(docs, groupOf, new RID(0, 4L), "B", Map.of(1, 0.01f));
      addDoc(docs, groupOf, new RID(0, 5L), "C", Map.of(2, 0.01f));
      addDoc(docs, groupOf, new RID(0, 6L), "C", Map.of(2, 0.01f));
      // Group Z: a single, much-later (higher-RID) document that matches every term - the single
      // best-scoring document in the corpus.
      addDoc(docs, groupOf, new RID(0, 1_000L), "Z", Map.of(0, 1.0f, 1, 1.0f, 2, 1.0f));

      final PaginatedSegmentReader[] readerHolder = new PaginatedSegmentReader[1];
      inTx(() -> readerHolder[0] = buildSegment("seg-6936", 1L, docs));

      inTx(() -> {
        final PaginatedSegmentReader reader = readerHolder[0];
        // A fresh cursor array per traversal, as the CursorSource contract requires: the scorer closes what it gets,
        // and may traverse twice when a winning group entered by eviction (issue #8002).
        final BmwScorer.CursorSource cursors = () -> {
          final DimCursor[] opened = new DimCursor[queryDims.length];
          for (int i = 0; i < queryDims.length; i++)
            opened[i] = new DimCursor(queryDims[i], List.of(reader.openCursor(queryDims[i])));
          return opened;
        };

        final List<RidScore> got = BmwScorer.topKGrouped(queryDims, queryWeights, cursors, 3, 2, rid -> groupOf.get(rid),
            null);

        final List<String> groups = got.stream().map(rs -> groupOf.get(rs.rid())).distinct().toList();
        assertThat(groups).as("groups returned for %s", got).contains("Z");
        assertThat(got.getFirst().rid()).isEqualTo(new RID(0, 1_000L));
      });
    }

    // ---------- helpers ----------

    private static void addDoc(final Map<RID, Map<Integer, Float>> docs, final Map<RID, String> groupOf, final RID rid,
        final String group, final Map<Integer, Float> weights) {
      docs.put(rid, weights);
      groupOf.put(rid, group);
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

    private SparseSegmentComponent newComponent(final String name) {
      final DatabaseInternal db = (DatabaseInternal) database;
      try {
        final SparseSegmentComponent c = new SparseSegmentComponent(db, name, db.getDatabasePath() + "/" + name,
            ComponentFile.MODE.READ_WRITE, SparseSegmentComponent.DEFAULT_PAGE_SIZE);
        ((LocalSchema) db.getSchema().getEmbedded()).registerFile(c);
        return c;
      } catch (final IOException e) {
        throw new RuntimeException("failed to create sparse segment component '" + name + "'", e);
      }
    }

    private PaginatedSegmentReader buildSegment(final String name, final long segmentId,
        final Map<RID, Map<Integer, Float>> docs) throws IOException {
      final TreeMap<Integer, TreeMap<RID, Float>> byDim = new TreeMap<>();
      for (final var doc : docs.entrySet()) {
        for (final var dw : doc.getValue().entrySet())
          byDim.computeIfAbsent(dw.getKey(), k -> new TreeMap<>()).put(doc.getKey(), dw.getValue());
      }
      final SparseSegmentComponent c = newComponent(name);
      try (final SparseSegmentBuilder b = new SparseSegmentBuilder(c, EXACT_PARAMS)) {
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

  /**
   * Regression for #8002: {@link BmwScorer#topKGrouped} must return exactly what the
   * {@code groupBy} / {@code groupSize} rule gives when applied in score order - the {@code limit} highest-peaking
   * groups, each with its {@code groupSize} best members - whatever order the ascending-RID traversal meets the rows in,
   * and however far the pruning threshold has risen by then. {@link BmwScorer#topKForGroups} must return every group's
   * best members above the floor.
   * <p>
   * Randomised against a brute-force reference over corpora large enough to span many posting blocks, so the block-max
   * skip and the essential/non-essential split both engage. Each corpus plants groups whose single best row sits far
   * above their others and late in RID order - the shape the issue reports.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class GroupedTopKExactness extends TestHelper {
    private static final SegmentParameters EXACT_PARAMS = SegmentParameters.builder()
        .weightQuantization(WeightQuantization.FP32)
        .build();
    private static final int               DIMS         = 4;
    private static final int               DOCS         = 1_500;
    private static final int               TRIALS       = 40;

    @Test
    void groupedTopKMatchesTheScoreOrderedAdmissionRule() {
      final Random rnd = new Random(8002L);
      for (int trial = 0; trial < TRIALS; trial++) {
        final Corpus corpus = corpus(rnd, "seg-8002-g-" + trial);
        final int limit = 1 + rnd.nextInt(5);
        final int groupSize = 1 + rnd.nextInt(4);

        final List<RidScore> expected = new ArrayList<>();
        final GroupAdmissionState admission = new GroupAdmissionState(limit, groupSize);
        for (final RidScore row : corpus.ranked) {
          if (admission.isFull())
            break;
          if (admission.admit(corpus.groupOf.get(row.rid())))
            expected.add(row);
        }

        final int t = trial;
        inTx(() -> {
          final List<RidScore> got = BmwScorer.topKGrouped(corpus.queryDims, corpus.queryWeights, corpus::open, limit, groupSize,
              corpus.groupOf::get, null);
          assertThat(rids(got)).as("trial %d, limit %d, groupSize %d", t, limit, groupSize).isEqualTo(rids(expected));
        });
      }
    }

    @Test
    void topKForGroupsReturnsEachGroupsBestMembersAboveTheFloor() {
      final Random rnd = new Random(80021L);
      for (int trial = 0; trial < TRIALS; trial++) {
        final Corpus corpus = corpus(rnd, "seg-8002-f-" + trial);
        final int groupSize = 1 + rnd.nextInt(4);
        final Set<Object> keys = new HashSet<>();
        final int wanted = 1 + rnd.nextInt(4);
        for (int i = 0; i < wanted; i++)
          keys.add("g" + rnd.nextInt(corpus.groups));
        // Half the trials with no floor, half with one somewhere inside the score range.
        final float floor = rnd.nextBoolean() ? Float.NEGATIVE_INFINITY : corpus.ranked.get(rnd.nextInt(corpus.ranked.size())).score();

        final List<RidScore> expected = new ArrayList<>();
        final Map<Object, Integer> perGroup = new HashMap<>();
        for (final RidScore row : corpus.ranked) {
          final Object key = corpus.groupOf.get(row.rid());
          if (row.score() > floor && keys.contains(key) && perGroup.merge(key, 1, Integer::sum) <= groupSize)
            expected.add(row);
        }

        final int t = trial;
        inTx(() -> {
          final List<RidScore> got = BmwScorer.topKForGroups(corpus.queryDims, corpus.queryWeights, corpus::open, keys,
              groupSize, floor, corpus.groupOf::get, null, null);
          assertThat(rids(got)).as("trial %d, keys %s, groupSize %d, floor %s", t, keys, groupSize, floor)
              .isEqualTo(rids(expected));
        });
      }
    }

    // ---------- corpus ----------

    private final class Corpus {
      int[]                  queryDims;
      float[]                queryWeights;
      int                    groups;
      Map<RID, String>       groupOf = new HashMap<>();
      /** Every scored row, best first, ties by RID: the order the admission rule walks. */
      List<RidScore>         ranked  = new ArrayList<>();
      PaginatedSegmentReader reader;

      DimCursor[] open() throws IOException {
        final DimCursor[] cursors = new DimCursor[queryDims.length];
        for (int i = 0; i < queryDims.length; i++)
          cursors[i] = new DimCursor(queryDims[i], List.of(reader.openCursor(queryDims[i])));
        return cursors;
      }
    }

    private Corpus corpus(final Random rnd, final String segmentName) {
      final Corpus c = new Corpus();
      c.groups = 3 + rnd.nextInt(30);
      c.queryDims = new int[DIMS];
      c.queryWeights = new float[DIMS];
      for (int d = 0; d < DIMS; d++) {
        c.queryDims[d] = d;
        c.queryWeights[d] = 0.1f + rnd.nextFloat();
      }

      final TreeMap<RID, Map<Integer, Float>> docs = new TreeMap<>();
      for (int i = 0; i < DOCS; i++) {
        final RID rid = new RID(0, i);
        final Map<Integer, Float> weights = new HashMap<>();
        // Every dim present at least once, so each query dim has a posting list to open.
        if (i < DIMS)
          weights.put(i, rnd.nextFloat());
        final int nnz = 1 + rnd.nextInt(3);
        for (int z = 0; z < nnz; z++)
          weights.put(rnd.nextInt(DIMS), rnd.nextFloat() * 0.5f);
        // A late row far above everything else: its group's peak, met only after its other rows.
        if (i > DOCS / 2 && rnd.nextInt(100) == 0)
          weights.put(rnd.nextInt(DIMS), 5.0f + rnd.nextFloat());
        docs.put(rid, weights);
        c.groupOf.put(rid, "g" + rnd.nextInt(c.groups));
      }

      for (final Map.Entry<RID, Map<Integer, Float>> doc : docs.entrySet()) {
        float score = 0.0f;
        for (final Map.Entry<Integer, Float> w : doc.getValue().entrySet())
          score += c.queryWeights[w.getKey()] * w.getValue();
        c.ranked.add(new RidScore(doc.getKey(), score));
      }
      c.ranked.sort(BmwScorer.BY_SCORE_DESC);

      inTx(() -> c.reader = buildSegment(segmentName, docs));
      return c;
    }

    private static List<RID> rids(final List<RidScore> rows) {
      final List<RID> out = new ArrayList<>(rows.size());
      for (final RidScore r : rows)
        out.add(r.rid());
      return out;
    }

    // ---------- helpers ----------

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

    private PaginatedSegmentReader buildSegment(final String name, final Map<RID, Map<Integer, Float>> docs) throws IOException {
      final TreeMap<Integer, TreeMap<RID, Float>> byDim = new TreeMap<>();
      for (final var doc : docs.entrySet())
        for (final var dw : doc.getValue().entrySet())
          byDim.computeIfAbsent(dw.getKey(), k -> new TreeMap<>()).put(doc.getKey(), dw.getValue());

      final DatabaseInternal db = (DatabaseInternal) database;
      final SparseSegmentComponent c = new SparseSegmentComponent(db, name, db.getDatabasePath() + "/" + name,
          ComponentFile.MODE.READ_WRITE, SparseSegmentComponent.DEFAULT_PAGE_SIZE);
      ((LocalSchema) db.getSchema().getEmbedded()).registerFile(c);
      try (final SparseSegmentBuilder b = new SparseSegmentBuilder(c, EXACT_PARAMS)) {
        b.setSegmentId(1L);
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
}
