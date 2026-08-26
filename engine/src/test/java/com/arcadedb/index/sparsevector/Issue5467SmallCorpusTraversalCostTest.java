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

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.index.sparsevector.SegmentFormat.WeightQuantization;
import com.arcadedb.schema.LocalSchema;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Third round of issue #5467: the small-corpus tier, and the posting-traversal cost that the
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
class Issue5467SmallCorpusTraversalCostTest extends TestHelper {

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
      // window has to fall inside the measured loop.
      final int reps = Integer.getInteger("bench.reps", 400);
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
