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
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression for the second round of issue #5388. The first round added a Block-Max WAND block-skip;
 * the reporter re-measured on real SPLADE vectors and found it never fires, with latency still a
 * near-pure function of the summed posting length of the query's terms (Spearman 0.95). Two
 * properties of learned-sparse data cause that, and this test reproduces both:
 * <ol>
 *   <li><b>Weights are near-uniform within a block</b>, so a per-block maximum is no tighter than
 *       the term's global maximum and no block can ever be proven non-competitive. Every posting
 *       here is drawn from a narrow band, which reduces the block bound to the global bound.</li>
 *   <li><b>Query weights are relatively flat across 30-120 terms</b>, while the document
 *       frequencies are heavily skewed - a few expansion terms cover a large fraction of the corpus
 *       and carry nearly all of the posting mass. Query weights here are uniform and the document
 *       frequencies follow a Zipf-like curve.</li>
 * </ol>
 * Under a pivot-based (Block-Max) WAND traversal the fat terms stay inside the pivot prefix and get
 * skip-seeked a few documents at a time; because their lists are dense every such seek lands in the
 * next block, so effectively every block of every term is decoded - the exhaustive-DAAT signature
 * the reporter measured. MaxScore instead proves the fat terms non-essential (their combined
 * maximum contribution cannot by itself beat the top-K watermark), stops using them to generate
 * candidates at all, and only point-probes them for candidates the essential terms produce, with an
 * early abandon as soon as the partial score plus the remaining ceiling drops under the watermark.
 * With flat weights the terms tie on that ceiling, which is why {@link BmwScorer} breaks the tie by
 * document frequency descending: on this data that is the only signal that separates a cheap term
 * from an expensive one.
 * <p>
 * The assertions are on decoded block payloads - the real I/O cost - not on wall-clock, so the test
 * is deterministic and CI-safe.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class MaxScorePruningTest extends TestHelper {

  /** FP32 so weights round-trip exactly and scores match brute force without quantization noise. */
  private static final SegmentParameters EXACT_PARAMS = SegmentParameters.builder()
      .weightQuantization(WeightQuantization.FP32)
      .build();

  private static final int   DOCS       = 40_000;
  private static final int   DIMS       = 48;
  private static final float HEAD_DF    = 0.50f;  // document frequency of the fattest term
  private static final float ZIPF_S     = 1.10f;
  private static final int   K          = 10;
  /** The fattest terms - the ones holding the posting mass MaxScore has to stop traversing. */
  private static final int   FAT_DIMS   = 6;
  /** Documents that genuinely answer the query: they match many terms and set a high watermark. */
  private static final int   RELEVANT_DOCS    = 60;
  private static final int   RELEVANT_MATCHES = 24;

  @Test
  void fatTermsAreProvenNonEssentialAndNotTraversed() throws Exception {
    final Map<RID, Map<Integer, Float>> docs = buildSpladeLikeCorpus();

    final List<PaginatedSegmentReader> readers = new ArrayList<>();
    inTx(() -> readers.add(buildSegment("seg-5388-maxscore", 1L, docs)));

    final int[] queryDims = new int[DIMS];
    final float[] queryWeights = new float[DIMS];
    for (int d = 0; d < DIMS; d++) {
      queryDims[d] = d;
      queryWeights[d] = 1.0f;  // flat query weights, as measured on real SPLADE queries
    }

    inTx(() -> {
      final PaginatedSegmentReader reader = readers.getFirst();
      final PaginatedSegmentDimCursor[] sources = new PaginatedSegmentDimCursor[DIMS];
      final DimCursor[] cursors = new DimCursor[DIMS];
      try {
        long fatBlocks = 0;
        long allBlocks = 0;
        for (int i = 0; i < DIMS; i++) {
          sources[i] = reader.openCursor(queryDims[i]);
          cursors[i] = new DimCursor(queryDims[i], List.of(sources[i]));
          allBlocks += sources[i].metadata().blockCount();
          if (i < FAT_DIMS)
            fatBlocks += sources[i].metadata().blockCount();
        }

        final List<RidScore> got = BmwScorer.topK(queryDims, queryWeights, cursors, K);

        long fatDecoded = 0;
        long allDecoded = 0;
        for (int i = 0; i < DIMS; i++) {
          allDecoded += sources[i].decodedBlockCount();
          if (i < FAT_DIMS)
            fatDecoded += sources[i].decodedBlockCount();
        }

        // Correctness first: identical top-K to the brute-force reference.
        assertTopKMatchesBruteForce(got, queryDims, queryWeights, readers);

        // The fat terms hold the bulk of the posting mass. A pivot-based traversal decodes every
        // block of every one of them (measured: 625 of 625 overall, 156 of 156 for the fattest
        // term); MaxScore must stop traversing them and only point-probe the few candidates that
        // survive the early-abandon check.
        assertThat(fatBlocks).isGreaterThan(100L);
        assertThat(fatBlocks * 2).isGreaterThan(allBlocks);  // the fat terms really do dominate
        assertThat(sources[0].decodedBlockCount())
            .as("decoded %d of %d blocks of the fattest term", sources[0].decodedBlockCount(),
                sources[0].metadata().blockCount())
            .isLessThan(sources[0].metadata().blockCount() / 2);
        assertThat(fatDecoded)
            .as("decoded %d of %d blocks of the fat terms", fatDecoded, fatBlocks)
            .isLessThan(fatBlocks * 3 / 5);
        assertThat(allDecoded)
            .as("decoded %d of %d blocks overall", allDecoded, allBlocks)
            .isLessThan(allBlocks * 4 / 5);
      } finally {
        for (final DimCursor c : cursors)
          if (c != null)
            c.close();
      }
    });
  }

  @Test
  void groupedTopKPrunesFatTermsAndMatchesBruteForce() throws Exception {
    final Map<RID, Map<Integer, Float>> docs = buildSpladeLikeCorpus();

    final List<PaginatedSegmentReader> readers = new ArrayList<>();
    inTx(() -> readers.add(buildSegment("seg-5388-maxscore-grouped", 1L, docs)));

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
        long fatBlocks = 0;
        for (int i = 0; i < DIMS; i++) {
          sources[i] = reader.openCursor(queryDims[i]);
          cursors[i] = new DimCursor(queryDims[i], List.of(sources[i]));
          if (i < FAT_DIMS)
            fatBlocks += sources[i].metadata().blockCount();
        }

        // One group of capacity K degenerates to plain top-K, so the same pruning must engage.
        final List<RidScore> got = BmwScorer.topKGrouped(queryDims, queryWeights, cursors, 1, K, rid -> "g", null);

        long fatDecoded = 0;
        for (int i = 0; i < FAT_DIMS; i++)
          fatDecoded += sources[i].decodedBlockCount();

        assertTopKMatchesBruteForce(got, queryDims, queryWeights, readers);
        assertThat(fatDecoded)
            .as("grouped decoded %d of %d blocks of the fat terms", fatDecoded, fatBlocks)
            .isLessThan(fatBlocks * 3 / 5);
      } finally {
        for (final DimCursor c : cursors)
          if (c != null)
            c.close();
      }
    });
  }

  /**
   * Wall-clock, decoded-block and copied-byte cost of one learned-sparse query, on the same corpus
   * shape as the regression tests but big enough for the numbers to mean something. Reproduces the
   * measurement quoted on issue #5388; run with {@code -Dgroups=benchmark}.
   * <p>
   * Measured on an M-series laptop, 200k documents, 48 flat-weight terms, top-10, paired arms in one
   * JVM (400 reps each, first 20% discarded) before and after the block-payload copy was bounded to
   * the block instead of the rest of the page:
   * <pre>
   * INT8 (production default)  7.27 ms -> 6.48 ms median (-10.8%), 33,479 -> 421 bytes/block (79x)
   * FP32                       7.22 ms -> 6.41 ms median (-11.2%), 33,320 -> 803 bytes/block (42x)
   * </pre>
   * Blocks decoded were identical in both arms (1538 of 3007), so the delta is copy cost alone. That
   * matches the 10.6% of query CPU the reporter's async-profiler run attributed to
   * {@code jbyte_disjoint_arraycopy}.
   */
  @Tag("benchmark")
  @Test
  void spladeShapedQueryCost() throws Exception {
    final int docs = 200_000;
    final Map<RID, Map<Integer, Float>> corpus = buildCorpus(docs);

    final List<PaginatedSegmentReader> readers = new ArrayList<>();
    inTx(() -> {
      readers.add(buildSegment("seg-5388-bench-int8", 1L, corpus,
          SegmentParameters.builder().weightQuantization(WeightQuantization.INT8).build()));
      readers.add(buildSegment("seg-5388-bench-fp32", 2L, corpus, EXACT_PARAMS));
    });

    final int[] queryDims = new int[DIMS];
    final float[] queryWeights = new float[DIMS];
    for (int d = 0; d < DIMS; d++) {
      queryDims[d] = d;
      queryWeights[d] = 1.0f;
    }

    for (int r = 0; r < readers.size(); r++) {
      final String label = r == 0 ? "INT8 (production default)" : "FP32";
      final PaginatedSegmentReader reader = readers.get(r);
      inTx(() -> benchmarkQuery(reader, label, docs, queryDims, queryWeights));
    }
  }

  private void benchmarkQuery(final PaginatedSegmentReader reader, final String label, final int docs, final int[] queryDims,
      final float[] queryWeights) {
    try {
      long totalBlocks = 0;
      for (int i = 0; i < DIMS; i++)
        totalBlocks += reader.openCursor(queryDims[i]).metadata().blockCount();

      final int reps = 400;
      final double[] samples = new double[reps];
      long decoded = 0;
      long payloadBytes = 0;

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
        for (final PaginatedSegmentDimCursor c : sources) {
          decoded += c.decodedBlockCount();
          payloadBytes += c.decodedPayloadBytes();
        }
        for (final DimCursor c : cursors)
          c.close();
      }

      // Discard the first 20% as warm-up before taking the statistics.
      final double[] tail = Arrays.copyOfRange(samples, reps / 5, reps);
      Arrays.sort(tail);
      System.out.printf("%n%s: docs=%,d dims=%d k=%d reps=%d (measured %d)%n", label, docs, DIMS, K, reps, tail.length);
      System.out.printf("  median %.2f ms  min %.2f ms  p90 %.2f ms | %d of %d blocks decoded, %,d bytes copied (%,d/block)%n",
          tail[tail.length / 2], tail[0], tail[(int) (tail.length * 0.9)], decoded, totalBlocks, payloadBytes,
          decoded == 0 ? 0 : payloadBytes / decoded);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void assertTopKMatchesBruteForce(final List<RidScore> got, final int[] queryDims, final float[] queryWeights,
      final List<PaginatedSegmentReader> readers) throws IOException {
    final List<RidScore> expected = BruteForceScorer.topK(queryDims, queryWeights,
        readers.toArray(new PaginatedSegmentReader[0]), K);
    assertThat(got).hasSize(expected.size());
    for (int i = 0; i < got.size(); i++) {
      assertThat(got.get(i).rid()).isEqualTo(expected.get(i).rid());
      assertThat(got.get(i).score()).isCloseTo(expected.get(i).score(), Offset.offset(1e-4f));
    }
  }

  /**
   * Corpus shaped like a SPLADE-expanded collection: document frequencies follow a Zipf-like curve
   * so a handful of expansion terms cover a large fraction of the corpus, and every posting weight
   * sits in a narrow band so per-block maxima carry no information beyond the global maximum.
   */
  private Map<RID, Map<Integer, Float>> buildSpladeLikeCorpus() {
    return buildCorpus(DOCS);
  }

  private Map<RID, Map<Integer, Float>> buildCorpus(final int docs) {
    final Random rnd = new Random(5388L);
    final float[] density = new float[DIMS];
    for (int d = 0; d < DIMS; d++)
      density[d] = (float) (HEAD_DF / Math.pow(d + 1, ZIPF_S));

    final TreeMap<RID, Map<Integer, Float>> out = new TreeMap<>();
    for (int i = 0; i < docs; i++) {
      final TreeMap<Integer, Float> doc = new TreeMap<>();
      for (int d = 0; d < DIMS; d++)
        if (rnd.nextFloat() < density[d])
          doc.put(d, 0.90f + rnd.nextFloat() * 0.10f);  // near-uniform: block max == global max
      if (!doc.isEmpty())
        out.put(new RID(0, 1L + i), doc);
    }

    // A real query has real answers: a small set of documents that match many of its terms and so
    // score far above the background. Without them the top-K watermark never rises much above a
    // single term's ceiling and no term can be proven non-essential - which is a property of the
    // synthetic corpus, not of the workload. The extra postings are drawn from the same narrow
    // weight band, so per-block maxima stay as uninformative as everywhere else.
    for (int r = 0; r < RELEVANT_DOCS; r++) {
      final Map<Integer, Float> doc = out.computeIfAbsent(new RID(0, 1L + rnd.nextInt(docs)), rid -> new TreeMap<>());
      for (int m = 0; m < RELEVANT_MATCHES; m++)
        doc.putIfAbsent(rnd.nextInt(DIMS), 0.90f + rnd.nextFloat() * 0.10f);
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

  private PaginatedSegmentReader buildSegment(final String name, final long segmentId,
      final Map<RID, Map<Integer, Float>> docs) throws IOException {
    return buildSegment(name, segmentId, docs, EXACT_PARAMS);
  }

  private PaginatedSegmentReader buildSegment(final String name, final long segmentId,
      final Map<RID, Map<Integer, Float>> docs, final SegmentParameters params) throws IOException {
    final TreeMap<Integer, TreeMap<RID, Float>> byDim = new TreeMap<>();
    for (final var doc : docs.entrySet()) {
      for (final var dw : doc.getValue().entrySet())
        byDim.computeIfAbsent(dw.getKey(), k -> new TreeMap<>()).put(doc.getKey(), dw.getValue());
    }
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
