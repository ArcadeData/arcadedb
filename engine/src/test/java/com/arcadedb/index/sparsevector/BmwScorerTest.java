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

import com.arcadedb.database.RID;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 2 verification: {@link BmwScorer} produces identical top-K to {@link BruteForceScorer}
 * across single-segment, multi-segment, tombstone, and varying-K scenarios.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class BmwScorerTest {

  /** Float tolerance: int8 quantization can perturb partial sums by ~ 1e-3 per dim. */
  private static final float SCORE_TOL = 1e-2f;

  @Test
  void singleSegmentTopKMatchesBruteForce(@TempDir final Path tmp) throws IOException {
    final Path file = tmp.resolve("seg.sparseseg");
    final Map<RID, Map<Integer, Float>> docs = randomCorpus(0xCAFEL, 200, 30, 5);
    writeSegment(file, docs);

    try (final SparseSegmentReader r = new SparseSegmentReader(file)) {
      final SparseSegmentReader[] segs = { r };
      runComparisons(segs, 30, new int[] { 1, 5, 10, 25 }, 0xBEEFL);
    }
  }

  @Test
  void multipleSegmentsNewerOverridesOlder(@TempDir final Path tmp) throws IOException {
    final Path s0 = tmp.resolve("0.sparseseg");
    final Path s1 = tmp.resolve("1.sparseseg");

    final Map<RID, Map<Integer, Float>> oldDocs = new TreeMap<>();
    oldDocs.put(new RID(0, 1), Map.of(0, 0.5f, 1, 0.3f));
    oldDocs.put(new RID(0, 2), Map.of(1, 0.7f, 2, 0.4f));
    oldDocs.put(new RID(0, 3), Map.of(0, 0.2f, 2, 0.9f));
    writeSegment(s0, oldDocs);

    // Newer segment: rewrites doc 1's dim-0 weight to 0.99, leaves doc 2 alone, adds new doc 4.
    final Map<RID, Map<Integer, Float>> newDocs = new TreeMap<>();
    newDocs.put(new RID(0, 1), Map.of(0, 0.99f));
    newDocs.put(new RID(0, 4), Map.of(0, 0.8f, 1, 0.6f));
    writeSegment(s1, newDocs);

    try (final SparseSegmentReader r0 = new SparseSegmentReader(s0);
         final SparseSegmentReader r1 = new SparseSegmentReader(s1)) {
      final SparseSegmentReader[] segs = { r0, r1 };
      // Query for dim 0 only at full weight - doc 1 should win since it was overwritten with 0.99.
      final List<RidScore> bmw = runBmw(segs, new int[] { 0 }, new float[] { 1.0f }, 5);
      assertThat(bmw).isNotEmpty();
      assertThat(bmw.getFirst().rid()).isEqualTo(new RID(0, 1));
      assertThat(bmw.getFirst().score()).isCloseTo(0.99f, Offset.offset(SCORE_TOL));

      // Sanity: brute-force agrees.
      final List<RidScore> bf = BruteForceScorer.topK(new int[] { 0 }, new float[] { 1.0f }, segs, 5);
      assertEquivalent(bmw, bf);
    }
  }

  @Test
  void tombstoneInNewerSegmentMasksDoc(@TempDir final Path tmp) throws IOException {
    final Path s0 = tmp.resolve("0.sparseseg");
    final Path s1 = tmp.resolve("1.sparseseg");

    final Map<RID, Map<Integer, Float>> oldDocs = new TreeMap<>();
    oldDocs.put(new RID(0, 1), Map.of(0, 0.9f, 1, 0.8f));
    oldDocs.put(new RID(0, 2), Map.of(0, 0.5f));
    writeSegment(s0, oldDocs);

    // Newer segment puts tombstone for (0, 1) on dim 0.
    try (final SparseSegmentWriter w = new SparseSegmentWriter(s1, SegmentParameters.defaults())) {
      w.startDim(0);
      w.appendTombstone(new RID(0, 1));
      w.endDim();
      w.finish();
    }

    try (final SparseSegmentReader r0 = new SparseSegmentReader(s0);
         final SparseSegmentReader r1 = new SparseSegmentReader(s1)) {
      final SparseSegmentReader[] segs = { r0, r1 };
      final List<RidScore> bmw = runBmw(segs, new int[] { 0 }, new float[] { 1.0f }, 10);
      // Doc (0,1) is masked; only (0,2) should remain.
      assertThat(bmw).hasSize(1);
      assertThat(bmw.getFirst().rid()).isEqualTo(new RID(0, 2));

      final List<RidScore> bf = BruteForceScorer.topK(new int[] { 0 }, new float[] { 1.0f }, segs, 10);
      assertEquivalent(bmw, bf);
    }
  }

  @Test
  void emptyQueryReturnsEmpty(@TempDir final Path tmp) throws IOException {
    final Path file = tmp.resolve("seg.sparseseg");
    writeSegment(file, randomCorpus(1L, 50, 10, 3));
    try (final SparseSegmentReader r = new SparseSegmentReader(file)) {
      final List<RidScore> result = BmwScorer.topK(new int[0], new float[0], new DimCursor[0], 10);
      assertThat(result).isEmpty();
    }
  }

  @Test
  void zeroKReturnsEmpty(@TempDir final Path tmp) throws IOException {
    final Path file = tmp.resolve("seg.sparseseg");
    writeSegment(file, randomCorpus(2L, 50, 10, 3));
    try (final SparseSegmentReader r = new SparseSegmentReader(file)) {
      final SparseSegmentReader[] segs = { r };
      final List<RidScore> result = runBmw(segs, new int[] { 0, 1 }, new float[] { 1.0f, 1.0f }, 0);
      assertThat(result).isEmpty();
    }
  }

  @Test
  void queryDimAbsentFromSegmentScoresZero(@TempDir final Path tmp) throws IOException {
    final Path file = tmp.resolve("seg.sparseseg");
    final Map<RID, Map<Integer, Float>> docs = new TreeMap<>();
    docs.put(new RID(0, 1), Map.of(0, 0.5f));
    writeSegment(file, docs);

    try (final SparseSegmentReader r = new SparseSegmentReader(file)) {
      final SparseSegmentReader[] segs = { r };
      // Query dim 99 doesn't exist anywhere.
      final List<RidScore> bmw = runBmw(segs, new int[] { 99 }, new float[] { 1.0f }, 10);
      assertThat(bmw).isEmpty();
    }
  }

  @Test
  void largeRandomCorpusMatchesBruteForce(@TempDir final Path tmp) throws IOException {
    final Path file = tmp.resolve("seg.sparseseg");
    // 2000 docs, dimensionality 200, ~20 nnz per doc = 40K postings.
    final Map<RID, Map<Integer, Float>> docs = randomCorpus(0xF00DL, 2_000, 200, 20);
    writeSegment(file, docs);

    try (final SparseSegmentReader r = new SparseSegmentReader(file)) {
      final SparseSegmentReader[] segs = { r };
      runComparisons(segs, 200, new int[] { 1, 10, 50, 100 }, 0xBA5EL);
    }
  }

  // ---------- helpers ----------

  /** Run several random queries, each compared between BMW and brute-force. */
  private static void runComparisons(final SparseSegmentReader[] segs, final int dims, final int[] ks, final long seed)
      throws IOException {
    final Random rnd = new Random(seed);
    for (final int k : ks) {
      for (int q = 0; q < 5; q++) {
        final int qDims = 3 + rnd.nextInt(5);
        final int[] queryDims = new int[qDims];
        final float[] queryWeights = new float[qDims];
        final java.util.HashSet<Integer> seen = new java.util.HashSet<>();
        int filled = 0;
        while (filled < qDims) {
          final int d = rnd.nextInt(dims);
          if (seen.add(d)) {
            queryDims[filled] = d;
            queryWeights[filled] = 0.1f + rnd.nextFloat();
            filled++;
          }
        }
        final List<RidScore> bmw = runBmw(segs, queryDims, queryWeights, k);
        final List<RidScore> bf = BruteForceScorer.topK(queryDims, queryWeights, segs, k);
        assertEquivalent(bmw, bf);
      }
    }
  }

  private static List<RidScore> runBmw(final SparseSegmentReader[] segs, final int[] queryDims, final float[] queryWeights,
      final int k) throws IOException {
    final DimCursor[] cursors = new DimCursor[queryDims.length];
    try {
      for (int i = 0; i < queryDims.length; i++)
        cursors[i] = DimCursor.open(queryDims[i], segs);
      return BmwScorer.topK(queryDims, queryWeights, cursors, k);
    } finally {
      for (final DimCursor c : cursors)
        if (c != null)
          c.close();
    }
  }

  /**
   * BMW vs brute force. Top-K is ambiguous when scores tie at the boundary, so we verify the
   * <i>score sequences</i> match (sorted descending) within tolerance, and the high-score
   * intersection is large enough to rule out a real correctness bug.
   */
  private static void assertEquivalent(final List<RidScore> bmw, final List<RidScore> bf) {
    assertThat(bmw).hasSize(bf.size());

    final List<Float> bmwScores = new ArrayList<>(bmw.size());
    for (final RidScore r : bmw)
      bmwScores.add(r.score());
    final List<Float> bfScores = new ArrayList<>(bf.size());
    for (final RidScore r : bf)
      bfScores.add(r.score());
    bmwScores.sort((a, b) -> Float.compare(b, a));
    bfScores.sort((a, b) -> Float.compare(b, a));
    for (int i = 0; i < bmwScores.size(); i++)
      assertThat(bmwScores.get(i)).isCloseTo(bfScores.get(i), Offset.offset(SCORE_TOL));

    // The strict-top region (anything strictly above the K-th score) must be RID-identical.
    if (!bmwScores.isEmpty()) {
      final float kthScore = bmwScores.getLast();
      final java.util.Set<RID> bmwStrict = new java.util.HashSet<>();
      for (final RidScore r : bmw)
        if (r.score() > kthScore + SCORE_TOL)
          bmwStrict.add(r.rid());
      final java.util.Set<RID> bfStrict = new java.util.HashSet<>();
      for (final RidScore r : bf)
        if (r.score() > kthScore + SCORE_TOL)
          bfStrict.add(r.rid());
      assertThat(bmwStrict).isEqualTo(bfStrict);
    }
  }

  /** Random corpus: docCount RIDs, each with `nnz` random dims (0..dims) of random weights. */
  private static Map<RID, Map<Integer, Float>> randomCorpus(final long seed, final int docCount, final int dims, final int nnz) {
    final Random rnd = new Random(seed);
    final Map<RID, Map<Integer, Float>> out = new TreeMap<>();
    for (int d = 0; d < docCount; d++) {
      final RID rid = new RID(0, d * 100L + 1);
      final Map<Integer, Float> doc = new TreeMap<>();
      while (doc.size() < nnz) {
        final int dim = rnd.nextInt(dims);
        doc.put(dim, 0.1f + rnd.nextFloat());
      }
      out.put(rid, doc);
    }
    return out;
  }

  /** Write a corpus as one sealed segment in the layout the writer expects (dim asc, RID asc). */
  private static void writeSegment(final Path file, final Map<RID, Map<Integer, Float>> docs) throws IOException {
    // Pivot: per-dim sorted RIDs.
    final TreeMap<Integer, TreeMap<RID, Float>> byDim = new TreeMap<>();
    for (final var e : docs.entrySet()) {
      for (final var dw : e.getValue().entrySet()) {
        byDim.computeIfAbsent(dw.getKey(), k -> new TreeMap<>()).put(e.getKey(), dw.getValue());
      }
    }
    try (final SparseSegmentWriter w = new SparseSegmentWriter(file, SegmentParameters.defaults())) {
      for (final var e : byDim.entrySet()) {
        w.startDim(e.getKey());
        for (final var p : e.getValue().entrySet())
          w.appendPosting(p.getKey(), p.getValue());
        w.endDim();
      }
      w.finish();
    }
  }
}
