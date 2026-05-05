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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * BlockMax-WAND scoring correctness against a brute-force baseline, exercised over the
 * page-component-backed segment format. Builds segments via {@link SparseSegmentBuilder} inside
 * real ArcadeDB transactions, opens them via {@link PaginatedSegmentReader}, and verifies that
 * {@link BmwScorer} agrees with {@link BruteForceScorer} across single-segment, multi-segment,
 * tombstone, and varying-K scenarios.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class BmwScorerCorrectnessTest extends TestHelper {

  /** FP32 quantization so per-posting weights round-trip exactly between the two scorers. */
  private static final SegmentParameters EXACT_PARAMS = SegmentParameters.builder()
      .weightQuantization(WeightQuantization.FP32)
      .build();

  /** Score-equality tolerance: int8 quantization noise on partial sums. */
  private static final float SCORE_TOL = 1e-2f;

  @Test
  void singleSegmentBmwMatchesBruteForce() throws Exception {
    final Map<RID, Map<Integer, Float>> docs = randomCorpus(0xCAFEL, 200, 30, 5);
    final List<PaginatedSegmentReader> readers = new ArrayList<>();
    inTx(() -> readers.add(buildSegment("seg-1", 1L, docs)));
    inTx(() -> runComparisons(readers, 30, new int[] { 1, 5, 10, 25 }, 0xBEEFL));
  }

  @Test
  void multipleSegmentsNewerOverridesOlder() throws Exception {
    final TreeMap<RID, Map<Integer, Float>> oldDocs = new TreeMap<>();
    oldDocs.put(new RID(0, 1), Map.of(0, 0.5f, 1, 0.3f));
    oldDocs.put(new RID(0, 2), Map.of(1, 0.7f, 2, 0.4f));
    oldDocs.put(new RID(0, 3), Map.of(0, 0.2f, 2, 0.9f));

    final TreeMap<RID, Map<Integer, Float>> newDocs = new TreeMap<>();
    newDocs.put(new RID(0, 1), Map.of(0, 0.99f));    // overrides old (0,1)/dim 0
    newDocs.put(new RID(0, 4), Map.of(0, 0.8f, 1, 0.6f));

    final List<PaginatedSegmentReader> readers = new ArrayList<>();
    inTx(() -> readers.add(buildSegment("seg-old", 1L, oldDocs)));
    inTx(() -> readers.add(buildSegment("seg-new", 2L, newDocs)));

    inTx(() -> {
      final List<RidScore> bmw = runBmw(readers, new int[] { 0 }, new float[] { 1.0f }, 5);
      assertThat(bmw).isNotEmpty();
      assertThat(bmw.getFirst().rid()).isEqualTo(new RID(0, 1));
      assertThat(bmw.getFirst().score()).isCloseTo(0.99f, Offset.offset(SCORE_TOL));

      final List<RidScore> bf = BruteForceScorer.topK(new int[] { 0 }, new float[] { 1.0f },
          readers.toArray(new PaginatedSegmentReader[0]), 5);
      assertEquivalent(bmw, bf);
    });
  }

  @Test
  void tombstoneInNewerSegmentMasksDoc() throws Exception {
    final TreeMap<RID, Map<Integer, Float>> oldDocs = new TreeMap<>();
    oldDocs.put(new RID(0, 1), Map.of(0, 0.9f, 1, 0.8f));
    oldDocs.put(new RID(0, 2), Map.of(0, 0.5f));

    final List<PaginatedSegmentReader> readers = new ArrayList<>();
    inTx(() -> readers.add(buildSegment("seg-old", 1L, oldDocs)));
    inTx(() -> {
      final SparseSegmentComponent c = newComponent("seg-tomb");
      try (final SparseSegmentBuilder b = new SparseSegmentBuilder(c, EXACT_PARAMS)) {
        b.setSegmentId(2L);
        b.startDim(0);
        b.appendTombstone(new RID(0, 1));
        b.endDim();
        b.finish();
      }
      readers.add(new PaginatedSegmentReader(c));
    });

    inTx(() -> {
      final List<RidScore> bmw = runBmw(readers, new int[] { 0 }, new float[] { 1.0f }, 10);
      // (0,1) is tombstoned by the newer segment, so only (0,2) survives.
      assertThat(bmw).hasSize(1);
      assertThat(bmw.getFirst().rid()).isEqualTo(new RID(0, 2));

      final List<RidScore> bf = BruteForceScorer.topK(new int[] { 0 }, new float[] { 1.0f },
          readers.toArray(new PaginatedSegmentReader[0]), 10);
      assertEquivalent(bmw, bf);
    });
  }

  @Tag("slow")
  @Test
  void largeRandomCorpusMatchesBruteForce() throws Exception {
    final Map<RID, Map<Integer, Float>> docs = randomCorpus(0xF00DL, 2_000, 200, 20);
    final List<PaginatedSegmentReader> readers = new ArrayList<>();
    inTx(() -> readers.add(buildSegment("seg-large", 1L, docs)));
    inTx(() -> runComparisons(readers, 200, new int[] { 1, 10, 50, 100 }, 0xBA5EL));
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

  /** Build a sealed segment from a per-RID map of (dim -> weight) and return a reader on it. */
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

  private List<RidScore> runBmw(final List<PaginatedSegmentReader> readers, final int[] queryDims, final float[] queryWeights,
      final int k) throws IOException {
    final DimCursor[] cursors = new DimCursor[queryDims.length];
    try {
      for (int i = 0; i < queryDims.length; i++)
        cursors[i] = openMergedCursor(queryDims[i], readers);
      return BmwScorer.topK(queryDims, queryWeights, cursors, k);
    } finally {
      for (final DimCursor c : cursors)
        if (c != null)
          c.close();
    }
  }

  private DimCursor openMergedCursor(final int dim, final List<PaginatedSegmentReader> readers) throws IOException {
    final List<SourceCursor> sources = new ArrayList<>(readers.size());
    for (final PaginatedSegmentReader r : readers) {
      final PaginatedSegmentDimCursor c = r.openCursor(dim);
      if (c != null)
        sources.add(c);
    }
    return sources.isEmpty() ? null : new DimCursor(dim, sources);
  }

  private void runComparisons(final List<PaginatedSegmentReader> readers, final int dims, final int[] ks, final long seed)
      throws IOException {
    final Random rnd = new Random(seed);
    for (final int k : ks) {
      for (int q = 0; q < 5; q++) {
        final int qDims = 3 + rnd.nextInt(5);
        final int[] queryDims = new int[qDims];
        final float[] queryWeights = new float[qDims];
        final HashSet<Integer> seen = new HashSet<>();
        int filled = 0;
        while (filled < qDims) {
          final int d = rnd.nextInt(dims);
          if (seen.add(d)) {
            queryDims[filled] = d;
            queryWeights[filled] = 0.1f + rnd.nextFloat();
            filled++;
          }
        }
        final List<RidScore> bmw = runBmw(readers, queryDims, queryWeights, k);
        final List<RidScore> bf = BruteForceScorer.topK(queryDims, queryWeights, readers.toArray(new PaginatedSegmentReader[0]), k);
        assertEquivalent(bmw, bf);
      }
    }
  }

  /**
   * BMW vs brute force. Top-K is ambiguous when scores tie at the boundary, so we verify the
   * <i>score sequences</i> match (sorted descending) within tolerance, and the strict-top
   * intersection (anything above the K-th score) is RID-identical.
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

    if (bmwScores.isEmpty())
      return;
    final float kthScore = bmwScores.getLast();
    final HashSet<RID> bmwStrict = new HashSet<>();
    for (final RidScore r : bmw)
      if (r.score() > kthScore + SCORE_TOL)
        bmwStrict.add(r.rid());
    final HashSet<RID> bfStrict = new HashSet<>();
    for (final RidScore r : bf)
      if (r.score() > kthScore + SCORE_TOL)
        bfStrict.add(r.rid());
    assertThat(bmwStrict).isEqualTo(bfStrict);
  }

  private static Map<RID, Map<Integer, Float>> randomCorpus(final long seed, final int docCount, final int dims, final int nnz) {
    final Random rnd = new Random(seed);
    final TreeMap<RID, Map<Integer, Float>> out = new TreeMap<>();
    for (int d = 0; d < docCount; d++) {
      final RID rid = new RID(0, d * 100L + 1);
      final TreeMap<Integer, Float> doc = new TreeMap<>();
      while (doc.size() < nnz) {
        final int dim = rnd.nextInt(dims);
        doc.put(dim, 0.1f + rnd.nextFloat());
      }
      out.put(rid, doc);
    }
    return out;
  }
}
