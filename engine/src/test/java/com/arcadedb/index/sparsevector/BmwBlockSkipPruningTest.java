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

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression for issue #5388: {@link BmwScorer} degenerated into an exhaustive DAAT pass on flat
 * weight distributions (real SPLADE vectors), with latency tracking total posting length rather
 * than the top-K. The root cause was that the scorer only ever consulted the loose suffix-max
 * upper bound ({@link PaginatedSegmentDimCursor#upperBoundRemaining()}) for its WAND pivot and
 * never used the tight per-block maxima that the segment already stores in every block header, so
 * the Block-Max WAND block-skip step was effectively missing.
 * <p>
 * This test builds a corpus whose per-<b>term</b> suffix-max stays high across the whole posting
 * list (a heavy "decoy" posting sits near the tail of every query dim) while every block in the
 * long body has a tiny per-block max. Plain WAND cannot prune such a list - the suffix-max keeps
 * its pivot alive to the very end - so it would decode nearly every block. Block-Max WAND skips
 * the whole body because each block's tight max sum falls far under the top-K threshold. The test
 * asserts both that the results still match brute force and that the fraction of blocks actually
 * decoded is a small fraction of the segment, which only holds once block-skip works.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class BmwBlockSkipPruningTest extends TestHelper {

  /** FP32 so weights round-trip exactly and scores match brute force without quantization noise. */
  private static final SegmentParameters EXACT_PARAMS = SegmentParameters.builder()
      .weightQuantization(WeightQuantization.FP32)
      .build();

  private static final int   QUERY_DIMS  = 8;
  private static final int   COLD_DOCS   = 4_000;
  private static final float HOT_WEIGHT  = 1.0f;   // 6 hot docs -> score 8.0, the top-K
  private static final float COLD_WEIGHT = 0.05f;  // flat body: block-max sum 8*0.05 = 0.4
  private static final float DECOY_WEIGHT = 1.2f;   // per-dim tail spike: keeps suffix-max high

  @Test
  void blockSkipPrunesFlatBodyAndMatchesBruteForce() throws Exception {
    final Map<RID, Map<Integer, Float>> docs = buildAdversarialCorpus();

    final List<PaginatedSegmentReader> readers = new ArrayList<>();
    inTx(() -> readers.add(buildSegment("seg-5388", 1L, docs)));

    final int[] queryDims = new int[QUERY_DIMS];
    final float[] queryWeights = new float[QUERY_DIMS];
    for (int d = 0; d < QUERY_DIMS; d++) {
      queryDims[d] = d;
      queryWeights[d] = 1.0f;
    }
    final int k = 6;

    inTx(() -> {
      // Open the per-dim source cursors ourselves so we can read their decoded-block counters
      // after the scan; the scorer merges each into a single-source DimCursor transparently.
      final PaginatedSegmentReader reader = readers.getFirst();
      final PaginatedSegmentDimCursor[] sources = new PaginatedSegmentDimCursor[QUERY_DIMS];
      final DimCursor[] cursors = new DimCursor[QUERY_DIMS];
      long totalBlocks = 0;
      try {
        for (int i = 0; i < QUERY_DIMS; i++) {
          sources[i] = reader.openCursor(queryDims[i]);
          cursors[i] = new DimCursor(queryDims[i], List.of(sources[i]));
          totalBlocks += sources[i].metadata().blockCount();
        }

        final List<RidScore> bmw = BmwScorer.topK(queryDims, queryWeights, cursors, k);

        long decodedBlocks = 0;
        for (final PaginatedSegmentDimCursor c : sources)
          decodedBlocks += c.decodedBlockCount();

        // Correctness: identical top-K to the brute-force reference.
        final List<RidScore> bf = BruteForceScorer.topK(queryDims, queryWeights,
            readers.toArray(new PaginatedSegmentReader[0]), k);
        assertThat(bmw).hasSize(bf.size());
        for (int i = 0; i < bmw.size(); i++) {
          assertThat(bmw.get(i).rid()).isEqualTo(bf.get(i).rid());
          assertThat(bmw.get(i).score()).isCloseTo(bf.get(i).score(), org.assertj.core.data.Offset.offset(1e-4f));
        }

        // The corpus is large enough that an exhaustive DAAT would decode hundreds of blocks.
        assertThat(totalBlocks).isGreaterThan(200L);
        // With block-skip working, only the head blocks that set the top-K threshold (one per query
        // dim) plus at most a handful of landing blocks are decoded; the entire flat body is skipped
        // consulting in-memory headers alone. Without the fix, plain WAND's suffix-max keeps its
        // pivot alive across the whole list and it decodes essentially every block (256/256 here).
        assertThat(decodedBlocks)
            .as("decoded %d of %d blocks", decodedBlocks, totalBlocks)
            .isLessThanOrEqualTo((long) QUERY_DIMS * 4);
      } finally {
        for (final DimCursor c : cursors)
          if (c != null)
            c.close();
      }
    });
  }

  @Test
  void groupedBlockSkipPrunesFlatBodyAndMatchesBruteForce() throws Exception {
    final Map<RID, Map<Integer, Float>> docs = buildAdversarialCorpus();

    final List<PaginatedSegmentReader> readers = new ArrayList<>();
    inTx(() -> readers.add(buildSegment("seg-5388-grouped", 1L, docs)));

    final int[] queryDims = new int[QUERY_DIMS];
    final float[] queryWeights = new float[QUERY_DIMS];
    for (int d = 0; d < QUERY_DIMS; d++) {
      queryDims[d] = d;
      queryWeights[d] = 1.0f;
    }
    final int k = 6;

    inTx(() -> {
      final PaginatedSegmentReader reader = readers.getFirst();
      final PaginatedSegmentDimCursor[] sources = new PaginatedSegmentDimCursor[QUERY_DIMS];
      final DimCursor[] cursors = new DimCursor[QUERY_DIMS];
      long totalBlocks = 0;
      try {
        for (int i = 0; i < QUERY_DIMS; i++) {
          sources[i] = reader.openCursor(queryDims[i]);
          cursors[i] = new DimCursor(queryDims[i], List.of(sources[i]));
          totalBlocks += sources[i].metadata().blockCount();
        }

        // A single group with capacity k reduces the grouped scorer to plain top-K: the threshold
        // is NEGATIVE_INFINITY until the group fills, then tracks the group's worst score, so BMW
        // block-skip must engage exactly as in the non-grouped path.
        final List<RidScore> bmw = BmwScorer.topKGrouped(queryDims, queryWeights, cursors, 1, k, rid -> "g", null);

        long decodedBlocks = 0;
        for (final PaginatedSegmentDimCursor c : sources)
          decodedBlocks += c.decodedBlockCount();

        final List<RidScore> bf = BruteForceScorer.topK(queryDims, queryWeights,
            readers.toArray(new PaginatedSegmentReader[0]), k);
        assertThat(bmw).hasSize(bf.size());
        for (int i = 0; i < bmw.size(); i++) {
          assertThat(bmw.get(i).rid()).isEqualTo(bf.get(i).rid());
          assertThat(bmw.get(i).score()).isCloseTo(bf.get(i).score(), org.assertj.core.data.Offset.offset(1e-4f));
        }
        assertThat(totalBlocks).isGreaterThan(200L);
        assertThat(decodedBlocks)
            .as("grouped decoded %d of %d blocks", decodedBlocks, totalBlocks)
            .isLessThanOrEqualTo((long) QUERY_DIMS * 4);
      } finally {
        for (final DimCursor c : cursors)
          if (c != null)
            c.close();
      }
    });
  }

  /**
   * Six heavy docs at the head (they set a high top-K threshold quickly), a long flat body, and one
   * heavy "decoy" posting per query dim spread across the tail so every term's suffix-max stays
   * high end-to-end. No decoy doc individually beats the head threshold, so the top-K is the six
   * head docs.
   */
  private Map<RID, Map<Integer, Float>> buildAdversarialCorpus() {
    final TreeMap<RID, Map<Integer, Float>> out = new TreeMap<>();

    // Head: 6 hot docs, RIDs (0,1)..(0,6), heavy in every query dim.
    for (int h = 1; h <= 6; h++) {
      final TreeMap<Integer, Float> doc = new TreeMap<>();
      for (int d = 0; d < QUERY_DIMS; d++)
        doc.put(d, HOT_WEIGHT);
      out.put(new RID(0, h), doc);
    }

    // Body: flat cold docs, RIDs (0,1000)..(0,1000+COLD_DOCS-1), tiny weight in every query dim.
    for (int c = 0; c < COLD_DOCS; c++) {
      final TreeMap<Integer, Float> doc = new TreeMap<>();
      for (int d = 0; d < QUERY_DIMS; d++)
        doc.put(d, COLD_WEIGHT);
      out.put(new RID(0, 1000L + c), doc);
    }

    // Tail decoys: boost dim d on a distinct doc near the end so each term keeps a high suffix-max
    // while the block that holds the spike still has a small per-block max sum overall.
    for (int d = 0; d < QUERY_DIMS; d++) {
      final RID rid = new RID(0, 1000L + COLD_DOCS - 1 - d * 137L);
      out.get(rid).put(d, DECOY_WEIGHT);
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
