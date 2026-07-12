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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression coverage for issue #5254: {@code LSM_SPARSE_VECTOR} used to require every per-dim
 * trailer (dim header + block locators + skip list) to fit within a single page. A dim dense enough
 * that its trailer overflows one page (&gt;~986k postings at the 64 KiB / blockSize-128 default -
 * reached on real learned-sparse corpora like the Big-ANN SPLADE base_full set, ~1.1B total
 * postings) threw {@code BufferOverflowException} from {@code SparseSegmentBuilder.writeDimTrailer}
 * during {@code compact()}. The overflow is quantization-independent (block count depends on posting
 * count, not weight width); the issue observed it on FP32 only because its 4 B/posting weights pushed
 * that corpus over the threshold that INT8 stayed under.
 * <p>
 * The builder now streams a trailer larger than one page across consecutive pages and the reader
 * reassembles it, so an arbitrarily dense dim round-trips. These tests pin that at a page/block size
 * small enough to reach the spanning threshold with a modest posting count in CI, plus one
 * default-page-size test (tagged {@code slow}) that mirrors the reporter's exact geometry.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SparseSegmentWideDimTest extends TestHelper {

  // Small page + tiny block so a dim of a few tens of thousands of postings already produces a
  // trailer several pages long, exercising the spanning path without a huge dataset.
  private static final int PAGE_SIZE  = 4096;
  private static final int BLOCK_SIZE = 16;

  /**
   * Build a segment holding narrow dims on both sides of one very wide dim, for each quantization,
   * then reopen it and verify every posting of every dim round-trips. The wide dim's trailer spans
   * several pages; the narrow dim written after it proves the builder's page cursor stays consistent
   * once a spanning trailer has been emitted and that the dim_index still resolves later dims.
   */
  @Test
  void wideDimTrailerSpansPagesAndRoundTrips() throws Exception {
    for (final WeightQuantization q : new WeightQuantization[] { WeightQuantization.FP32, WeightQuantization.INT8 })
      buildAndVerify(q, /* wideDimPostings */ 50_000);
  }

  /**
   * The reporter's exact geometry: default 64 KiB page, default blockSize 128, a single dim past the
   * ~986k-posting one-page-trailer ceiling. Tagged {@code slow} (≈1M appends) so CI skips it, but it
   * reproduces the original {@code BufferOverflowException} on a pre-fix build and passes on this one.
   */
  @Test
  @Tag("slow")
  void wideDimAtDefaultPageSizeReproducesIssue() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final int postings = 1_200_000; // > ~986k one-page-trailer ceiling at 64 KiB / blockSize 128
    final SegmentParameters params = SegmentParameters.builder()
        .pageSize(65536).blockSize(128).weightQuantization(WeightQuantization.FP32).build();
    final SparseSegmentComponent component = newComponent("default-wide", 65536);

    try (final SparseSegmentBuilder b = new SparseSegmentBuilder(component, params, 8L * 1024 * 1024)) {
      b.setSegmentId(1L);
      b.startDim(7);
      for (int i = 0; i < postings; i++)
        b.appendPosting(new RID(0, i + 1L), 0.25f + (i % 97) * 0.01f);
      b.endDim();
      b.finish();
    }

    final PaginatedSegmentReader reader = new PaginatedSegmentReader(component);
    assertThat(reader.totalDims()).isEqualTo(1);
    assertThat(reader.totalPostings()).isEqualTo(postings);
    try (final PaginatedSegmentDimCursor c = reader.openCursor(7)) {
      int seen = 0;
      while (c.advance()) {
        assertThat(c.currentRid()).isEqualTo(new RID(0, seen + 1L));
        assertThat(c.currentWeight()).isEqualTo(0.25f + (seen % 97) * 0.01f);
        seen++;
      }
      assertThat(seen).isEqualTo(postings);
    }
  }

  /**
   * End-to-end through the engine (put → flush → compactAll) at a small page size so compaction of a
   * dim with a few tens of thousands of postings builds a spanning trailer, then verify top-K is
   * still correct - the exact path (compact()) that failed in the issue.
   */
  @Test
  void engineCompactionOfWideDimStreamsSpanningTrailer() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final SegmentParameters params = SegmentParameters.builder()
        .pageSize(PAGE_SIZE).blockSize(BLOCK_SIZE).weightQuantization(WeightQuantization.FP32).build();
    final int half = 25_000; // 50k postings on dim 0 after compaction -> spanning trailer

    try (final PaginatedSparseVectorEngine engine = new PaginatedSparseVectorEngine(
        db, "WideDimEngine", params,
        /* memtableFlushThreshold */ 10_000_000L, /* tierFanout */ 4, /* tierBasePostings */ 1_000_000L)) {

      for (int i = 0; i < half; i++)
        engine.put(0, new RID(0, i + 1L), 0.001f * (i + 1));
      assertThat(engine.flush()).isGreaterThan(-1L);

      for (int i = half; i < 2 * half; i++)
        engine.put(0, new RID(0, i + 1L), 0.001f * (i + 1));
      assertThat(engine.flush()).isGreaterThan(-1L);
      assertThat(engine.segmentCount()).isEqualTo(2);

      assertThat(engine.compactAll()).as("compactAll must merge into one spanning-trailer segment").isGreaterThan(-1L);
      assertThat(engine.segmentCount()).isEqualTo(1);
      assertThat(engine.totalPostings()).isEqualTo(2L * half);

      // Weights increase with position, so the top-3 are the three highest positions.
      final List<RidScore> top = engine.topK(new int[] { 0 }, new float[] { 1.0f }, 3);
      assertThat(top).hasSize(3);
      assertThat(top.get(0).rid()).isEqualTo(new RID(0, 2L * half));
      assertThat(top.get(1).rid()).isEqualTo(new RID(0, 2L * half - 1));
      assertThat(top.get(2).rid()).isEqualTo(new RID(0, 2L * half - 2));
    }
  }

  private void buildAndVerify(final WeightQuantization q, final int wideDimPostings) throws IOException {
    final SegmentParameters params = SegmentParameters.builder()
        .pageSize(PAGE_SIZE).blockSize(BLOCK_SIZE).weightQuantization(q).build();
    final SparseSegmentComponent component = newComponent("wide-" + q, PAGE_SIZE);

    final int narrow = 100;
    // Dims in strictly ascending order: two narrow, one very wide, one narrow after it.
    final int[] dims        = { 0, 1, 2, 3 };
    final int[] dimPostings = { narrow, narrow, wideDimPostings, narrow };

    try (final SparseSegmentBuilder b = new SparseSegmentBuilder(component, params, /* maxUnflushedBytes */ 64L * 1024)) {
      b.setSegmentId(11L);
      for (int d = 0; d < dims.length; d++) {
        b.startDim(dims[d]);
        for (int i = 0; i < dimPostings[d]; i++)
          b.appendPosting(new RID(0, i + 1L), weightFor(dims[d], i));
        b.endDim();
      }
      b.finish();
    }

    final PaginatedSegmentReader reader = new PaginatedSegmentReader(component);
    assertThat(reader.totalDims()).isEqualTo(dims.length);
    long expectedTotal = 0;
    for (final int pc : dimPostings)
      expectedTotal += pc;
    assertThat(reader.totalPostings()).as("quant %s", q).isEqualTo(expectedTotal);

    for (int d = 0; d < dims.length; d++) {
      final int dim = dims[d];
      final int expected = dimPostings[d];
      assertThat(reader.hasDim(dim)).as("dim %d present (quant %s)", dim, q).isTrue();
      try (final PaginatedSegmentDimCursor c = reader.openCursor(dim)) {
        int seen = 0;
        while (c.advance()) {
          assertThat(c.currentRid()).as("dim %d posting %d rid (quant %s)", dim, seen, q).isEqualTo(new RID(0, seen + 1L));
          assertThat(c.isTombstone()).isFalse();
          if (q == WeightQuantization.FP32) // FP32 is lossless; INT8 is lossy so only check RID/count there
            assertThat(c.currentWeight()).as("dim %d posting %d weight", dim, seen).isEqualTo(weightFor(dim, seen));
          seen++;
        }
        assertThat(seen).as("dim %d posting count (quant %s)", dim, q).isEqualTo(expected);
      }
    }
  }

  private static float weightFor(final int dim, final int i) {
    return 0.01f + ((dim * 31 + i) % 97) * 0.01f;
  }

  private SparseSegmentComponent newComponent(final String name, final int pageSize) {
    final DatabaseInternal db = (DatabaseInternal) database;
    final String filePath = db.getDatabasePath() + "/" + name;
    try {
      final SparseSegmentComponent c = new SparseSegmentComponent(db, name, filePath, ComponentFile.MODE.READ_WRITE, pageSize);
      ((LocalSchema) db.getSchema().getEmbedded()).registerFile(c);
      return c;
    } catch (final IOException e) {
      throw new RuntimeException("failed to create sparse segment component '" + name + "'", e);
    }
  }
}
