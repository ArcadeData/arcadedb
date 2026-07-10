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
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.index.sparsevector.SegmentFormat.WeightQuantization;
import com.arcadedb.schema.LocalSchema;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression coverage for issue #5189: {@code LSM_SPARSE_VECTOR} flush/compaction used to build a
 * whole segment inside a single {@code database.transaction(...)}, which capped a segment at the
 * {@code WALFile} 2 GB per-transaction ceiling (FP32 hit it ~4x sooner than INT8) and held every
 * page in heap until commit. The builder now streams pages straight to disk in RAM-bounded chunks
 * outside any transaction. These tests pin that behaviour at a scale small enough for CI:
 * <ul>
 *   <li>a segment spanning many pages builds and round-trips correctly with a <b>one-page</b> flush
 *       budget and <b>no enclosing transaction at all</b> (the old code could not persist a page
 *       without an active transaction, so this exercises exactly what changed);</li>
 *   <li>the engine's flush + compaction path produces a correct top-K after streaming a segment
 *       under a minimal {@code INDEX_COMPACTION_RAM_MB} budget.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SparseSegmentChunkedBuildTest extends TestHelper {

  // Small page + small block so a modest posting count still spans many pages (and therefore many
  // one-page flush chunks) without needing a large dataset in CI.
  private static final int PAGE_SIZE  = 4096;
  private static final int BLOCK_SIZE = 16;

  private static final SegmentParameters EXACT_PARAMS = SegmentParameters.builder()
      .pageSize(PAGE_SIZE)
      .blockSize(BLOCK_SIZE)
      .weightQuantization(WeightQuantization.FP32) // exact weight equality on read-back
      .build();

  /**
   * Build a multi-page segment with a one-page unflushed budget and <b>no transaction</b>, then
   * reopen it and verify every posting round-trips. If the builder still needed an ambient
   * transaction (the pre-#5189 design) nothing would persist and the reader would fail; if it
   * buffered the whole segment in heap the one-page budget would be meaningless. The large final
   * page count proves the segment really did span far more than one chunk.
   */
  @Test
  void builderStreamsMultiPageSegmentWithoutTransaction() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final int dims = 100;
    final int postingsPerDim = 300;

    final SparseSegmentComponent component = newComponent("chunked-seg-1");

    // Deliberately NOT wrapped in database.transaction(): the streaming builder owns its own
    // durability via writePages(), so it must not depend on an active transaction.
    try (final SparseSegmentBuilder b = new SparseSegmentBuilder(component, EXACT_PARAMS, /* maxUnflushedBytes */ 1L)) {
      b.setSegmentId(7L);
      for (int d = 0; d < dims; d++) {
        b.startDim(d);
        for (int i = 0; i < postingsPerDim; i++)
          b.appendPosting(new RID(0, i + 1L), weightFor(d, i));
        b.endDim();
      }
      b.finish();
    }

    assertThat(component.getTotalPages())
        .as("a %d-posting segment must span many 4 KiB pages, proving repeated one-page flushes", dims * postingsPerDim)
        .isGreaterThan(50);

    final PaginatedSegmentReader reader = new PaginatedSegmentReader(component);
    assertThat(reader.totalDims()).isEqualTo(dims);
    assertThat(reader.totalPostings()).isEqualTo((long) dims * postingsPerDim);
    assertThat(reader.segmentId()).isEqualTo(7L);

    // Full round-trip check on a sample of dims spread across the segment.
    for (final int d : new int[] { 0, 1, 37, 50, 99 }) {
      assertThat(reader.hasDim(d)).isTrue();
      try (final PaginatedSegmentDimCursor c = reader.openCursor(d)) {
        for (int i = 0; i < postingsPerDim; i++) {
          assertThat(c.advance()).as("dim %d posting %d must be present", d, i).isTrue();
          assertThat(c.currentRid()).isEqualTo(new RID(0, i + 1L));
          assertThat(c.isTombstone()).isFalse();
          assertThat(c.currentWeight()).isEqualTo(weightFor(d, i));
        }
        assertThat(c.advance()).as("dim %d must be exhausted after %d postings", d, postingsPerDim).isFalse();
      }
    }
  }

  /**
   * Drive the full engine path (flush then compactAll) with the smallest possible
   * {@code INDEX_COMPACTION_RAM_MB} budget so the segment build streams in chunks, and verify the
   * top-K is exactly right afterwards. Guards the removal of the single-transaction wrapper end to
   * end plus the config-derived chunk budget wiring.
   */
  @Test
  void engineFlushAndCompactUnderMinimalRamBudgetKeepsTopKCorrect() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final Object previousRam = GlobalConfiguration.INDEX_COMPACTION_RAM_MB.getValue();
    GlobalConfiguration.INDEX_COMPACTION_RAM_MB.setValue(1); // 1 MB → tiny chunk budget
    try (final PaginatedSparseVectorEngine engine = new PaginatedSparseVectorEngine(
        db, "ChunkedBuildEngine", EXACT_PARAMS,
        /* memtableFlushThreshold */ 10_000_000L, // far above the test's posting count: no auto-flush
        /* tierFanout */ 4,
        /* tierBasePostings */ 1_000_000L)) {

      final int postings = 4_000;
      // Ascending RIDs on dim 0 with weights strictly increasing in position, so the highest RID is
      // the unambiguous top score and rank is easy to assert exactly.
      for (int i = 0; i < postings; i++)
        engine.put(0, new RID(0, i + 1L), 0.001f * (i + 1));

      assertThat(engine.flush()).as("flush of a full memtable must seal a segment").isGreaterThan(-1L);
      assertThat(engine.segmentCount()).isEqualTo(1);
      assertThat(engine.totalPostings()).isEqualTo(postings);

      // compactAll on a single segment is a no-op; add a second segment so compaction actually
      // streams a merged segment under the tiny budget.
      for (int i = postings; i < postings + 2_000; i++)
        engine.put(0, new RID(0, i + 1L), 0.001f * (i + 1));
      engine.flush();
      assertThat(engine.segmentCount()).isEqualTo(2);

      assertThat(engine.compactAll()).as("compactAll must merge the two segments").isGreaterThan(-1L);
      assertThat(engine.segmentCount()).isEqualTo(1);
      assertThat(engine.totalPostings()).isEqualTo(postings + 2_000L);

      // Top-3 on dim 0 must be the three highest positions (weights increase with position).
      final List<RidScore> top = engine.topK(new int[] { 0 }, new float[] { 1.0f }, 3);
      assertThat(top).hasSize(3);
      assertThat(top.get(0).rid()).isEqualTo(new RID(0, postings + 2_000L));
      assertThat(top.get(1).rid()).isEqualTo(new RID(0, postings + 2_000L - 1));
      assertThat(top.get(2).rid()).isEqualTo(new RID(0, postings + 2_000L - 2));
    } finally {
      GlobalConfiguration.INDEX_COMPACTION_RAM_MB.setValue(previousRam);
    }
  }

  /**
   * Because the streaming builder writes outside a transaction, a hard crash mid-build can leave a
   * partial segment file whose page-0 header was never back-patched. On reopen the engine must skip
   * that never-published orphan (not throw) and must not reuse its id for a fresh segment. Simulate
   * the orphan with a segment file whose page 0 is all zeros (bad magic → reader fails).
   */
  @Test
  void reopenSkipsCrashOrphanAndDoesNotReuseItsId() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final String indexName = "CrashOrphanEngine";

    // 1) A valid segment (id 1) sealed through the normal engine path.
    try (final PaginatedSparseVectorEngine engine = new PaginatedSparseVectorEngine(
        db, indexName, EXACT_PARAMS, 10_000_000L, 4, 1_000_000L)) {
      for (int i = 0; i < 200; i++)
        engine.put(0, new RID(0, i + 1L), 0.01f * (i + 1));
      assertThat(engine.flush()).isEqualTo(1L);
    }

    // 2) An orphan "id 5" file with a corrupt (all-zero) page 0, mimicking a crash-interrupted build.
    writeCorruptSegmentFile(indexName + "_seg5");

    // 3) Reopen: the orphan is skipped, the valid segment still serves queries, and the next flush
    //    lands ABOVE the orphan id (6+), never colliding with the orphan's on-disk file name.
    try (final PaginatedSparseVectorEngine reopened = new PaginatedSparseVectorEngine(
        db, indexName, EXACT_PARAMS, 10_000_000L, 4, 1_000_000L)) {
      assertThat(reopened.segmentCount()).as("orphan must be skipped, valid segment kept").isEqualTo(1);
      assertThat(reopened.topK(new int[] { 0 }, new float[] { 1.0f }, 1)).hasSize(1);

      for (int i = 0; i < 50; i++)
        reopened.put(1, new RID(0, i + 1L), 0.02f * (i + 1));
      final long newId = reopened.flush();
      assertThat(newId).as("new segment id must clear the orphan id 5").isGreaterThan(5L);
    }
  }

  private void writeCorruptSegmentFile(final String name) throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final SparseSegmentComponent c = newComponent(name);
    // A single zeroed page 0: the reader validates the segment magic at offset 0 and will reject it.
    final MutablePage page0 = new MutablePage(new PageId(db, c.getFileId(), 0), PAGE_SIZE);
    db.getPageManager().writePages(List.of(page0), false);
  }

  private static float weightFor(final int dim, final int i) {
    // Deterministic, always positive and finite; varied enough to catch a mis-decoded weight.
    return 0.01f + ((dim * 31 + i) % 97) * 0.01f;
  }

  private SparseSegmentComponent newComponent(final String name) {
    final DatabaseInternal db = (DatabaseInternal) database;
    final String filePath = db.getDatabasePath() + "/" + name;
    try {
      final SparseSegmentComponent c = new SparseSegmentComponent(db, name, filePath, ComponentFile.MODE.READ_WRITE, PAGE_SIZE);
      ((LocalSchema) db.getSchema().getEmbedded()).registerFile(c);
      return c;
    } catch (final IOException e) {
      throw new RuntimeException("failed to create sparse segment component '" + name + "'", e);
    }
  }
}
