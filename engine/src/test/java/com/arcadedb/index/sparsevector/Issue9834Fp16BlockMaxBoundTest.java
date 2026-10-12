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
import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.index.sparsevector.SegmentFormat.WeightQuantization;
import com.arcadedb.schema.LocalSchema;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression for issue #9834: every bound a sparse segment stores for pruning must bound the value the reader will
 * actually DECODE from that segment. The builder took the block-max, the dim's global max and the skip list's
 * max-to-end from the raw float32 weight, while an FP16 segment reads back {@code fromFp16(toFp16(w))}, which is
 * round-to-nearest and so lands above the raw weight about half the time. A Block-Max WAND / MaxScore traversal whose
 * threshold sits on such an under-stated bound declares the term non-essential and never scores the document that
 * belongs in the top-K.
 * <p>
 * The tests pin the RULE (decoded weight never above any stored bound, for every quantization and segment shape), the
 * end-to-end symptom (the top-1 document is returned), and the read side for FP16 segments written before the fix,
 * which carry the raw bound on disk.
 */
class Issue9834Fp16BlockMaxBoundTest extends TestHelper {

  private static final int[]   BLOCK_SIZES  = { SegmentFormat.MIN_BLOCK_SIZE, SegmentFormat.MIN_BLOCK_SIZE + 1, 128 };
  private static final int[]   SKIP_STRIDES = { 1, 3, 8 };
  private static final float   TARGET_RAW   = 0.999f;
  private static final RID     TARGET_RID   = new RID(0, 500L);

  /** The extracted "what will the reader see" helper must agree with the reader's own decode, bit for bit. */
  @Test
  void decodedValueOfMatchesTheReaderDecode() {
    final Random rnd = new Random(9834L);
    for (int i = 0; i < 100_000; i++) {
      final float a = rnd.nextFloat() * 100f;
      final float b = rnd.nextFloat() * 100f;
      final float min = Math.min(a, b);
      final float max = Math.max(a, b);
      final float w = min + rnd.nextFloat() * (max - min);
      assertThat(WeightCodec.decodedValueOf(w, WeightQuantization.FP32, min, max)).isEqualTo(w);
      assertThat(WeightCodec.decodedValueOf(w, WeightQuantization.FP16, min, max)).isEqualTo(
          WeightCodec.fromFp16(WeightCodec.toFp16(w)));
      assertThat(WeightCodec.decodedValueOf(w, WeightQuantization.INT8, min, max)).isEqualTo(
          WeightCodec.dequantizeInt8(WeightCodec.quantizeInt8(w, min, max), min, max));
    }
    // The case quoted in the issue: raw 16.007818 decodes to 16.015625 in fp16.
    assertThat(WeightCodec.decodedValueOf(16.007818f, WeightQuantization.FP16, 0f, 16.007818f)).isEqualTo(16.015625f);
  }

  /**
   * The stored-bound invariant across every quantization and a spread of block sizes and skip strides, with
   * tombstones mixed in: for each live posting the reader returns, the decoded weight is at most the block max the
   * cursor reports for it, at most {@code upperBoundRemaining()} at that position, and at most the dim's global max.
   */
  @Test
  void everyStoredBoundCoversTheDecodedWeight() throws Exception {
    int segment = 0;
    for (final WeightQuantization q : WeightQuantization.values())
      for (final int blockSize : BLOCK_SIZES)
        for (final int skipStride : SKIP_STRIDES) {
          final SegmentParameters params = SegmentParameters.builder().weightQuantization(q).blockSize(blockSize)
              .skipStride(skipStride).build();
          final String name = "seg-9834-inv-" + segment++;
          final AtomicReference<SparseSegmentComponent> component = new AtomicReference<>();
          inTx(() -> component.set(buildRandomSegment(name, params, blockSize, skipStride)));
          inTx(() -> assertBoundsCoverDecodedWeights(new PaginatedSegmentReader(component.get()),
              q + "/blockSize=" + blockSize + "/skipStride=" + skipStride));
        }
  }

  /**
   * The end-to-end case from the issue: the 0.999f document's stored ceiling was 0.999 while it decodes to
   * 0.99902344, the threshold rises to exactly 0.999 on the first dim-0 document, dim 1 is proven non-essential on the
   * under-stated ceiling and the true top-1 is never scored.
   */
  @Test
  void fp16TopOneIsNotPrunedOnTheStoredBound() throws Exception {
    final AtomicReference<SparseSegmentComponent> component = new AtomicReference<>();
    inTx(() -> component.set(buildIssueSegment("seg-9834-fp16-e2e", WeightQuantization.FP16)));
    inTx(() -> assertTopOneIsTheTarget(new PaginatedSegmentReader(component.get()), true));
  }

  /**
   * The WRITE side on its own: the bytes the builder persists in the target's block header are already the decoded
   * ceiling, so a new segment does not depend on the read-side lift. The lift would hide a builder regression from
   * every reader-level assertion, which is why this one reads the page directly.
   */
  @Test
  void builderPersistsTheDecodedCeilingInTheBlockHeader() throws Exception {
    final AtomicReference<SparseSegmentComponent> component = new AtomicReference<>();
    inTx(() -> component.set(buildIssueSegment("seg-9834-fp16-bytes", WeightQuantization.FP16)));
    inTx(() -> {
      final PaginatedSegmentReader reader = new PaginatedSegmentReader(component.get());
      final PaginatedDimMetadata md = reader.dimMetadata(1);
      final int lastBlock = md.blockCount() - 1;
      // bmw_upper_bound sits after the block header's two RIDs and its 2-byte posting count.
      final float stored = component.get().readPage(md.blockPageNum(lastBlock))
          .readFloat(md.blockOffset(lastBlock) + 2 * SegmentFormat.RID_SIZE_BYTES + 2);
      assertThat(stored).isEqualTo(WeightCodec.fromFp16(WeightCodec.toFp16(TARGET_RAW)));
    });
  }

  /** Control: on FP32 the same data scores the target exactly at the threshold, so it correctly does not win. */
  @Test
  void fp32ControlKeepsTheTieBreakResult() throws Exception {
    final AtomicReference<SparseSegmentComponent> component = new AtomicReference<>();
    inTx(() -> component.set(buildIssueSegment("seg-9834-fp32-e2e", WeightQuantization.FP32)));
    inTx(() -> assertTopOneIsTheTarget(new PaginatedSegmentReader(component.get()), false));
  }

  /**
   * An FP16 segment written before the fix carries the RAW bound on disk. Simulate one by rewriting every stored
   * bound back to the raw 0.999f (block headers, the dim trailer's global max and the skip list's max-to-end all hold
   * the same float), then check the reader still hands the scorer a bound that covers the decoded weight, and the
   * top-1 still comes back, without a rebuild.
   */
  @Test
  void legacyFp16SegmentWithRawBoundsIsLiftedOnRead() throws Exception {
    final AtomicReference<SparseSegmentComponent> component = new AtomicReference<>();
    inTx(() -> component.set(buildIssueSegment("seg-9834-fp16-legacy", WeightQuantization.FP16)));

    final float decoded = WeightCodec.fromFp16(WeightCodec.toFp16(TARGET_RAW));
    assertThat(decoded).isGreaterThan(TARGET_RAW);

    // Exactly the three bound slots: the target's block header, the dim's global max and the dim's single skip entry
    // (4 blocks at the default stride of 8).
    rewriteBoundsToRaw(component.get(), decoded, TARGET_RAW, 3);

    inTx(() -> {
      final PaginatedSegmentReader reader = new PaginatedSegmentReader(component.get());
      assertBoundsCoverDecodedWeights(reader, "legacy FP16");
      assertTopOneIsTheTarget(reader, true);
    });
  }

  /**
   * INT8 is affected too, which the issue ruled out: its top level dequantizes to {@code min + 1.0f * (max - min)},
   * and float rounding puts that one ulp above {@code max} for bands like this one. A fixed builder stores the decoded
   * ceiling; a segment written before the fix stored the raw maximum and is lifted on read from the block's own band.
   */
  @Test
  void int8TopLevelDecodingAboveTheBandMaxIsCovered() throws Exception {
    final float min = 18.354017f;
    final float max = 50.413555f;
    final float decoded = WeightCodec.decodedValueOf(max, WeightQuantization.INT8, min, max);
    assertThat(decoded).as("this band's top INT8 level decodes above the raw maximum").isGreaterThan(max);

    final AtomicReference<SparseSegmentComponent> component = new AtomicReference<>();
    inTx(() -> component.set(buildTwoPostingSegment("seg-9834-int8", WeightQuantization.INT8, min, max)));
    inTx(() -> assertBoundsCoverDecodedWeights(new PaginatedSegmentReader(component.get()), "INT8"));

    // The same segment as a pre-fix builder would have written it: block header, global max and skip entry raw.
    rewriteBoundsToRaw(component.get(), decoded, max, 3);
    inTx(() -> assertBoundsCoverDecodedWeights(new PaginatedSegmentReader(component.get()), "legacy INT8"));
  }

  // ---------- helpers ----------

  private void assertTopOneIsTheTarget(final PaginatedSegmentReader reader, final boolean expected) throws IOException {
    final int[] queryDims = { 0, 1 };
    final float[] queryWeights = { 1.998f, 1.0f };
    final DimCursor[] cursors = new DimCursor[queryDims.length];
    for (int i = 0; i < queryDims.length; i++)
      cursors[i] = new DimCursor(queryDims[i], List.of(reader.openCursor(queryDims[i])));
    try {
      final List<RidScore> top = BmwScorer.topK(queryDims, queryWeights, cursors, 1);
      assertThat(top).hasSize(1);
      if (expected)
        assertThat(top.getFirst().rid()).as("the document decoding above the threshold must be top-1").isEqualTo(TARGET_RID);
      else
        assertThat(top.getFirst().rid()).isNotEqualTo(TARGET_RID);
    } finally {
      for (final DimCursor c : cursors)
        c.close();
    }
  }

  private static void assertBoundsCoverDecodedWeights(final PaginatedSegmentReader reader, final String label)
      throws IOException {
    for (final int dim : reader.dims()) {
      final float globalMax = reader.dimMetadata(dim).globalMaxWeight();
      try (final PaginatedSegmentDimCursor c = reader.openCursor(dim)) {
        final float beforeStart = c.upperBoundRemaining();
        c.start();
        while (!c.isExhausted()) {
          if (!c.isTombstone()) {
            final float w = c.currentWeight();
            final RID rid = c.currentRid();
            assertThat(c.blockMaxAt(rid)).as("%s dim %d %s: blockMaxAt vs decoded weight", label, dim, rid)
                .isGreaterThanOrEqualTo(w);
            assertThat(c.upperBoundRemaining()).as("%s dim %d %s: upperBoundRemaining vs decoded weight", label, dim, rid)
                .isGreaterThanOrEqualTo(w);
            assertThat(beforeStart).as("%s dim %d %s: bound before start vs decoded weight", label, dim, rid)
                .isGreaterThanOrEqualTo(w);
            assertThat(globalMax).as("%s dim %d %s: global max vs decoded weight", label, dim, rid)
                .isGreaterThanOrEqualTo(w);
          }
          if (!c.advance())
            break;
        }
      }
    }
  }

  /** Dims with posting counts at the block / stride edges, random weights spread over several fp16 binades. */
  private SparseSegmentComponent buildRandomSegment(final String name, final SegmentParameters params, final int blockSize,
      final int skipStride) throws IOException {
    final Random rnd = new Random(name.hashCode());
    final int[] counts = { 1, blockSize, blockSize + 1, blockSize * skipStride, blockSize * skipStride + 7 };
    final SparseSegmentComponent c = newComponent(name);
    try (final SparseSegmentBuilder b = new SparseSegmentBuilder(c, params)) {
      b.setSegmentId(9834L);
      for (int dim = 0; dim < counts.length; dim++) {
        b.startDim(dim);
        long position = 1L;
        int bucket = 0;
        for (int i = 0; i < counts[dim]; i++) {
          if (rnd.nextInt(50) == 0) {
            bucket++;
            position = 1L;
          }
          final RID rid = new RID(bucket, position);
          position += 1 + rnd.nextInt(3);
          if (rnd.nextInt(10) == 0)
            b.appendTombstone(rid);
          else
            b.appendPosting(rid, rnd.nextFloat() * 100f);
        }
        b.endDim();
      }
      b.finish();
    }
    return c;
  }

  /**
   * dim 0: 40 documents at 0.5 (exact in fp16), so with query weight 1.998 each scores 0.999. dim 1: 399 filler
   * postings at 0.001 (so the term sorts non-essential first on the tie) plus the target at 0.999f.
   */
  private SparseSegmentComponent buildIssueSegment(final String name, final WeightQuantization q) throws IOException {
    final Map<Integer, TreeMap<RID, Float>> byDim = new TreeMap<>();
    final TreeMap<RID, Float> dim0 = new TreeMap<>();
    for (int i = 0; i < 40; i++)
      dim0.put(new RID(0, 1L + i), 0.5f);
    final TreeMap<RID, Float> dim1 = new TreeMap<>();
    for (int i = 0; i < 399; i++)
      dim1.put(new RID(0, 100L + i), 0.001f);
    dim1.put(TARGET_RID, TARGET_RAW);
    byDim.put(0, dim0);
    byDim.put(1, dim1);

    final SparseSegmentComponent c = newComponent(name);
    try (final SparseSegmentBuilder b = new SparseSegmentBuilder(c, SegmentParameters.builder().weightQuantization(q).build())) {
      b.setSegmentId(9834L);
      for (final var dim : byDim.entrySet()) {
        b.startDim(dim.getKey());
        for (final var p : dim.getValue().entrySet())
          b.appendPosting(p.getKey(), p.getValue());
        b.endDim();
      }
      b.finish();
    }
    return c;
  }

  /**
   * Simulates a segment written before the fix by rewriting every 4-byte slot holding the decoded ceiling back to the
   * raw maximum. The count is exact so an accidental match inside a posting payload, which would corrupt the segment
   * instead of simulating an old one, fails here rather than somewhere less obvious.
   */
  private void rewriteBoundsToRaw(final SparseSegmentComponent c, final float decoded, final float raw, final int expectedSlots) {
    final int decodedBits = Float.floatToRawIntBits(decoded);
    final int rawBits = Float.floatToRawIntBits(raw);
    final int[] patched = { 0 };
    inTx(() -> {
      final int contentSize = c.pageContentSize();
      for (int pageNum = 0; pageNum < c.getTotalPages(); pageNum++) {
        final BasePage page = c.readPage(pageNum);
        for (int off = 0; off + 4 <= contentSize; off++) {
          if (page.readInt(off) == decodedBits) {
            final MutablePage mp = c.modifyPage(pageNum);
            mp.writeInt(off, rawBits);
            patched[0]++;
          }
        }
      }
    });
    assertThat(patched[0]).as("bound slots holding %s rewritten to %s; a different count means the segment layout changed"
        + " (block count, skip stride) or a posting payload happens to contain the same bytes", decoded, raw)
        .isEqualTo(expectedSlots);
  }

  private SparseSegmentComponent buildTwoPostingSegment(final String name, final WeightQuantization q, final float w1,
      final float w2) throws IOException {
    final SparseSegmentComponent c = newComponent(name);
    try (final SparseSegmentBuilder b = new SparseSegmentBuilder(c, SegmentParameters.builder().weightQuantization(q).build())) {
      b.setSegmentId(9834L);
      b.startDim(0);
      b.appendPosting(new RID(0, 1L), w1);
      b.appendPosting(new RID(0, 2L), w2);
      b.endDim();
      b.finish();
    }
    return c;
  }

  private SparseSegmentComponent newComponent(final String name) throws IOException {
    final DatabaseInternal db = (DatabaseInternal) database;
    final SparseSegmentComponent c = new SparseSegmentComponent(db, name, db.getDatabasePath() + "/" + name,
        ComponentFile.MODE.READ_WRITE, SparseSegmentComponent.DEFAULT_PAGE_SIZE);
    ((LocalSchema) db.getSchema().getEmbedded()).registerFile(c);
    return c;
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
}
