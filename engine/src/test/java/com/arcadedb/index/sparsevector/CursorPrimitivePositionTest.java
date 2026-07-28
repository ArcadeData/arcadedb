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
import java.util.Random;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Guards the invariant introduced in issue #5467: a cursor's primitive position accessors
 * ({@link SourceCursor#currentBucketId()} / {@link SourceCursor#currentPosition()}) must agree with
 * {@link SourceCursor#currentRid()} at every point of the iteration, on every cursor type.
 * <p>
 * The traversal now navigates and compares entirely on the primitives and materialises the
 * {@link RID} only for a document that reaches the result set, so a cursor state where the two
 * disagree would not be caught by a result-level assertion in the common case - it would silently
 * mis-order the merge or skip a posting. That makes the agreement worth asserting directly, in
 * particular across the two states where the position is set from something other than a decoded
 * posting: a cursor parked on a block boundary without having decoded its payload (the Block-Max
 * skip fast path), and an exhausted cursor, which must report {@code -1} rather than a stale
 * position.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CursorPrimitivePositionTest extends TestHelper {

  /** Wide enough to span many blocks at the default blockSize, so seeks cross block boundaries. */
  private static final int POSTINGS = 5_000;

  @Test
  void segmentCursorPrimitivesTrackTheRidThroughAdvanceAndSeek() throws Exception {
    for (final WeightQuantization q : WeightQuantization.values()) {
      final List<RID> rids = ridSequence(POSTINGS);
      inTx(() -> {
        final PaginatedSegmentReader reader = buildSegment("seg-5467-" + q, 1L, singleDim(rids), q);
        final PaginatedSegmentDimCursor c = reader.openCursor(0);

        c.start();
        int seen = 0;
        while (!c.isExhausted()) {
          assertAgrees(c.currentRid(), c.currentBucketId(), c.currentPosition());
          seen++;
          c.advance();
        }
        assertThat(seen).isEqualTo(POSTINGS);

        // Exhausted: no stale position may survive.
        assertThat(c.currentBucketId()).isEqualTo(-1);
        assertThat(c.currentPosition()).isEqualTo(-1L);
      });
    }
  }

  @Test
  void segmentCursorPrimitivesTrackTheRidWhenParkedOnABlockBoundary() throws Exception {
    final List<RID> rids = ridSequence(POSTINGS);
    inTx(() -> {
      final PaginatedSegmentReader reader = buildSegment("seg-5467-park", 1L, singleDim(rids),
          WeightQuantization.INT8);

      // Seek to each posting in turn with a fresh cursor, so every seek lands from a different
      // distance - including the block-boundary park, where the position is taken from an in-memory
      // block header and the payload is deliberately left undecoded.
      for (int i = 0; i < rids.size(); i += 37) {
        final PaginatedSegmentDimCursor c = reader.openCursor(0);
        c.start();
        final RID target = rids.get(i);
        assertThat(c.seekTo(target)).isTrue();
        assertAgrees(c.currentRid(), c.currentBucketId(), c.currentPosition());
        assertThat(c.currentRid()).isEqualTo(target);

        // Resolving the parked block must not move the position.
        final int bucketBefore = c.currentBucketId();
        final long positionBefore = c.currentPosition();
        c.currentWeight();
        assertThat(c.currentBucketId()).isEqualTo(bucketBefore);
        assertThat(c.currentPosition()).isEqualTo(positionBefore);
        assertAgrees(c.currentRid(), c.currentBucketId(), c.currentPosition());
      }

      // A seek past the end exhausts and clears. The postings span buckets 0..2, so the target has
      // to be past the last bucket, not merely past the last position of the first one.
      final PaginatedSegmentDimCursor c = reader.openCursor(0);
      c.start();
      assertThat(c.seekTo(new RID(9, Long.MAX_VALUE))).isFalse();
      assertThat(c.currentBucketId()).isEqualTo(-1);
      assertThat(c.currentPosition()).isEqualTo(-1L);
    });
  }

  @Test
  void mergedCursorPrimitivesTrackTheRidAcrossSources() throws Exception {
    // Two segments with interleaved RIDs plus an overlapping run, so the merge exercises the
    // newest-source-wins tie path where the position is picked from one source and the weight from
    // another.
    final List<RID> older = new ArrayList<>();
    final List<RID> newer = new ArrayList<>();
    for (int i = 0; i < 2_000; i++) {
      older.add(new RID(0, 1L + i * 2L));
      newer.add(new RID(0, 1L + i * 3L));
    }
    inTx(() -> {
      final PaginatedSegmentReader oldReader = buildSegment("seg-5467-merge-old", 1L, singleDim(older),
          WeightQuantization.FP32);
      final PaginatedSegmentReader newReader = buildSegment("seg-5467-merge-new", 2L, singleDim(newer),
          WeightQuantization.FP32);

      try (final DimCursor merged = new DimCursor(0, List.of(oldReader.openCursor(0), newReader.openCursor(0)))) {
        merged.start();
        RID previous = null;
        while (!merged.isExhausted()) {
          final RID rid = merged.currentRid();
          assertAgrees(rid, merged.currentBucketId(), merged.currentPosition());
          if (previous != null)
            assertThat(SparseSegmentBuilder.compareRid(previous, rid)).isNegative();
          previous = rid;
          merged.advance();
        }
        assertThat(merged.currentBucketId()).isEqualTo(-1);
        assertThat(merged.currentPosition()).isEqualTo(-1L);
      }

      // The primitive seek overload must land where the RID overload does.
      try (final DimCursor a = new DimCursor(0, List.of(oldReader.openCursor(0), newReader.openCursor(0)));
           final DimCursor b = new DimCursor(0, List.of(oldReader.openCursor(0), newReader.openCursor(0)))) {
        a.start();
        b.start();
        final RID target = new RID(0, 2_501L);
        a.seekTo(target);
        b.seekTo(target.getBucketId(), target.getPosition());
        assertThat(b.currentRid()).isEqualTo(a.currentRid());
        assertThat(b.currentBucketId()).isEqualTo(a.currentBucketId());
        assertThat(b.currentPosition()).isEqualTo(a.currentPosition());
      }
    });
  }

  // ---------- helpers ----------

  private static void assertAgrees(final RID rid, final int bucketId, final long position) {
    assertThat(rid).isNotNull();
    assertThat(bucketId).isEqualTo(rid.getBucketId());
    assertThat(position).isEqualTo(rid.getPosition());
  }

  private static List<RID> ridSequence(final int n) {
    final List<RID> out = new ArrayList<>(n);
    for (int i = 0; i < n; i++)
      out.add(new RID(i % 3, 1L + i));
    out.sort(SparseSegmentBuilder::compareRid);
    return out;
  }

  private static Map<RID, Float> singleDim(final List<RID> rids) {
    final Random rnd = new Random(5467L);
    final TreeMap<RID, Float> out = new TreeMap<>();
    for (final RID rid : rids)
      out.put(rid, 0.10f + rnd.nextFloat());
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

  private PaginatedSegmentReader buildSegment(final String name, final long segmentId, final Map<RID, Float> postings,
      final WeightQuantization quantization) throws IOException {
    final DatabaseInternal db = (DatabaseInternal) database;
    final SparseSegmentComponent c = new SparseSegmentComponent(db, name, db.getDatabasePath() + "/" + name,
        ComponentFile.MODE.READ_WRITE, SparseSegmentComponent.DEFAULT_PAGE_SIZE);
    ((LocalSchema) db.getSchema().getEmbedded()).registerFile(c);

    try (final SparseSegmentBuilder b = new SparseSegmentBuilder(c,
        SegmentParameters.builder().weightQuantization(quantization).build())) {
      b.setSegmentId(segmentId);
      b.startDim(0);
      for (final var e : postings.entrySet())
        b.appendPosting(e.getKey(), e.getValue());
      b.endDim();
      b.finish();
    }
    return new PaginatedSegmentReader(c);
  }
}
