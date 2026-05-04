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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 1 verification: {@link SegmentDimCursor#seekTo(RID)} lands at the correct posting under a
 * variety of skip-list configurations and target patterns.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SparseSegmentSeekTest {

  @Test
  void seekHitsExactRid(@TempDir final Path tmp) throws IOException {
    final Path file = tmp.resolve("seg.sparseseg");
    final List<RID> rids = ridSequence(0, 0, 1000);
    writeDim(file, 0, rids);

    try (final SparseSegmentReader r = new SparseSegmentReader(file);
         final SegmentDimCursor c = r.openCursor(0)) {
      assertThat(c.seekTo(rids.get(500))).isTrue();
      assertThat(c.currentRid()).isEqualTo(rids.get(500));
    }
  }

  @Test
  void seekToBetweenRidsLandsAtNext(@TempDir final Path tmp) throws IOException {
    final Path file = tmp.resolve("seg.sparseseg");
    final List<RID> rids = new ArrayList<>();
    for (int i = 0; i < 200; i++)
      rids.add(new RID(0, i * 10L));
    writeDim(file, 1, rids);

    try (final SparseSegmentReader r = new SparseSegmentReader(file);
         final SegmentDimCursor c = r.openCursor(1)) {
      // Seek between rids[10] (offset 100) and rids[11] (offset 110).
      assertThat(c.seekTo(new RID(0, 105))).isTrue();
      assertThat(c.currentRid()).isEqualTo(new RID(0, 110));
    }
  }

  @Test
  void seekPastLastRidExhausts(@TempDir final Path tmp) throws IOException {
    final Path file = tmp.resolve("seg.sparseseg");
    final List<RID> rids = ridSequence(0, 0, 100);
    writeDim(file, 7, rids);

    try (final SparseSegmentReader r = new SparseSegmentReader(file);
         final SegmentDimCursor c = r.openCursor(7)) {
      assertThat(c.seekTo(new RID(99, 0))).isFalse();
      assertThat(c.isExhausted()).isTrue();
    }
  }

  @Test
  void seekBeforeFirstRidLandsAtFirst(@TempDir final Path tmp) throws IOException {
    final Path file = tmp.resolve("seg.sparseseg");
    final List<RID> rids = new ArrayList<>();
    for (int i = 0; i < 50; i++)
      rids.add(new RID(0, 1000L + i));
    writeDim(file, 0, rids);

    try (final SparseSegmentReader r = new SparseSegmentReader(file);
         final SegmentDimCursor c = r.openCursor(0)) {
      assertThat(c.seekTo(new RID(0, 0))).isTrue();
      assertThat(c.currentRid()).isEqualTo(rids.get(0));
    }
  }

  @Test
  void seekIsForwardOnly(@TempDir final Path tmp) throws IOException {
    final Path file = tmp.resolve("seg.sparseseg");
    final List<RID> rids = ridSequence(0, 0, 100);
    writeDim(file, 0, rids);

    try (final SparseSegmentReader r = new SparseSegmentReader(file);
         final SegmentDimCursor c = r.openCursor(0)) {
      // Move the cursor forward to position 50.
      assertThat(c.seekTo(rids.get(50))).isTrue();
      // A backwards seek is a no-op (returns true since cursor is already past target).
      assertThat(c.seekTo(rids.get(10))).isTrue();
      // Cursor must still be at position 50, not 10.
      assertThat(c.currentRid()).isEqualTo(rids.get(50));
    }
  }

  @Test
  void seekAcrossMultipleBlocksWithSmallStride(@TempDir final Path tmp) throws IOException {
    final Path file = tmp.resolve("seg.sparseseg");
    final SegmentParameters params = SegmentParameters.builder().blockSize(16).skipStride(2).build();
    final List<RID> rids = ridSequence(0, 0, 500);

    try (final SparseSegmentWriter w = new SparseSegmentWriter(file, params)) {
      w.startDim(0);
      for (final RID rid : rids)
        w.appendPosting(rid, 0.5f);
      w.endDim();
      w.finish();
    }

    try (final SparseSegmentReader r = new SparseSegmentReader(file);
         final SegmentDimCursor c = r.openCursor(0)) {
      // Probe 30 random forward seeks; cursor should always land on a RID >= target.
      int idx = 0;
      for (int i = 0; i < 30; i++) {
        idx += 11;
        if (idx >= rids.size())
          break;
        assertThat(c.seekTo(rids.get(idx))).isTrue();
        assertThat(c.currentRid()).isEqualTo(rids.get(idx));
      }
    }
  }

  @Test
  void advanceAfterSeekContinuesInOrder(@TempDir final Path tmp) throws IOException {
    final Path file = tmp.resolve("seg.sparseseg");
    final List<RID> rids = ridSequence(0, 0, 200);
    writeDim(file, 0, rids);

    try (final SparseSegmentReader r = new SparseSegmentReader(file);
         final SegmentDimCursor c = r.openCursor(0)) {
      assertThat(c.seekTo(rids.get(100))).isTrue();
      assertThat(c.currentRid()).isEqualTo(rids.get(100));
      assertThat(c.advance()).isTrue();
      assertThat(c.currentRid()).isEqualTo(rids.get(101));
      assertThat(c.advance()).isTrue();
      assertThat(c.currentRid()).isEqualTo(rids.get(102));
    }
  }

  // ---------- helpers ----------

  private static void writeDim(final Path file, final int dimId, final List<RID> rids) throws IOException {
    try (final SparseSegmentWriter w = new SparseSegmentWriter(file, SegmentParameters.defaults())) {
      w.startDim(dimId);
      for (final RID rid : rids)
        w.appendPosting(rid, 0.5f);
      w.endDim();
      w.finish();
    }
  }

  private static List<RID> ridSequence(final int bucket, final long startOffset, final int n) {
    final List<RID> out = new ArrayList<>(n);
    long pos = startOffset;
    for (int i = 0; i < n; i++) {
      out.add(new RID(bucket, pos));
      pos += 1 + (i % 7);
    }
    return out;
  }
}
