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
 * Phase 3 verification: {@link CompactionWorker} merges multiple sealed segments into one with
 * correct precedence rules (newest source wins, tombstones shadow live, dropAllTombstones).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CompactionWorkerTest {

  @Test
  void compactNewerSegmentOverridesOlderWeight(@TempDir final Path tmp) throws IOException {
    final Path s0 = writeSegment(tmp, "0", 1L, dim -> {
      dim.startDim(0);
      dim.appendPosting(new RID(0, 1), 0.1f);
      dim.appendPosting(new RID(0, 2), 0.2f);
      dim.endDim();
    });
    final Path s1 = writeSegment(tmp, "1", 2L, dim -> {
      dim.startDim(0);
      dim.appendPosting(new RID(0, 1), 0.99f);  // override
      dim.appendPosting(new RID(0, 3), 0.3f);
      dim.endDim();
    });
    final Path out = tmp.resolve("merged.sparseseg");

    try (final SparseSegmentReader r0 = new SparseSegmentReader(s0);
         final SparseSegmentReader r1 = new SparseSegmentReader(s1)) {
      final SparseSegmentReader merged = CompactionWorker.compact(new SparseSegmentReader[] { r0, r1 }, out, 99L,
          TEST_PARAMS);
      try (merged) {
        assertThat(merged.segmentId()).isEqualTo(99L);
        assertThat(merged.parentSegments()).containsExactly(1L, 2L);
        try (final SegmentDimCursor c = merged.openCursor(0)) {
          final List<float[]> got = drainPostings(c);
          assertThat(got).hasSize(3);
          // RIDs 1, 2, 3; weight at 1 must be 0.99 (newer wins).
          assertThat(got.get(0)).containsExactly(1L, 0.99f);
          assertThat(got.get(1)).containsExactly(2L, 0.2f);
          assertThat(got.get(2)).containsExactly(3L, 0.3f);
        }
      }
    }
  }

  @Test
  void tombstoneInNewerSegmentShadowsLiveInOlder(@TempDir final Path tmp) throws IOException {
    final Path s0 = writeSegment(tmp, "0", 1L, dim -> {
      dim.startDim(0);
      dim.appendPosting(new RID(0, 1), 0.5f);
      dim.appendPosting(new RID(0, 2), 0.7f);
      dim.endDim();
    });
    final Path s1 = writeSegment(tmp, "1", 2L, dim -> {
      dim.startDim(0);
      dim.appendTombstone(new RID(0, 1));
      dim.endDim();
    });
    final Path out = tmp.resolve("merged.sparseseg");

    try (final SparseSegmentReader r0 = new SparseSegmentReader(s0);
         final SparseSegmentReader r1 = new SparseSegmentReader(s1)) {
      final SparseSegmentReader merged = CompactionWorker.compact(new SparseSegmentReader[] { r0, r1 }, out, 99L,
          TEST_PARAMS, false);
      try (merged) {
        try (final SegmentDimCursor c = merged.openCursor(0)) {
          assertThat(c.advance()).isTrue();
          assertThat(c.currentRid()).isEqualTo(new RID(0, 1));
          assertThat(c.isTombstone()).isTrue();
          assertThat(c.advance()).isTrue();
          assertThat(c.currentRid()).isEqualTo(new RID(0, 2));
          assertThat(c.isTombstone()).isFalse();
          assertThat(c.advance()).isFalse();
        }
      }
    }
  }

  @Test
  void dropAllTombstonesProducesLiveOnlyOutput(@TempDir final Path tmp) throws IOException {
    final Path s0 = writeSegment(tmp, "0", 1L, dim -> {
      dim.startDim(0);
      dim.appendPosting(new RID(0, 1), 0.5f);
      dim.appendPosting(new RID(0, 2), 0.7f);
      dim.endDim();
    });
    final Path s1 = writeSegment(tmp, "1", 2L, dim -> {
      dim.startDim(0);
      dim.appendTombstone(new RID(0, 1));
      dim.appendPosting(new RID(0, 3), 0.3f);
      dim.endDim();
    });
    final Path out = tmp.resolve("merged.sparseseg");

    try (final SparseSegmentReader r0 = new SparseSegmentReader(s0);
         final SparseSegmentReader r1 = new SparseSegmentReader(s1)) {
      final SparseSegmentReader merged = CompactionWorker.compact(new SparseSegmentReader[] { r0, r1 }, out, 99L,
          TEST_PARAMS, true);
      try (merged) {
        try (final SegmentDimCursor c = merged.openCursor(0)) {
          final List<float[]> got = drainPostings(c);
          assertThat(got).hasSize(2);
          assertThat(got.get(0)).containsExactly(2L, 0.7f);
          assertThat(got.get(1)).containsExactly(3L, 0.3f);
        }
      }
    }
  }

  @Test
  void compactingThreeSegmentsAcrossDimsMerges(@TempDir final Path tmp) throws IOException {
    final Path s0 = writeSegment(tmp, "0", 1L, dim -> {
      dim.startDim(0);
      dim.appendPosting(new RID(0, 1), 0.1f);
      dim.endDim();
      dim.startDim(2);
      dim.appendPosting(new RID(0, 5), 0.5f);
      dim.endDim();
    });
    final Path s1 = writeSegment(tmp, "1", 2L, dim -> {
      dim.startDim(1);
      dim.appendPosting(new RID(0, 2), 0.2f);
      dim.endDim();
      dim.startDim(2);
      dim.appendPosting(new RID(0, 5), 0.99f);  // override
      dim.endDim();
    });
    final Path s2 = writeSegment(tmp, "2", 3L, dim -> {
      dim.startDim(0);
      dim.appendPosting(new RID(0, 1), 0.88f);  // override across two-step lineage
      dim.endDim();
      dim.startDim(3);
      dim.appendPosting(new RID(0, 9), 0.9f);
      dim.endDim();
    });
    final Path out = tmp.resolve("merged.sparseseg");

    try (final SparseSegmentReader r0 = new SparseSegmentReader(s0);
         final SparseSegmentReader r1 = new SparseSegmentReader(s1);
         final SparseSegmentReader r2 = new SparseSegmentReader(s2)) {
      final SparseSegmentReader merged = CompactionWorker.compact(new SparseSegmentReader[] { r0, r1, r2 }, out, 100L,
          TEST_PARAMS);
      try (merged) {
        assertThat(merged.parentSegments()).containsExactly(1L, 2L, 3L);
        assertThat(merged.totalDims()).isEqualTo(4);

        // dim 0: (RID 1, 0.88)
        try (final SegmentDimCursor c = merged.openCursor(0)) {
          final List<float[]> got = drainPostings(c);
          assertThat(got).hasSize(1);
          assertThat(got.getFirst()).containsExactly(1L, 0.88f);
        }
        // dim 1: (RID 2, 0.2)
        try (final SegmentDimCursor c = merged.openCursor(1)) {
          final List<float[]> got = drainPostings(c);
          assertThat(got).hasSize(1);
          assertThat(got.getFirst()).containsExactly(2L, 0.2f);
        }
        // dim 2: (RID 5, 0.99) - override
        try (final SegmentDimCursor c = merged.openCursor(2)) {
          final List<float[]> got = drainPostings(c);
          assertThat(got).hasSize(1);
          assertThat(got.getFirst()).containsExactly(5L, 0.99f);
        }
        // dim 3: (RID 9, 0.9)
        try (final SegmentDimCursor c = merged.openCursor(3)) {
          final List<float[]> got = drainPostings(c);
          assertThat(got).hasSize(1);
          assertThat(got.getFirst()).containsExactly(9L, 0.9f);
        }
      }
    }
  }

  @Test
  void compactingAllTombstonesWithDropReturnsNull(@TempDir final Path tmp) throws IOException {
    final Path s0 = writeSegment(tmp, "0", 1L, dim -> {
      dim.startDim(0);
      dim.appendTombstone(new RID(0, 1));
      dim.endDim();
    });
    final Path out = tmp.resolve("merged.sparseseg");

    try (final SparseSegmentReader r0 = new SparseSegmentReader(s0)) {
      final SparseSegmentReader merged = CompactionWorker.compact(new SparseSegmentReader[] { r0 }, out, 100L,
          TEST_PARAMS, true);
      assertThat(merged).isNull();
      assertThat(java.nio.file.Files.exists(out)).isFalse();
    }
  }

  // ---------- helpers ----------

  @FunctionalInterface
  private interface SegmentBuilder {
    void build(SparseSegmentWriter w) throws IOException;
  }

  /** Use FP32 quantization for compaction tests so we can assert exact weight equality. */
  private static final SegmentParameters TEST_PARAMS = SegmentParameters.builder()
      .weightQuantization(SegmentFormat.WeightQuantization.FP32)
      .build();

  private static Path writeSegment(final Path tmpDir, final String name, final long segmentId, final SegmentBuilder builder)
      throws IOException {
    final Path file = tmpDir.resolve(name + ".sparseseg");
    try (final SparseSegmentWriter w = new SparseSegmentWriter(file, TEST_PARAMS)) {
      w.setSegmentId(segmentId);
      builder.build(w);
      w.finish();
    }
    return file;
  }

  private static List<float[]> drainPostings(final SegmentDimCursor c) throws IOException {
    final List<float[]> out = new ArrayList<>();
    while (c.advance()) {
      if (c.isTombstone())
        continue;
      out.add(new float[] { (float) c.currentRid().getPosition(), c.currentWeight() });
    }
    return out;
  }
}
