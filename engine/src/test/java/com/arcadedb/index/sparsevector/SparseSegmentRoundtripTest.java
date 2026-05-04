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
import com.arcadedb.index.sparsevector.SegmentFormat.RidCompression;
import com.arcadedb.index.sparsevector.SegmentFormat.WeightQuantization;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 1 verification: a posting set written through {@link SparseSegmentWriter} reads back
 * identically through {@link SparseSegmentReader}, regardless of block_size, skip_stride, RID
 * compression or weight quantization.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SparseSegmentRoundtripTest {

  @Test
  void singleDimRoundtripWithDefaultParams(@TempDir final Path tmp) throws IOException {
    final Path file = tmp.resolve("seg.sparseseg");
    final List<RID>   rids = ridSequence(1, 0, 1024);
    final float[] weights = randomWeights(rids.size(), 0xCAFEL);

    try (final SparseSegmentWriter w = new SparseSegmentWriter(file, SegmentParameters.defaults())) {
      w.startDim(7);
      for (int i = 0; i < rids.size(); i++)
        w.appendPosting(rids.get(i), weights[i]);
      w.endDim();
      w.finish();
    }

    try (final SparseSegmentReader r = new SparseSegmentReader(file)) {
      assertThat(r.totalDims()).isEqualTo(1);
      assertThat(r.totalPostings()).isEqualTo(rids.size());
      assertThat(r.hasDim(7)).isTrue();
      assertThat(r.hasDim(8)).isFalse();

      try (final SegmentDimCursor c = r.openCursor(7)) {
        for (int i = 0; i < rids.size(); i++) {
          assertThat(c.advance()).isTrue();
          assertThat(c.currentRid()).isEqualTo(rids.get(i));
          assertThat(c.isTombstone()).isFalse();
          assertThat(c.currentWeight()).isCloseTo(weights[i],
              org.assertj.core.data.Offset.offset(WeightCodec.maxQuantizationError(0.0f, 1.0f)));
        }
        assertThat(c.advance()).isFalse();
        assertThat(c.isExhausted()).isTrue();
      }
    }
  }

  @Test
  void multipleDimsAcrossMultipleBucketsRoundtrip(@TempDir final Path tmp) throws IOException {
    final Path file = tmp.resolve("seg.sparseseg");
    final TreeMap<Integer, TreeMap<RID, Float>> truth = new TreeMap<>();
    final Random rnd = new Random(0xBEEF);
    for (int dim = 0; dim < 25; dim++) {
      final int bucketStart = dim % 4;     // postings span multiple buckets
      final int n = 50 + rnd.nextInt(200);
      final TreeMap<RID, Float> postings = new TreeMap<>();
      long position = rnd.nextInt(1024);
      int bucket = bucketStart;
      for (int i = 0; i < n; i++) {
        postings.put(new RID(bucket, position), rnd.nextFloat());
        position += 1 + rnd.nextInt(64);
        if (rnd.nextFloat() < 0.05f) {
          bucket++;
          position = rnd.nextInt(1024);
        }
      }
      truth.put(dim, postings);
    }

    try (final SparseSegmentWriter w = new SparseSegmentWriter(file, SegmentParameters.defaults())) {
      for (final Integer dim : truth.keySet()) {
        w.startDim(dim);
        for (final var e : truth.get(dim).entrySet())
          w.appendPosting(e.getKey(), e.getValue());
        w.endDim();
      }
      w.finish();
    }

    try (final SparseSegmentReader r = new SparseSegmentReader(file)) {
      assertThat(r.totalDims()).isEqualTo(truth.size());
      for (final Integer dim : truth.keySet()) {
        try (final SegmentDimCursor c = r.openCursor(dim)) {
          for (final var e : truth.get(dim).entrySet()) {
            assertThat(c.advance()).isTrue();
            assertThat(c.currentRid()).isEqualTo(e.getKey());
            assertThat(c.currentWeight()).isCloseTo(e.getValue(),
                org.assertj.core.data.Offset.offset(WeightCodec.maxQuantizationError(0.0f, 1.0f) * 1.5f));
          }
          assertThat(c.advance()).isFalse();
        }
      }
    }
  }

  @Test
  void roundtripWithFp32QuantizationIsExact(@TempDir final Path tmp) throws IOException {
    final Path file = tmp.resolve("seg.sparseseg");
    final SegmentParameters params = SegmentParameters.builder()
        .weightQuantization(WeightQuantization.FP32)
        .build();
    final List<RID> rids = ridSequence(2, 100, 256);
    final float[] weights = randomWeights(rids.size(), 0xDEAD);

    try (final SparseSegmentWriter w = new SparseSegmentWriter(file, params)) {
      w.startDim(0);
      for (int i = 0; i < rids.size(); i++)
        w.appendPosting(rids.get(i), weights[i]);
      w.endDim();
      w.finish();
    }

    try (final SparseSegmentReader r = new SparseSegmentReader(file);
         final SegmentDimCursor c = r.openCursor(0)) {
      for (int i = 0; i < rids.size(); i++) {
        assertThat(c.advance()).isTrue();
        assertThat(c.currentRid()).isEqualTo(rids.get(i));
        assertThat(c.currentWeight()).isEqualTo(weights[i]);
      }
    }
  }

  @Test
  void roundtripWithRawRidsRoundtrips(@TempDir final Path tmp) throws IOException {
    final Path file = tmp.resolve("seg.sparseseg");
    final SegmentParameters params = SegmentParameters.builder()
        .ridCompression(RidCompression.RAW)
        .build();
    final List<RID> rids = ridSequence(3, 1_000_000L, 64);
    final float[] weights = randomWeights(rids.size(), 0xBADL);

    try (final SparseSegmentWriter w = new SparseSegmentWriter(file, params)) {
      w.startDim(99);
      for (int i = 0; i < rids.size(); i++)
        w.appendPosting(rids.get(i), weights[i]);
      w.endDim();
      w.finish();
    }

    try (final SparseSegmentReader r = new SparseSegmentReader(file);
         final SegmentDimCursor c = r.openCursor(99)) {
      for (int i = 0; i < rids.size(); i++) {
        assertThat(c.advance()).isTrue();
        assertThat(c.currentRid()).isEqualTo(rids.get(i));
      }
    }
  }

  @Test
  void tombstonesRoundtripAndAreFlaggedOnRead(@TempDir final Path tmp) throws IOException {
    final Path file = tmp.resolve("seg.sparseseg");
    try (final SparseSegmentWriter w = new SparseSegmentWriter(file, SegmentParameters.defaults())) {
      w.startDim(5);
      w.appendPosting(new RID(0, 100), 0.5f);
      w.appendTombstone(new RID(0, 200));
      w.appendPosting(new RID(0, 300), 0.7f);
      w.endDim();
      w.finish();
    }
    try (final SparseSegmentReader r = new SparseSegmentReader(file);
         final SegmentDimCursor c = r.openCursor(5)) {
      assertThat(c.advance()).isTrue();
      assertThat(c.currentRid()).isEqualTo(new RID(0, 100));
      assertThat(c.isTombstone()).isFalse();

      assertThat(c.advance()).isTrue();
      assertThat(c.currentRid()).isEqualTo(new RID(0, 200));
      assertThat(c.isTombstone()).isTrue();
      assertThat(Float.isNaN(c.currentWeight())).isTrue();

      assertThat(c.advance()).isTrue();
      assertThat(c.currentRid()).isEqualTo(new RID(0, 300));
      assertThat(c.isTombstone()).isFalse();

      assertThat(c.advance()).isFalse();
    }
  }

  @Test
  void smallBlockSizeForcesManyBlocks(@TempDir final Path tmp) throws IOException {
    final Path file = tmp.resolve("seg.sparseseg");
    final SegmentParameters params = SegmentParameters.builder().blockSize(16).skipStride(4).build();
    final List<RID> rids = ridSequence(0, 0, 200);  // 13 blocks at blockSize=16
    final float[] weights = randomWeights(rids.size(), 1L);

    try (final SparseSegmentWriter w = new SparseSegmentWriter(file, params)) {
      w.startDim(1);
      for (int i = 0; i < rids.size(); i++)
        w.appendPosting(rids.get(i), weights[i]);
      w.endDim();
      w.finish();
    }

    try (final SparseSegmentReader r = new SparseSegmentReader(file)) {
      final DimMetadata md = r.dimMetadata(1);
      assertThat(md).isNotNull();
      assertThat(md.blockCount()).isEqualTo(13);  // 200/16 = 12.5 -> 13
      assertThat(md.skipList().length).isEqualTo(4);  // 13/4 = 3.25 -> 4
      try (final SegmentDimCursor c = r.openCursor(1)) {
        int seen = 0;
        while (c.advance())
          seen++;
        assertThat(seen).isEqualTo(rids.size());
      }
    }
  }

  @Test
  void writerRejectsNegativeWeight(@TempDir final Path tmp) throws IOException {
    final Path file = tmp.resolve("seg.sparseseg");
    try (final SparseSegmentWriter w = new SparseSegmentWriter(file, SegmentParameters.defaults())) {
      w.startDim(0);
      try {
        w.appendPosting(new RID(0, 1), -0.5f);
        org.assertj.core.api.Assertions.fail("expected IllegalArgumentException for negative weight");
      } catch (final IllegalArgumentException expected) {
        assertThat(expected.getMessage()).contains("non-negative");
      }
    }
  }

  @Test
  void writerRejectsOutOfOrderRids(@TempDir final Path tmp) throws IOException {
    final Path file = tmp.resolve("seg.sparseseg");
    try (final SparseSegmentWriter w = new SparseSegmentWriter(file, SegmentParameters.defaults())) {
      w.startDim(0);
      w.appendPosting(new RID(0, 100), 0.5f);
      try {
        w.appendPosting(new RID(0, 50), 0.5f);
        org.assertj.core.api.Assertions.fail("expected IllegalArgumentException for out-of-order RID");
      } catch (final IllegalArgumentException expected) {
        assertThat(expected.getMessage()).contains("ascending");
      }
    }
  }

  @Test
  void writerRejectsOutOfOrderDims(@TempDir final Path tmp) throws IOException {
    final Path file = tmp.resolve("seg.sparseseg");
    try (final SparseSegmentWriter w = new SparseSegmentWriter(file, SegmentParameters.defaults())) {
      w.startDim(5);
      w.appendPosting(new RID(0, 1), 0.5f);
      w.endDim();
      try {
        w.startDim(3);
        org.assertj.core.api.Assertions.fail("expected IllegalArgumentException for out-of-order dim");
      } catch (final IllegalArgumentException expected) {
        assertThat(expected.getMessage()).contains("ascending");
      }
    }
  }

  @Test
  void manifestFieldsRoundtrip(@TempDir final Path tmp) throws IOException {
    final Path file = tmp.resolve("seg.sparseseg");
    try (final SparseSegmentWriter w = new SparseSegmentWriter(file, SegmentParameters.defaults())) {
      w.setSegmentId(42L);
      w.setParentSegments(new long[] { 10L, 20L, 30L });
      w.setTombstoneFloorSegment(15L);
      w.startDim(0);
      w.appendPosting(new RID(0, 1), 0.5f);
      w.endDim();
      w.finish();
    }
    try (final SparseSegmentReader r = new SparseSegmentReader(file)) {
      assertThat(r.segmentId()).isEqualTo(42L);
      assertThat(r.parentSegments()).containsExactly(10L, 20L, 30L);
      assertThat(r.tombstoneFloorSegment()).isEqualTo(15L);
    }
  }

  // ---------- helpers ----------

  private static List<RID> ridSequence(final int bucket, final long startOffset, final int n) {
    final List<RID> out = new ArrayList<>(n);
    long pos = startOffset;
    for (int i = 0; i < n; i++) {
      out.add(new RID(bucket, pos));
      pos += 1 + (i % 7);
    }
    return out;
  }

  private static float[] randomWeights(final int n, final long seed) {
    final Random rnd = new Random(seed);
    final float[] out = new float[n];
    for (int i = 0; i < n; i++)
      out[i] = rnd.nextFloat();
    return out;
  }
}
