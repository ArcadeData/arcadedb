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
package com.arcadedb.index.columnar.spike;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * Spike-only approximation of the real off-heap block layout: the stored values of a {@link SpikeSegment} split in blocks of
 * {@link #BLOCK_ROWS} rows, each frame-of-reference encoded (min + bit width of max - min, bit-packed) into one direct
 * little-endian buffer. Presence and rank stay on the heap segment, so the same match bitmaps apply.
 * <p>
 * Block layout at {@code blockOffsets[b]}: int min, int width, then ceil(rows * width / 64) longs, plus one padding long so a
 * value straddling two words can always read the next word.
 */
final class SpikeBlockSegment {
  static final int BLOCK_SHIFT = 14;
  static final int BLOCK_ROWS  = 1 << BLOCK_SHIFT;

  private final SpikeSegment segment;
  private final ByteBuffer   buffer;
  private final long[]       blockOffsets;
  private final int          blockCount;

  private SpikeBlockSegment(final SpikeSegment segment, final ByteBuffer buffer, final long[] blockOffsets, final int blockCount) {
    this.segment = segment;
    this.buffer = buffer;
    this.blockOffsets = blockOffsets;
    this.blockCount = blockCount;
  }

  static SpikeBlockSegment build(final SpikeSegment segment) {
    final long rows = segment.rows();
    final int blockCount = (int) ((rows + BLOCK_ROWS - 1) / BLOCK_ROWS);
    final long[] blockOffsets = new long[blockCount + 1];
    final int[] mins = new int[blockCount];
    final int[] widths = new int[blockCount];
    long size = 0;
    for (int b = 0; b < blockCount; b++) {
      final int from = b * BLOCK_ROWS;
      final int count = (int) Math.min(BLOCK_ROWS, rows - from);
      int min = Integer.MAX_VALUE;
      int max = Integer.MIN_VALUE;
      for (int i = 0; i < count; i++) {
        final int v = segment.storedValueAtRow(from + i);
        if (v < min)
          min = v;
        if (v > max)
          max = v;
      }
      mins[b] = min;
      widths[b] = 32 - Integer.numberOfLeadingZeros(max - min);
      blockOffsets[b] = size;
      size += 8 + ((((long) count * widths[b] + 63) / 64) + 1) * 8;
    }
    blockOffsets[blockCount] = size;

    final ByteBuffer buffer = ByteBuffer.allocateDirect((int) size).order(ByteOrder.LITTLE_ENDIAN);
    for (int b = 0; b < blockCount; b++) {
      final int from = b * BLOCK_ROWS;
      final int count = (int) Math.min(BLOCK_ROWS, rows - from);
      final int width = widths[b];
      final int words = (int) (((long) count * width + 63) / 64) + 1;
      final long[] packed = new long[words];
      for (int i = 0; i < count; i++) {
        final long v = segment.storedValueAtRow(from + i) - mins[b];
        final long bit = (long) i * width;
        final int word = (int) (bit >>> 6);
        final int shift = (int) (bit & 63);
        packed[word] |= v << shift;
        if (shift + width > 64)
          packed[word + 1] |= v >>> (64 - shift);
      }
      final int base = (int) blockOffsets[b];
      buffer.putInt(base, mins[b]);
      buffer.putInt(base + 4, width);
      for (int w = 0; w < words; w++)
        buffer.putLong(base + 8 + w * 8, packed[w]);
    }
    return new SpikeBlockSegment(segment, buffer, blockOffsets, blockCount);
  }

  SpikeSegment segment() {
    return segment;
  }

  int blockCount() {
    return blockCount;
  }

  /** Decodes the stored values of the block into the caller-owned scratch and returns the number of rows decoded. */
  int decodeBlock(final int block, final int[] scratch) {
    final int count = (int) Math.min(BLOCK_ROWS, segment.rows() - (long) block * BLOCK_ROWS);
    final int base = (int) blockOffsets[block];
    final int min = buffer.getInt(base);
    final int width = buffer.getInt(base + 4);
    if (width == 0) {
      Arrays.fill(scratch, 0, count, min);
      return count;
    }
    final int data = base + 8;
    final long mask = (1L << width) - 1;
    long bit = 0;
    for (int i = 0; i < count; i++, bit += width) {
      final int word = (int) (bit >>> 6);
      final int shift = (int) (bit & 63);
      long v = buffer.getLong(data + (word << 3)) >>> shift;
      if (shift + width > 64)
        v |= buffer.getLong(data + (word << 3) + 8) << (64 - shift);
      scratch[i] = (int) (v & mask) + min;
    }
    return count;
  }

  long offHeapBytes() {
    return buffer.capacity();
  }

  /** The presence, rank and index structures that stay on the heap. */
  long heapBytes() {
    return segment.presence().length * 8L + segment.pageRowStart().length * 4L + blockOffsets.length * 8L;
  }
}
