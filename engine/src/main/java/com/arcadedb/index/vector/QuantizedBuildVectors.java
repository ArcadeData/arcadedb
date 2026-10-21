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
package com.arcadedb.index.vector;

import java.util.Arrays;

/**
 * The quantized rows of an INT8 or BINARY index, held on the heap for the duration of a graph build, by ordinal
 * (issue #10543).
 * <p>
 * A graph build asks for the operand of a distance evaluation thousands of times per inserted vector, so where those
 * operands live decides the build. The float cache holds them as {@code float[dimensions]} objects: 3 KB for a
 * 768-dimension vector that the index pages store in 768 bytes. When the corpus outgrows that cache the build re-reads
 * the missing vectors from the pages and dequantizes them into a fresh array each time, and each of those arrays is
 * published into the cache, evicting an older one. On a 13.8M-vector corpus in a 24 GB heap that is gigabytes a
 * second of long-lived objects replaced by new ones, which is the one allocation pattern a generational collector
 * cannot absorb: the heap sat between 17 and 24 GB and the build advanced one vector every five seconds.
 * <p>
 * This keeps the row exactly as the page stores it - the quantized bytes and the two floats that scale them - in
 * large primitive arrays that are written once and never replaced, so the collector has nothing to trace and nothing
 * to copy. The same heap budget therefore covers four times as many INT8 vectors (a hundred times as many BINARY ones),
 * and a vector is materialized only when it is used, into a scratch the reading thread owns
 * ({@link QuantizedBuildVectorValues}).
 * <p>
 * <b>Lifecycle.</b> Filled by one thread, in ordinal order, while the build validates the corpus; read-only
 * afterwards. Publication to the build threads is the executor submit that starts the insertion, so the fields are
 * plain: a volatile read per vector would sit on the hottest path of the build to guard a write that has finished
 * before any reader exists.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class QuantizedBuildVectors {
  /**
   * What one chunk aims to weigh. Large enough that a big corpus needs a few hundred arrays, small enough that no
   * single allocation asks the collector for more contiguous space than a handful of its regions.
   */
  private static final int TARGET_CHUNK_BYTES = 8 * 1024 * 1024;

  private final VectorQuantizationType type;
  private final int                    dimensions;
  private final int                    rowBytes;
  private final int                    capacity;
  private final int                    chunkShift;
  private final int                    chunkMask;
  private       byte[][]               chunks;
  // INT8: the minimum and the maximum the row was scaled by. BINARY: the median, and no second float.
  private       float[]                scaleA;
  private       float[]                scaleB;
  private       int                    size;

  private QuantizedBuildVectors(final VectorQuantizationType type, final int dimensions, final int capacity) {
    this.type = type;
    this.dimensions = dimensions;
    this.rowBytes = rowBytes(type, dimensions);
    this.capacity = capacity;

    // Rows per chunk is a power of two so locating a row is a shift and a mask
    final int rowsPerChunk = Math.max(1, Integer.highestOneBit(Math.max(1, TARGET_CHUNK_BYTES / rowBytes)));
    this.chunkShift = Integer.numberOfTrailingZeros(rowsPerChunk);
    this.chunkMask = rowsPerChunk - 1;
    this.chunks = new byte[(int) (((long) capacity + rowsPerChunk - 1) / rowsPerChunk)][];
    this.scaleA = new float[capacity];
    this.scaleB = type == VectorQuantizationType.INT8 ? new float[capacity] : null;
  }

  /**
   * @param type       INT8 or BINARY: the quantizations whose rows live in the index pages
   * @param dimensions the width of the vectors
   * @param capacity   the most rows this store will hold
   */
  static QuantizedBuildVectors create(final VectorQuantizationType type, final int dimensions, final int capacity) {
    if (capacity < 0)
      throw new IllegalArgumentException("Negative capacity: " + capacity);
    return new QuantizedBuildVectors(type, dimensions, capacity);
  }

  /** @return the bytes one row occupies in the page and in this store. */
  static int rowBytes(final VectorQuantizationType type, final int dimensions) {
    return switch (type) {
      case INT8 -> dimensions;
      case BINARY -> (dimensions + 7) / 8;
      default -> throw new IllegalArgumentException("Only INT8 and BINARY rows are kept quantized, not " + type);
    };
  }

  /** @return what one resident vector costs on the heap: its row and the floats that scale it. */
  static long bytesPerVector(final VectorQuantizationType type, final int dimensions) {
    return rowBytes(type, dimensions) + (type == VectorQuantizationType.INT8 ? 2L * Float.BYTES : Float.BYTES);
  }

  int capacity() {
    return capacity;
  }

  /** @return how many rows are resident: the ordinals {@code [0, size)}. */
  int size() {
    return size;
  }

  boolean hasRoom() {
    return size < capacity;
  }

  boolean isResident(final int ordinal) {
    return ordinal >= 0 && ordinal < size;
  }

  /** @return the heap this store holds right now, rows and scales. */
  long ramBytes() {
    long bytes = (long) scaleA.length * Float.BYTES + (scaleB != null ? (long) scaleB.length * Float.BYTES : 0L);
    for (final byte[] chunk : chunks)
      if (chunk != null)
        bytes += chunk.length;
    return bytes;
  }

  /**
   * The array the NEXT row goes into, allocated on first use. Together with {@link #writableOffset()} it lets a
   * reader copy a page's bytes straight into place; the row only becomes part of the store when {@link #commit} is
   * called, so a candidate that turns out to be unusable costs nothing but the next candidate's overwrite.
   */
  byte[] writableChunk() {
    final int index = size >>> chunkShift;
    byte[] chunk = chunks[index];
    if (chunk == null) {
      final long rowsInChunk = Math.min((long) chunkMask + 1, (long) capacity - ((long) index << chunkShift));
      chunk = new byte[(int) (rowsInChunk * rowBytes)];
      chunks[index] = chunk;
    }
    return chunk;
  }

  /** @return where, in {@link #writableChunk()}, the next row starts. */
  int writableOffset() {
    return (size & chunkMask) * rowBytes;
  }

  /**
   * Makes the row written at {@link #writableOffset()} the next ordinal.
   *
   * @param a INT8: the minimum the row was scaled by. BINARY: the median
   * @param b INT8: the maximum. BINARY: ignored
   */
  void commit(final float a, final float b) {
    if (size >= capacity)
      throw new IllegalStateException("The store is full: " + capacity + " rows");
    scaleA[size] = a;
    if (scaleB != null)
      scaleB[size] = b;
    ++size;
  }

  /**
   * Reconstructs a resident row into {@code destination}, the same floats the index pages would have produced for it.
   *
   * @param ordinal     a resident ordinal
   * @param destination at least {@code dimensions} floats
   */
  void dequantizeInto(final int ordinal, final float[] destination) {
    final byte[] chunk = chunks[ordinal >>> chunkShift];
    final int offset = (ordinal & chunkMask) * rowBytes;
    if (type == VectorQuantizationType.INT8)
      dequantizeInt8(chunk, offset, dimensions, scaleA[ordinal], scaleB[ordinal], destination);
    else
      dequantizeBinary(chunk, offset, dimensions, scaleA[ordinal], destination);
  }

  /**
   * Hands the rows back to the collector. For a build that finished normally; a cancelled one leaves them to be
   * collected with the store, since a straggler worker may still be reading.
   */
  void release() {
    chunks = new byte[0][];
    scaleA = new float[0];
    scaleB = scaleB != null ? new float[0] : null;
    size = 0;
  }

  /**
   * {@code (q + 128) * ((max - min) / 255) + min}, or {@code min} throughout when the vector was constant. The one
   * definition of INT8 dequantization: the index pages' reader goes through here as well, so the build and a search
   * can never disagree about what a stored row means.
   * <p>
   * <b>Why this shape.</b> The reconstruction used to divide every component by 255 ({@code ((q + 128) / 255) * range +
   * min}). A graph build dequantizes a row for every distance it evaluates, so this loop is the build's inner loop:
   * profiled on a 300K x 768 build it was 65% of all CPU, three times what the distance itself cost, because the
   * division keeps the JIT from turning the loop into SIMD. Folding the 255 into a step computed once lets the same
   * loop vectorize, and it is three times as fast (about 70 ns a 768-dimension row against 210 ns).
   * <p>
   * It differs from the divided form by at most about 3e-5 of the vector's range, which is under 1% of one
   * quantization step - half a step is the error INT8 stores anyway - and it keeps the endpoints: the lowest code is
   * {@code min} exactly. The faster {@code q * step + base} is NOT an option: it adds {@code min} to a term of the
   * size of the range and loses the range entirely when {@code |min|} dwarfs it (a vector of 1000.000 .. 1000.001
   * came back with an error of the whole range), where this form adds the two once, at the end.
   *
   * @param source       the array holding the quantized bytes
   * @param sourceOffset where the row starts in it
   * @param length       the number of components
   */
  static void dequantizeInt8(final byte[] source, final int sourceOffset, final int length, final float min, final float max,
      final float[] destination) {
    final float range = max - min;
    if (range == 0.0f) {
      // All values were the same
      Arrays.fill(destination, 0, length, min);
      return;
    }
    final float step = range / 255.0f;
    for (int i = 0; i < length; i++)
      // The signed byte [-128, 127] shifted to [0, 255] in float, which is exact, then onto the original range
      destination[i] = (source[sourceOffset + i] + 128.0f) * step + min;
  }

  /**
   * One bit per component says "at or above the vector's own median" or "below it": the sign of the component once the
   * vector is centred on its median, at unit norm (issue #8960). A row with every bit set is how a constant vector is
   * stored, and it keeps the sign of that constant, which also gives the all-zero vector back as the origin.
   *
   * @param source       the array holding the packed bits
   * @param sourceOffset where the row starts in it
   * @param length       the number of components (bits), not the number of bytes
   */
  static void dequantizeBinary(final byte[] source, final int sourceOffset, final int length, final float median,
      final float[] destination) {
    final float magnitude = (float) (1.0 / Math.sqrt(length));

    // Bits beyond `length` in the last byte are padding and never counted
    final int fullBytes = length >> 3;
    int ones = 0;
    for (int b = 0; b < fullBytes; b++)
      ones += Integer.bitCount(source[sourceOffset + b] & 0xFF);
    for (int i = fullBytes << 3; i < length; i++)
      if ((source[sourceOffset + (i >> 3)] & (1 << (i & 7))) != 0)
        ones++;

    if (ones == length) {
      Arrays.fill(destination, 0, length, Math.signum(median) * magnitude);
      return;
    }

    for (int b = 0; b < fullBytes; b++) {
      final int bits = source[sourceOffset + b];
      final int base = b << 3;
      for (int bit = 0; bit < 8; bit++)
        destination[base + bit] = (bits & (1 << bit)) != 0 ? magnitude : -magnitude;
    }
    for (int i = fullBytes << 3; i < length; i++)
      destination[i] = (source[sourceOffset + (i >> 3)] & (1 << (i & 7))) != 0 ? magnitude : -magnitude;
  }
}
