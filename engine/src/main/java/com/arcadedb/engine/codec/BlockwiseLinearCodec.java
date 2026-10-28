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
package com.arcadedb.engine.codec;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

/**
 * Codec for arrays of signed longs that follow a piecewise line. The array is cut in blocks of {@link #BLOCK_SIZE}
 * values and each block is encoded on its own with {@link LinearCodec}, so a slowly drifting series pays for the
 * residuals of one block instead of the whole array, and {@code get(i)} stays O(1): one offset read, then
 * {@link LinearCodec#get}.
 * <p>
 * Layout, little-endian: id(1) reserved(3) count(4), then one int per block with the offset of its {@link LinearCodec}
 * encoding from the start of the array, zero-padded to a multiple of 8, then the blocks in order. Every block is a
 * multiple of 8 bytes, so the packed words of each block stay 8-aligned. {@code blocks = ceil(count / 512)}; a count
 * of 0 is the 8-byte header alone.
 * <p>
 * {@code count} validates the id, the count and that the offset table fits the array. {@code decode} also validates
 * the offset chain, which the layout fully determines (the first block starts at the end of the table, each next one
 * where the previous ends, the last one inside the array), and refuses malformed input with
 * {@link IllegalArgumentException} before allocating anything from the count. {@code get} relies on the JVM array
 * bounds checks instead of validating the payload, to keep the point read cheap.
 */
public final class BlockwiseLinearCodec {
  public static final byte CODEC_ID   = 3;
  public static final int  BLOCK_SIZE = 512;

  private static final int SHIFT       = 9;
  private static final int HEADER_SIZE = 8;

  private static final VarHandle INT_LE = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);

  private BlockwiseLinearCodec() {
  }

  public static byte[] encode(final long[] values, final int offset, final int length) {
    final int size = encodedSize(values, offset, length);
    final byte[] out = new byte[size];
    out[0] = CODEC_ID;
    INT_LE.set(out, 4, length);
    final int blocks = blocks(length);
    int pos = tableEnd(blocks);
    for (int b = 0; b < blocks; b++) {
      final int start = b << SHIFT;
      final int n = Math.min(BLOCK_SIZE, length - start);
      INT_LE.set(out, HEADER_SIZE + 4 * b, pos);
      LinearCodec.encodeInto(values, offset + start, n, out, pos);
      pos += LinearCodec.encodedLength(out, pos);
    }
    return out;
  }

  public static void decode(final byte[] encoded, final long[] out) {
    final int count = count(encoded);
    if (out.length < count)
      throw new IllegalArgumentException("Output array of " + out.length + " is smaller than the count " + count);
    final int blocks = blocks(count);
    long expected = tableEnd(blocks);
    for (int b = 0; b < blocks; b++) {
      final int start = b << SHIFT;
      final int base = (int) INT_LE.get(encoded, HEADER_SIZE + 4 * b);
      if (base != expected || base > encoded.length - LinearCodec.HEADER_BYTES)
        throw new IllegalArgumentException("Block " + b + " offset " + base + " does not follow the previous block");
      if (LinearCodec.count(encoded, base) != Math.min(BLOCK_SIZE, count - start))
        throw new IllegalArgumentException("Block " + b + " holds an unexpected number of values");
      expected = (long) base + LinearCodec.encodedLength(encoded, base);
      if (expected > encoded.length)
        throw new IllegalArgumentException("Block " + b + " ends outside the array");
      LinearCodec.decode(encoded, base, out, start);
    }
  }

  public static long get(final byte[] encoded, final int index) {
    final int base = (int) INT_LE.get(encoded, HEADER_SIZE + 4 * (index >>> SHIFT));
    return LinearCodec.get(encoded, base, index & (BLOCK_SIZE - 1));
  }

  public static int count(final byte[] encoded) {
    if (encoded.length < HEADER_SIZE)
      throw new IllegalArgumentException("Truncated blockwise header");
    if (encoded[0] != CODEC_ID)
      throw new IllegalArgumentException("Not a blockwise linear block, codec id " + encoded[0]);
    final int count = (int) INT_LE.get(encoded, 4);
    if (count < 0)
      throw new IllegalArgumentException("Negative count " + count);
    if (tableEnd(blocks(count)) > encoded.length)
      throw new IllegalArgumentException("Count " + count + " does not fit the offset table in " + encoded.length + " bytes");
    return count;
  }

  static int encodedSize(final long[] values, final int offset, final int length) {
    if (offset < 0 || length < 0 || (long) offset + length > values.length)
      throw new IndexOutOfBoundsException("Slice [" + offset + ", +" + length + ") outside array of " + values.length);
    final int blocks = blocks(length);
    long size = tableEnd(blocks);
    for (int b = 0; b < blocks; b++) {
      final int start = b << SHIFT;
      size += LinearCodec.encodedSize(values, offset + start, Math.min(BLOCK_SIZE, length - start));
    }
    if (size > Integer.MAX_VALUE)
      throw new IllegalArgumentException("Encoded size exceeds the array limit");
    return (int) size;
  }

  private static int blocks(final int count) {
    return (int) (((long) count + BLOCK_SIZE - 1) >>> SHIFT);
  }

  private static int tableEnd(final int blocks) {
    return (HEADER_SIZE + 4 * blocks + 7) & ~7;
  }
}
