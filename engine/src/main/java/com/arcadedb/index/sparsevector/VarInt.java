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

import java.nio.ByteBuffer;

/**
 * VarInt + ZigZag codec for the sparse-vector segment format.
 * <p>
 * VarInt is a byte-by-byte encoding and has no endianness of its own; the surrounding
 * {@link ByteBuffer} carries an endianness for fixed-width fields (the block payload context
 * uses {@code BIG_ENDIAN}), but the VarInt bytes themselves are written and read in stream
 * order regardless. The encoding matches Protocol Buffers / Snappy: 7 data bits per byte, top
 * bit set means "more bytes follow". ZigZag maps signed longs into unsigned space so that small
 * magnitudes (positive or negative) stay short. Used for RID-delta and (eventually) other
 * relative offsets in the on-disk format.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class VarInt {
  /** Maximum bytes needed to encode a 64-bit value. */
  public static final int MAX_VARLONG_BYTES = 10;

  public static int writeUnsignedVarLong(final ByteBuffer out, long value) {
    // No sign check here: this method also backs writeSignedVarLong via zigZagEncode, and
    // zigZag-encoding values like Long.MIN_VALUE + 1 produces longs whose Java sign bit is set
    // but represent legitimate unsigned 64-bit numbers. Callers that need to enforce a non-
    // negative invariant (e.g. RID-delta encoding) check at the call site - see
    // SparseSegmentBuilder.flushBlock.
    int bytes = 0;
    while ((value & ~0x7FL) != 0L) {
      out.put((byte) ((value & 0x7F) | 0x80));
      value >>>= 7;
      bytes++;
    }
    out.put((byte) value);
    return bytes + 1;
  }

  public static int writeSignedVarLong(final ByteBuffer out, final long value) {
    return writeUnsignedVarLong(out, zigZagEncode(value));
  }

  public static long readUnsignedVarLong(final ByteBuffer in) {
    long result = 0L;
    int shift = 0;
    while (true) {
      if (shift >= 64)
        throw new IllegalStateException("VarLong overflow at position " + in.position());
      final byte b = in.get();
      result |= ((long) (b & 0x7F)) << shift;
      if ((b & 0x80) == 0)
        return result;
      shift += 7;
    }
  }

  public static long readSignedVarLong(final ByteBuffer in) {
    return zigZagDecode(readUnsignedVarLong(in));
  }

  public static long zigZagEncode(final long v) {
    return (v << 1) ^ (v >> 63);
  }

  public static long zigZagDecode(final long v) {
    return (v >>> 1) ^ -(v & 1L);
  }

  /** Returns the number of bytes that would be used to encode {@code value} as an unsigned VarLong. */
  public static int unsignedVarLongSize(long value) {
    int bytes = 1;
    while ((value & ~0x7FL) != 0L) {
      value >>>= 7;
      bytes++;
    }
    return bytes;
  }

  public static int signedVarLongSize(final long value) {
    return unsignedVarLongSize(zigZagEncode(value));
  }

  private VarInt() {
    // utility class
  }
}
