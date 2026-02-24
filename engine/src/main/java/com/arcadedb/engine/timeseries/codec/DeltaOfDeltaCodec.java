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
package com.arcadedb.engine.timeseries.codec;

import java.nio.ByteBuffer;

/**
 * Delta-of-delta encoding for monotonically increasing timestamps.
 * Based on the Facebook Gorilla paper: stores first value raw, then deltas,
 * then delta-of-deltas using variable-bit encoding.
 * <p>
 * Encoding scheme for delta-of-deltas (dod):
 * - dod == 0: store '0' (1 bit)
 * - dod in [-64, 63]: store '10' + 7-bit ZigZag value (9 bits)
 * - dod in [-256, 255]: store '110' + 9-bit ZigZag value (12 bits)
 * - dod in [-2047, 2047]: store '1110' + 12-bit ZigZag value (16 bits)
 * - otherwise: store '1111' + 64-bit raw value (68 bits)
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class DeltaOfDeltaCodec {

  /** Maximum number of values per encoded block — mirrors TimeSeriesSealedStore.MAX_BLOCK_SIZE. */
  public static final int MAX_BLOCK_SIZE = 65536;

  private DeltaOfDeltaCodec() {
  }

  public static byte[] encode(final long[] timestamps) {
    if (timestamps == null || timestamps.length == 0)
      return new byte[0];

    final BitWriter writer = new BitWriter(timestamps.length * 2 + 16);

    // Write count
    writer.writeBits(timestamps.length, 32);
    // Write first value raw (64 bits)
    writer.writeBits(timestamps[0], 64);

    if (timestamps.length == 1)
      return writer.toByteArray();

    // Write first delta raw (64 bits)
    long prevDelta = timestamps[1] - timestamps[0];
    writer.writeBits(prevDelta, 64);

    for (int i = 2; i < timestamps.length; i++) {
      final long delta = timestamps[i] - timestamps[i - 1];
      final long dod = delta - prevDelta;
      prevDelta = delta;

      if (dod == 0) {
        writer.writeBit(0);
      } else if (dod >= -64 && dod <= 63) {
        writer.writeBits(0b10, 2);
        writer.writeBits(zigZagEncode(dod), 7);
      } else if (dod >= -255 && dod <= 255) {
        writer.writeBits(0b110, 3);
        writer.writeBits(zigZagEncode(dod), 9);
      } else if (dod >= -2047 && dod <= 2047) {
        writer.writeBits(0b1110, 4);
        writer.writeBits(zigZagEncode(dod), 12);
      } else {
        writer.writeBits(0b1111, 4);
        writer.writeBits(dod, 64);
      }
    }
    return writer.toByteArray();
  }

  public static long[] decode(final byte[] data) {
    if (data == null || data.length == 0)
      return new long[0];

    final BitReader reader = new BitReader(data);

    final int count = (int) reader.readBits(32);
    if (count <= 0 || count > MAX_BLOCK_SIZE)
      throw new IllegalArgumentException("DeltaOfDelta decode: invalid count " + count + " (expected 1.." + MAX_BLOCK_SIZE + ")");
    final long[] result = new long[count];
    result[0] = reader.readBits(64);

    if (count == 1)
      return result;

    long prevDelta = reader.readBits(64);
    result[1] = result[0] + prevDelta;

    for (int i = 2; i < count; i++) {
      long dod;
      if (reader.readBit() == 0) {
        dod = 0;
      } else if (reader.readBit() == 0) {
        // prefix '10'
        dod = zigZagDecode(reader.readBits(7));
      } else if (reader.readBit() == 0) {
        // prefix '110'
        dod = zigZagDecode(reader.readBits(9));
      } else if (reader.readBit() == 0) {
        // prefix '1110'
        dod = zigZagDecode(reader.readBits(12));
      } else {
        // prefix '1111'
        dod = reader.readBits(64);
      }
      prevDelta = prevDelta + dod;
      result[i] = result[i - 1] + prevDelta;
    }
    return result;
  }

  /**
   * Decodes into a pre-allocated output buffer, returning the number of decoded values.
   * The output array must be at least as large as the encoded count.
   */
  public static int decode(final byte[] data, final long[] output) {
    if (data == null || data.length == 0)
      return 0;

    final BitReader reader = new BitReader(data);
    final int count = (int) reader.readBits(32);
    if (count <= 0 || count > MAX_BLOCK_SIZE)
      throw new IllegalArgumentException("DeltaOfDelta decode: invalid count " + count + " (expected 1.." + MAX_BLOCK_SIZE + ")");
    output[0] = reader.readBits(64);

    if (count == 1)
      return count;

    long prevDelta = reader.readBits(64);
    output[1] = output[0] + prevDelta;

    for (int i = 2; i < count; i++) {
      long dod;
      if (reader.readBit() == 0) {
        dod = 0;
      } else if (reader.readBit() == 0) {
        dod = zigZagDecode(reader.readBits(7));
      } else if (reader.readBit() == 0) {
        dod = zigZagDecode(reader.readBits(9));
      } else if (reader.readBit() == 0) {
        dod = zigZagDecode(reader.readBits(12));
      } else {
        dod = reader.readBits(64);
      }
      prevDelta = prevDelta + dod;
      output[i] = output[i - 1] + prevDelta;
    }
    return count;
  }

  static long zigZagEncode(final long value) {
    return (value << 1) ^ (value >> 63);
  }

  static long zigZagDecode(final long encoded) {
    return (encoded >>> 1) ^ -(encoded & 1);
  }

  /**
   * Bit-level writer backed by a growing byte array.
   */
  static final class BitWriter {
    private byte[] buffer;
    private int    bitPos = 0;

    BitWriter(final int initialCapacity) {
      this.buffer = new byte[Math.max(initialCapacity, 16)];
    }

    void writeBit(final int bit) {
      ensureCapacity(1);
      if (bit != 0)
        buffer[bitPos >> 3] |= (byte) (1 << (7 - (bitPos & 7)));
      bitPos++;
    }

    void writeBits(final long value, final int numBits) {
      ensureCapacity(numBits);
      for (int i = numBits - 1; i >= 0; i--) {
        if (((value >> i) & 1) != 0)
          buffer[bitPos >> 3] |= (byte) (1 << (7 - (bitPos & 7)));
        bitPos++;
      }
    }

    byte[] toByteArray() {
      final int byteLen = (bitPos + 7) >> 3;
      final byte[] result = new byte[byteLen];
      System.arraycopy(buffer, 0, result, 0, byteLen);
      return result;
    }

    private void ensureCapacity(final int additionalBits) {
      final int requiredBytes = ((bitPos + additionalBits) + 7) >> 3;
      if (requiredBytes > buffer.length) {
        final byte[] newBuffer = new byte[Math.max(buffer.length * 2, requiredBytes)];
        System.arraycopy(buffer, 0, newBuffer, 0, buffer.length);
        buffer = newBuffer;
      }
    }
  }

  /**
   * Sliding-window bit reader over a byte array.
   * Maintains a pre-loaded 64-bit register ({@code window}) with bits MSB-aligned.
   * Each {@code readBits(n)} extracts the top n bits via a single shift, avoiding
   * the per-call byte-assembly loop of the previous implementation.
   * <p>
   * Refill happens when the window drops to ≤56 valid bits, loading up to 8 bytes
   * in one pass. This amortizes array access across ~7-8 decoded values, converting
   * the critical Gorilla XOR decode loop from ~10 array loads per value to ~1.
   */
  static final class BitReader {
    private final byte[] data;
    private final int    dataLen;
    private long         window;       // up to 64 valid bits, MSB-aligned
    private int          bitsInWindow; // number of valid bits in window
    private int          bytePos;      // next byte to consume from data[]

    BitReader(final byte[] data) {
      this.data = data;
      this.dataLen = data.length;
      this.window = 0;
      this.bitsInWindow = 0;
      this.bytePos = 0;
      refill();
    }

    int readBit() {
      if (bitsInWindow == 0)
        refill();
      final int bit = (int) (window >>> 63);
      window <<= 1;
      bitsInWindow--;
      return bit;
    }

    long readBits(final int numBits) {
      if (numBits == 0)
        return 0;
      if (numBits <= bitsInWindow) {
        // Fast path: extract directly from window — no array access
        final long result = window >>> (64 - numBits);
        // Java shift: (long << 64) is a no-op (shift distance masked to 0..63), so special-case it
        window = numBits < 64 ? window << numBits : 0;
        bitsInWindow -= numBits;
        if (bitsInWindow <= 56)
          refill();
        return result;
      }
      // Slow path: numBits > bitsInWindow (only for 64-bit header reads)
      if (bitsInWindow > 0) {
        final int have = bitsInWindow;
        long result = window >>> (64 - have);
        window = 0;
        bitsInWindow = 0;
        refill();
        final int remaining = numBits - have;
        result = (result << remaining) | (window >>> (64 - remaining));
        window <<= remaining;
        bitsInWindow -= remaining;
        if (bitsInWindow <= 56)
          refill();
        return result;
      }
      // bitsInWindow == 0
      refill();
      final long result = window >>> (64 - numBits);
      window = numBits < 64 ? window << numBits : 0;
      bitsInWindow -= numBits;
      if (bitsInWindow <= 56)
        refill();
      return result;
    }

    private void refill() {
      // Pack bytes into the lower portion of the window until we have >56 bits or exhaust input.
      // The threshold of 56 ensures adding 8 bits never overflows the 64-bit register.
      while (bitsInWindow <= 56 && bytePos < dataLen) {
        window |= (long) (data[bytePos++] & 0xFF) << (56 - bitsInWindow);
        bitsInWindow += 8;
      }
    }
  }
}
