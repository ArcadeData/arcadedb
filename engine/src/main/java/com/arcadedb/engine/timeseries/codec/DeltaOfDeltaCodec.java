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
 * - |dod| <= 63: store '10' + 7-bit value (9 bits)
 * - |dod| <= 255: store '110' + 9-bit value (12 bits)
 * - |dod| <= 2047: store '1110' + 12-bit value (16 bits)
 * - otherwise: store '1111' + 64-bit raw value (68 bits)
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class DeltaOfDeltaCodec {

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
      } else if (dod >= -63 && dod <= 63) {
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
   * Bit-level reader over a byte array.
   */
  static final class BitReader {
    private final byte[] data;
    private       int    bitPos = 0;

    BitReader(final byte[] data) {
      this.data = data;
    }

    int readBit() {
      final int byteIndex = bitPos >> 3;
      final int bitIndex = 7 - (bitPos & 7);
      bitPos++;
      return (data[byteIndex] >> bitIndex) & 1;
    }

    long readBits(final int numBits) {
      long result = 0;
      for (int i = 0; i < numBits; i++) {
        result = (result << 1) | readBit();
      }
      return result;
    }
  }
}
