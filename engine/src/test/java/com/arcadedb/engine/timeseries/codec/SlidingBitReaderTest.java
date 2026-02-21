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

import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the sliding-window BitReader implementation.
 * Validates correctness of bit-level reads, refill boundaries, and round-trip
 * compatibility with GorillaXOR and DeltaOfDelta codecs.
 */
class SlidingBitReaderTest {

  @Test
  void testReadBitAndReadBits1Match() {
    // Write alternating 0s and 1s, read them back via readBit() and readBits(1)
    final DeltaOfDeltaCodec.BitWriter writer = new DeltaOfDeltaCodec.BitWriter(16);
    writer.writeBit(1);
    writer.writeBit(0);
    writer.writeBit(1);
    writer.writeBit(1);
    writer.writeBit(0);

    final byte[] data = writer.toByteArray();
    final DeltaOfDeltaCodec.BitReader reader = new DeltaOfDeltaCodec.BitReader(data);

    assertThat(reader.readBit()).isEqualTo(1);
    assertThat((int) reader.readBits(1)).isEqualTo(0);
    assertThat(reader.readBit()).isEqualTo(1);
    assertThat((int) reader.readBits(1)).isEqualTo(1);
    assertThat(reader.readBit()).isEqualTo(0);
  }

  @Test
  void testReadZeroBits() {
    final DeltaOfDeltaCodec.BitWriter writer = new DeltaOfDeltaCodec.BitWriter(16);
    writer.writeBits(0xAB, 8);
    final byte[] data = writer.toByteArray();
    final DeltaOfDeltaCodec.BitReader reader = new DeltaOfDeltaCodec.BitReader(data);

    assertThat(reader.readBits(0)).isEqualTo(0);
    assertThat(reader.readBits(8)).isEqualTo(0xAB);
  }

  @Test
  void testRead64Bits() {
    // 64-bit reads are used for the header (count, first value, first delta)
    final long value = 0x123456789ABCDEF0L;
    final DeltaOfDeltaCodec.BitWriter writer = new DeltaOfDeltaCodec.BitWriter(16);
    writer.writeBits(value, 64);
    final byte[] data = writer.toByteArray();

    final DeltaOfDeltaCodec.BitReader reader = new DeltaOfDeltaCodec.BitReader(data);
    assertThat(reader.readBits(64)).isEqualTo(value);
  }

  @Test
  void testMultiple64BitReads() {
    // Gorilla XOR header: 32-bit count + 64-bit first value
    final DeltaOfDeltaCodec.BitWriter writer = new DeltaOfDeltaCodec.BitWriter(32);
    writer.writeBits(42, 32);
    writer.writeBits(Double.doubleToRawLongBits(3.14), 64);
    writer.writeBits(0xFFL, 8);

    final byte[] data = writer.toByteArray();
    final DeltaOfDeltaCodec.BitReader reader = new DeltaOfDeltaCodec.BitReader(data);

    assertThat(reader.readBits(32)).isEqualTo(42);
    assertThat(Double.longBitsToDouble(reader.readBits(64))).isEqualTo(3.14);
    assertThat(reader.readBits(8)).isEqualTo(0xFF);
  }

  @Test
  void testRefillBoundary() {
    // Write enough bits to force multiple refills
    final DeltaOfDeltaCodec.BitWriter writer = new DeltaOfDeltaCodec.BitWriter(64);
    // Write 57 bits then another 57 bits — this forces a refill mid-stream
    final long val1 = (1L << 57) - 1; // all 1s in 57 bits
    final long val2 = 0x1234567890ABCL;  // arbitrary 49-bit value
    writer.writeBits(val1, 57);
    writer.writeBits(val2, 49);

    final byte[] data = writer.toByteArray();
    final DeltaOfDeltaCodec.BitReader reader = new DeltaOfDeltaCodec.BitReader(data);

    assertThat(reader.readBits(57)).isEqualTo(val1);
    assertThat(reader.readBits(49)).isEqualTo(val2);
  }

  @Test
  void testVeryShortData() {
    // 1 byte = 8 bits
    final DeltaOfDeltaCodec.BitWriter writer = new DeltaOfDeltaCodec.BitWriter(4);
    writer.writeBits(0b10110, 5);
    final byte[] data = writer.toByteArray();

    final DeltaOfDeltaCodec.BitReader reader = new DeltaOfDeltaCodec.BitReader(data);
    assertThat(reader.readBits(5)).isEqualTo(0b10110);
  }

  @Test
  void testMixedBitAndBitsReads() {
    // Simulates the Gorilla XOR decode pattern: readBit + readBit + readBits(N)
    final DeltaOfDeltaCodec.BitWriter writer = new DeltaOfDeltaCodec.BitWriter(32);
    writer.writeBit(1);       // control bit
    writer.writeBit(0);       // case '10' indicator
    writer.writeBits(0x1234567890AL, 45);  // payload

    writer.writeBit(1);       // control bit
    writer.writeBit(1);       // case '11' indicator
    writer.writeBits(12, 6);  // leading zeros
    writer.writeBits(50, 6);  // block size - 1 = 50, so blockSize = 51
    writer.writeBits(0x7FFFFFFFFFFFFL, 51); // XOR payload

    final byte[] data = writer.toByteArray();
    final DeltaOfDeltaCodec.BitReader reader = new DeltaOfDeltaCodec.BitReader(data);

    // Case '10'
    assertThat(reader.readBit()).isEqualTo(1);
    assertThat(reader.readBit()).isEqualTo(0);
    assertThat(reader.readBits(45)).isEqualTo(0x1234567890AL);

    // Case '11'
    assertThat(reader.readBit()).isEqualTo(1);
    assertThat(reader.readBit()).isEqualTo(1);
    assertThat(reader.readBits(6)).isEqualTo(12);
    assertThat(reader.readBits(6)).isEqualTo(50);
    assertThat(reader.readBits(51)).isEqualTo(0x7FFFFFFFFFFFFL);
  }

  @Test
  void testGorillaXorRoundTripLargeRandom() {
    // Simulates benchmark data pattern: 20.0 + random * 15.0
    final Random rng = new Random(12345);
    final double[] input = new double[65536];
    for (int i = 0; i < input.length; i++)
      input[i] = 20.0 + rng.nextDouble() * 15.0;

    final byte[] encoded = GorillaXORCodec.encode(input);
    final double[] decoded = GorillaXORCodec.decode(encoded);
    assertThat(decoded).containsExactly(input);

    // Also test buffer-reuse variant
    final double[] buf = new double[65536];
    final int count = GorillaXORCodec.decode(encoded, buf);
    assertThat(count).isEqualTo(input.length);
    for (int i = 0; i < count; i++)
      assertThat(buf[i]).isEqualTo(input[i]);
  }

  @Test
  void testDeltaOfDeltaRoundTripLargeMonotonic() {
    // Monotonically increasing timestamps at ~100ms intervals with jitter
    final Random rng = new Random(54321);
    final long[] input = new long[65536];
    input[0] = System.currentTimeMillis();
    for (int i = 1; i < input.length; i++)
      input[i] = input[i - 1] + 100 + rng.nextInt(10) - 5;

    final byte[] encoded = DeltaOfDeltaCodec.encode(input);
    final long[] decoded = DeltaOfDeltaCodec.decode(encoded);
    assertThat(decoded).containsExactly(input);

    // Also test buffer-reuse variant
    final long[] buf = new long[65536];
    final int count = DeltaOfDeltaCodec.decode(encoded, buf);
    assertThat(count).isEqualTo(input.length);
    for (int i = 0; i < count; i++)
      assertThat(buf[i]).isEqualTo(input[i]);
  }

  @Test
  void testGorillaXorRoundTripSpecialValues() {
    final double[] input = {
        0.0, -0.0, Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY,
        Double.MAX_VALUE, Double.MIN_VALUE, Math.PI, Math.E,
        1.0, 1.0, 1.0, // consecutive identical values (zero XOR path)
        -1.0, 2.0, -2.0 // sign changes (large XOR)
    };

    final byte[] encoded = GorillaXORCodec.encode(input);
    final double[] decoded = GorillaXORCodec.decode(encoded);

    assertThat(decoded.length).isEqualTo(input.length);
    for (int i = 0; i < input.length; i++)
      assertThat(Double.doubleToRawLongBits(decoded[i])).isEqualTo(Double.doubleToRawLongBits(input[i]));
  }

  @Test
  void testDeltaOfDeltaRoundTripConstantDelta() {
    // Perfectly regular timestamps — all delta-of-deltas are 0
    final long[] input = new long[10000];
    for (int i = 0; i < input.length; i++)
      input[i] = 1000000L + i * 100L;

    final byte[] encoded = DeltaOfDeltaCodec.encode(input);
    final long[] decoded = DeltaOfDeltaCodec.decode(encoded);
    assertThat(decoded).containsExactly(input);
  }

  @Test
  void testDeltaOfDeltaRoundTripAllBuckets() {
    // Exercise all encoding buckets: dod=0, |dod|<=63, |dod|<=255, |dod|<=2047, else
    final long[] input = new long[20];
    input[0] = 1000;
    input[1] = 1100;  // delta=100
    input[2] = 1200;  // delta=100, dod=0
    input[3] = 1310;  // delta=110, dod=10 (bucket: |dod|<=63)
    input[4] = 1410;  // delta=100, dod=-10
    input[5] = 1710;  // delta=300, dod=200 (bucket: |dod|<=255)
    input[6] = 1810;  // delta=100, dod=-200
    input[7] = 3810;  // delta=2000, dod=1900 (bucket: |dod|<=2047)
    input[8] = 3910;  // delta=100, dod=-1900
    input[9] = 53910; // delta=50000, dod=49900 (bucket: raw 64-bit)
    // Fill rest with regular increments
    for (int i = 10; i < input.length; i++)
      input[i] = input[i - 1] + 100;

    final byte[] encoded = DeltaOfDeltaCodec.encode(input);
    final long[] decoded = DeltaOfDeltaCodec.decode(encoded);
    assertThat(decoded).containsExactly(input);
  }
}
