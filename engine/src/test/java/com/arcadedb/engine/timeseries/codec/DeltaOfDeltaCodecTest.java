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
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class DeltaOfDeltaCodecTest {

  @Test
  void testEmpty() {
    assertThat(DeltaOfDeltaCodec.decode(DeltaOfDeltaCodec.encode(new long[0]))).isEmpty();
    assertThat(DeltaOfDeltaCodec.decode(DeltaOfDeltaCodec.encode(null))).isEmpty();
  }

  @Test
  void testSingleValue() {
    final long[] input = { 1000000000L };
    final byte[] encoded = DeltaOfDeltaCodec.encode(input);
    assertThat(DeltaOfDeltaCodec.decode(encoded)).containsExactly(input);
  }

  @Test
  void testRegularIntervals() {
    // Regular 10-second intervals — all delta-of-deltas are 0
    final long[] input = new long[1000];
    for (int i = 0; i < input.length; i++)
      input[i] = 1_000_000_000L + i * 10_000_000_000L;

    final byte[] encoded = DeltaOfDeltaCodec.encode(input);
    assertThat(DeltaOfDeltaCodec.decode(encoded)).containsExactly(input);

    // Should compress well: regular intervals encode to ~1 bit per sample after first two
    assertThat(encoded.length).isLessThan(input.length * 8 / 4);
  }

  @Test
  void testMonotonicIncreasing() {
    final long[] input = { 100, 200, 300, 400, 500, 600 };
    final byte[] encoded = DeltaOfDeltaCodec.encode(input);
    assertThat(DeltaOfDeltaCodec.decode(encoded)).containsExactly(input);
  }

  @Test
  void testNonMonotonic() {
    final long[] input = { 100, 300, 250, 400, 350, 500 };
    final byte[] encoded = DeltaOfDeltaCodec.encode(input);
    assertThat(DeltaOfDeltaCodec.decode(encoded)).containsExactly(input);
  }

  @Test
  void testRandomTimestamps() {
    final Random rng = new Random(42);
    final long[] input = new long[500];
    input[0] = Math.abs(rng.nextLong() % 1_000_000_000_000L);
    for (int i = 1; i < input.length; i++)
      input[i] = input[i - 1] + Math.abs(rng.nextInt(10000)) + 1;

    final byte[] encoded = DeltaOfDeltaCodec.encode(input);
    assertThat(DeltaOfDeltaCodec.decode(encoded)).containsExactly(input);
  }

  @Test
  void testTwoValues() {
    final long[] input = { 100, 200 };
    final byte[] encoded = DeltaOfDeltaCodec.encode(input);
    assertThat(DeltaOfDeltaCodec.decode(encoded)).containsExactly(input);
  }

  @Test
  void testLargeDeltaOfDelta() {
    // Large jumps that require 64-bit encoding
    final long[] input = { 0, 1_000_000_000_000L, 1_000_000_000_001L, 5_000_000_000_000L };
    final byte[] encoded = DeltaOfDeltaCodec.encode(input);
    assertThat(DeltaOfDeltaCodec.decode(encoded)).containsExactly(input);
  }

  @Test
  void testZigZagEncoding() {
    assertThat(DeltaOfDeltaCodec.zigZagEncode(0)).isEqualTo(0);
    assertThat(DeltaOfDeltaCodec.zigZagEncode(-1)).isEqualTo(1);
    assertThat(DeltaOfDeltaCodec.zigZagEncode(1)).isEqualTo(2);
    assertThat(DeltaOfDeltaCodec.zigZagEncode(-2)).isEqualTo(3);
    assertThat(DeltaOfDeltaCodec.zigZagDecode(DeltaOfDeltaCodec.zigZagEncode(63))).isEqualTo(63);
    assertThat(DeltaOfDeltaCodec.zigZagDecode(DeltaOfDeltaCodec.zigZagEncode(-63))).isEqualTo(-63);
  }

  @Test
  void testAllSameTimestamp() {
    final long[] input = { 42, 42, 42, 42, 42 };
    final byte[] encoded = DeltaOfDeltaCodec.encode(input);
    assertThat(DeltaOfDeltaCodec.decode(encoded)).containsExactly(input);
  }

  /**
   * Regression test: dod == -64 must use the compact 7-bit ZigZag bucket, not the 64-bit fallback.
   * ZigZag(-64) = 127 which fits in 7 bits. Previously the range check excluded -64.
   */
  @Test
  void testDodMinusSixtyFourUsesCompactEncoding() {
    // Construct timestamps where dod == -64: delta[1]=100, delta[2]=36 → dod = 36 - 100 = -64
    final long[] input = { 1000L, 1100L, 1136L };
    final byte[] encoded = DeltaOfDeltaCodec.encode(input);
    assertThat(DeltaOfDeltaCodec.decode(encoded)).containsExactly(input);

    // Verify it uses the compact bucket (encoded should be shorter than if using 64-bit fallback)
    // 64-bit fallback: 4(count) + 8(first) + 8(firstDelta) + (4 + 64) / 8 = 29 bytes
    // 7-bit bucket: 4(count) + 8(first) + 8(firstDelta) + (2 + 7 + 7) / 8 = ~22 bytes
    assertThat(encoded.length).isLessThan(29);
  }
}
