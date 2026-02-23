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
class Simple8bCodecTest {

  @Test
  void testEmpty() {
    assertThat(Simple8bCodec.decode(Simple8bCodec.encode(new long[0]))).isEmpty();
    assertThat(Simple8bCodec.decode(Simple8bCodec.encode(null))).isEmpty();
  }

  @Test
  void testSingleValue() {
    final long[] input = { 42 };
    assertThat(Simple8bCodec.decode(Simple8bCodec.encode(input))).containsExactly(input);
  }

  @Test
  void testAllZeros() {
    final long[] input = new long[240];
    final byte[] encoded = Simple8bCodec.encode(input);
    assertThat(Simple8bCodec.decode(encoded)).containsExactly(input);

    // 240 zeros should fit in a single 8-byte word + 4 bytes header
    assertThat(encoded.length).isEqualTo(12);
  }

  @Test
  void testSmallInts() {
    // Values 0-1 (1 bit each) → 60 per word
    final long[] input = new long[60];
    for (int i = 0; i < input.length; i++)
      input[i] = i % 2;

    assertThat(Simple8bCodec.decode(Simple8bCodec.encode(input))).containsExactly(input);
  }

  @Test
  void testMediumInts() {
    // Values 0-255 (8 bits each) → 7 per word
    final long[] input = new long[100];
    for (int i = 0; i < input.length; i++)
      input[i] = i % 256;

    assertThat(Simple8bCodec.decode(Simple8bCodec.encode(input))).containsExactly(input);
  }

  @Test
  void testLargeInts() {
    // Values that need 30 bits → 2 per word
    final long[] input = { 500_000_000L, 700_000_000L, 100_000_000L, 999_999_999L };
    assertThat(Simple8bCodec.decode(Simple8bCodec.encode(input))).containsExactly(input);
  }

  @Test
  void testVeryLargeInts() {
    // After zigzag encoding, max positive value that fits in 60 bits is (1L << 59) - 1
    final long[] input = { (1L << 59) - 1, (1L << 58) + 1, (1L << 58) - 1 };
    assertThat(Simple8bCodec.decode(Simple8bCodec.encode(input))).containsExactly(input);
  }

  @Test
  void testNegativeValues() {
    // Zigzag encoding allows negative values
    final long[] input = { -1, -100, -1000, 0, 42, -42 };
    assertThat(Simple8bCodec.decode(Simple8bCodec.encode(input))).containsExactly(input);
  }

  @Test
  void testLargeNegativeValues() {
    // Values within the zigzag-encodable range: max zigzag output must fit in 60 bits
    final long[] input = { -(1L << 58), -(1L << 57), -999_999_999L, 999_999_999L };
    assertThat(Simple8bCodec.decode(Simple8bCodec.encode(input))).containsExactly(input);
  }

  @Test
  void testMixedSizes() {
    final long[] input = { 0, 1, 255, 1000, 0, 0, 0, 50000, 1 };
    assertThat(Simple8bCodec.decode(Simple8bCodec.encode(input))).containsExactly(input);
  }

  @Test
  void testAllSameNonZero() {
    final long[] input = new long[100];
    java.util.Arrays.fill(input, 7L);
    assertThat(Simple8bCodec.decode(Simple8bCodec.encode(input))).containsExactly(input);
  }

  @Test
  void testRandomValues() {
    final Random rng = new Random(42);
    final long[] input = new long[200];
    for (int i = 0; i < input.length; i++)
      input[i] = Math.abs(rng.nextInt(10000));

    assertThat(Simple8bCodec.decode(Simple8bCodec.encode(input))).containsExactly(input);
  }
}
