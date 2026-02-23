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
class GorillaXORCodecTest {

  @Test
  void testEmpty() {
    assertThat(GorillaXORCodec.decode(GorillaXORCodec.encode(new double[0]))).isEmpty();
    assertThat(GorillaXORCodec.decode(GorillaXORCodec.encode(null))).isEmpty();
  }

  @Test
  void testSingleValue() {
    final double[] input = { 22.5 };
    final byte[] encoded = GorillaXORCodec.encode(input);
    assertThat(GorillaXORCodec.decode(encoded)).containsExactly(input);
  }

  @Test
  void testConstantValues() {
    final double[] input = new double[100];
    java.util.Arrays.fill(input, 42.0);

    final byte[] encoded = GorillaXORCodec.encode(input);
    assertThat(GorillaXORCodec.decode(encoded)).containsExactly(input);

    // Constant values should compress extremely well (1 bit per sample after first)
    assertThat(encoded.length).isLessThan(input.length);
  }

  @Test
  void testSlowlyChangingValues() {
    // Temperature-like data: small increments
    final double[] input = new double[500];
    input[0] = 20.0;
    for (int i = 1; i < input.length; i++)
      input[i] = input[i - 1] + 0.1;

    final byte[] encoded = GorillaXORCodec.encode(input);
    assertThat(GorillaXORCodec.decode(encoded)).containsExactly(input);
  }

  @Test
  void testRandomDoubles() {
    final Random rng = new Random(42);
    final double[] input = new double[300];
    for (int i = 0; i < input.length; i++)
      input[i] = rng.nextDouble() * 1000.0;

    final byte[] encoded = GorillaXORCodec.encode(input);
    assertThat(GorillaXORCodec.decode(encoded)).containsExactly(input);
  }

  @Test
  void testSpecialValues() {
    final double[] input = { 0.0, -0.0, Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, Double.MAX_VALUE,
        Double.MIN_VALUE };
    final byte[] encoded = GorillaXORCodec.encode(input);
    final double[] decoded = GorillaXORCodec.decode(encoded);

    assertThat(decoded.length).isEqualTo(input.length);
    assertThat(decoded[0]).isEqualTo(0.0);
    assertThat(Double.doubleToRawLongBits(decoded[1])).isEqualTo(Double.doubleToRawLongBits(-0.0));
    assertThat(decoded[2]).isNaN();
    assertThat(decoded[3]).isEqualTo(Double.POSITIVE_INFINITY);
    assertThat(decoded[4]).isEqualTo(Double.NEGATIVE_INFINITY);
    assertThat(decoded[5]).isEqualTo(Double.MAX_VALUE);
    assertThat(decoded[6]).isEqualTo(Double.MIN_VALUE);
  }

  @Test
  void testTwoValues() {
    final double[] input = { 1.0, 2.0 };
    final byte[] encoded = GorillaXORCodec.encode(input);
    assertThat(GorillaXORCodec.decode(encoded)).containsExactly(input);
  }

  @Test
  void testNegativeValues() {
    final double[] input = { -100.5, -100.3, -100.1, -99.9, -99.7 };
    final byte[] encoded = GorillaXORCodec.encode(input);
    assertThat(GorillaXORCodec.decode(encoded)).containsExactly(input);
  }

  /**
   * Regression test: decoder must initialise prevLeading to Integer.MAX_VALUE, not 0.
   * If prevLeading starts at 0, the '10' path (reuse block) may fire on the second pair
   * without a prior '11' header having been written, producing wrong values.
   * The encoder always emits '11' first, so the decoder must start with MAX_VALUE
   * so that leading >= prevLeading is false and the '11' path is taken correctly.
   */
  @Test
  void testDecoderPrevLeadingInitialisedToMaxValue() {
    // Construct two values whose XOR has the same leading/trailing zeros as
    // "no prior block" — use a constant array where the third differs.
    // 1.0 XOR 2.0 has many leading zeros; encoding must round-trip correctly.
    final double[] input = { 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0 };
    assertThat(GorillaXORCodec.decode(GorillaXORCodec.encode(input))).containsExactly(input);
  }

  @Test
  void testDecodeBufferVariant() {
    final double[] input = { 1.0, 2.0, 3.0, 4.0 };
    final byte[] encoded = GorillaXORCodec.encode(input);
    final double[] output = new double[input.length];
    GorillaXORCodec.decode(encoded, output);
    assertThat(output).containsExactly(input);
  }
}
