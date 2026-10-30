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

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NumericCodecChooserTest {

  private static long[] random() {
    final Random r = new Random(11);
    final long[] v = new long[4096];
    for (int i = 0; i < v.length; i++)
      v[i] = r.nextInt(1 << 20);
    return v;
  }

  private static long[] arithmetic() {
    final long[] v = new long[10_000];
    for (int i = 0; i < v.length; i++)
      v[i] = 5 + 3L * i;
    return v;
  }

  private static long[] piecewise() {
    final long[] v = new long[4 * 512];
    for (int b = 0; b < 4; b++)
      for (int i = 0; i < 512; i++)
        v[b * 512 + i] = b * 1_000_000_000_000L + i * (b + 1) * 7L;
    return v;
  }

  private static long[] constant() {
    final long[] v = new long[1000];
    Arrays.fill(v, 7);
    return v;
  }

  private static void assertWins(final long[] v, final byte id) {
    final byte[] e = NumericCodecChooser.encodeBest(v, 0, v.length);
    assertThat(e[0]).isEqualTo(id);
    assertReads(v, e);
  }

  private static void assertReads(final long[] v, final byte[] e) {
    assertThat(NumericCodecChooser.count(e)).isEqualTo(v.length);
    final long[] out = new long[v.length];
    NumericCodecChooser.decode(e, out);
    assertThat(out).isEqualTo(v);
    for (int i = 0; i < v.length; i++)
      assertThat(NumericCodecChooser.get(e, i)).isEqualTo(v[i]);
  }

  @Test
  void picksBitPackedForRandomValues() {
    assertWins(random(), BitPackedCodec.CODEC_ID);
  }

  @Test
  void picksLinearForArithmeticSequence() {
    assertWins(arithmetic(), LinearCodec.CODEC_ID);
  }

  @Test
  void picksBlockwiseForPiecewiseLinear() {
    assertWins(piecewise(), BlockwiseLinearCodec.CODEC_ID);
  }

  @Test
  void allEqualAndEmptyPickBitPacked() {
    assertWins(constant(), BitPackedCodec.CODEC_ID);
    assertWins(new long[0], BitPackedCodec.CODEC_ID);
  }

  @Test
  void singleValueAndExtremes() {
    assertWins(new long[] { 42 }, BitPackedCodec.CODEC_ID);
    assertWins(new long[] { Long.MIN_VALUE }, BitPackedCodec.CODEC_ID);
    final long[] v = new long[2000];
    for (int i = 0; i < v.length; i++)
      v[i] = (i & 1) == 0 ? Long.MIN_VALUE : Long.MAX_VALUE;
    final byte[] e = NumericCodecChooser.encodeBest(v, 0, v.length);
    assertReads(v, e);
    assertThat(e.length).isEqualTo(smallest(v, 0, v.length));
    final long[] line = { Long.MIN_VALUE, Long.MAX_VALUE };
    assertReads(line, NumericCodecChooser.encodeBest(line, 0, 2));
  }

  @Test
  void readsDispatchOnTheCodecId() {
    for (final long[] v : new long[][] { random(), arithmetic(), piecewise() })
      assertReads(v, NumericCodecChooser.encodeBest(v, 0, v.length));
  }

  @Test
  void encodesOnlyTheSlice() {
    final long[] all = arithmetic();
    final byte[] e = NumericCodecChooser.encodeBest(all, 100, 50);
    assertThat(NumericCodecChooser.count(e)).isEqualTo(50);
    assertReads(Arrays.copyOfRange(all, 100, 150), e);
  }

  @Test
  void chosenSizeIsAlwaysTheSmallestAndTiesFollowTheOrder() {
    final Random r = new Random(5);
    for (int n = 0; n < 40; n++) {
      final long[] v = new long[n];
      final int shape = n % 4;
      for (int i = 0; i < n; i++)
        v[i] = shape == 0 ? r.nextInt(100) : shape == 1 ? 3L * i : shape == 2 ? 7 : i * i;
      final byte[] e = NumericCodecChooser.encodeBest(v, 0, n);
      if (n > 0)
        assertThat(e.length).isEqualTo(smallest(v, 0, n));
      final int bp = BitPackedCodec.encodedSize(v, 0, n);
      final int li = LinearCodec.encodedSize(v, 0, n);
      final int bw = BlockwiseLinearCodec.encodedSize(v, 0, n);
      final byte expected = n == 0 || (bp <= li && bp <= bw) ? BitPackedCodec.CODEC_ID : li <= bw ? LinearCodec.CODEC_ID : BlockwiseLinearCodec.CODEC_ID;
      assertThat(e[0]).isEqualTo(expected);
      assertReads(v, e);
    }
  }

  @Test
  void unknownIdIsRefused() {
    assertThatThrownBy(() -> NumericCodecChooser.get(new byte[] { 9, 0, 0, 0 }, 0)).isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unknown numeric codec id 9");
    assertThatThrownBy(() -> NumericCodecChooser.count(new byte[] { 9, 0, 0, 0 })).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> NumericCodecChooser.decode(new byte[] { 9, 0, 0, 0 }, new long[1])).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> NumericCodecChooser.get(new byte[0], 0)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> NumericCodecChooser.count(new byte[0])).isInstanceOf(IllegalArgumentException.class);
  }

  private static int smallest(final long[] v, final int off, final int n) {
    return Math.min(BitPackedCodec.encodedSize(v, off, n),
        Math.min(LinearCodec.encodedSize(v, off, n), BlockwiseLinearCodec.encodedSize(v, off, n)));
  }
}
