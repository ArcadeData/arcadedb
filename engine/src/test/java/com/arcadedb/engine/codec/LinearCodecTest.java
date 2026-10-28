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

class LinearCodecTest {

  private static void assertRoundTrip(final long[] values, final byte[] encoded) {
    assertThat(LinearCodec.count(encoded)).isEqualTo(values.length);
    final long[] out = new long[values.length];
    LinearCodec.decode(encoded, out);
    assertThat(out).isEqualTo(values);
    for (int i = 0; i < values.length; i++)
      assertThat(LinearCodec.get(encoded, i)).isEqualTo(values[i]);
  }

  @Test
  void emptyAndSingleValue() {
    final byte[] e = LinearCodec.encode(new long[0], 0, 0);
    assertThat(LinearCodec.count(e)).isZero();
    LinearCodec.decode(e, new long[0]);
    assertThat(e.length).isEqualTo(48);

    final byte[] one = LinearCodec.encode(new long[] { 9 }, 0, 1);
    assertThat(LinearCodec.get(one, 0)).isEqualTo(9L);
    assertRoundTrip(new long[] { 9 }, one);
  }

  @Test
  void arithmeticSequenceHasZeroWidthResiduals() {
    final long[] v = new long[10_000];
    for (int i = 0; i < v.length; i++)
      v[i] = 5 + 3L * i;
    final byte[] e = LinearCodec.encode(v, 0, v.length);
    assertThat(e.length).isEqualTo(48);
    assertThat(e[0]).isEqualTo(LinearCodec.CODEC_ID);
    assertRoundTrip(v, e);
  }

  @Test
  void noisyLineRoundTrips() {
    final Random r = new Random(3);
    final long[] v = new long[5000];
    for (int i = 0; i < v.length; i++)
      v[i] = i * 1000L + r.nextInt(101) - 50;
    assertRoundTrip(v, LinearCodec.encode(v, 0, v.length));
  }

  @Test
  void negativeSlopeRoundTrips() {
    final long[] v = new long[2000];
    for (int i = 0; i < v.length; i++)
      v[i] = 1_000_000L - 17L * i;
    final byte[] e = LinearCodec.encode(v, 0, v.length);
    assertThat(e.length).isEqualTo(48);
    assertRoundTrip(v, e);
  }

  @Test
  void extremesRoundTrip() {
    final long[] a = { Long.MIN_VALUE, Long.MAX_VALUE, Long.MIN_VALUE, Long.MAX_VALUE, 0 };
    assertRoundTrip(a, LinearCodec.encode(a, 0, a.length));

    // MAX descending to MIN in 1000 even steps
    final long[] b = new long[1000];
    final long step = Long.divideUnsigned(-1L, 999);
    for (int i = 0; i < b.length; i++)
      b[i] = Long.MAX_VALUE - step * i;
    assertRoundTrip(b, LinearCodec.encode(b, 0, b.length));

    final long[] c = new long[1000];
    final long stepC = (Long.MAX_VALUE / 500) + 1;
    for (int i = 0; i < c.length; i++)
      c[i] = Long.MAX_VALUE - 2 * stepC * i;
    c[c.length - 1] = Long.MIN_VALUE;
    assertRoundTrip(c, LinearCodec.encode(c, 0, c.length));

    final long[] d = { Long.MAX_VALUE, Long.MIN_VALUE };
    assertRoundTrip(d, LinearCodec.encode(d, 0, 2));
    final long[] e2 = { Long.MIN_VALUE, Long.MAX_VALUE };
    assertRoundTrip(e2, LinearCodec.encode(e2, 0, 2));
  }

  @Test
  void subrangeEncodesOnlyTheSlice() {
    final long[] values = new long[200];
    for (int i = 0; i < values.length; i++)
      values[i] = 40 + 6L * i;
    values[6] = Long.MIN_VALUE;
    values[107] = Long.MAX_VALUE;
    final byte[] e = LinearCodec.encode(values, 7, 100);
    assertThat(LinearCodec.count(e)).isEqualTo(100);
    assertThat(LinearCodec.get(e, 0)).isEqualTo(values[7]);
    assertThat(e.length).isEqualTo(48);
    assertThat(e[24 + 1]).isEqualTo((byte) 0);
    assertThat(LinearCodec.encodedSize(values, 7, 100)).isEqualTo(48);
    assertRoundTrip(Arrays.copyOfRange(values, 7, 107), e);
    assertThatThrownBy(() -> LinearCodec.encode(values, 150, 100)).isInstanceOf(IndexOutOfBoundsException.class);
  }

  @Test
  void encodedSizeMatchesEncode() {
    final Random r = new Random(11);
    for (int t = 0; t < 50; t++) {
      final int n = r.nextInt(400);
      final long[] v = new long[n];
      final int kind = t % 4;
      for (int i = 0; i < n; i++)
        v[i] = switch (kind) {
          case 0 -> r.nextLong();
          case 1 -> i * 8L + r.nextInt(4) * 8L;
          case 2 -> r.nextInt(20) - 10;
          default -> 1000L * i + r.nextInt(3);
        };
      final byte[] e = LinearCodec.encode(v, 0, n);
      assertThat(LinearCodec.encodedSize(v, 0, n)).isEqualTo(e.length);
      assertRoundTrip(v, e);
    }
  }

  @Test
  void decodeIntoOffsetAndBase() {
    final long[] v = { 4, 9, 14, 20, 24 };
    final byte[] e = LinearCodec.encode(v, 0, v.length);
    final byte[] shifted = new byte[e.length + 5];
    System.arraycopy(e, 0, shifted, 5, e.length);
    final long[] out = new long[8];
    Arrays.fill(out, -1);
    LinearCodec.decode(shifted, 5, out, 2);
    assertThat(out).containsExactly(-1, -1, 4, 9, 14, 20, 24, -1);
    assertThat(LinearCodec.count(shifted, 5)).isEqualTo(5);
    assertThat(LinearCodec.get(shifted, 5, 3)).isEqualTo(20L);
  }

  @Test
  void decodeRefusesForeignInput() {
    final byte[] e = LinearCodec.encode(new long[] { 1, 2, 3 }, 0, 3);
    e[0] = 1;
    assertThatThrownBy(() -> LinearCodec.decode(e, new long[3])).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> LinearCodec.count(e)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> LinearCodec.get(e, 0)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void malformedInputThrowsIllegalArgument() {
    assertThatThrownBy(() -> LinearCodec.count(new byte[10])).isInstanceOf(IllegalArgumentException.class);
    final byte[] good = LinearCodec.encode(new long[] { 1, 5, 2, 8, 3 }, 0, 5);
    // truncated payload
    final byte[] cut = Arrays.copyOf(good, good.length - 8);
    assertThatThrownBy(() -> LinearCodec.decode(cut, new long[5])).isInstanceOf(IllegalArgumentException.class);
    // output too short
    assertThatThrownBy(() -> LinearCodec.decode(good, new long[2])).isInstanceOf(IllegalArgumentException.class);
    // inner bit-packed id corrupted
    final byte[] bad = good.clone();
    bad[24] = 9;
    assertThatThrownBy(() -> LinearCodec.decode(bad, new long[5])).isInstanceOf(IllegalArgumentException.class);
    // negative count
    final byte[] neg = good.clone();
    neg[24 + 7] = (byte) 0x80;
    assertThatThrownBy(() -> LinearCodec.count(neg)).isInstanceOf(IllegalArgumentException.class);
    // crafted count of 0 with a header-only body decodes to nothing
    final byte[] zero = LinearCodec.encode(new long[0], 0, 0);
    LinearCodec.decode(zero, new long[0]);
  }
}
