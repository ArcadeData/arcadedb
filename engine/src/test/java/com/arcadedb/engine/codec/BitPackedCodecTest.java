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

class BitPackedCodecTest {

  private static void assertRoundTrip(final long[] values, final byte[] encoded) {
    assertThat(BitPackedCodec.count(encoded)).isEqualTo(values.length);
    final long[] out = new long[values.length];
    BitPackedCodec.decode(encoded, out);
    assertThat(out).isEqualTo(values);
    for (int i = 0; i < values.length; i++)
      assertThat(BitPackedCodec.get(encoded, i)).isEqualTo(values[i]);
  }

  @Test
  void emptyEncodesHeaderOnly() {
    final byte[] e = BitPackedCodec.encode(new long[0], 0, 0);
    assertThat(e.length).isEqualTo(24);
    assertThat(BitPackedCodec.count(e)).isZero();
    BitPackedCodec.decode(e, new long[0]);
  }

  @Test
  void singleValueHasWidthZero() {
    final byte[] e = BitPackedCodec.encode(new long[] { 42 }, 0, 1);
    assertThat(e.length).isEqualTo(24);
    assertThat(e[1]).isEqualTo((byte) 0);
    assertThat(BitPackedCodec.get(e, 0)).isEqualTo(42L);
  }

  @Test
  void allEqualHasWidthZero() {
    final long[] v = new long[1000];
    Arrays.fill(v, -7);
    final byte[] e = BitPackedCodec.encode(v, 0, v.length);
    assertThat(e.length).isEqualTo(24);
    assertRoundTrip(v, e);
  }

  @Test
  void negativeValuesRoundTrip() {
    final long[] v = new long[1000];
    for (int i = 0; i < v.length; i++)
      v[i] = i - 500;
    final byte[] e = BitPackedCodec.encode(v, 0, v.length);
    assertThat(e[1]).isEqualTo((byte) 10);
    assertRoundTrip(v, e);
  }

  @Test
  void gcdReducesWidth() {
    final long[] v = new long[1000];
    for (int i = 0; i < v.length; i++)
      v[i] = 5 + i * 1000L;
    final byte[] e = BitPackedCodec.encode(v, 0, v.length);
    assertThat(e[1]).isEqualTo((byte) 10);
    assertRoundTrip(v, e);
  }

  @Test
  void fullRangeUsesWidth64() {
    final long[] v = { Long.MIN_VALUE, Long.MAX_VALUE, 0, -1, 12345 };
    final byte[] e = BitPackedCodec.encode(v, 0, v.length);
    assertThat(e[1]).isEqualTo((byte) 64);
    assertRoundTrip(v, e);
  }

  @Test
  void gcdHandlesUnsignedDeltas() {
    final long[] v = { Long.MIN_VALUE, 0L };
    final byte[] e = BitPackedCodec.encode(v, 0, v.length);
    assertThat(e[1]).isEqualTo((byte) 1);
    assertRoundTrip(v, e);
  }

  @Test
  void everyWidthRoundTrips() {
    for (int w = 1; w <= 64; w++) {
      final Random r = new Random(w);
      final long mask = w == 64 ? -1L : (1L << w) - 1;
      final long[] v = new long[1000];
      for (int i = 0; i < v.length; i++)
        v[i] = r.nextLong() & mask;
      v[0] = mask;
      v[1] = 0;
      v[2] = 1;
      final byte[] e = BitPackedCodec.encode(v, 0, v.length);
      assertThat(e[1]).as("width %d", w).isEqualTo((byte) w);
      assertRoundTrip(v, e);
    }
  }

  @Test
  void subrangeEncodesOnlyTheSlice() {
    final long[] v = new long[100];
    for (int i = 0; i < v.length; i++)
      v[i] = i;
    v[9] = Long.MIN_VALUE;
    v[30] = Long.MAX_VALUE;
    final byte[] e = BitPackedCodec.encode(v, 10, 20);
    assertThat(e[1]).isEqualTo((byte) 5);
    assertThat(e.length).isEqualTo(24 + 2 * 8);
    assertThat(BitPackedCodec.encodedSize(v, 10, 20)).isEqualTo(e.length);
    assertThat(BitPackedCodec.count(e)).isEqualTo(20);
    assertThat(BitPackedCodec.get(e, 0)).isEqualTo(10L);
    assertThat(BitPackedCodec.get(e, 19)).isEqualTo(29L);
    assertRoundTrip(Arrays.copyOfRange(v, 10, 30), e);
    assertThatThrownBy(() -> BitPackedCodec.encode(v, 90, 20)).isInstanceOf(IndexOutOfBoundsException.class);
    assertThatThrownBy(() -> BitPackedCodec.encode(v, -1, 2)).isInstanceOf(IndexOutOfBoundsException.class);
  }

  @Test
  void encodedSizeMatchesEncode() {
    final Random r = new Random(7);
    for (int t = 0; t < 50; t++) {
      final long[] v = new long[r.nextInt(3001)];
      final int bits = 1 + r.nextInt(64);
      final long step = t % 3 == 0 ? 1 + r.nextInt(1000) : 1;
      final long shift = t % 2 == 0 ? r.nextLong() : 0;
      for (int i = 0; i < v.length; i++)
        v[i] = (r.nextLong() >>> (64 - bits)) * step + shift;
      final byte[] e = BitPackedCodec.encode(v, 0, v.length);
      assertThat(BitPackedCodec.encodedSize(v, 0, v.length)).isEqualTo(e.length);
      assertRoundTrip(v, e);
    }
  }

  @Test
  void minAndMaxLongWrapBack() {
    final long[] v = { Long.MIN_VALUE, Long.MAX_VALUE };
    final byte[] e = BitPackedCodec.encode(v, 0, 2);
    assertThat(e[1]).isEqualTo((byte) 1);
    assertThat(e.length).isEqualTo(32);
    assertRoundTrip(v, e);
    final long[] rev = { Long.MAX_VALUE, Long.MIN_VALUE, Long.MAX_VALUE };
    assertRoundTrip(rev, BitPackedCodec.encode(rev, 0, 3));
  }

  @Test
  void craftedEmptyHeaderWithWidthIsHarmless() {
    final byte[] e = BitPackedCodec.encode(new long[0], 0, 0);
    e[1] = 7;
    BitPackedCodec.decode(e, new long[0]);
  }

  @Test
  void embeddedAtANonZeroBase() {
    final long[] v = { 3, 9, 27, 81, -5 };
    final int base = 13;
    final byte[] buf = new byte[base + BitPackedCodec.encodedSize(v, 0, v.length)];
    BitPackedCodec.encodeInto(v, 0, v.length, buf, base);
    assertThat(BitPackedCodec.count(buf, base)).isEqualTo(5);
    final long[] out = new long[7];
    BitPackedCodec.decode(buf, base, out, 2);
    assertThat(Arrays.copyOfRange(out, 2, 7)).isEqualTo(v);
    for (int i = 0; i < v.length; i++)
      assertThat(BitPackedCodec.get(buf, base, i)).isEqualTo(v[i]);
  }

  @Test
  void unsignedGcdBasics() {
    assertThat(BitPackedCodec.unsignedGcd(0, 5)).isEqualTo(5L);
    assertThat(BitPackedCodec.unsignedGcd(5, 0)).isEqualTo(5L);
    assertThat(BitPackedCodec.unsignedGcd(12, 18)).isEqualTo(6L);
    assertThat(BitPackedCodec.unsignedGcd(Long.MIN_VALUE, 0)).isEqualTo(Long.MIN_VALUE);
    assertThat(BitPackedCodec.unsignedGcd(-1L, Long.MIN_VALUE)).isEqualTo(1L);
  }

  @Test
  void decodeRefusesForeignOrTruncatedInput() {
    final byte[] e = BitPackedCodec.encode(new long[] { 1, 2, 3, 400, 5000 }, 0, 5);
    final byte[] foreign = e.clone();
    foreign[0] = 2;
    assertThatThrownBy(() -> BitPackedCodec.decode(foreign, new long[5])).isInstanceOf(IllegalArgumentException.class);
    final byte[] truncated = Arrays.copyOf(e, e.length - 1);
    assertThatThrownBy(() -> BitPackedCodec.decode(truncated, new long[5])).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> BitPackedCodec.decode(e, new long[4])).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void getOutsideTheRangeThrows() {
    final byte[] e = BitPackedCodec.encode(new long[] { 1, 2, 3 }, 0, 3);
    assertThatThrownBy(() -> BitPackedCodec.get(e, -1)).isInstanceOf(IndexOutOfBoundsException.class);
    assertThatThrownBy(() -> BitPackedCodec.get(e, 3)).isInstanceOf(IndexOutOfBoundsException.class);
  }
}
