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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BlockwiseLinearCodecTest {
  private static final VarHandle INT_LE = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);

  private static long[] random(final Random r, final int n) {
    final long[] v = new long[n];
    for (int i = 0; i < n; i++)
      v[i] = r.nextLong();
    return v;
  }

  private static void assertRoundTrip(final long[] values, final byte[] encoded) {
    assertThat(BlockwiseLinearCodec.count(encoded)).isEqualTo(values.length);
    final long[] out = new long[values.length];
    BlockwiseLinearCodec.decode(encoded, out);
    assertThat(out).isEqualTo(values);
    for (int i = 0; i < values.length; i++)
      assertThat(BlockwiseLinearCodec.get(encoded, i)).isEqualTo(values[i]);
  }

  @Test
  void emptyIsHeaderOnly() {
    final byte[] e = BlockwiseLinearCodec.encode(new long[0], 0, 0);
    assertThat(e.length).isEqualTo(8);
    assertThat(e[0]).isEqualTo(BlockwiseLinearCodec.CODEC_ID);
    assertThat(BlockwiseLinearCodec.count(e)).isZero();
    BlockwiseLinearCodec.decode(e, new long[0]);
  }

  @Test
  void lessThanOneBlockRoundTrips() {
    final long[] v = random(new Random(1), 100);
    assertRoundTrip(v, BlockwiseLinearCodec.encode(v, 0, v.length));
  }

  @Test
  void blockBoundariesRoundTrip() {
    final Random r = new Random(2);
    for (final int n : new int[] { 1, 511, 512, 513, 1024, 1025 }) {
      final long[] v = new long[n];
      for (int i = 0; i < n; i++)
        v[i] = 1000L * i + r.nextInt(50);
      final byte[] e = BlockwiseLinearCodec.encode(v, 0, n);
      assertRoundTrip(v, e);
      assertThat(BlockwiseLinearCodec.get(e, n - 1)).isEqualTo(v[n - 1]);
      if (n > 512) {
        assertThat(BlockwiseLinearCodec.get(e, 511)).isEqualTo(v[511]);
        assertThat(BlockwiseLinearCodec.get(e, 512)).isEqualTo(v[512]);
      }
    }
  }

  @Test
  void piecewiseLinearCompressesToHeaders() {
    final long[] v = new long[4 * 512];
    for (int b = 0; b < 4; b++)
      for (int i = 0; i < 512; i++)
        v[b * 512 + i] = b * 1_000_000_000_000L + i * (b + 1) * 7L;
    final byte[] e = BlockwiseLinearCodec.encode(v, 0, v.length);
    assertThat(e.length).isEqualTo(24 + 4 * 48);
    assertRoundTrip(v, e);
  }

  @Test
  void extremesRoundTrip() {
    final long[] v = new long[1500];
    for (int i = 0; i < v.length; i++)
      v[i] = (i & 1) == 0 ? Long.MIN_VALUE : Long.MAX_VALUE;
    assertRoundTrip(v, BlockwiseLinearCodec.encode(v, 0, v.length));
    final long[] mixed = random(new Random(3), 1030);
    mixed[0] = Long.MAX_VALUE;
    mixed[511] = Long.MIN_VALUE;
    mixed[512] = Long.MIN_VALUE;
    mixed[1029] = Long.MAX_VALUE;
    assertRoundTrip(mixed, BlockwiseLinearCodec.encode(mixed, 0, mixed.length));
  }

  @Test
  void subrangeEncodesOnlyTheSlice() {
    final long[] v = random(new Random(4), 1500);
    final byte[] e = BlockwiseLinearCodec.encode(v, 300, 900);
    assertThat(BlockwiseLinearCodec.count(e)).isEqualTo(900);
    assertThat(BlockwiseLinearCodec.get(e, 0)).isEqualTo(v[300]);
    assertThat(BlockwiseLinearCodec.get(e, 899)).isEqualTo(v[1199]);
    assertRoundTrip(Arrays.copyOfRange(v, 300, 1200), e);
  }

  @Test
  void sliceWithGuardValuesNeverReadsOutside() {
    final Random r = new Random(5);
    for (final int n : new int[] { 0, 1, 511, 512, 513, 1024, 1025 }) {
      final long[] inner = random(r, n);
      final long[] padded = new long[n + 2];
      padded[0] = Long.MIN_VALUE;
      padded[n + 1] = Long.MAX_VALUE;
      System.arraycopy(inner, 0, padded, 1, n);
      final byte[] e = BlockwiseLinearCodec.encode(padded, 1, n);
      assertThat(e).isEqualTo(BlockwiseLinearCodec.encode(inner, 0, n));
      assertThat(e.length).isEqualTo(BlockwiseLinearCodec.encodedSize(padded, 1, n));
      assertThat(e.length % 8).isZero();
      assertRoundTrip(inner, e);
    }
  }

  @Test
  void encodedSizeMatchesEncode() {
    final Random r = new Random(6);
    for (int t = 0; t < 50; t++) {
      final int n = r.nextInt(3001);
      final long[] v = new long[n];
      for (int i = 0; i < n; i++)
        v[i] = (t % 2 == 0) ? r.nextLong() : 10L * i + r.nextInt(1 << (t % 20));
      assertThat(BlockwiseLinearCodec.encode(v, 0, n).length).isEqualTo(BlockwiseLinearCodec.encodedSize(v, 0, n));
    }
  }

  @Test
  void decodeRefusesForeignInput() {
    final byte[] e = BlockwiseLinearCodec.encode(new long[] { 1, 2, 3 }, 0, 3);
    e[0] = 2;
    assertThatThrownBy(() -> BlockwiseLinearCodec.decode(e, new long[3])).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> BlockwiseLinearCodec.count(e)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void malformedInputIsRefused() {
    final long[] v = random(new Random(7), 1100);
    final byte[] good = BlockwiseLinearCodec.encode(v, 0, v.length);

    final byte[] crafted = good.clone();
    crafted[4] = (byte) 0xFF;
    crafted[5] = (byte) 0xFF;
    crafted[6] = (byte) 0xFF;
    crafted[7] = 0x7F;
    assertThatThrownBy(() -> BlockwiseLinearCodec.count(crafted)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> BlockwiseLinearCodec.decode(crafted, new long[1100])).isInstanceOf(IllegalArgumentException.class);

    final byte[] negative = good.clone();
    negative[7] = (byte) 0x80;
    assertThatThrownBy(() -> BlockwiseLinearCodec.count(negative)).isInstanceOf(IllegalArgumentException.class);

    final byte[] truncated = Arrays.copyOf(good, 12);
    assertThatThrownBy(() -> BlockwiseLinearCodec.count(truncated)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> BlockwiseLinearCodec.decode(truncated, new long[1100])).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> BlockwiseLinearCodec.count(new byte[4])).isInstanceOf(IllegalArgumentException.class);

    final byte[] badOffset = good.clone();
    badOffset[8] = (byte) 0xFF;
    badOffset[9] = (byte) 0xFF;
    badOffset[10] = (byte) 0xFF;
    badOffset[11] = 0x3F;
    assertThatThrownBy(() -> BlockwiseLinearCodec.decode(badOffset, new long[1100])).isInstanceOf(IllegalArgumentException.class);

    for (final int delta : new int[] { 1, -8, -16 }) {
      final byte[] shifted = good.clone();
      final int block1 = (int) INT_LE.get(shifted, 12) + delta;
      INT_LE.set(shifted, 12, block1);
      assertThatThrownBy(() -> BlockwiseLinearCodec.decode(shifted, new long[1100])).isInstanceOf(IllegalArgumentException.class);
    }
    final byte[] belowTable = good.clone();
    INT_LE.set(belowTable, 8, 8);
    assertThatThrownBy(() -> BlockwiseLinearCodec.decode(belowTable, new long[1100])).isInstanceOf(IllegalArgumentException.class);
    final byte[] duplicate = good.clone();
    INT_LE.set(duplicate, 12, (int) INT_LE.get(duplicate, 8));
    assertThatThrownBy(() -> BlockwiseLinearCodec.decode(duplicate, new long[1100])).isInstanceOf(IllegalArgumentException.class);
    final byte[] backwards = good.clone();
    INT_LE.set(backwards, 16, (int) INT_LE.get(backwards, 8));
    assertThatThrownBy(() -> BlockwiseLinearCodec.decode(backwards, new long[1100])).isInstanceOf(IllegalArgumentException.class);

    assertThatThrownBy(() -> BlockwiseLinearCodec.decode(good, new long[10])).isInstanceOf(IllegalArgumentException.class);
  }
}
