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
package com.arcadedb.database;

import org.junit.jupiter.api.Test;

import java.util.SplittableRandom;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for the varint encode/decode fast paths in {@link Binary}. Focuses on boundary values where one path transitions to the next (127 vs 128,
 * 16383 vs 16384, ...) and on the length-calculation shortcut.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class BinaryVarIntTest {

  @Test
  void roundTripCoversEveryByteLength() {
    // Representative values for each expected byte length (1..10) including the 1<->2 and 2<->3 boundaries the fast path switches on.
    final long[] values = {
        0L, 1L, 126L, 127L,                             // 1-byte: 0..2^7-1
        128L, 129L, 8000L, 16382L, 16383L,              // 2-byte: 2^7..2^14-1
        16384L, 16385L, (1L << 21) - 1,                 // 3-byte
        1L << 21, (1L << 28) - 1,                       // 4-byte
        1L << 28, (1L << 35) - 1,                       // 5-byte
        1L << 35, (1L << 42) - 1,                       // 6-byte
        1L << 42, (1L << 49) - 1,                       // 7-byte
        1L << 49, (1L << 56) - 1,                       // 8-byte
        1L << 56, (1L << 63) - 1,                       // 9-byte (63-bit unsigned)
        1L << 63                                        // 10-byte (64th bit set, signed-long negative)
    };

    final Binary buf = new Binary(values.length * 10);
    for (final long v : values) {
      final int wrote = buf.putUnsignedNumber(v);
      assertThat(wrote).isEqualTo(Binary.getUnsignedNumberSpace(v));
    }

    buf.position(0);
    for (final long expected : values)
      assertThat(buf.getUnsignedNumber()).isEqualTo(expected);
  }

  @Test
  void getUnsignedNumberSpaceMatchesOldLoopFormula() {
    // Cross-check the O(1) numberOfLeadingZeros formula against the previous iterative definition. Uses fixed-seed PRNG so the test is deterministic.
    final SplittableRandom rnd = new SplittableRandom(1234);
    for (int i = 0; i < 10_000; i++) {
      final long v = rnd.nextLong();
      assertThat(Binary.getUnsignedNumberSpace(v)).isEqualTo(referenceLoopLength(v));
    }
    // Edge cases
    assertThat(Binary.getUnsignedNumberSpace(0L)).isEqualTo(1);
    assertThat(Binary.getUnsignedNumberSpace(-1L)).isEqualTo(10);
    assertThat(Binary.getUnsignedNumberSpace(Long.MAX_VALUE)).isEqualTo(9);
    assertThat(Binary.getUnsignedNumberSpace(Long.MIN_VALUE)).isEqualTo(10);
  }

  @Test
  void signedZigZagRoundTrip() {
    final long[] values = { 0, 1, -1, 127, -127, 128, -128, 16383, -16383, Long.MAX_VALUE, Long.MIN_VALUE };
    final Binary buf = new Binary(values.length * 10);
    for (final long v : values)
      buf.putNumber(v);
    buf.position(0);
    for (final long expected : values)
      assertThat(buf.getNumber()).isEqualTo(expected);
  }

  @Test
  void numberAndSizeReportsExactBytesConsumed() {
    final Binary buf = new Binary(64);
    buf.putUnsignedNumber(5L);       // 1 byte
    buf.putUnsignedNumber(200L);     // 2 bytes
    buf.putUnsignedNumber(1L << 35); // 6 bytes

    buf.position(0);
    final long[] a = buf.getUnsignedNumberAndSize();
    assertThat(a[0]).isEqualTo(5L);
    assertThat(a[1]).isEqualTo(1L);
    final long[] b = buf.getUnsignedNumberAndSize();
    assertThat(b[0]).isEqualTo(200L);
    assertThat(b[1]).isEqualTo(2L);
    final long[] c = buf.getUnsignedNumberAndSize();
    assertThat(c[0]).isEqualTo(1L << 35);
    assertThat(c[1]).isEqualTo(6L);
  }

  private static int referenceLoopLength(final long value) {
    int bytes = 0;
    long v = value;
    while ((v & 0xFFFFFFFFFFFFFF80L) != 0L) {
      bytes++;
      v >>>= 7;
    }
    return bytes + 1;
  }
}
