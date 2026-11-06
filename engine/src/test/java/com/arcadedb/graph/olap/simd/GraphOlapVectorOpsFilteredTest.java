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
package com.arcadedb.graph.olap.simd;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Random;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the filtered bitmask kernels (and-not, and-popcount) against a plain loop on both implementations.
 */
class GraphOlapVectorOpsFilteredTest {

  static Stream<GraphOlapVectorOps> implementations() {
    return Stream.of(new ScalarGraphOlapVectorOps(), new SimdGraphOlapVectorOps());
  }

  @ParameterizedTest
  @MethodSource("implementations")
  void andNotAndAndPopcountAgreeWithScalar(final GraphOlapVectorOps ops) {
    final Random random = new Random(3);
    for (int length = 0; length <= 1000; length++) {
      final long[] a = new long[length + 1];
      final long[] b = new long[length + 1];
      for (int i = 0; i < length; i++) {
        a[i] = random.nextLong();
        b[i] = random.nextLong();
      }
      // guard word beyond length must stay untouched
      a[length] = 0x5A5A5A5AL;
      final long[] out = new long[length + 1];
      out[length] = 0xC0FFEEL;

      ops.bitmaskAndNot(a, b, out, length);

      int expectedPopcount = 0;
      for (int i = 0; i < length; i++) {
        assertThat(out[i]).isEqualTo(a[i] & ~b[i]);
        expectedPopcount += Long.bitCount(a[i] & b[i]);
      }
      assertThat(out[length]).isEqualTo(0xC0FFEEL);
      assertThat(ops.bitmaskAndPopcount(a, b, length)).isEqualTo(expectedPopcount);
    }
  }

  @ParameterizedTest
  @MethodSource("implementations")
  void andNotCanWriteInPlace(final GraphOlapVectorOps ops) {
    final long[] a = { -1L, -1L, -1L, -1L, -1L, 7L };
    final long[] b = { 1L, 2L, 4L, 8L, 16L, 5L };
    ops.bitmaskAndNot(a, b, a, 6);
    assertThat(a).containsExactly(~1L, ~2L, ~4L, ~8L, ~16L, 2L);
  }
}
