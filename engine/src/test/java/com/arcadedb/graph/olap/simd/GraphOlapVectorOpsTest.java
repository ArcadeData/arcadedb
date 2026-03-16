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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Random;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Tests both scalar and SIMD implementations produce identical results.
 * Each test runs against both implementations via parameterization.
 */
class GraphOlapVectorOpsTest {

  static Stream<GraphOlapVectorOps> implementations() {
    return Stream.of(new ScalarGraphOlapVectorOps(), new SimdGraphOlapVectorOps());
  }

  // ── Gather tests ───────────────────────────────────────────────────────

  @ParameterizedTest
  @MethodSource("implementations")
  void testGatherInt(final GraphOlapVectorOps ops) {
    final int[] src = { 10, 20, 30, 40, 50, 60, 70, 80 };
    final int[] indices = { 7, 0, 3, 5, 1 };
    final int[] dst = new int[5];

    ops.gatherInt(src, indices, dst, 0, 5);

    assertThat(dst).containsExactly(80, 10, 40, 60, 20);
  }

  @ParameterizedTest
  @MethodSource("implementations")
  void testGatherIntWithOffset(final GraphOlapVectorOps ops) {
    final int[] src = { 100, 200, 300, 400, 500 };
    final int[] indices = { 0, 1, 4, 3, 2 };
    final int[] dst = new int[3];

    ops.gatherInt(src, indices, dst, 2, 3);

    assertThat(dst).containsExactly(500, 400, 300);
  }

  @ParameterizedTest
  @MethodSource("implementations")
  void testGatherLong(final GraphOlapVectorOps ops) {
    final long[] src = { 100L, 200L, 300L, 400L, 500L };
    final int[] indices = { 4, 2, 0 };
    final long[] dst = new long[3];

    ops.gatherLong(src, indices, dst, 0, 3);

    assertThat(dst).containsExactly(500L, 300L, 100L);
  }

  @ParameterizedTest
  @MethodSource("implementations")
  void testGatherDouble(final GraphOlapVectorOps ops) {
    final double[] src = { 1.1, 2.2, 3.3, 4.4, 5.5 };
    final int[] indices = { 3, 1, 4 };
    final double[] dst = new double[3];

    ops.gatherDouble(src, indices, dst, 0, 3);

    assertThat(dst[0]).isCloseTo(4.4, within(1e-10));
    assertThat(dst[1]).isCloseTo(2.2, within(1e-10));
    assertThat(dst[2]).isCloseTo(5.5, within(1e-10));
  }

  @ParameterizedTest
  @MethodSource("implementations")
  void testGatherIntLargeArray(final GraphOlapVectorOps ops) {
    // Test with array larger than SIMD lane width to exercise vectorized + remainder paths
    final int n = 2048;
    final int[] src = new int[n];
    final int[] indices = new int[n];
    final Random rnd = new Random(42);
    for (int i = 0; i < n; i++) {
      src[i] = rnd.nextInt(10000);
      indices[i] = rnd.nextInt(n);
    }

    final int[] dstScalar = new int[n];
    final int[] dstOps = new int[n];
    new ScalarGraphOlapVectorOps().gatherInt(src, indices, dstScalar, 0, n);
    ops.gatherInt(src, indices, dstOps, 0, n);

    assertThat(dstOps).isEqualTo(dstScalar);
  }

  // ── Aggregation tests ──────────────────────────────────────────────────

  @ParameterizedTest
  @MethodSource("implementations")
  void testSumDouble(final GraphOlapVectorOps ops) {
    final double[] data = { 1.0, 2.0, 3.0, 4.0, 5.0 };
    assertThat(ops.sumDouble(data, 0, 5)).isCloseTo(15.0, within(1e-10));
    assertThat(ops.sumDouble(data, 1, 3)).isCloseTo(9.0, within(1e-10));
  }

  @ParameterizedTest
  @MethodSource("implementations")
  void testMinMaxDouble(final GraphOlapVectorOps ops) {
    final double[] data = { 3.0, 1.0, 4.0, 1.5, 9.0, 2.6 };
    assertThat(ops.minDouble(data, 0, 6)).isCloseTo(1.0, within(1e-10));
    assertThat(ops.maxDouble(data, 0, 6)).isCloseTo(9.0, within(1e-10));
  }

  @ParameterizedTest
  @MethodSource("implementations")
  void testSumInt(final GraphOlapVectorOps ops) {
    final int[] data = { 10, 20, 30, 40, 50 };
    assertThat(ops.sumInt(data, 0, 5)).isEqualTo(150);
    assertThat(ops.sumInt(data, 2, 2)).isEqualTo(70);
  }

  @ParameterizedTest
  @MethodSource("implementations")
  void testSumIntOverflow(final GraphOlapVectorOps ops) {
    // Regression: values whose lane-wise sum exceeds Integer.MAX_VALUE
    final int[] data = new int[16];
    Arrays.fill(data, 400_000_000); // 16 × 400M = 6.4B > Integer.MAX_VALUE
    assertThat(ops.sumInt(data, 0, 16)).isEqualTo(16L * 400_000_000L);
  }

  @ParameterizedTest
  @MethodSource("implementations")
  void testMinMaxInt(final GraphOlapVectorOps ops) {
    final int[] data = { 30, 10, 40, 15, 90, 26 };
    assertThat(ops.minInt(data, 0, 6)).isEqualTo(10);
    assertThat(ops.maxInt(data, 0, 6)).isEqualTo(90);
  }

  @ParameterizedTest
  @MethodSource("implementations")
  void testSumLong(final GraphOlapVectorOps ops) {
    final long[] data = { 100L, 200L, 300L };
    assertThat(ops.sumLong(data, 0, 3)).isEqualTo(600L);
  }

  @ParameterizedTest
  @MethodSource("implementations")
  void testMinMaxLong(final GraphOlapVectorOps ops) {
    final long[] data = { 30L, 10L, 40L, 15L, 90L };
    assertThat(ops.minLong(data, 0, 5)).isEqualTo(10L);
    assertThat(ops.maxLong(data, 0, 5)).isEqualTo(90L);
  }

  @ParameterizedTest
  @MethodSource("implementations")
  void testAggregationLargeArray(final GraphOlapVectorOps ops) {
    // Test with array larger than SIMD lanes to exercise vectorized + remainder
    final int n = 1025; // odd size to test remainder handling
    final double[] data = new double[n];
    double expectedSum = 0;
    for (int i = 0; i < n; i++) {
      data[i] = i + 0.5;
      expectedSum += data[i];
    }

    assertThat(ops.sumDouble(data, 0, n)).isCloseTo(expectedSum, within(1e-6));
    assertThat(ops.minDouble(data, 0, n)).isCloseTo(0.5, within(1e-10));
    assertThat(ops.maxDouble(data, 0, n)).isCloseTo(1024.5, within(1e-10));
  }

  // ── Bitmask tests ──────────────────────────────────────────────────────

  @ParameterizedTest
  @MethodSource("implementations")
  void testBitmaskAnd(final GraphOlapVectorOps ops) {
    final long[] a = { 0xFF00FF00FF00FF00L, 0x0F0F0F0F0F0F0F0FL };
    final long[] b = { 0xFFFF0000FFFF0000L, 0x00FF00FF00FF00FFL };
    final long[] out = new long[2];

    ops.bitmaskAnd(a, b, out, 2);

    assertThat(out[0]).isEqualTo(0xFF000000FF000000L);
    assertThat(out[1]).isEqualTo(0x000F000F000F000FL);
  }

  @ParameterizedTest
  @MethodSource("implementations")
  void testBitmaskOr(final GraphOlapVectorOps ops) {
    final long[] a = { 0xFF00L };
    final long[] b = { 0x00FFL };
    final long[] out = new long[1];

    ops.bitmaskOr(a, b, out, 1);

    assertThat(out[0]).isEqualTo(0xFFFFL);
  }

  @ParameterizedTest
  @MethodSource("implementations")
  void testBitmaskNot(final GraphOlapVectorOps ops) {
    final long[] a = { 0L, ~0L };
    final long[] out = new long[2];

    ops.bitmaskNot(a, out, 2);

    assertThat(out[0]).isEqualTo(~0L);
    assertThat(out[1]).isEqualTo(0L);
  }

  @ParameterizedTest
  @MethodSource("implementations")
  void testBitmaskPopcount(final GraphOlapVectorOps ops) {
    final long[] mask = { 0xFFL, 0xFF00L }; // 8 + 8 = 16 set bits
    assertThat(ops.bitmaskPopcount(mask, 2)).isEqualTo(16);
  }

  @ParameterizedTest
  @MethodSource("implementations")
  void testBitmaskOpsLarge(final GraphOlapVectorOps ops) {
    // Larger than SIMD lane width
    final int n = 17; // 17 longs = 1088 bits
    final long[] a = new long[n];
    final long[] b = new long[n];
    final long[] outAnd = new long[n];
    final long[] outOr = new long[n];
    final Random rnd = new Random(123);
    for (int i = 0; i < n; i++) {
      a[i] = rnd.nextLong();
      b[i] = rnd.nextLong();
    }

    ops.bitmaskAnd(a, b, outAnd, n);
    ops.bitmaskOr(a, b, outOr, n);

    for (int i = 0; i < n; i++) {
      assertThat(outAnd[i]).isEqualTo(a[i] & b[i]);
      assertThat(outOr[i]).isEqualTo(a[i] | b[i]);
    }
  }

  // ── Null mask extraction tests ─────────────────────────────────────────

  @ParameterizedTest
  @MethodSource("implementations")
  void testExtractNullMaskSequential(final GraphOlapVectorOps ops) {
    // Simulate a 128-node column where nodes 0, 5, 63, 64, 127 are null
    final long[] nullBitset = new long[2];
    nullBitset[0] = (1L << 0) | (1L << 5) | (1L << 63);
    nullBitset[1] = (1L << 0) | (1L << 63); // node 64, 127

    final boolean[] dst = new boolean[128];
    ops.extractNullMaskSequential(nullBitset, 0, dst, 128);

    assertThat(dst[0]).isTrue();
    assertThat(dst[1]).isFalse();
    assertThat(dst[5]).isTrue();
    assertThat(dst[63]).isTrue();
    assertThat(dst[64]).isTrue();
    assertThat(dst[100]).isFalse();
    assertThat(dst[127]).isTrue();
  }

  @ParameterizedTest
  @MethodSource("implementations")
  void testExtractNullMaskSequentialWithOffset(final GraphOlapVectorOps ops) {
    final long[] nullBitset = new long[2];
    nullBitset[0] = (1L << 60); // node 60 is null
    nullBitset[1] = (1L << 5);  // node 69 is null

    // Extract starting from node 58, length 15 (covers nodes 58-72)
    final boolean[] dst = new boolean[15];
    ops.extractNullMaskSequential(nullBitset, 58, dst, 15);

    assertThat(dst[0]).isFalse(); // node 58
    assertThat(dst[1]).isFalse(); // node 59
    assertThat(dst[2]).isTrue();  // node 60
    assertThat(dst[3]).isFalse(); // node 61
    assertThat(dst[11]).isTrue(); // node 69
  }

  @ParameterizedTest
  @MethodSource("implementations")
  void testExtractNullMaskGather(final GraphOlapVectorOps ops) {
    final long[] nullBitset = new long[2];
    nullBitset[0] = (1L << 3) | (1L << 10);
    nullBitset[1] = (1L << 0); // node 64

    final int[] indices = { 3, 10, 20, 64, 0 };
    final boolean[] dst = new boolean[5];
    ops.extractNullMaskGather(nullBitset, indices, dst, 0, 5);

    assertThat(dst[0]).isTrue();  // node 3
    assertThat(dst[1]).isTrue();  // node 10
    assertThat(dst[2]).isFalse(); // node 20
    assertThat(dst[3]).isTrue();  // node 64
    assertThat(dst[4]).isFalse(); // node 0
  }

  // ── Parity tests (scalar vs SIMD) ─────────────────────────────────────

  @Test
  void testScalarSimdParityGather() {
    final ScalarGraphOlapVectorOps scalar = new ScalarGraphOlapVectorOps();
    final SimdGraphOlapVectorOps simd = new SimdGraphOlapVectorOps();
    final Random rnd = new Random(99);

    final int srcLen = 4096;
    final int gatherLen = 2048;
    final int[] srcInt = new int[srcLen];
    final long[] srcLong = new long[srcLen];
    final double[] srcDouble = new double[srcLen];
    final int[] indices = new int[gatherLen];

    for (int i = 0; i < srcLen; i++) {
      srcInt[i] = rnd.nextInt();
      srcLong[i] = rnd.nextLong();
      srcDouble[i] = rnd.nextDouble();
    }
    for (int i = 0; i < gatherLen; i++)
      indices[i] = rnd.nextInt(srcLen);

    final int[] dstIntScalar = new int[gatherLen];
    final int[] dstIntSimd = new int[gatherLen];
    scalar.gatherInt(srcInt, indices, dstIntScalar, 0, gatherLen);
    simd.gatherInt(srcInt, indices, dstIntSimd, 0, gatherLen);
    assertThat(dstIntSimd).isEqualTo(dstIntScalar);

    final long[] dstLongScalar = new long[gatherLen];
    final long[] dstLongSimd = new long[gatherLen];
    scalar.gatherLong(srcLong, indices, dstLongScalar, 0, gatherLen);
    simd.gatherLong(srcLong, indices, dstLongSimd, 0, gatherLen);
    assertThat(dstLongSimd).isEqualTo(dstLongScalar);

    final double[] dstDoubleScalar = new double[gatherLen];
    final double[] dstDoubleSimd = new double[gatherLen];
    scalar.gatherDouble(srcDouble, indices, dstDoubleScalar, 0, gatherLen);
    simd.gatherDouble(srcDouble, indices, dstDoubleSimd, 0, gatherLen);
    assertThat(dstDoubleSimd).isEqualTo(dstDoubleScalar);
  }

  @Test
  void testScalarSimdParityAggregation() {
    final ScalarGraphOlapVectorOps scalar = new ScalarGraphOlapVectorOps();
    final SimdGraphOlapVectorOps simd = new SimdGraphOlapVectorOps();
    final Random rnd = new Random(77);

    final int n = 1025; // odd size
    final double[] doubleData = new double[n];
    final int[] intData = new int[n];
    final long[] longData = new long[n];
    for (int i = 0; i < n; i++) {
      doubleData[i] = rnd.nextDouble() * 1000;
      intData[i] = rnd.nextInt(100000);
      longData[i] = rnd.nextLong();
    }

    assertThat(simd.sumDouble(doubleData, 0, n)).isCloseTo(scalar.sumDouble(doubleData, 0, n), within(1e-6));
    assertThat(simd.minDouble(doubleData, 0, n)).isCloseTo(scalar.minDouble(doubleData, 0, n), within(1e-10));
    assertThat(simd.maxDouble(doubleData, 0, n)).isCloseTo(scalar.maxDouble(doubleData, 0, n), within(1e-10));

    assertThat(simd.sumInt(intData, 0, n)).isEqualTo(scalar.sumInt(intData, 0, n));
    assertThat(simd.minInt(intData, 0, n)).isEqualTo(scalar.minInt(intData, 0, n));
    assertThat(simd.maxInt(intData, 0, n)).isEqualTo(scalar.maxInt(intData, 0, n));

    assertThat(simd.sumLong(longData, 0, n)).isEqualTo(scalar.sumLong(longData, 0, n));
    assertThat(simd.minLong(longData, 0, n)).isEqualTo(scalar.minLong(longData, 0, n));
    assertThat(simd.maxLong(longData, 0, n)).isEqualTo(scalar.maxLong(longData, 0, n));
  }

  @Test
  void testProviderReturnsInstance() {
    final GraphOlapVectorOps ops = GraphOlapVectorOpsProvider.getInstance();
    assertThat(ops).isNotNull();

    // Basic functionality check
    final int[] src = { 5, 10, 15 };
    final int[] idx = { 2, 0 };
    final int[] dst = new int[2];
    ops.gatherInt(src, idx, dst, 0, 2);
    assertThat(dst).containsExactly(15, 5);
  }
}
