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
package com.arcadedb.engine.timeseries.simd;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Random;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Tests both scalar and SIMD implementations produce identical results.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class TimeSeriesVectorOpsTest {

  static Stream<TimeSeriesVectorOps> implementations() {
    return Stream.of(new ScalarTimeSeriesVectorOps(), new SimdTimeSeriesVectorOps());
  }

  @ParameterizedTest
  @MethodSource("implementations")
  void testSumDouble(final TimeSeriesVectorOps ops) {
    final double[] data = { 1.0, 2.0, 3.0, 4.0, 5.0 };
    assertThat(ops.sum(data, 0, 5)).isCloseTo(15.0, within(1e-10));
    assertThat(ops.sum(data, 1, 3)).isCloseTo(9.0, within(1e-10));
  }

  @ParameterizedTest
  @MethodSource("implementations")
  void testMinMaxDouble(final TimeSeriesVectorOps ops) {
    final double[] data = { 5.0, 1.0, 3.0, -2.0, 4.0, 0.0, 7.0 };
    assertThat(ops.min(data, 0, 7)).isEqualTo(-2.0);
    assertThat(ops.max(data, 0, 7)).isEqualTo(7.0);
    assertThat(ops.min(data, 2, 3)).isEqualTo(-2.0);
    assertThat(ops.max(data, 2, 3)).isEqualTo(4.0);
  }

  @ParameterizedTest
  @MethodSource("implementations")
  void testSumLong(final TimeSeriesVectorOps ops) {
    final long[] data = { 10, 20, 30, 40, 50 };
    assertThat(ops.sumLong(data, 0, 5)).isEqualTo(150);
    assertThat(ops.sumLong(data, 2, 2)).isEqualTo(70);
  }

  @ParameterizedTest
  @MethodSource("implementations")
  void testMinMaxLong(final TimeSeriesVectorOps ops) {
    final long[] data = { 50, 10, 30, -20, 40, 0, 70 };
    assertThat(ops.minLong(data, 0, 7)).isEqualTo(-20);
    assertThat(ops.maxLong(data, 0, 7)).isEqualTo(70);
  }

  @ParameterizedTest
  @MethodSource("implementations")
  void testSingleElement(final TimeSeriesVectorOps ops) {
    final double[] data = { 42.0 };
    assertThat(ops.sum(data, 0, 1)).isEqualTo(42.0);
    assertThat(ops.min(data, 0, 1)).isEqualTo(42.0);
    assertThat(ops.max(data, 0, 1)).isEqualTo(42.0);
  }

  @ParameterizedTest
  @MethodSource("implementations")
  void testNonAlignedLength(final TimeSeriesVectorOps ops) {
    // Length not a multiple of SIMD lane width
    final double[] data = new double[17];
    for (int i = 0; i < data.length; i++)
      data[i] = i + 1;

    assertThat(ops.sum(data, 0, 17)).isCloseTo(153.0, within(1e-10));
    assertThat(ops.min(data, 0, 17)).isEqualTo(1.0);
    assertThat(ops.max(data, 0, 17)).isEqualTo(17.0);
  }

  @ParameterizedTest
  @MethodSource("implementations")
  void testFilteredSum(final TimeSeriesVectorOps ops) {
    final double[] data = { 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0 };
    // Bitmask: bits 0,2,4,6 set → select 1.0, 3.0, 5.0, 7.0
    final long[] bitmask = { 0b01010101L };
    assertThat(ops.sumFiltered(data, bitmask, 0, 8)).isCloseTo(16.0, within(1e-10));
  }

  @ParameterizedTest
  @MethodSource("implementations")
  void testCountFiltered(final TimeSeriesVectorOps ops) {
    final long[] bitmask = { 0b01010101L };
    assertThat(ops.countFiltered(bitmask, 0, 8)).isEqualTo(4);
  }

  @ParameterizedTest
  @MethodSource("implementations")
  void testGreaterThan(final TimeSeriesVectorOps ops) {
    final double[] data = { 1.0, 5.0, 2.0, 8.0, 3.0, 6.0, 0.5, 4.0 };
    final long[] out = new long[1];
    ops.greaterThan(data, 3.0, out, 0, 8);

    // Elements > 3.0 at indices 1,3,5,7 → bits 1,3,5,7
    assertThat(out[0] & (1L << 1)).isNotZero();
    assertThat(out[0] & (1L << 3)).isNotZero();
    assertThat(out[0] & (1L << 5)).isNotZero();
    assertThat(out[0] & (1L << 7)).isNotZero();
    assertThat(out[0] & (1L << 0)).isZero();
    assertThat(out[0] & (1L << 2)).isZero();
  }

  @ParameterizedTest
  @MethodSource("implementations")
  void testBitmaskAndOr(final TimeSeriesVectorOps ops) {
    final long[] a = { 0b1100L };
    final long[] b = { 0b1010L };
    final long[] andOut = new long[1];
    final long[] orOut = new long[1];

    ops.bitmaskAnd(a, b, andOut, 1);
    ops.bitmaskOr(a, b, orOut, 1);

    assertThat(andOut[0]).isEqualTo(0b1000L);
    assertThat(orOut[0]).isEqualTo(0b1110L);
  }

  @Test
  void testScalarAndSimdProduceIdenticalResults() {
    final ScalarTimeSeriesVectorOps scalar = new ScalarTimeSeriesVectorOps();
    final SimdTimeSeriesVectorOps simd = new SimdTimeSeriesVectorOps();

    final Random rng = new Random(42);
    final int size = 1000;
    final double[] dblData = new double[size];
    final long[] longData = new long[size];
    for (int i = 0; i < size; i++) {
      dblData[i] = rng.nextDouble() * 1000 - 500;
      longData[i] = rng.nextLong();
    }

    // Basic aggregations
    assertThat(simd.sum(dblData, 0, size)).isCloseTo(scalar.sum(dblData, 0, size), within(1e-6));
    assertThat(simd.min(dblData, 0, size)).isEqualTo(scalar.min(dblData, 0, size));
    assertThat(simd.max(dblData, 0, size)).isEqualTo(scalar.max(dblData, 0, size));
    assertThat(simd.sumLong(longData, 0, size)).isEqualTo(scalar.sumLong(longData, 0, size));
    assertThat(simd.minLong(longData, 0, size)).isEqualTo(scalar.minLong(longData, 0, size));
    assertThat(simd.maxLong(longData, 0, size)).isEqualTo(scalar.maxLong(longData, 0, size));

    // With offset
    assertThat(simd.sum(dblData, 100, 500)).isCloseTo(scalar.sum(dblData, 100, 500), within(1e-6));
    assertThat(simd.min(dblData, 100, 500)).isEqualTo(scalar.min(dblData, 100, 500));
    assertThat(simd.max(dblData, 100, 500)).isEqualTo(scalar.max(dblData, 100, 500));
    assertThat(simd.sumLong(longData, 100, 500)).isEqualTo(scalar.sumLong(longData, 100, 500));
    assertThat(simd.minLong(longData, 100, 500)).isEqualTo(scalar.minLong(longData, 100, 500));
    assertThat(simd.maxLong(longData, 100, 500)).isEqualTo(scalar.maxLong(longData, 100, 500));

    // greaterThan parity
    final int bitmaskWords = (size + 63) / 64;
    final long[] scalarGt = new long[bitmaskWords];
    final long[] simdGt = new long[bitmaskWords];
    scalar.greaterThan(dblData, 0.0, scalarGt, 0, size);
    simd.greaterThan(dblData, 0.0, simdGt, 0, size);
    assertThat(simdGt).isEqualTo(scalarGt);

    // sumFiltered parity (using the greaterThan output as bitmask)
    assertThat(simd.sumFiltered(dblData, scalarGt, 0, size))
        .isCloseTo(scalar.sumFiltered(dblData, scalarGt, 0, size), within(1e-6));

    // countFiltered parity
    assertThat(simd.countFiltered(scalarGt, 0, size)).isEqualTo(scalar.countFiltered(scalarGt, 0, size));

    // bitmaskAnd / bitmaskOr parity
    final long[] secondMask = new long[bitmaskWords];
    scalar.greaterThan(dblData, -100.0, secondMask, 0, size);

    final long[] scalarAnd = new long[bitmaskWords];
    final long[] simdAnd = new long[bitmaskWords];
    scalar.bitmaskAnd(scalarGt, secondMask, scalarAnd, bitmaskWords);
    simd.bitmaskAnd(scalarGt, secondMask, simdAnd, bitmaskWords);
    assertThat(simdAnd).isEqualTo(scalarAnd);

    final long[] scalarOr = new long[bitmaskWords];
    final long[] simdOr = new long[bitmaskWords];
    scalar.bitmaskOr(scalarGt, secondMask, scalarOr, bitmaskWords);
    simd.bitmaskOr(scalarGt, secondMask, simdOr, bitmaskWords);
    assertThat(simdOr).isEqualTo(scalarOr);
  }

  @Test
  void testProviderReturnsInstance() {
    final TimeSeriesVectorOps ops = TimeSeriesVectorOpsProvider.getInstance();
    assertThat(ops).isNotNull();
    // Smoke test
    assertThat(ops.sum(new double[] { 1.0, 2.0, 3.0 }, 0, 3)).isCloseTo(6.0, within(1e-10));
  }
}
