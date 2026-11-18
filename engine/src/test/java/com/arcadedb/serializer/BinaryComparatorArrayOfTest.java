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
package com.arcadedb.serializer;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Direct tests of the ARRAY_OF_* and LIST/ARRAY_OF_* arms of {@link BinaryComparator#compare(Object, byte, Object, byte)}
 * (issue #11596): the order is element by element, a prefix first, and antisymmetric in both directions.
 */
class BinaryComparatorArrayOfTest {
  private final BinaryComparator comparator = new BinaryComparator();

  private void assertOrder(final Object smaller, final byte smallerType, final Object larger, final byte largerType) {
    assertThat(comparator.compare(smaller, smallerType, larger, largerType)).isLessThan(0);
    assertThat(comparator.compare(larger, largerType, smaller, smallerType)).isGreaterThan(0);
  }

  private void assertEqual(final Object a, final byte aType, final Object b, final byte bType) {
    assertThat(comparator.compare(a, aType, b, bType)).isZero();
    assertThat(comparator.compare(b, bType, a, aType)).isZero();
  }

  @Test
  void sameTypeArraysKeepPrefixFirst() {
    assertOrder(new int[] { 1, 2 }, BinaryTypes.TYPE_ARRAY_OF_INTEGERS, new int[] { 1, 2, 0 }, BinaryTypes.TYPE_ARRAY_OF_INTEGERS);
    assertOrder(new int[] { 1, 2 }, BinaryTypes.TYPE_ARRAY_OF_INTEGERS, new int[] { 3 }, BinaryTypes.TYPE_ARRAY_OF_INTEGERS);
    assertOrder(new long[] { 1L, 2L }, BinaryTypes.TYPE_ARRAY_OF_LONGS, new long[] { 1L, 2L, 0L }, BinaryTypes.TYPE_ARRAY_OF_LONGS);
    assertOrder(new short[] { 1, 2 }, BinaryTypes.TYPE_ARRAY_OF_SHORTS, new short[] { 3 }, BinaryTypes.TYPE_ARRAY_OF_SHORTS);
    assertOrder(new float[] { 0.1f }, BinaryTypes.TYPE_ARRAY_OF_FLOATS, new float[] { 0.1f, 0f }, BinaryTypes.TYPE_ARRAY_OF_FLOATS);
    assertOrder(new double[] { 1.5 }, BinaryTypes.TYPE_ARRAY_OF_DOUBLES, new double[] { 2.5 }, BinaryTypes.TYPE_ARRAY_OF_DOUBLES);
    assertEqual(new int[] { 1, 2 }, BinaryTypes.TYPE_ARRAY_OF_INTEGERS, new int[] { 1, 2 }, BinaryTypes.TYPE_ARRAY_OF_INTEGERS);
  }

  @Test
  void differentArrayTypesAreAntisymmetric() {
    assertOrder(new int[] { 1, 2 }, BinaryTypes.TYPE_ARRAY_OF_INTEGERS, new double[] { 1, 2, 0 }, BinaryTypes.TYPE_ARRAY_OF_DOUBLES);
    assertOrder(new int[] { 1, 2 }, BinaryTypes.TYPE_ARRAY_OF_INTEGERS, new double[] { 3 }, BinaryTypes.TYPE_ARRAY_OF_DOUBLES);
    assertEqual(new int[] { 1, 2 }, BinaryTypes.TYPE_ARRAY_OF_INTEGERS, new double[] { 1, 2 }, BinaryTypes.TYPE_ARRAY_OF_DOUBLES);
  }

  @Test
  void listAgainstArrayIsAntisymmetric() {
    assertOrder(List.of(1, 2), BinaryTypes.TYPE_LIST, new int[] { 1, 2, 0 }, BinaryTypes.TYPE_ARRAY_OF_INTEGERS);
    assertOrder(List.of(1, 2), BinaryTypes.TYPE_LIST, new int[] { 3 }, BinaryTypes.TYPE_ARRAY_OF_INTEGERS);
    assertOrder(new int[] { 1, 2 }, BinaryTypes.TYPE_ARRAY_OF_INTEGERS, List.of(1, 2, 0), BinaryTypes.TYPE_LIST);
    assertOrder(new int[] { 1, 2 }, BinaryTypes.TYPE_ARRAY_OF_INTEGERS, List.of(3), BinaryTypes.TYPE_LIST);
    assertEqual(List.of(1, 2), BinaryTypes.TYPE_LIST, new int[] { 1, 2 }, BinaryTypes.TYPE_ARRAY_OF_INTEGERS);
  }
}
