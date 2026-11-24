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
package com.arcadedb.query.sql.executor;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static com.arcadedb.query.sql.executor.HeapEstimator.REFERENCE_BYTES;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The heap estimate of a value that holds its data several levels down: a list, map or array held by a list held by a list held
 * by a row is charged for what it holds, as the same value held by the row directly is, and not as one object.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class HeapEstimatorNestedValuesTest {
  private static final int  ELEMENTS    = 10_000;
  private static final long NUMBER_SLOT = REFERENCE_BYTES + 24L;

  /** The value held by {@code levels} lists of one element each. */
  private static Object wrap(final Object value, final int levels) {
    Object wrapped = value;
    for (int i = 0; i < levels; i++) {
      final List<Object> list = new ArrayList<>(1);
      list.add(wrapped);
      wrapped = list;
    }
    return wrapped;
  }

  private static ResultInternal row(final Object value) {
    final ResultInternal row = new ResultInternal();
    row.setProperty("n", value);
    return row;
  }

  private static List<Object> numbers(final int size) {
    final List<Object> numbers = new ArrayList<>(size);
    for (long i = 0; i < size; i++)
      numbers.add(i * 1_000_003L);
    return numbers;
  }

  @ParameterizedTest
  @ValueSource(ints = { 0, 1, 2, 3, 4, 5, 8, 16, 31, 32, 33, 40, 64, 500, 5_000 })
  void aListOfNumbersIsChargedForItsElementsAtEveryLevel(final int levels) {
    final long bytes = HeapEstimator.estimate(row(wrap(numbers(ELEMENTS), levels)));

    assertThat(bytes).as("a list of %d numbers held %d levels down", ELEMENTS, levels).isGreaterThanOrEqualTo(ELEMENTS * NUMBER_SLOT);
  }

  static Stream<Arguments> arraysOfOneMegabyte() {
    final List<Arguments> arguments = new ArrayList<>();
    for (final int levels : new int[] { 0, 2, 3, 4, 7, 31, 32, 33, 100 }) {
      arguments.add(Arguments.of("byte[]", levels, new byte[1_000_000]));
      arguments.add(Arguments.of("float[]", levels, new float[250_000]));
      arguments.add(Arguments.of("int[]", levels, new int[250_000]));
      arguments.add(Arguments.of("long[]", levels, new long[125_000]));
      arguments.add(Arguments.of("double[]", levels, new double[125_000]));
    }
    return arguments.stream();
  }

  @ParameterizedTest(name = "{0} held {1} levels down")
  @MethodSource("arraysOfOneMegabyte")
  void aPrimitiveArrayIsChargedForItsSizeAtEveryLevel(final String kind, final int levels, final Object array) {
    assertThat(HeapEstimator.estimate(row(wrap(array, levels)))).isGreaterThanOrEqualTo(1_000_000L);
  }

  static Stream<Arguments> containersOfTenThousandNumbers() {
    final List<Arguments> arguments = new ArrayList<>();
    for (final int levels : new int[] { 0, 2, 3, 4, 7, 31, 32, 33, 100 }) {
      final Object[] array = numbers(ELEMENTS).toArray();
      arguments.add(Arguments.of("Object[]", levels, array, REFERENCE_BYTES + 24L));

      final Set<Object> set = new HashSet<>(numbers(ELEMENTS));
      arguments.add(Arguments.of("Set", levels, set, HeapEstimator.HASH_ENTRY_BYTES + 24L));

      final Map<Object, Object> map = new LinkedHashMap<>();
      for (final Object number : numbers(ELEMENTS))
        map.put("k" + number, number);
      arguments.add(Arguments.of("Map", levels, map, (long) HeapEstimator.HASH_ENTRY_BYTES + 24L));
    }
    return arguments.stream();
  }

  @ParameterizedTest(name = "{0} held {1} levels down")
  @MethodSource("containersOfTenThousandNumbers")
  void anArrayASetAndAMapAreChargedForTheirSlotsAtEveryLevel(final String kind, final int levels, final Object container,
      final long slotBytes) {
    assertThat(HeapEstimator.estimate(row(wrap(container, levels)))).isGreaterThanOrEqualTo(ELEMENTS * slotBytes);
  }

  @Test
  void aValueIsChargedTheSameWhetherTheRowHoldsItDirectlyOrNestedInLists() {
    final List<Object> numbers = numbers(ELEMENTS);
    final long direct = HeapEstimator.estimate(row(numbers));

    assertThat(HeapEstimator.estimate(row(wrap(numbers, 3)))).isGreaterThanOrEqualTo(direct);
    assertThat(HeapEstimator.estimate(row(wrap(numbers, 12)))).isGreaterThanOrEqualTo(direct);
  }

  @Test
  void aValueNestedBeyondAnyLimitTerminatesAndIsChargedForTheLevelsItHas() {
    // Far more levels than a walk follows, built without recursion
    final int levels = 200_000;
    final long bytes = HeapEstimator.estimate(row(wrap(numbers(ELEMENTS), levels)));

    assertThat(bytes).isPositive();
    assertThat(bytes).as("every level that is followed is a list of one slot").isGreaterThan(1_000L * REFERENCE_BYTES);
  }

  @ParameterizedTest
  @ValueSource(ints = { 0, 3, 10, 31, 32, 33, 100 })
  void theKeyOfAMapIsChargedForItsSizeAtEveryLevel(final int levels) {
    final Map<Object, Object> map = new LinkedHashMap<>();
    map.put(new byte[1_000_000], 0L);

    assertThat(HeapEstimator.estimate(row(wrap(map, levels)))).isGreaterThanOrEqualTo(1_000_000L);
  }

  @Test
  @Timeout(60)
  void aValueHoldingItselfIsCountedOnceHoweverManyTimesItIsHeld() {
    final List<Object> cycle = new ArrayList<>();
    cycle.add(cycle);
    Object held = cycle;
    for (int i = 0; i < 5; i++)
      held = new ArrayList<>(Collections.nCopies(16, held));

    assertThat(HeapEstimator.estimate(row(held))).as("each list that holds itself is counted for one round of its cycle").isPositive()
        .isLessThan(1L << 34);
  }

  @Test
  void aListHoldingItselfIsEstimatedOnce() {
    final List<Object> single = new ArrayList<>();
    single.add(single);
    assertThat(HeapEstimator.estimate(single)).isPositive();

    final List<Object> many = new ArrayList<>(Collections.nCopies(20, null));
    for (int i = 0; i < many.size(); i++)
      many.set(i, many);
    assertThat(HeapEstimator.estimate(many)).isPositive();

    final Map<String, Object> map = new LinkedHashMap<>();
    map.put("self", map);
    assertThat(HeapEstimator.estimate(row(map))).isPositive();
  }

  @Test
  void anEstimateNeverWrapsAround() {
    // Lists of two million references to one list of the same shape, five levels: far more elements than a long counts
    Object nested = Collections.nCopies(2_000_000, 1L);
    for (int i = 0; i < 6; i++)
      nested = Collections.nCopies(2_000_000, nested);

    assertThat(HeapEstimator.estimate(row(nested))).isGreaterThan(1L << 40);
    assertThat(HeapEstimator.estimate(nested)).isGreaterThan(1L << 40);
  }

  @Test
  void aSharedRegistryGivesBackWhatItChargedForContainersNestedDeep() {
    for (final int levels : new int[] { 0, 3, 4, 10, 20, 29, 30, 31, 32, 33, 64 }) {
      final ResultInternal row = row(largeContainersNestedDeep(levels));
      final HeapEstimator.Shared shared = new HeapEstimator.Shared();

      final long added = shared.add(row);
      assertThat(added).as("levels %d", levels).isPositive();
      if (levels < 30)
        assertThat(shared.containers()).as("levels %d: the list and the array it holds are remembered", levels).isEqualTo(2);

      assertThat(shared.remove(row)).as("levels %d", levels).isEqualTo(added);
      assertThat(shared.containers()).as("levels %d", levels).isZero();
    }
  }

  @Test
  void aSharedRegistryChargesANestedContainerOnceForTheRowsThatHoldIt() {
    final Object nested = largeContainersNestedDeep(10);
    final HeapEstimator.Shared shared = new HeapEstimator.Shared();

    final long first = shared.add(row(nested));
    final long second = shared.add(row(nested));

    assertThat(first).isGreaterThan(4_096L);
    assertThat(second).as("the second row holds the nested list as a reference").isLessThan(first / 2);
  }

  /** A list of more elements than a registry counts apart, which holds a large array, under {@code levels} lists of one element. */
  private static Object largeContainersNestedDeep(final int levels) {
    final List<Object> list = new ArrayList<>(Collections.nCopies(HeapEstimator.SAMPLED_ELEMENTS + 4, null));
    list.set(0, new byte[4_096]);
    return wrap(list, levels);
  }
}
