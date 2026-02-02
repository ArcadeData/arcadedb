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
package com.arcadedb.utility;

import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

class CollectionUtilsTest {

  @Test
  void compareEqualLists() {
    final List<String> list1 = Arrays.asList("a", "b", "c");
    final List<String> list2 = Arrays.asList("a", "b", "c");

    assertThat(CollectionUtils.compare(list1, list2)).isEqualTo(0);
  }

  @Test
  void compareDifferentLists() {
    final List<String> list1 = Arrays.asList("a", "b", "c");
    final List<String> list2 = Arrays.asList("a", "b", "d");

    assertThat(CollectionUtils.compare(list1, list2)).isLessThan(0);
  }

  @Test
  void compareDifferentSizeLists() {
    final List<String> list1 = Arrays.asList("a", "b");
    final List<String> list2 = Arrays.asList("a", "b", "c");

    assertThat(CollectionUtils.compare(list1, list2)).isLessThan(0);
    assertThat(CollectionUtils.compare(list2, list1)).isGreaterThan(0);
  }

  @Test
  void compareEqualMaps() {
    final Map<String, Comparable> map1 = new HashMap<>();
    map1.put("key1", "value1");
    map1.put("key2", 42);

    final Map<String, Comparable> map2 = new HashMap<>();
    map2.put("key1", "value1");
    map2.put("key2", 42);

    assertThat(CollectionUtils.compare(map1, map2)).isEqualTo(0);
  }

  @Test
  void compareDifferentMaps() {
    final Map<String, Comparable> map1 = new HashMap<>();
    map1.put("key1", "value1");

    final Map<String, Comparable> map2 = new HashMap<>();
    map2.put("key1", "value2");

    assertThat(CollectionUtils.compare(map1, map2)).isNotEqualTo(0);
  }

  @Test
  void compareMapsWithDifferentSizes() {
    final Map<String, Comparable> map1 = new HashMap<>();
    map1.put("key1", "value1");

    final Map<String, Comparable> map2 = new HashMap<>();
    map2.put("key1", "value1");
    map2.put("key2", "value2");

    assertThat(CollectionUtils.compare(map1, map2)).isLessThan(0);
    assertThat(CollectionUtils.compare(map2, map1)).isGreaterThan(0);
  }

  @Test
  void countEntriesWithIterator() {
    final List<String> list = Arrays.asList("a", "b", "c", "d", "e");
    assertThat(CollectionUtils.countEntries(list.iterator())).isEqualTo(5);
  }

  @Test
  void countEntriesWithEmptyIterator() {
    final List<String> list = Collections.emptyList();
    assertThat(CollectionUtils.countEntries(list.iterator())).isEqualTo(0);
  }

  @Test
  void isNotEmptyWithNonEmptyCollection() {
    assertThat(CollectionUtils.isNotEmpty(Arrays.asList("a", "b"))).isTrue();
  }

  @Test
  void isNotEmptyWithEmptyCollection() {
    assertThat(CollectionUtils.isNotEmpty(Collections.emptyList())).isFalse();
  }

  @Test
  void isNotEmptyWithNull() {
    assertThat(CollectionUtils.isNotEmpty(null)).isFalse();
  }

  @Test
  void isEmptyWithEmptyCollection() {
    assertThat(CollectionUtils.isEmpty(Collections.emptyList())).isTrue();
  }

  @Test
  void isEmptyWithNonEmptyCollection() {
    assertThat(CollectionUtils.isEmpty(Arrays.asList("a"))).isFalse();
  }

  @Test
  void isEmptyWithNull() {
    assertThat(CollectionUtils.isEmpty(null)).isTrue();
  }

  @Test
  void arrayToListWithObjectArray() {
    final String[] array = {"a", "b", "c"};
    final List<?> result = CollectionUtils.arrayToList(array);
    assertThat(result).hasSize(3);
    assertThat(result.get(0)).isEqualTo("a");
    assertThat(result.get(1)).isEqualTo("b");
    assertThat(result.get(2)).isEqualTo("c");
  }

  @Test
  void arrayToListWithIntArray() {
    final int[] array = {1, 2, 3};
    final List<?> result = CollectionUtils.arrayToList(array);
    assertThat(result).hasSize(3);
    assertThat(result.get(0)).isEqualTo(1);
    assertThat(result.get(1)).isEqualTo(2);
    assertThat(result.get(2)).isEqualTo(3);
  }

  @Test
  void arrayToListWithLongArray() {
    final long[] array = {1L, 2L, 3L};
    final List<?> result = CollectionUtils.arrayToList(array);
    assertThat(result).hasSize(3);
    assertThat(result.get(0)).isEqualTo(1L);
    assertThat(result.get(1)).isEqualTo(2L);
    assertThat(result.get(2)).isEqualTo(3L);
  }

  @Test
  void arrayToListWithDoubleArray() {
    final double[] array = {1.0, 2.0, 3.0};
    final List<?> result = CollectionUtils.arrayToList(array);
    assertThat(result).hasSize(3);
    assertThat(result.get(0)).isEqualTo(1.0);
  }

  @Test
  void arrayToListWithFloatArray() {
    final float[] array = {1.0f, 2.0f, 3.0f};
    final List<?> result = CollectionUtils.arrayToList(array);
    assertThat(result).hasSize(3);
    assertThat(result.get(0)).isEqualTo(1.0f);
  }

  @Test
  void arrayToListWithBooleanArray() {
    final boolean[] array = {true, false, true};
    final List<?> result = CollectionUtils.arrayToList(array);
    assertThat(result).hasSize(3);
    assertThat(result.get(0)).isEqualTo(true);
    assertThat(result.get(1)).isEqualTo(false);
  }

  @Test
  void arrayToListWithByteArray() {
    final byte[] array = {1, 2, 3};
    final List<?> result = CollectionUtils.arrayToList(array);
    assertThat(result).hasSize(3);
    assertThat(result.get(0)).isEqualTo((byte) 1);
  }

  @Test
  void arrayToListWithShortArray() {
    final short[] array = {1, 2, 3};
    final List<?> result = CollectionUtils.arrayToList(array);
    assertThat(result).hasSize(3);
    assertThat(result.get(0)).isEqualTo((short) 1);
  }

  @Test
  void arrayToListWithCharArray() {
    final char[] array = {'a', 'b', 'c'};
    final List<?> result = CollectionUtils.arrayToList(array);
    assertThat(result).hasSize(3);
    assertThat(result.get(0)).isEqualTo('a');
    assertThat(result.get(1)).isEqualTo('b');
    assertThat(result.get(2)).isEqualTo('c');
  }

  @Test
  void addToUnmodifiableListCreatesNewList() {
    final List<String> original = Arrays.asList("a", "b");
    final List<String> result = CollectionUtils.addToUnmodifiableList(original, "c");

    assertThat(result).hasSize(3);
    assertThat(result.get(0)).isEqualTo("a");
    assertThat(result.get(1)).isEqualTo("b");
    assertThat(result.get(2)).isEqualTo("c");
  }

  @Test
  void removeFromUnmodifiableListCreatesNewList() {
    final List<String> original = Arrays.asList("a", "b", "c");
    final List<String> result = CollectionUtils.removeFromUnmodifiableList(original, "b");

    assertThat(result).hasSize(2);
    assertThat(result.get(0)).isEqualTo("a");
    assertThat(result.get(1)).isEqualTo("c");
  }

  @Test
  void removeFromUnmodifiableListWhenElementNotPresent() {
    final List<String> original = Arrays.asList("a", "b");
    final List<String> result = CollectionUtils.removeFromUnmodifiableList(original, "c");

    assertThat(result).hasSize(2);
    assertThat(result.get(0)).isEqualTo("a");
    assertThat(result.get(1)).isEqualTo("b");
  }

  @Test
  void addAllToUnmodifiableList() {
    final List<String> original = Arrays.asList("a", "b");
    final List<String> toAdd = Arrays.asList("c", "d");
    final List<String> result = CollectionUtils.addAllToUnmodifiableList(original, toAdd);

    assertThat(result).hasSize(4);
    assertThat(result).contains("a", "b", "c", "d");
  }

  @Test
  void removeAllFromUnmodifiableList() {
    final List<String> original = Arrays.asList("a", "b", "c", "d");
    final List<String> toRemove = Arrays.asList("b", "d");
    final List<String> result = CollectionUtils.removeAllFromUnmodifiableList(original, toRemove);

    assertThat(result).hasSize(2);
    assertThat(result.get(0)).isEqualTo("a");
    assertThat(result.get(1)).isEqualTo("c");
  }
}
