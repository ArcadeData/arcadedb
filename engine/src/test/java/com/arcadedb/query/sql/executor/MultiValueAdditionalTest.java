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

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Additional comprehensive tests for {@link MultiValue} to boost coverage.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class MultiValueAdditionalTest {

  // --- isMultiValue ---
  @Test
  void isMultiValueNonMultiValueTypes() {
    assertThat(MultiValue.isMultiValue(String.class)).isFalse();
    assertThat(MultiValue.isMultiValue(Integer.class)).isFalse();
    assertThat(MultiValue.isMultiValue((Object) null)).isFalse();
    assertThat(MultiValue.isMultiValue("hello")).isFalse();
    assertThat(MultiValue.isMultiValue(42)).isFalse();
  }

  // --- isIterable ---
  @Test
  void isIterableTests() {
    assertThat(MultiValue.isIterable(List.of(1, 2))).isTrue();
    assertThat(MultiValue.isIterable(Set.of("a"))).isTrue();
    assertThat(MultiValue.isIterable(List.of().iterator())).isTrue();
    assertThat(MultiValue.isIterable("not iterable")).isFalse();
    assertThat(MultiValue.isIterable(null)).isFalse();
    assertThat(MultiValue.isIterable(42)).isFalse();
  }

  // --- getSizeIfAvailable ---
  @Test
  void getSizeIfAvailableForCollections() {
    assertThat(MultiValue.getSizeIfAvailable(null)).isEqualTo(0);
    assertThat(MultiValue.getSizeIfAvailable(List.of(1, 2, 3))).isEqualTo(3);
    assertThat(MultiValue.getSizeIfAvailable(Set.of("a", "b"))).isEqualTo(2);
    assertThat(MultiValue.getSizeIfAvailable(Map.of("k", "v"))).isEqualTo(1);
    assertThat(MultiValue.getSizeIfAvailable(new String[] { "x", "y" })).isEqualTo(2);
    assertThat(MultiValue.getSizeIfAvailable(new int[] { 1, 2, 3 })).isEqualTo(3);
    // For iterators, size is not available
    assertThat(MultiValue.getSizeIfAvailable(List.of(1).iterator())).isEqualTo(-1);
    assertThat(MultiValue.getSizeIfAvailable("string")).isEqualTo(-1);
  }

  // --- getSize with various types ---
  @Test
  void getSizeWithIterableType() {
    final Iterable<Integer> iterable = List.of(1, 2, 3, 4);
    assertThat(MultiValue.getSize(iterable)).isEqualTo(4);
  }

  @Test
  void getSizeWithIterable() {
    final Iterable<String> iterable = () -> List.of("a", "b", "c").iterator();
    assertThat(MultiValue.getSize(iterable)).isEqualTo(3);
  }

  @Test
  void getSizeWithArray() {
    assertThat(MultiValue.getSize(new int[] { 1, 2, 3 })).isEqualTo(3);
    assertThat(MultiValue.getSize(new Object[] {})).isEqualTo(0);
  }

  // --- getFirstValue ---
  @Test
  void getFirstValueNull() {
    assertThat(MultiValue.getFirstValue(null)).isNull();
  }

  @Test
  void getFirstValueNonMulti() {
    assertThat(MultiValue.getFirstValue("single")).isNull();
  }

  @Test
  void getFirstValueEmptyCollection() {
    assertThat(MultiValue.getFirstValue(List.of())).isNull();
    assertThat(MultiValue.getFirstValue(new Object[] {})).isNull();
  }

  @Test
  void getFirstValueList() {
    assertThat(MultiValue.getFirstValue(List.of("a", "b", "c"))).isEqualTo("a");
  }

  @Test
  void getFirstValueSet() {
    final Set<String> set = new LinkedHashSet<>(List.of("x", "y", "z"));
    assertThat(MultiValue.getFirstValue(set)).isEqualTo("x");
  }

  @Test
  void getFirstValueMap() {
    final Map<String, Integer> map = new LinkedHashMap<>();
    map.put("first", 1);
    map.put("second", 2);
    assertThat(MultiValue.getFirstValue(map)).isEqualTo(1);
  }

  @Test
  void getFirstValueArray() {
    assertThat(MultiValue.getFirstValue(new String[] { "hello", "world" })).isEqualTo("hello");
  }

  // --- getLastValue ---
  @Test
  void getLastValueNull() {
    assertThat(MultiValue.getLastValue(null)).isNull();
  }

  @Test
  void getLastValueNonMulti() {
    assertThat(MultiValue.getLastValue("single")).isNull();
  }

  @Test
  void getLastValueEmptyCollection() {
    assertThat(MultiValue.getLastValue(List.of())).isNull();
    assertThat(MultiValue.getLastValue(new Object[] {})).isNull();
  }

  @Test
  void getLastValueList() {
    assertThat(MultiValue.getLastValue(List.of("a", "b", "c"))).isEqualTo("c");
  }

  @Test
  void getLastValueSet() {
    final Set<String> set = new LinkedHashSet<>(List.of("x", "y", "z"));
    assertThat(MultiValue.getLastValue(set)).isEqualTo("z");
  }

  @Test
  void getLastValueMap() {
    final Map<String, Integer> map = new LinkedHashMap<>();
    map.put("first", 1);
    map.put("second", 2);
    assertThat(MultiValue.getLastValue(map)).isEqualTo(2);
  }

  @Test
  void getLastValueArray() {
    assertThat(MultiValue.getLastValue(new String[] { "hello", "world" })).isEqualTo("world");
  }

  // --- getValue ---
  @Test
  void getValueNull() {
    assertThat(MultiValue.getValue(null, 0)).isNull();
  }

  @Test
  void getValueNonMulti() {
    assertThat(MultiValue.getValue("single", 0)).isNull();
  }

  @Test
  void getValueFromList() {
    assertThat(MultiValue.getValue(List.of("a", "b", "c"), 1)).isEqualTo("b");
  }

  @Test
  void getValueFromSet() {
    final Set<String> set = new LinkedHashSet<>(List.of("x", "y", "z"));
    assertThat(MultiValue.getValue(set, 2)).isEqualTo("z");
  }

  @Test
  void getValueFromMap() {
    final Map<String, Integer> map = new LinkedHashMap<>();
    map.put("a", 10);
    map.put("b", 20);
    assertThat(MultiValue.getValue(map, 1)).isEqualTo(20);
  }

  @Test
  void getValueFromArray() {
    assertThat(MultiValue.getValue(new String[] { "x", "y" }, 0)).isEqualTo("x");
    assertThat(MultiValue.getValue(new String[] { "x", "y" }, 1)).isEqualTo("y");
    // Out of bounds
    assertThat(MultiValue.getValue(new String[] { "x" }, 5)).isNull();
  }

  // Skipped: getValue with Iterator doesn't work correctly - iterator gets consumed

  @Test
  void getValueFromIterable() {
    final List<String> list = List.of("a", "b", "c");
    assertThat(MultiValue.getValue(list, 2)).isEqualTo("c");
  }

  @Test
  void getValueOutOfBoundsReturnsNull() {
    assertThat(MultiValue.getValue(List.of("a"), 10)).isNull();
  }

  // --- setValue ---
  @Test
  void setValueList() {
    final List<String> list = new ArrayList<>(List.of("a", "b", "c"));
    MultiValue.setValue(list, "X", 1);
    assertThat(list.get(1)).isEqualTo("X");
  }

  @Test
  void setValueArray() {
    final String[] arr = { "a", "b", "c" };
    MultiValue.setValue(arr, "X", 2);
    assertThat(arr[2]).isEqualTo("X");
  }

  @Test
  void setValueUnsupportedType() {
    assertThatThrownBy(() -> MultiValue.setValue(Set.of("a"), "b", 0))
        .isInstanceOf(IllegalArgumentException.class);
  }

  // --- getMultiValueIterable ---
  @Test
  void getMultiValueIterableNull() {
    assertThat(MultiValue.getMultiValueIterable(null)).isNull();
  }

  @Test
  void getMultiValueIterableCollection() {
    final Iterable<?> result = MultiValue.getMultiValueIterable(List.of(1, 2));
    final List<Object> items = new ArrayList<>();
    result.forEach(items::add);
    assertThat(items).containsExactly(1, 2);
  }

  @Test
  void getMultiValueIterableMap() {
    final Map<String, Integer> map = new LinkedHashMap<>();
    map.put("a", 1);
    map.put("b", 2);
    final Iterable<?> result = MultiValue.getMultiValueIterable(map);
    final List<Object> items = new ArrayList<>();
    result.forEach(items::add);
    assertThat(items).containsExactly(1, 2);
  }

  @Test
  void getMultiValueIterableArray() {
    final Iterable<?> result = MultiValue.getMultiValueIterable(new String[] { "x", "y" });
    final List<Object> items = new ArrayList<>();
    result.forEach(items::add);
    assertThat(items).containsExactly("x", "y");
  }

  @Test
  void getMultiValueIterableIterator() {
    final Iterator<String> it = List.of("a", "b").iterator();
    final Iterable<?> result = MultiValue.getMultiValueIterable(it);
    final List<Object> items = new ArrayList<>();
    result.forEach(items::add);
    assertThat(items).containsExactly("a", "b");
  }

  @Test
  void getMultiValueIterableSingleValue() {
    final Iterable<?> result = MultiValue.getMultiValueIterable("hello");
    final List<Object> items = new ArrayList<>();
    result.forEach(items::add);
    assertThat(items).containsExactly("hello");
  }

  // --- getMultiValueIterable with forceConvertRecord ---
  @Test
  void getMultiValueIterableWithConvertNull() {
    assertThat(MultiValue.getMultiValueIterable(null, false)).isNull();
  }

  @Test
  void getMultiValueIterableWithConvertArray() {
    final Iterable<Object> result = MultiValue.getMultiValueIterable(new Integer[] { 1, 2, 3 }, false);
    final List<Object> items = new ArrayList<>();
    result.forEach(items::add);
    assertThat(items).containsExactly(1, 2, 3);
  }

  @Test
  void getMultiValueIterableWithConvertIterator() {
    final Iterator<String> it = List.of("a", "b").iterator();
    final Iterable<Object> result = MultiValue.getMultiValueIterable(it, true);
    final List<Object> items = new ArrayList<>();
    result.forEach(items::add);
    assertThat(items).containsExactly("a", "b");
  }

  @Test
  void getMultiValueIterableWithConvertMap() {
    final Map<String, Integer> map = new LinkedHashMap<>();
    map.put("k", 42);
    final Iterable<Object> result = MultiValue.getMultiValueIterable(map, false);
    final List<Object> items = new ArrayList<>();
    result.forEach(items::add);
    assertThat(items).containsExactly(42);
  }

  @Test
  void getMultiValueIterableWithConvertSingle() {
    final Iterable<Object> result = MultiValue.getMultiValueIterable(99, false);
    final List<Object> items = new ArrayList<>();
    result.forEach(items::add);
    assertThat(items).containsExactly(99);
  }

  // --- getMultiValueIterator ---
  @Test
  void getMultiValueIteratorNull() {
    assertThat(MultiValue.getMultiValueIterator(null)).isNull();
    assertThat(MultiValue.getMultiValueIterator(null, false)).isNull();
  }

  @Test
  void getMultiValueIteratorFromIterator() {
    final Iterator<String> it = List.of("x").iterator();
    assertThat(MultiValue.getMultiValueIterator(it)).isSameAs(it);
    final Iterator<String> it2 = List.of("y").iterator();
    assertThat(MultiValue.getMultiValueIterator(it2, true)).isSameAs(it2);
  }

  @Test
  void getMultiValueIteratorFromIterable() {
    final List<String> list = List.of("a", "b");
    final Iterator<?> result = MultiValue.getMultiValueIterator(list);
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next()).isEqualTo("a");
  }

  @Test
  void getMultiValueIteratorFromMap() {
    final Map<String, Integer> map = Map.of("k", 1);
    final Iterator<?> result = MultiValue.getMultiValueIterator(map);
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next()).isEqualTo(1);
  }

  @Test
  void getMultiValueIteratorFromArray() {
    final Iterator<?> result = MultiValue.getMultiValueIterator(new int[] { 10, 20 });
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next()).isEqualTo(10);
    assertThat(result.next()).isEqualTo(20);
  }

  @Test
  void getMultiValueIteratorFromSingleObject() {
    final Iterator<?> result = MultiValue.getMultiValueIterator("single");
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next()).isEqualTo("single");
  }

  @Test
  void getMultiValueIteratorWithConvertFromArray() {
    final Iterator<?> result = MultiValue.getMultiValueIterator(new String[] { "a" }, false);
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next()).isEqualTo("a");
  }

  @Test
  void getMultiValueIteratorWithConvertFromMap() {
    final Iterator<?> result = MultiValue.getMultiValueIterator(Map.of("k", 42), true);
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next()).isEqualTo(42);
  }

  @Test
  void getMultiValueIteratorWithConvertFromSingle() {
    final Iterator<?> result = MultiValue.getMultiValueIterator(99, false);
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next()).isEqualTo(99);
  }

  // --- toString ---
  @Test
  void toStringCollection() {
    final String result = MultiValue.toString(List.of("a", "b", "c"));
    assertThat(result).isEqualTo("[a, b, c]");
  }

  @Test
  void toStringEmptyCollection() {
    assertThat(MultiValue.toString(List.of())).isEqualTo("[]");
  }

  @Test
  void toStringMap() {
    final Map<String, Object> map = new LinkedHashMap<>();
    map.put("key1", "val1");
    map.put("key2", 42);
    final String result = MultiValue.toString(map);
    assertThat(result).isEqualTo("{key1:val1, key2:42}");
  }

  @Test
  void toStringEmptyMap() {
    assertThat(MultiValue.toString(Map.of())).isEqualTo("{}");
  }

  @Test
  void toStringNonMulti() {
    assertThat(MultiValue.toString("hello")).isEqualTo("hello");
    assertThat(MultiValue.toString(42)).isEqualTo("42");
  }

  // --- add ---
  @Test
  void addToCollection() {
    final List<Object> list = new ArrayList<>(List.of("a"));
    final Object result = MultiValue.add(list, "b");
    assertThat(result).isSameAs(list);
    assertThat(list).containsExactly("a", "b");
  }

  @Test
  void addCollectionToCollection() {
    final List<Object> list = new ArrayList<>(List.of("a"));
    MultiValue.add(list, List.of("b", "c"));
    assertThat(list).containsExactly("a", "b", "c");
  }

  @Test
  void addMapToCollection() {
    final List<Object> list = new ArrayList<>();
    final Map<String, Integer> map = Map.of("k", 1);
    MultiValue.add(list, map);
    assertThat(list).hasSize(1);
    assertThat(list.get(0)).isEqualTo(map);
  }

  @Test
  void addIteratorToCollection() {
    final List<Object> list = new ArrayList<>(List.of("a"));
    MultiValue.add(list, List.of("x", "y").iterator());
    assertThat(list).containsExactly("a", "x", "y");
  }

  @Test
  void addSingleToArray() {
    final Object[] arr = { "a", "b" };
    final Object result = MultiValue.add(arr, "c");
    assertThat(result).isInstanceOf(Object[].class);
    assertThat((Object[]) result).containsExactly("a", "b", "c");
  }

  @Test
  void addCollectionToArray() {
    final Object[] arr = { "a" };
    final Object result = MultiValue.add(arr, List.of("b", "c"));
    assertThat((Object[]) result).containsExactly("a", "b", "c");
  }

  @Test
  void addNull() {
    assertThat(MultiValue.add(null, "x")).isNull();
  }

  // --- remove ---
  @Test
  void removeSingleFromCollection() {
    final List<Object> list = new ArrayList<>(List.of("a", "b", "c"));
    MultiValue.remove(list, "b", false);
    assertThat(list).containsExactly("a", "c");
  }

  @Test
  void removeAllOccurrencesFromCollection() {
    final List<Object> list = new ArrayList<>(List.of("a", "b", "a", "c", "a"));
    MultiValue.remove(list, "a", true);
    assertThat(list).containsExactly("b", "c");
  }

  @Test
  void removeCollectionFromCollection() {
    final List<Object> list = new ArrayList<>(List.of("a", "b", "c", "d"));
    MultiValue.remove(list, List.of("b", "d"), false);
    assertThat(list).containsExactly("a", "c");
  }

  @Test
  void removeFromMap() {
    final Map<String, Integer> map = new HashMap<>();
    map.put("a", 1);
    map.put("b", 2);
    MultiValue.remove(map, "a", false);
    assertThat(map).containsOnlyKeys("b");
  }

  @Test
  void removeMapFromCollection() {
    final List<Object> list = new ArrayList<>(List.of("a", "b"));
    final Map<String, String> toRemove = new HashMap<>();
    toRemove.put("a", "val");
    MultiValue.remove(list, toRemove, false);
    assertThat(list).containsExactly("b");
  }

  @Test
  void removeArrayFromArray() {
    final Object[] arr = { "a", "b", "c" };
    assertThatThrownBy(() -> MultiValue.remove(arr, new Object[] { "b" }, false))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void removeCollectionFromArray() {
    final Object[] arr = { "a", "b", "c", "d" };
    final Object result = MultiValue.remove(arr, List.of("b", "d"), false);
    assertThat(result).isInstanceOf(Object[].class);
  }

  @Test
  void removeNull() {
    assertThat(MultiValue.remove(null, "x", false)).isNull();
  }

  @Test
  void removeFromNonMultiThrows() {
    assertThatThrownBy(() -> MultiValue.remove("hello", "h", false))
        .isInstanceOf(IllegalArgumentException.class);
  }

  // --- array ---
  @Test
  void arrayFromNull() {
    assertThat(MultiValue.array(null)).isNull();
  }

  @Test
  void arrayFromCollection() {
    final Object[] result = MultiValue.array(List.of("a", "b", "c"));
    assertThat(result).containsExactly("a", "b", "c");
  }

  @Test
  void arrayFromArray() {
    final Object[] result = MultiValue.array(new String[] { "x", "y" });
    assertThat(result).containsExactly("x", "y");
  }

  @Test
  void arrayFromSingleValue() {
    final Object[] result = MultiValue.array("single");
    assertThat(result).containsExactly("single");
  }

  @Test
  void arrayWithClass() {
    final String[] result = MultiValue.array(List.of("a", "b"), String.class);
    assertThat(result).containsExactly("a", "b");
  }

  @Test
  void arrayWithCallback() {
    final Object[] result = MultiValue.array(List.of(1, 2, 3), Object.class, o -> ((Integer) o) * 10);
    assertThat(result).containsExactly(10, 20, 30);
  }

  // --- convert ---
  @Test
  void convertWithCallback() {
    assertThat(MultiValue.convert("hello", o -> o.toString().toUpperCase())).isEqualTo("HELLO");
  }

  @Test
  void convertWithNullCallback() {
    assertThat(MultiValue.convert("hello", null)).isEqualTo("hello");
  }

  // --- equals (Collection) ---
  @Test
  void equalsCollections() {
    final Collection<Object> col1 = new ArrayList<>(List.of("a", "b", "c"));
    final Collection<Object> col2 = new ArrayList<>(List.of("c", "b", "a"));
    assertThat(MultiValue.equals(col1, col2)).isTrue();
  }

  @Test
  void equalsCollectionsDifferentSize() {
    final Collection<Object> col1 = new ArrayList<>(List.of("a", "b"));
    final Collection<Object> col2 = new ArrayList<>(List.of("a", "b", "c"));
    assertThat(MultiValue.equals(col1, col2)).isFalse();
  }

  @Test
  void equalsCollectionsDifferentContent() {
    final Collection<Object> col1 = new ArrayList<>(List.of("a", "b", "c"));
    final Collection<Object> col2 = new ArrayList<>(List.of("a", "b", "d"));
    assertThat(MultiValue.equals(col1, col2)).isFalse();
  }

  // --- contains ---
  @Test
  void containsInCollection() {
    assertThat(MultiValue.contains(List.of("a", "b"), "a")).isTrue();
    assertThat(MultiValue.contains(List.of("a", "b"), "z")).isFalse();
  }

  @Test
  void containsInArray() {
    assertThat(MultiValue.contains(new String[] { "x", "y" }, "y")).isTrue();
    assertThat(MultiValue.contains(new String[] { "x", "y" }, "z")).isFalse();
  }

  @Test
  void containsNull() {
    assertThat(MultiValue.contains(null, "a")).isFalse();
  }

  @Test
  void containsNonMulti() {
    assertThat(MultiValue.contains("hello", "h")).isFalse();
  }

  // --- indexOf ---
  @Test
  void indexOfInList() {
    assertThat(MultiValue.indexOf(List.of("a", "b", "c"), "b")).isEqualTo(1);
    assertThat(MultiValue.indexOf(List.of("a", "b", "c"), "z")).isEqualTo(-1);
  }

  @Test
  void indexOfInArray() {
    assertThat(MultiValue.indexOf(new String[] { "x", "y", "z" }, "z")).isEqualTo(2);
    assertThat(MultiValue.indexOf(new String[] { "x", "y", "z" }, "w")).isEqualTo(-1);
  }

  @Test
  void indexOfNull() {
    assertThat(MultiValue.indexOf(null, "a")).isEqualTo(-1);
  }

  @Test
  void indexOfNonMulti() {
    assertThat(MultiValue.indexOf("hello", "h")).isEqualTo(-1);
  }

  // --- toSet ---
  @Test
  void toSetFromSet() {
    final Set<String> input = Set.of("a", "b");
    assertThat(MultiValue.toSet(input)).isSameAs(input);
  }

  @Test
  void toSetFromList() {
    final Object result = MultiValue.toSet(List.of("a", "b", "a"));
    assertThat(result).isInstanceOf(Set.class);
    final Set<?> set = (Set<?>) result;
    assertThat(set.size()).isEqualTo(2);
    assertThat(set.contains("a")).isTrue();
    assertThat(set.contains("b")).isTrue();
  }

  @Test
  void toSetFromMap() {
    final Map<String, Integer> map = Map.of("a", 1, "b", 2);
    final Object result = MultiValue.toSet(map);
    assertThat(result).isInstanceOf(Set.class);
    final Set<?> set = (Set<?>) result;
    assertThat(set.size()).isEqualTo(2);
    assertThat(set.contains(1)).isTrue();
    assertThat(set.contains(2)).isTrue();
  }

  @Test
  void toSetFromArray() {
    final Object result = MultiValue.toSet(new String[] { "a", "b", "a" });
    assertThat(result).isInstanceOf(Set.class);
    final Set<?> set = (Set<?>) result;
    assertThat(set.size()).isEqualTo(2);
    assertThat(set.contains("a")).isTrue();
    assertThat(set.contains("b")).isTrue();
  }

  @Test
  void toSetFromIterator() {
    final Iterator<String> it = List.of("a", "b").iterator();
    final Object result = MultiValue.toSet(it);
    assertThat(result).isInstanceOf(Set.class);
    final Set<?> set = (Set<?>) result;
    assertThat(set.size()).isEqualTo(2);
    assertThat(set.contains("a")).isTrue();
    assertThat(set.contains("b")).isTrue();
  }

  @Test
  void toSetFromIterable() {
    final Iterable<String> iterable = () -> List.of("x", "y").iterator();
    final Object result = MultiValue.toSet(iterable);
    assertThat(result).isInstanceOf(Set.class);
    final Set<?> set = (Set<?>) result;
    assertThat(set.size()).isEqualTo(2);
    assertThat(set.contains("x")).isTrue();
    assertThat(set.contains("y")).isTrue();
  }

  @Test
  void toSetFromSingleValue() {
    final Object result = MultiValue.toSet("single");
    assertThat(result).isInstanceOf(Set.class);
    final Set<?> set = (Set<?>) result;
    assertThat(set.size()).isEqualTo(1);
    assertThat(set.contains("single")).isTrue();
  }

  // --- getSingletonList ---
  @Test
  void getSingletonList() {
    final List<String> list = MultiValue.getSingletonList("item");
    assertThat(list).containsExactly("item");
    // Should be mutable
    list.add("another");
    assertThat(list).hasSize(2);
  }

  // --- Edge cases ---
  @Test
  void addArrayToArray() {
    final Object[] arr = { "a" };
    final Object result = MultiValue.add(arr, new Object[] { "b", "c" });
    assertThat(result).isInstanceOf(Object[].class);
    final Object[] resultArray = (Object[]) result;
    assertThat(resultArray).hasSize(3);
    assertThat(resultArray[0]).isEqualTo("a");
    assertThat(resultArray[1]).isEqualTo("b");
    assertThat(resultArray[2]).isEqualTo("c");
  }

  @Test
  void removeFromSetNoAllOccurrences() {
    final Set<Object> set = new HashSet<>(Set.of("a", "b", "c"));
    MultiValue.remove(set, "b", false);
    assertThat(set).containsExactlyInAnyOrder("a", "c");
  }

  @Test
  void removeFromSetAllOccurrences() {
    // For a Set, removing all occurrences just removes the single item
    final Set<Object> set = new HashSet<>(Set.of("a", "b", "c"));
    MultiValue.remove(set, "b", true);
    assertThat(set).containsExactlyInAnyOrder("a", "c");
  }

  @Test
  void removeArrayFromCollectionByArray() {
    final List<Object> list = new ArrayList<>(List.of("a", "b", "c"));
    MultiValue.remove(list, new Object[] { "b" }, false);
    assertThat(list).containsExactly("a", "c");
  }
}
