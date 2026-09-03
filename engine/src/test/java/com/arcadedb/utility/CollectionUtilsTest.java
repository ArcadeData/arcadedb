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

import com.arcadedb.serializer.BinaryComparator;
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

  /**
   * Issue #7111: for maps with disjoint keys the comparison answered "greater" in both directions, because every key of
   * the first map was missing from the second whichever way round the arguments went.
   */
  @Test
  void compareDisjointKeyMapsIsAntisymmetric() {
    final Map<String, Comparable> m1 = new HashMap<>(Map.of("a", 1));
    final Map<String, Comparable> m2 = new HashMap<>(Map.of("b", 1));

    assertThat(CollectionUtils.compare(m1, m2)).isNotZero();
    assertThat(Integer.signum(CollectionUtils.compare(m1, m2))).isEqualTo(-Integer.signum(CollectionUtils.compare(m2, m1)));
    // The index-ordering entry point goes through the same comparison
    assertThat(Integer.signum(BinaryComparator.compareTo(m1, m2))).isEqualTo(-Integer.signum(BinaryComparator.compareTo(m2, m1)));
  }

  /**
   * Issue #7111: a null value on both sides of the first shared key declared the whole maps equal, so a later key that
   * differed was never examined.
   */
  @Test
  void compareMapsSharingNullValueKeepsComparingTheRemainingKeys() {
    final Map<String, Comparable> m1 = new LinkedHashMap<>();
    m1.put("k", null);
    m1.put("z", 1);
    final Map<String, Comparable> m2 = new LinkedHashMap<>();
    m2.put("k", null);
    m2.put("z", 2);

    assertThat(CollectionUtils.compare(m1, m2)).isLessThan(0);
    assertThat(CollectionUtils.compare(m2, m1)).isGreaterThan(0);

    final Map<String, Comparable> m3 = new LinkedHashMap<>();
    m3.put("k", null);
    m3.put("z", 1);
    assertThat(CollectionUtils.compare(m1, m3)).isZero();
  }

  @Test
  void compareMapsIgnoresInsertionOrder() {
    final Map<String, Comparable> m1 = new LinkedHashMap<>();
    m1.put("a", 1);
    m1.put("b", 2);
    final Map<String, Comparable> m2 = new LinkedHashMap<>();
    m2.put("b", 2);
    m2.put("a", 1);

    assertThat(CollectionUtils.compare(m1, m2)).isZero();
    assertThat(CollectionUtils.compare(m2, m1)).isZero();
  }

  @Test
  void compareMapsNullValueSortsBeforeAnyValue() {
    final Map<String, Comparable> m1 = new HashMap<>();
    m1.put("a", null);
    final Map<String, Comparable> m2 = new HashMap<>(Map.of("a", 0));

    assertThat(CollectionUtils.compare(m1, m2)).isLessThan(0);
    assertThat(CollectionUtils.compare(m2, m1)).isGreaterThan(0);
  }

  /**
   * The comparator contract over a handful of maps: antisymmetry and transitivity of the induced order, which is what a
   * sorted index relies on to place the same entries in the same order on every build.
   */
  @Test
  void compareMapsSatisfiesComparatorContract() {
    final List<Map<String, Comparable>> maps = new ArrayList<>();
    maps.add(new HashMap<>(Map.of("a", 1)));
    maps.add(new HashMap<>(Map.of("b", 1)));
    maps.add(new HashMap<>(Map.of("a", 1, "b", 1)));
    maps.add(new HashMap<>(Map.of("a", 2)));
    maps.add(new HashMap<>(Map.of("c", "x")));
    maps.add(new HashMap<>());
    final Map<String, Comparable> withNull = new HashMap<>();
    withNull.put("a", null);
    maps.add(withNull);
    final Map<String, Comparable> withNullAndMore = new HashMap<>();
    withNullAndMore.put("a", null);
    withNullAndMore.put("b", 3);
    maps.add(withNullAndMore);

    for (final Map<String, Comparable> x : maps) {
      assertThat(CollectionUtils.compare(x, x)).isZero();
      for (final Map<String, Comparable> y : maps) {
        final int xy = Integer.signum(CollectionUtils.compare(x, y));
        assertThat(xy).as("%s vs %s", x, y).isEqualTo(-Integer.signum(CollectionUtils.compare(y, x)));
        if (x != y)
          assertThat(xy).as("%s vs %s must not be equal", x, y).isNotZero();
        for (final Map<String, Comparable> z : maps) {
          final int yz = Integer.signum(CollectionUtils.compare(y, z));
          if (xy <= 0 && yz <= 0)
            assertThat(Integer.signum(CollectionUtils.compare(x, z))).as("%s <= %s <= %s", x, y, z).isLessThanOrEqualTo(0);
        }
      }
    }

    // TimSort's own contract check must stay quiet on every permutation of the sample
    for (int seed = 0; seed < 50; seed++) {
      final List<Map<String, Comparable>> shuffled = new ArrayList<>(maps);
      Collections.shuffle(shuffled, new Random(seed));
      shuffled.sort(CollectionUtils::compare);
      for (int i = 1; i < shuffled.size(); i++)
        assertThat(CollectionUtils.compare(shuffled.get(i - 1), shuffled.get(i))).isLessThan(0);
    }
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

  @Test
  void removeAllWithMoreElementsToRemoveThanPresent() {
    // objsToRemove is larger than the list and most elements are absent: must skip them gracefully
    // instead of throwing IllegalArgumentException for a negative initial capacity.
    final List<String> original = Arrays.asList("a", "b");
    final List<String> toRemove = Arrays.asList("a", "x", "y", "z");
    final List<String> result = CollectionUtils.removeAllFromUnmodifiableList(original, toRemove);

    assertThat(result).containsExactly("b");
  }

  @Test
  void removeAllFromEmptyList() {
    final List<String> result = CollectionUtils.removeAllFromUnmodifiableList(List.of(), Arrays.asList("a", "b"));
    assertThat(result).isEmpty();
  }

  @Test
  void removeFromEmptyList() {
    // Removing from an empty list (capacity hint would be -1) must return an empty list, not throw.
    final List<String> result = CollectionUtils.removeFromUnmodifiableList(List.of(), "a");
    assertThat(result).isEmpty();
  }

  @Test
  void removeAbsentElementKeepsListIntact() {
    final List<String> original = Arrays.asList("a", "b");
    final List<String> result = CollectionUtils.removeFromUnmodifiableList(original, "z");
    assertThat(result).containsExactly("a", "b");
  }

  /** #5773: under the cap, the retained set itself is the de-duplication - a repeat is not a first sighting. */
  @Test
  void addBoundedRetainsAndDeduplicatesUnderTheCap() {
    final Set<String> retained = new LinkedHashSet<>();

    assertThat(CollectionUtils.addBounded(retained, 3, "a")).isEqualTo(CollectionUtils.BoundedAdd.RETAINED);
    assertThat(CollectionUtils.addBounded(retained, 3, "b")).isEqualTo(CollectionUtils.BoundedAdd.RETAINED);
    assertThat(CollectionUtils.addBounded(retained, 3, "a")).as("a repeat is not a first sighting")
        .isEqualTo(CollectionUtils.BoundedAdd.DUPLICATE);

    assertThat(retained).containsExactly("a", "b");
  }

  /** Past the cap nothing more is retained, so the collection stops growing - and says so. */
  @Test
  void addBoundedStopsRetainingAtTheCap() {
    final Set<String> retained = new LinkedHashSet<>();
    CollectionUtils.addBounded(retained, 2, "a");
    CollectionUtils.addBounded(retained, 2, "b");

    final CollectionUtils.BoundedAdd outcome = CollectionUtils.addBounded(retained, 2, "c");
    assertThat(outcome).as("unseen past the cap is a first sighting, but a dropped one")
        .isEqualTo(CollectionUtils.BoundedAdd.DROPPED);
    assertThat(outcome.isFirstSighting()).as("so a counter beside the set still ticks").isTrue();
    assertThat(retained).as("but it is not retained").containsExactly("a", "b");
  }

  /**
   * The documented degradation, pinned so it cannot silently change in either direction: past the cap an item that
   * is STILL retained is recognised (this is the case the two CHECK DATABASE counters used to disagree on), while an
   * item the cap refused to retain cannot be, so a second sighting of it counts again.
   */
  @Test
  void addBoundedRecognisesARetainedItemPastTheCapButNotADroppedOne() {
    final Set<String> retained = new LinkedHashSet<>();
    CollectionUtils.addBounded(retained, 1, "kept");

    assertThat(CollectionUtils.addBounded(retained, 1, "kept")).as("still in the set, so still recognised")
        .isEqualTo(CollectionUtils.BoundedAdd.DUPLICATE);

    assertThat(CollectionUtils.addBounded(retained, 1, "dropped")).isEqualTo(CollectionUtils.BoundedAdd.DROPPED);
    assertThat(CollectionUtils.addBounded(retained, 1, "dropped"))
        .as("never retained, so it cannot be recognised - counted again, by design")
        .isEqualTo(CollectionUtils.BoundedAdd.DROPPED);
  }

  /** A cap of zero retains nothing and therefore recognises nothing: every call is a dropped first sighting. */
  @Test
  void addBoundedWithAZeroCapRetainsNothing() {
    final Set<String> retained = new LinkedHashSet<>();

    assertThat(CollectionUtils.addBounded(retained, 0, "a")).isEqualTo(CollectionUtils.BoundedAdd.DROPPED);
    assertThat(CollectionUtils.addBounded(retained, 0, "a")).isEqualTo(CollectionUtils.BoundedAdd.DROPPED);
    assertThat(retained).isEmpty();
  }

  /** Only a duplicate is not a first sighting - the property both CHECK DATABASE counters tick on. */
  @Test
  void onlyADuplicateIsNotAFirstSighting() {
    assertThat(CollectionUtils.BoundedAdd.RETAINED.isFirstSighting()).isTrue();
    assertThat(CollectionUtils.BoundedAdd.DROPPED.isFirstSighting()).isTrue();
    assertThat(CollectionUtils.BoundedAdd.DUPLICATE.isFirstSighting()).isFalse();
  }
}
