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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IteratorTest {

  @Test
  void iterableObjectWithSingleValue() {
    final IterableObject<String> iterable = new IterableObject<>("hello");

    final List<String> result = new ArrayList<>();
    iterable.forEach(result::add);

    assertThat(result).containsExactly("hello");
  }

  @Test
  void iterableObjectIterator() {
    final IterableObject<Integer> iterable = new IterableObject<>(42);
    final Iterator<Integer> iterator = iterable.iterator();

    assertThat(iterator.hasNext()).isTrue();
    assertThat(iterator.next()).isEqualTo(42);
    assertThat(iterator.hasNext()).isFalse();
  }

  @Test
  void iterableObjectIteratorThrowsOnExhausted() {
    final IterableObject<String> iterable = new IterableObject<>("value");
    final Iterator<String> iterator = iterable.iterator();

    iterator.next(); // Consume the single element

    assertThatThrownBy(iterator::next)
        .isInstanceOf(NoSuchElementException.class);
  }

  @Test
  void iterableObjectArrayWithObjectArray() {
    final String[] array = {"a", "b", "c"};
    final IterableObjectArray<String> iterable = new IterableObjectArray<>(array);

    final List<String> result = new ArrayList<>();
    for (final String s : iterable) {
      result.add(s);
    }

    assertThat(result).containsExactly("a", "b", "c");
  }

  @Test
  void iterableObjectArrayWithIntArray() {
    final int[] array = {1, 2, 3};
    final IterableObjectArray<Integer> iterable = new IterableObjectArray<>(array);

    final List<Integer> result = new ArrayList<>();
    for (final Integer i : iterable) {
      result.add(i);
    }

    assertThat(result).containsExactly(1, 2, 3);
  }

  @Test
  void iterableObjectArrayIterator() {
    final Double[] array = {1.0, 2.0};
    final IterableObjectArray<Double> iterable = new IterableObjectArray<>(array);
    final Iterator<Double> iterator = iterable.iterator();

    assertThat(iterator.hasNext()).isTrue();
    assertThat(iterator.next()).isEqualTo(1.0);
    assertThat(iterator.hasNext()).isTrue();
    assertThat(iterator.next()).isEqualTo(2.0);
    assertThat(iterator.hasNext()).isFalse();
  }

  @Test
  void multiIteratorWithSingleSource() {
    final MultiIterator<String> multi = new MultiIterator<>();
    multi.addIterator(Arrays.asList("a", "b", "c"));

    final List<String> result = new ArrayList<>();
    while (multi.hasNext()) {
      result.add(multi.next());
    }

    assertThat(result).containsExactly("a", "b", "c");
  }

  @Test
  void multiIteratorWithMultipleSources() {
    final MultiIterator<Integer> multi = new MultiIterator<>();
    multi.addIterator(Arrays.asList(1, 2));
    multi.addIterator(Arrays.asList(3, 4));
    multi.addIterator(Arrays.asList(5, 6));

    final List<Integer> result = new ArrayList<>();
    while (multi.hasNext()) {
      result.add(multi.next());
    }

    assertThat(result).containsExactly(1, 2, 3, 4, 5, 6);
  }

  @Test
  void multiIteratorWithSkip() {
    final MultiIterator<String> multi = new MultiIterator<>();
    multi.addIterator(Arrays.asList("a", "b", "c", "d", "e"));
    multi.setSkip(2);

    final List<String> result = new ArrayList<>();
    while (multi.hasNext()) {
      result.add(multi.next());
    }

    assertThat(result).containsExactly("c", "d", "e");
  }

  @Test
  void multiIteratorWithLimit() {
    final MultiIterator<String> multi = new MultiIterator<>();
    multi.addIterator(Arrays.asList("a", "b", "c", "d", "e"));
    multi.setLimit(3);

    final List<String> result = new ArrayList<>();
    while (multi.hasNext()) {
      result.add(multi.next());
    }

    assertThat(result).containsExactly("a", "b", "c");
  }

  @Test
  void multiIteratorWithSkipAndLimit() {
    final MultiIterator<String> multi = new MultiIterator<>();
    multi.addIterator(Arrays.asList("a", "b", "c", "d", "e"));
    multi.setSkip(1);
    multi.setLimit(2);

    final List<String> result = new ArrayList<>();
    while (multi.hasNext()) {
      result.add(multi.next());
    }

    assertThat(result).containsExactly("b", "c");
  }

  @Test
  void multiIteratorCountEntries() {
    final MultiIterator<String> multi = new MultiIterator<>();
    multi.addIterator(Arrays.asList("a", "b"));
    multi.addIterator(Arrays.asList("c", "d", "e"));

    assertThat(multi.countEntries()).isEqualTo(5);
  }

  @Test
  void multiIteratorGetBrowsed() {
    final MultiIterator<String> multi = new MultiIterator<>();
    multi.addIterator(Arrays.asList("a", "b", "c"));

    multi.next();
    assertThat(multi.getBrowsed()).isEqualTo(1);

    multi.next();
    assertThat(multi.getBrowsed()).isEqualTo(2);
  }

  @Test
  void multiIteratorWithIteratorSource() {
    final MultiIterator<String> multi = new MultiIterator<>();
    multi.addIterator(Arrays.asList("a", "b").iterator());
    multi.addIterator(Arrays.asList("c", "d").iterator());

    final List<String> result = new ArrayList<>();
    while (multi.hasNext()) {
      result.add(multi.next());
    }

    assertThat(result).containsExactly("a", "b", "c", "d");
  }

  @Test
  void multiIteratorReset() {
    final MultiIterator<String> multi = new MultiIterator<>();
    multi.addIterator(Arrays.asList("a", "b", "c"));

    // Consume all
    while (multi.hasNext()) {
      multi.next();
    }

    multi.reset();

    // Should be able to iterate again
    final List<String> result = new ArrayList<>();
    while (multi.hasNext()) {
      result.add(multi.next());
    }

    assertThat(result).containsExactly("a", "b", "c");
  }

  @Test
  void lruCacheBasicOperations() {
    // LRUCache evicts when size >= cacheSize, so with size 3, it holds max 2 elements
    final LRUCache<String, Integer> cache = new LRUCache<>(4);

    cache.put("a", 1);
    cache.put("b", 2);
    cache.put("c", 3);

    assertThat(cache.get("a")).isEqualTo(1);
    assertThat(cache.get("b")).isEqualTo(2);
    assertThat(cache.get("c")).isEqualTo(3);
  }

  @Test
  void lruCacheEviction() {
    // LRUCache with size 3 evicts when size reaches 3
    // So it effectively holds at most 2 entries
    final LRUCache<String, Integer> cache = new LRUCache<>(3);

    cache.put("a", 1);
    cache.put("b", 2);
    // At this point, adding "c" would make size 3, so eldest ("a") is evicted
    cache.put("c", 3);

    assertThat(cache.containsKey("a")).isFalse();
    assertThat(cache.containsKey("b")).isTrue();
    assertThat(cache.containsKey("c")).isTrue();
  }

  @Test
  void lruCacheAccessOrder() {
    // LinkedHashMap with accessOrder=true reorders on get
    final LRUCache<String, Integer> cache = new LRUCache<>(3);

    cache.put("a", 1);
    cache.put("b", 2);

    // Access "a" to make it more recently used
    cache.get("a");

    // Adding "c" evicts "b" (the oldest in access order)
    cache.put("c", 3);

    assertThat(cache.containsKey("a")).isTrue();
    assertThat(cache.containsKey("b")).isFalse();
    assertThat(cache.containsKey("c")).isTrue();
  }
}
