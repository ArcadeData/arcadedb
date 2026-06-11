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

import static org.assertj.core.api.Assertions.assertThat;

class LRUCacheTest {

  /**
   * removeEldestEntry is called after the new entry has been inserted, so the
   * eviction threshold must be size() &gt; cacheSize; with &gt;= the cache held
   * only cacheSize - 1 entries (issue #4559).
   */
  @Test
  void shouldHoldConfiguredNumberOfEntries() {
    final LRUCache<Integer, String> cache = new LRUCache<>(3);
    cache.put(1, "one");
    cache.put(2, "two");
    cache.put(3, "three");

    assertThat(cache).hasSize(3);
    assertThat(cache.keySet()).containsExactlyInAnyOrder(1, 2, 3);
  }

  @Test
  void shouldEvictEldestOnlyBeyondCapacity() {
    final LRUCache<Integer, String> cache = new LRUCache<>(3);
    cache.put(1, "one");
    cache.put(2, "two");
    cache.put(3, "three");
    cache.put(4, "four");

    assertThat(cache).hasSize(3);
    assertThat(cache.keySet()).containsExactlyInAnyOrder(2, 3, 4);
  }

  @Test
  void shouldEvictLeastRecentlyAccessedEntry() {
    final LRUCache<Integer, String> cache = new LRUCache<>(3);
    cache.put(1, "one");
    cache.put(2, "two");
    cache.put(3, "three");

    cache.get(1); // 2 becomes the least recently used
    cache.put(4, "four");

    assertThat(cache.keySet()).containsExactlyInAnyOrder(1, 3, 4);
  }

  @Test
  void shouldKeepSingleEntryWithCapacityOne() {
    final LRUCache<Integer, String> cache = new LRUCache<>(1);
    cache.put(1, "one");

    assertThat(cache).hasSize(1);
    assertThat(cache.get(1)).isEqualTo("one");

    cache.put(2, "two");
    assertThat(cache).hasSize(1);
    assertThat(cache.get(2)).isEqualTo("two");
  }
}
