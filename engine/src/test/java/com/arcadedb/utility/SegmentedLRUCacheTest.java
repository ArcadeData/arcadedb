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

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SegmentedLRUCacheTest {

  @Test
  void hotEntriesSurviveAScanOfOneOffKeys() {
    final SegmentedLRUCache<String, Integer> cache = new SegmentedLRUCache<>(100);
    cache.put("hot", 1);
    assertThat(cache.get("hot")).isEqualTo(1);
    for (int i = 0; i < 10_000; i++)
      cache.put("scan" + i, i);

    assertThat(cache.get("hot")).isEqualTo(1);
    assertThat(cache.size()).isEqualTo(100);
  }

  @Test
  void neverExceedsCapacityAndEvictsOldestOneOff() {
    final SegmentedLRUCache<Integer, Integer> cache = new SegmentedLRUCache<>(3);
    for (int i = 0; i < 5; i++)
      cache.put(i, i);
    assertThat(cache.size()).isEqualTo(3);
    assertThat(cache.containsKey(0)).isFalse();
    assertThat(cache.containsKey(4)).isTrue();
  }

  @Test
  void protectedOverflowDemotesInsteadOfLosingEntries() {
    final SegmentedLRUCache<Integer, Integer> cache = new SegmentedLRUCache<>(10);
    for (int i = 0; i < 10; i++) {
      cache.put(i, i);
      cache.get(i);
    }
    assertThat(cache.size()).isEqualTo(10);
    for (int i = 0; i < 10; i++)
      assertThat(cache.get(i)).isEqualTo(i);
  }

  @Test
  void tinyCapacitiesWork() {
    final SegmentedLRUCache<String, String> one = new SegmentedLRUCache<>(1);
    one.put("a", "a");
    one.put("b", "b");
    assertThat(one.size()).isEqualTo(1);
    assertThat(one.get("b")).isEqualTo("b");
    assertThat(one.get("a")).isNull();
  }

  @Test
  void removeAndClear() {
    final SegmentedLRUCache<String, String> cache = new SegmentedLRUCache<>(10);
    cache.put("a", "1");
    cache.get("a");
    cache.put("b", "2");
    assertThat(cache.remove("a")).isEqualTo("1");
    assertThat(cache.remove("b")).isEqualTo("2");
    cache.put("c", "3");
    cache.clear();
    assertThat(cache.size()).isZero();
  }

  @Test
  void zeroCapacityCachesNothing() {
    final SegmentedLRUCache<String, String> cache = new SegmentedLRUCache<>(0);
    cache.put("a", "1");
    assertThat(cache.size()).isZero();
    assertThat(cache.get("a")).isNull();
  }
}
