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
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
package com.arcadedb.utility;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LongObjectHashMapTest {

  @Test
  void emptyMap() {
    final LongObjectHashMap<String> m = new LongObjectHashMap<>();
    assertThat(m.size()).isZero();
    assertThat(m.isEmpty()).isTrue();
    assertThat(m.get(42L)).isNull();
    assertThat(m.containsKey(42L)).isFalse();
  }

  @Test
  void putAndGetSingle() {
    final LongObjectHashMap<String> m = new LongObjectHashMap<>();
    assertThat(m.put(42L, "hello")).isNull();
    assertThat(m.get(42L)).isEqualTo("hello");
    assertThat(m.containsKey(42L)).isTrue();
    assertThat(m.size()).isEqualTo(1);
  }

  @Test
  void putReplacesAndReturnsPrevious() {
    final LongObjectHashMap<String> m = new LongObjectHashMap<>();
    m.put(42L, "first");
    assertThat(m.put(42L, "second")).isEqualTo("first");
    assertThat(m.get(42L)).isEqualTo("second");
    assertThat(m.size()).isEqualTo(1);
  }

  @Test
  void resizesAndKeepsAllEntries() {
    final LongObjectHashMap<Integer> m = new LongObjectHashMap<>(16);
    for (int i = 0; i < 10_000; i++)
      assertThat(m.put((long) i, i * 7)).isNull();
    assertThat(m.size()).isEqualTo(10_000);
    for (int i = 0; i < 10_000; i++)
      assertThat(m.get((long) i)).isEqualTo(i * 7);
    assertThat(m.get(10_000L)).isNull();
  }

  @Test
  void clearResetsSizeAndReleasesValueRefs() {
    final LongObjectHashMap<Object> m = new LongObjectHashMap<>();
    final Object marker = new Object();
    for (long i = 0; i < 100; i++)
      m.put(i, marker);
    m.clear();
    assertThat(m.size()).isZero();
    for (long i = 0; i < 100; i++)
      assertThat(m.get(i)).isNull();
  }

  @Test
  void rejectsSentinelKey() {
    final LongObjectHashMap<String> m = new LongObjectHashMap<>();
    assertThatThrownBy(() -> m.put(Long.MIN_VALUE, "x"))
        .isInstanceOf(IllegalArgumentException.class);
    assertThat(m.get(Long.MIN_VALUE)).isNull();
    assertThat(m.containsKey(Long.MIN_VALUE)).isFalse();
  }

  @Test
  void supportsNullValues() {
    final LongObjectHashMap<String> m = new LongObjectHashMap<>();
    assertThat(m.put(1L, null)).isNull();
    assertThat(m.get(1L)).isNull();
    // containsKey distinguishes "absent" from "present with null"
    assertThat(m.containsKey(1L)).isTrue();
    assertThat(m.containsKey(2L)).isFalse();
  }

  @Test
  void handlesNegativeKeysAndExtremes() {
    final LongObjectHashMap<String> m = new LongObjectHashMap<>();
    m.put(-1L, "neg");
    m.put(Long.MIN_VALUE + 1, "near-min");
    m.put(Long.MAX_VALUE, "max");
    assertThat(m.get(-1L)).isEqualTo("neg");
    assertThat(m.get(Long.MIN_VALUE + 1)).isEqualTo("near-min");
    assertThat(m.get(Long.MAX_VALUE)).isEqualTo("max");
    assertThat(m.size()).isEqualTo(3);
  }

  @Test
  void forEachVisitsAllEntries() {
    final LongObjectHashMap<Integer> m = new LongObjectHashMap<>();
    for (int i = 0; i < 100; i++)
      m.put((long) (i * 7), i);

    final HashMap<Long, Integer> seen = new HashMap<>();
    m.forEach(seen::put);
    assertThat(seen).hasSize(100);
    for (int i = 0; i < 100; i++)
      assertThat(seen).containsEntry((long) (i * 7), i);
  }

  @Test
  void keysArrayReturnsAllKeys() {
    final LongObjectHashMap<String> m = new LongObjectHashMap<>();
    for (long i = 0; i < 50; i++)
      m.put(i, "v" + i);
    final long[] arr = m.keysArray();
    assertThat(arr).hasSize(50);
    final HashSet<Long> seen = new HashSet<>();
    for (final long k : arr)
      seen.add(k);
    assertThat(seen).hasSize(50);
  }

  @Test
  void removeAbsentKeyReturnsNullAndLeavesMapUnchanged() {
    final LongObjectHashMap<String> m = new LongObjectHashMap<>();
    m.put(1L, "one");
    assertThat(m.remove(2L)).isNull();
    assertThat(m.size()).isEqualTo(1);
    assertThat(m.get(1L)).isEqualTo("one");
  }

  @Test
  void removePresentKeyReturnsPreviousValueAndForgetsIt() {
    final LongObjectHashMap<String> m = new LongObjectHashMap<>();
    m.put(42L, "hello");
    assertThat(m.remove(42L)).isEqualTo("hello");
    assertThat(m.get(42L)).isNull();
    assertThat(m.containsKey(42L)).isFalse();
    assertThat(m.size()).isZero();
    assertThat(m.isEmpty()).isTrue();
  }

  @Test
  void removeDoesNotBreakProbeChainForLaterKeys() {
    // Force several keys to collide into the same initial bucket by using a tiny capacity, then
    // remove the FIRST one inserted and confirm the others (which had to probe past it) are still
    // reachable - proves the tombstone doesn't terminate the probe chain like an empty slot would.
    final LongObjectHashMap<Integer> m = new LongObjectHashMap<>(16);
    final long[] keys = new long[10];
    for (int i = 0; i < keys.length; i++) {
      keys[i] = i * 16L; // all hash to slot 0 mod 16 before probing
      m.put(keys[i], i);
    }
    assertThat(m.remove(keys[0])).isEqualTo(0);
    for (int i = 1; i < keys.length; i++)
      assertThat(m.get(keys[i])).as("key %d must still be reachable after removing an earlier colliding key", i).isEqualTo(i);
    assertThat(m.get(keys[0])).isNull();
    assertThat(m.size()).isEqualTo(keys.length - 1);
  }

  @Test
  void putAfterRemoveRevivesTheSlotAndRestoresSize() {
    final LongObjectHashMap<String> m = new LongObjectHashMap<>();
    m.put(42L, "first");
    m.remove(42L);
    assertThat(m.put(42L, "revived")).isNull(); // was absent (tombstoned), not "first"
    assertThat(m.get(42L)).isEqualTo("revived");
    assertThat(m.size()).isEqualTo(1);
  }

  @Test
  void forEachAndKeysArraySkipTombstones() {
    final LongObjectHashMap<Integer> m = new LongObjectHashMap<>();
    for (int i = 0; i < 20; i++)
      m.put((long) i, i);
    for (int i = 0; i < 20; i += 2)
      m.remove((long) i);

    final HashMap<Long, Integer> seen = new HashMap<>();
    m.forEach(seen::put);
    assertThat(seen).hasSize(10);
    for (int i = 1; i < 20; i += 2)
      assertThat(seen).containsEntry((long) i, i);

    assertThat(m.keysArray()).hasSize(10);
  }

  @Test
  void resizeReclaimsTombstonedSlots() {
    final LongObjectHashMap<Integer> m = new LongObjectHashMap<>(16);
    for (int i = 0; i < 1_000; i++)
      m.put((long) i, i);
    for (int i = 0; i < 900; i++)
      m.remove((long) i);
    // Triggers further growth/resizing while tombstones are present; must not resurrect removed keys
    // nor lose live ones.
    for (int i = 1_000; i < 5_000; i++)
      m.put((long) i, i);

    assertThat(m.size()).isEqualTo(100 + 4_000);
    for (int i = 0; i < 900; i++)
      assertThat(m.get((long) i)).isNull();
    for (int i = 900; i < 5_000; i++)
      assertThat(m.get((long) i)).isEqualTo(i);
  }

  @Test
  void equivalentToHashMapUnderRandomLoad() {
    final LongObjectHashMap<String> a = new LongObjectHashMap<>();
    final HashMap<Long, String> b = new HashMap<>();
    final ThreadLocalRandom rnd = ThreadLocalRandom.current();
    for (int i = 0; i < 10_000; i++) {
      final long k = rnd.nextLong(Long.MIN_VALUE + 1, Long.MAX_VALUE);
      final String v = "v" + i;
      assertThat(a.put(k, v)).isEqualTo(b.put(k, v));
    }
    assertThat(a.size()).isEqualTo(b.size());
    for (final var e : b.entrySet())
      assertThat(a.get(e.getKey())).isEqualTo(e.getValue());
  }
}
