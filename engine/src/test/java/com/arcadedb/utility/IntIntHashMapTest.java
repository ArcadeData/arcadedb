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

class IntIntHashMapTest {

  @Test
  void emptyMap() {
    final IntIntHashMap m = new IntIntHashMap();
    assertThat(m.size()).isZero();
    assertThat(m.isEmpty()).isTrue();
    assertThat(m.get(42, -1)).isEqualTo(-1);
    assertThat(m.containsKey(42)).isFalse();
  }

  @Test
  void putAndGet() {
    final IntIntHashMap m = new IntIntHashMap();
    assertThat(m.put(42, 100)).isEqualTo(Integer.MIN_VALUE);
    assertThat(m.get(42, -1)).isEqualTo(100);
    assertThat(m.containsKey(42)).isTrue();
    assertThat(m.size()).isEqualTo(1);
    assertThat(m.put(42, 200)).isEqualTo(100);
    assertThat(m.get(42, -1)).isEqualTo(200);
  }

  @Test
  void addIncrementsExisting() {
    final IntIntHashMap m = new IntIntHashMap();
    m.add(7, 5);
    m.add(7, 3);
    m.add(7, 2);
    assertThat(m.get(7, 0)).isEqualTo(10);
    assertThat(m.size()).isEqualTo(1);
  }

  @Test
  void removeReturnsPreviousAndDecreasesSize() {
    final IntIntHashMap m = new IntIntHashMap();
    m.put(1, 10);
    m.put(2, 20);
    m.put(3, 30);
    assertThat(m.size()).isEqualTo(3);

    assertThat(m.remove(2, -1)).isEqualTo(20);
    assertThat(m.size()).isEqualTo(2);
    assertThat(m.containsKey(2)).isFalse();
    assertThat(m.get(2, -1)).isEqualTo(-1);

    // Surrounding keys still resolve correctly across the tombstone.
    assertThat(m.get(1, -1)).isEqualTo(10);
    assertThat(m.get(3, -1)).isEqualTo(30);
  }

  @Test
  void removeOfAbsentKeyReturnsDefault() {
    final IntIntHashMap m = new IntIntHashMap();
    m.put(1, 10);
    assertThat(m.remove(999, -1)).isEqualTo(-1);
    assertThat(m.size()).isEqualTo(1);
    assertThat(m.get(1, -1)).isEqualTo(10);
  }

  @Test
  void putReusesTombstoneSlot() {
    // Force collisions by using a small-capacity map and aligned keys.
    final IntIntHashMap m = new IntIntHashMap(16);
    for (int i = 0; i < 12; i++)
      m.put(i, i * 10);

    // Remove a few entries to create tombstones.
    m.remove(3, -1);
    m.remove(5, -1);
    m.remove(7, -1);
    assertThat(m.size()).isEqualTo(9);

    // Re-insert one of the removed keys; tombstone slot should be reused.
    m.put(5, 500);
    assertThat(m.size()).isEqualTo(10);
    assertThat(m.get(5, -1)).isEqualTo(500);

    // All other keys still resolve.
    for (int i = 0; i < 12; i++)
      if (i != 3 && i != 7)
        assertThat(m.get(i, -1)).isEqualTo(i == 5 ? 500 : i * 10);
  }

  @Test
  void getProbesAcrossTombstones() {
    final IntIntHashMap m = new IntIntHashMap();
    // Insert many entries
    for (int i = 0; i < 1000; i++)
      m.put(i, i);

    // Remove every other entry
    for (int i = 0; i < 1000; i += 2)
      m.remove(i, -1);

    // Surviving entries must still be reachable across the holes
    for (int i = 1; i < 1000; i += 2)
      assertThat(m.get(i, -1)).isEqualTo(i);

    // Removed entries must report absent
    for (int i = 0; i < 1000; i += 2)
      assertThat(m.containsKey(i)).isFalse();
  }

  @Test
  void heavyChurnDoesNotCorruptOrLeak() {
    // Repeatedly add and remove to exercise tombstone cleanup via resize.
    final IntIntHashMap m = new IntIntHashMap();
    final HashMap<Integer, Integer> ref = new HashMap<>();
    final ThreadLocalRandom rnd = ThreadLocalRandom.current();

    for (int round = 0; round < 50_000; round++) {
      final int key = rnd.nextInt(1, 5_000);
      if (rnd.nextBoolean()) {
        final int v = rnd.nextInt(1_000_000);
        m.put(key, v);
        ref.put(key, v);
      } else {
        final int rm = m.remove(key, Integer.MIN_VALUE);
        final Integer rmRef = ref.remove(key);
        if (rmRef == null)
          assertThat(rm).isEqualTo(Integer.MIN_VALUE);
        else
          assertThat(rm).isEqualTo(rmRef.intValue());
      }
    }

    assertThat(m.size()).isEqualTo(ref.size());
    for (final var e : ref.entrySet())
      assertThat(m.get(e.getKey(), Integer.MIN_VALUE)).isEqualTo(e.getValue().intValue());
  }

  @Test
  void rejectsSentinels() {
    final IntIntHashMap m = new IntIntHashMap();
    assertThatThrownBy(() -> m.put(Integer.MIN_VALUE, 1))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> m.put(Integer.MIN_VALUE + 1, 1))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> m.add(Integer.MIN_VALUE, 1))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> m.add(Integer.MIN_VALUE + 1, 1))
        .isInstanceOf(IllegalArgumentException.class);

    // Lookups of sentinels are safe and just report absent
    assertThat(m.get(Integer.MIN_VALUE, -1)).isEqualTo(-1);
    assertThat(m.get(Integer.MIN_VALUE + 1, -1)).isEqualTo(-1);
    assertThat(m.containsKey(Integer.MIN_VALUE)).isFalse();
    assertThat(m.containsKey(Integer.MIN_VALUE + 1)).isFalse();
    assertThat(m.remove(Integer.MIN_VALUE, -1)).isEqualTo(-1);
    assertThat(m.remove(Integer.MIN_VALUE + 1, -1)).isEqualTo(-1);
  }

  @Test
  void clearResetsTombstonesToo() {
    final IntIntHashMap m = new IntIntHashMap();
    for (int i = 1; i <= 100; i++)
      m.put(i, i);
    for (int i = 1; i <= 50; i++)
      m.remove(i, -1);
    m.clear();
    assertThat(m.size()).isZero();
    // Re-fill to original capacity should not trigger spurious resize from leftover tombstones.
    for (int i = 1; i <= 200; i++)
      m.put(i, i);
    assertThat(m.size()).isEqualTo(200);
  }

  @Test
  void forEachVisitsOnlyLiveEntries() {
    final IntIntHashMap m = new IntIntHashMap();
    for (int i = 0; i < 100; i++)
      m.put(i, i * 10);
    for (int i = 0; i < 100; i += 2)
      m.remove(i, -1);

    final HashSet<Integer> seen = new HashSet<>();
    m.forEach((k, v) -> {
      assertThat(v).isEqualTo(k * 10);
      seen.add(k);
    });
    assertThat(seen).hasSize(50);
    for (int i = 1; i < 100; i += 2)
      assertThat(seen).contains(i);
  }
}
