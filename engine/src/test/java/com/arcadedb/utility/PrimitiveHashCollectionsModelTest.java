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

import com.arcadedb.database.RID;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The open-addressing collections of this package replace {@code HashMap}/{@code HashSet} on hot paths (visited sets of a graph
 * traversal, aggregation counters, the transaction's page tables), so each is held here to the JDK collection it stands for:
 * random sequences of operations over a small key domain (collisions, growth past the load factor, tombstones, a clear in the
 * middle) and over the keys at the edges of the integer range, replayed on both and compared after every read and in full at the
 * end. {@link RidIntHashMap}, {@link RidLongHashMap} and {@link LongLongHashMap} had no test of their own.
 */
@Tag("slow")
class PrimitiveHashCollectionsModelTest {
  private static final int[]  SPECIAL_INTS  = { 0, -1, 1, Integer.MAX_VALUE, Integer.MIN_VALUE + 2 };
  private static final long[] SPECIAL_LONGS = { 0, -1, 1, Long.MAX_VALUE, Long.MIN_VALUE + 3, 1L << 32, -(1L << 32), 0xFFFFFFFFL };
  private static final int[]  DOMAINS       = { 4, 20, 200, 5_000 };
  private static final int[]  CAPACITIES    = { 0, 1, 16, 1_024 };
  private static final int    ROUNDS        = 120;

  private static long longKey(final Random random, final int domain) {
    if (random.nextInt(10) == 0)
      return SPECIAL_LONGS[random.nextInt(SPECIAL_LONGS.length)];
    return random.nextInt(3) == 0 ? (long) random.nextInt(domain) << 20 : random.nextInt(domain);
  }

  private static int intKey(final Random random, final int domain) {
    return random.nextInt(10) == 0 ? SPECIAL_INTS[random.nextInt(SPECIAL_INTS.length)] : random.nextInt(domain);
  }

  private static int bucketId(final Random random, final int domain) {
    return random.nextInt(10) == 0 ? SPECIAL_INTS[random.nextInt(SPECIAL_INTS.length)] : random.nextInt(Math.min(domain, 40));
  }

  @Test
  void longLongHashMapAgreesWithHashMap() {
    for (int round = 0; round < ROUNDS; round++) {
      final Random random = new Random(11_000L + round);
      final int domain = DOMAINS[random.nextInt(DOMAINS.length)];
      final int capacity = CAPACITIES[random.nextInt(CAPACITIES.length)];
      final LongLongHashMap map = capacity == 0 ? new LongLongHashMap() : new LongLongHashMap(capacity);
      final Map<Long, Long> model = new HashMap<>();

      final int operations = 50 + random.nextInt(3_000);
      for (int i = 0; i < operations; i++) {
        final long key = longKey(random, domain);
        switch (random.nextInt(4)) {
        case 0 -> {
          final long value = random.nextLong();
          map.put(key, value);
          model.put(key, value);
        }
        case 1 -> {
          map.increment(key);
          model.merge(key, 1L, Long::sum);
        }
        case 2 -> {
          final long delta = random.nextInt(100) - 50;
          map.add(key, delta);
          model.merge(key, delta, Long::sum);
        }
        default -> assertThat(map.get(key, -77)).as("round %d get %d", round, key).isEqualTo(model.getOrDefault(key, -77L));
        }
      }
      assertThat(map.size()).as("round %d size", round).isEqualTo(model.size());
      assertThat(entriesOf(map)).as("round %d entries", round).isEqualTo(model);

      // mergeFrom sums the values of the keys both maps hold
      final LongLongHashMap other = new LongLongHashMap();
      final Map<Long, Long> otherModel = new HashMap<>();
      for (int i = 0; i < 50; i++) {
        final long key = longKey(random, domain);
        final long value = random.nextInt(1_000);
        other.put(key, value);
        otherModel.put(key, value);
      }
      map.mergeFrom(other);
      otherModel.forEach((key, value) -> model.merge(key, value, Long::sum));
      assertThat(entriesOf(map)).as("round %d after mergeFrom", round).isEqualTo(model);
    }
  }

  @Test
  void longLongHashMapKeepsTheKeysAtTheEdgesOfTheRange() {
    final LongLongHashMap map = new LongLongHashMap(16);
    map.put(Long.MAX_VALUE, 1);
    map.put(Long.MIN_VALUE + 1, 2);
    map.put(0, 3);
    map.put(-1, 4);
    assertThat(map.get(Long.MAX_VALUE, 0)).isEqualTo(1);
    assertThat(map.get(Long.MIN_VALUE + 1, 0)).isEqualTo(2);
    assertThat(map.get(0, 0)).isEqualTo(3);
    assertThat(map.get(-1, 0)).isEqualTo(4);
    assertThat(map.get(5, 99)).isEqualTo(99);
    assertThat(map.size()).isEqualTo(4);
  }

  @Test
  void intIntHashMapAgreesWithHashMap() {
    for (int round = 0; round < ROUNDS; round++) {
      final Random random = new Random(12_000L + round);
      final int domain = DOMAINS[random.nextInt(DOMAINS.length)];
      final int capacity = CAPACITIES[random.nextInt(CAPACITIES.length)];
      final IntIntHashMap map = capacity == 0 ? new IntIntHashMap() : new IntIntHashMap(capacity);
      final Map<Integer, Integer> model = new HashMap<>();

      final int operations = 50 + random.nextInt(3_000);
      for (int i = 0; i < operations; i++) {
        final int key = intKey(random, domain);
        switch (random.nextInt(7)) {
        case 0 -> {
          final int value = random.nextInt();
          final int previous = map.put(key, value);
          final Integer expected = model.put(key, value);
          assertThat(previous).as("round %d put %d", round, key).isEqualTo(expected == null ? Integer.MIN_VALUE : expected);
        }
        case 1 -> {
          map.increment(key);
          model.merge(key, 1, Integer::sum);
        }
        case 2 -> {
          final int delta = random.nextInt(100) - 50;
          map.add(key, delta);
          model.merge(key, delta, Integer::sum);
        }
        case 3 -> {
          final Integer expected = model.remove(key);
          assertThat(map.remove(key, -77)).as("round %d remove %d", round, key).isEqualTo(expected == null ? -77 : expected);
        }
        case 4 -> assertThat(map.containsKey(key)).as("round %d containsKey %d", round, key).isEqualTo(model.containsKey(key));
        case 5 -> assertThat(map.get(key, -77)).as("round %d get %d", round, key).isEqualTo(model.getOrDefault(key, -77));
        default -> {
          if (random.nextInt(50) == 0) {
            map.clear();
            model.clear();
          }
        }
        }
      }
      assertThat(map.size()).as("round %d size", round).isEqualTo(model.size());
      assertThat(map.isEmpty()).isEqualTo(model.isEmpty());
      final Map<Integer, Integer> entries = new HashMap<>();
      map.forEach((key, value) -> assertThat(entries.put(key, value)).as("key %d visited twice", key).isNull());
      assertThat(entries).as("round %d entries", round).isEqualTo(model);
      if (!model.isEmpty()) {
        final int best = map.keyWithMaxValue(Integer.MIN_VALUE + 5);
        assertThat(model).containsKey(best);
        assertThat(model.get(best)).isEqualTo(model.values().stream().mapToInt(Integer::intValue).max().getAsInt());
      } else
        assertThat(map.keyWithMaxValue(-123)).isEqualTo(-123);
    }
  }

  @Test
  void reservedSentinelKeysAreRefusedNotStored() {
    assertThatThrownBy(() -> new IntIntHashMap().put(Integer.MIN_VALUE, 1)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new IntIntHashMap().put(Integer.MIN_VALUE + 1, 1)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new IntHashSet().add(Integer.MIN_VALUE)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new LongHashSet().add(Long.MIN_VALUE)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new LongObjectHashMap<String>().put(Long.MIN_VALUE, "x")).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void longObjectHashMapAgreesWithHashMap() {
    for (int round = 0; round < ROUNDS; round++) {
      final Random random = new Random(13_000L + round);
      final int domain = DOMAINS[random.nextInt(DOMAINS.length)];
      final int capacity = CAPACITIES[random.nextInt(CAPACITIES.length)];
      final LongObjectHashMap<String> map = capacity == 0 ? new LongObjectHashMap<>() : new LongObjectHashMap<>(capacity);
      final Map<Long, String> model = new HashMap<>();

      final int operations = 50 + random.nextInt(3_000);
      for (int i = 0; i < operations; i++) {
        final long key = longKey(random, domain);
        switch (random.nextInt(6)) {
        case 0 -> {
          final String value = "v" + random.nextInt();
          assertThat(map.put(key, value)).as("round %d put %d", round, key).isEqualTo(model.put(key, value));
        }
        case 1 -> assertThat(map.remove(key)).as("round %d remove %d", round, key).isEqualTo(model.remove(key));
        case 2 -> assertThat(map.containsKey(key)).as("round %d containsKey %d", round, key).isEqualTo(model.containsKey(key));
        case 3 -> assertThat(map.get(key)).as("round %d get %d", round, key).isEqualTo(model.get(key));
        case 4 -> {
          if (random.nextInt(80) == 0) {
            map.clear();
            model.clear();
          }
        }
        default -> assertThat(map.size()).isEqualTo(model.size());
        }
      }
      assertThat(map.size()).as("round %d size", round).isEqualTo(model.size());
      assertThat(map.isEmpty()).isEqualTo(model.isEmpty());
      final Map<Long, String> entries = new HashMap<>();
      map.forEach(entries::put);
      assertThat(entries).as("round %d entries", round).isEqualTo(model);
      final Set<Long> keys = new HashSet<>();
      for (final long key : map.keysArray())
        keys.add(key);
      assertThat(map.keysArray()).hasSize(model.size());
      assertThat(keys).isEqualTo(model.keySet());
    }
  }

  @Test
  void intHashSetAgreesWithHashSet() {
    for (int round = 0; round < ROUNDS; round++) {
      final Random random = new Random(14_000L + round);
      final int domain = DOMAINS[random.nextInt(DOMAINS.length)];
      final int capacity = CAPACITIES[random.nextInt(CAPACITIES.length)];
      final IntHashSet set = capacity == 0 ? new IntHashSet() : new IntHashSet(capacity);
      final Set<Integer> model = new HashSet<>();

      final int operations = 50 + random.nextInt(3_000);
      for (int i = 0; i < operations; i++) {
        final int value = intKey(random, domain);
        if (random.nextInt(3) > 0)
          assertThat(set.add(value)).as("round %d add %d", round, value).isEqualTo(model.add(value));
        else
          assertThat(set.contains(value)).as("round %d contains %d", round, value).isEqualTo(model.contains(value));
        if (random.nextInt(500) == 0) {
          set.clear();
          model.clear();
        }
      }
      assertThat(set.size()).as("round %d size", round).isEqualTo(model.size());
      assertThat(set.isEmpty()).isEqualTo(model.isEmpty());
      final Set<Integer> visited = new HashSet<>();
      set.forEach(visited::add);
      assertThat(visited).isEqualTo(model);
      final Set<Integer> array = new HashSet<>();
      for (final int value : set.toArray())
        array.add(value);
      assertThat(set.toArray()).hasSize(model.size());
      assertThat(array).isEqualTo(model);
    }
  }

  @Test
  void longHashSetAgreesWithHashSet() {
    for (int round = 0; round < ROUNDS; round++) {
      final Random random = new Random(15_000L + round);
      final int domain = DOMAINS[random.nextInt(DOMAINS.length)];
      final int capacity = CAPACITIES[random.nextInt(CAPACITIES.length)];
      final LongHashSet set = capacity == 0 ? new LongHashSet() : new LongHashSet(capacity);
      final Set<Long> model = new HashSet<>();

      final int operations = 50 + random.nextInt(3_000);
      for (int i = 0; i < operations; i++) {
        final long value = longKey(random, domain);
        if (random.nextInt(3) > 0)
          assertThat(set.add(value)).as("round %d add %d", round, value).isEqualTo(model.add(value));
        else
          assertThat(set.contains(value)).as("round %d contains %d", round, value).isEqualTo(model.contains(value));
        if (random.nextInt(500) == 0) {
          set.clear();
          model.clear();
        }
      }
      assertThat(set.size()).as("round %d size", round).isEqualTo(model.size());
      assertThat(set.isEmpty()).isEqualTo(model.isEmpty());
      final Set<Long> visited = new HashSet<>();
      set.forEach(visited::add);
      assertThat(visited).isEqualTo(model);
      final Set<Long> array = new HashSet<>();
      for (final long value : set.toArray())
        array.add(value);
      assertThat(set.toArray()).hasSize(model.size());
      assertThat(array).isEqualTo(model);
    }
  }

  @Test
  void ridHashSetAgreesWithHashSet() {
    for (int round = 0; round < ROUNDS; round++) {
      final Random random = new Random(16_000L + round);
      final int domain = DOMAINS[random.nextInt(DOMAINS.length)];
      final int capacity = CAPACITIES[random.nextInt(CAPACITIES.length)];
      final RidHashSet set = capacity == 0 ? new RidHashSet() : new RidHashSet(capacity);
      final Set<String> model = new HashSet<>();

      final int operations = 50 + random.nextInt(3_000);
      for (int i = 0; i < operations; i++) {
        final int bucket = bucketId(random, domain);
        final long offset = longKey(random, domain);
        final String key = bucket + ":" + offset;
        switch (random.nextInt(4)) {
        case 0, 1 -> assertThat(set.add(bucket, offset)).as("round %d add %s", round, key).isEqualTo(model.add(key));
        case 2 -> assertThat(set.contains(bucket, offset)).as("round %d contains %s", round, key).isEqualTo(model.contains(key));
        default -> assertThat(set.contains(new RID(bucket, offset))).as("round %d contains(rid) %s", round, key)
            .isEqualTo(model.contains(key));
        }
        if (random.nextInt(500) == 0) {
          set.clear();
          model.clear();
        }
      }
      assertThat(set.size()).as("round %d size", round).isEqualTo(model.size());
      assertThat(set.isEmpty()).isEqualTo(model.isEmpty());
      final Set<String> visited = new HashSet<>();
      set.forEach((bucket, offset) -> visited.add(bucket + ":" + offset));
      assertThat(visited).isEqualTo(model);
    }
  }

  @Test
  void ridIntHashMapAgreesWithHashMap() {
    for (int round = 0; round < ROUNDS; round++) {
      final Random random = new Random(17_000L + round);
      final int domain = DOMAINS[random.nextInt(DOMAINS.length)];
      final int capacity = CAPACITIES[random.nextInt(CAPACITIES.length)];
      final RidIntHashMap map = capacity == 0 ? new RidIntHashMap() : new RidIntHashMap(capacity);
      final Map<String, Integer> model = new HashMap<>();

      final int operations = 50 + random.nextInt(3_000);
      for (int i = 0; i < operations; i++) {
        final int bucket = bucketId(random, domain);
        final long offset = longKey(random, domain);
        final String key = bucket + ":" + offset;
        switch (random.nextInt(6)) {
        case 0 -> {
          final int value = random.nextInt();
          final int previous = map.put(bucket, offset, value);
          final Integer expected = model.put(key, value);
          assertThat(previous).as("round %d put %s", round, key).isEqualTo(expected == null ? Integer.MIN_VALUE : expected);
        }
        case 1 -> {
          final int delta = random.nextInt(50) - 25;
          map.add(bucket, offset, delta);
          model.merge(key, delta, Integer::sum);
        }
        case 2 -> {
          final int delta = random.nextInt(50) - 25;
          map.add(new RID(bucket, offset), delta);
          model.merge(key, delta, Integer::sum);
        }
        case 3 -> assertThat(map.containsKey(bucket, offset)).as("round %d containsKey %s", round, key)
            .isEqualTo(model.containsKey(key));
        case 4 -> assertThat(map.get(bucket, offset, -77)).as("round %d get %s", round, key).isEqualTo(model.getOrDefault(key, -77));
        default -> {
          final RID rid = new RID(bucket, offset);
          assertThat(map.get(rid, -5)).as("round %d get(rid) %s", round, key).isEqualTo(model.getOrDefault(key, -5));
          assertThat(map.containsKey(rid)).isEqualTo(model.containsKey(key));
        }
        }
      }
      assertThat(map.size()).as("round %d size", round).isEqualTo(model.size());
      assertThat(map.isEmpty()).isEqualTo(model.isEmpty());
      final Map<String, Integer> entries = new HashMap<>();
      map.forEach((bucket, offset, value) -> entries.put(bucket + ":" + offset, value));
      assertThat(entries).as("round %d entries", round).isEqualTo(model);
    }
  }

  @Test
  void ridLongHashMapAgreesWithHashMap() {
    for (int round = 0; round < ROUNDS; round++) {
      final Random random = new Random(18_000L + round);
      final int domain = DOMAINS[random.nextInt(DOMAINS.length)];
      final int capacity = CAPACITIES[random.nextInt(CAPACITIES.length)];
      final RidLongHashMap map = capacity == 0 ? new RidLongHashMap() : new RidLongHashMap(capacity);
      final Map<String, Long> model = new HashMap<>();

      final int operations = 50 + random.nextInt(3_000);
      for (int i = 0; i < operations; i++) {
        final int bucket = bucketId(random, domain);
        final long offset = longKey(random, domain);
        final String key = bucket + ":" + offset;
        switch (random.nextInt(6)) {
        case 0 -> {
          final long value = random.nextLong();
          final long previous = map.put(bucket, offset, value);
          final Long expected = model.put(key, value);
          assertThat(previous).as("round %d put %s", round, key).isEqualTo(expected == null ? Long.MIN_VALUE : expected);
        }
        case 1 -> {
          final long delta = random.nextInt(50) - 25;
          map.add(bucket, offset, delta);
          model.merge(key, delta, Long::sum);
        }
        case 2 -> {
          final long delta = random.nextInt(50) - 25;
          map.add(new RID(bucket, offset), delta);
          model.merge(key, delta, Long::sum);
        }
        case 3 -> assertThat(map.containsKey(bucket, offset)).as("round %d containsKey %s", round, key)
            .isEqualTo(model.containsKey(key));
        case 4 -> assertThat(map.get(bucket, offset, -77)).as("round %d get %s", round, key).isEqualTo(model.getOrDefault(key, -77L));
        default -> assertThat(map.get(new RID(bucket, offset), -5)).as("round %d get(rid) %s", round, key)
            .isEqualTo(model.getOrDefault(key, -5L));
        }
      }
      assertThat(map.size()).as("round %d size", round).isEqualTo(model.size());
      final Map<String, Long> entries = new HashMap<>();
      map.forEach((bucket, offset, value) -> entries.put(bucket + ":" + offset, value));
      assertThat(entries).as("round %d entries", round).isEqualTo(model);

      // mergeFrom sums the values of the keys both maps hold
      final RidLongHashMap other = new RidLongHashMap();
      final Map<String, Long> otherModel = new HashMap<>();
      for (int i = 0; i < 40; i++) {
        final int bucket = bucketId(random, domain);
        final long offset = longKey(random, domain);
        final long value = random.nextInt(100);
        other.put(bucket, offset, value);
        otherModel.put(bucket + ":" + offset, value);
      }
      map.mergeFrom(other);
      otherModel.forEach((key, value) -> model.merge(key, value, Long::sum));
      final Map<String, Long> merged = new HashMap<>();
      map.forEach((bucket, offset, value) -> merged.put(bucket + ":" + offset, value));
      assertThat(merged).as("round %d after mergeFrom", round).isEqualTo(model);
    }
  }

  @Test
  void stringRidHashMapAgreesWithHashMap() {
    for (int round = 0; round < ROUNDS; round++) {
      final Random random = new Random(19_000L + round);
      final int domain = DOMAINS[random.nextInt(DOMAINS.length)];
      final int capacity = CAPACITIES[random.nextInt(CAPACITIES.length)];
      final StringRidHashMap map = capacity == 0 ? new StringRidHashMap() : new StringRidHashMap(capacity);
      final Map<String, String> model = new HashMap<>();

      final int operations = 50 + random.nextInt(3_000);
      for (int i = 0; i < operations; i++) {
        // the empty key, ASCII, a Latin-1 letter and a surrogate pair: the keys are UTF-8 encoded into a reusable buffer
        final String key = random.nextInt(8) == 0 ? "" : "k" + random.nextInt(domain) + (random.nextBoolean() ? "\u00e9\uD83D\uDE00" : "");
        final int bucket = random.nextInt(30);
        final long position = random.nextInt(100_000);
        switch (random.nextInt(4)) {
        case 0 -> {
          map.put(key, bucket, position);
          model.put(key, bucket + ":" + position);
        }
        case 1 -> {
          final boolean expected = !model.containsKey(key);
          if (expected)
            model.put(key, bucket + ":" + position);
          assertThat(map.putIfAbsent(key, bucket, position)).as("round %d putIfAbsent %s", round, key).isEqualTo(expected);
        }
        default -> {
          final RID rid = map.get(key);
          final String expected = model.get(key);
          assertThat(rid == null ? null : rid.getBucketId() + ":" + rid.getPosition()).as("round %d get %s", round, key)
              .isEqualTo(expected);
        }
        }
      }
      assertThat(map.size()).as("round %d size", round).isEqualTo(model.size());
      assertThat(map.isEmpty()).isEqualTo(model.isEmpty());
      final Map<String, String> entries = new HashMap<>();
      map.forEach((key, rid) -> entries.put(key, rid.getBucketId() + ":" + rid.getPosition()));
      assertThat(entries).as("round %d entries", round).isEqualTo(model);
    }
  }

  private static Map<Long, Long> entriesOf(final LongLongHashMap map) {
    final Map<Long, Long> entries = new HashMap<>();
    map.forEach((key, value) -> assertThat(entries.put(key, value)).as("key %d visited twice", key).isNull());
    return entries;
  }
}
