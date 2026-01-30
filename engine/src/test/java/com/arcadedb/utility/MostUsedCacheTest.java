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

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test suite for MostUsedCache.
 */
class MostUsedCacheTest {

  @Test
  void basicOperations() {
    final MostUsedCache<String, Integer> cache = new MostUsedCache<>(10);

    // Test put and get
    cache.put("key1", 1);
    cache.put("key2", 2);
    assertThat(cache.get("key1")).isEqualTo(1);
    assertThat(cache.get("key2")).isEqualTo(2);
    assertThat(cache.size()).isEqualTo(2);

    // Test update
    cache.put("key1", 10);
    assertThat(cache.get("key1")).isEqualTo(10);
    assertThat(cache.size()).isEqualTo(2);

    // Test remove
    cache.remove("key1");
    assertThat(cache.get("key1")).isNull();
    assertThat(cache.size()).isEqualTo(1);

    // Test clear
    cache.clear();
    assertThat(cache.size()).isEqualTo(0);
  }

  @Test
  void accessCounting() {
    final MostUsedCache<String, String> cache = new MostUsedCache<>(10);

    cache.put("key1", "value1");
    assertThat(cache.getAccessCount("key1")).isEqualTo(1);

    cache.get("key1");
    assertThat(cache.getAccessCount("key1")).isEqualTo(2);

    cache.get("key1");
    cache.get("key1");
    assertThat(cache.getAccessCount("key1")).isEqualTo(4);

    // Update should increment count
    cache.put("key1", "newValue");
    assertThat(cache.getAccessCount("key1")).isEqualTo(5);
  }

  @Test
  void evictionKeepsMostUsed() {
    final MostUsedCache<Integer, String> cache = new MostUsedCache<>(100);

    // Fill cache with 100 entries
    for (int i = 0; i < 100; i++) {
      cache.put(i, "value" + i);
    }
    assertThat(cache.size()).isEqualTo(100);

    // Access first 10 entries multiple times (make them "hot")
    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < 50; j++) {
        cache.get(i);
      }
    }

    // Access middle 10 entries moderately
    for (int i = 10; i < 20; i++) {
      for (int j = 0; j < 20; j++) {
        cache.get(i);
      }
    }

    // Now add one more entry to trigger eviction
    cache.put(100, "value100");

    // Cache should now have ~75 entries (75% of 100)
    final int sizeAfterEviction = cache.size();
    assertThat(sizeAfterEviction >= 75 && sizeAfterEviction <= 76).as("Expected size ~75-76 after eviction, got: " + sizeAfterEviction).isTrue();

    // The most frequently accessed entries should still be present
    for (int i = 0; i < 10; i++) {
      assertThat(cache.get(i)).as("Key " + i + " should be retained after eviction").isNotNull();
    }

    // Some of the rarely accessed entries should be gone (25% evicted)
    int removedCount = 0;
    for (int i = 50; i < 100; i++) {
      if (cache.get(i) == null) {
        removedCount++;
      }
    }
    assertThat(removedCount >= 10).as("Expected some rarely-accessed entries to be evicted, removed: " + removedCount).isTrue();
  }

  @Test
  void evictionRatio() {
    final MostUsedCache<Integer, String> cache = new MostUsedCache<>(1000);

    // Fill cache to capacity
    for (int i = 0; i < 1000; i++) {
      cache.put(i, "value" + i);
    }
    assertThat(cache.size()).isEqualTo(1000);

    // Trigger eviction
    cache.put(1000, "value1000");

    // Should keep approximately 75% (750 entries)
    final int sizeAfterEviction = cache.size();
    assertThat(sizeAfterEviction >= 750 && sizeAfterEviction <= 751).as("Expected ~750-751 entries after eviction, got: " + sizeAfterEviction).isTrue();
  }

  @Test
  void multipleEvictions() {
    final MostUsedCache<String, String> cache = new MostUsedCache<>(50);

    // Fill cache and trigger first eviction
    for (int i = 0; i < 51; i++) {
      cache.put("key" + i, "value" + i);
    }

    final int sizeAfterFirst = cache.size();
    assertThat(sizeAfterFirst >= 37 && sizeAfterFirst <= 38).as("First eviction should reduce size to ~37-38 (75% of 50), got: " + sizeAfterFirst).isTrue();

    // Mark some entries as "hot" by accessing them many times
    for (int i = 0; i < Math.min(3, sizeAfterFirst); i++) {
      for (int j = 0; j < 50; j++) {
        cache.get("key" + i);
      }
    }

    // Record the hot entries we expect to survive
    final Set<String> hotKeys = new HashSet<>();
    for (int i = 0; i < Math.min(3, sizeAfterFirst); i++) {
      hotKeys.add("key" + i);
    }

    // Add enough entries to trigger another eviction
    for (int i = 100; i < 150; i++) {
      cache.put("key" + i, "value" + i);
    }

    // After adding 50 more entries, we should have triggered at least one more eviction
    final int sizeAfterSecond = cache.size();
    assertThat(sizeAfterSecond < 50).as("After adding entries, size should be less than maxSize due to eviction, got: " + sizeAfterSecond).isTrue();

    // The heavily accessed entries should still be present
    int hotEntriesRetained = 0;
    for (final String key : hotKeys) {
      if (cache.get(key) != null) {
        hotEntriesRetained++;
      }
    }
    assertThat(hotEntriesRetained >= 1).as("At least one hot entry should be retained, found: " + hotEntriesRetained).isTrue();
  }

  @Test
  void cacheStats() {
    final MostUsedCache<String, String> cache = new MostUsedCache<>(10);

    // Empty cache stats
    MostUsedCache.CacheStats stats = cache.getStats();
    assertThat(stats.size()).isEqualTo(0);

    // Add entries with different access patterns
    cache.put("hot", "value1");
    cache.put("warm", "value2");
    cache.put("cold", "value3");

    // Access hot entry many times
    for (int i = 0; i < 100; i++) {
      cache.get("hot");
    }

    // Access warm entry moderately
    for (int i = 0; i < 10; i++) {
      cache.get("warm");
    }

    // Don't access cold entry (count = 1 from put)

    stats = cache.getStats();
    assertThat(stats.size()).isEqualTo(3);
    assertThat(stats.maxAccessCount() >= 100).as("Max should be at least 100").isTrue();
    assertThat(stats.minAccessCount()).as("Min should be 1").isEqualTo(1);
    assertThat(stats.avgAccessCount() > 1).as("Average should be > 1").isTrue();
  }

  @Test
  void accessCountReset() {
    final MostUsedCache<Integer, String> cache = new MostUsedCache<>(10);

    // Fill cache
    for (int i = 0; i < 10; i++) {
      cache.put(i, "value" + i);
    }

    // Make one entry very hot
    for (int i = 0; i < 1000; i++) {
      cache.get(0);
    }

    final long countBeforeEviction = cache.getAccessCount(0);
    assertThat(countBeforeEviction > 1000).as("Count should be > 1000").isTrue();

    // Trigger eviction
    cache.put(10, "value10");

    // Key 0 should still exist but with reduced count (to prevent overflow)
    assertThat(cache.get(0)).isNotNull();
    final long countAfterEviction = cache.getAccessCount(0);
    assertThat(countAfterEviction < countBeforeEviction).as("Access count should be reduced after eviction to prevent overflow").isTrue();
  }

  @Test
  void containsKey() {
    final MostUsedCache<String, String> cache = new MostUsedCache<>(10);

    assertThat(cache.containsKey("missing")).isFalse();

    cache.put("present", "value");
    assertThat(cache.containsKey("present")).isTrue();

    cache.remove("present");
    assertThat(cache.containsKey("present")).isFalse();
  }

  @Test
  void entrySet() {
    final MostUsedCache<String, Integer> cache = new MostUsedCache<>(10);

    cache.put("a", 1);
    cache.put("b", 2);
    cache.put("c", 3);

    assertThat(cache.entrySet().size()).isEqualTo(3);

    // Verify all entries are present
    boolean foundA = false, foundB = false, foundC = false;
    for (final var entry : cache.entrySet()) {
      if (entry.getKey().equals("a") && entry.getValue().equals(1)) foundA = true;
      if (entry.getKey().equals("b") && entry.getValue().equals(2)) foundB = true;
      if (entry.getKey().equals("c") && entry.getValue().equals(3)) foundC = true;
    }

    assertThat(foundA && foundB && foundC).as("All entries should be present in entrySet").isTrue();
  }

  @Test
  void smallCache() {
    // Test edge case with very small cache
    final MostUsedCache<Integer, String> cache = new MostUsedCache<>(5);

    for (int i = 0; i < 6; i++) {
      cache.put(i, "value" + i);
    }

    // Should keep at least 1 entry
    assertThat(cache.size() >= 1).isTrue();
  }

  @Test
  void unlimitedSize() {
    // Test with maxSize = -1 (unlimited)
    final MostUsedCache<Integer, String> cache = new MostUsedCache<>(-1);

    // Add many entries - should never trigger eviction
    for (int i = 0; i < 10000; i++) {
      cache.put(i, "value" + i);
    }

    assertThat(cache.size()).as("Unlimited cache should hold all entries").isEqualTo(10000);

    // Verify all entries are still present
    for (int i = 0; i < 10000; i++) {
      assertThat(cache.get(i)).as("Entry " + i + " should still be present in unlimited cache").isNotNull();
    }

    // Test with maxSize = 0 (also unlimited)
    final MostUsedCache<Integer, String> cache2 = new MostUsedCache<>(0);

    for (int i = 0; i < 5000; i++) {
      cache2.put(i, "value" + i);
    }

    assertThat(cache2.size()).as("Cache with maxSize=0 should be unlimited").isEqualTo(5000);
  }

  @Test
  void performance() {
    final MostUsedCache<Integer, String> cache = new MostUsedCache<>(10000);

    // Test insertion performance
    final long startInsert = System.currentTimeMillis();
    for (int i = 0; i < 10000; i++) {
      cache.put(i, "value" + i);
    }
    final long insertTime = System.currentTimeMillis() - startInsert;

    // Test access performance
    final long startAccess = System.currentTimeMillis();
    for (int i = 0; i < 100000; i++) {
      cache.get(i % 10000);
    }
    final long accessTime = System.currentTimeMillis() - startAccess;

    // Test eviction performance
    final long startEviction = System.currentTimeMillis();
    cache.put(10000, "trigger eviction");
    final long evictionTime = System.currentTimeMillis() - startEviction;

    //System.out.println("Performance test:");
    //System.out.println("  Insert 10K entries: " + insertTime + "ms");
    //System.out.println("  100K accesses: " + accessTime + "ms");
    //System.out.println("  Batch eviction (25% removed, 75% kept): " + evictionTime + "ms");
    //System.out.println("  Final size: " + cache.size());
    //System.out.println("  " + cache.getStats());

    // Sanity checks (not strict performance assertions)
    assertThat(insertTime < 1000).as("Insert should be fast").isTrue();
    assertThat(accessTime < 500).as("Access should be very fast").isTrue();
    assertThat(evictionTime < 500).as("Eviction should complete reasonably fast").isTrue();
  }
}
