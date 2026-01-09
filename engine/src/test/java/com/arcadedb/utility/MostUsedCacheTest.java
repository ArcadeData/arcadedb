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

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test suite for MostUsedCache.
 */
public class MostUsedCacheTest {

  @Test
  public void testBasicOperations() {
    final MostUsedCache<String, Integer> cache = new MostUsedCache<>(10);

    // Test put and get
    cache.put("key1", 1);
    cache.put("key2", 2);
    assertEquals(1, cache.get("key1"));
    assertEquals(2, cache.get("key2"));
    assertEquals(2, cache.size());

    // Test update
    cache.put("key1", 10);
    assertEquals(10, cache.get("key1"));
    assertEquals(2, cache.size());

    // Test remove
    cache.remove("key1");
    assertNull(cache.get("key1"));
    assertEquals(1, cache.size());

    // Test clear
    cache.clear();
    assertEquals(0, cache.size());
  }

  @Test
  public void testAccessCounting() {
    final MostUsedCache<String, String> cache = new MostUsedCache<>(10);

    cache.put("key1", "value1");
    assertEquals(1, cache.getAccessCount("key1"));

    cache.get("key1");
    assertEquals(2, cache.getAccessCount("key1"));

    cache.get("key1");
    cache.get("key1");
    assertEquals(4, cache.getAccessCount("key1"));

    // Update should increment count
    cache.put("key1", "newValue");
    assertEquals(5, cache.getAccessCount("key1"));
  }

  @Test
  public void testEvictionKeepsMostUsed() {
    final MostUsedCache<Integer, String> cache = new MostUsedCache<>(100);

    // Fill cache with 100 entries
    for (int i = 0; i < 100; i++) {
      cache.put(i, "value" + i);
    }
    assertEquals(100, cache.size());

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
    assertTrue(sizeAfterEviction >= 75 && sizeAfterEviction <= 76,
      "Expected size ~75-76 after eviction, got: " + sizeAfterEviction);

    // The most frequently accessed entries should still be present
    for (int i = 0; i < 10; i++) {
      assertNotNull(cache.get(i), "Key " + i + " should be retained after eviction");
    }

    // Some of the rarely accessed entries should be gone (25% evicted)
    int removedCount = 0;
    for (int i = 50; i < 100; i++) {
      if (cache.get(i) == null) {
        removedCount++;
      }
    }
    assertTrue(removedCount >= 10, "Expected some rarely-accessed entries to be evicted, removed: " + removedCount);
  }

  @Test
  public void testEvictionRatio() {
    final MostUsedCache<Integer, String> cache = new MostUsedCache<>(1000);

    // Fill cache to capacity
    for (int i = 0; i < 1000; i++) {
      cache.put(i, "value" + i);
    }
    assertEquals(1000, cache.size());

    // Trigger eviction
    cache.put(1000, "value1000");

    // Should keep approximately 75% (750 entries)
    final int sizeAfterEviction = cache.size();
    assertTrue(sizeAfterEviction >= 750 && sizeAfterEviction <= 751,
      "Expected ~750-751 entries after eviction, got: " + sizeAfterEviction);
  }

  @Test
  public void testMultipleEvictions() {
    final MostUsedCache<String, String> cache = new MostUsedCache<>(50);

    // Fill cache and trigger first eviction
    for (int i = 0; i < 51; i++) {
      cache.put("key" + i, "value" + i);
    }

    final int sizeAfterFirst = cache.size();
    assertTrue(sizeAfterFirst >= 37 && sizeAfterFirst <= 38, "First eviction should reduce size to ~37-38 (75% of 50), got: " + sizeAfterFirst);

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
    assertTrue(sizeAfterSecond < 50, "After adding entries, size should be less than maxSize due to eviction, got: " + sizeAfterSecond);

    // The heavily accessed entries should still be present
    int hotEntriesRetained = 0;
    for (final String key : hotKeys) {
      if (cache.get(key) != null) {
        hotEntriesRetained++;
      }
    }
    assertTrue(hotEntriesRetained >= 1, "At least one hot entry should be retained, found: " + hotEntriesRetained);
  }

  @Test
  public void testCacheStats() {
    final MostUsedCache<String, String> cache = new MostUsedCache<>(10);

    // Empty cache stats
    MostUsedCache.CacheStats stats = cache.getStats();
    assertEquals(0, stats.size());

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
    assertEquals(3, stats.size());
    assertTrue(stats.maxAccessCount() >= 100, "Max should be at least 100");
    assertTrue(stats.minAccessCount() == 1, "Min should be 1");
    assertTrue(stats.avgAccessCount() > 1, "Average should be > 1");
  }

  @Test
  public void testAccessCountReset() {
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
    assertTrue(countBeforeEviction > 1000, "Count should be > 1000");

    // Trigger eviction
    cache.put(10, "value10");

    // Key 0 should still exist but with reduced count (to prevent overflow)
    assertNotNull(cache.get(0));
    final long countAfterEviction = cache.getAccessCount(0);
    assertTrue(countAfterEviction < countBeforeEviction,
      "Access count should be reduced after eviction to prevent overflow");
  }

  @Test
  public void testContainsKey() {
    final MostUsedCache<String, String> cache = new MostUsedCache<>(10);

    assertFalse(cache.containsKey("missing"));

    cache.put("present", "value");
    assertTrue(cache.containsKey("present"));

    cache.remove("present");
    assertFalse(cache.containsKey("present"));
  }

  @Test
  public void testEntrySet() {
    final MostUsedCache<String, Integer> cache = new MostUsedCache<>(10);

    cache.put("a", 1);
    cache.put("b", 2);
    cache.put("c", 3);

    assertEquals(3, cache.entrySet().size());

    // Verify all entries are present
    boolean foundA = false, foundB = false, foundC = false;
    for (final var entry : cache.entrySet()) {
      if (entry.getKey().equals("a") && entry.getValue().equals(1)) foundA = true;
      if (entry.getKey().equals("b") && entry.getValue().equals(2)) foundB = true;
      if (entry.getKey().equals("c") && entry.getValue().equals(3)) foundC = true;
    }

    assertTrue(foundA && foundB && foundC, "All entries should be present in entrySet");
  }

  @Test
  public void testSmallCache() {
    // Test edge case with very small cache
    final MostUsedCache<Integer, String> cache = new MostUsedCache<>(5);

    for (int i = 0; i < 6; i++) {
      cache.put(i, "value" + i);
    }

    // Should keep at least 1 entry
    assertTrue(cache.size() >= 1);
  }

  @Test
  public void testUnlimitedSize() {
    // Test with maxSize = -1 (unlimited)
    final MostUsedCache<Integer, String> cache = new MostUsedCache<>(-1);

    // Add many entries - should never trigger eviction
    for (int i = 0; i < 10000; i++) {
      cache.put(i, "value" + i);
    }

    assertEquals(10000, cache.size(), "Unlimited cache should hold all entries");

    // Verify all entries are still present
    for (int i = 0; i < 10000; i++) {
      assertNotNull(cache.get(i), "Entry " + i + " should still be present in unlimited cache");
    }

    // Test with maxSize = 0 (also unlimited)
    final MostUsedCache<Integer, String> cache2 = new MostUsedCache<>(0);

    for (int i = 0; i < 5000; i++) {
      cache2.put(i, "value" + i);
    }

    assertEquals(5000, cache2.size(), "Cache with maxSize=0 should be unlimited");
  }

  @Test
  public void testPerformance() {
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

    System.out.println("Performance test:");
    System.out.println("  Insert 10K entries: " + insertTime + "ms");
    System.out.println("  100K accesses: " + accessTime + "ms");
    System.out.println("  Batch eviction (25% removed, 75% kept): " + evictionTime + "ms");
    System.out.println("  Final size: " + cache.size());
    System.out.println("  " + cache.getStats());

    // Sanity checks (not strict performance assertions)
    assertTrue(insertTime < 1000, "Insert should be fast");
    assertTrue(accessTime < 500, "Access should be very fast");
    assertTrue(evictionTime < 500, "Eviction should complete reasonably fast");
  }
}
