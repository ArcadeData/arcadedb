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

import java.util.Arrays;

/**
 * Zero-boxing open-addressing hash set for RID identity (bucketId + offset).
 * <p>
 * Uses parallel {@code int[]} + {@code long[]} arrays instead of RID objects,
 * eliminating 56-byte RID allocation, database reference, and cachedHashCode per entry.
 * <p>
 * Memory: 12 bytes per entry vs {@code HashSet<RID>} at ~104 bytes — <b>8.7x savings</b>.
 * <p>
 * Supports any offset value (no 32-bit packing limitation).
 * Designed for graph traversal visited sets, cycle detection, and deduplication.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class RidHashSet {
  private static final int  EMPTY_BUCKET = Integer.MIN_VALUE;
  private static final long EMPTY_OFFSET = Long.MIN_VALUE;
  private static final float LOAD_FACTOR = 0.75f;

  private int[]  bucketIds;
  private long[] offsets;
  private int    capacity;
  private int    mask;
  private int    size;
  private int    threshold;

  public RidHashSet() {
    this(256);
  }

  public RidHashSet(final int initialCapacity) {
    capacity = nextPowerOfTwo(Math.max(16, initialCapacity));
    mask = capacity - 1;
    bucketIds = new int[capacity];
    offsets = new long[capacity];
    threshold = (int) (capacity * LOAD_FACTOR);
    Arrays.fill(bucketIds, EMPTY_BUCKET);
  }

  /**
   * Adds a RID by its components. Returns true if the set was modified (RID was new).
   */
  public boolean add(final int bucketId, final long offset) {
    int idx = hash(bucketId, offset) & mask;
    while (true) {
      if (bucketIds[idx] == EMPTY_BUCKET) {
        bucketIds[idx] = bucketId;
        offsets[idx] = offset;
        if (++size >= threshold)
          resize();
        return true;
      }
      if (bucketIds[idx] == bucketId && offsets[idx] == offset)
        return false; // already present
      idx = (idx + 1) & mask;
    }
  }

  /**
   * Adds a RID object. Convenience method that extracts bucketId and offset.
   */
  public boolean add(final RID rid) {
    return add(rid.getBucketId(), rid.getPosition());
  }

  /**
   * Returns true if the set contains the given RID components.
   */
  public boolean contains(final int bucketId, final long offset) {
    int idx = hash(bucketId, offset) & mask;
    while (true) {
      if (bucketIds[idx] == EMPTY_BUCKET)
        return false;
      if (bucketIds[idx] == bucketId && offsets[idx] == offset)
        return true;
      idx = (idx + 1) & mask;
    }
  }

  /**
   * Returns true if the set contains the given RID.
   */
  public boolean contains(final RID rid) {
    return contains(rid.getBucketId(), rid.getPosition());
  }

  public int size() {
    return size;
  }

  public boolean isEmpty() {
    return size == 0;
  }

  /**
   * Removes all entries from the set.
   */
  public void clear() {
    Arrays.fill(bucketIds, EMPTY_BUCKET);
    size = 0;
  }

  /**
   * Iterates over all entries. No object allocation during iteration.
   */
  public void forEach(final EntryConsumer consumer) {
    for (int i = 0; i < capacity; i++)
      if (bucketIds[i] != EMPTY_BUCKET)
        consumer.accept(bucketIds[i], offsets[i]);
  }

  @FunctionalInterface
  public interface EntryConsumer {
    void accept(int bucketId, long offset);
  }

  private static int hash(final int bucketId, final long offset) {
    long h = bucketId * 0x9E3779B97F4A7C15L + offset;
    h ^= h >>> 33;
    h *= 0xff51afd7ed558ccdL;
    h ^= h >>> 33;
    return (int) h;
  }

  private void resize() {
    final int newCapacity = capacity << 1;
    final int newMask = newCapacity - 1;
    final int[] newBucketIds = new int[newCapacity];
    final long[] newOffsets = new long[newCapacity];
    Arrays.fill(newBucketIds, EMPTY_BUCKET);

    for (int i = 0; i < capacity; i++) {
      if (bucketIds[i] != EMPTY_BUCKET) {
        int idx = hash(bucketIds[i], offsets[i]) & newMask;
        while (newBucketIds[idx] != EMPTY_BUCKET)
          idx = (idx + 1) & newMask;
        newBucketIds[idx] = bucketIds[i];
        newOffsets[idx] = offsets[i];
      }
    }

    bucketIds = newBucketIds;
    offsets = newOffsets;
    capacity = newCapacity;
    mask = newMask;
    threshold = (int) (newCapacity * LOAD_FACTOR);
  }

  private static int nextPowerOfTwo(final int v) {
    int n = v - 1;
    n |= n >>> 1;
    n |= n >>> 2;
    n |= n >>> 4;
    n |= n >>> 8;
    n |= n >>> 16;
    return n + 1;
  }
}
