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
 * Zero-boxing open-addressing hash map: RID identity (bucketId + offset) → primitive int values.
 * <p>
 * Uses parallel {@code int[]} + {@code long[]} + {@code int[]} arrays instead of
 * {@code HashMap<RID, Integer>}, eliminating RID object, Integer boxing,
 * and Map.Entry per entry.
 * <p>
 * Memory: 16 bytes per entry vs {@code HashMap<RID, Integer>} at ~112 bytes — <b>7x savings</b>.
 * <p>
 * Designed for graph algorithm vertex indexing, scoring, and counting keyed by vertex RID.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class RidIntHashMap {
  private static final int   EMPTY_BUCKET = Integer.MIN_VALUE;
  private static final float LOAD_FACTOR  = 0.75f;

  private int[]  keyBucketIds;
  private long[] keyOffsets;
  private int[]  values;
  private int    capacity;
  private int    mask;
  private int    size;
  private int    threshold;

  public RidIntHashMap() {
    this(256);
  }

  public RidIntHashMap(final int initialCapacity) {
    capacity = nextPowerOfTwo(Math.max(16, initialCapacity));
    mask = capacity - 1;
    keyBucketIds = new int[capacity];
    keyOffsets = new long[capacity];
    values = new int[capacity];
    threshold = (int) (capacity * LOAD_FACTOR);
    Arrays.fill(keyBucketIds, EMPTY_BUCKET);
  }

  /**
   * Sets the value for the given RID key. Returns the previous value, or Integer.MIN_VALUE if not present.
   */
  public int put(final int bucketId, final long offset, final int value) {
    int idx = hash(bucketId, offset) & mask;
    while (true) {
      if (keyBucketIds[idx] == EMPTY_BUCKET) {
        keyBucketIds[idx] = bucketId;
        keyOffsets[idx] = offset;
        values[idx] = value;
        if (++size >= threshold)
          resize();
        return Integer.MIN_VALUE;
      }
      if (keyBucketIds[idx] == bucketId && keyOffsets[idx] == offset) {
        final int old = values[idx];
        values[idx] = value;
        return old;
      }
      idx = (idx + 1) & mask;
    }
  }

  /**
   * Convenience: put using a RID object.
   */
  public int put(final RID rid, final int value) {
    return put(rid.getBucketId(), rid.getPosition(), value);
  }

  /**
   * Returns the value for the given RID key, or defaultValue if not present.
   */
  public int get(final int bucketId, final long offset, final int defaultValue) {
    int idx = hash(bucketId, offset) & mask;
    while (true) {
      if (keyBucketIds[idx] == EMPTY_BUCKET)
        return defaultValue;
      if (keyBucketIds[idx] == bucketId && keyOffsets[idx] == offset)
        return values[idx];
      idx = (idx + 1) & mask;
    }
  }

  /**
   * Convenience: get using a RID object.
   */
  public int get(final RID rid, final int defaultValue) {
    return get(rid.getBucketId(), rid.getPosition(), defaultValue);
  }

  /**
   * Adds delta to the value for the given RID key. If not present, inserts with delta as value.
   */
  public void add(final int bucketId, final long offset, final int delta) {
    int idx = hash(bucketId, offset) & mask;
    while (true) {
      if (keyBucketIds[idx] == EMPTY_BUCKET) {
        keyBucketIds[idx] = bucketId;
        keyOffsets[idx] = offset;
        values[idx] = delta;
        if (++size >= threshold)
          resize();
        return;
      }
      if (keyBucketIds[idx] == bucketId && keyOffsets[idx] == offset) {
        values[idx] += delta;
        return;
      }
      idx = (idx + 1) & mask;
    }
  }

  /**
   * Convenience: add using a RID object.
   */
  public void add(final RID rid, final int delta) {
    add(rid.getBucketId(), rid.getPosition(), delta);
  }

  public boolean containsKey(final int bucketId, final long offset) {
    int idx = hash(bucketId, offset) & mask;
    while (true) {
      if (keyBucketIds[idx] == EMPTY_BUCKET)
        return false;
      if (keyBucketIds[idx] == bucketId && keyOffsets[idx] == offset)
        return true;
      idx = (idx + 1) & mask;
    }
  }

  public boolean containsKey(final RID rid) {
    return containsKey(rid.getBucketId(), rid.getPosition());
  }

  public int size() {
    return size;
  }

  public boolean isEmpty() {
    return size == 0;
  }

  /**
   * Iterates over all entries. No object allocation during iteration.
   */
  public void forEach(final EntryConsumer consumer) {
    for (int i = 0; i < capacity; i++)
      if (keyBucketIds[i] != EMPTY_BUCKET)
        consumer.accept(keyBucketIds[i], keyOffsets[i], values[i]);
  }

  @FunctionalInterface
  public interface EntryConsumer {
    void accept(int bucketId, long offset, int value);
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
    final int[] newValues = new int[newCapacity];
    Arrays.fill(newBucketIds, EMPTY_BUCKET);

    for (int i = 0; i < capacity; i++) {
      if (keyBucketIds[i] != EMPTY_BUCKET) {
        int idx = hash(keyBucketIds[i], keyOffsets[i]) & newMask;
        while (newBucketIds[idx] != EMPTY_BUCKET)
          idx = (idx + 1) & newMask;
        newBucketIds[idx] = keyBucketIds[i];
        newOffsets[idx] = keyOffsets[i];
        newValues[idx] = values[i];
      }
    }

    keyBucketIds = newBucketIds;
    keyOffsets = newOffsets;
    values = newValues;
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
