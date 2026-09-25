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
package com.arcadedb.query.sql.executor;

import com.arcadedb.database.RID;

import java.util.Arrays;

/**
 * The RIDs an index search returned, held as primitive positions grouped by bucket so they can be loaded in physical
 * order: ascending bucket, then ascending position inside it. That turns the one random page access per record an
 * index-order fetch costs into a forward sweep over each bucket's pages, touching every page once however many of its
 * records the search returned (the same idea as PostgreSQL's bitmap heap scan, issue #8333).
 * <p>
 * No {@link RID} is retained while buffering, so a large range leaves one {@code long} per entry and nothing for the
 * collector to trace.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class PhysicalOrderRidBuffer {
  private static final int      INITIAL_BUCKET_CAPACITY = 16;
  // Indexed by bucket id: the slot in positions/sizes holding that bucket's entries, or -1. Bucket ids are file ids,
  // small and dense, so a direct table beats hashing the id for every entry.
  private              int[]    slotByBucketId          = new int[0];
  private              int[]    bucketIds               = new int[4];
  private              long[][] positions               = new long[4][];
  private              int[]    sizes                   = new int[4];
  private              int      slots                   = 0;
  private              int      size                    = 0;

  // Iteration state, valid after sort()
  private int[] slotOrder;
  private int   iterSlot;
  private int   iterPos;

  void add(final int bucketId, final long position) {
    int slot = bucketId < slotByBucketId.length ? slotByBucketId[bucketId] : -1;
    if (slot < 0)
      slot = newSlot(bucketId);

    long[] bucketPositions = positions[slot];
    final int bucketSize = sizes[slot];
    if (bucketSize == bucketPositions.length)
      positions[slot] = bucketPositions = Arrays.copyOf(bucketPositions, bucketPositions.length << 1);
    bucketPositions[bucketSize] = position;
    sizes[slot] = bucketSize + 1;
    ++size;
  }

  int size() {
    return size;
  }

  /**
   * Orders the buffered entries physically and rewinds the iteration. Duplicates are kept: an index can legitimately
   * return one record once per matching key (a multi-value index), and dropping them here would change the answer.
   */
  void sort() {
    slotOrder = new int[slots];
    for (int i = 0; i < slots; i++) {
      slotOrder[i] = i;
      Arrays.sort(positions[i], 0, sizes[i]);
    }
    // Insertion sort on the bucket ids: a handful of buckets per type
    for (int i = 1; i < slots; i++) {
      final int current = slotOrder[i];
      int j = i - 1;
      while (j >= 0 && bucketIds[slotOrder[j]] > bucketIds[current]) {
        slotOrder[j + 1] = slotOrder[j];
        --j;
      }
      slotOrder[j + 1] = current;
    }
    iterSlot = 0;
    iterPos = 0;
  }

  boolean hasNext() {
    while (iterSlot < slots) {
      if (iterPos < sizes[slotOrder[iterSlot]])
        return true;
      ++iterSlot;
      iterPos = 0;
    }
    return false;
  }

  /**
   * @return the next RID in physical order. Call {@link #hasNext()} first.
   */
  RID next() {
    final int slot = slotOrder[iterSlot];
    return new RID(bucketIds[slot], positions[slot][iterPos++]);
  }

  /**
   * Empties the buffer and keeps the arrays it grew, so the next chunk of a large range reuses them.
   */
  void clear() {
    for (int i = 0; i < slots; i++)
      sizes[i] = 0;
    size = 0;
    slotOrder = null;
    iterSlot = 0;
    iterPos = 0;
  }

  private int newSlot(final int bucketId) {
    if (bucketId >= slotByBucketId.length) {
      final int oldLength = slotByBucketId.length;
      slotByBucketId = Arrays.copyOf(slotByBucketId, Math.max(bucketId + 1, oldLength << 1));
      Arrays.fill(slotByBucketId, oldLength, slotByBucketId.length, -1);
    }
    if (slots == bucketIds.length) {
      bucketIds = Arrays.copyOf(bucketIds, slots << 1);
      positions = Arrays.copyOf(positions, slots << 1);
      sizes = Arrays.copyOf(sizes, slots << 1);
    }
    final int slot = slots++;
    bucketIds[slot] = bucketId;
    positions[slot] = new long[INITIAL_BUCKET_CAPACITY];
    sizes[slot] = 0;
    slotByBucketId[bucketId] = slot;
    return slot;
  }
}
