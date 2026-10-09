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
package com.arcadedb.graph;

import com.arcadedb.database.RID;

import java.util.Arrays;

/**
 * The head segment {@link GraphBatch} has given each vertex's OUT and IN edge lists, kept until {@code close()} writes
 * them onto the vertex records in one sorted pass.
 * <p>
 * One entry per vertex the batch touches, so on a bulk load it is the batch's largest structure and the only one that
 * grows with the vertex count (issue #9575). It used to be two {@code LongObjectHashMap<RID>}, one per direction: a
 * power-of-two table of 12 bytes per slot plus a 24-byte {@link RID} per entry, ~80 to 110 bytes for a vertex present
 * in both, which at 200M vertices is more than 20 GB. Here both heads of a vertex share one slot of three
 * {@code long}s, the RIDs are packed rather than boxed, and the table grows by half rather than doubling, so a vertex
 * costs 32 to 48 bytes and a load stops paying for the power of two it happens to land just above.
 * <p>
 * A RID is packed as {@code bucketId << 40 | position}, the layout {@code GraphBatch.packVertexKey} already relies
 * on, and a vertex key is such a packing too. Neither can be negative, which is what lets {@link Long#MIN_VALUE} mark
 * a free slot and {@code -1} a direction with no deferred head.
 * <p>
 * Not thread-safe: written by the single-threaded paths of {@link GraphBatch} only. Reads take no lock and mutate
 * nothing, so the parallel connectors may read it while no writer runs, exactly as they read the maps it replaces.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class DeferredHeadChunks {
  private static final long   FREE_SLOT        = Long.MIN_VALUE;
  private static final long   NO_HEAD          = -1L;
  private static final long   POSITION_MASK    = 0xFFFFFFFFFFL;
  private static final int    MAX_BUCKET_ID    = (1 << 23) - 1;
  private static final int    INITIAL_CAPACITY = 1024;
  // Linear probing at 0.75 expects ~8.5 probes for a miss: the price of keeping the slot count low on a table that,
  // on the loads this exists for, is the largest object of the process
  private static final double MAX_LOAD         = 0.75;
  // The largest array the JVM reliably allocates
  private static final int    MAX_CAPACITY     = Integer.MAX_VALUE - 8;

  private long[] keys;
  private long[] outHeads;
  private long[] inHeads;
  private int    capacity;
  private int    threshold;
  private int    size;
  private int    outCount;
  private int    inCount;

  DeferredHeadChunks() {
    this(0);
  }

  /**
   * @param expectedVertices how many vertices the batch is expected to touch, 0 when unknown. A table sized for them
   *                         up front never grows, and growing is when the table costs most: the old one and the new
   *                         one, half as large again, are both alive while the entries move, two and a half times
   *                         what the map holds. At 200M vertices that copy alone is more than 15 GB.
   */
  DeferredHeadChunks(final int expectedVertices) {
    allocate((int) Math.max(INITIAL_CAPACITY, Math.min(MAX_CAPACITY, (long) (expectedVertices / MAX_LOAD) + 1)));
  }

  RID getOut(final long vertexKey) {
    final int i = indexOf(vertexKey);
    return i < 0 ? null : unpack(outHeads[i]);
  }

  RID getIn(final long vertexKey) {
    final int i = indexOf(vertexKey);
    return i < 0 ? null : unpack(inHeads[i]);
  }

  void putOut(final long vertexKey, final RID head) {
    final long packed = pack(head);
    final int i = claim(vertexKey);
    if (outHeads[i] == NO_HEAD)
      outCount++;
    outHeads[i] = packed;
  }

  void putIn(final long vertexKey, final RID head) {
    final long packed = pack(head);
    final int i = claim(vertexKey);
    if (inHeads[i] == NO_HEAD)
      inCount++;
    inHeads[i] = packed;
  }

  void removeOut(final long vertexKey) {
    final int i = indexOf(vertexKey);
    if (i < 0 || outHeads[i] == NO_HEAD)
      return;
    outHeads[i] = NO_HEAD;
    outCount--;
    if (inHeads[i] == NO_HEAD)
      delete(i);
  }

  void removeIn(final long vertexKey) {
    final int i = indexOf(vertexKey);
    if (i < 0 || inHeads[i] == NO_HEAD)
      return;
    inHeads[i] = NO_HEAD;
    inCount--;
    if (outHeads[i] == NO_HEAD)
      delete(i);
  }

  /** Vertices with a deferred OUT head. */
  int outSize() {
    return outCount;
  }

  /** Vertices with a deferred IN head. */
  int inSize() {
    return inCount;
  }

  /** Vertices with a deferred head in either direction: each is written once, both pointers at a time. */
  int size() {
    return size;
  }

  boolean isEmpty() {
    return size == 0;
  }

  /** The keys of every vertex with a deferred head, in no particular order. */
  long[] keys() {
    final long[] result = new long[size];
    int n = 0;
    for (int i = 0; i < capacity; i++)
      if (keys[i] != FREE_SLOT)
        result[n++] = keys[i];
    return result;
  }

  /** Empties the map and gives its table back: on a bulk load that table is gigabytes. */
  void clear() {
    allocate(INITIAL_CAPACITY);
    outCount = 0;
    inCount = 0;
  }

  static long pack(final RID rid) {
    final int bucketId = rid.getBucketId();
    final long position = rid.getPosition();
    if (bucketId < 0 || bucketId > MAX_BUCKET_ID || position < 0 || position > POSITION_MASK)
      throw new IllegalArgumentException("RID " + rid + " cannot be packed: the bucket id must be within 0.." + MAX_BUCKET_ID
          + " and the position within 0.." + POSITION_MASK);
    return ((long) bucketId << 40) | position;
  }

  private static RID unpack(final long packed) {
    return packed == NO_HEAD ? null : new RID((int) (packed >>> 40), packed & POSITION_MASK);
  }

  /**
   * The home slot of a key: the high 32 bits of a Fibonacci scramble, every bit of the key having a say in them, scaled
   * onto the table by a multiply-shift instead of a mask, which is what lets the capacity be any size. Vertex keys
   * carry the bucket id in their top bits and dense positions in the low ones, the shape a low window would collide.
   */
  private int home(final long key) {
    return (int) ((((key * 0x9E3779B97F4A7C15L) >>> 32) * capacity) >>> 32);
  }

  private int indexOf(final long key) {
    int i = home(key);
    while (true) {
      final long k = keys[i];
      if (k == key)
        return i;
      if (k == FREE_SLOT)
        return -1;
      if (++i == capacity)
        i = 0;
    }
  }

  /**
   * The slot of {@code key}, taken with no head in either direction when the key is not there yet. Only a key that
   * takes a new slot can grow the table: overwriting a head, as the undo logs do when they restore one, never does.
   */
  private int claim(final long key) {
    final int existing = indexOf(key);
    if (existing >= 0)
      return existing;
    if (size >= threshold)
      grow();
    int i = home(key);
    while (keys[i] != FREE_SLOT)
      if (++i == capacity)
        i = 0;
    keys[i] = key;
    size++;
    return i;
  }

  /** The number of slots of the table, for tests. */
  int capacity() {
    return capacity;
  }

  /**
   * Frees slot {@code hole} by shifting back every entry of the run after it that would no longer be reachable from its
   * home slot, so lookups never need tombstones.
   */
  private void delete(int hole) {
    int j = hole;
    while (true) {
      if (++j == capacity)
        j = 0;
      final long k = keys[j];
      if (k == FREE_SLOT)
        break;
      final int home = home(k);
      // The entry at j stays where it is when its home lies cyclically within (hole, j]
      final boolean reachable = hole <= j ? (hole < home && home <= j) : (hole < home || home <= j);
      if (!reachable) {
        keys[hole] = k;
        outHeads[hole] = outHeads[j];
        inHeads[hole] = inHeads[j];
        hole = j;
      }
    }
    keys[hole] = FREE_SLOT;
    outHeads[hole] = NO_HEAD;
    inHeads[hole] = NO_HEAD;
    size--;
  }

  private void grow() {
    if (capacity == MAX_CAPACITY)
      throw new IllegalStateException("GraphBatch cannot track the edge list heads of more than " + size + " vertices in one batch");
    final long[] oldKeys = keys;
    final long[] oldOut = outHeads;
    final long[] oldIn = inHeads;
    final int oldCapacity = capacity;
    allocate((int) Math.min(MAX_CAPACITY, (long) oldCapacity + (oldCapacity >> 1)));
    for (int i = 0; i < oldCapacity; i++) {
      final long k = oldKeys[i];
      if (k == FREE_SLOT)
        continue;
      int j = home(k);
      while (keys[j] != FREE_SLOT)
        if (++j == capacity)
          j = 0;
      keys[j] = k;
      outHeads[j] = oldOut[i];
      inHeads[j] = oldIn[i];
      size++;
    }
  }

  private void allocate(final int newCapacity) {
    capacity = newCapacity;
    threshold = (int) (newCapacity * MAX_LOAD);
    keys = new long[newCapacity];
    outHeads = new long[newCapacity];
    inHeads = new long[newCapacity];
    Arrays.fill(keys, FREE_SLOT);
    Arrays.fill(outHeads, NO_HEAD);
    Arrays.fill(inHeads, NO_HEAD);
    // The head counts are left alone: grow() re-adds every entry with the heads it had
    size = 0;
  }
}
