/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.index.lsm;

import java.util.Arrays;

/**
 * The live values of one key of an LSM index, as primitive arrays sorted by RID (bucket id, then position), each with the
 * term frequency and document length a full-text BM25 index stores beside it (issue #12458). It answers what
 * {@link LSMTreeIndex#get(Object[])} answers for the key, without a {@code RID}, an {@code IndexCursorEntry} or a hash set entry
 * per value: a BM25 search reads every posting of every query term, and those objects were most of its cost.
 * <p>
 * The values are appended page by page as the read path walks the pages, newest page first, each page's values as the page holds
 * them, oldest first, deletion markers included. The WALK order - pages newest first, a page's values newest first - is the order
 * {@code LSMTreeIndexAbstract.lookupInPageAndAddInResultset} resolves them in, and {@link #resolve()} applies its rules: a per-RID
 * deletion marker removes the older values of its RID, and of several live values of one RID the newest is the one kept. Both come
 * down to "the first value of a RID in walk order decides". The values are sorted by RID, which puts those of one RID together,
 * and the walk order of the few RIDs with more than one value is worked out from the pages they are on, so no value is moved to
 * put the pages in walk order. A key-wide deletion marker removes every older value of the key, so the walk stops there.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class LSMTreeIndexPostings {
  private static final int INITIAL_CAPACITY = 64;
  // the radix sort takes digits of up to this many bits: two passes cover a bucket of up to 67 million slots
  private static final int MAX_RADIX_BITS   = 13;
  // below this many values a comparison sort costs less than the radix sort's counters
  private static final int RADIX_MIN_VALUES = 256;

  private int    size;
  private int[]  bucketIds     = new int[INITIAL_CAPACITY];
  private long[] positions     = new long[INITIAL_CAPACITY];
  private int[]  tfs           = new int[INITIAL_CAPACITY];
  private int[]  docLengths    = new int[INITIAL_CAPACITY];
  // where each page's values start, in walk order of the pages
  private int[]  pageStarts    = new int[8];
  private int    pages;
  // the index of the newest key-wide deletion marker of the page being appended, or -1
  private int    keyWideMarker = -1;

  /** The number of live values once {@link #resolve()} ran. */
  public int size() {
    return size;
  }

  /** The bucket id of every value, valid up to {@link #size()}. A full-text bucket index holds the values of one bucket. */
  public int[] bucketIds() {
    return bucketIds;
  }

  /** The position of every value, ascending within a bucket, valid up to {@link #size()}. */
  public long[] positions() {
    return positions;
  }

  /** The term frequency of every value, valid up to {@link #size()}: 0 on an index that stores none. */
  public int[] termFrequencies() {
    return tfs;
  }

  /** The document length of every value, valid up to {@link #size()}: 0 on an index that stores none. */
  public int[] documentLengths() {
    return docLengths;
  }

  /**
   * Starts the values of the next page in walk order.
   */
  void startPage() {
    if (pages == pageStarts.length)
      pageStarts = Arrays.copyOf(pageStarts, pages << 1);
    pageStarts[pages++] = size;
  }

  /**
   * Appends a value as stored: a deletion marker keeps its negative bucket id.
   */
  void addRaw(final int bucketId, final long position, final int tf, final int docLength) {
    if (size == positions.length) {
      final int capacity = size << 1;
      bucketIds = Arrays.copyOf(bucketIds, capacity);
      positions = Arrays.copyOf(positions, capacity);
      tfs = Arrays.copyOf(tfs, capacity);
      docLengths = Arrays.copyOf(docLengths, capacity);
    }
    if (bucketId == -1 && position == -1)
      // REMOVED_ENTRY_RID. Of several on one page, the one appended last is the newest
      keyWideMarker = size;
    bucketIds[size] = bucketId;
    positions[size] = position;
    tfs[size] = tf;
    docLengths[size] = docLength;
    ++size;
  }

  /**
   * Ends the values of the page started last. A key-wide deletion marker among them removes every value of the key older than
   * itself: those of the page appended before it, and those of every page still to walk.
   *
   * @return false when a key-wide deletion marker ends the walk
   */
  boolean endPage() {
    if (keyWideMarker < 0)
      return true;
    final int from = pageStarts[pages - 1];
    final int newer = size - keyWideMarker - 1;
    System.arraycopy(bucketIds, keyWideMarker + 1, bucketIds, from, newer);
    System.arraycopy(positions, keyWideMarker + 1, positions, from, newer);
    System.arraycopy(tfs, keyWideMarker + 1, tfs, from, newer);
    System.arraycopy(docLengths, keyWideMarker + 1, docLengths, from, newer);
    size = from + newer;
    keyWideMarker = -1;
    return false;
  }

  /**
   * Applies the deletion markers and drops the older duplicates, leaving the live values sorted by RID.
   */
  void resolve() {
    if (size == 0)
      return;
    // where each value was appended, or null when they are in RID order as appended
    final int[] appended = isSortedByRid() ? null : sortByRid();

    int kept = 0;
    for (int i = 0; i < size; ) {
      final int bucketId = ridBucket(i);
      final long position = positions[i];
      int group = i + 1;
      while (group < size && positions[group] == position && ridBucket(group) == bucketId)
        ++group;
      // the first value of the RID in walk order decides: a deletion marker removes the RID, a live value is the newest one
      int first = i;
      if (group - i > 1) {
        long firstWalk = walkIndex(appended != null ? appended[i] : i);
        for (int j = i + 1; j < group; ++j) {
          final long walk = walkIndex(appended != null ? appended[j] : j);
          if (walk < firstWalk) {
            firstWalk = walk;
            first = j;
          }
        }
      }
      if (bucketIds[first] >= 0) {
        bucketIds[kept] = bucketIds[first];
        positions[kept] = position;
        tfs[kept] = tfs[first];
        docLengths[kept] = docLengths[first];
        ++kept;
      }
      i = group;
    }
    size = kept;
  }

  /**
   * The place in walk order of the value appended at {@code index}: its page's place, then its place from the end of the page,
   * where the newest value of the page is. Values appended outside a page (the merged answer of a transaction) keep their order.
   */
  private long walkIndex(final int index) {
    if (pages == 0)
      return index;
    int low = 0;
    int high = pages - 1;
    while (low < high) {
      final int mid = (low + high + 1) >>> 1;
      if (pageStarts[mid] <= index)
        low = mid;
      else
        high = mid - 1;
    }
    final int pageEnd = low + 1 < pages ? pageStarts[low + 1] : size;
    return ((long) low << 32) | (pageEnd - 1 - index);
  }

  /** The bucket id of the RID a value is about: a deletion marker stores {@code -(bucketId + 2)}. */
  private int ridBucket(final int i) {
    final int bucketId = bucketIds[i];
    return bucketId >= 0 ? bucketId : -bucketId - 2;
  }

  private int compareRid(final int i, final int j) {
    final int cmp = Integer.compare(ridBucket(i), ridBucket(j));
    return cmp != 0 ? cmp : Long.compare(positions[i], positions[j]);
  }

  private boolean isSortedByRid() {
    for (int i = 1; i < size; ++i)
      if (compareRid(i - 1, i) > 0)
        return false;
    return true;
  }

  /**
   * Sorts the values by RID, stably, and answers where each one was appended: the order of the values of one RID has to be
   * recoverable for {@link #resolve()} to find the first one in walk order.
   */
  private int[] sortByRid() {
    int[] order = size >= RADIX_MIN_VALUES ? radixOrder() : null;
    if (order == null)
      order = mergeOrder();
    permute(order);
    return order;
  }

  /** The stable order of the values by RID, by a bottom-up merge sort of a permutation. */
  private int[] mergeOrder() {
    int[] order = new int[size];
    int[] buffer = new int[size];
    for (int i = 0; i < size; ++i)
      order[i] = i;
    for (int width = 1; width < size; width <<= 1) {
      for (int left = 0; left < size; left += width << 1) {
        final int mid = Math.min(left + width, size);
        final int right = Math.min(left + (width << 1), size);
        int i = left;
        int j = mid;
        int k = left;
        while (i < mid && j < right)
          buffer[k++] = compareRid(order[i], order[j]) <= 0 ? order[i++] : order[j++];
        while (i < mid)
          buffer[k++] = order[i++];
        while (j < right)
          buffer[k++] = order[j++];
      }
      final int[] swap = order;
      order = buffer;
      buffer = swap;
    }
    return order;
  }

  /**
   * The stable order of the values by position, through a least-significant-digit radix sort on the distance from the smallest
   * position, in as few passes of up to {@link #MAX_RADIX_BITS} bits as the widest distance needs. Null when the values are of
   * several buckets, which a full-text bucket index never holds.
   */
  private int[] radixOrder() {
    final int bucketId = ridBucket(0);
    long min = positions[0];
    long max = positions[0];
    for (int i = 1; i < size; ++i) {
      if (ridBucket(i) != bucketId)
        return null;
      final long position = positions[i];
      if (position < min)
        min = position;
      else if (position > max)
        max = position;
    }
    final int bits = 64 - Long.numberOfLeadingZeros(max - min);
    final int passes = Math.max(1, (bits + MAX_RADIX_BITS - 1) / MAX_RADIX_BITS);
    final int digitBits = Math.max(1, (bits + passes - 1) / passes);
    final int mask = (1 << digitBits) - 1;

    int[] order = new int[size];
    int[] orderBuffer = new int[size];
    long[] keys = new long[size];
    long[] keysBuffer = new long[size];
    for (int i = 0; i < size; ++i) {
      order[i] = i;
      keys[i] = positions[i] - min;
    }
    final int[] counts = new int[1 << digitBits];
    for (int shift = 0; shift < bits; shift += digitBits) {
      Arrays.fill(counts, 0);
      for (int i = 0; i < size; ++i)
        ++counts[(int) (keys[i] >>> shift) & mask];
      for (int d = 0, total = 0; d < counts.length; ++d) {
        final int count = counts[d];
        counts[d] = total;
        total += count;
      }
      for (int i = 0; i < size; ++i) {
        final int slot = counts[(int) (keys[i] >>> shift) & mask]++;
        keysBuffer[slot] = keys[i];
        orderBuffer[slot] = order[i];
      }
      final long[] k = keys;
      keys = keysBuffer;
      keysBuffer = k;
      final int[] o = order;
      order = orderBuffer;
      orderBuffer = o;
    }
    return order;
  }

  /** Rearranges the arrays so the value at {@code order[i]} comes i-th. */
  private void permute(final int[] order) {
    final int[] sortedBuckets = new int[size];
    final long[] sortedPositions = new long[size];
    final int[] sortedTfs = new int[size];
    final int[] sortedDocLengths = new int[size];
    for (int i = 0; i < size; ++i) {
      final int from = order[i];
      sortedBuckets[i] = bucketIds[from];
      sortedPositions[i] = positions[from];
      sortedTfs[i] = tfs[from];
      sortedDocLengths[i] = docLengths[from];
    }
    bucketIds = sortedBuckets;
    positions = sortedPositions;
    tfs = sortedTfs;
    docLengths = sortedDocLengths;
  }
}
