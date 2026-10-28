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
package com.arcadedb.index.columnar;

import com.arcadedb.graph.olap.simd.GraphOlapVectorOps;
import com.arcadedb.graph.olap.simd.GraphOlapVectorOpsProvider;

import java.util.Arrays;
import java.util.function.LongConsumer;

/**
 * Sparse bitmap of record positions of one bucket, one fixed-size stripe per page. The page and slot of a position follow
 * {@code LocalBucket.recordPosition}: {@code page = position / maxRecordsInPage}, {@code slot = position % maxRecordsInPage}.
 * <p>
 * Snapshots are copy-on-write: {@link #snapshot()} copies the page index only and shares every stripe; the first write to a
 * shared stripe, on either side, clones it. A stripe is owned by a bitmap when its generation equals the bitmap's current
 * generation. Not thread safe: a bitmap must not be written while another thread reads it.
 * <p>
 * {@link #snapshot()} MUTATES its receiver (it advances the generation), so call it only on the writer's own instance, never
 * on an instance already shared with readers; use {@link #copy()} there. Once taken, a snapshot is an independent bitmap that
 * either side may write, each cloning the stripes it touches.
 */
public final class PositionBitmap {
  private static final int[]    NO_PAGES   = new int[0];
  private static final long[][] NO_STRIPES = new long[0][];
  private static final long[]   NO_GENERATIONS = new long[0];

  private final int             maxRecordsInPage;
  private final int             wordsPerStripe;
  private int[]                 pageNumbers      = NO_PAGES;
  private long[][]              stripes          = NO_STRIPES;
  private long[]                stripeGeneration = NO_GENERATIONS;
  private int                   size;
  private long                  generation;

  /**
   * @param maxRecordsInPage slots per page, a positive multiple of 64
   */
  public PositionBitmap(final int maxRecordsInPage) {
    if (maxRecordsInPage <= 0 || maxRecordsInPage % 64 != 0)
      throw new IllegalArgumentException("maxRecordsInPage must be a positive multiple of 64, found " + maxRecordsInPage);
    this.maxRecordsInPage = maxRecordsInPage;
    this.wordsPerStripe = maxRecordsInPage / 64;
  }

  public int maxRecordsInPage() {
    return maxRecordsInPage;
  }

  public boolean isEmpty() {
    return size == 0;
  }

  public void set(final long position) {
    final int slot = (int) (position % maxRecordsInPage);
    final int index = indexOrInsert((int) (position / maxRecordsInPage));
    writableStripe(index)[slot >>> 6] |= 1L << slot;
  }

  public boolean get(final long position) {
    final int index = Arrays.binarySearch(pageNumbers, 0, size, (int) (position / maxRecordsInPage));
    if (index < 0)
      return false;
    final int slot = (int) (position % maxRecordsInPage);
    return (stripes[index][slot >>> 6] & (1L << slot)) != 0;
  }

  public void clear(final long position) {
    final int index = Arrays.binarySearch(pageNumbers, 0, size, (int) (position / maxRecordsInPage));
    if (index < 0)
      return;
    final int slot = (int) (position % maxRecordsInPage);
    if ((stripes[index][slot >>> 6] & (1L << slot)) == 0)
      return;
    final long[] stripe = writableStripe(index);
    stripe[slot >>> 6] &= ~(1L << slot);
    if (stripe[slot >>> 6] == 0 && isZero(stripe))
      removeAt(index);
  }

  /**
   * Sets every slot of the page.
   */
  public void setPage(final int page) {
    final long[] stripe = writableStripe(indexOrInsert(page));
    Arrays.fill(stripe, -1L);
  }

  /**
   * Clears every slot of the page, dropping its stripe.
   */
  public void clearPage(final int page) {
    final int index = Arrays.binarySearch(pageNumbers, 0, size, page);
    if (index >= 0)
      removeAt(index);
  }

  public void and(final PositionBitmap other) {
    intersectOrSubtract(other, false);
  }

  public void andNot(final PositionBitmap other) {
    intersectOrSubtract(other, true);
  }

  public void or(final PositionBitmap other) {
    checkCompatible(other);
    final GraphOlapVectorOps ops = GraphOlapVectorOpsProvider.getInstance();
    final int capacity = size + other.size;
    final int[] mergedPages = new int[capacity];
    final long[][] mergedStripes = new long[capacity][];
    final long[] mergedGenerations = new long[capacity];
    int i = 0;
    int j = 0;
    int n = 0;
    while (i < size || j < other.size) {
      final int mine = i < size ? pageNumbers[i] : Integer.MAX_VALUE;
      final int theirs = j < other.size ? other.pageNumbers[j] : Integer.MAX_VALUE;
      if (i < size && (j >= other.size || mine < theirs)) {
        mergedPages[n] = mine;
        mergedStripes[n] = stripes[i];
        mergedGenerations[n] = stripeGeneration[i];
        i++;
      } else if (i < size && mine == theirs) {
        final long[] stripe = writableStripe(i);
        ops.bitmaskOr(stripe, other.stripes[j], stripe, wordsPerStripe);
        mergedPages[n] = mine;
        mergedStripes[n] = stripe;
        mergedGenerations[n] = generation;
        i++;
        j++;
      } else {
        // never share a stripe with the other bitmap: it would write it in place
        mergedPages[n] = theirs;
        mergedStripes[n] = other.stripes[j].clone();
        mergedGenerations[n] = generation;
        j++;
      }
      n++;
    }
    pageNumbers = mergedPages;
    stripes = mergedStripes;
    stripeGeneration = mergedGenerations;
    size = n;
  }

  public long cardinality() {
    final GraphOlapVectorOps ops = GraphOlapVectorOpsProvider.getInstance();
    long total = 0;
    for (int i = 0; i < size; i++)
      total += ops.bitmaskPopcount(stripes[i], wordsPerStripe);
    return total;
  }

  public long andCardinality(final PositionBitmap other) {
    checkCompatible(other);
    final GraphOlapVectorOps ops = GraphOlapVectorOpsProvider.getInstance();
    long total = 0;
    int j = 0;
    for (int i = 0; i < size && j < other.size; i++) {
      final int page = pageNumbers[i];
      while (j < other.size && other.pageNumbers[j] < page)
        j++;
      if (j < other.size && other.pageNumbers[j] == page)
        total += ops.bitmaskAndPopcount(stripes[i], other.stripes[j], wordsPerStripe);
    }
    return total;
  }

  /**
   * Visits every set position in ascending order.
   */
  public void forEach(final LongConsumer position) {
    for (int i = 0; i < size; i++) {
      final long pageBase = (long) pageNumbers[i] * maxRecordsInPage;
      final long[] stripe = stripes[i];
      for (int w = 0; w < wordsPerStripe; w++) {
        long word = stripe[w];
        final long wordBase = pageBase + ((long) w << 6);
        while (word != 0) {
          position.accept(wordBase + Long.numberOfTrailingZeros(word));
          word &= word - 1;
        }
      }
    }
  }

  /**
   * @return the pages holding at least one bit, ascending
   */
  public int[] pages() {
    return Arrays.copyOf(pageNumbers, size);
  }

  /**
   * O(pages) reference copy: stripes are shared, and a later write to either side clones the touched stripe first.
   */
  public PositionBitmap snapshot() {
    // every existing stripe becomes older than the new generation, so it is shared and cloned by its first writer
    generation++;
    final PositionBitmap snapshot = new PositionBitmap(maxRecordsInPage);
    snapshot.pageNumbers = Arrays.copyOf(pageNumbers, size);
    snapshot.stripes = Arrays.copyOf(stripes, size);
    snapshot.stripeGeneration = Arrays.copyOf(stripeGeneration, size);
    snapshot.size = size;
    snapshot.generation = generation;
    return snapshot;
  }

  /**
   * Deep copy: every stripe is cloned and nothing is shared with this bitmap.
   */
  public PositionBitmap copy() {
    final PositionBitmap copy = new PositionBitmap(maxRecordsInPage);
    copy.pageNumbers = Arrays.copyOf(pageNumbers, size);
    copy.stripes = new long[size][];
    for (int i = 0; i < size; i++)
      copy.stripes[i] = stripes[i].clone();
    copy.stripeGeneration = new long[size];
    copy.size = size;
    return copy;
  }

  long[] stripeForTesting(final int page) {
    final int index = Arrays.binarySearch(pageNumbers, 0, size, page);
    return index < 0 ? null : stripes[index];
  }

  private void checkCompatible(final PositionBitmap other) {
    if (other.maxRecordsInPage != maxRecordsInPage)
      throw new IllegalArgumentException(
          "Cannot combine bitmaps of different page sizes: " + maxRecordsInPage + " and " + other.maxRecordsInPage);
  }

  private void intersectOrSubtract(final PositionBitmap other, final boolean subtract) {
    checkCompatible(other);
    final GraphOlapVectorOps ops = GraphOlapVectorOpsProvider.getInstance();
    int kept = 0;
    int j = 0;
    for (int i = 0; i < size; i++) {
      final int page = pageNumbers[i];
      while (j < other.size && other.pageNumbers[j] < page)
        j++;
      final boolean present = j < other.size && other.pageNumbers[j] == page;
      long[] stripe = stripes[i];
      long stripeGen = stripeGeneration[i];
      if (present) {
        stripe = writableStripe(i);
        stripeGen = generation;
        if (subtract)
          ops.bitmaskAndNot(stripe, other.stripes[j], stripe, wordsPerStripe);
        else
          ops.bitmaskAnd(stripe, other.stripes[j], stripe, wordsPerStripe);
        if (isZero(stripe))
          continue;
      } else if (!subtract)
        continue;
      pageNumbers[kept] = page;
      stripes[kept] = stripe;
      stripeGeneration[kept] = stripeGen;
      kept++;
    }
    Arrays.fill(stripes, kept, size, null);
    size = kept;
  }

  /**
   * Returns the stripe at the index, cloning it first when a snapshot still shares it.
   */
  private long[] writableStripe(final int index) {
    if (stripeGeneration[index] != generation) {
      stripes[index] = stripes[index].clone();
      stripeGeneration[index] = generation;
    }
    return stripes[index];
  }

  private int indexOrInsert(final int page) {
    final int found = Arrays.binarySearch(pageNumbers, 0, size, page);
    if (found >= 0)
      return found;
    final int index = -found - 1;
    if (size == pageNumbers.length) {
      final int capacity = Math.max(4, size + (size >> 1));
      pageNumbers = Arrays.copyOf(pageNumbers, capacity);
      stripes = Arrays.copyOf(stripes, capacity);
      stripeGeneration = Arrays.copyOf(stripeGeneration, capacity);
    }
    final int tail = size - index;
    System.arraycopy(pageNumbers, index, pageNumbers, index + 1, tail);
    System.arraycopy(stripes, index, stripes, index + 1, tail);
    System.arraycopy(stripeGeneration, index, stripeGeneration, index + 1, tail);
    pageNumbers[index] = page;
    stripes[index] = new long[wordsPerStripe];
    stripeGeneration[index] = generation;
    size++;
    return index;
  }

  private void removeAt(final int index) {
    final int tail = size - index - 1;
    System.arraycopy(pageNumbers, index + 1, pageNumbers, index, tail);
    System.arraycopy(stripes, index + 1, stripes, index, tail);
    System.arraycopy(stripeGeneration, index + 1, stripeGeneration, index, tail);
    size--;
    stripes[size] = null;
  }

  private static boolean isZero(final long[] stripe) {
    for (final long word : stripe)
      if (word != 0)
        return false;
    return true;
  }
}
