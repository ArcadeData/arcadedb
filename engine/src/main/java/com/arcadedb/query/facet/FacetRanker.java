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
package com.arcadedb.query.facet;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.IntBinaryOperator;

/**
 * Picks the top buckets of a facet. Ties never depend on iteration order: the default is count descending then value ascending,
 * {@code COUNT_ASC} is count ascending then value ascending, {@code VALUE_*} orders by value alone, and the ordinal breaks any
 * remaining tie.
 */
public final class FacetRanker {
  private FacetRanker() {
  }

  /**
   * The best {@code top} ordinals of the counter with at least {@code minCount}, best first, kept in a bounded heap of primitives.
   *
   * @param valueOrder compares the values of two ordinals (negative when the first sorts first), e.g. {@code GlobalDictionary::compare}
   */
  public static int[] topOrdinals(final FacetCounter counter, final int top, final int minCount, final FacetOrder order,
      final IntBinaryOperator valueOrder) {
    if (top <= 0)
      return new int[0];
    final TopHeap heap = new TopHeap(top, order, valueOrder);
    counter.forEachNonZero((ordinal, count) -> {
      if (count >= minCount)
        heap.offer(ordinal, count);
    });
    return heap.drainBestFirst();
  }

  /**
   * The best {@code top} entries of the counts (the first element of each array) with at least {@code minCount}, best first. Keys
   * are normalized once, not per comparison; the entries keep the caller's keys.
   */
  public static List<Map.Entry<Object, Long>> topValues(final Map<Object, long[]> counts, final int top, final int minCount,
      final FacetOrder order) {
    final List<Candidate> candidates = new ArrayList<>(counts.size());
    for (final Map.Entry<Object, long[]> e : counts.entrySet())
      if (e.getValue()[0] >= minCount)
        candidates.add(new Candidate(e.getKey(), FacetValueComparator.normalize(e.getKey()), e.getValue()[0]));
    final FacetValueComparator values = FacetValueComparator.INSTANCE;
    candidates.sort((a, b) -> {
      int cmp = switch (order) {
        case COUNT_DESC -> Long.compare(b.count, a.count);
        case COUNT_ASC -> Long.compare(a.count, b.count);
        default -> 0;
      };
      if (cmp == 0) {
        // null stays last in both directions
        cmp = order == FacetOrder.VALUE_DESC && a.normalized != null && b.normalized != null ? values.compareNormalized(b.normalized,
            a.normalized) : values.compareNormalized(a.normalized, b.normalized);
      }
      return cmp;
    });
    final int n = Math.min(Math.max(top, 0), candidates.size());
    final List<Map.Entry<Object, Long>> result = new ArrayList<>(n);
    for (int i = 0; i < n; i++)
      result.add(new AbstractMap.SimpleImmutableEntry<>(candidates.get(i).key, candidates.get(i).count));
    return result;
  }

  private record Candidate(Object key, Object normalized, long count) {
  }

  /** A binary heap whose root is the worst of the {@code top} best candidates seen so far. */
  private static final class TopHeap {
    private final int               top;
    private final FacetOrder        order;
    private final IntBinaryOperator valueOrder;
    private       int[]             ordinals;
    private       long[]            counts;
    private       int               size;

    TopHeap(final int top, final FacetOrder order, final IntBinaryOperator valueOrder) {
      this.top = top;
      this.order = order;
      this.valueOrder = valueOrder;
      final int initial = Math.min(top, 1024);
      ordinals = new int[initial];
      counts = new long[initial];
    }

    void offer(final int ordinal, final long count) {
      if (size < top) {
        if (size == ordinals.length) {
          final int grown = (int) Math.min((long) top, size * 2L);
          ordinals = Arrays.copyOf(ordinals, grown);
          counts = Arrays.copyOf(counts, grown);
        }
        ordinals[size] = ordinal;
        counts[size] = count;
        siftUp(size++);
      } else if (before(ordinal, count, ordinals[0], counts[0])) {
        ordinals[0] = ordinal;
        counts[0] = count;
        siftDown(0, size);
      }
    }

    int[] drainBestFirst() {
      final int[] result = new int[size];
      int n = size;
      while (n > 0) {
        result[n - 1] = ordinals[0];
        n--;
        ordinals[0] = ordinals[n];
        counts[0] = counts[n];
        siftDown(0, n);
      }
      return result;
    }

    private boolean before(final int ordinalA, final long countA, final int ordinalB, final long countB) {
      int cmp = switch (order) {
        case COUNT_DESC -> Long.compare(countB, countA);
        case COUNT_ASC -> Long.compare(countA, countB);
        default -> 0;
      };
      if (cmp == 0) {
        cmp = valueOrder.applyAsInt(ordinalA, ordinalB);
        if (order == FacetOrder.VALUE_DESC)
          cmp = -cmp;
      }
      if (cmp == 0)
        cmp = Integer.compare(ordinalA, ordinalB);
      return cmp < 0;
    }

    private void siftUp(int child) {
      while (child > 0) {
        final int parent = (child - 1) >>> 1;
        if (!before(ordinals[parent], counts[parent], ordinals[child], counts[child]))
          return;
        swap(parent, child);
        child = parent;
      }
    }

    private void siftDown(int parent, final int n) {
      while (true) {
        int worst = parent;
        final int left = 2 * parent + 1;
        final int right = left + 1;
        if (left < n && before(ordinals[worst], counts[worst], ordinals[left], counts[left]))
          worst = left;
        if (right < n && before(ordinals[worst], counts[worst], ordinals[right], counts[right]))
          worst = right;
        if (worst == parent)
          return;
        swap(parent, worst);
        parent = worst;
      }
    }

    private void swap(final int i, final int j) {
      final int o = ordinals[i];
      ordinals[i] = ordinals[j];
      ordinals[j] = o;
      final long c = counts[i];
      counts[i] = counts[j];
      counts[j] = c;
    }
  }
}
