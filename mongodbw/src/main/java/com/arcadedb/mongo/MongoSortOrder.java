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
package com.arcadedb.mongo;

import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.MongoServerError;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

/**
 * The order of a {@code find} sort, as {@code mongod} sorts (issue #12069). SQL {@code ORDER BY} does not: it coerces across types
 * and sees an array as one opaque value. Here every document gets one key per sort field, and the keys are compared with the BSON
 * order of {@link MongoQueryMatcher#compareValues}:
 * <ul>
 *   <li>the values a path reaches are those the filters see ({@link MongoQueryMatcher#values}): a dotted path enters the embedded
 *   documents of an array, a missing value is null;</li>
 *   <li>an array offers its elements, and the key is the smallest of all the values for an ascending field and the largest for a
 *   descending one;</li>
 *   <li>an empty array sorts before null, in either direction;</li>
 *   <li>values of different types compare by their type bracket, numbers by exact value whatever their type.</li>
 * </ul>
 * Documents with equal keys keep the order they were read in.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class MongoSortOrder implements Comparator<Object[]> {
  /** The key of an empty array: below every value, null included. */
  private static final Object EMPTY_ARRAY = EmptyArray.INSTANCE;

  private final String[][]  paths;
  private final boolean[]   descending;
  private final Set<String> properties;

  /**
   * @throws MongoServerError for a direction other than 1 or -1, as {@code mongod} refuses it
   */
  MongoSortOrder(final Document sort) {
    this.paths = new String[sort.size()][];
    this.descending = new boolean[sort.size()];
    this.properties = new HashSet<>();
    int i = 0;
    for (final Map.Entry<String, Object> entry : sort.entrySet()) {
      final String field = entry.getKey();
      if (!(entry.getValue() instanceof Number number))
        throw new MongoServerError(15974, "Location15974", "Illegal key in $sort specification: " + field + ": " + entry.getValue());
      final double direction = number.doubleValue();
      if (direction != 1D && direction != -1D)
        throw new MongoServerError(15975, "Location15975", "$sort key ordering must be 1 (for ascending) or -1 (for descending)");
      paths[i] = field.split("\\.", -1);
      descending[i] = direction < 0;
      properties.add(paths[i][0]);
      ++i;
    }
  }

  /**
   * The top-level properties the keys are computed from: only those need to be read off a record to place it.
   */
  Set<String> properties() {
    return properties;
  }

  /**
   * The keys of a document, one per sort field.
   */
  Object[] keyOf(final Document document) {
    final Object[] keys = new Object[paths.length];
    for (int i = 0; i < paths.length; i++)
      keys[i] = extreme(MongoQueryMatcher.values(document, paths[i]), descending[i]);
    return keys;
  }

  @Override
  public int compare(final Object[] a, final Object[] b) {
    for (int i = 0; i < paths.length; i++) {
      final int comparison = compareKeys(a[i], b[i]);
      if (comparison != 0)
        return descending[i] ? -comparison : comparison;
    }
    return 0;
  }

  /**
   * The smallest (ascending) or largest (descending) of the values a path reaches, an array offering its elements; null when the
   * path reaches nothing.
   */
  private static Object extreme(final List<Object> values, final boolean descending) {
    Object best = null;
    boolean found = false;
    for (final Object value : values) {
      if (value instanceof List<?> list) {
        if (list.isEmpty()) {
          if (!found || better(EMPTY_ARRAY, best, descending))
            best = EMPTY_ARRAY;
          found = true;
        } else
          for (final Object item : list) {
            if (!found || better(item, best, descending))
              best = item;
            found = true;
          }
      } else {
        if (!found || better(value, best, descending))
          best = value;
        found = true;
      }
    }
    return best;
  }

  private static boolean better(final Object candidate, final Object best, final boolean descending) {
    final int comparison = compareKeys(candidate, best);
    return descending ? comparison > 0 : comparison < 0;
  }

  private static int compareKeys(final Object a, final Object b) {
    final boolean emptyA = a instanceof EmptyArray;
    final boolean emptyB = b instanceof EmptyArray;
    if (emptyA || emptyB)
      return emptyA == emptyB ? 0 : emptyA ? -1 : 1;
    return MongoQueryMatcher.compareValues(a, b);
  }

  /** The type of {@link #EMPTY_ARRAY}, which no stored value has. */
  private enum EmptyArray {
    INSTANCE
  }

  /**
   * Sorts entries by their keys. The sort is stable: entries with equal keys keep the order they were read in.
   */
  <T> void sort(final List<Entry<T>> entries) {
    entries.sort((a, b) -> compare(a.keys, b.keys));
  }

  /**
   * A document (or the row it is read from) with its sort keys, and the position it was read at: equal keys keep that order, also
   * through the bounded heap of a sort with a limit, which is not stable by itself.
   */
  static final class Entry<T> {
    final Object[] keys;
    final long     position;
    final T        value;

    Entry(final Object[] keys, final long position, final T value) {
      this.keys = keys;
      this.position = position;
      this.value = value;
    }
  }

  /**
   * Keeps the first {@code limit} entries of the order out of any number offered, holding at most {@code limit} of them: a sort
   * with a limit never holds more documents than it answers (plus the ones it skips).
   */
  static final class TopEntries<T> {
    private final MongoSortOrder           sortOrder;
    private final Comparator<Entry<T>>     order;
    private final PriorityQueue<Entry<T>> heap;
    private final int                      limit;

    TopEntries(final MongoSortOrder sortOrder, final int limit) {
      this.sortOrder = sortOrder;
      this.limit = limit;
      // ties broken by the position the entries were read at, so that the answer is the one of a stable sort
      this.order = (a, b) -> {
        final int comparison = sortOrder.compare(a.keys, b.keys);
        return comparison != 0 ? comparison : Long.compare(a.position, b.position);
      };
      // the worst entry kept is on top, ready to be replaced
      this.heap = new PriorityQueue<>(Math.min(limit, 1024) + 1, order.reversed());
    }

    /**
     * Whether an entry with these keys, read after every entry offered so far, would be kept: the caller builds it only then. With
     * equal keys the entry read later loses.
     */
    boolean accepts(final Object[] keys) {
      return heap.size() < limit || sortOrder.compare(keys, heap.peek().keys) < 0;
    }

    void offer(final Entry<T> entry) {
      heap.add(entry);
      if (heap.size() > limit)
        heap.poll();
    }

    List<Entry<T>> sorted() {
      final List<Entry<T>> result = new ArrayList<>(heap);
      result.sort(order);
      return result;
    }
  }
}
