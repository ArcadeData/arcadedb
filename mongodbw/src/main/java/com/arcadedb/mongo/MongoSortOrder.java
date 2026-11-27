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
import de.bwaldvogel.mongo.bson.MinKey;
import de.bwaldvogel.mongo.exception.ErrorCode;
import de.bwaldvogel.mongo.exception.MongoServerError;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

/**
 * The order of a {@code find} sort, as {@code mongod} sorts (issue #12069). SQL {@code ORDER BY} does not: it coerces across types
 * and sees an array as one opaque value. Here every document gets one key per sort field, chosen as {@code mongod}'s sort key
 * generator chooses it, and the keys are compared with the BSON order of {@link MongoQueryMatcher#compareValues}:
 * <ul>
 *   <li>a dotted path enters embedded documents; through an array it enters each element, a scalar element (and a missing field)
 *   giving null; a numeric segment reads the element at that position;</li>
 *   <li>an array at the end of a path offers its elements, an empty one a key below null (but above MinKey);</li>
 *   <li>the candidate keys of a document are tuples, one value per sort field: fields whose paths go through the same array take
 *   their values from the same element, and the document's key is the tuple that sorts first under the whole sort (the smallest
 *   for an ascending field, the largest for a descending one); paths through two different arrays of one document are refused, as
 *   {@code mongod} refuses parallel arrays;</li>
 *   <li>values of different types compare by their type bracket, numbers by exact value whatever their type, strings by code point
 *   (the order of their UTF-8 bytes).</li>
 * </ul>
 * Documents with equal keys keep the order they were read in.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class MongoSortOrder implements Comparator<Object[]> {
  /** The key of an empty array: below every value but MinKey, null included. */
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
   * The keys of a document, one per sort field: the candidate tuple that sorts first.
   *
   * @throws MongoServerError when the sort paths go through two different arrays of the document
   */
  Object[] keyOf(final Document document) {
    final int fields = paths.length;
    final Object[] values = new Object[fields];
    final int[] depths = new int[fields];
    final boolean[] expanded = new boolean[fields];
    for (int i = 0; i < fields; i++)
      values[i] = document;
    final Object[][] best = new Object[1][];
    generate(values, depths, expanded, best);
    return best[0];
  }

  /**
   * Walks the sort paths of one document together, expanding at most one array at a time, and keeps the candidate tuple that sorts
   * first in {@code best[0]}.
   *
   * @param values   per field, the value reached so far
   * @param depths   per field, the number of path segments consumed to reach it
   * @param expanded per field, whether the value is an element of the array that ended its path (a nested array is then a value)
   */
  private void generate(final Object[] values, final int[] depths, final boolean[] expanded, final Object[][] best) {
    final int fields = paths.length;
    // walk each field through embedded documents and positional segments, up to the end of its path or an array to expand
    for (int i = 0; i < fields; i++) {
      final String[] path = paths[i];
      while (depths[i] < path.length) {
        final Object value = values[i];
        final String segment = path[depths[i]];
        if (value instanceof Document document) {
          values[i] = document.get(segment);
          ++depths[i];
        } else if (value instanceof List<?> list && position(segment) >= 0) {
          // a numeric segment reads the element at that position, the array is not expanded
          final int position = position(segment);
          values[i] = position < list.size() ? list.get(position) : null;
          ++depths[i];
        } else if (value instanceof List)
          break;
        else {
          // a scalar or a null has no field: the path reaches null
          values[i] = null;
          depths[i] = path.length;
        }
      }
    }

    // the array to expand: a field stopped in the middle of its path on one, or a field whose path ended on one
    int expanding = -1;
    for (int i = 0; i < fields && expanding < 0; i++)
      if (values[i] instanceof List && !expanded[i])
        expanding = i;
    if (expanding < 0) {
      offer(values.clone(), best);
      return;
    }

    // the fields that reached the same array, by the same path; any other field on an array is a parallel array
    final boolean[] together = new boolean[fields];
    for (int i = 0; i < fields; i++) {
      if (!(values[i] instanceof List) || expanded[i])
        continue;
      if (Arrays.equals(paths[i], 0, depths[i], paths[expanding], 0, depths[expanding]))
        together[i] = true;
      else
        throw new MongoServerError(ErrorCode.BadValue.getValue(), ErrorCode.BadValue.getName(), "cannot sort with keys that are parallel arrays");
    }

    final List<?> array = (List<?>) values[expanding];
    if (array.isEmpty()) {
      final Object[] nextValues = values.clone();
      final int[] nextDepths = depths.clone();
      final boolean[] nextExpanded = expanded.clone();
      for (int i = 0; i < fields; i++)
        if (together[i]) {
          // an empty array ends a path with its own key, and reaches nothing below it
          nextValues[i] = nextDepths[i] == paths[i].length ? EMPTY_ARRAY : null;
          nextDepths[i] = paths[i].length;
          nextExpanded[i] = true;
        }
      generate(nextValues, nextDepths, nextExpanded, best);
      return;
    }

    for (final Object element : array) {
      final Object[] nextValues = values.clone();
      final int[] nextDepths = depths.clone();
      final boolean[] nextExpanded = expanded.clone();
      for (int i = 0; i < fields; i++)
        if (together[i]) {
          if (nextDepths[i] == paths[i].length) {
            // the path ended on the array: the element is the value, a nested array included
            nextValues[i] = element;
            nextExpanded[i] = true;
          } else if (element instanceof Document)
            nextValues[i] = element;
          else {
            // a scalar element, or a nested array, has no field: null
            nextValues[i] = null;
            nextDepths[i] = paths[i].length;
            nextExpanded[i] = true;
          }
        }
      generate(nextValues, nextDepths, nextExpanded, best);
    }
  }

  private void offer(final Object[] candidate, final Object[][] best) {
    if (best[0] == null || compare(candidate, best[0]) < 0)
      best[0] = candidate;
  }

  private static int position(final String segment) {
    final int length = segment.length();
    if (length == 0 || length > 9)
      return -1;
    for (int i = 0; i < length; i++)
      if (segment.charAt(i) < '0' || segment.charAt(i) > '9')
        return -1;
    return Integer.parseInt(segment);
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

  private static int compareKeys(final Object a, final Object b) {
    final boolean emptyA = a instanceof EmptyArray;
    final boolean emptyB = b instanceof EmptyArray;
    if (emptyA || emptyB) {
      if (emptyA == emptyB)
        return 0;
      // MinKey stays below everything, the empty array included
      if (a instanceof MinKey || b instanceof MinKey)
        return a instanceof MinKey ? -1 : 1;
      return emptyA ? -1 : 1;
    }
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

    /**
     * @return the entry the offered one took the place of, or {@code null} when the buffer had room for it
     */
    Entry<T> offer(final Entry<T> entry) {
      heap.add(entry);
      return heap.size() > limit ? heap.poll() : null;
    }

    List<Entry<T>> sorted() {
      final List<Entry<T>> result = new ArrayList<>(heap);
      result.sort(order);
      return result;
    }
  }
}
