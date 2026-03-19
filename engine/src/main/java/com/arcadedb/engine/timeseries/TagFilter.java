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
package com.arcadedb.engine.timeseries;

import com.arcadedb.utility.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Predicate for tag column filtering. Supports multiple tag conditions ANDed together.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class TagFilter {

  private final List<Condition> conditions;

  private TagFilter(final List<Condition> conditions) {
    this.conditions = conditions;
  }

  /**
   * Creates a filter matching a single tag equality.
   *
   * @param nonTsColumnIndex zero-based column index excluding the timestamp column.
   *                         In {@link #matches(Object[])}, this is offset by +1 to account
   *                         for the timestamp at row[0].
   * @param value            the value to match against
   */
  public static TagFilter eq(final int nonTsColumnIndex, final Object value) {
    final List<Condition> conditions = new ArrayList<>(1);
    conditions.add(new Condition(nonTsColumnIndex, CollectionUtils.singletonSet(value)));
    return new TagFilter(conditions);
  }

  /**
   * Creates a filter matching a single tag against a set of values (IN).
   *
   * @param nonTsColumnIndex zero-based column index excluding the timestamp column
   * @param values           the set of values to match against
   */
  public static TagFilter in(final int nonTsColumnIndex, final Set<Object> values) {
    final List<Condition> conditions = new ArrayList<>(1);
    conditions.add(new Condition(nonTsColumnIndex, values));
    return new TagFilter(conditions);
  }

  /**
   * Returns a new TagFilter that ANDs this filter with an additional tag equality condition.
   *
   * @param nonTsColumnIndex zero-based column index excluding the timestamp column
   * @param value            the value to match against
   */
  public TagFilter and(final int nonTsColumnIndex, final Object value) {
    final List<Condition> newConditions = new ArrayList<>(conditions.size() + 1);
    newConditions.addAll(conditions);
    newConditions.add(new Condition(nonTsColumnIndex, CollectionUtils.singletonSet(value)));
    return new TagFilter(newConditions);
  }

  /**
   * Returns a new TagFilter that ANDs this filter with an additional IN condition.
   *
   * @param nonTsColumnIndex zero-based column index excluding the timestamp column
   * @param values           the set of values to match against
   */
  public TagFilter andIn(final int nonTsColumnIndex, final Set<Object> values) {
    final List<Condition> newConditions = new ArrayList<>(conditions.size() + 1);
    newConditions.addAll(conditions);
    newConditions.add(new Condition(nonTsColumnIndex, values));
    return new TagFilter(newConditions);
  }

  /**
   * Returns the column index of the first condition (for backward compatibility).
   */
  public int getColumnIndex() {
    return conditions.isEmpty() ? -1 : conditions.getFirst().columnIndex;
  }

  /**
   * Returns the number of conditions in this filter.
   */
  public int getConditionCount() {
    return conditions.size();
  }

  /**
   * Tests if a sample row matches all conditions in this filter.
   * Assumes the row was built from <em>all</em> non-timestamp columns in schema order:
   * {@code row[0] = timestamp, row[1] = non-ts col 0, row[2] = non-ts col 1, ...}
   *
   * @param row the sample row (index 0 = timestamp, index 1+ = columns in full schema order)
   */
  public boolean matches(final Object[] row) {
    for (final Condition cond : conditions) {
      if (cond.columnIndex + 1 >= row.length)
        return false;
      if (!cond.values.contains(row[cond.columnIndex + 1]))
        return false;
    }
    return true;
  }

  /**
   * Tests if a sample row matches all conditions in this filter, resolving column positions
   * through the supplied {@code columnIndices} mapping.
   * <p>
   * Use this overload when the row was built from a <em>subset</em> of columns (i.e.
   * {@code columnIndices != null} was passed to {@code scanRange} / {@code iterateRange}).
   * In that case {@code row[i+1]} holds the column whose non-timestamp schema index equals
   * {@code columnIndices[i]}, so a direct {@code cond.columnIndex+1} offset would be wrong.
   * <p>
   * Falls back to {@link #matches(Object[])} when {@code columnIndices} is {@code null}
   * (all columns present in schema order).
   *
   * @param row           the sample row (index 0 = timestamp, index 1+ = selected columns)
   * @param columnIndices the non-timestamp schema indices that were used to build the row,
   *                      in ascending order; {@code null} means all columns in schema order
   */
  public boolean matchesMapped(final Object[] row, final int[] columnIndices) {
    if (columnIndices == null)
      return matches(row);
    for (final Condition cond : conditions) {
      int outPos = -1;
      for (int i = 0; i < columnIndices.length; i++) {
        if (columnIndices[i] == cond.columnIndex) {
          outPos = i;
          break;
        }
      }
      if (outPos < 0)
        return false; // tag column was not included in the requested subset
      if (outPos + 1 >= row.length)
        return false;
      if (!cond.values.contains(row[outPos + 1]))
        return false;
    }
    return true;
  }

  record Condition(int columnIndex, Set<Object> values) {
  }

  List<Condition> getConditions() {
    return conditions;
  }
}
