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
   */
  public static TagFilter eq(final int columnIndex, final Object value) {
    final List<Condition> conditions = new ArrayList<>(1);
    conditions.add(new Condition(columnIndex, Set.of(value)));
    return new TagFilter(conditions);
  }

  /**
   * Creates a filter matching a single tag against a set of values (IN).
   */
  public static TagFilter in(final int columnIndex, final Set<Object> values) {
    final List<Condition> conditions = new ArrayList<>(1);
    conditions.add(new Condition(columnIndex, values));
    return new TagFilter(conditions);
  }

  /**
   * Returns a new TagFilter that ANDs this filter with an additional tag equality condition.
   */
  public TagFilter and(final int columnIndex, final Object value) {
    final List<Condition> newConditions = new ArrayList<>(conditions.size() + 1);
    newConditions.addAll(conditions);
    newConditions.add(new Condition(columnIndex, Set.of(value)));
    return new TagFilter(newConditions);
  }

  /**
   * Returns a new TagFilter that ANDs this filter with an additional IN condition.
   */
  public TagFilter andIn(final int columnIndex, final Set<Object> values) {
    final List<Condition> newConditions = new ArrayList<>(conditions.size() + 1);
    newConditions.addAll(conditions);
    newConditions.add(new Condition(columnIndex, values));
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
   *
   * @param row the sample row (index 0 = timestamp, index 1+ = columns)
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

  record Condition(int columnIndex, Set<Object> values) {
  }

  List<Condition> getConditions() {
    return conditions;
  }
}
