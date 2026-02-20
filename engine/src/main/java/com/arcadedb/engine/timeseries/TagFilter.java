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

import java.util.Set;

/**
 * Simple predicate for tag column filtering.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class TagFilter {

  private final int        columnIndex; // index among non-timestamp columns
  private final Set<Object> values;

  private TagFilter(final int columnIndex, final Set<Object> values) {
    this.columnIndex = columnIndex;
    this.values = values;
  }

  public static TagFilter eq(final int columnIndex, final Object value) {
    return new TagFilter(columnIndex, Set.of(value));
  }

  public static TagFilter in(final int columnIndex, final Set<Object> values) {
    return new TagFilter(columnIndex, values);
  }

  public int getColumnIndex() {
    return columnIndex;
  }

  /**
   * Tests if a sample row matches this filter.
   *
   * @param row the sample row (index 0 = timestamp, index 1+ = columns)
   */
  public boolean matches(final Object[] row) {
    if (columnIndex + 1 >= row.length)
      return false;
    return values.contains(row[columnIndex + 1]);
  }
}
