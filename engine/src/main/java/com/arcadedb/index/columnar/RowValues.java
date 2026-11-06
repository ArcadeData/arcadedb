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

/**
 * Reusable holder of one row's values, one slot per column of a {@link SegmentWriter}, owned and refilled by the caller. Only the
 * field of the column's kind is read: {@link #ordinal} for ORDINAL, {@link #list} with {@link #listCount} for ORDINAL_LIST (strictly
 * ascending ordinals), {@link #longs} for LONG, {@link #doubles} for DOUBLE; a slot with {@link #present} false is missing.
 */
final class RowValues {
  final int[]     ordinal;
  final int[][]   list;
  final int[]     listCount;
  final long[]    longs;
  final double[]  doubles;
  final boolean[] present;

  RowValues(final int columns) {
    ordinal = new int[columns];
    list = new int[columns][];
    listCount = new int[columns];
    longs = new long[columns];
    doubles = new double[columns];
    present = new boolean[columns];
  }

  /**
   * Validates the slot of column {@code k} for the column's kind: an ORDINAL is non-negative, an ORDINAL_LIST has a count within its
   * array and strictly ascending non-negative ordinals. A missing slot is always valid.
   *
   * @throws IllegalArgumentException when the slot breaks a rule
   */
  void check(final int k, final ColumnDefinition column) {
    if (!present[k])
      return;
    switch (column.kind()) {
    case ORDINAL -> {
      if (ordinal[k] < 0)
        throw new IllegalArgumentException("Negative ordinal " + ordinal[k] + " for column " + column.property());
    }
    case ORDINAL_LIST -> {
      final int[] values = list[k];
      final int count = listCount[k];
      if (count < 0 || count > 0 && (values == null || count > values.length))
        throw new IllegalArgumentException("Invalid list count " + count + " for column " + column.property());
      for (int i = 0; i < count; i++)
        if (values[i] < 0 || i > 0 && values[i] <= values[i - 1])
          throw new IllegalArgumentException("List ordinals of column " + column.property() + " must be non-negative and strictly ascending");
    }
    default -> {
    }
    }
  }
}
