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

import java.util.List;

/**
 * Adapts the historical {@code long[] timestamps + Object[][] columnValues} ingest form to
 * {@link TimeSeriesRowSource}, so both the boxed and the primitive callers converge on one append
 * loop in {@link TimeSeriesBucket}.
 * <p>
 * The adapter is a view: it holds the caller's arrays and unboxes on demand, exactly once per value,
 * at the point the bits are written to the page. No intermediate copy is made.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class ObjectColumnsRowSource implements TimeSeriesRowSource {

  private final long[]     timestamps;
  private final Object[][] columnValues;
  private final byte[]     kinds;

  /**
   * @param kinds the value-column kinds of the type, as returned by {@link #kindsOf(List)}. Held by
   *              reference: callers resolve it once per component, not once per append, so a
   *              single-sample append (the SQL INSERT path) allocates only this view.
   */
  ObjectColumnsRowSource(final byte[] kinds, final long[] timestamps, final Object[][] columnValues) {
    this.kinds = kinds;
    this.timestamps = timestamps;
    this.columnValues = columnValues;
  }

  /**
   * Resolves the {@link TimeSeriesBatch} kind of every non-timestamp column, in column order.
   */
  static byte[] kindsOf(final List<ColumnDefinition> typeColumns) {
    int valueColumns = 0;
    for (final ColumnDefinition col : typeColumns)
      if (col.getRole() != ColumnDefinition.ColumnRole.TIMESTAMP)
        valueColumns++;

    final byte[] kinds = new byte[valueColumns];
    int colIdx = 0;
    for (final ColumnDefinition col : typeColumns) {
      if (col.getRole() == ColumnDefinition.ColumnRole.TIMESTAMP)
        continue;
      kinds[colIdx++] = TimeSeriesBatch.kindOf(col.getDataType());
    }
    return kinds;
  }

  @Override
  public int size() {
    return timestamps.length;
  }

  @Override
  public long getTimestamp(final int row) {
    return timestamps[row];
  }

  @Override
  public long getRawValue(final int row, final int columnIndex) {
    final Object value = columnValues[columnIndex][row];
    final byte kind = kinds[columnIndex];

    if (kind == TimeSeriesBatch.KIND_BOOLEAN)
      return Boolean.TRUE.equals(value) ? 1L : 0L;
    if (value == null)
      return 0L;

    final Number number = (Number) value;
    return switch (kind) {
      case TimeSeriesBatch.KIND_DOUBLE -> Double.doubleToRawLongBits(number.doubleValue());
      case TimeSeriesBatch.KIND_FLOAT -> Float.floatToRawIntBits(number.floatValue());
      case TimeSeriesBatch.KIND_INTEGER -> number.intValue();
      case TimeSeriesBatch.KIND_SHORT -> number.shortValue();
      case TimeSeriesBatch.KIND_BYTE -> number.byteValue();
      default -> number.longValue();
    };
  }

  @Override
  public String getStringValue(final int row, final int columnIndex) {
    final Object value = columnValues[columnIndex][row];
    // A KIND_TEXT column has no native form in the row, so it is stored as its text form; a real
    // STRING column keeps the strict cast it always had.
    if (kinds[columnIndex] == TimeSeriesBatch.KIND_TEXT)
      return value != null ? value.toString() : null;
    return (String) value;
  }
}
