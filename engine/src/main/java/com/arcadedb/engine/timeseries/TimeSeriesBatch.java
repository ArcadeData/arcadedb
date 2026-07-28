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

import com.arcadedb.schema.Type;

import java.util.List;

/**
 * Column-oriented, primitive-backed buffer of samples to be appended to a TimeSeries type.
 * <p>
 * Every fixed-width column is kept in a {@code long[]} holding the raw bits the mutable row format
 * stores, so filling a batch from primitive data allocates the column arrays once and nothing per
 * sample. This is the fast ingest path of issue #5474: the previous {@code Object[]}-per-column API
 * forced a caller with primitive samples to box each value into a {@code Double}/{@code Long} that
 * the engine unboxed a few frames later to write the very same bits.
 * <p>
 * Obtain one from {@link TimeSeriesEngine#newBatch(int)}, fill it row by row and hand it to
 * {@link TimeSeriesEngine#appendSamples(TimeSeriesRowSource)} or
 * {@link TimeSeriesEngine#appendBatch(TimeSeriesRowSource)}:
 * <pre>
 * final TimeSeriesBatch batch = engine.newBatch(points);
 * for (int i = 0; i &lt; points; i++) {
 *   final int row = batch.addRow(timestampMs[i]);
 *   batch.setString(row, 0, host[i]);   // TAG column 0
 *   batch.setDouble(row, 1, usage[i]);  // FIELD column 1
 * }
 * engine.appendBatch(batch);
 * </pre>
 * Column indexes are ordinals among the <b>non-timestamp</b> columns of the type, the same order
 * used by the {@code Object[][]} form of the append API. A column left untouched on a row keeps its
 * zero value, matching how the {@code Object[][]} path stores a {@code null}.
 * <p>
 * A batch is a plain buffer with no synchronization: fill it on one thread, then append it.
 * {@link #clear()} makes it reusable across batches without reallocating the column arrays.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 * @see TimeSeriesRowSource
 */
public class TimeSeriesBatch implements TimeSeriesRowSource {

  // Column kinds, resolved once at construction so the per-value setters switch on a dense int
  // instead of walking the Type enum.
  static final byte KIND_DOUBLE   = 0;
  static final byte KIND_FLOAT    = 1;
  static final byte KIND_LONG     = 2;
  static final byte KIND_INTEGER  = 3;
  static final byte KIND_SHORT    = 4;
  static final byte KIND_BYTE     = 5;
  static final byte KIND_BOOLEAN  = 6;
  static final byte KIND_STRING   = 7;
  // A declared type with no fixed width and no native form in the row: stored as its text form, the
  // only encoding the reader can walk past. CREATE TIMESERIES TYPE refuses these, so only a type
  // whose schema predates issue #5475 can still carry one.
  static final byte KIND_TEXT     = 8;

  private final ColumnDefinition[] columns;
  private final byte[]             kinds;
  private       long[]             timestamps;
  private final long[][]           rawValues;
  private final String[][]         stringValues;
  private       int                size;
  // Rows written by a previous fill, i.e. the prefix whose column slots still hold stale values and
  // must be reset by addRow(). Zero on a fresh batch, so filling one costs nothing per row.
  private       int                staleRows;

  /**
   * Creates a batch for the given type columns.
   *
   * @param typeColumns the full column list of the TimeSeries type, timestamp column included
   * @param capacity    expected number of samples; the batch grows on demand if exceeded
   */
  public TimeSeriesBatch(final List<ColumnDefinition> typeColumns, final int capacity) {
    int valueColumns = 0;
    for (final ColumnDefinition col : typeColumns)
      if (col.getRole() != ColumnDefinition.ColumnRole.TIMESTAMP)
        valueColumns++;

    this.columns = new ColumnDefinition[valueColumns];
    this.kinds = new byte[valueColumns];
    this.rawValues = new long[valueColumns][];
    this.stringValues = new String[valueColumns][];

    final int initialCapacity = Math.max(1, capacity);
    this.timestamps = new long[initialCapacity];

    int colIdx = 0;
    for (final ColumnDefinition col : typeColumns) {
      if (col.getRole() == ColumnDefinition.ColumnRole.TIMESTAMP)
        continue;
      columns[colIdx] = col;
      final byte kind = kindOf(col.getDataType());
      kinds[colIdx] = kind;
      // KIND_TEXT columns are stored length-prefixed in the row exactly like a STRING, so they need
      // the String backing too (issue #5475).
      if (kind == KIND_STRING || kind == KIND_TEXT)
        stringValues[colIdx] = new String[initialCapacity];
      else
        rawValues[colIdx] = new long[initialCapacity];
      colIdx++;
    }
  }

  /**
   * Starts a new sample and returns its row index, to be passed to the setters below.
   */
  public int addRow(final long timestamp) {
    if (size == timestamps.length)
      grow();

    final int row = size++;
    timestamps[row] = timestamp;

    // A refilled batch must not leak the previous fill's value into a column the caller skips.
    if (row < staleRows)
      for (int c = 0; c < columns.length; c++) {
        if (rawValues[c] != null)
          rawValues[c][row] = 0L;
        else
          stringValues[c][row] = null;
      }
    return row;
  }

  /**
   * Drops all samples, keeping the column arrays so the batch can be refilled without reallocating.
   */
  public void clear() {
    staleRows = Math.max(staleRows, size);
    size = 0;
  }

  public void setDouble(final int row, final int columnIndex, final double value) {
    rawColumn(columnIndex)[row] = switch (kinds[columnIndex]) {
      case KIND_DOUBLE -> Double.doubleToRawLongBits(value);
      case KIND_FLOAT -> Float.floatToRawIntBits((float) value);
      case KIND_BOOLEAN -> value != 0 ? 1L : 0L;
      default -> (long) value;
    };
  }

  public void setFloat(final int row, final int columnIndex, final float value) {
    rawColumn(columnIndex)[row] = switch (kinds[columnIndex]) {
      case KIND_FLOAT -> Float.floatToRawIntBits(value);
      case KIND_DOUBLE -> Double.doubleToRawLongBits(value);
      case KIND_BOOLEAN -> value != 0 ? 1L : 0L;
      default -> (long) value;
    };
  }

  public void setLong(final int row, final int columnIndex, final long value) {
    rawColumn(columnIndex)[row] = switch (kinds[columnIndex]) {
      case KIND_DOUBLE -> Double.doubleToRawLongBits(value);
      case KIND_FLOAT -> Float.floatToRawIntBits(value);
      case KIND_BOOLEAN -> value != 0 ? 1L : 0L;
      default -> value;
    };
  }

  public void setInt(final int row, final int columnIndex, final int value) {
    setLong(row, columnIndex, value);
  }

  public void setShort(final int row, final int columnIndex, final short value) {
    setLong(row, columnIndex, value);
  }

  public void setByte(final int row, final int columnIndex, final byte value) {
    setLong(row, columnIndex, value);
  }

  public void setBoolean(final int row, final int columnIndex, final boolean value) {
    setLong(row, columnIndex, value ? 1L : 0L);
  }

  public void setString(final int row, final int columnIndex, final String value) {
    if (kinds[columnIndex] != KIND_STRING && kinds[columnIndex] != KIND_TEXT)
      throw new IllegalArgumentException(
          "Column '" + columns[columnIndex].getName() + "' is of type " + columns[columnIndex].getDataType()
              + ", not STRING");
    stringValues[columnIndex][row] = value;
  }

  /**
   * Generic setter for callers that already hold a boxed value (SQL, line protocol, JSON payloads).
   * Applies the same coercion the {@code Object[][]} append path applies, so a batch filled this way
   * stores exactly what that path would have stored.
   */
  public void setValue(final int row, final int columnIndex, final Object value) {
    final byte kind = kinds[columnIndex];
    if (kind == KIND_STRING) {
      stringValues[columnIndex][row] = (String) value;
      return;
    }
    if (kind == KIND_TEXT) {
      stringValues[columnIndex][row] = value != null ? value.toString() : null;
      return;
    }
    if (kind == KIND_BOOLEAN) {
      rawValues[columnIndex][row] = Boolean.TRUE.equals(value) ? 1L : 0L;
      return;
    }
    if (value == null) {
      rawValues[columnIndex][row] = 0L;
      return;
    }

    final Number number = (Number) value;
    rawValues[columnIndex][row] = switch (kind) {
      case KIND_DOUBLE -> Double.doubleToRawLongBits(number.doubleValue());
      case KIND_FLOAT -> Float.floatToRawIntBits(number.floatValue());
      case KIND_INTEGER -> number.intValue();
      case KIND_SHORT -> number.shortValue();
      case KIND_BYTE -> number.byteValue();
      default -> number.longValue();
    };
  }

  /**
   * Number of non-timestamp columns addressable by the setters.
   */
  public int getColumnCount() {
    return columns.length;
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public long getTimestamp(final int row) {
    return timestamps[row];
  }

  @Override
  public long getRawValue(final int row, final int columnIndex) {
    return rawValues[columnIndex][row];
  }

  @Override
  public String getStringValue(final int row, final int columnIndex) {
    return stringValues[columnIndex][row];
  }

  private long[] rawColumn(final int columnIndex) {
    final long[] column = rawValues[columnIndex];
    if (column == null)
      throw new IllegalArgumentException("Column '" + columns[columnIndex].getName() + "' of type "
          + columns[columnIndex].getDataType() + " is stored as text, use setString()");
    return column;
  }

  private void grow() {
    final int newCapacity = timestamps.length + (timestamps.length >> 1) + 1;
    final long[] newTimestamps = new long[newCapacity];
    System.arraycopy(timestamps, 0, newTimestamps, 0, size);
    timestamps = newTimestamps;

    for (int c = 0; c < columns.length; c++) {
      if (rawValues[c] != null) {
        final long[] grown = new long[newCapacity];
        System.arraycopy(rawValues[c], 0, grown, 0, size);
        rawValues[c] = grown;
      } else {
        final String[] grown = new String[newCapacity];
        System.arraycopy(stringValues[c], 0, grown, 0, size);
        stringValues[c] = grown;
      }
    }
  }

  static byte kindOf(final Type type) {
    return switch (type) {
      case DOUBLE -> KIND_DOUBLE;
      case FLOAT -> KIND_FLOAT;
      case LONG, DATETIME, DATE, DATETIME_SECOND, DATETIME_MICROS, DATETIME_NANOS -> KIND_LONG;
      case INTEGER -> KIND_INTEGER;
      case SHORT -> KIND_SHORT;
      case BYTE -> KIND_BYTE;
      case BOOLEAN -> KIND_BOOLEAN;
      case STRING -> KIND_STRING;
      default -> KIND_TEXT;
    };
  }
}
