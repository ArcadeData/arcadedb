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

import com.arcadedb.database.Database;
import com.arcadedb.schema.Type;

import java.util.Arrays;
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
 * used by the {@code Object[][]} form of the append API. A column left untouched on a row reads back as
 * what a {@code null} reads back as on that column - the absent marker on a floating-point column, and on an
 * integral, boolean or datetime one a sample with no value ({@link #isNull}, issue #9721) - which is the same
 * thing the {@code Object[][]} path stores for a {@code null} (issue #7743, see {@link #rawNull}).
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
  /** {@link TimeSeriesNaN#ABSENT} in the raw forms {@link #rawNull} hands back (issue #7743). */
  private static final long ABSENT_DOUBLE_BITS = Double.doubleToRawLongBits(TimeSeriesNaN.ABSENT);
  private static final long ABSENT_FLOAT_BITS  = Float.floatToRawIntBits((float) TimeSeriesNaN.ABSENT);

  // ORDER MATTERS: the numeric kinds (DOUBLE up to BYTE, the datetimes being LONG) come first and BOOLEAN, STRING and TEXT after
  // them, which ColumnDefinition.storableValue relies on (kind <= KIND_BYTE means "numeric"); KindOrderingTest pins it.
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

  private final Database           database;
  private final ColumnDefinition[] columns;
  private final byte[]             kinds;
  /** {@link #rawNull} per column, precomputed: the raw bits a column with no value set reads back as. */
  private final long[]             nullRaw;
  /**
   * The columns whose "no value" is something other than zero, which is the only work a FRESH row needs: a
   * new {@code long[]} is already zero everywhere else. Empty - the common all-integer schema - means a fresh
   * row costs nothing at all, as it did before issue #7743 (code review on PR #7747).
   */
  private final int[]              absentMarkerColumns;
  private       long[]             timestamps;
  private final long[][]           rawValues;
  /**
   * Per column whose absence is not a value ({@link #tracksNull}), a bitmap of the rows a value was SET on (issue #9721); null for
   * every other column. A set bit is a value, so a row nothing was written to is absent without any work on the row itself:
   * {@link #clear()} zeroes the words it used, and a fresh row lies past them.
   */
  private final long[][]           present;
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
    this(typeColumns, capacity, null);
  }

  /**
   * Creates a batch for the given type columns, whose {@link #setValue} reads a date or an instant given as text through
   * {@code database}, with its formats and its time zone, as an {@code INSERT} does.
   *
   * @param typeColumns the full column list of the TimeSeries type, timestamp column included
   * @param capacity    expected number of samples; the batch grows on demand if exceeded
   * @param database    the database of the type, or {@code null} to read such text with the default formats and the JVM's time zone,
   *                    which names another instant than an {@code INSERT} does whenever that zone is not UTC. Use
   *                    {@link TimeSeriesEngine#newBatch(int)}, which passes it
   */
  public TimeSeriesBatch(final List<ColumnDefinition> typeColumns, final int capacity, final Database database) {
    this.database = database;
    int valueColumns = 0;
    for (final ColumnDefinition col : typeColumns)
      if (col.getRole() != ColumnDefinition.ColumnRole.TIMESTAMP)
        valueColumns++;

    this.columns = new ColumnDefinition[valueColumns];
    this.kinds = new byte[valueColumns];
    this.nullRaw = new long[valueColumns];
    this.rawValues = new long[valueColumns][];
    this.present = new long[valueColumns][];
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
      nullRaw[colIdx] = rawNull(kind);
      // KIND_TEXT columns are stored length-prefixed in the row exactly like a STRING, so they need
      // the String backing too (issue #5475).
      if (kind == KIND_STRING || kind == KIND_TEXT)
        stringValues[colIdx] = new String[initialCapacity];
      else
        rawValues[colIdx] = new long[initialCapacity];
      if (tracksNull(kind))
        present[colIdx] = new long[bitmapWords(initialCapacity)];
      colIdx++;
    }

    int absentMarkers = 0;
    for (final long raw : nullRaw)
      if (raw != 0L)
        ++absentMarkers;
    this.absentMarkerColumns = new int[absentMarkers];
    for (int c = 0, next = 0; c < nullRaw.length; c++)
      if (nullRaw[c] != 0L)
        absentMarkerColumns[next++] = c;
  }

  /**
   * Starts a new sample and returns its row index, to be passed to the setters below.
   */
  public int addRow(final long timestamp) {
    if (size == timestamps.length)
      grow();

    final int row = size++;
    timestamps[row] = timestamp;

    // Two reasons to fill the row in: a refilled batch must not leak the previous fill's value into a column the
    // caller skips, and a column the caller never sets must read back as what a null reads back as - the absent
    // marker where the column can carry one, zero elsewhere, where the missing presence bit says there is no value
    // (issues #7743, #9721). The second reason applies to a fresh row too, which is why the cheap "only stale rows"
    // test is not the whole condition; a batch with no floating-point column has nothing to write into a fresh row and
    // skips the loop as it always did. The presence bits need no work here: clear() zeroed every word a fill used.
    if (row < staleRows) {
      // A refilled row: every column has to be reset, whatever its null reads back as.
      for (int c = 0; c < columns.length; c++) {
        if (rawValues[c] != null)
          rawValues[c][row] = nullRaw[c];
        else
          stringValues[c][row] = null;
      }
    } else
      // A fresh row is already zero everywhere, so only the columns whose absence is NOT zero are touched -
      // none at all on an all-integer schema, which is the bulk-ingest case this class is built for.
      //
      // "Already zero" is an invariant of this class, not just of the JVM: a fresh row is one at an index no
      // fill has reached, and every backing array it can live in is freshly ALLOCATED - by the constructor or by
      // grow(), which copies into a new array rather than reusing one. A growth path that ever recycled storage
      // would have to fill these rows in full, like the stale branch above (code review on PR #7747).
      for (final int c : absentMarkerColumns)
        rawValues[c][row] = nullRaw[c];
    return row;
  }

  /**
   * Drops all samples, keeping the column arrays so the batch can be refilled without reallocating.
   */
  public void clear() {
    staleRows = Math.max(staleRows, size);
    final int words = bitmapWords(size);
    for (final long[] bits : present)
      if (bits != null)
        Arrays.fill(bits, 0, words, 0L);
    size = 0;
  }

  /**
   * Whether a column of this kind has no value of its own to spend on absence, so its absence is tracked per row (issue #9721):
   * the integral, boolean and datetime kinds. A floating-point column stores the NaN marker and a text column the empty string.
   */
  static boolean tracksNull(final byte kind) {
    return kind == KIND_LONG || kind == KIND_INTEGER || kind == KIND_SHORT || kind == KIND_BYTE || kind == KIND_BOOLEAN;
  }

  private static int bitmapWords(final int rows) {
    return (rows + 63) >>> 6;
  }

  private void markPresent(final int row, final int columnIndex) {
    final long[] bits = present[columnIndex];
    if (bits != null)
      bits[row >>> 6] |= 1L << row;
  }

  public void setDouble(final int row, final int columnIndex, final double value) {
    markPresent(row, columnIndex);
    rawColumn(columnIndex)[row] = switch (kinds[columnIndex]) {
      case KIND_DOUBLE -> Double.doubleToRawLongBits(value);
      case KIND_FLOAT -> Float.floatToRawIntBits((float) value);
      case KIND_BOOLEAN -> value != 0 ? 1L : 0L;
      default -> (long) value;
    };
  }

  public void setFloat(final int row, final int columnIndex, final float value) {
    markPresent(row, columnIndex);
    rawColumn(columnIndex)[row] = switch (kinds[columnIndex]) {
      case KIND_FLOAT -> Float.floatToRawIntBits(value);
      case KIND_DOUBLE -> Double.doubleToRawLongBits(value);
      case KIND_BOOLEAN -> value != 0 ? 1L : 0L;
      default -> (long) value;
    };
  }

  public void setLong(final int row, final int columnIndex, final long value) {
    markPresent(row, columnIndex);
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

  /**
   * Sets a text column. The text is stored as given, which keeps this setter free of any per-value work: a text over
   * {@link TimeSeriesBucket#MAX_STRING_BYTES} UTF-8 bytes is refused where the batch is appended, on the calling thread of
   * {@code DatabaseAsyncExecutor#appendSamples(String, TimeSeriesRowSource)} and by the engine's own append (issue #10487). Use
   * {@link #setValue} to have the text checked here.
   */
  public void setString(final int row, final int columnIndex, final String value) {
    if (kinds[columnIndex] != KIND_STRING && kinds[columnIndex] != KIND_TEXT)
      throw new IllegalArgumentException(
          "Column '" + columns[columnIndex].getName() + "' is of type " + columns[columnIndex].getDataType()
              + ", not STRING");
    stringValues[columnIndex][row] = value;
  }

  /**
   * Generic setter for callers that already hold a boxed value (SQL, line protocol, JSON payloads).
   * Converts the value like {@link ColumnDefinition#storableValue} does, the conversion every surface that takes a boxed value
   * applies, so a batch filled this way stores exactly what the {@code Object[][]} path and the SQL {@code INSERT} would have
   * stored (issue #10487):
   * <ul>
   * <li>a number must be in the range of an INTEGER, SHORT or BYTE column - a long past it is refused, not stored modulo 2^32 -
   * and numeric text is parsed for a number column;</li>
   * <li>a BOOLEAN column takes a boolean or the text {@code true} / {@code false}, and refuses anything else rather than
   * storing {@code false};</li>
   * <li>a STRING column takes a text of at most {@link TimeSeriesBucket#MAX_STRING_BYTES} UTF-8 bytes;</li>
   * <li>a DATE/DATETIME column takes an epoch count in its unit, or a date or an instant in any form the engine reads.</li>
   * </ul>
   *
   * @throws IllegalArgumentException naming the column, when the column cannot store the value. The row is left as it was before
   *                                  the call for that column, so the caller may set the column again
   */
  public void setValue(final int row, final int columnIndex, final Object value) {
    final byte kind = kinds[columnIndex];
    final Object stored = columns[columnIndex].storableValue(database, value);
    if (kind == KIND_STRING || kind == KIND_TEXT) {
      stringValues[columnIndex][row] = (String) stored;
      return;
    }
    if (stored == null) {
      rawValues[columnIndex][row] = rawNull(kind);
      final long[] bits = present[columnIndex];
      if (bits != null)
        bits[row >>> 6] &= ~(1L << row);
      return;
    }
    markPresent(row, columnIndex);
    if (kind == KIND_BOOLEAN) {
      rawValues[columnIndex][row] = (Boolean) stored ? 1L : 0L;
      return;
    }

    final Number number = (Number) stored;
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
   * The raw bits a {@code null} measurement takes in a column of this kind (issue #7743).
   * <p>
   * "No measurement here" is what a caller means by a null field value, and on a floating-point column that is
   * exactly what {@link TimeSeriesNaN#ABSENT} says: every aggregate skips it, so an AVG divides by the samples
   * that were really taken and a MIN is the smallest of them rather than a phantom zero. The bits survive the
   * round trip on both layers - the mutable page stores them verbatim and {@code GORILLA_XOR} encodes a double
   * by its bits - so the two layers keep answering alike, which is the property {@link TimeSeriesNaN#asMeasurement}
   * exists to hold.
   * <p>
   * Every other numeric column stores zero for it, because an integer has no value outside its own range to
   * spend on absence: what says the zero is no measurement is the row's null bitmap (issue #9721), fed by
   * {@link #isNull}. A type created before that format has no bitmap and still reads such a null back as a real 0.
   */
  static long rawNull(final byte kind) {
    return switch (kind) {
      case KIND_DOUBLE -> ABSENT_DOUBLE_BITS;
      case KIND_FLOAT -> ABSENT_FLOAT_BITS;
      default -> 0L;
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

  @Override
  public boolean isNull(final int row, final int columnIndex) {
    final long[] bits = present[columnIndex];
    return bits != null && (bits[row >>> 6] & (1L << row)) == 0;
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
      if (present[c] != null && present[c].length < bitmapWords(newCapacity))
        present[c] = Arrays.copyOf(present[c], bitmapWords(newCapacity));
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
