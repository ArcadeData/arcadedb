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
import com.arcadedb.exception.ValidationException;
import com.arcadedb.schema.LocalTimeSeriesType;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * The rows of one ingestion batch, converted into the column-major grid {@link TimeSeriesEngine#appendBatch(long[], Object[][])}
 * takes, with the rows whose values a column cannot store left out and named (issue #10030).
 * <p>
 * This is the conversion contract of the client-facing ingestion endpoints in one place: every raw value goes through
 * {@link ColumnDefinition#storableValue}, a row carrying a value its column refuses is dropped alone - before anything of it reaches
 * the engine, whose row source would otherwise cast it in the middle of the append and fail the whole request (issue #9809) - and
 * the reason is reported as {@code <measurement>.<column> (tag|field): <reason>}, capped at
 * {@link TimeSeriesGateway#MAX_REPORTED_UNREADABLE_VALUES}. {@link TimeSeriesGateway#write} (line protocol and gRPC) and the
 * Prometheus remote-write handler both build their batches here, so the two cannot drift apart again (issue #9972 had to fix the
 * second one by hand).
 * <p>
 * A row is refused at its FIRST unreadable column, in schema order, and the columns after it are not read: one reason per refused
 * row. A value the column can store but a constraint declared on the column refuses (MANDATORY, NOTNULL, MIN, MAX, REGEXP, see
 * {@link TimeSeriesColumnConstraints}) refuses its row the same way, the reason naming the constraint (issue #10238).
 * <p>
 * <b>This is the only way the client-facing paths reach the engine's raw-grid {@code appendBatch}</b> (issue #10107): the grid
 * {@link #convert} builds is appended with {@link #appendTo}, and no production code outside this class and the engine calls
 * {@code appendBatch(long[], Object[][])} with a grid of its own. The JSONL importer was the third caller and the only one that
 * handed the engine the raw values of its source, so text for a typed column aborted the whole import; a fourth path would have
 * been in the same position, which {@code Issue10107AppendBatchCallersConvertTest} now rules out. The embedded async API was the
 * fourth (issue #10381): the two {@code requireStorable} overloads, for a boxed grid and for a primitive source, are its two doors (the
 * second held it only to the declared constraints until issue #10487), and the census now looks for
 * every entry point into the engine's storage, not for one method name.
 * <p>
 * <b>Shared tags.</b> A caller whose TAG values are the same on every row of the batch - Prometheus, where the labels belong to the
 * series and not to the sample - says so with {@code sharedTags}. Each TAG column is then read and converted once, from row 0, and
 * broadcast down the column, so a series of N samples pays for its labels once instead of N times; a tag its column refuses refuses
 * every row of the batch, which {@link #sharedValueRefused()} reports. Shared columns are converted before any row is read.
 */
public final class TimeSeriesStorableRows {

  /**
   * Hands over the raw, not yet converted value of one value column on one row; {@code null} is an absent value. Called at most
   * once per (row, column), and not at all for the columns after the one that refused a row.
   */
  @FunctionalInterface
  public interface RawValues {
    /**
     * @param row         the row, in {@code [0, rows)}
     * @param valueColumn the column's ordinal among the non-timestamp columns, the index of {@link #columnValues()}
     * @param column      the column itself
     */
    Object get(int row, int valueColumn, ColumnDefinition column);
  }

  private final long[]     timestamps;
  private final Object[][] columnValues;
  private final int        size;
  private final int        refused;
  private final boolean    sharedValueRefused;

  private TimeSeriesStorableRows(final long[] timestamps, final Object[][] columnValues, final int size, final int refused,
      final boolean sharedValueRefused) {
    this.timestamps = timestamps;
    this.columnValues = columnValues;
    this.size = size;
    this.refused = refused;
    this.sharedValueRefused = sharedValueRefused;
  }

  /**
   * Converts {@code rows} rows of {@code type} into the grid the engine appends, holding every converted value to the constraints
   * the type's columns declare now (issue #10238): a row that violates one is refused like a row carrying a value its column cannot
   * store, and reported the same way.
   *
   * @param database    the database the type lives in, which reads a DATE/DATETIME value given as text
   * @param measurement the name the reasons are reported under
   * @param type        the type the rows are appended to
   * @param timestamps  the timestamp of every row, at least {@code rows} long. Compacted IN PLACE when a row is refused, so the
   *                    caller must not read it afterwards: {@link #timestamps()} is the array to append
   * @param rows        how many rows the batch holds
   * @param sharedTags  whether every TAG column carries the same value on every row (see the class javadoc)
   * @param raw         where the raw values come from
   * @param unreadable  where the reasons of the refused rows are added, up to {@link TimeSeriesGateway#MAX_REPORTED_UNREADABLE_VALUES}
   *                    entries in all
   */
  public static TimeSeriesStorableRows convert(final Database database, final String measurement, final LocalTimeSeriesType type,
      final long[] timestamps, final int rows, final boolean sharedTags, final RawValues raw, final Set<String> unreadable) {
    return convert(database, measurement, type.getTsColumns(), TimeSeriesColumnConstraints.of(database, type), timestamps, rows,
        sharedTags, raw, unreadable);
  }

  /**
   * {@link #convert(Database, String, LocalTimeSeriesType, long[], int, boolean, RawValues, Set)} over a bare column list.
   *
   * @param columns     the full column list of the type, timestamp column included
   * @param constraints the constraints of the columns, {@code null} for none
   */
  static TimeSeriesStorableRows convert(final Database database, final String measurement, final List<ColumnDefinition> columns,
      final TimeSeriesColumnConstraints constraints, final long[] timestamps, final int rows, final boolean sharedTags,
      final RawValues raw, final Set<String> unreadable) {
    ColumnDefinition timestampColumn = null;
    final ColumnDefinition[] valueColumns = new ColumnDefinition[columns.size() - 1]; // exclude timestamp
    int next = 0;
    for (final ColumnDefinition col : columns)
      if (col.getRole() != ColumnDefinition.ColumnRole.TIMESTAMP)
        valueColumns[next++] = col;
      else
        timestampColumn = col;

    final Object[][] columnValues = new Object[valueColumns.length][rows];

    // Shared columns first: a refusal there refuses the whole batch, so no row is read at all
    Object[] sharedValues = null;
    if (sharedTags) {
      final long deadline = constraints != null ? constraints.sampleDeadline() : 0L;
      for (int c = 0; c < valueColumns.length; c++) {
        final ColumnDefinition col = valueColumns[c];
        if (col.getRole() != ColumnDefinition.ColumnRole.TAG)
          continue;
        if (sharedValues == null)
          sharedValues = new Object[valueColumns.length];
        String reason;
        try {
          sharedValues[c] = col.storableValue(database, raw.get(0, c, col));
          reason = constraints != null ? constraints.check(c, sharedValues[c], deadline) : null;
        } catch (final IllegalArgumentException e) {
          reason = e.getMessage();
        }
        if (reason != null) {
          addReason(unreadable, measurement, col, reason);
          return new TimeSeriesStorableRows(new long[0], new Object[valueColumns.length][0], 0, rows, true);
        }
      }
    }

    // `valid` is the next free row, so a refused row leaves a partial row that the next row overwrites in full: that holds only
    // because every non-shared column is assigned on every pass, a null included, so keep it so
    int valid = 0;
    for (int row = 0; row < rows; row++) {
      final long deadline = constraints != null ? constraints.sampleDeadline() : 0L;
      boolean refused = false;
      if (constraints != null) {
        final String reason = constraints.checkTimestamp(timestamps[row], deadline);
        if (reason != null) {
          addReason(unreadable, measurement, timestampColumn, reason);
          continue;
        }
      }
      for (int c = 0; c < valueColumns.length; c++) {
        final ColumnDefinition col = valueColumns[c];
        if (sharedValues != null && col.getRole() == ColumnDefinition.ColumnRole.TAG)
          continue;
        final Object value = raw.get(row, c, col);
        // Only the conversion is guarded: an IllegalArgumentException from the caller's RawValues is not a column problem
        String reason;
        try {
          columnValues[c][valid] = col.storableValue(database, value);
          reason = constraints != null ? constraints.check(c, columnValues[c][valid], deadline) : null;
        } catch (final IllegalArgumentException e) {
          reason = e.getMessage();
        }
        if (reason != null) {
          addReason(unreadable, measurement, col, reason);
          refused = true;
          break;
        }
      }
      if (!refused)
        timestamps[valid++] = timestamps[row];
    }

    long[] keptTimestamps = timestamps;
    if (valid < rows) {
      // Only when a row was refused, so the copy is paid by the failing request and never by a clean one
      keptTimestamps = Arrays.copyOf(timestamps, valid);
      for (int c = 0; c < columnValues.length; c++)
        columnValues[c] = Arrays.copyOf(columnValues[c], valid);
    } else if (timestamps.length != rows)
      keptTimestamps = Arrays.copyOf(timestamps, rows);

    if (sharedValues != null)
      for (int c = 0; c < valueColumns.length; c++)
        if (valueColumns[c].getRole() == ColumnDefinition.ColumnRole.TAG)
          Arrays.fill(columnValues[c], sharedValues[c]);

    return new TimeSeriesStorableRows(keptTimestamps, columnValues, valid, rows - valid, false);
  }

  /**
   * The boxed rows of an embedded caller - {@code DatabaseAsyncExecutor.appendSamples(String, long[], Object[][])} - converted and
   * held to the constraints of the type like the rows of every other surface, as the source the shard appends (issue #10381). It
   * used to hand the engine the caller's grid as it was, so a value a column refuses was stored when a constraint refused it and
   * thrown away, with the whole batch, inside the worker when the column could not store it, where nobody could see it.
   * <p>
   * All or nothing, and on the calling thread: a batch with one refused row is not appended at all, and says which and why, as the
   * statement of an {@code INSERT} does. The caller's arrays are not touched and not retained, so it may refill them at once.
   *
   * @param timestamps   the timestamp of every row
   * @param columnValues one array per non-timestamp column in schema order, each as long as {@code timestamps}; a {@code null} array
   *                     is a column with no value on any row
   *
   * @throws IllegalArgumentException when the grid does not have the shape of the type
   * @throws ValidationException      when a row is refused, naming every reason (at most
   *                                  {@link TimeSeriesGateway#MAX_REPORTED_UNREADABLE_VALUES})
   */
  public static TimeSeriesRowSource requireStorable(final Database database, final LocalTimeSeriesType type,
      final TimeSeriesEngine engine, final long[] timestamps, final Object[][] columnValues) {
    final int rows = timestamps.length;
    final int valueColumns = type.getTsColumns().size() - 1; // exclude timestamp
    if (columnValues.length != valueColumns)
      throw new IllegalArgumentException("The TIMESERIES type '" + type.getName() + "' has " + valueColumns + " value columns (tags and fields)"
          + " but " + columnValues.length + " arrays of values were given");
    for (int c = 0; c < valueColumns; c++)
      if (columnValues[c] != null && columnValues[c].length < rows)
        throw new IllegalArgumentException("The value column " + c + " of the TIMESERIES type '" + type.getName() + "' has "
            + columnValues[c].length + " values for " + rows + " timestamps");

    final Set<String> unreadable = new LinkedHashSet<>();
    // convert() compacts the timestamps in place when it refuses a row, so it gets its own copy: the caller's array stays the caller's
    final TimeSeriesStorableRows stored = convert(database, type.getName(), type, timestamps.clone(), rows, false,
        (row, valueColumn, column) -> columnValues[valueColumn] != null ? columnValues[valueColumn][row] : null, unreadable);
    if (stored.refused() > 0)
      throw refused(type, stored.refused(), rows, unreadable);
    return engine.newRowSource(stored.timestamps(), stored.columnValues());
  }

  /**
   * Holds a primitive source - {@code DatabaseAsyncExecutor.appendSamples(String, TimeSeriesRowSource)} - to what the type's columns
   * can store and to the constraints the type declares (issues #10381, #10487). A source does NOT carry values its columns can
   * store by construction: a text column takes whatever {@link TimeSeriesBatch#setString} was given, and a TAG or STRING text over
   * {@link TimeSeriesBucket#MAX_STRING_BYTES} UTF-8 bytes failed inside the worker, with the whole batch, conforming rows
   * included, and nobody to tell. The numeric and boolean cells are the ones the typed setters wrote, so they need no conversion;
   * the text cells are read once each on the calling thread and held to the limit with the code every other surface uses
   * ({@link ColumnDefinition#storableValue}).
   * <p>
   * A type with no text column and no declared constraint pays one pass over its columns and no read of the source. A type with a
   * text column (nearly every type: a TAG is one) reads its text cells and nothing else, a length multiplication per cell for a
   * short text; a type that declares constraints reads every cell of the source once (a value boxed per cell): the price of
   * refusing a sample where the caller can see it.
   * <p>
   * All or nothing, as {@link #requireStorable(Database, LocalTimeSeriesType, TimeSeriesEngine, long[], Object[][])} is: a batch
   * with one refused row is not appended at all.
   *
   * @throws ValidationException when a row is refused, naming every reason (at most
   *                             {@link TimeSeriesGateway#MAX_REPORTED_UNREADABLE_VALUES}); nothing is appended
   */
  public static void requireStorable(final Database database, final LocalTimeSeriesType type, final TimeSeriesRowSource source) {
    final TimeSeriesColumnConstraints constraints = TimeSeriesColumnConstraints.of(database, type);

    final List<ColumnDefinition> columns = type.getTsColumns();
    final ColumnDefinition[] valueColumns = new ColumnDefinition[columns.size() - 1];
    final byte[] kinds = new byte[valueColumns.length];
    ColumnDefinition timestampColumn = null;
    boolean hasText = false;
    int next = 0;
    for (final ColumnDefinition column : columns)
      if (column.getRole() == ColumnDefinition.ColumnRole.TIMESTAMP)
        timestampColumn = column;
      else {
        final byte kind = TimeSeriesBatch.kindOf(column.getDataType());
        kinds[next] = kind;
        hasText |= kind == TimeSeriesBatch.KIND_STRING || kind == TimeSeriesBatch.KIND_TEXT;
        valueColumns[next++] = column;
      }
    if (constraints == null && !hasText)
      return;

    final Set<String> unreadable = new LinkedHashSet<>();
    final int rows = source.size();
    int refused = 0;
    for (int row = 0; row < rows; row++) {
      final long deadline = constraints != null ? constraints.sampleDeadline() : 0L;
      String reason = constraints != null ? constraints.checkTimestamp(source.getTimestamp(row), deadline) : null;
      if (reason != null)
        addReason(unreadable, type.getName(), timestampColumn, reason);
      else
        for (int c = 0; c < valueColumns.length && reason == null; c++) {
          final boolean text = kinds[c] == TimeSeriesBatch.KIND_STRING || kinds[c] == TimeSeriesBatch.KIND_TEXT;
          if (constraints == null && !text)
            continue;
          final Object value = storedValue(source, row, c, kinds[c]);
          // The result of storableValue is dropped on purpose: the source keeps the text it holds, which is what the shard stores. That
          // holds while storableValue only checks a STRING/TEXT value and never rewrites it (trims it, normalizes it); a rewrite would
          // have to be applied to the source here, or what is validated would not be what is stored
          if (text)
            try {
              valueColumns[c].storableValue(database, value);
            } catch (final IllegalArgumentException e) {
              reason = e.getMessage();
            }
          if (reason == null && constraints != null)
            reason = constraints.check(c, value, deadline);
          if (reason != null)
            addReason(unreadable, type.getName(), valueColumns[c], reason);
        }
      if (reason != null)
        refused++;
    }
    if (refused > 0)
      throw refused(type, refused, rows, unreadable);
  }

  /** The value a row of a primitive source stores in a column, as {@link ColumnDefinition#storableValue} would have returned it. */
  private static Object storedValue(final TimeSeriesRowSource source, final int row, final int valueColumn, final byte kind) {
    return switch (kind) {
      case TimeSeriesBatch.KIND_STRING, TimeSeriesBatch.KIND_TEXT -> source.getStringValue(row, valueColumn);
      case TimeSeriesBatch.KIND_DOUBLE -> Double.longBitsToDouble(source.getRawValue(row, valueColumn));
      case TimeSeriesBatch.KIND_FLOAT -> Float.intBitsToFloat((int) source.getRawValue(row, valueColumn));
      case TimeSeriesBatch.KIND_BOOLEAN -> source.isNull(row, valueColumn) ? null : source.getRawValue(row, valueColumn) != 0L;
      default -> source.isNull(row, valueColumn) ? null : (Object) source.getRawValue(row, valueColumn);
    };
  }

  private static ValidationException refused(final LocalTimeSeriesType type, final int refused, final int rows,
      final Set<String> unreadable) {
    return new ValidationException("Cannot append the samples to the TIMESERIES type '" + type.getName() + "': " + refused + " of "
        + rows + " rows are refused and none was appended - " + String.join("; ", unreadable));
  }

  private static void addReason(final Set<String> unreadable, final String measurement, final ColumnDefinition col,
      final String reason) {
    // The reason names the column and its type, or the constraint, never the value, which comes from the client
    if (unreadable.size() < TimeSeriesGateway.MAX_REPORTED_UNREADABLE_VALUES)
      unreadable.add(measurement + "." + col.getName() + " (" + switch (col.getRole()) {
        case TAG -> "tag";
        case TIMESTAMP -> "timestamp";
        default -> "field";
      } + "): " + reason);
  }

  /**
   * Appends the rows kept to {@code engine}. A batch with nothing kept appends nothing.
   *
   * @throws IOException when the engine cannot append the rows
   */
  public void appendTo(final TimeSeriesEngine engine) throws IOException {
    if (size > 0)
      engine.appendBatch(timestamps, columnValues);
  }

  /** The timestamps of the rows kept, {@link #size()} long. */
  public long[] timestamps() {
    return timestamps;
  }

  /** The converted values of the rows kept, one array of {@link #size()} values per non-timestamp column, in schema order. */
  public Object[][] columnValues() {
    return columnValues;
  }

  /** How many rows were kept. Zero means there is nothing to append. */
  public int size() {
    return size;
  }

  /** How many rows were refused, each for a value its column cannot store. */
  public int refused() {
    return refused;
  }

  /** Whether the whole batch was refused because a shared TAG value cannot be stored (always false without shared tags). */
  public boolean sharedValueRefused() {
    return sharedValueRefused;
  }
}
