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

import java.io.IOException;
import java.util.Arrays;
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
 * row.
 * <p>
 * <b>This is the only way the client-facing paths reach the engine's raw-grid {@code appendBatch}</b> (issue #10107): the grid
 * {@link #convert} builds is appended with {@link #appendTo}, and no production code outside this class and the engine calls
 * {@code appendBatch(long[], Object[][])} with a grid of its own. The JSONL importer was the third caller and the only one that
 * handed the engine the raw values of its source, so text for a typed column aborted the whole import; a fourth path would have
 * been in the same position, which {@code Issue10107AppendBatchCallersConvertTest} now rules out.
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
   * Converts {@code rows} rows into the grid the engine appends.
   *
   * @param database    the database the type lives in, which reads a DATE/DATETIME value given as text
   * @param measurement the name the reasons are reported under
   * @param columns     the full column list of the type, timestamp column included
   * @param timestamps  the timestamp of every row, at least {@code rows} long. Compacted IN PLACE when a row is refused, so the
   *                    caller must not read it afterwards: {@link #timestamps()} is the array to append
   * @param rows        how many rows the batch holds
   * @param sharedTags  whether every TAG column carries the same value on every row (see the class javadoc)
   * @param raw         where the raw values come from
   * @param unreadable  where the reasons of the refused rows are added, up to {@link TimeSeriesGateway#MAX_REPORTED_UNREADABLE_VALUES}
   *                    entries in all
   */
  public static TimeSeriesStorableRows convert(final Database database, final String measurement, final List<ColumnDefinition> columns,
      final long[] timestamps, final int rows, final boolean sharedTags, final RawValues raw, final Set<String> unreadable) {
    final ColumnDefinition[] valueColumns = new ColumnDefinition[columns.size() - 1]; // exclude timestamp
    int next = 0;
    for (final ColumnDefinition col : columns)
      if (col.getRole() != ColumnDefinition.ColumnRole.TIMESTAMP)
        valueColumns[next++] = col;

    final Object[][] columnValues = new Object[valueColumns.length][rows];

    // Shared columns first: a refusal there refuses the whole batch, so no row is read at all
    Object[] sharedValues = null;
    if (sharedTags) {
      for (int c = 0; c < valueColumns.length; c++) {
        final ColumnDefinition col = valueColumns[c];
        if (col.getRole() != ColumnDefinition.ColumnRole.TAG)
          continue;
        if (sharedValues == null)
          sharedValues = new Object[valueColumns.length];
        try {
          sharedValues[c] = col.storableValue(database, raw.get(0, c, col));
        } catch (final IllegalArgumentException e) {
          addReason(unreadable, measurement, col, e);
          return new TimeSeriesStorableRows(new long[0], new Object[valueColumns.length][0], 0, rows, true);
        }
      }
    }

    // `valid` is the next free row, so a refused row leaves a partial row that the next row overwrites in full: that holds only
    // because every non-shared column is assigned on every pass, a null included, so keep it so
    int valid = 0;
    for (int row = 0; row < rows; row++) {
      boolean refused = false;
      for (int c = 0; c < valueColumns.length; c++) {
        final ColumnDefinition col = valueColumns[c];
        if (sharedValues != null && col.getRole() == ColumnDefinition.ColumnRole.TAG)
          continue;
        final Object value = raw.get(row, c, col);
        // Only the conversion is guarded: an IllegalArgumentException from the caller's RawValues is not a column problem
        try {
          columnValues[c][valid] = col.storableValue(database, value);
        } catch (final IllegalArgumentException e) {
          addReason(unreadable, measurement, col, e);
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

  private static void addReason(final Set<String> unreadable, final String measurement, final ColumnDefinition col,
      final IllegalArgumentException e) {
    // The reason names the column and its type, never the value, which comes from the client
    if (unreadable.size() < TimeSeriesGateway.MAX_REPORTED_UNREADABLE_VALUES)
      unreadable.add(measurement + "." + col.getName() + " (" + (col.getRole() == ColumnDefinition.ColumnRole.TAG ? "tag" : "field")
          + "): " + e.getMessage());
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
