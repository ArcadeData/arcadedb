/*
 * Python-bindings bridge: primitive columnar append into a native TIMESERIES type.
 *
 * AsyncExecutor.append_samples' original path hands the engine Object[] columns, so every
 * numeric sample is boxed once on the way in and unboxed immediately on the way out
 * (ArcadeDB issue #5474: 7.8M dead Double allocations per TSBS ingest). The engine now
 * accepts a TimeSeriesBatch of primitive columns instead.
 *
 * Filling that batch row-by-row from Python would cost one JNI call per value, which is far
 * worse than the boxing it replaces. These helpers move the loop to the Java side: one FFI
 * crossing per column, each carrying a contiguous primitive array copied straight out of the
 * caller's numpy buffer.
 */
package com.arcadedb.python;

import com.arcadedb.database.Database;
import com.arcadedb.engine.timeseries.ColumnDefinition;
import com.arcadedb.engine.timeseries.TimeSeriesBatch;
import com.arcadedb.schema.LocalTimeSeriesType;

import java.util.List;

public final class TimeSeriesBatcher {

  private TimeSeriesBatcher() {
  }

  /**
   * Allocates a batch for the type and opens one row per timestamp. Column values are filled
   * afterwards by the setters below, which is why the rows must exist first.
   */
  public static TimeSeriesBatch newBatch(final Database db, final String typeName, final long[] timestamps) {
    final LocalTimeSeriesType type = (LocalTimeSeriesType) db.getSchema().getType(typeName);
    final List<ColumnDefinition> columns = type.getTsColumns();
    final TimeSeriesBatch batch = new TimeSeriesBatch(columns, timestamps.length);
    for (final long ts : timestamps)
      batch.addRow(ts);
    return batch;
  }

  public static void setDoubleColumn(final TimeSeriesBatch batch, final int columnIndex, final double[] values) {
    for (int row = 0; row < values.length; row++)
      batch.setDouble(row, columnIndex, values[row]);
  }

  public static void setLongColumn(final TimeSeriesBatch batch, final int columnIndex, final long[] values) {
    for (int row = 0; row < values.length; row++)
      batch.setLong(row, columnIndex, values[row]);
  }

  public static void setStringColumn(final TimeSeriesBatch batch, final int columnIndex, final String[] values) {
    for (int row = 0; row < values.length; row++)
      batch.setString(row, columnIndex, values[row]);
  }

  /**
   * Fallback for a column whose Python values are not a single primitive kind (mixed types, or a
   * declared type the batch stores as text). Values arrive already converted to Java objects.
   */
  public static void setObjectColumn(final TimeSeriesBatch batch, final int columnIndex, final Object[] values) {
    for (int row = 0; row < values.length; row++)
      batch.setValue(row, columnIndex, values[row]);
  }
}
