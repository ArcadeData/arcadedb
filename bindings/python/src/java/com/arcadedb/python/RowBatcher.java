/*
 * Python-bindings bridge: batched row transport.
 *
 * Materializing large result sets from Python via JPype costs 2+C boundary
 * crossings per row (hasNext/next + one getProperty per column), which
 * dominates wide scans (measured 15-21x slower than Java-native iteration).
 * This helper serializes up to `max` rows into ONE JSON-array string
 * Java-side, so Python pays a single crossing per batch and parses with the
 * C-fast json module (measured ~6x faster end-to-end, ~2.7x of Java-native).
 *
 * Rows are serialized property-by-property into the engine's JSONObject,
 * which since 26.7.2 (#4967) serializes primitive arrays like float[] as
 * real JSON arrays natively.
 *
 * This class has NO temporal encoding of its own (#10522): every date and
 * date-time is written by JSONObject.put, the writer the engine uses for the
 * rows of a projection and for the members of a list or a map of any row, and
 * ColumnBatcher's epoch milliseconds are the same numbers. A DATE is the epoch
 * milliseconds of its UTC midnight, never a count of days, whatever the JVM
 * time zone. The one place the engine writes days is the top level of a
 * record's own toJSON() (Result.to_json() of a SELECT FROM T row), which is
 * the form fromJSON() reads back for a DATE column; to_json_list() is not that
 * round trip, and test_temporal_writers_agree.py pins both.
 *
 * Compiled into arcadedb-python-bridge.jar during the wheel build
 * (scripts/Dockerfile.build and scripts/build-native.sh) and consumed by
 * ResultSet.to_json_list() in the Python bindings. Values carry JSON-native
 * types (DATE and DATETIME as epoch-millisecond integers), as documented on
 * the Python side.
 */
package com.arcadedb.python;

import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONObject;

public final class RowBatcher {

  private RowBatcher() {
  }

  /**
   * Serialize up to {@code max} rows of the result set into a JSON array
   * string. Returns fewer than {@code max} rows only when the result set is
   * drained, so a caller can stop after a short batch instead of calling again
   * to see {@code "[]"}, and a drained result set is closed here, so the caller must not use it again.
   *
   * <p>The builder starts at the default size and grows: a 64 KB start was
   * allocated on every call and cost 2.5x the engine lookup on a one-row
   * result, while a large batch grows the default buffer at no measurable cost
   * (0.97x to 1.00x on a 9,892-row scan).
   */
  public static String nextJsonBatch(final ResultSet rs, final int max) {
    final StringBuilder sb = new StringBuilder();
    sb.append('[');
    int n = 0;
    while (n < max && rs.hasNext()) {
      final Result row = rs.next();
      if (n > 0)
        sb.append(',');
      appendRow(sb, row);
      n++;
    }
    if (n < max)
      closeDrained(rs);
    return sb.append(']').toString();
  }

  /**
   * The result set is drained, so the caller will not come back: close it here instead of making Python cross the
   * bridge once more just to call close() (a JPype call costs 3 to 4 microseconds, a fifth of a one-row read).
   * Best effort, like the Python-side close it replaces.
   */
  static void closeDrained(final ResultSet rs) {
    try {
      rs.close();
    } catch (final Exception ignored) {
      // closing is hygiene; the rows were read
    }
  }

  private static void appendRow(final StringBuilder sb, final Result row) {
    final JSONObject obj = new JSONObject();
    for (final String property : row.getPropertyNames()) {
      // No conversion of our own: JSONObject.put anchors a LocalDate at UTC midnight whatever the JVM zone (#9037), writes a
      // java.util.Calendar as its epoch milliseconds like a Date (#9630), and writes a date past the range of a long of
      // milliseconds as its ISO text (#10483), where the encoder this replaced threw and failed the whole batch. The members
      // of a list or a map go the same way
      final Object value = row.getProperty(property);
      obj.put(property, value);
    }
    sb.append(obj);
  }
}
