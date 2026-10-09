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

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
    for (final String property : row.getPropertyNames())
      obj.put(property, utcDates(row.getProperty(property)));
    sb.append(obj);
  }

  /**
   * JSONObject.put writes a LocalDate as the epoch milliseconds of midnight in the JVM's default time zone, while
   * Result.toJSON() and a DATETIME (a UTC wall clock) are written in UTC, so the same row read twice gave two integers
   * and decoded to the previous day east of UTC. A DATE is written as midnight UTC here, at the top level and inside a
   * list or map. A collection without a LocalDate is returned as it is.
   */
  private static Object utcDates(final Object value) {
    if (value instanceof LocalDate date)
      return date.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
    if (value instanceof List<?> list) {
      List<Object> copy = null;
      for (int i = 0; i < list.size(); i++) {
        final Object element = list.get(i);
        final Object converted = utcDates(element);
        if (converted != element && copy == null)
          copy = new ArrayList<>(list);
        if (copy != null)
          copy.set(i, converted);
      }
      return copy != null ? copy : value;
    }
    if (value instanceof Map<?, ?> map) {
      Map<Object, Object> copy = null;
      for (final Map.Entry<?, ?> entry : map.entrySet()) {
        final Object converted = utcDates(entry.getValue());
        if (converted != entry.getValue() && copy == null)
          copy = new LinkedHashMap<>(map);
        if (copy != null)
          copy.put(entry.getKey(), converted);
      }
      return copy != null ? copy : value;
    }
    return value;
  }
}
