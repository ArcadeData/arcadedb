/*
 * Python-bindings bridge: one crossing where the per-value path takes several.
 *
 * A JPype method call costs a few microseconds of dispatch (overload
 * resolution and argument conversion), so small results and small parameter
 * maps are dominated by the NUMBER of calls, not by the data. Materializing
 * one row took hasNext/next, getPropertyNames, and one getProperty per
 * property. This does the same work Java-side in one call and hands Python
 * arrays, whose elements are read without method dispatch. (Parameters go the
 * other way, see DbCalls: a Python list converted to a Java array was slower
 * than the put() calls it saved, but the same values passed as the varargs of
 * one static method cross in a single call.)
 *
 * Values are returned as the engine's own objects, so Python converts them
 * exactly as before (full type fidelity, unlike RowBatcher's JSON).
 *
 * Compiled into arcadedb-python-bridge.jar during the wheel build
 * (scripts/Dockerfile.build and scripts/build-native.sh) and consumed by
 * Result.to_dict() in the Python bindings (ResultSet.to_list() and iter_dicts() read through TypedRows, and fall back to
 * nextRows only when that class is missing).
 */
package com.arcadedb.python;

import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public final class RowAccess {

  private RowAccess() {
  }

  /**
   * One row's property names and values, in the order getPropertyNames() gives them:
   * {@code {String[] names, Object[] values}}.
   */
  public static Object[] namesAndValues(final Result row) {
    final Set<String> names = row.getPropertyNames();
    final String[] n = names.toArray(new String[0]);
    final Object[] v = new Object[n.length];
    for (int i = 0; i < n.length; i++)
      v[i] = row.getProperty(n[i]);
    return new Object[] { n, v };
  }

  /**
   * Up to {@code max} rows, each as {@link #namesAndValues(Result)}; an empty array once the result set is drained.
   * For callers that consume the whole result: rows are taken from the result set as they are batched. Fewer than
   * {@code max} rows means the result set is drained, and it is closed here (see {@link RowBatcher#closeDrained}), so
   * a caller stops after a short batch and does not use the result set again.
   */
  public static Object[] nextRows(final ResultSet rs, final int max) {
    final List<Object[]> rows = new ArrayList<>(Math.min(max, 1024));
    while (rows.size() < max && rs.hasNext())
      rows.add(namesAndValues(rs.next()));
    if (rows.size() < max)
      RowBatcher.closeDrained(rs);
    return rows.toArray();
  }

  /**
   * The first row of a result set, or null when it is empty, and the result set is closed either way: the
   * {@code hasNext()}, {@code next()}, and {@code close()} of {@code ResultSet.first()} in one crossing instead of
   * three. An error in the statement surfaces from {@code hasNext()} or {@code next()} as before; a failing close is
   * ignored, as the Python close() ignores it.
   */
  public static Result firstAndClose(final ResultSet rs) {
    try {
      return rs.hasNext() ? rs.next() : null;
    } finally {
      try {
        rs.close();
      } catch (final Exception ignored) {
        // best-effort hygiene, like the Python close()
      }
    }
  }

  /**
   * {@code row.getProperty(name)} when {@code row.hasProperty(name)}, else null: the two calls of
   * {@code Result.get()} in one crossing.
   */
  public static Object propertyOrNull(final Result row, final String name) {
    return row.hasProperty(name) ? row.getProperty(name) : null;
  }
}
