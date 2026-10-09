/*
 * Python-bindings bridge: a statement with named parameters in ONE crossing.
 *
 * Binding {@code {"id": 7, "v": 1}} from Python took a HashMap constructor call, one put() per key, a cast to the Map
 * overload, and then the overloaded command()/query() call: four to six JPype crossings, each a few microseconds of
 * dispatch, around an engine call that is itself 5 to 10 microseconds for a one-row statement. Here the flat
 * {@code key, value, key, value} list crosses as the varargs of ONE non-overloaded static method, and the map is built
 * Java-side. The values are boxed by JPype exactly as put(String, Object) boxed them, so the engine sees the same
 * parameter objects.
 *
 * Compiled into arcadedb-python-bridge.jar during the wheel build (scripts/Dockerfile.build and
 * scripts/build-native.sh) and consumed by Database.query() and Database.command() in the Python bindings.
 */
package com.arcadedb.python;

import com.arcadedb.database.Database;
import com.arcadedb.database.Record;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.HashMap;
import java.util.Map;

public final class DbCalls {

  private DbCalls() {
  }

  private static Map<String, Object> named(final Object[] keysAndValues) {
    final Map<String, Object> params = new HashMap<>(keysAndValues.length);
    for (int i = 0; i + 1 < keysAndValues.length; i += 2)
      params.put((String) keysAndValues[i], keysAndValues[i + 1]);
    return params;
  }

  /** {@code db.command(language, command, Map)} with the map given as {@code key, value, key, value, ...}. */
  public static ResultSet commandNamed(final Database db, final String language, final String command,
      final Object... keysAndValues) {
    return db.command(language, command, named(keysAndValues));
  }

  /** {@code db.query(language, command, Map)} with the map given as {@code key, value, key, value, ...}. */
  public static ResultSet queryNamed(final Database db, final String language, final String command,
      final Object... keysAndValues) {
    return db.query(language, command, named(keysAndValues));
  }

  /** {@code db.command(language, command, Object...)} with the positional parameters as the varargs. */
  public static ResultSet commandPositional(final Database db, final String language, final String command,
      final Object... params) {
    return db.command(language, command, params);
  }

  /** {@code db.query(language, command, Object...)} with the positional parameters as the varargs. */
  public static ResultSet queryPositional(final Database db, final String language, final String command,
      final Object... params) {
    return db.query(language, command, params);
  }

  /**
   * The first record of {@code db.lookupByKey(type, keys, values)}, or null when there is none: the lookup, hasNext(),
   * next(), and getRecord() of {@code Database.lookup_by_key()} in one crossing instead of four.
   */
  public static Record lookupFirst(final Database db, final String type, final String[] keys, final Object[] values) {
    final IndexCursor cursor = db.lookupByKey(type, keys, values);
    return cursor.hasNext() ? cursor.next().getRecord() : null;
  }
}
