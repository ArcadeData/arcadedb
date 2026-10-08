/*
 * Python-bindings bridge: bulk document insertion.
 *
 * Database.insert_many() from Python would otherwise pay ~5 JNI calls per
 * row (newDocument + set per property + save), which caps ingest around
 * 30-130k rows/s regardless of engine speed. This helper accepts the rows
 * as ONE JSON string and loops Java-side, so the whole batch costs one bulk
 * string copy plus the engine's own write path.
 *
 * Two modes: transactional batches on the calling thread (commitEvery), or
 * the async executor's parallel bucket writers (insertManyJsonParallel; the
 * Python insert_many wrapper waits for completion itself, then reads the
 * failures the writers reported). insertColumns takes whole columns instead of
 * JSON (Database.insert_columns). The boxDoubles/boxLongs
 * helpers below serve AsyncExecutor.append_samples' numpy fast path.
 *
 * JSON-representable property values only (str/int/float/bool/null and
 * nested lists/maps thereof) — the Python side falls back to the per-row
 * new_document path for anything else (e.g. datetime, bytes).
 */
package com.arcadedb.python;

import com.arcadedb.database.Database;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.async.ErrorCallback;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public final class DocumentBatcher {

  private DocumentBatcher() {
  }

  public static long insertManyJson(final Database db, final String typeName, final String jsonRows,
      final int commitEvery, final boolean parallel) {
    final JSONArray rows = new JSONArray(jsonRows);
    final int n = rows.length();
    if (parallel) {
      insertManyJsonParallel(db, typeName, rows);
      return n;
    }
    // A caller's own transaction is the caller's to commit or roll back: batch
    // commits and the failure rollback apply only to the transactions opened
    // here, matching the Python per-row fallback (#7882).
    final boolean wasActive = db.isTransactionActive();
    if (!wasActive)
      db.begin();
    try {
      for (int i = 0; i < n; i++) {
        final MutableDocument doc = db.newDocument(typeName);
        fill(doc, rows.getJSONObject(i));
        doc.save();
        if (!wasActive && commitEvery > 0 && (i + 1) % commitEvery == 0) {
          db.commit();
          db.begin();
        }
      }
      if (!wasActive)
        db.commit();
    } catch (final Throwable e) {
      if (!wasActive && db.isTransactionActive()) {
        try {
          db.rollback();
        } catch (final Throwable rollbackError) {
          e.addSuppressed(rollbackError);
        }
      }
      throw e;
    }
    return n;
  }

  /**
   * Columnar insert: the documents are built Java-side from whole columns, so each column crosses the FFI once (a long[] or
   * double[] copied from a numpy buffer, a boolean[], or an Object[] of Strings, boxed numbers, and nulls) instead of one
   * JSON text per batch (bindings issue #150: 2.24x over insertManyJson on the first
   * 2,000,000 TPC-H SF1 line items, nine typed properties, commit every 10,000, same sums and count).
   *
   * Same failure contract as insertManyJson (#7882): batch commits and the failure rollback apply only to a transaction
   * opened here; a caller's own transaction is the caller's to commit or roll back.
   *
   * @param columns one array per name, each of length n: long[], double[], boolean[] or Object[]; a null element sets
   *                the property to null, as a JSON null does on the insertManyJson path
   */
  public static long insertColumns(final Database db, final String typeName, final String[] names, final Object[] columns,
      final int n, final int commitEvery) {
    checkColumns(names, columns, n);
    final boolean wasActive = db.isTransactionActive();
    if (!wasActive)
      db.begin();
    try {
      for (int i = 0; i < n; i++) {
        final MutableDocument doc = db.newDocument(typeName);
        fillRow(doc, names, columns, i);
        doc.save();
        if (!wasActive && commitEvery > 0 && (i + 1) % commitEvery == 0) {
          db.commit();
          db.begin();
        }
      }
      if (!wasActive)
        db.commit();
    } catch (final Throwable e) {
      if (!wasActive && db.isTransactionActive()) {
        try {
          db.rollback();
        } catch (final Throwable rollbackError) {
          e.addSuppressed(rollbackError);
        }
      }
      throw e;
    }
    return n;
  }

  /**
   * The parallel twin of insertColumns: each document is built Java-side from the columns and handed to the async executor's
   * bucket writers, as insertManyJsonParallel does with parsed JSON rows. The caller waits for completion, then reads the
   * failures the writers reported (a record they reject reaches only the error callback).
   */
  public static AsyncFailures insertColumnsParallel(final Database db, final String typeName, final String[] names,
      final Object[] columns, final int n) {
    checkColumns(names, columns, n);
    final AsyncFailures failures = new AsyncFailures();
    final ErrorCallback onError = failures::record;
    for (int i = 0; i < n; i++) {
      final MutableDocument doc = db.newDocument(typeName);
      fillRow(doc, names, columns, i);
      db.async().createRecord(doc, null, onError);
    }
    return failures;
  }

  private static void checkColumns(final String[] names, final Object[] columns, final int n) {
    final int k = names.length;
    if (columns.length != k)
      throw new IllegalArgumentException("columns has " + columns.length + " arrays for " + k + " names");
    for (int c = 0; c < k; c++) {
      final Object col = columns[c];
      final int len;
      if (col instanceof long[] a)
        len = a.length;
      else if (col instanceof double[] a)
        len = a.length;
      else if (col instanceof boolean[] a)
        len = a.length;
      else if (col instanceof Object[] a)
        len = a.length;
      else
        throw new IllegalArgumentException("column '" + names[c] + "' is a " + (col == null ? "null" : col.getClass().getName())
            + ", not a long[], double[], boolean[] or Object[]");
      if (len != n)
        throw new IllegalArgumentException("column '" + names[c] + "' has " + len + " values for " + n + " rows");
    }
  }

  private static void fillRow(final MutableDocument doc, final String[] names, final Object[] columns, final int i) {
    for (int c = 0; c < names.length; c++) {
      final Object col = columns[c];
      final Object v;
      if (col instanceof long[] a)
        v = a[i];
      else if (col instanceof double[] a)
        v = a[i];
      else if (col instanceof boolean[] a)
        v = a[i];
      else
        v = ((Object[]) col)[i];
      doc.set(names[c], v);
    }
  }

  /**
   * The failures the async writers reported for one parallel load. A record the writers reject (a duplicate key, a
   * failed batch commit that abandons every record buffered with it) reaches only the per-record error callback and the
   * executor's global one, which by default just logs: without this the load returned its input row count while
   * dropping records (ArcadeData/arcadedb#8478: register an error callback "so a failed record can't pass silently").
   * Read it after waitCompletion().
   */
  public static final class AsyncFailures {
    private final AtomicLong                 count = new AtomicLong();
    private final AtomicReference<Throwable> first = new AtomicReference<>();

    void record(final Throwable exception) {
      count.incrementAndGet();
      first.compareAndSet(null, exception);
    }

    public long getCount() {
      return count.get();
    }

    public String getFirstMessage() {
      final Throwable t = first.get();
      return t == null ? null : t.toString();
    }
  }

  public static AsyncFailures insertManyJsonParallel(final Database db, final String typeName, final String jsonRows) {
    return insertManyJsonParallel(db, typeName, new JSONArray(jsonRows));
  }

  private static AsyncFailures insertManyJsonParallel(final Database db, final String typeName, final JSONArray rows) {
    final AsyncFailures failures = new AsyncFailures();
    final ErrorCallback onError = failures::record;
    final int n = rows.length();
    for (int i = 0; i < n; i++) {
      final MutableDocument doc = db.newDocument(typeName);
      fill(doc, rows.getJSONObject(i));
      db.async().createRecord(doc, null, onError);
    }
    return failures;
  }

  /** Box primitive columns Java-side so numpy arrays can cross the FFI as
   * one buffer copy and still feed Object[]-typed engine APIs (e.g.
   * TimeSeriesEngine.appendSamples). */
  public static Object[] boxDoubles(final double[] a) {
    final Object[] out = new Object[a.length];
    for (int i = 0; i < a.length; i++)
      out[i] = a[i];
    return out;
  }

  public static Object[] boxLongs(final long[] a) {
    final Object[] out = new Object[a.length];
    for (int i = 0; i < a.length; i++)
      out[i] = a[i];
    return out;
  }

  private static void fill(final MutableDocument doc, final JSONObject row) {
    // toMap() turns nested objects and arrays into plain Map and List. Storing the parsed JSONArray itself reads back
    // as a JSONArray inside the transaction that wrote it, and only becomes a list once the record is serialized.
    for (final Map.Entry<String, Object> entry : row.toMap().entrySet())
      doc.set(entry.getKey(), entry.getValue());
  }
}
