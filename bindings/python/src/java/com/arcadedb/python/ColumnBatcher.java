/*
 * Python-bindings bridge: binary columnar transport.
 *
 * Encodes up to `max` rows of a ResultSet into ONE byte[]: a JSON header
 * (column names/types/sizes) followed by per-column buffers — fixed-width
 * little-endian for numerics/bools/temporals (epoch millis), offset+UTF-8
 * for strings, plus a null bitmap per column. Columns whose values don't
 * fit those encodings are serialized as a JSON array ("json" type) using
 * RowBatcher's normalization. Python decodes with numpy.frombuffer
 * (ResultSet.to_columns / the fast to_dataframe path). Measured ~1.2x of
 * Java-native iteration on 100k-row scans (vs 3.4x for the JSON row path).
 */
package com.arcadedb.python;

import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class ColumnBatcher {

  private ColumnBatcher() {
  }

  /** Wall-clock-consistent epoch millis: naive temporals are read as UTC so
   * numpy's (naive) datetime64 shows the same wall-clock the record carries. */
  private static long toEpochMillis(final Object v) {
    if (v instanceof java.util.Date d)
      return d.getTime();
    if (v instanceof java.time.LocalDateTime ldt)
      return ldt.toInstant(java.time.ZoneOffset.UTC).toEpochMilli();
    if (v instanceof java.time.LocalDate ld)
      return ld.atStartOfDay().toInstant(java.time.ZoneOffset.UTC).toEpochMilli();
    if (v instanceof java.time.Instant i)
      return i.toEpochMilli();
    if (v instanceof java.time.ZonedDateTime z)
      return z.toInstant().toEpochMilli();
    if (v instanceof java.time.OffsetDateTime o)
      return o.toInstant().toEpochMilli();
    throw new IllegalArgumentException("not a temporal: " + v.getClass());
  }

  public static byte[] nextColumnBatch(final ResultSet rs, final int max, final String joinedColumns) {
    // empty joinedColumns: derive the column set from the first row
    String[] columns = joinedColumns.isEmpty() ? null : joinedColumns.split(";");
    final List<Result> results = new ArrayList<>(Math.min(max, 1 << 16));
    while (results.size() < max && rs.hasNext())
      results.add(rs.next());
    if (columns == null) {
      if (results.isEmpty())
        columns = new String[0];
      else
        columns = results.get(0).getPropertyNames().toArray(new String[0]);
    }
    final int n = columns.length;
    final List<Object[]> rows = new ArrayList<>(results.size());
    for (final Result row : results) {
      final Object[] vals = new Object[n];
      for (int c = 0; c < n; c++)
        vals[c] = row.getProperty(columns[c]);
      rows.add(vals);
    }
    final int count = rows.size();

    final StringBuilder header = new StringBuilder("{\"count\":" + count + ",\"cols\":[");
    final ByteArrayOutputStream body = new ByteArrayOutputStream(count * n * 8);

    for (int c = 0; c < n; c++) {
      // null bitmap
      final byte[] nulls = new byte[(count + 7) / 8];
      // detect the column type across ALL values; mixed/unsupported -> json
      String type = null;
      for (int r = 0; r < count; r++) {
        final Object v = rows.get(r)[c];
        if (v == null)
          continue;
        final String t;
        if (v instanceof Long || v instanceof Integer || v instanceof Short || v instanceof Byte)
          t = "i8";
        else if (v instanceof Double || v instanceof Float)
          t = "f8";
        else if (v instanceof Boolean)
          t = "b1";
        else if (v instanceof java.util.Date || v instanceof java.time.LocalDateTime
            || v instanceof java.time.LocalDate || v instanceof java.time.Instant
            || v instanceof java.time.ZonedDateTime)
          t = "dt";
        else if (v instanceof String || v instanceof Character)
          t = "str";
        else
          t = "json";
        if (type == null)
          type = t;
        else if (!type.equals(t)) {
          // numeric widening is fine; anything else degrades to json
          if ((type.equals("i8") && t.equals("f8")) || (type.equals("f8") && t.equals("i8")))
            type = "f8";
          else {
            type = "json";
            break;
          }
        }
      }
      if (type == null)
        type = "str"; // all-null column

      byte[] colBuf;
      if (type.equals("i8") || type.equals("dt")) {
        final ByteBuffer bb = ByteBuffer.allocate(count * 8).order(ByteOrder.LITTLE_ENDIAN);
        for (int r = 0; r < count; r++) {
          final Object v = rows.get(r)[c];
          if (v == null) {
            nulls[r >> 3] |= (1 << (r & 7));
            bb.putLong(0);
          } else if (type.equals("dt"))
            bb.putLong(toEpochMillis(v));
          else
            bb.putLong(((Number) v).longValue());
        }
        colBuf = bb.array();
      } else if (type.equals("f8")) {
        final ByteBuffer bb = ByteBuffer.allocate(count * 8).order(ByteOrder.LITTLE_ENDIAN);
        for (int r = 0; r < count; r++) {
          final Object v = rows.get(r)[c];
          if (v == null) {
            nulls[r >> 3] |= (1 << (r & 7));
            bb.putDouble(Double.NaN);
          } else
            bb.putDouble(((Number) v).doubleValue());
        }
        colBuf = bb.array();
      } else if (type.equals("b1")) {
        final ByteBuffer bb = ByteBuffer.allocate(count);
        for (int r = 0; r < count; r++) {
          final Object v = rows.get(r)[c];
          if (v == null) {
            nulls[r >> 3] |= (1 << (r & 7));
            bb.put((byte) 0);
          } else
            bb.put((byte) (((Boolean) v) ? 1 : 0));
        }
        colBuf = bb.array();
      } else if (type.equals("json")) {
        // one JSON array for the whole column (JSONObject/JSONArray handle
        // primitive arrays natively since engine 26.7.2, #4967)
        final com.arcadedb.serializer.json.JSONArray arr = new com.arcadedb.serializer.json.JSONArray();
        for (int r = 0; r < count; r++) {
          final Object v = rows.get(r)[c];
          if (v == null)
            nulls[r >> 3] |= (1 << (r & 7));
          arr.put(v);
        }
        colBuf = arr.toString().getBytes(StandardCharsets.UTF_8);
      } else { // strings: int32 offsets (count+1) then utf8 bytes
        final ByteArrayOutputStream chars = new ByteArrayOutputStream(count * 16);
        final ByteBuffer offs = ByteBuffer.allocate((count + 1) * 4).order(ByteOrder.LITTLE_ENDIAN);
        int pos = 0;
        offs.putInt(0);
        for (int r = 0; r < count; r++) {
          final Object v = rows.get(r)[c];
          if (v == null)
            nulls[r >> 3] |= (1 << (r & 7));
          else {
            final byte[] b = v.toString().getBytes(StandardCharsets.UTF_8);
            chars.write(b, 0, b.length);
            pos += b.length;
          }
          offs.putInt(pos);
        }
        final byte[] charBytes = chars.toByteArray();
        final ByteBuffer bb = ByteBuffer.allocate(offs.capacity() + charBytes.length);
        bb.put(offs.array());
        bb.put(charBytes);
        colBuf = bb.array();
      }

      if (c > 0)
        header.append(',');
      header.append("{\"name\":\"").append(columns[c]).append("\",\"type\":\"").append(type)
          .append("\",\"nulls\":").append(nulls.length).append(",\"bytes\":").append(colBuf.length).append('}');
      body.write(nulls, 0, nulls.length);
      body.write(colBuf, 0, colBuf.length);
    }
    header.append("]}");

    final byte[] headerBytes = header.toString().getBytes(StandardCharsets.UTF_8);
    final byte[] bodyBytes = body.toByteArray();
    final ByteBuffer out = ByteBuffer.allocate(4 + headerBytes.length + bodyBytes.length)
        .order(ByteOrder.LITTLE_ENDIAN);
    out.putInt(headerBytes.length);
    out.put(headerBytes);
    out.put(bodyBytes);
    return out.array();
  }
}
