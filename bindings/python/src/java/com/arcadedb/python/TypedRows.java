/*
 * Python-bindings bridge: type-faithful batched row transport.
 *
 * RowBatcher is the fast way to move rows (one JSON string per batch) but its values are JSON-native: DATE and
 * DATETIME arrive as epoch integers and DECIMAL as a float. RowAccess keeps every type but hands Python one engine
 * object per value, and converting those one by one through JPype costs about 2.4 microseconds a value.
 *
 * This serializes a batch as JSON too, and tags the values JSON cannot carry exactly, so Python restores each one to
 * the type convert_java_to_python gives it today:
 *
 *   {"\u0001": ["n", "1.25"]}                      BigDecimal (Decimal)
 *   {"\u0001": ["i", "123..."]}                    BigInteger
 *   {"\u0001": ["D", "2020-01-01"]}                LocalDate
 *   {"\u0001": ["T", "2020-01-01T12:00:00.000000"]}                 LocalDateTime (microseconds)
 *   {"\u0001": ["Z", "2020-01-01T12:00:00.000000+00:00"]}           Instant, ZonedDateTime, OffsetDateTime (UTC)
 *   {"\u0001": ["s", [..]]}                        Set
 *   {"\u0001": ["x", index]}                       anything else: the engine's own object, taken from the side
 *                                                  array and converted by Python exactly as before (RIDs, embedded
 *                                                  documents, vertices, java.util.Date, NaN and the infinities, maps
 *                                                  with non-String keys, large float[], ...)
 *   {"\u0001": ["r", index]}                       a whole row whose property name is the tag key (never in practice)
 *
 * Plain values are plain JSON: String, Character, the integer types, finite Float and Double (Double.toString
 * round-trips exactly), Boolean, null, Lists and Collections (arrays), Maps with String keys (objects), and the
 * integer and boolean primitive arrays. float[] and double[] go through the side array: Python converts them in bulk
 * through the buffer protocol, which measured 6x faster on 2,000 rows of 128 floats than writing and parsing a text number
 * per element.
 *
 * Compiled into arcadedb-python-bridge.jar during the wheel build and consumed by ResultSet.to_list().
 */
package com.arcadedb.python;

import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class TypedRows {

  /** The key of a tagged value; Python reads the same character. */
  static final String TAG = "\u0001";

  private TypedRows() {
  }

  /**
   * Up to {@code max} rows as {@code {String json, Object[] side}}: {@code json} is an array of row objects and
   * {@code side} holds the engine objects the JSON refers to by index. Fewer than {@code max} rows means the result
   * set is drained, and it is closed here (see {@link RowBatcher#closeDrained}), so a caller stops after a short batch.
   */
  public static Object[] nextRows(final ResultSet rs, final int max) {
    final StringBuilder sb = new StringBuilder();
    final List<Object> side = new ArrayList<>();
    sb.append('[');
    int n = 0;
    while (n < max && rs.hasNext()) {
      final Result row = rs.next();
      if (n > 0)
        sb.append(',');
      appendRow(sb, row, side);
      n++;
    }
    if (n < max)
      RowBatcher.closeDrained(rs);
    sb.append(']');
    return new Object[] { sb.toString(), side.toArray() };
  }

  private static void appendRow(final StringBuilder sb, final Result row, final List<Object> side) {
    final String[] names = row.getPropertyNames().toArray(new String[0]);
    for (final String name : names)
      if (TAG.equals(name)) {
        sb.append("{\"\\u0001\":[\"r\",").append(side.size()).append("]}");
        side.add(RowAccess.namesAndValues(row));
        return;
      }
    sb.append('{');
    for (int i = 0; i < names.length; i++) {
      if (i > 0)
        sb.append(',');
      quote(sb, names[i]);
      sb.append(':');
      append(sb, row.getProperty(names[i]), side);
    }
    sb.append('}');
  }

  private static void append(final StringBuilder sb, final Object value, final List<Object> side) {
    if (value == null) {
      sb.append("null");
    } else if (value instanceof String s) {
      quote(sb, s);
    } else if (value instanceof Boolean b) {
      sb.append(b.booleanValue() ? "true" : "false");
    } else if (value instanceof Integer || value instanceof Long || value instanceof Short || value instanceof Byte) {
      sb.append(value);
    } else if (value instanceof Double || value instanceof Float) {
      final double d = ((Number) value).doubleValue();
      if (Double.isFinite(d))
        sb.append(d);
      else
        toSide(sb, value, side);
    } else if (value instanceof Character c) {
      quote(sb, String.valueOf(c));
    } else if (value instanceof BigDecimal bd) {
      tagged(sb, "n").append('"').append(bd.toString()).append("\"]}");
    } else if (value instanceof BigInteger bi) {
      tagged(sb, "i").append('"').append(bi.toString()).append("\"]}");
    } else if (value instanceof LocalDate date) {
      if (yearFits(date.getYear())) {
        tagged(sb, "D").append('"');
        appendDate(sb, date.getYear(), date.getMonthValue(), date.getDayOfMonth());
        sb.append("\"]}");
      } else
        toSide(sb, value, side);
    } else if (value instanceof LocalDateTime dt) {
      appendDateTime(sb, "T", dt, "", value, side);
    } else if (value instanceof Instant instant) {
      appendDateTime(sb, "Z", LocalDateTime.ofEpochSecond(instant.getEpochSecond(), instant.getNano(), ZoneOffset.UTC),
          "+00:00", value, side);
    } else if (value instanceof ZonedDateTime zdt) {
      final Instant instant = zdt.toInstant();
      appendDateTime(sb, "Z", LocalDateTime.ofEpochSecond(instant.getEpochSecond(), instant.getNano(), ZoneOffset.UTC),
          "+00:00", value, side);
    } else if (value instanceof OffsetDateTime odt) {
      final Instant instant = odt.toInstant();
      appendDateTime(sb, "Z", LocalDateTime.ofEpochSecond(instant.getEpochSecond(), instant.getNano(), ZoneOffset.UTC),
          "+00:00", value, side);
    } else if (value instanceof Map<?, ?> map) {
      for (final Object key : map.keySet())
        if (!(key instanceof String k) || TAG.equals(k)) {
          toSide(sb, value, side);
          return;
        }
      sb.append('{');
      boolean first = true;
      for (final Map.Entry<?, ?> entry : map.entrySet()) {
        if (!first)
          sb.append(',');
        first = false;
        quote(sb, (String) entry.getKey());
        sb.append(':');
        append(sb, entry.getValue(), side);
      }
      sb.append('}');
    } else if (value instanceof Set<?> set) {
      tagged(sb, "s").append('[');
      appendElements(sb, set, side);
      sb.append("]]}");
    } else if (value instanceof Collection<?> collection) {
      sb.append('[');
      appendElements(sb, collection, side);
      sb.append(']');
    } else if (value instanceof byte[] a) {
      sb.append('[');
      for (int i = 0; i < a.length; i++)
        sb.append(i > 0 ? "," : "").append(a[i]);
      sb.append(']');
    } else if (value instanceof short[] a) {
      sb.append('[');
      for (int i = 0; i < a.length; i++)
        sb.append(i > 0 ? "," : "").append(a[i]);
      sb.append(']');
    } else if (value instanceof int[] a) {
      sb.append('[');
      for (int i = 0; i < a.length; i++)
        sb.append(i > 0 ? "," : "").append(a[i]);
      sb.append(']');
    } else if (value instanceof long[] a) {
      sb.append('[');
      for (int i = 0; i < a.length; i++)
        sb.append(i > 0 ? "," : "").append(a[i]);
      sb.append(']');
    } else if (value instanceof boolean[] a) {
      sb.append('[');
      for (int i = 0; i < a.length; i++)
        sb.append(i > 0 ? "," : "").append(a[i]);
      sb.append(']');
    } else
      toSide(sb, value, side);
  }

  private static void appendElements(final StringBuilder sb, final Collection<?> elements, final List<Object> side) {
    boolean first = true;
    for (final Object element : elements) {
      if (!first)
        sb.append(',');
      first = false;
      append(sb, element, side);
    }
  }

  private static StringBuilder tagged(final StringBuilder sb, final String kind) {
    return sb.append("{\"\\u0001\":[\"").append(kind).append("\",");
  }

  private static void toSide(final StringBuilder sb, final Object value, final List<Object> side) {
    tagged(sb, "x").append(side.size()).append("]}");
    side.add(value);
  }

  /** Python's datetime covers years 1 to 9999. */
  private static boolean yearFits(final int year) {
    return year >= 1 && year <= 9999;
  }

  private static void appendDateTime(final StringBuilder sb, final String kind, final LocalDateTime dt,
      final String suffix, final Object original, final List<Object> side) {
    if (!yearFits(dt.getYear())) {
      toSide(sb, original, side);
      return;
    }
    tagged(sb, kind).append('"');
    appendDate(sb, dt.getYear(), dt.getMonthValue(), dt.getDayOfMonth());
    sb.append('T');
    pad(sb, dt.getHour(), 2).append(':');
    pad(sb, dt.getMinute(), 2).append(':');
    pad(sb, dt.getSecond(), 2).append('.');
    pad(sb, dt.getNano() / 1000, 6).append(suffix).append("\"]}");
  }

  private static void appendDate(final StringBuilder sb, final int year, final int month, final int day) {
    pad(sb, year, 4).append('-');
    pad(sb, month, 2).append('-');
    pad(sb, day, 2);
  }

  private static StringBuilder pad(final StringBuilder sb, final int value, final int width) {
    int digits = 1;
    for (int v = value; v >= 10; v /= 10)
      digits++;
    for (int i = digits; i < width; i++)
      sb.append('0');
    return sb.append(value);
  }

  /**
   * A JSON string. Surrogates are always written as \\u escapes (a lone one is not valid UTF-16 text to hand across the
   * bridge), as are control characters.
   */
  private static void quote(final StringBuilder sb, final String s) {
    sb.append('"');
    final int length = s.length();
    for (int i = 0; i < length; i++) {
      final char c = s.charAt(i);
      if (c == '"')
        sb.append("\\\"");
      else if (c == '\\')
        sb.append("\\\\");
      else if (c < 0x20 || (c >= 0xD800 && c <= 0xDFFF)) {
        sb.append("\\u");
        final String hex = Integer.toHexString(c);
        for (int k = hex.length(); k < 4; k++)
          sb.append('0');
        sb.append(hex);
      } else
        sb.append(c);
    }
    sb.append('"');
  }
}
