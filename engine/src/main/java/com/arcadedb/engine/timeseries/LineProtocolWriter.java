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

import java.math.BigDecimal;
import java.util.Map;

/**
 * Serializes samples to InfluxDB Line Protocol, the encoding {@code POST /api/v1/ts/{database}/write} accepts.
 * The exact inverse of {@link LineProtocolParser}: everything this class escapes, that class unescapes, which is
 * what {@code LineProtocolWriterTest} asserts by round-tripping (issue #7305).
 * <p>
 * Timestamps are emitted in <b>milliseconds</b>, so a request carrying this body must declare
 * {@code precision=ms}; the server defaults to nanoseconds when the parameter is absent, which would divide
 * every timestamp here by a million.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class LineProtocolWriter {

  private LineProtocolWriter() {
  }

  /**
   * Appends one sample as a single line, terminated by {@code \n}.
   *
   * @param out         the buffer to append to
   * @param measurement the time-series type name
   * @param tags        tag columns; a null value is skipped, since line protocol has no null tag
   * @param fields      field columns; at least one is required, a line with none is rejected by the parser
   * @param timestampMs the sample timestamp, epoch milliseconds
   *
   * @throws IllegalArgumentException if the measurement is blank, no field is present, or any element carries a
   *                                  line terminator (which would split one sample across two lines)
   */
  public static void appendLine(final StringBuilder out, final String measurement, final Map<String, ?> tags,
      final Map<String, ?> fields, final long timestampMs) {
    if (measurement == null || measurement.isBlank())
      throw new IllegalArgumentException("Line protocol requires a measurement name");
    if (fields == null || fields.isEmpty())
      throw new IllegalArgumentException(
          "Line protocol requires at least one field on measurement '" + measurement + "'");

    escapeKey(out, measurement);

    if (tags != null) {
      for (final Map.Entry<String, ?> tag : tags.entrySet()) {
        if (tag.getValue() == null)
          continue;
        out.append(',');
        escapeKey(out, tag.getKey());
        out.append('=');
        escapeKey(out, String.valueOf(tag.getValue()));
      }
    }

    out.append(' ');

    boolean first = true;
    int written = 0;
    for (final Map.Entry<String, ?> field : fields.entrySet()) {
      if (field.getValue() == null)
        // Line protocol has no null field value, and an absent field is exactly how a sample says "no
        // measurement for this column" - the same thing the reader renders back as null.
        continue;
      if (!first)
        out.append(',');
      first = false;
      escapeKey(out, field.getKey());
      out.append('=');
      appendFieldValue(out, field.getValue());
      written++;
    }

    if (written == 0)
      throw new IllegalArgumentException(
          "Line protocol requires at least one non-null field on measurement '" + measurement + "'");

    out.append(' ').append(timestampMs).append('\n');
  }

  /**
   * Escapes a measurement name, tag key, tag value or field key. {@link LineProtocolParser} decodes any
   * {@code \x} back to {@code x}, so backslash-escaping the four characters that would otherwise terminate the
   * token round-trips exactly.
   */
  private static void escapeKey(final StringBuilder out, final String value) {
    for (int i = 0; i < value.length(); i++) {
      final char c = value.charAt(i);
      rejectLineTerminator(c, value);
      if (c == ',' || c == ' ' || c == '=' || c == '\\')
        out.append('\\');
      out.append(c);
    }
  }

  /**
   * Writes a field value in the encoding {@link LineProtocolParser#parseLine} reads back as the same Java type:
   * an integral value carries the {@code i} suffix, a boolean is spelled out, and anything else is quoted as a
   * string. A {@code double}/{@code float} is written bare, which the parser's default branch reads as a double.
   * <p>
   * A {@link BigDecimal} is written bare too, and therefore read back as a double. That is not a loss of
   * fidelity relative to storage: {@code ColumnDefinition.isStorableType} refuses DECIMAL, so no TimeSeries
   * column can hold one, and a caller passing a BigDecimal is filling a FLOAT or DOUBLE column. Quoting it
   * instead would send text to a numeric column.
   */
  private static void appendFieldValue(final StringBuilder out, final Object value) {
    switch (value) {
    case Boolean b -> out.append(b ? "true" : "false");
    case Byte b -> out.append(b.longValue()).append('i');
    case Short s -> out.append(s.longValue()).append('i');
    case Integer i -> out.append(i.longValue()).append('i');
    case Long l -> out.append(l.longValue()).append('i');
    case Double d -> appendDouble(out, d);
    case Float f -> appendDouble(out, f.doubleValue());
    case BigDecimal d -> out.append(d.toPlainString());
    default -> appendQuotedString(out, String.valueOf(value));
    }
  }

  /**
   * A non-finite double has no line-protocol spelling that survives a round trip through a numeric column, and
   * it means "no measurement" rather than a value, so it is refused here instead of being written as the text
   * {@code NaN} and read back as a number.
   */
  private static void appendDouble(final StringBuilder out, final double value) {
    if (!Double.isFinite(value))
      throw new IllegalArgumentException("Line protocol cannot carry the non-finite field value " + value);
    out.append(value);
  }

  private static void appendQuotedString(final StringBuilder out, final String value) {
    out.append('"');
    for (int i = 0; i < value.length(); i++) {
      final char c = value.charAt(i);
      rejectLineTerminator(c, value);
      if (c == '"' || c == '\\')
        out.append('\\');
      out.append(c);
    }
    out.append('"');
  }

  /**
   * Line protocol is line-delimited, so a terminator inside any element would split one sample into two
   * malformed ones that the server silently skips. Refuse it where it can still be reported to the caller.
   */
  private static void rejectLineTerminator(final char c, final String value) {
    if (c == '\n' || c == '\r')
      throw new IllegalArgumentException(
          "Line protocol cannot carry a line terminator inside a value: '" + value.replace('\n', '?')
              .replace('\r', '?') + "'");
  }
}
