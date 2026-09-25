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

import com.arcadedb.log.LogManager;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.logging.Level;

/**
 * Parser for InfluxDB Line Protocol.
 * Format: {@code <measurement>[,<tag_key>=<tag_value>...] <field_key>=<field_value>[,<field_key>=<field_value>...] [<timestamp>]}
 * <p>
 * Type suffixes: no suffix = double, {@code i} = long, quoted = string, true/false = boolean.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class LineProtocolParser {

  public enum Precision {
    NANOSECONDS(1_000_000L, 1L),
    MICROSECONDS(1_000L, 1L),
    MILLISECONDS(1L, 1L),
    SECONDS(1L, 1_000L);

    private final long divisor;
    private final long multiplier;

    Precision(final long divisor, final long multiplier) {
      this.divisor = divisor;
      this.multiplier = multiplier;
    }

    public long toMillis(final long value) {
      // Narrow first (sub-millisecond units divide down), then multiply with an exact check so a very
      // large seconds-precision timestamp throws instead of silently wrapping to a negative ms epoch.
      return Math.multiplyExact(value / divisor, multiplier);
    }

    public static Precision fromString(final String s) {
      if (s == null || s.isEmpty())
        return NANOSECONDS;
      return switch (s.toLowerCase(Locale.ROOT)) {
        case "ns" -> NANOSECONDS;
        case "us", "u" -> MICROSECONDS;
        case "ms" -> MILLISECONDS;
        case "s" -> SECONDS;
        default -> {
          LogManager.instance().log(Precision.class, Level.WARNING,
              "Unrecognized precision '%s'; defaulting to nanoseconds", null, s);
          yield NANOSECONDS;
        }
      };
    }
  }

  public static class Sample {
    private final String              measurement;
    private final Map<String, String> tags;
    private final Map<String, Object> fields;
    private final long                timestampMs;

    public Sample(final String measurement, final Map<String, String> tags, final Map<String, Object> fields,
        final long timestampMs) {
      this.measurement = measurement;
      this.tags = tags;
      this.fields = fields;
      this.timestampMs = timestampMs;
    }

    public String getMeasurement() {
      return measurement;
    }

    public Map<String, String> getTags() {
      return tags;
    }

    public Map<String, Object> getFields() {
      return fields;
    }

    public long getTimestampMs() {
      return timestampMs;
    }
  }

  private record ParsedString(String value, int length) {}

  private record ParsedValue(Object value, int length) {}

  /**
   * Parses one or more lines of InfluxDB Line Protocol. A malformed line is skipped and logged.
   */
  public static List<Sample> parse(final String text, final Precision precision) {
    return parse(text, precision, null);
  }

  /**
   * Parses one or more lines of InfluxDB Line Protocol. A malformed line is skipped, logged and, when
   * {@code malformedLines} is not null, its 1-based line number in {@code text} is appended to it, so a caller can
   * report the lines it did not ingest instead of answering the request as a complete success (issue #8302).
   */
  public static List<Sample> parse(final String text, final Precision precision, final List<Integer> malformedLines) {
    final List<Sample> samples = new ArrayList<>();
    if (text == null || text.isEmpty())
      return samples;

    // Use \R (any line terminator) to handle Unix (\n), Windows (\r\n), and classic Mac (\r)
    final String[] lines = text.split("\\R");
    for (int i = 0; i < lines.length; i++) {
      final String line = lines[i].trim();
      if (line.isEmpty() || line.startsWith("#"))
        continue;

      final Sample sample = parseLine(line, precision);
      if (sample != null)
        samples.add(sample);
      else {
        if (malformedLines != null)
          malformedLines.add(i + 1);
        LogManager.instance().log(LineProtocolParser.class, Level.WARNING,
            "Skipping malformed line protocol line: '%s'", null,
            sanitizeForLog(line.length() > 120 ? line.substring(0, 120) + "..." : line));
      }
    }
    return samples;
  }

  /**
   * Parses a single line of InfluxDB Line Protocol.
   * Returns {@code null} if the line is malformed (missing measurement, no fields, or unparseable numbers).
   */
  static Sample parseLine(final String line, final Precision precision) {
    // Split into: measurement+tags, fields, [timestamp]
    // Space separates measurement+tags from fields, and fields from timestamp
    // But commas and equals within the measurement+tags section are significant

    try {
      int pos = 0;
      final int len = line.length();

      // Parse measurement name (up to first unescaped comma or space)
      final StringBuilder measurement = new StringBuilder();
      while (pos < len) {
        final char c = line.charAt(pos);
        if (c == '\\' && pos + 1 < len) {
          measurement.append(line.charAt(pos + 1));
          pos += 2;
          continue;
        }
        if (c == ',' || c == ' ')
          break;
        measurement.append(c);
        pos++;
      }

      if (measurement.isEmpty())
        return null;

      // Parse tags (comma-separated key=value pairs)
      final Map<String, String> tags = new LinkedHashMap<>();
      if (pos < len && line.charAt(pos) == ',') {
        pos++; // skip comma
        while (pos < len && line.charAt(pos) != ' ') {
          final ParsedString keyResult = readKey(line, pos);
          pos += keyResult.length() + 1; // +1 for '='
          final ParsedString valResult = readTagValueWithLength(line, pos);
          pos += valResult.length();
          // InfluxDB spec mandates non-empty tag keys; skip silently to avoid polluting the schema
          if (!keyResult.value().isEmpty())
            tags.put(keyResult.value(), valResult.value());
          if (pos < len && line.charAt(pos) == ',')
            pos++; // skip comma separator
        }
      }

      // Skip space before fields
      if (pos < len && line.charAt(pos) == ' ')
        pos++;

      // Parse fields (comma-separated key=value pairs)
      final Map<String, Object> fields = new LinkedHashMap<>();
      while (pos < len && line.charAt(pos) != ' ') {
        final ParsedString keyResult = readKey(line, pos);
        if (keyResult.value().isEmpty())
          throw new IllegalArgumentException("Missing field key at position " + pos);
        pos += keyResult.length() + 1; // +1 for '='
        final ParsedValue valueAndLen = readFieldValue(line, pos);
        fields.put(keyResult.value(), valueAndLen.value());
        pos += valueAndLen.length();
        if (pos < len && line.charAt(pos) == ',')
          pos++; // skip comma separator
      }

      if (fields.isEmpty())
        return null;

      // Parse optional timestamp
      long timestampMs;
      if (pos < len && line.charAt(pos) == ' ') {
        pos++; // skip space
        final String tsStr = line.substring(pos).trim();
        if (!tsStr.isEmpty()) {
          final long rawTs = Long.parseLong(tsStr);
          timestampMs = precision.toMillis(rawTs);
        } else {
          timestampMs = System.currentTimeMillis();
        }
      } else {
        timestampMs = System.currentTimeMillis();
      }

      return new Sample(measurement.toString(), tags, fields, timestampMs);
    } catch (final IllegalArgumentException | ArithmeticException e) {
      // Malformed numeric value or timestamp (including unsigned integer overflow and seconds-precision
      // timestamps that overflow when converted to milliseconds) — skip this line rather than halting
      // batch parse
      return null;
    }
  }

  /**
   * Reads a key (tag key or field key) terminated by '=', handling backslash escapes.
   * Returns the decoded string and the raw byte length consumed (not including the '=').
   * An unescaped ',' or ' ' cannot be part of a key, so reaching one, or the end of the line, before the '=' means the
   * key has no value: the line is malformed. Reading on to the end of the line instead turned a line truncated after a
   * field key into a field the caller then stored with a fabricated value (issue #8302).
   */
  private static ParsedString readKey(final String line, final int start) {
    final StringBuilder sb = new StringBuilder();
    int pos = start;
    while (pos < line.length()) {
      final char c = line.charAt(pos);
      if (c == '\\' && pos + 1 < line.length()) {
        sb.append(line.charAt(pos + 1));
        pos += 2;
        continue;
      }
      if (c == '=')
        return new ParsedString(sb.toString(), pos - start);
      if (c == ',' || c == ' ')
        break;
      sb.append(c);
      pos++;
    }
    throw new IllegalArgumentException("Missing '=' after key '" + sb + "' at position " + start);
  }

  /**
   * Reads a tag value terminated by ',' or ' ', handling backslash escapes.
   * Returns the decoded string and the raw byte length consumed.
   */
  private static ParsedString readTagValueWithLength(final String line, final int start) {
    final StringBuilder sb = new StringBuilder();
    int pos = start;
    while (pos < line.length()) {
      final char c = line.charAt(pos);
      if (c == '\\' && pos + 1 < line.length()) {
        sb.append(line.charAt(pos + 1));
        pos += 2;
        continue;
      }
      if (c == ',' || c == ' ')
        break;
      sb.append(c);
      pos++;
    }
    return new ParsedString(sb.toString(), pos - start);
  }

  /**
   * Strips control characters and newlines from user-controlled input before logging
   * to prevent log injection attacks.
   */
  private static String sanitizeForLog(final String s) {
    if (s == null)
      return null;
    final StringBuilder sb = new StringBuilder(s.length());
    for (int i = 0; i < s.length(); i++) {
      final char c = s.charAt(i);
      if (c >= 0x20 && c != 0x7F)
        sb.append(c);
      else
        sb.append('?');
    }
    return sb.toString();
  }

  /**
   * Reads a field value and returns the parsed value and the raw byte length consumed.
   */
  private static ParsedValue readFieldValue(final String line, final int start) {
    if (start >= line.length())
      throw new IllegalArgumentException("Missing field value at position " + start);

    final char first = line.charAt(start);

    // Quoted string — enforce MAX_STRING_BYTES to prevent multi-megabyte allocations
    if (first == '"') {
      final StringBuilder sb = new StringBuilder();
      int pos = start + 1;
      boolean closed = false;
      while (pos < line.length()) {
        final char c = line.charAt(pos);
        if (c == '\\' && pos + 1 < line.length()) {
          if (sb.length() >= TimeSeriesBucket.MAX_STRING_BYTES)
            throw new IllegalArgumentException(
                "Quoted field value exceeds maximum length of " + TimeSeriesBucket.MAX_STRING_BYTES + " bytes");
          sb.append(line.charAt(pos + 1));
          pos += 2;
          continue;
        }
        if (c == '"') {
          pos++;
          closed = true;
          break;
        }
        if (sb.length() >= TimeSeriesBucket.MAX_STRING_BYTES)
          throw new IllegalArgumentException(
              "Quoted field value exceeds maximum length of " + TimeSeriesBucket.MAX_STRING_BYTES + " bytes");
        sb.append(c);
        pos++;
      }
      if (!closed)
        throw new IllegalArgumentException("Unterminated quoted string in field value at position " + start);
      return new ParsedValue(sb.toString(), pos - start);
    }

    // Read until comma or space
    int pos = start;
    while (pos < line.length() && line.charAt(pos) != ',' && line.charAt(pos) != ' ')
      pos++;

    final String raw = line.substring(start, pos);
    final int rawLen = pos - start;

    // Boolean
    if ("true".equalsIgnoreCase(raw) || "t".equalsIgnoreCase(raw))
      return new ParsedValue(true, rawLen);
    if ("false".equalsIgnoreCase(raw) || "f".equalsIgnoreCase(raw))
      return new ParsedValue(false, rawLen);

    // Integer (suffix 'i')
    if (raw.endsWith("i")) {
      final long intVal = Long.parseLong(raw.substring(0, raw.length() - 1));
      return new ParsedValue(intVal, rawLen);
    }

    // Unsigned integer (suffix 'u'): values in [0, 2^64-1] are stored as the bit-pattern
    // in a signed long (values >= 2^63 appear negative but are valid uint64 encodings)
    if (raw.endsWith("u")) {
      final long uintVal = Long.parseUnsignedLong(raw.substring(0, raw.length() - 1));
      return new ParsedValue(uintVal, rawLen);
    }

    // Default: double
    return new ParsedValue(Double.parseDouble(raw), rawLen);
  }

}
