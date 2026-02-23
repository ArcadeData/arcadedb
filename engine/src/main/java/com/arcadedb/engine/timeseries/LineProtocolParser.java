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
      return (value / divisor) * multiplier;
    }

    public static Precision fromString(final String s) {
      if (s == null || s.isEmpty())
        return NANOSECONDS;
      return switch (s.toLowerCase()) {
        case "ns" -> NANOSECONDS;
        case "us", "u" -> MICROSECONDS;
        case "ms" -> MILLISECONDS;
        case "s" -> SECONDS;
        default -> NANOSECONDS;
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

  /**
   * Parses one or more lines of InfluxDB Line Protocol.
   */
  public static List<Sample> parse(final String text, final Precision precision) {
    final List<Sample> samples = new ArrayList<>();
    if (text == null || text.isEmpty())
      return samples;

    final String[] lines = text.split("\n");
    for (final String rawLine : lines) {
      final String line = rawLine.trim();
      if (line.isEmpty() || line.startsWith("#"))
        continue;

      final Sample sample = parseLine(line, precision);
      if (sample != null)
        samples.add(sample);
      else
        LogManager.instance().log(LineProtocolParser.class, Level.WARNING,
            "Skipping malformed line protocol line: '%s'", null,
            line.length() > 120 ? line.substring(0, 120) + "..." : line);
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
          final Object[] keyResult = readKeyWithLength(line, pos, '=');
          final String key = (String) keyResult[0];
          pos += (int) keyResult[1] + 1; // +1 for '='
          final Object[] valResult = readTagValueWithLength(line, pos);
          final String value = (String) valResult[0];
          pos += (int) valResult[1];
          tags.put(key, value);
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
        final Object[] keyResult = readKeyWithLength(line, pos, '=');
        final String key = (String) keyResult[0];
        pos += (int) keyResult[1] + 1; // +1 for '='
        final Object[] valueAndLen = readFieldValue(line, pos);
        fields.put(key, valueAndLen[0]);
        pos += (int) valueAndLen[1];
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
    } catch (final IllegalArgumentException e) {
      // Malformed numeric value or timestamp (including unsigned integer overflow) —
      // skip this line rather than halting batch parse
      return null;
    }
  }

  /**
   * Reads a key (tag key or field key) terminated by {@code stopChar}, handling backslash escapes.
   * Returns {@code Object[] { decodedString, rawLength }} so the caller advances the position once.
   */
  private static Object[] readKeyWithLength(final String line, final int start, final char stopChar) {
    final StringBuilder sb = new StringBuilder();
    int pos = start;
    while (pos < line.length()) {
      final char c = line.charAt(pos);
      if (c == '\\' && pos + 1 < line.length()) {
        sb.append(line.charAt(pos + 1));
        pos += 2;
        continue;
      }
      if (c == stopChar)
        break;
      sb.append(c);
      pos++;
    }
    return new Object[] { sb.toString(), pos - start };
  }

  /**
   * Reads a tag value terminated by ',' or ' ', handling backslash escapes.
   * Returns {@code Object[] { decodedString, rawLength }} so the caller advances the position once.
   */
  private static Object[] readTagValueWithLength(final String line, final int start) {
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
    return new Object[] { sb.toString(), pos - start };
  }

  /**
   * Reads a field value and returns [value, rawLength].
   */
  private static Object[] readFieldValue(final String line, final int start) {
    if (start >= line.length())
      return new Object[] { 0.0, 0 };

    final char first = line.charAt(start);

    // Quoted string
    if (first == '"') {
      final StringBuilder sb = new StringBuilder();
      int pos = start + 1;
      while (pos < line.length()) {
        final char c = line.charAt(pos);
        if (c == '\\' && pos + 1 < line.length()) {
          sb.append(line.charAt(pos + 1));
          pos += 2;
          continue;
        }
        if (c == '"') {
          pos++;
          break;
        }
        sb.append(c);
        pos++;
      }
      return new Object[] { sb.toString(), pos - start };
    }

    // Read until comma or space
    int pos = start;
    while (pos < line.length() && line.charAt(pos) != ',' && line.charAt(pos) != ' ')
      pos++;

    final String raw = line.substring(start, pos);
    final int rawLen = pos - start;

    // Boolean
    if ("true".equalsIgnoreCase(raw) || "t".equalsIgnoreCase(raw))
      return new Object[] { true, rawLen };
    if ("false".equalsIgnoreCase(raw) || "f".equalsIgnoreCase(raw))
      return new Object[] { false, rawLen };

    // Integer (suffix 'i')
    if (raw.endsWith("i")) {
      final long intVal = Long.parseLong(raw.substring(0, raw.length() - 1));
      return new Object[] { intVal, rawLen };
    }

    // Unsigned integer (suffix 'u')
    if (raw.endsWith("u")) {
      final long uintVal = Long.parseUnsignedLong(raw.substring(0, raw.length() - 1));
      if (uintVal < 0)
        throw new IllegalArgumentException(
            "Unsigned integer value cannot be represented as a signed 64-bit integer (exceeds " + Long.MAX_VALUE + "): " + raw);
      return new Object[] { uintVal, rawLen };
    }

    // Default: double
    return new Object[] { Double.parseDouble(raw), rawLen };
  }

}
