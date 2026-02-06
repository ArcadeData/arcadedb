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
package com.arcadedb.query.opencypher.tck;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Parses TCK value format strings into Java objects for comparison.
 * Handles: integers, floats, strings, booleans, null, lists, maps, nodes, relationships, paths.
 */
public class TCKValueParser {

  /**
   * Parses a TCK value string into a Java object.
   */
  public static Object parse(final String value) {
    if (value == null)
      return null;

    final String trimmed = value.trim();
    if (trimmed.isEmpty())
      return "";

    // null
    if ("null".equals(trimmed))
      return null;

    // boolean
    if ("true".equals(trimmed))
      return Boolean.TRUE;
    if ("false".equals(trimmed))
      return Boolean.FALSE;

    // string (single-quoted)
    if (trimmed.startsWith("'") && trimmed.endsWith("'"))
      return unescapeString(trimmed.substring(1, trimmed.length() - 1));

    // list
    if (trimmed.startsWith("[") && trimmed.endsWith("]"))
      return parseList(trimmed);

    // map
    if (trimmed.startsWith("{") && trimmed.endsWith("}") && !isNode(trimmed))
      return parseMap(trimmed);

    // node
    if (trimmed.startsWith("(") && trimmed.endsWith(")"))
      return parseNode(trimmed);

    // relationship
    if (trimmed.startsWith("[") && trimmed.endsWith("]") && trimmed.contains(":"))
      return parseRelationship(trimmed);

    // path
    if (trimmed.startsWith("<") && trimmed.endsWith(">"))
      return parsePath(trimmed);

    // NaN, Inf
    if ("NaN".equals(trimmed))
      return Double.NaN;
    if ("Inf".equals(trimmed))
      return Double.POSITIVE_INFINITY;
    if ("-Inf".equals(trimmed))
      return Double.NEGATIVE_INFINITY;

    // float (contains dot or scientific notation)
    if (trimmed.contains(".") || (trimmed.contains("e") && !trimmed.startsWith("'")) || trimmed.contains("E"))
      try {
        return Double.parseDouble(trimmed);
      } catch (final NumberFormatException ignored) {
        // fall through
      }

    // integer
    try {
      return Long.parseLong(trimmed);
    } catch (final NumberFormatException ignored) {
      // fall through
    }

    // fallback: return as string
    return trimmed;
  }

  private static boolean isNode(final String trimmed) {
    // A map starts with { but a node pattern like (:Label {prop: val}) starts with (
    return false;
  }

  static List<Object> parseList(final String value) {
    final String inner = value.substring(1, value.length() - 1).trim();
    if (inner.isEmpty())
      return new ArrayList<>();

    final List<Object> result = new ArrayList<>();
    int depth = 0;
    int start = 0;
    boolean inString = false;

    for (int i = 0; i < inner.length(); i++) {
      final char c = inner.charAt(i);
      if (c == '\'' && !isEscaped(inner, i))
        inString = !inString;
      else if (!inString) {
        if (c == '[' || c == '{' || c == '(' || c == '<')
          depth++;
        else if (c == ']' || c == '}' || c == ')' || c == '>')
          depth--;
        else if (c == ',' && depth == 0) {
          result.add(parse(inner.substring(start, i).trim()));
          start = i + 1;
        }
      }
    }
    final String last = inner.substring(start).trim();
    if (!last.isEmpty())
      result.add(parse(last));

    return result;
  }

  static Map<String, Object> parseMap(final String value) {
    final String inner = value.substring(1, value.length() - 1).trim();
    if (inner.isEmpty())
      return new LinkedHashMap<>();

    final Map<String, Object> result = new LinkedHashMap<>();
    int depth = 0;
    int start = 0;
    boolean inString = false;

    for (int i = 0; i < inner.length(); i++) {
      final char c = inner.charAt(i);
      if (c == '\'' && !isEscaped(inner, i))
        inString = !inString;
      else if (!inString) {
        if (c == '[' || c == '{' || c == '(' || c == '<')
          depth++;
        else if (c == ']' || c == '}' || c == ')' || c == '>')
          depth--;
        else if (c == ',' && depth == 0) {
          addMapEntry(result, inner.substring(start, i).trim());
          start = i + 1;
        }
      }
    }
    final String last = inner.substring(start).trim();
    if (!last.isEmpty())
      addMapEntry(result, last);

    return result;
  }

  private static void addMapEntry(final Map<String, Object> map, final String entry) {
    final int colonIdx = entry.indexOf(':');
    if (colonIdx < 0)
      return;
    final String key = entry.substring(0, colonIdx).trim();
    final String val = entry.substring(colonIdx + 1).trim();
    map.put(key, parse(val));
  }

  /**
   * Parses a node pattern like (:Label {prop: val}) or ({prop: val}) or (:A:B:C).
   */
  static TCKNode parseNode(final String value) {
    final String inner = value.substring(1, value.length() - 1).trim();
    final List<String> labels = new ArrayList<>();
    Map<String, Object> properties = new LinkedHashMap<>();

    int idx = 0;
    // parse labels
    while (idx < inner.length() && inner.charAt(idx) == ':') {
      idx++; // skip ':'
      final int labelStart = idx;
      while (idx < inner.length() && inner.charAt(idx) != ':' && inner.charAt(idx) != ' ' && inner.charAt(idx) != '{')
        idx++;
      labels.add(inner.substring(labelStart, idx).trim());
    }

    // parse properties
    final int braceStart = inner.indexOf('{', idx);
    if (braceStart >= 0) {
      final int braceEnd = inner.lastIndexOf('}');
      if (braceEnd > braceStart)
        properties = parseMap(inner.substring(braceStart, braceEnd + 1));
    }

    return new TCKNode(labels, properties);
  }

  /**
   * Parses a relationship pattern like [:TYPE {prop: val}] or [:TYPE].
   */
  static TCKRelationship parseRelationship(final String value) {
    final String inner = value.substring(1, value.length() - 1).trim();
    String type = null;
    Map<String, Object> properties = new LinkedHashMap<>();

    int idx = 0;
    if (idx < inner.length() && inner.charAt(idx) == ':') {
      idx++; // skip ':'
      final int typeStart = idx;
      while (idx < inner.length() && inner.charAt(idx) != ' ' && inner.charAt(idx) != '{')
        idx++;
      type = inner.substring(typeStart, idx).trim();
    }

    final int braceStart = inner.indexOf('{', idx);
    if (braceStart >= 0) {
      final int braceEnd = inner.lastIndexOf('}');
      if (braceEnd > braceStart)
        properties = parseMap(inner.substring(braceStart, braceEnd + 1));
    }

    return new TCKRelationship(type, properties);
  }

  /**
   * Parses a path pattern like <(n1)-[:REL]->(n2)>.
   */
  static TCKPath parsePath(final String value) {
    // Path parsing is complex - for now store the raw string for structural comparison
    return new TCKPath(value.substring(1, value.length() - 1).trim());
  }

  private static String unescapeString(final String s) {
    return s.replace("\\'", "'").replace("\\\\", "\\");
  }

  private static boolean isEscaped(final String s, final int pos) {
    int backslashes = 0;
    int idx = pos - 1;
    while (idx >= 0 && s.charAt(idx) == '\\') {
      backslashes++;
      idx--;
    }
    return backslashes % 2 != 0;
  }

  /**
   * Represents a parsed TCK node pattern.
   */
  public static class TCKNode {
    final List<String> labels;
    final Map<String, Object> properties;

    TCKNode(final List<String> labels, final Map<String, Object> properties) {
      this.labels = labels;
      this.properties = properties;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder("(");
      for (final String label : labels)
        sb.append(":").append(label);
      if (!properties.isEmpty())
        sb.append(" ").append(properties);
      sb.append(")");
      return sb.toString();
    }
  }

  /**
   * Represents a parsed TCK relationship pattern.
   */
  public static class TCKRelationship {
    final String type;
    final Map<String, Object> properties;

    TCKRelationship(final String type, final Map<String, Object> properties) {
      this.type = type;
      this.properties = properties;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder("[");
      if (type != null)
        sb.append(":").append(type);
      if (!properties.isEmpty())
        sb.append(" ").append(properties);
      sb.append("]");
      return sb.toString();
    }
  }

  /**
   * Represents a parsed TCK path pattern.
   */
  public static class TCKPath {
    final String rawPattern;

    TCKPath(final String rawPattern) {
      this.rawPattern = rawPattern;
    }

    @Override
    public String toString() {
      return "<" + rawPattern + ">";
    }
  }
}
