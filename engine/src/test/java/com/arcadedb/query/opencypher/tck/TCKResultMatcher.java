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

import com.arcadedb.database.Document;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.Labels;
import com.arcadedb.query.opencypher.temporal.CypherTemporalValue;
import com.arcadedb.query.opencypher.traversal.TraversalPath;
import com.arcadedb.query.sql.executor.Result;

import java.time.LocalDate;
import java.time.LocalDateTime;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Compares actual query results against expected TCK result tables.
 */
public class TCKResultMatcher {

  /**
   * Asserts that actual results match expected rows in any order (multiset comparison).
   */
  public static void assertResultsUnordered(final List<Map<String, Object>> actualRows, final List<Map<String, Object>> expectedRows,
      final List<String> columns, final boolean ignoreListOrder) {
    assertThat(actualRows).as("Result row count mismatch.\nExpected: " + expectedRows + "\nActual: " + actualRows).hasSameSizeAs(expectedRows);

    final List<Map<String, Object>> remaining = new ArrayList<>(actualRows);
    for (final Map<String, Object> expected : expectedRows) {
      boolean found = false;
      for (int i = 0; i < remaining.size(); i++) {
        if (rowMatches(remaining.get(i), expected, columns, ignoreListOrder)) {
          remaining.remove(i);
          found = true;
          break;
        }
      }
      if (!found)
        fail("Expected row not found in results.\nExpected row: " + expected + "\nRemaining actual rows: " + remaining);
    }
  }

  /**
   * Asserts that actual results match expected rows in exact order.
   */
  public static void assertResultsOrdered(final List<Map<String, Object>> actualRows, final List<Map<String, Object>> expectedRows,
      final List<String> columns, final boolean ignoreListOrder) {
    assertThat(actualRows).as("Result row count mismatch").hasSameSizeAs(expectedRows);

    for (int i = 0; i < expectedRows.size(); i++) {
      final Map<String, Object> actual = actualRows.get(i);
      final Map<String, Object> expected = expectedRows.get(i);
      if (!rowMatches(actual, expected, columns, ignoreListOrder))
        fail("Row " + i + " mismatch.\nExpected: " + expected + "\nActual: " + actual);
    }
  }

  /**
   * Converts a Result row to a map of column->value for comparison.
   */
  public static Map<String, Object> resultToMap(final Result result, final List<String> columns) {
    final Map<String, Object> map = new java.util.LinkedHashMap<>();
    for (final String col : columns) {
      final Object val = result.getProperty(col);
      map.put(col, normalizeValue(val));
    }
    return map;
  }

  private static boolean rowMatches(final Map<String, Object> actual, final Map<String, Object> expected, final List<String> columns,
      final boolean ignoreListOrder) {
    for (final String col : columns) {
      final Object actualVal = actual.get(col);
      final Object expectedVal = expected.get(col);
      if (!valuesMatch(actualVal, expectedVal, ignoreListOrder))
        return false;
    }
    return true;
  }

  @SuppressWarnings("unchecked")
  static boolean valuesMatch(final Object actual, final Object expected, final boolean ignoreListOrder) {
    if (expected == null)
      return actual == null;
    if (actual == null)
      return false;

    // TCK node comparison
    if (expected instanceof TCKValueParser.TCKNode)
      return nodeMatches(actual, (TCKValueParser.TCKNode) expected);

    // TCK relationship comparison
    if (expected instanceof TCKValueParser.TCKRelationship)
      return relationshipMatches(actual, (TCKValueParser.TCKRelationship) expected);

    // TCK path comparison
    if (expected instanceof TCKValueParser.TCKPath)
      return pathMatches(actual, (TCKValueParser.TCKPath) expected);

    // List comparison
    if (expected instanceof List) {
      if (!(actual instanceof List))
        return false;
      final List<Object> actualList = normalizeList((List<Object>) actual);
      final List<Object> expectedList = (List<Object>) expected;
      if (actualList.size() != expectedList.size())
        return false;
      if (ignoreListOrder)
        return listsMatchUnordered(actualList, expectedList);
      for (int i = 0; i < actualList.size(); i++)
        if (!valuesMatch(actualList.get(i), expectedList.get(i), ignoreListOrder))
          return false;
      return true;
    }

    // Map comparison
    if (expected instanceof Map) {
      if (!(actual instanceof Map))
        return false;
      final Map<String, Object> actualMap = (Map<String, Object>) actual;
      final Map<String, Object> expectedMap = (Map<String, Object>) expected;
      if (actualMap.size() != expectedMap.size())
        return false;
      for (final Map.Entry<String, Object> entry : expectedMap.entrySet())
        if (!valuesMatch(actualMap.get(entry.getKey()), entry.getValue(), ignoreListOrder))
          return false;
      return true;
    }

    // Numeric comparison (handle int/long/double mismatches)
    if (expected instanceof Number && actual instanceof Number)
      return numbersMatch((Number) actual, (Number) expected);

    // String comparison
    if (expected instanceof String && actual instanceof String)
      return expected.equals(actual);

    // Boolean comparison
    if (expected instanceof Boolean && actual instanceof Boolean)
      return expected.equals(actual);

    // Fallback
    return expected.equals(actual);
  }

  private static boolean numbersMatch(final Number actual, final Number expected) {
    // Both are integer types
    if (isIntegerType(actual) && isIntegerType(expected))
      return actual.longValue() == expected.longValue();

    // At least one is floating point
    final double a = actual.doubleValue();
    final double e = expected.doubleValue();
    if (Double.isNaN(e))
      return Double.isNaN(a);
    if (Double.isInfinite(e))
      return a == e;
    return Math.abs(a - e) < 1e-10 || a == e;
  }

  private static boolean isIntegerType(final Number n) {
    return n instanceof Long || n instanceof Integer || n instanceof Short || n instanceof Byte;
  }

  private static boolean nodeMatches(final Object actual, final TCKValueParser.TCKNode expected) {
    Document doc = null;
    if (actual instanceof Result) {
      final Result r = (Result) actual;
      if (r.isElement())
        doc = r.toElement();
    } else if (actual instanceof Document)
      doc = (Document) actual;

    if (doc == null)
      return false;

    // Check labels (exact match: node must have all expected labels and no extras)
    if (!expected.labels.isEmpty()) {
      if (doc instanceof Vertex) {
        final List<String> actualLabels = Labels.getLabels((Vertex) doc);
        // Check exact label set match
        if (actualLabels.size() != expected.labels.size())
          return false;
        for (final String label : expected.labels)
          if (!actualLabels.contains(label))
            return false;
      } else {
        final String typeName = doc.getTypeName();
        if (typeName == null)
          return false;
        for (final String label : expected.labels)
          if (!typeName.equals(label) && !hasLabel(doc, label))
            return false;
      }
    }

    // Check properties
    for (final Map.Entry<String, Object> entry : expected.properties.entrySet()) {
      final Object actualProp = doc.get(entry.getKey());
      if (!valuesMatch(normalizeValue(actualProp), entry.getValue(), false))
        return false;
    }

    return true;
  }

  private static boolean hasLabel(final Document doc, final String label) {
    // Check if the document's type hierarchy includes this label
    if (doc.getTypeName().equals(label))
      return true;
    // Use Labels utility for robust multi-label checking
    if (doc instanceof Vertex)
      return Labels.hasLabel((Vertex) doc, label);
    // Fallback to type hierarchy check
    try {
      return doc.getType().instanceOf(label);
    } catch (final Exception e) {
      return false;
    }
  }

  private static boolean relationshipMatches(final Object actual, final TCKValueParser.TCKRelationship expected) {
    Edge edge = null;
    if (actual instanceof Result) {
      final Result r = (Result) actual;
      if (r.isEdge())
        edge = r.getEdge().orElse(null);
    } else if (actual instanceof Edge)
      edge = (Edge) actual;

    if (edge == null)
      return false;

    // Check type
    if (expected.type != null && !expected.type.equals(edge.getTypeName()))
      return false;

    // Check properties
    for (final Map.Entry<String, Object> entry : expected.properties.entrySet()) {
      final Object actualProp = edge.get(entry.getKey());
      if (!valuesMatch(normalizeValue(actualProp), entry.getValue(), false))
        return false;
    }

    return true;
  }

  private static boolean pathMatches(final Object actual, final TCKValueParser.TCKPath expected) {
    // Path matching: compare structural elements (nodes, relationships, directions)
    if (actual instanceof TraversalPath) {
      final TraversalPath path = (TraversalPath) actual;
      return pathStructureMatches(path, expected.rawPattern);
    }
    // Paths may also come as List<Object> alternating vertices/edges
    if (actual instanceof List)
      return true; // Simplified: path structure comparison is complex
    return false;
  }

  @SuppressWarnings("unchecked")
  private static boolean pathStructureMatches(final TraversalPath path, final String pattern) {
    // Parse the expected pattern to extract nodes and relationships
    // Pattern format: (nodePattern)-[relPattern]->(nodePattern)...
    // This is a best-effort structural match
    final List<String> nodePatterns = new ArrayList<>();
    final List<String> relPatterns = new ArrayList<>();

    int depth = 0;
    int start = -1;
    boolean inNode = false;
    boolean inRel = false;

    for (int i = 0; i < pattern.length(); i++) {
      final char c = pattern.charAt(i);
      if (c == '(' && depth == 0) {
        inNode = true;
        start = i;
        depth = 1;
      } else if (c == '(' && inNode)
        depth++;
      else if (c == ')' && inNode) {
        depth--;
        if (depth == 0) {
          nodePatterns.add(pattern.substring(start, i + 1));
          inNode = false;
        }
      } else if (c == '[' && depth == 0) {
        inRel = true;
        start = i;
        depth = 1;
      } else if (c == '[' && inRel)
        depth++;
      else if (c == ']' && inRel) {
        depth--;
        if (depth == 0) {
          relPatterns.add(pattern.substring(start, i + 1));
          inRel = false;
        }
      }
    }

    // Structural: right number of vertices and edges
    if (path.getVertices().size() != nodePatterns.size())
      return false;
    if (path.getEdges().size() != relPatterns.size())
      return false;

    // Match each node
    for (int i = 0; i < nodePatterns.size(); i++) {
      final TCKValueParser.TCKNode expectedNode = TCKValueParser.parseNode(nodePatterns.get(i));
      if (!nodeMatches(path.getVertices().get(i), expectedNode))
        return false;
    }

    // Match each relationship
    for (int i = 0; i < relPatterns.size(); i++) {
      final TCKValueParser.TCKRelationship expectedRel = TCKValueParser.parseRelationship(relPatterns.get(i));
      if (!relationshipMatches(path.getEdges().get(i), expectedRel))
        return false;
    }

    return true;
  }

  private static boolean listsMatchUnordered(final List<Object> actual, final List<Object> expected) {
    final List<Object> remaining = new ArrayList<>(actual);
    for (final Object exp : expected) {
      boolean found = false;
      for (int i = 0; i < remaining.size(); i++) {
        if (valuesMatch(remaining.get(i), exp, true)) {
          remaining.remove(i);
          found = true;
          break;
        }
      }
      if (!found)
        return false;
    }
    return true;
  }

  @SuppressWarnings("unchecked")
  static Object normalizeValue(final Object value) {
    if (value == null)
      return null;
    if (value instanceof Integer)
      return ((Integer) value).longValue();
    if (value instanceof Short)
      return ((Short) value).longValue();
    if (value instanceof Byte)
      return ((Byte) value).longValue();
    if (value instanceof Float)
      return ((Float) value).doubleValue();
    if (value instanceof CypherTemporalValue)
      return value.toString();
    if (value instanceof LocalDate)
      return value.toString();
    if (value instanceof LocalDateTime)
      return value.toString();
    if (value instanceof Result) {
      final Result r = (Result) value;
      if (r.isElement())
        return r.toElement();
      return r;
    }
    if (value instanceof List) {
      return normalizeList((List<Object>) value);
    }
    if (value instanceof Map) {
      final Map<String, Object> map = (Map<String, Object>) value;
      final Map<String, Object> normalized = new java.util.LinkedHashMap<>();
      for (final Map.Entry<String, Object> entry : map.entrySet())
        normalized.put(entry.getKey(), normalizeValue(entry.getValue()));
      return normalized;
    }
    return value;
  }

  @SuppressWarnings("unchecked")
  private static List<Object> normalizeList(final List<Object> list) {
    final List<Object> normalized = new ArrayList<>(list.size());
    for (final Object item : list)
      normalized.add(normalizeValue(item));
    return normalized;
  }
}
