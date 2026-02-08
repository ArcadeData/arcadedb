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
package com.arcadedb.query.opencypher.parser;

import com.arcadedb.query.opencypher.ast.Direction;
import com.arcadedb.query.opencypher.ast.NodePattern;
import com.arcadedb.query.opencypher.ast.PathPattern;
import com.arcadedb.query.opencypher.ast.RelationshipPattern;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Simple regex-based pattern parser for Cypher path patterns.
 * Phase 2: Handles basic relationship patterns.
 * TODO: Replace with full ANTLR4 parser integration in later phases.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class PatternParser {

  // Pattern: (node)-[relationship]->(node) or (node)<-[relationship]-(node) or (node)-[relationship]-(node)
  private static final Pattern PATH_PATTERN = Pattern.compile(
      "\\(([^)]*)\\)\\s*([<-]?)\\s*-\\s*\\[([^\\]]*)\\]\\s*-\\s*([>-]?)\\s*\\(([^)]*)\\)");

  // Pattern: single node (n:Label)
  private static final Pattern SINGLE_NODE_PATTERN = Pattern.compile("\\(([^)]*)\\)");

  /**
   * Parses path patterns from a match pattern string.
   * Supports: (a:Label)-[r:TYPE]->(b:Label), (a)-[*1..3]->(b), (n:Person), etc.
   *
   * @param pattern pattern string
   * @return list of parsed path patterns
   */
  public static List<PathPattern> parsePathPatterns(final String pattern) {
    final List<PathPattern> result = new ArrayList<>();

    final Matcher pathMatcher = PATH_PATTERN.matcher(pattern);
    if (pathMatcher.find()) {
      final String node1Str = pathMatcher.group(1).trim();
      final String leftArrow = pathMatcher.group(2).trim();
      final String relStr = pathMatcher.group(3).trim();
      final String rightArrow = pathMatcher.group(4).trim();
      final String node2Str = pathMatcher.group(5).trim();

      final NodePattern node1 = parseNodePattern(node1Str);
      final RelationshipPattern rel = parseRelationshipPattern(relStr, leftArrow, rightArrow);
      final NodePattern node2 = parseNodePattern(node2Str);

      result.add(new PathPattern(node1, rel, node2));
    } else {
      // Single node pattern: (n:Label)
      final Matcher singleMatcher = SINGLE_NODE_PATTERN.matcher(pattern);
      if (singleMatcher.find()) {
        final String nodeStr = singleMatcher.group(1).trim();
        final NodePattern node = parseNodePattern(nodeStr);
        result.add(new PathPattern(node));
      }
    }

    return result;
  }

  /**
   * Parses a node pattern string: n:Label or n or :Label or n:Label {prop: value}
   *
   * @param nodeStr node pattern string
   * @return parsed NodePattern
   */
  public static NodePattern parseNodePattern(final String nodeStr) {
    if (nodeStr.isEmpty())
      return new NodePattern(null, null, null);

    String variable = null;
    List<String> labels = null;
    Map<String, Object> properties = null;

    // Extract properties if present: {...}
    String remainingStr = nodeStr;
    if (nodeStr.contains("{")) {
      final int braceStart = nodeStr.indexOf('{');
      final int braceEnd = nodeStr.lastIndexOf('}');
      if (braceEnd > braceStart) {
        final String propsStr = nodeStr.substring(braceStart + 1, braceEnd);
        properties = parseProperties(propsStr);
        remainingStr = nodeStr.substring(0, braceStart).trim();
      }
    }

    // Check for label: n:Label or :Label
    if (remainingStr.contains(":")) {
      final String[] parts = remainingStr.split(":", 2);
      if (!parts[0].trim().isEmpty())
        variable = parts[0].trim();

      if (parts.length > 1 && !parts[1].trim().isEmpty())
        labels = List.of(parts[1].trim());

    } else
      variable = remainingStr.trim();

    return new NodePattern(variable, labels, properties);
  }

  /**
   * Parses property map from string: name: 'Alice', age: 30
   *
   * @param propsStr properties string
   * @return parsed property map
   */
  private static Map<String, Object> parseProperties(final String propsStr) {
    final Map<String, Object> properties = new HashMap<>();
    if (propsStr.trim().isEmpty()) {
      return properties;
    }

    // Simple comma-split (doesn't handle nested objects or arrays)
    final String[] parts = propsStr.split(",");
    for (final String part : parts) {
      final String[] keyValue = part.split(":", 2);
      if (keyValue.length == 2) {
        final String key = keyValue[0].trim();
        String value = keyValue[1].trim();

        // Parse value type
        Object parsedValue;
        if (value.startsWith("'") || value.startsWith("\"")) {
          // String literal - keep quotes for later processing
          parsedValue = value;
        } else if (value.equalsIgnoreCase("true") || value.equalsIgnoreCase("false")) {
          // Boolean
          parsedValue = Boolean.parseBoolean(value);
        } else {
          // Try to parse as number
          try {
            if (value.contains(".")) {
              parsedValue = Double.parseDouble(value);
            } else {
              parsedValue = Integer.parseInt(value);
            }
          } catch (final NumberFormatException e) {
            // Keep as string if parsing fails
            parsedValue = value;
          }
        }

        properties.put(key, parsedValue);
      }
    }

    return properties;
  }

  /**
   * Parses a relationship pattern string: r:TYPE or r or :TYPE or *1..3 or r:TYPE {prop: value}
   *
   * @param relStr     relationship pattern string
   * @param leftArrow  left arrow indicator ("<" or empty)
   * @param rightArrow right arrow indicator (">" or empty)
   * @return parsed RelationshipPattern
   */
  public static RelationshipPattern parseRelationshipPattern(final String relStr, final String leftArrow,
                                                             final String rightArrow) {
    String variable = null;
    List<String> types = null;
    Integer minHops = null;
    Integer maxHops = null;
    Map<String, Object> properties = null;

    // Determine direction
    final Direction direction;
    if ("<".equals(leftArrow)) {
      direction = Direction.IN;
    } else if (">".equals(rightArrow)) {
      direction = Direction.OUT;
    } else {
      direction = Direction.BOTH;
    }

    if (relStr.isEmpty()) {
      return new RelationshipPattern(null, null, direction, null, minHops, maxHops);
    }

    // Extract properties if present: {...}
    String remainingStr = relStr;
    if (relStr.contains("{")) {
      final int braceStart = relStr.indexOf('{');
      final int braceEnd = relStr.lastIndexOf('}');
      if (braceEnd > braceStart) {
        final String propsStr = relStr.substring(braceStart + 1, braceEnd);
        properties = parseProperties(propsStr);
        remainingStr = relStr.substring(0, braceStart).trim();
      }
    }

    // Check for variable-length: *1..3 or *..3 or *1.. or *
    if (remainingStr.contains("*")) {
      final String[] parts = remainingStr.split("\\*", 2);

      // Parse variable and type before *
      if (!parts[0].trim().isEmpty()) {
        if (parts[0].contains(":")) {
          final String[] labelParts = parts[0].split(":", 2);
          if (!labelParts[0].trim().isEmpty()) {
            variable = labelParts[0].trim();
          }
          if (labelParts.length > 1 && !labelParts[1].trim().isEmpty()) {
            types = Arrays.asList(labelParts[1].trim());
          }
        } else {
          variable = parts[0].trim();
        }
      }

      // Parse range after *
      if (parts.length > 1 && !parts[1].isEmpty()) {
        final String rangeStr = parts[1].trim();
        if (rangeStr.contains("..")) {
          final String[] rangeParts = rangeStr.split("\\.\\.", 2);
          if (!rangeParts[0].isEmpty()) {
            minHops = Integer.parseInt(rangeParts[0]);
          }
          if (rangeParts.length > 1 && !rangeParts[1].isEmpty()) {
            maxHops = Integer.parseInt(rangeParts[1]);
          }
        } else if (!rangeStr.isEmpty()) {
          // Single number: *3 means exactly 3 hops
          minHops = maxHops = Integer.parseInt(rangeStr);
        }
      }
      // Bare * with no range: [*] means [*1..] (min=1, max=unbounded)
      if (minHops == null && maxHops == null)
        minHops = 1;
    } else {
      // Fixed-length relationship
      if (remainingStr.contains(":")) {
        final String[] parts = remainingStr.split(":", 2);
        if (!parts[0].trim().isEmpty()) {
          variable = parts[0].trim();
        }
        if (parts.length > 1 && !parts[1].trim().isEmpty()) {
          types = Arrays.asList(parts[1].trim());
        }
      } else {
        variable = remainingStr.trim();
      }
    }

    return new RelationshipPattern(variable, types, direction, properties, minHops, maxHops);
  }
}
