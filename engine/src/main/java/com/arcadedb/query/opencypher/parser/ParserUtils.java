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

import com.arcadedb.query.opencypher.grammar.Cypher25Parser;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility methods for Cypher parser operations.
 * Provides common parsing utilities to reduce code duplication and improve maintainability.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ParserUtils {

  /**
   * Strips backticks from an escaped symbolic name.
   * Handles both regular backticks and double backticks (escaped backticks).
   *
   * @param name the name potentially wrapped in backticks
   * @return the name without backticks
   */
  public static String stripBackticks(final String name) {
    if (name == null || name.length() < 2)
      return name;

    // Check if wrapped in backticks
    if (name.startsWith("`") && name.endsWith("`")) {
      // Remove outer backticks
      String inner = name.substring(1, name.length() - 1);
      // Replace double backticks (escaped backticks) with single backticks
      inner = inner.replace("``", "`");
      return inner;
    }

    return name;
  }

  /**
   * Extract labels from a label expression context using grammar-based parsing.
   * Handles multiple labels, alternative labels, and combinations.
   * Examples:
   * - :Person:Developer -> ["Person", "Developer"]
   * - :Person|Developer -> ["Person", "Developer"]
   * - :Person:Developer|Manager -> ["Person", "Developer", "Manager"]
   *
   * @param ctx the label expression context
   * @return list of label names with backticks stripped
   */
  public static List<String> extractLabels(final Cypher25Parser.LabelExpressionContext ctx) {
    // Use text-based parsing for now (can be improved with grammar traversal later)
    final String text = ctx.getText();

    // Remove leading colon and split by both : and | to get all labels
    final String cleanText = text.replaceAll("^:+", "");

    // Split by both : and | and & to handle multiple labels and alternatives
    final String[] parts = cleanText.split("[:&|]+");

    // Strip backticks from each label if present
    final List<String> labels = new ArrayList<>();
    for (final String part : parts) {
      if (!part.isEmpty())
        labels.add(stripBackticks(part));
    }
    return labels;
  }

  /**
   * Parse a property expression in the form "variable.property" and return the parts.
   *
   * @param propertyExpression the property expression text (e.g., "n.name")
   * @return array of [variable, property], or null if invalid format
   */
  public static String[] extractPropertyParts(final String propertyExpression) {
    if (propertyExpression == null || !propertyExpression.contains("."))
      return null;

    final String[] parts = propertyExpression.split("\\.", 2);
    if (parts.length == 2)
      return parts;

    return null;
  }

  /**
   * Parse a value string into its appropriate type (String, Number, Boolean, null).
   * Handles quoted strings, numbers (integer/decimal), booleans, and null.
   *
   * @param value the string representation of the value
   * @return the parsed value object
   */
  public static Object parseValueString(final String value) {
    if (value == null)
      return null;

    // Remove quotes from strings
    if (value.startsWith("'") && value.endsWith("'"))
      return value.substring(1, value.length() - 1);

    if (value.startsWith("\"") && value.endsWith("\""))
      return value.substring(1, value.length() - 1);

    // Check for null
    if ("null".equalsIgnoreCase(value))
      return null;

    // Check for boolean
    if ("true".equalsIgnoreCase(value))
      return Boolean.TRUE;

    if ("false".equalsIgnoreCase(value))
      return Boolean.FALSE;

    // Try to parse as number
    try {
      if (value.contains("."))
        return Double.parseDouble(value);
      else
        return Long.parseLong(value);
    } catch (final NumberFormatException e) {
      // Not a number, return as string
      return value;
    }
  }

  /**
   * Decodes escape sequences in a string literal.
   * Handles: \n (newline), \t (tab), \r (carriage return), \\ (backslash), \' (single quote), \" (double quote)
   *
   * @param input the string with escape sequences (without surrounding quotes)
   * @return the decoded string
   */
  public static String decodeStringLiteral(final String input) {
    if (input == null || input.isEmpty())
      return input;

    // Quick check: if no backslash, return as-is to avoid allocation
    if (input.indexOf('\\') == -1)
      return input;

    final StringBuilder result = new StringBuilder(input.length());
    boolean escaped = false;

    for (int i = 0; i < input.length(); i++) {
      final char c = input.charAt(i);

      if (escaped) {
        escaped = false;
        switch (c) {
          case 'n':
            result.append('\n');
            break;
          case 't':
            result.append('\t');
            break;
          case 'r':
            result.append('\r');
            break;
          case 'b':
            result.append('\b');
            break;
          case 'f':
            result.append('\f');
            break;
          case '\\':
            result.append('\\');
            break;
          case '\'':
            result.append('\'');
            break;
          case '"':
            result.append('"');
            break;
          case '0':
            result.append('\0');
            break;
          default:
            // For unrecognized escape sequences, keep the character as-is
            result.append(c);
            break;
        }
      } else if (c == '\\') {
        escaped = true;
      } else {
        result.append(c);
      }
    }

    // Handle trailing backslash (keep it as-is)
    if (escaped)
      result.append('\\');

    return result.toString();
  }

  /**
   * Find an operator outside parentheses in an expression string.
   * This is used to parse comparison expressions while respecting parenthesized sub-expressions.
   * Also tracks string literals and bracket depth.
   *
   * @param text the expression text
   * @param operator the operator to find
   * @return the index of the operator, or -1 if not found outside parentheses
   */
  public static int findOperatorOutsideParentheses(final String text, final String operator) {
    int parenDepth = 0;
    int bracketDepth = 0;
    boolean inString = false;
    char stringChar = 0;
    final int opLen = operator.length();

    for (int i = 0; i <= text.length() - opLen; i++) {
      final char c = text.charAt(i);

      // Track string literals
      if ((c == '\'' || c == '"') && (i == 0 || text.charAt(i - 1) != '\\')) {
        if (!inString) {
          inString = true;
          stringChar = c;
        } else if (c == stringChar) {
          inString = false;
        }
        continue;
      }

      if (inString)
        continue;

      // Track parentheses
      if (c == '(') {
        parenDepth++;
        continue;
      }
      if (c == ')') {
        parenDepth--;
        continue;
      }

      // Track brackets
      if (c == '[') {
        bracketDepth++;
        continue;
      }
      if (c == ']') {
        bracketDepth--;
        continue;
      }

      // Only match operator at top level
      if (parenDepth == 0 && bracketDepth == 0 && text.substring(i, i + opLen).equals(operator))
        return i;
    }

    return -1;
  }
}
