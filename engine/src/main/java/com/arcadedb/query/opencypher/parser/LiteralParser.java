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

import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.query.opencypher.ast.*;

/**
 * Reads a literal from its text: null, a boolean, a string, an integer in decimal, hexadecimal or octal, a float.
 * <p>
 * Split out of {@link CypherExpressionBuilder}, which decides what an expression is and builds it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class LiteralParser {
  private LiteralParser() {
  }

  /**
   * Try to parse text as a literal value (number, string, boolean, null).
   */
  static Object tryParseLiteral(final String text) {
    // Null
    if ("null".equalsIgnoreCase(text)) {
      return null; // Return null as a marker that we found a literal
    }

    // Boolean
    if ("true".equalsIgnoreCase(text)) {
      return Boolean.TRUE;
    }
    if ("false".equalsIgnoreCase(text)) {
      return Boolean.FALSE;
    }

    // String (quoted) - strip quotes and decode escape sequences
    if (text.startsWith("'") && text.endsWith("'") && text.length() >= 2) {
      return CypherASTBuilder.decodeStringLiteral(text.substring(1, text.length() - 1));
    }
    if (text.startsWith("\"") && text.endsWith("\"") && text.length() >= 2) {
      return CypherASTBuilder.decodeStringLiteral(text.substring(1, text.length() - 1));
    }

    // Number
    try {
      // Handle floats without integer part (.1, .5e-3, etc.)
      if (text.startsWith(".") && text.length() > 1 && Character.isDigit(text.charAt(1)))
        return parseDoubleChecked(text);
      if (text.startsWith("-.") && text.length() > 2 && Character.isDigit(text.charAt(2)))
        return parseDoubleChecked(text);

      // Handle negative numbers
      if (text.startsWith("-") || text.startsWith("+") || Character.isDigit(text.charAt(0))) {
        // Check hex/octal BEFORE float detection because hex digits can contain 'e'/'E'
        if (text.startsWith("0x") || text.startsWith("0X") ||
            text.startsWith("-0x") || text.startsWith("-0X")) {
          final String hexPart = text.startsWith("-") ? text.substring(3) : text.substring(2);
          return parseSignedHexOrOctal(hexPart, 16, text.startsWith("-"), text);
        } else if (text.startsWith("0o") || text.startsWith("0O") ||
            text.startsWith("-0o") || text.startsWith("-0O")) {
          final String octPart = text.startsWith("-") ? text.substring(3) : text.substring(2);
          return parseSignedHexOrOctal(octPart, 8, text.startsWith("-"), text);
        } else if (text.contains(".") || text.contains("e") || text.contains("E")) {
          return parseDoubleChecked(text);
        } else {
          return Long.parseLong(text);
        }
      }
    } catch (final NumberFormatException e) {
      // Check if it looks like a number but overflowed or is invalid
      final String numText = text.startsWith("-") || text.startsWith("+") ? text.substring(1) : text;
      if (numText.startsWith("0x") || numText.startsWith("0X") ||
          numText.startsWith("0o") || numText.startsWith("0O"))
        throw new CommandParsingException("InvalidNumberLiteral: Invalid number literal: " + text);
      if (!numText.isEmpty() && Character.isDigit(numText.charAt(0))) {
        if (numText.chars().allMatch(c -> Character.isDigit(c)))
          throw new CommandParsingException("IntegerOverflow: Integer literal is too large: " + text);
        throw new CommandParsingException("InvalidNumberLiteral: Invalid number literal: " + text);
      }
    }

    return null; // Not a literal
  }

  private static double parseDoubleChecked(final String text) {
    final double d = Double.parseDouble(text);
    if (Double.isInfinite(d))
      throw new CommandParsingException("FloatingPointOverflow: Floating point number is too large: " + text);
    return d;
  }

  private static long parseSignedHexOrOctal(final String digits, final int radix,
      final boolean negative, final String originalText) {
    final long val;
    try {
      val = Long.parseUnsignedLong(digits, radix);
    } catch (final NumberFormatException e) {
      throw new CommandParsingException("IntegerOverflow: Integer literal is too large: " + originalText);
    }
    if (negative) {
      // -0x8000000000000000 is Long.MIN_VALUE, anything more negative overflows
      if (val == Long.MIN_VALUE)
        return Long.MIN_VALUE; // -(-2^63) would overflow, but -val wraps correctly for MIN_VALUE
      if (val < 0) // unsigned value > Long.MAX_VALUE (bit 63 set), so magnitude > 2^63
        throw new CommandParsingException("IntegerOverflow: Integer literal is too large: " + originalText);
      return -val;
    } else {
      if (val < 0) // unsigned value > Long.MAX_VALUE (bit 63 set)
        throw new CommandParsingException("IntegerOverflow: Integer literal is too large: " + originalText);
      return val;
    }
  }
}
