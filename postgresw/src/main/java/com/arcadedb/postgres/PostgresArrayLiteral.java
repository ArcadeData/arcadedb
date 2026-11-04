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
package com.arcadedb.postgres;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Reads a PostgreSQL array literal - the text format of every array type (issue #10870) - with the grammar of PostgreSQL's own
 * {@code array_in}:
 * <ul>
 *   <li>an element is SQL NULL only when it is the unquoted token {@code NULL}, in any case; the quoted {@code "NULL"} is the
 *   four-character string</li>
 *   <li>a quoted element is never NULL and never dropped: {@code ""} is the empty string</li>
 *   <li>an unquoted element is trimmed of the whitespace around it, a backslash escapes the next character in either form, and an
 *   escaped character is never trimmed</li>
 *   <li>an empty unquoted element ({@code {a,,b}}), junk after a closing quote or brace, and an unbalanced brace are malformed, and
 *   refused rather than read as some other value</li>
 *   <li>a nested brace is a dimension, and the sub-arrays of one level must all have the same length, as PostgreSQL requires. A
 *   multidimensional array is read FLAT, in row-major order - {@code {{1,2},{3,4}}} is {@code [1, 2, 3, 4]} - exactly as the binary
 *   format's decoder reads one: the query engine binds an array parameter as one flat collection (an {@code IN} list), and the two
 *   formats of the same array must not reach it as two different values</li>
 *   <li>an optional dimension decoration ({@code [1:3]={a,b,c}}) is accepted and its bounds ignored, since a list has no lower
 *   bound</li>
 *   <li>two leniencies this protocol has always had are kept, since neither loses an element: an empty text is the empty array,
 *   and a list without braces ({@code 1,2,3}) is read as if it had them</li>
 * </ul>
 * Elements come back in the order and the count the literal declares, never filtered: what this class reads is exactly the list
 * {@code PostgresType}'s array writer wrote, so the server can read back its own text.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class PostgresArrayLiteral {
  // PostgreSQL caps array dimensions at 6 (MAXDIM): a literal nested deeper is refused before it can recurse further, so a client
  // cannot exhaust the stack with a few kilobytes of opening braces
  private static final int MAX_DIMENSIONS    = 6;
  private static final int MAX_QUOTED_LENGTH = 128;

  private final String              text;
  private final Function<String, ?> elementParser;
  private       int                 pos;
  private       int                 depth;

  private PostgresArrayLiteral(final String text, final Function<String, ?> elementParser) {
    this.text = text;
    this.elementParser = elementParser;
  }

  /**
   * Parses an array literal, converting every non-null element with {@code elementParser}.
   *
   * @throws PostgresProtocolException when the text is not a well-formed array literal
   */
  static ArrayList<Object> parse(final String text, final Function<String, ?> elementParser) {
    if (text == null)
      throw malformed(null, "the value is missing");
    final String trimmed = text.strip();
    if (trimmed.isEmpty())
      return new ArrayList<>();
    if (trimmed.charAt(0) != '{' && trimmed.charAt(0) != '[')
      return new PostgresArrayLiteral("{" + trimmed + "}", elementParser).parseLiteral();
    return new PostgresArrayLiteral(text, elementParser).parseLiteral();
  }

  /**
   * Parses the text as an array literal of strings when it is a well-formed one, and answers null otherwise. For a parameter whose
   * type the client left unspecified: a value is a list only when it is an array literal PostgreSQL itself would read as one, so a
   * JSON document such as {@code {"k": 1}}, which merely starts and ends with a brace, stays the string it was sent as.
   */
  static ArrayList<Object> tryParse(final String text) {
    try {
      return parse(text, s -> s);
    } catch (final PostgresProtocolException e) {
      return null;
    }
  }

  private ArrayList<Object> parseLiteral() {
    skipWhitespace();
    if (pos < text.length() && text.charAt(pos) == '[')
      skipDimensionDecoration();

    skipWhitespace();
    if (pos >= text.length() || text.charAt(pos) != '{')
      throw malformed(text, "array value must start with \"{\" or dimension information");

    final ArrayList<Object> result = parseLevel();

    skipWhitespace();
    if (pos < text.length())
      throw malformed(text, "junk after closing right brace");
    return result;
  }

  /** Reads one {@code {...}} level, the opening brace at {@code pos}, its sub-arrays flattened into it. */
  private ArrayList<Object> parseLevel() {
    if (++depth > MAX_DIMENSIONS)
      throw malformed(text, "number of array dimensions exceeds the maximum allowed (" + MAX_DIMENSIONS + ")");
    try {
      return parseLevelElements();
    } finally {
      --depth;
    }
  }

  private ArrayList<Object> parseLevelElements() {
    ++pos; // the opening brace
    final ArrayList<Object> elements = new ArrayList<>();

    skipWhitespace();
    if (pos < text.length() && text.charAt(pos) == '}') {
      ++pos;
      return elements;
    }

    // whether this level holds sub-arrays or scalars: PostgreSQL refuses a level that mixes the two
    Boolean nested = null;
    int subArrayLength = -1;
    while (true) {
      skipWhitespace();
      if (pos >= text.length())
        throw malformed(text, "unexpected end of input");

      final char c = text.charAt(pos);
      if (c == '{') {
        if (Boolean.FALSE.equals(nested))
          throw malformed(text, "unexpected \"{\" character");
        nested = Boolean.TRUE;
        final ArrayList<Object> subArray = parseLevel();
        if (subArrayLength == -1)
          subArrayLength = subArray.size();
        else if (subArrayLength != subArray.size())
          throw malformed(text, "multidimensional arrays must have sub-arrays with matching dimensions");
        elements.addAll(subArray);
      } else {
        if (Boolean.TRUE.equals(nested))
          throw malformed(text, "unexpected array element");
        nested = Boolean.FALSE;
        elements.add(c == '"' ? convert(readQuoted()) : readUnquoted());
      }

      skipWhitespace();
      if (pos >= text.length())
        throw malformed(text, "unexpected end of input");
      final char separator = text.charAt(pos++);
      if (separator == '}')
        return elements;
      if (separator != ',')
        throw malformed(text, "unexpected \"" + separator + "\" character");
    }
  }

  /** A quoted element, the opening quote at {@code pos}: everything up to the closing quote, never NULL, possibly empty. */
  private String readQuoted() {
    ++pos; // the opening quote
    final StringBuilder element = new StringBuilder();
    while (pos < text.length()) {
      final char c = text.charAt(pos++);
      if (c == '"')
        return element.toString();
      if (c == '\\') {
        if (pos >= text.length())
          break;
        element.append(text.charAt(pos++));
      } else
        element.append(c);
    }
    throw malformed(text, "unexpected end of input");
  }

  /** An unquoted element: up to the next unescaped comma or closing brace, trimmed, NULL when it is the bare token NULL. */
  private Object readUnquoted() {
    final StringBuilder element = new StringBuilder();
    // the length the element has without its trailing whitespace: an escaped character counts as content, so it is never trimmed
    int significantLength = 0;
    boolean escaped = false;
    while (pos < text.length()) {
      final char c = text.charAt(pos);
      if (c == ',' || c == '}')
        break;
      if (c == '{' || c == '"')
        throw malformed(text, "unexpected \"" + c + "\" character");
      ++pos;
      if (c == '\\') {
        if (pos >= text.length())
          throw malformed(text, "unexpected end of input");
        element.append(text.charAt(pos++));
        significantLength = element.length();
        escaped = true;
      } else {
        element.append(c);
        if (!Character.isWhitespace(c))
          significantLength = element.length();
      }
    }
    element.setLength(significantLength);

    if (element.isEmpty())
      throw malformed(text, "unexpected \"" + (pos < text.length() ? text.charAt(pos) : ' ') + "\" character");

    final String value = element.toString();
    if (!escaped && value.equalsIgnoreCase("NULL"))
      return null;
    return convert(value);
  }

  private Object convert(final String element) {
    return elementParser.apply(element);
  }

  /** Skips {@code [lower:upper]...=}: the bounds a list cannot carry. */
  private void skipDimensionDecoration() {
    while (pos < text.length() && text.charAt(pos) == '[') {
      final int close = text.indexOf(']', pos);
      if (close < 0)
        throw malformed(text, "missing \"]\" in array dimensions");
      pos = close + 1;
      skipWhitespace();
    }
    if (pos >= text.length() || text.charAt(pos) != '=')
      throw malformed(text, "missing \"=\" after array dimensions");
    ++pos;
  }

  private void skipWhitespace() {
    while (pos < text.length() && Character.isWhitespace(text.charAt(pos)))
      ++pos;
  }

  private static PostgresProtocolException malformed(final String text, final String detail) {
    // the literal is the client's, and can be megabytes: the message quotes enough of it to recognise, not all of it
    final String quoted = text == null || text.length() <= MAX_QUOTED_LENGTH ? text : text.substring(0, MAX_QUOTED_LENGTH) + "...";
    return new PostgresProtocolException("malformed array literal: \"" + quoted + "\" (" + detail + ")");
  }
}
