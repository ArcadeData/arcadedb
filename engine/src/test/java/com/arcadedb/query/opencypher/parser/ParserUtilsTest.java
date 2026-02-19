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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for ParserUtils.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ParserUtilsTest {

  @Test
  void stripBackticks() {
    // Simple backticked identifier
    assertThat(ParserUtils.stripBackticks("`name`")).isEqualTo("name");

    // Double backticks (escaped)
    assertThat(ParserUtils.stripBackticks("`my``name`")).isEqualTo("my`name");

    // No backticks
    assertThat(ParserUtils.stripBackticks("name")).isEqualTo("name");

    // Null input
    assertThat(ParserUtils.stripBackticks(null)).isNull();

    // Empty string
    assertThat(ParserUtils.stripBackticks("")).isEmpty();

    // Single backtick (invalid, but should handle gracefully)
    assertThat(ParserUtils.stripBackticks("`")).isEqualTo("`");
  }

  @Test
  void extractPropertyParts() {
    // Valid property expression
    final String[] parts = ParserUtils.extractPropertyParts("n.name");
    assertThat(parts).containsExactly("n", "name");

    // Nested property
    final String[] nested = ParserUtils.extractPropertyParts("n.address.city");
    assertThat(nested).containsExactly("n", "address.city");

    // No dot
    assertThat(ParserUtils.extractPropertyParts("name")).isNull();

    // Null input
    assertThat(ParserUtils.extractPropertyParts(null)).isNull();
  }

  @Test
  void parseValueString() {
    // String with single quotes
    assertThat(ParserUtils.parseValueString("'hello'")).isEqualTo("hello");

    // String with double quotes
    assertThat(ParserUtils.parseValueString("\"world\"")).isEqualTo("world");

    // Integer
    assertThat(ParserUtils.parseValueString("42")).isEqualTo(42L);

    // Decimal
    assertThat(ParserUtils.parseValueString("3.14")).isEqualTo(3.14);

    // Boolean true
    assertThat(ParserUtils.parseValueString("true")).isEqualTo(Boolean.TRUE);

    // Boolean false
    assertThat(ParserUtils.parseValueString("false")).isEqualTo(Boolean.FALSE);

    // Null
    assertThat(ParserUtils.parseValueString("null")).isNull();

    // Unquoted string (fallback)
    assertThat(ParserUtils.parseValueString("xyz")).isEqualTo("xyz");
  }

  @Test
  void decodeStringLiteral() {
    // No escape sequences
    assertThat(ParserUtils.decodeStringLiteral("hello")).isEqualTo("hello");

    // Newline
    assertThat(ParserUtils.decodeStringLiteral("hello\\nworld")).isEqualTo("hello\nworld");

    // Tab
    assertThat(ParserUtils.decodeStringLiteral("hello\\tworld")).isEqualTo("hello\tworld");

    // Backslash
    assertThat(ParserUtils.decodeStringLiteral("hello\\\\world")).isEqualTo("hello\\world");

    // Single quote
    assertThat(ParserUtils.decodeStringLiteral("it\\'s")).isEqualTo("it's");

    // Double quote
    assertThat(ParserUtils.decodeStringLiteral("say \\\"hello\\\"")).isEqualTo("say \"hello\"");

    // Multiple escape sequences
    assertThat(ParserUtils.decodeStringLiteral("line1\\nline2\\ttab")).isEqualTo("line1\nline2\ttab");

    // No backslash (fast path)
    assertThat(ParserUtils.decodeStringLiteral("simple")).isEqualTo("simple");

    // Null input
    assertThat(ParserUtils.decodeStringLiteral(null)).isNull();

    // Empty string
    assertThat(ParserUtils.decodeStringLiteral("")).isEmpty();
  }

  @Test
  void findOperatorOutsideParentheses() {
    // Operator at top level
    assertThat(ParserUtils.findOperatorOutsideParentheses("a = b", "=")).isEqualTo(2);

    // Operator inside parentheses (should not match)
    assertThat(ParserUtils.findOperatorOutsideParentheses("func(a = b)", "=")).isEqualTo(-1);

    // Operator after parentheses
    assertThat(ParserUtils.findOperatorOutsideParentheses("func(a) = b", "=")).isEqualTo(8);

    // Operator inside string literal (should not match)
    assertThat(ParserUtils.findOperatorOutsideParentheses("'a = b'", "=")).isEqualTo(-1);

    // Operator inside brackets (should not match)
    assertThat(ParserUtils.findOperatorOutsideParentheses("[a = b]", "=")).isEqualTo(-1);

    // Multiple operators, find first
    assertThat(ParserUtils.findOperatorOutsideParentheses("a = b = c", "=")).isEqualTo(2);

    // Operator with multiple characters
    assertThat(ParserUtils.findOperatorOutsideParentheses("a >= b", ">=")).isEqualTo(2);

    // No operator found
    assertThat(ParserUtils.findOperatorOutsideParentheses("a + b", "=")).isEqualTo(-1);
  }

  @Test
  void findOperatorWithNestedParentheses() {
    // Nested parentheses
    assertThat(ParserUtils.findOperatorOutsideParentheses("func(a, func2(b)) = c", "=")).isEqualTo(18);

    // Multiple levels of nesting
    assertThat(ParserUtils.findOperatorOutsideParentheses("((a + b)) = c", "=")).isEqualTo(10);

    // Mixed parentheses and brackets
    assertThat(ParserUtils.findOperatorOutsideParentheses("arr[func(x)] = y", "=")).isEqualTo(13);
  }

  @Test
  void findOperatorWithEscapedStrings() {
    // String with escaped quote
    assertThat(ParserUtils.findOperatorOutsideParentheses("'it\\'s' = x", "=")).isEqualTo(8);

    // String with double quotes
    assertThat(ParserUtils.findOperatorOutsideParentheses("\"hello\" = x", "=")).isEqualTo(8);
  }
}
