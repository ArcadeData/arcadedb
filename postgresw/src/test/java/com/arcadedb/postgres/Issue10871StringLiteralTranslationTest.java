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

import com.arcadedb.exception.CommandSQLParsingException;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10871: a PostgreSQL string literal is read with PostgreSQL's rules - a backslash is an ordinary character in a
 * plain literal while {@code standard_conforming_strings} is on, an escape in an {@code E'...'} literal and in every
 * literal with the setting off - and handed to the engine in the form its lexer decodes to that same value.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10871StringLiteralTranslationTest {

  private static String on(final String sql) {
    return PostgresQuotedIdentifierRewriter.toEngineLiterals(sql, true);
  }

  private static String off(final String sql) {
    return PostgresQuotedIdentifierRewriter.toEngineLiterals(sql, false);
  }

  @Test
  void aBackslashInAPlainLiteralIsAnOrdinaryCharacterWithStandardConformingStrings() {
    assertThat(on("SELECT 'a\\b' AS v")).isEqualTo("SELECT 'a\\\\b' AS v");
    assertThat(on("SELECT 'x\\ty' AS v")).isEqualTo("SELECT 'x\\\\ty' AS v");
    // a literal ending in a backslash is complete, and the one after it is a literal of its own
    assertThat(on("SELECT 'a\\' AS v, 'b' AS w")).isEqualTo("SELECT 'a\\\\' AS v, 'b' AS w");
    assertThat(on("INSERT INTO Bs (v) VALUES ('C:\\')")).isEqualTo("INSERT INTO Bs (v) VALUES ('C:\\\\')");
  }

  @Test
  void theDoubledQuoteIsWrittenTheWayTheEngineEscapesAQuote() {
    assertThat(on("SELECT 'it''s' AS v")).isEqualTo("SELECT 'it\\'s' AS v");
    assertThat(on("SELECT '''' AS v")).isEqualTo("SELECT '\\'' AS v");
  }

  @Test
  void anEscapeStringDecodesPostgresEscapes() {
    assertThat(on("SELECT E'a\\tb' AS v")).isEqualTo("SELECT 'a\tb' AS v");
    assertThat(on("SELECT e'a\\\\b' AS v")).isEqualTo("SELECT 'a\\\\b' AS v");
    assertThat(on("SELECT E'it\\'s' AS v")).isEqualTo("SELECT 'it\\'s' AS v");
    // \b is a backspace, not a dropped backslash
    assertThat(on("SELECT E'a\\bc' AS v")).isEqualTo("SELECT 'a\bc' AS v");
    assertThat(on("SELECT E'\\101\\x42\\u0043\\U00000044' AS v")).isEqualTo("SELECT 'ABCD' AS v");
    assertThat(on("SELECT E'\\U0001F600' AS v")).isEqualTo("SELECT '\uD83D\uDE00' AS v");
    // an escaped character with no meaning of its own stands for itself, and so does an \x with no digit
    assertThat(on("SELECT E'\\q\\xz' AS v")).isEqualTo("SELECT 'qxz' AS v");
    // line breaks, which the engine lexer does not accept inside a literal, are escaped
    assertThat(on("SELECT E'a\\nb\\rc' AS v")).isEqualTo("SELECT 'a\\nb\\rc' AS v");
    assertThat(on("SELECT 'a\nb' AS v")).isEqualTo("SELECT 'a\\nb' AS v");
  }

  @Test
  void aMalformedUnicodeEscapeIsRefusedAsPostgresRefusesIt() {
    assertThatThrownBy(() -> on("SELECT E'abc \\u30 def' AS v")).isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("invalid Unicode escape");
    assertThatThrownBy(() -> on("SELECT E'\\U00110000' AS v")).isInstanceOf(CommandSQLParsingException.class);
    assertThatThrownBy(() -> on("SELECT E'\\UFFFFFFFF' AS v")).isInstanceOf(CommandSQLParsingException.class);
    // a pair of 4-digit surrogate escapes spells one character
    assertThat(on("SELECT E'\\uD83D\\uDE00' AS v")).isEqualTo("SELECT '😀' AS v");
    // in a plain literal the backslash is an ordinary character: nothing to refuse
    assertThat(on("SELECT 'abc \\u30 def' AS v")).isEqualTo("SELECT 'abc \\\\u30 def' AS v");
  }

  @Test
  void anEOnlyPrefixesAnEscapeStringWhenItIsNotTheEndOfAWord() {
    assertThat(on("SELECT name FROM V WHERE'a\\b' = name")).isEqualTo("SELECT name FROM V WHERE'a\\\\b' = name");
    assertThat(on("SELECT (E'a\\tb') AS v")).isEqualTo("SELECT ('a\tb') AS v");
  }

  @Test
  void everyLiteralProcessesEscapesWithStandardConformingStringsOff() {
    assertThat(off("SELECT 'a\\\\b' AS v")).isEqualTo("SELECT 'a\\\\b' AS v");
    assertThat(off("SELECT 'a\\tb' AS v")).isEqualTo("SELECT 'a\tb' AS v");
    assertThat(off("SELECT 'it\\'s', 'b' AS v")).isEqualTo("SELECT 'it\\'s', 'b' AS v");
  }

  @Test
  void whatIsNotAPostgresLiteralIsLeftAsItIs() {
    // nothing to translate: the same instance
    final String plain = "SELECT 'abc' AS v FROM V WHERE x = 1";
    assertThat(on(plain)).isSameAs(plain);
    final String noLiteral = "SELECT `a\\b` FROM V";
    assertThat(on(noLiteral)).isSameAs(noLiteral);

    // comments, back-tick identifiers and ArcadeDB double-quoted strings are copied verbatim
    assertThat(on("SELECT `it's` FROM V -- 'a\\b'\n WHERE \"@rid\" = '#1:0' /* 'c\\' */"))
        .isEqualTo("SELECT `it's` FROM V -- 'a\\b'\n WHERE \"@rid\" = '#1:0' /* 'c\\' */");
    // inside an ArcadeDB JSON object a single-quoted literal keeps the ArcadeDB meaning
    assertThat(on("INSERT INTO T CONTENT {\"k\": 'a\\tb'}")).isEqualTo("INSERT INTO T CONTENT {\"k\": 'a\\tb'}");
    // square brackets are also PostgreSQL's: a literal between them is a PostgreSQL literal
    assertThat(on("SELECT ARRAY['C:\\'] AS v")).isEqualTo("SELECT ARRAY['C:\\\\'] AS v");
    // an unterminated literal is left for the parser to report
    assertThat(on("SELECT 'abc")).isEqualTo("SELECT 'abc");
    assertThat(on("SELECT E'abc\\'")).isEqualTo("SELECT E'abc\\'");
  }
}
