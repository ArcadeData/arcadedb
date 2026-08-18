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

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the catalog query lexer of issue #6412. Tokenising is what lets the catalog work on the
 * structure of a client's query rather than on its exact text, so the cases that matter here are the ones
 * where a character means something other than itself: quotes, escapes, comments and multi-character
 * operators.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PostgresCatalogTokenTest {

  @Test
  void wordsNumbersAndSymbolsAreSeparated() {
    final List<PostgresCatalogToken> tokens = PostgresCatalogToken.tokenize("SELECT oid, 42 FROM pg_type");

    assertThat(texts(tokens)).containsExactly("SELECT", "oid", ",", "42", "FROM", "pg_type");
    assertThat(tokens.get(0).type).isEqualTo(PostgresCatalogToken.Type.IDENTIFIER);
    assertThat(tokens.get(3).type).isEqualTo(PostgresCatalogToken.Type.NUMBER);
    assertThat(tokens.get(2).type).isEqualTo(PostgresCatalogToken.Type.SYMBOL);
  }

  @Test
  void aStringLiteralKeepsItsContentAndDoublesAsItsOwnEscape() {
    final List<PostgresCatalogToken> tokens = PostgresCatalogToken.tokenize("WHERE relname = 'it''s here'");

    final PostgresCatalogToken literal = tokens.get(tokens.size() - 1);
    assertThat(literal.type).isEqualTo(PostgresCatalogToken.Type.STRING);
    assertThat(literal.text).isEqualTo("it's here");
  }

  @Test
  void anEscapeStringLiteralUnescapesBackslashes() {
    // pgjdbc writes E'%' and E'\\_' in the LIKE patterns of its metadata queries.
    final List<PostgresCatalogToken> tokens = PostgresCatalogToken.tokenize("nspname LIKE E'a\\\\_b\\nc'");

    final PostgresCatalogToken literal = tokens.get(tokens.size() - 1);
    assertThat(literal.type).isEqualTo(PostgresCatalogToken.Type.STRING);
    assertThat(literal.text).isEqualTo("a\\_b\nc");
  }

  @Test
  void aQuotedIdentifierKeepsItsCase() {
    final List<PostgresCatalogToken> tokens = PostgresCatalogToken.tokenize("SELECT nspname AS \"TABLE_SCHEM\"");

    final PostgresCatalogToken alias = tokens.get(tokens.size() - 1);
    assertThat(alias.type).isEqualTo(PostgresCatalogToken.Type.QUOTED_IDENTIFIER);
    assertThat(alias.text).isEqualTo("TABLE_SCHEM");
  }

  @Test
  void aBacktickQuotedIdentifierIsReadTheSameWay() {
    // The executor rewrites a client's "double quoted" identifiers into backticks before dispatching, so this
    // is the form a catalog query actually arrives in.
    final List<PostgresCatalogToken> tokens = PostgresCatalogToken.tokenize("SELECT nspname AS `TABLE_SCHEM`");

    final PostgresCatalogToken alias = tokens.get(tokens.size() - 1);
    assertThat(alias.type).isEqualTo(PostgresCatalogToken.Type.QUOTED_IDENTIFIER);
    assertThat(alias.text).isEqualTo("TABLE_SCHEM");
  }

  @Test
  void multiCharacterOperatorsAreOneTokenEach() {
    final List<PostgresCatalogToken> tokens = PostgresCatalogToken.tokenize("a <= b >= c <> d != e || f :: g ~* h !~ i !~* j");

    assertThat(texts(tokens)).contains("<=", ">=", "<>", "!=", "||", "::", "~*", "!~", "!~*");
  }

  @Test
  void placeholdersSurviveAsTheirOwnToken() {
    final List<PostgresCatalogToken> tokens = PostgresCatalogToken.tokenize("relname LIKE $12");

    final PostgresCatalogToken placeholder = tokens.get(tokens.size() - 1);
    assertThat(placeholder.type).isEqualTo(PostgresCatalogToken.Type.SYMBOL);
    assertThat(placeholder.text).isEqualTo("$12");
  }

  @Test
  void commentsAreSkipped() {
    assertThat(texts(PostgresCatalogToken.tokenize("SELECT oid -- the type's id\nFROM pg_type"))).containsExactly("SELECT",
        "oid", "FROM", "pg_type");
    assertThat(texts(PostgresCatalogToken.tokenize("SELECT /* a /* nested */ comment */ oid"))).containsExactly("SELECT",
        "oid");
  }

  @Test
  void exponentNotationIsOneNumber() {
    final List<PostgresCatalogToken> tokens = PostgresCatalogToken.tokenize("1.5e-3");

    assertThat(tokens).hasSize(1);
    assertThat(tokens.get(0).type).isEqualTo(PostgresCatalogToken.Type.NUMBER);
    assertThat(tokens.get(0).text).isEqualTo("1.5e-3");
  }

  @Test
  void anUnterminatedQuoteOrCommentIsRefusedRatherThanGuessedAt() {
    assertThat(PostgresCatalogToken.tokenize("SELECT 'unterminated")).isNull();
    assertThat(PostgresCatalogToken.tokenize("SELECT \"unterminated")).isNull();
    assertThat(PostgresCatalogToken.tokenize("SELECT `unterminated")).isNull();
    assertThat(PostgresCatalogToken.tokenize("SELECT E'unterminated")).isNull();
    assertThat(PostgresCatalogToken.tokenize("SELECT /* unterminated")).isNull();
  }

  @Test
  void aBoundParameterBecomesTheLiteralTheClientSent() {
    assertThat(PostgresCatalogToken.literal("Article").type).isEqualTo(PostgresCatalogToken.Type.STRING);
    assertThat(PostgresCatalogToken.literal("Article").text).isEqualTo("Article");
    assertThat(PostgresCatalogToken.literal(42).type).isEqualTo(PostgresCatalogToken.Type.NUMBER);
    assertThat(PostgresCatalogToken.literal(Boolean.TRUE).text).isEqualTo("TRUE");
    assertThat(PostgresCatalogToken.literal('x').type).isEqualTo(PostgresCatalogToken.Type.STRING);
    // A value with no literal form leaves the placeholder in place, and the predicate around it unread.
    assertThat(PostgresCatalogToken.literal(null)).isNull();
    assertThat(PostgresCatalogToken.literal(new byte[] { 1 })).isNull();
  }

  @Test
  void tokensDescribeThemselves() {
    // toString() is only ever read by a human debugging a declined query, but a broken one is worse than none.
    assertThat(PostgresCatalogToken.tokenize("oid").get(0).toString()).isEqualTo("IDENTIFIER(oid)");
  }

  private static List<String> texts(final List<PostgresCatalogToken> tokens) {
    return tokens.stream().map(token -> token.text).toList();
  }
}
