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
package com.arcadedb.query.sql.method;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #7896: the parenthesis half of the #7774 fix made
 * {@code BaseExpression#rendersParentheses()} answer yes for EVERY parenthesised expression carrying a modifier
 * chain. That method is on the projection-naming path - an unaliased column is named after the text its expression
 * renders to - so {@code SELECT (name).asString() FROM T} silently became the column {@code (name).asString()} where
 * it had always been {@code name.asString()}, and a client reading the result by its historical key got null.
 * <p>
 * The parentheses are only load-bearing around an atom the grammar gives no {@code modifier*} tail of its own: a
 * NUMBER, {@code NULL}, a boolean or RID literal. For an identifier, a string literal, a function call, a map or
 * array literal or a CASE block, {@code X.method()} parses exactly as {@code (X).method()} means.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7896UnaliasedParenthesizedProjectionNameTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType("T7896");
    database.transaction(() -> database.newDocument("T7896").set("name", "bob").set("tags", java.util.List.of("a", "b")).save());
  }

  @Test
  void redundantParenthesesAroundAnIdentifierDoNotRenameTheColumn() {
    assertThat(columnOf("SELECT (name).asString() FROM T7896")).isEqualTo("name.asString()");
    assertThat(valueOf("SELECT (name).asString() FROM T7896", "name.asString()")).isEqualTo("bob");
  }

  @Test
  void theParenthesisedAndTheBareFormAgreeOnTheName() {
    assertThat(columnOf("SELECT (name).asString() FROM T7896")).isEqualTo(columnOf("SELECT name.asString() FROM T7896"));
    assertThat(columnOf("SELECT (tags)[0] FROM T7896")).isEqualTo(columnOf("SELECT tags[0] FROM T7896"));
  }

  @Test
  void redundantParenthesesAroundAStringLiteralOrAFunctionCallDoNotRenameTheColumn() {
    assertThat(columnOf("SELECT ('a').append('b') FROM T7896")).isEqualTo("'a'.append('b')");
    assertThat(columnOf("SELECT (uuid()).asString() FROM T7896")).isEqualTo("uuid().asString()");
  }

  @Test
  void redundantParenthesesAroundABlockLiteralDoNotRenameTheColumn() {
    assertThat(columnOf("SELECT ([1,2,3]).size() FROM T7896")).doesNotStartWith("(");
    assertThat(valueOf("SELECT ([1,2,3]).size() FROM T7896", columnOf("SELECT ([1,2,3]).size() FROM T7896"))).isEqualTo(3);
  }

  @Test
  void aParenthesisedAtomWithNoModifierTailOfItsOwnKeepsItsParentheses() {
    // The cases issue #7774 is about: without the parentheses the rendered text is no longer parseable, so the
    // column name is the lesser of the two evils there.
    assertThat(columnOf("SELECT (42).asString() FROM T7896")).isEqualTo("(42).asString()");
    assertThat(columnOf("SELECT (null).ifNull('d') FROM T7896")).isEqualTo("(NULL).ifNull('d')");
    assertThat(valueOf("SELECT (42).asString() FROM T7896", "(42).asString()")).isEqualTo("42");
    assertThat(valueOf("SELECT (null).ifNull('d') FROM T7896", "(NULL).ifNull('d')")).isEqualTo("d");
  }

  @Test
  void parenthesesAroundACompoundArithmeticExpressionStillSurvive() {
    // Unchanged by this fix and by #7774 before it: dropping these re-associates the operators (issue #6359).
    assertThat(valueOf("SELECT (1 + 2) * 3 AS r FROM T7896", "r")).isEqualTo(9);
    assertThat(columnOf("SELECT (1 + 2) * 3 FROM T7896")).contains("(1 + 2)");
  }

  @Test
  void parenthesesWithNoModifierAtAllAreStillDropped() {
    assertThat(columnOf("SELECT (name) FROM T7896")).isEqualTo("name");
  }

  /** The name of the single (unaliased) column the query projects. */
  private String columnOf(final String query) {
    try (final ResultSet rs = database.query("sql", query, Map.of())) {
      assertThat(rs.hasNext()).isTrue();
      final Result result = rs.next();
      assertThat(result.getPropertyNames()).hasSize(1);
      return result.getPropertyNames().iterator().next();
    }
  }

  private Object valueOf(final String query, final String columnName) {
    try (final ResultSet rs = database.query("sql", query, Map.of())) {
      assertThat(rs.hasNext()).isTrue();
      return rs.next().getProperty(columnName);
    }
  }
}
