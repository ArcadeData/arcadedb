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
package com.arcadedb.query.opencypher;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression for issue #5383: {@code true OR null} returns {@code null} when the null
 * operand is derived from a function call.
 */
class Issue5383BooleanNullFunctionTest {
  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/testIssue5383");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void literalNullOrWorks() {
    assertThat(scalar("RETURN (null IS NULL OR null = null) AS v")).isEqualTo(true);
  }

  @Test
  void toStringNullOr() {
    assertThat(scalar("RETURN (toString(null) IS NULL OR toString(null) = '') AS v")).isEqualTo(true);
  }

  @Test
  void lTrimNullOr() {
    assertThat(scalar("RETURN (lTrim(null) IS NULL OR lTrim(null) = '') AS v")).isEqualTo(true);
  }

  @Test
  void substringNullOr() {
    assertThat(scalar("RETURN (substring(null, 0, 3) IS NULL OR substring(null, 0, 3) = '') AS v")).isEqualTo(true);
  }

  @Test
  void parenthesizedFunctionIsNullYieldsConcreteBoolean() {
    // The parenthesized wrapper must not collapse the whole expression to just the inner
    // function call: IS NULL over a non-null function result is a concrete false.
    assertThat(scalar("RETURN (toString('x') IS NULL) AS v")).isEqualTo(false);
    assertThat(scalar("RETURN (toString(null) IS NULL) AS v")).isEqualTo(true);
  }

  @Test
  void parenthesizedTrueOrFunctionIsTrue() {
    // true OR <anything> must short-circuit to true even when the right operand contains a
    // null-returning function call inside the parentheses.
    assertThat(scalar("RETURN (true OR toString(null) = '') AS v")).isEqualTo(true);
  }

  @Test
  void nestedParenthesesFunctionOr() {
    assertThat(scalar("RETURN ((toString(null) IS NULL OR toString(null) = '')) AS v")).isEqualTo(true);
  }

  @Test
  void withProjectionParenthesizedFunctionOr() {
    assertThat(scalar(
        "WITH (toString(null) IS NULL OR toString(null) = '') AS v RETURN v")).isEqualTo(true);
  }

  private Object scalar(final String cypher) {
    try (final ResultSet rs = database.query("opencypher", cypher)) {
      assertThat(rs.hasNext()).as("query returned no rows: %s", cypher).isTrue();
      final Result r = rs.next();
      return r.getProperty("v");
    }
  }
}
