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

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #11692: the leading argument of {@code left()}, {@code right()} and {@code substring()} is
 * the STRING being sliced, yet any other value reached {@code Object.toString()} and was sliced as text, so
 * {@code left([1, 2], 1)} answered {@code "["}, {@code substring(a.v, 0)} on an integer property answered {@code "1"} and
 * {@code left(a.b, 2)} on a boolean property answered {@code "tr"}. Cypher declares {@code f(original :: STRING, ...)},
 * and Neo4j, Memgraph and FalkorDB all raise a type error: so must ArcadeDB, as the client error (HTTP 400) the rest of
 * the STRING family already raises (#5798, #6608) and the numeric positions of the same three functions raise (#6609).
 */
class CypherStringFunctionSlicedArgumentIssue11692Test extends TestHelper {

  @BeforeEach
  void createNode() {
    database.transaction(() -> database.command("opencypher", "CREATE (a:Issue11692 {id: 1, v: 1, s: 'abc', b: true, l: [1, 2]})"));
  }

  // ===================== list literals (the reproducer) =====================

  @Test
  void leftOfListLiteralIsAClientTypeError() {
    assertTypeError("RETURN left([1, 2], 1) AS r", "left()", "LIST");
  }

  @Test
  void rightOfListLiteralIsAClientTypeError() {
    assertTypeError("RETURN right([1, 2], 1) AS r", "right()", "LIST");
  }

  @Test
  void substringOfListLiteralIsAClientTypeError() {
    assertTypeError("RETURN substring([1, 2], 0) AS r", "substring()", "LIST");
  }

  // ===================== property values, known only once the query runs =====================

  @Test
  void substringOfIntegerPropertyIsAClientTypeError() {
    assertTypeError("MATCH (a:Issue11692 {id: 1}) RETURN substring(a.v, 0) AS r", "substring()", "INTEGER");
  }

  @Test
  void leftOfBooleanPropertyIsAClientTypeError() {
    assertTypeError("MATCH (a:Issue11692 {id: 1}) RETURN left(a.b, 2) AS r", "left()", "BOOLEAN");
  }

  @Test
  void leftAndRightOfListPropertyAreClientTypeErrors() {
    assertTypeError("MATCH (a:Issue11692 {id: 1}) RETURN left(a.l, 1) AS r", "left()", "LIST");
    assertTypeError("MATCH (a:Issue11692 {id: 1}) RETURN right(a.l, 1) AS r", "right()", "LIST");
  }

  @Test
  void substringWithLengthOfIntegerPropertyIsAClientTypeError() {
    assertTypeError("MATCH (a:Issue11692 {id: 1}) RETURN substring(a.v, 0, 1) AS r", "substring()", "INTEGER");
  }

  @Test
  void theTypeIsCheckedBeforeANullLengthDecidesTheAnswer() {
    // Same order as replace() (#5798): an out-of-domain argument is reported whichever other position is null.
    assertTypeError("MATCH (a:Issue11692 {id: 1}) RETURN left(a.l, null) AS r", "left()", "LIST");
    assertTypeError("MATCH (a:Issue11692 {id: 1}) RETURN substring(a.v, null) AS r", "substring()", "INTEGER");
  }

  // ===================== literal non-string controls (#6608 family) =====================

  @Test
  void nonStringLiteralsAreClientTypeErrors() {
    assertTypeError("RETURN left(5, 1) AS r", "left()", "INTEGER");
    assertTypeError("RETURN right(true, 1) AS r", "right()", "BOOLEAN");
    assertTypeError("RETURN substring(1.5, 0) AS r", "substring()", "FLOAT");
    assertTypeError("RETURN left({a: 1}, 1) AS r", "left()", "MAP");
  }

  @Test
  void aNonStringLiteralFailsEvenWhenNoRowMatches() {
    // A literal's type is readable in the query text, so it is rejected before the query runs, as Neo4j does and as the
    // numeric positions of the same functions already are (#6609).
    assertTypeError("MATCH (a:Issue11692) WHERE a.id = 42 RETURN left([1, 2], 1) AS r", "left()", "LIST");
    assertTypeError("MATCH (a:Issue11692) WHERE a.id = 42 RETURN right(5, 1) AS r", "right()", "INTEGER");
    assertTypeError("MATCH (a:Issue11692) WHERE a.id = 42 RETURN substring(true, 0) AS r", "substring()", "BOOLEAN");
  }

  // ===================== arguments that are still accepted =====================

  @Test
  void stringArgumentsStillWork() {
    assertThat(single("MATCH (a:Issue11692 {id: 1}) RETURN left(a.s, 2) AS r")).isEqualTo("ab");
    assertThat(single("MATCH (a:Issue11692 {id: 1}) RETURN right(a.s, 2) AS r")).isEqualTo("bc");
    assertThat(single("MATCH (a:Issue11692 {id: 1}) RETURN substring(a.s, 1) AS r")).isEqualTo("bc");
    assertThat(single("MATCH (a:Issue11692 {id: 1}) RETURN substring(a.s, 0, 1) AS r")).isEqualTo("a");
    assertThat(single("RETURN left('hello', 3) AS r")).isEqualTo("hel");
    assertThat(single("MATCH (a:Issue11692 {id: 1}) RETURN left(toString(a.v), 1) AS r")).isEqualTo("1");
  }

  @Test
  void nullPropagationIsNotATypeError() {
    assertThat(single("RETURN left(null, 1) AS r")).isNull();
    assertThat(single("RETURN right(null, 1) AS r")).isNull();
    assertThat(single("RETURN substring(null, 0) AS r")).isNull();
    assertThat(single("MATCH (a:Issue11692 {id: 1}) RETURN left(a.missing, 1) AS r")).isNull();
    assertThat(single("MATCH (a:Issue11692 {id: 1}) RETURN right(a.missing, 1) AS r")).isNull();
    assertThat(single("MATCH (a:Issue11692 {id: 1}) RETURN substring(a.missing, 0, 1) AS r")).isNull();
    assertThat(single("MATCH (a:Issue11692 {id: 1}) RETURN left(a.s, null) AS r")).isNull();
  }

  private void assertTypeError(final String query, final String function, final String actualType) {
    assertThatThrownBy(() -> consume(query))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("Type mismatch")
        .hasMessageContaining(function)
        .hasMessageContaining("a STRING")
        .hasMessageContaining(actualType);
  }

  private Object single(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat(rs.hasNext()).isFalse();
      return row.getProperty("r");
    }
  }

  private void consume(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext())
        rs.next();
    }
  }
}
