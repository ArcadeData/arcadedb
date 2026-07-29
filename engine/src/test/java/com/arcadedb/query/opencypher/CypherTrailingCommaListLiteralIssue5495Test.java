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
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Regression test for issue #5495: a trailing comma in a Cypher list literal ({@code [1, 2,]}) was
 * silently accepted and parsed as {@code [1, 2]}, so a malformed query produced by a generator, an
 * editing slip or a bad copy/paste looked like a valid one.
 * <p>
 * Reference semantics: the openCypher {@code ListLiteral} production requires every comma to be
 * followed by an expression, and both reference implementations agree. Verified against Neo4j
 * 2026.06.0 ({@code Invalid input ']', expected: an expression}) and Memgraph 3.x
 * ({@code no viable alternative at input '[1,2,]'}); the same holds for map literals
 * ({@code {a:1,}}) and for function arguments ({@code coalesce(1,2,)}), which ArcadeDB already
 * rejected.
 * <p>
 * This reverses the earlier issue #5180, which asked for the trailing comma to be accepted on the
 * premise that Neo4j and Memgraph allowed it. Re-tested against both engines: neither does, and
 * Neo4j's own {@code Cypher25Parser.g4} spells the rule
 * {@code LBRACKET (expression (COMMA expression)*)? RBRACKET}, with no optional trailing comma.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherTrailingCommaListLiteralIssue5495Test {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/cyphertrailingcomma5495").create();
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void trailingCommaInListLiteralIsRejected() {
    assertSyntaxError("RETURN [1, 2,] AS v");
  }

  @Test
  void trailingCommaAfterSingleElementIsRejected() {
    assertSyntaxError("RETURN [42,] AS v");
  }

  @Test
  void trailingCommaWithMixedElementsIsRejected() {
    assertSyntaxError("RETURN [1, 'a', true,] AS v");
  }

  @Test
  void trailingCommaInNestedListIsRejected() {
    assertSyntaxError("RETURN [[1, 2,], 3] AS v");
  }

  @Test
  void trailingCommaInListPassedToFunctionIsRejected() {
    assertSyntaxError("RETURN size([1, 2,]) AS v");
  }

  @Test
  void trailingCommaInListBoundByWithIsRejected() {
    assertSyntaxError("WITH [1, 2,] AS v RETURN v");
  }

  @Test
  void trailingCommaInListUsedAsPropertyValueIsRejected() {
    final Throwable thrown = catchThrowable(
        () -> database.transaction(() -> database.command("opencypher", "CREATE (n:Sample {tags: ['x', 'y',]}) RETURN n")));

    assertThat(thrown).isInstanceOf(CommandParsingException.class);
  }

  @Test
  void loneCommaInListIsRejected() {
    assertSyntaxError("RETURN [,] AS v");
  }

  @Test
  void doubleCommaInListIsRejected() {
    assertSyntaxError("RETURN [1,, 2] AS v");
  }

  /**
   * Map literals and function arguments never accepted a trailing comma; guard that they keep
   * rejecting it, so the list-literal alignment is not undone somewhere else.
   */
  @Test
  void trailingCommaInMapLiteralAndFunctionArgumentsIsRejected() {
    assertSyntaxError("RETURN {a: 1, b: 2,} AS v");
    assertSyntaxError("RETURN coalesce(1, 2,) AS v");
  }

  @Test
  void wellFormedListLiteralsStillWork() {
    assertThat(this.<List<Object>>queryValue("RETURN [1, 2] AS v")).containsExactly(1L, 2L);
    assertThat(this.<List<Object>>queryValue("RETURN [42] AS v")).containsExactly(42L);
    assertThat(this.<List<Object>>queryValue("RETURN [] AS v")).isEmpty();
    assertThat(this.<List<Object>>queryValue("RETURN [1, 'a', true] AS v")).containsExactly(1L, "a", true);
    assertThat(this.<List<Object>>queryValue("RETURN [[1, 2], [3]] AS v")).containsExactly(List.of(1L, 2L), List.of(3L));
    assertThat(this.<List<Object>>queryValue("WITH [1, 2] AS v RETURN v")).containsExactly(1L, 2L);
    assertThat(this.<Map<String, Object>>queryValue("RETURN {a: 1, b: 2} AS v")).containsEntry("a", 1L).containsEntry("b", 2L);
  }

  private void assertSyntaxError(final String query) {
    final Throwable thrown = catchThrowable(() -> {
      try (final ResultSet rs = database.query("opencypher", query)) {
        while (rs.hasNext())
          rs.next();
      }
    });

    assertThat(thrown).as("query <%s> must be rejected", query).isInstanceOf(CommandParsingException.class);
  }

  @SuppressWarnings("unchecked")
  private <T> T queryValue(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      assertThat(rs.hasNext()).as("query <%s> must return a row", query).isTrue();
      final Result r = rs.next();
      return (T) r.getProperty("v");
    }
  }
}
