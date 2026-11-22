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
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12975: a list literal used as a predicate was true in a bare WHERE and refused under AND/OR/NOT, so the same value
 * gave two answers. A list literal is refused in every predicate position, as Neo4j refuses the coercion of a list to a
 * boolean ({@code Coercion of list to boolean is not allowed}), while a list read from a property keeps the coercion by
 * emptiness, which Neo4j accepts too.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherListLiteralPredicateIssue12975Test extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> database.command("opencypher",
        "CREATE (:Node {id: 1, l: [1, 2]}), (:Node {id: 2, l: []}), (:Node {id: 3, l: [3]})"));
  }

  /** The condition of a WHERE, of a WITH and of a FILTER is a predicate position. */
  @Test
  void aListLiteralIsRefusedInAWhere() {
    assertRefused("MATCH (n:Node) WHERE [1, 2] RETURN n.id");
    assertRefused("MATCH (n:Node) WHERE [] RETURN n.id");
    assertRefused("MATCH (n:Node) WITH n WHERE [1, 2] RETURN n.id");
    assertRefused("MATCH (n:Node) FILTER WHERE [1, 2] RETURN n.id");
    // the same literal under a logical operator was already refused
    assertRefused("MATCH (n:Node) WHERE [1, 2] AND true RETURN n.id");
    assertRefused("MATCH (n:Node) WHERE NOT [1, 2] RETURN n.id");
    assertRefused("MATCH (n:Node) WHERE true OR [1, 2] RETURN n.id");
  }

  /** The inline WHERE of a node or of a relationship pattern is a predicate position too. */
  @Test
  void aListLiteralIsRefusedInAnInlinePatternWhere() {
    assertRefused("MATCH (n:Node WHERE [1, 2]) RETURN n.id");
    assertRefused("MATCH (n:Node)-[r:REL WHERE [1, 2]]->(m:Node) RETURN n.id");
  }

  /** The WHEN of a CASE is a predicate position. */
  @Test
  void aListLiteralIsRefusedInACaseWhen() {
    assertRefused("MATCH (n:Node) RETURN CASE WHEN [1, 2] THEN 1 ELSE 0 END AS r");
    // the extended CASE compares a value with the WHEN, it does not coerce it
    final List<Result> extended = rows(
        "MATCH (n:Node) RETURN n.id AS id, CASE n.l WHEN [1, 2] THEN 1 ELSE 0 END AS r ORDER BY id");
    assertThat(extended).hasSize(3);
    assertThat(extended.getFirst().<Number>getProperty("r").longValue()).isEqualTo(1L);
    assertThat(extended.get(1).<Number>getProperty("r").longValue()).isEqualTo(0L);
    assertThat(extended.get(2).<Number>getProperty("r").longValue()).isEqualTo(0L);
  }

  /** The WHERE of a list predicate and the filter of a comprehension are predicate positions. */
  @Test
  void aListLiteralIsRefusedInAComprehension() {
    assertRefused("MATCH (n:Node) WHERE none(x IN [1] WHERE [1, 2]) RETURN n.id");
    assertRefused("MATCH (n:Node) RETURN [x IN [1] WHERE [1, 2] | x] AS r");
    assertRefused("MATCH (n:Node) RETURN [(n)-->(m) WHERE [1, 2] | m] AS r");
  }

  /**
   * A list read from a property is a different case: its type is not known when the statement is parsed, so the value is
   * taken by whether it is empty - the coercion Neo4j applies too.
   */
  @Test
  void aListReadFromAPropertyIsTrueWhenNotEmpty() {
    assertThat(ids("MATCH (n:Node) WHERE n.l RETURN n.id AS id ORDER BY id")).containsExactly(1L, 3L);
    assertThat(ids("MATCH (n:Node) WHERE NOT n.l RETURN n.id AS id ORDER BY id")).containsExactly(2L);
    assertThat(ids("MATCH (n:Node) WHERE n.l OR false RETURN n.id AS id ORDER BY id")).containsExactly(1L, 3L);
  }

  /** A list in a value position is a value, not a predicate. */
  @Test
  void aListInAValuePositionIsNotRefused() {
    assertThat(single("RETURN [1, 2] AS r").<Object>getProperty("r")).isEqualTo(List.of(1L, 2L));
    assertThat(ids("MATCH (n:Node) WHERE [1, 2] = [1, 2] RETURN n.id AS id ORDER BY id")).containsExactly(1L, 2L, 3L);
    assertThat(ids("MATCH (n:Node) WHERE size([1, 2]) = 2 RETURN n.id AS id ORDER BY id")).containsExactly(1L, 2L, 3L);
  }

  private void assertRefused(final String query) {
    assertThatThrownBy(() -> rows(query)).as(query)
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("Expected Boolean");
  }

  private List<Long> ids(final String query) {
    final List<Long> out = new ArrayList<>();
    for (final Result row : rows(query))
      out.add(row.<Number>getProperty("id").longValue());
    return out;
  }

  private Result single(final String query) {
    final List<Result> rows = rows(query);
    assertThat(rows).hasSize(1);
    return rows.getFirst();
  }

  private List<Result> rows(final String query) {
    final List<Result> out = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext())
        out.add(rs.next());
    }
    return out;
  }
}
