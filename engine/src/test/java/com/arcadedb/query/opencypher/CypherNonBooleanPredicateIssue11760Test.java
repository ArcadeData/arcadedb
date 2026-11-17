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
import com.arcadedb.exception.TypeMismatchException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11760: a non-Boolean value in a predicate position was true under OR (any non-null value was truthy) and false
 * under WHERE (only {@code true} passed), so the same integer answered both ways. A predicate now takes the value the way
 * Neo4j's runtime does, checked against Neo4j 2026.09: a BOOLEAN is itself, null is unknown, a LIST is true when it is not
 * empty, and anything else (INTEGER, FLOAT, STRING, MAP, NODE, ...) is a type error, wherever the predicate sits.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherNonBooleanPredicateIssue11760Test extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> database.command("opencypher",
        "CREATE (:Node {id: 1, v: 1, b: true, l: [1, 2], s: 'a'}), (:Node {id: 2, v: 2, b: false, l: [], s: ''}), "
            + "(:Node {id: 3, v: 3, b: true, l: [3], s: 'c'})"));
  }

  @Test
  void anIntegerUnderOrIsATypeError() {
    assertTypeError("UNWIND [3] AS x RETURN (x OR false) AS r", "INTEGER");
    // a literal operand is refused before anything runs, as Neo4j's semantic analysis does
    assertRefusedAtParse("RETURN 1 OR false AS r");
    assertRefusedAtParse("RETURN 0 OR false AS r");
    assertTypeError("UNWIND ['a'] AS x RETURN x OR false AS r", "STRING");
  }

  @Test
  void anIntegerInWhereIsATypeError() {
    assertTypeError("MATCH (n:Node) WHERE n.v RETURN n.id ORDER BY n.id", "INTEGER");
    assertTypeError("MATCH (n:Node) WHERE n.l[0] RETURN n.id ORDER BY n.id", "INTEGER");
    assertTypeError("MATCH (n:Node) WHERE n.v + 1 RETURN n.id ORDER BY n.id", "INTEGER");
    assertTypeError("MATCH (n:Node) WHERE 1 RETURN n.id", "INTEGER");
    assertTypeError("MATCH (n:Node) WHERE 0 RETURN n.id", "INTEGER");
    assertTypeError("MATCH (n:Node) WHERE n.s RETURN n.id", "STRING");
    assertTypeError("MATCH (n:Node) WHERE n RETURN n.id", "NODE");
    assertTypeError("UNWIND [1.5] AS x WITH x WHERE x RETURN x", "FLOAT");
    assertTypeError("UNWIND [{a: 1}] AS x WITH x WHERE x RETURN x", "MAP");
  }

  /** Short-circuiting must not decide the type: Neo4j refuses {@code true OR 1} and {@code false AND 1} as well. */
  @Test
  void everyLogicalOperatorChecksItsOperands() {
    assertTypeError("UNWIND [1] AS x RETURN x AND false AS r", "INTEGER");
    assertTypeError("UNWIND [1] AS x RETURN NOT x AS r", "INTEGER");
    assertTypeError("UNWIND [1] AS x RETURN x XOR true AS r", "INTEGER");
    assertTypeError("MATCH (n:Node) WHERE n.v OR false RETURN n.id", "INTEGER");
    assertTypeError("MATCH (n:Node) WHERE NOT n.v RETURN n.id", "INTEGER");
    assertTypeError("MATCH (n:Node) WHERE n.b AND n.v RETURN n.id", "INTEGER");
  }

  @Test
  void caseWhenAndListPredicatesCheckTheirCondition() {
    assertTypeError("UNWIND [1] AS x RETURN CASE WHEN x THEN 'y' ELSE 'n' END AS r", "INTEGER");
    assertTypeError("UNWIND [1] AS x RETURN [y IN [1, 2] WHERE x] AS r", "INTEGER");
    assertTypeError("RETURN any(y IN [1, 2] WHERE y) AS r", "INTEGER");
  }

  @Test
  void aBooleanPredicateStillWorks() {
    assertThat(ids("MATCH (n:Node) WHERE n.b RETURN n.id AS id ORDER BY id")).containsExactly(1L, 3L);
    assertThat(ids("MATCH (n:Node) WHERE NOT n.b RETURN n.id AS id ORDER BY id")).containsExactly(2L);
    assertThat(ids("MATCH (n:Node) WHERE n.b OR false RETURN n.id AS id ORDER BY id")).containsExactly(1L, 3L);
    assertThat(single("UNWIND [true] AS x RETURN CASE WHEN x THEN 'y' ELSE 'n' END AS r").<String>getProperty("r")).isEqualTo("y");
  }

  /** A null predicate is unknown: no row in WHERE, null under OR with false, the ELSE branch of a CASE. */
  @Test
  void aNullPredicateIsUnknown() {
    assertThat(ids("MATCH (n:Node) WHERE n.missing RETURN n.id AS id")).isEmpty();
    assertThat(single("UNWIND [null] AS x RETURN (x OR false) AS r").<Object>getProperty("r")).isNull();
    assertThat(single("UNWIND [null] AS x RETURN (x OR true) AS r").<Boolean>getProperty("r")).isTrue();
    assertThat(single("UNWIND [null] AS x RETURN CASE WHEN x THEN 'y' ELSE 'n' END AS r").<String>getProperty("r")).isEqualTo("n");
  }

  /** Neo4j's runtime takes a stored list as a predicate by whether it is empty, in WHERE and under NOT and OR alike. */
  @Test
  void aListPredicateIsTrueWhenNotEmpty() {
    assertThat(ids("MATCH (n:Node) WHERE n.l RETURN n.id AS id ORDER BY id")).containsExactly(1L, 3L);
    assertThat(ids("MATCH (n:Node) WHERE NOT n.l RETURN n.id AS id ORDER BY id")).containsExactly(2L);
    assertThat(ids("MATCH (n:Node) WHERE n.l OR false RETURN n.id AS id ORDER BY id")).containsExactly(1L, 3L);
  }

  /**
   * The parser took {@code COUNT { ... } > 0} written as an operand of AND/OR for the bare COUNT, dropping the comparison:
   * the count was then a non-Boolean, true under the old truthiness whatever it was, so a count of zero passed.
   */
  @Test
  void aCountComparisonUnderAndOrKeepsItsComparison() {
    assertThat(single("RETURN COUNT { MATCH (m:Missing) } > 0 AND true AS r").<Boolean>getProperty("r")).isFalse();
    assertThat(single("RETURN true AND COUNT { MATCH (m:Missing) } > 0 AS r").<Boolean>getProperty("r")).isFalse();
    assertThat(single("RETURN COUNT { MATCH (m:Missing) } = 0 OR false AS r").<Boolean>getProperty("r")).isTrue();
    assertThat(single("RETURN null OR COUNT { MATCH (n:Node) } > 0 AS r").<Boolean>getProperty("r")).isTrue();
    assertThat(ids("MATCH (n:Node) WHERE n.b AND COUNT { MATCH (m:Missing) } > 0 RETURN n.id AS id")).isEmpty();
  }

  private void assertTypeError(final String query, final String type) {
    assertThatThrownBy(() -> rows(query)).as(query)
        .isInstanceOf(TypeMismatchException.class)
        .hasMessageContaining("expected Boolean")
        .hasMessageContaining(type);
  }

  private void assertRefusedAtParse(final String query) {
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
