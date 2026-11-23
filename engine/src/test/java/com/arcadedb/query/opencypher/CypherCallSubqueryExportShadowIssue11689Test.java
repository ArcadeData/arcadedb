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
 * Regression tests for issue #11689: a CALL subquery RETURN must not export a name already bound in the outer scope.
 */
class CypherCallSubqueryExportShadowIssue11689Test extends TestHelper {

  @Test
  void returningImportedVariableUnderSameNameIsRejected() {
    assertThatThrownBy(() -> database.query("opencypher", "UNWIND [1, 2] AS i CALL { WITH i RETURN i } RETURN i").close())
        .isInstanceOf(CommandParsingException.class).hasMessageContaining("already declared in outer scope");
  }

  @Test
  void aliasingExpressionToImportedNameIsRejected() {
    assertThatThrownBy(() -> database.query("opencypher", "UNWIND [1, 2] AS i CALL { WITH i RETURN i + 1 AS i } RETURN i").close())
        .isInstanceOf(CommandParsingException.class).hasMessageContaining("already declared in outer scope");
  }

  @Test
  void exportingNonImportedOuterNameIsRejected() {
    assertThatThrownBy(() -> database.query("opencypher", "UNWIND [1, 2] AS i CALL { RETURN 5 AS i } RETURN i").close())
        .isInstanceOf(CommandParsingException.class).hasMessageContaining("already declared in outer scope");
  }

  @Test
  void nestedSubqueryClashIsRejected() {
    assertThatThrownBy(() -> database.query("opencypher",
        "UNWIND [1] AS i CALL { WITH i UNWIND [2] AS k CALL { WITH k RETURN k } RETURN i AS j } RETURN j").close())
        .isInstanceOf(CommandParsingException.class).hasMessageContaining("'k' already declared in outer scope");
  }

  /** The WHERE of a WITH was never scope-walked, so a CALL subquery inside an EXISTS there escaped the export check. */
  @Test
  void clashInsideAWithWhereSubqueryIsRejected() {
    for (final String query : List.of(//
        "UNWIND [1, 2] AS i WITH i WHERE EXISTS { CALL { RETURN 5 AS i } RETURN 1 } RETURN i",//
        "UNWIND [1, 2] AS i WITH * WHERE EXISTS { CALL { RETURN 5 AS i } RETURN 1 } RETURN i",//
        "UNWIND [1, 2] AS i WITH i AS j WHERE COUNT { CALL { RETURN 5 AS j } RETURN 1 } > 0 RETURN j"))
      assertThatThrownBy(() -> database.query("opencypher", query).close()).as(query)
          .isInstanceOf(CommandParsingException.class).hasMessageContaining("already declared in outer scope");
  }

  /**
   * A WITH WHERE sees the projected names, and the incoming ones unless the WITH aggregates or is DISTINCT, where only what
   * repeats a grouping key is read, from its column (issue #12245): a name outside that scope filtered every row out instead
   * of being refused.
   */
  @Test
  void withWhereReadsTheProjectedAndIncomingNames() {
    for (final String query : List.of(//
        "UNWIND [1, 2] AS i WITH i WHERE zz > 1 RETURN i",//
        // A collapsing WITH hides an incoming variable no grouping key projects as itself, as in Neo4j
        "UNWIND [1, 2] AS i WITH i + 0 AS j, count(*) AS c WHERE i > 1 RETURN j"))
      assertThatThrownBy(() -> database.query("opencypher", query).close()).as(query)
          .isInstanceOf(CommandParsingException.class).hasMessageContaining("not defined");
    for (final String query : List.of(//
        // A grouping key repeated in the WHERE reads its column, as in Neo4j 5.26 (issue #12245)
        "UNWIND [1, 2] AS i WITH i AS j, count(*) AS c WHERE i > 1 RETURN j",//
        "UNWIND [1, 2] AS i WITH DISTINCT i AS j WHERE i > 1 RETURN j",//
        "UNWIND [1, 2] AS i WITH i AS j, count(*) AS c WHERE i + 0 > 1 RETURN j",//
        "UNWIND [1, 2] AS i WITH i AS j WHERE i > 1 RETURN j",//
        "UNWIND [1, 2] AS i WITH i AS j WHERE j > 1 RETURN j",//
        "UNWIND [1, 2] AS i WITH DISTINCT i AS j WHERE j > 1 RETURN j",//
        "UNWIND [1, 2] AS i WITH *, i * 10 AS t WHERE i > 1 AND t > 10 RETURN i AS j"))
      try (final ResultSet rs = database.query("opencypher", query)) {
        assertThat(rs.stream().map(r -> ((Number) r.getProperty("j")).longValue()).toList()).as(query).containsExactly(2L);
      }
  }

  /** A COLLECT body in a WITH WHERE is walked like EXISTS and COUNT, so its CALL export clash is refused too. */
  @Test
  void clashInsideAWithWhereCollectSubqueryIsRejected() {
    assertThatThrownBy(() -> database.query("opencypher",
        "UNWIND [1, 2] AS i WITH i WHERE size(COLLECT { CALL { RETURN 5 AS i } RETURN i }) > 0 RETURN i").close())
        .isInstanceOf(CommandParsingException.class).hasMessageContaining("already declared in outer scope");
  }

  /**
   * The WITH WHERE scope check must not refuse the names a pattern predicate, a subquery body or a list predicate binds
   * locally, nor a property projected under an alias.
   */
  @Test
  void withWhereAcceptsLocallyBoundAndAliasedNames() {
    database.transaction(() -> database.command("opencypher",
        "CREATE (:P11930 {name: 'a'})-[:R11930]->(:P11930 {name: 'b'}), (:P11930 {name: 'c'})"));
    for (final String query : List.of(//
        "MATCH (n:P11930) WITH n.name AS x WHERE x = 'a' RETURN x AS name",//
        "MATCH (n:P11930) WITH n WHERE (n)-[:R11930]->() RETURN n.name AS name",//
        "MATCH (n:P11930) WITH n WHERE (n)-[:R11930]->(:P11930 {name: 'b'}) RETURN n.name AS name",//
        "MATCH (n:P11930) WITH n WHERE EXISTS { MATCH (n)-[:R11930]->(m) WHERE m.name = 'b' } RETURN n.name AS name",//
        "MATCH (n:P11930) WITH n WHERE COUNT { (n)-[:R11930]->(m) WHERE m.name = 'b' } > 0 RETURN n.name AS name",//
        "MATCH (n:P11930) WITH n WHERE 'b' IN COLLECT { MATCH (n)-[:R11930]->(m) RETURN m.name } RETURN n.name AS name",//
        "MATCH (n:P11930) WITH n WHERE EXISTS { CALL { RETURN 5 AS k } WITH n, k MATCH (n)-[:R11930]->() RETURN k } RETURN n.name AS name",//
        "MATCH (n:P11930) WITH n WHERE any(x IN [n.name] WHERE x = 'a') RETURN n.name AS name"))
      try (final ResultSet rs = database.query("opencypher", query)) {
        assertThat(rs.stream().map(r -> (String) r.getProperty("name")).toList()).as(query).containsExactly("a");
      }
  }

  /**
   * Issue #12387: a {@code RETURN *} of the body exports every variable in scope at it, and one the body declared afresh under
   * the name of an outer variable it did not import is the same clash as an explicit item. It used to pass, and the outer
   * variable was then overwritten on the rows the body answered and kept on the rows it did not.
   */
  @Test
  void returnStarOfANonImportedOuterNameIsRejected() {
    database.transaction(() -> {
      database.getSchema().createVertexType("A12387");
      database.getSchema().createVertexType("B12387");
      database.getSchema().createEdgeType("X12387");
    });
    for (final String query : List.of(//
        "MATCH (z:A12387), (b:B12387) OPTIONAL CALL (z) { MATCH (z)-[r:X12387]->(b) RETURN * } RETURN *",//
        "MATCH (z:A12387), (b:B12387) CALL (z) { MATCH (z)-[r:X12387]->(b) RETURN * } RETURN *",//
        "MATCH (z:A12387) CALL { MATCH (z:A12387) RETURN * } RETURN *",//
        "MATCH (z:A12387), (b:B12387) CALL { WITH z MATCH (z)-[r:X12387]->(b) RETURN * } RETURN *",//
        "UNWIND [1, 2] AS i CALL { UNWIND [5] AS i RETURN * } RETURN i",//
        // every branch of a UNION body must have imported the name: the second one declares its own i
        "UNWIND [1, 2] AS i CALL { WITH i RETURN * UNION UNWIND [5] AS i RETURN * } RETURN i",//
        // nested: the inner subquery clashes with the variable the outer body bound
        "UNWIND [1] AS k CALL { UNWIND [2] AS j CALL { UNWIND [3] AS j RETURN * } RETURN j AS m } RETURN m",//
        // a leading WITH that aliases a value to the outer name is no import: the body's z is a value of its own
        "MATCH (z:A12387) CALL { WITH 1 AS z RETURN * } RETURN z",//
        "MATCH (z:A12387), (b:B12387) CALL { WITH b AS z RETURN * } RETURN z",//
        // imported, then re-bound by the body before the RETURN *
        "MATCH (z:A12387) CALL (z) { WITH 1 AS z RETURN * } RETURN z",//
        "MATCH (z:A12387) CALL { WITH z WITH 1 AS z RETURN * } RETURN z",//
        "MATCH (z:A12387) OPTIONAL CALL (*) { WITH 1 AS z RETURN * } RETURN z",//
        "MATCH (z:A12387) CALL (z) { WITH 1 AS x MATCH (z:A12387) RETURN * } RETURN z"))
      assertThatThrownBy(() -> database.query("opencypher", query).close(), query)
          .isInstanceOf(CommandParsingException.class).hasMessageContaining("already declared in outer scope");
  }

  /** The control of #12387: an imported variable handed back by {@code RETURN *} is still accepted, whatever imported it. */
  @Test
  void returnStarOfAnImportedOuterNameIsAccepted() {
    database.transaction(() -> {
      database.getSchema().createVertexType("A12387");
      database.getSchema().createVertexType("B12387");
      database.getSchema().createEdgeType("X12387");
      database.command("opencypher", "CREATE (:A12387 {i: 1})-[:X12387]->(:B12387 {j: 2})");
    });
    for (final String query : List.of(//
        "MATCH (z:A12387) CALL (z) { MATCH (z)-[r:X12387]->(b) RETURN * } RETURN z.i AS i, b.j AS j",//
        "MATCH (z:A12387) CALL (*) { MATCH (z)-[r:X12387]->(b) RETURN * } RETURN z.i AS i, b.j AS j",//
        "MATCH (z:A12387) CALL { WITH z MATCH (z)-[r:X12387]->(b) RETURN * } RETURN z.i AS i, b.j AS j",//
        "MATCH (z:A12387) CALL { WITH * MATCH (z)-[r:X12387]->(b) RETURN * } RETURN z.i AS i, b.j AS j",//
        "MATCH (z:A12387) OPTIONAL CALL (z) { MATCH (z)-[r:X12387]->(b) RETURN * } RETURN z.i AS i, b.j AS j",//
        // forwarded as itself through later projections, it is still the outer variable
        "MATCH (z:A12387) CALL { WITH z AS z WITH z, 1 AS x MATCH (z)-[r:X12387]->(b) RETURN * } RETURN z.i AS i, b.j AS j"))
      try (final ResultSet rs = database.query("opencypher", query)) {
        final Result row = rs.next();
        assertThat(((Number) row.getProperty("i")).longValue()).as(query).isEqualTo(1L);
        assertThat(((Number) row.getProperty("j")).longValue()).as(query).isEqualTo(2L);
        assertThat(rs.hasNext()).as(query).isFalse();
      }
  }

  @Test
  void distinctExportNameWorks() {
    final List<Long> js = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", "UNWIND [1, 2] AS i CALL { WITH i RETURN i AS j } RETURN i, j ORDER BY i")) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        assertThat(((Number) r.getProperty("i")).longValue()).isEqualTo(((Number) r.getProperty("j")).longValue());
        js.add(((Number) r.getProperty("j")).longValue());
      }
    }
    assertThat(js).containsExactly(1L, 2L);
  }

  @Test
  void duplicateColumnInsideReturnKeepsColumnNameConflict() {
    assertThatThrownBy(() -> database.query("opencypher", "CALL { RETURN 1 AS a, 2 AS a } RETURN a").close())
        .isInstanceOf(CommandParsingException.class).hasMessageNotContaining("already declared in outer scope");
  }

  @Test
  void unitSubqueryIsUnaffected() {
    database.transaction(() -> database.command("opencypher", "UNWIND [1, 2] AS i CALL { WITH i CREATE (:N {v: i}) } RETURN i").close());
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:N) RETURN count(n) AS c")) {
      assertThat(((Number) rs.next().getProperty("c")).longValue()).isEqualTo(2L);
    }
  }
}
