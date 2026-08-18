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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6310: the row count of one MATCH must not depend on the shape of the clauses around it.
 * <p>
 * The reported pair - the same {@code OPTIONAL MATCH} read once through {@code WITH *} and once through a
 * {@code map -> collect -> UNWIND} round trip - was the Cartesian-product defect of issue #6311 and is covered
 * by {@link CypherWithStarScopeIssue6311Test}; it is pinned again here in the reporter's own spelling because
 * that spelling routes the MATCH through the traditional (non-optimizer) plan, which #6311's tests do not.
 * <p>
 * What the reported invariant also uncovered is a second, independent way the same MATCH could answer
 * differently depending on which plan ran it: Cypher scopes relationship uniqueness to the whole MATCH clause,
 * so two comma-separated pattern parts of one clause may never bind the same relationship. The traditional plan
 * decided per pattern part whether a hop had to bind its edge at all, so a part that could not collide with
 * itself - typically a single-hop part - bound nothing, and the uniqueness check of a later part had nothing to
 * compare against. The cost-based optimizer got this right, so the very same MATCH answered 0 rows on its own
 * and 4 inside a {@code CALL} subquery, after an {@code OPTIONAL}, or next to a named path.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherMatchClauseUniquenessIssue6310Test extends TestHelper {

  @Override
  protected void beginTest() {
    // Three disjoint C -> E -> A chains, plus one extra C that feeds the first E, so that a pattern which
    // ignores relationship uniqueness has a strictly larger answer than one which honours it.
    database.command("opencypher", "CREATE (a1:A {n:'a1'})<-[:R]-(e1:E {n:'e1'})<-[:R]-(c1:C {n:'c1'})");
    database.command("opencypher", "CREATE (a2:A {n:'a2'})<-[:R]-(e2:E {n:'e2'})<-[:R]-(c2:C {n:'c2'})");
    database.command("opencypher", "CREATE (a3:A {n:'a3'})<-[:R]-(e3:E {n:'e3'})<-[:R]-(c3:C {n:'c3'})");
    database.command("opencypher", "MATCH (e:E {n:'e1'}), (c:C {n:'c2'}) CREATE (c)-[:R]->(e)");
  }

  // ---------------------------------------------------------------------------------------------------------
  // The reported pair: materialising the bindings through collect/UNWIND must not change the row count.
  // ---------------------------------------------------------------------------------------------------------

  @Test
  void materializingThroughCollectAndUnwindKeepsTheCardinality() {
    final long direct = count("""
        CALL {
          OPTIONAL MATCH (n0:A), (n3:C)-[]->(:E)-[]->(n0)
          WITH *
          UNWIND [1] AS k
          RETURN count(*) AS rows
        }
        RETURN rows""");

    final long materialized = count("""
        CALL {
          OPTIONAL MATCH (n0:A), (n3:C)-[]->(:E)-[]->(n0)
          WITH {n0: n0, n3: n3} AS x
          WITH collect(x) AS xs
          UNWIND xs AS x
          WITH x.n0 AS n0, x.n3 AS n3
          UNWIND [1] AS k
          RETURN count(*) AS rows
        }
        RETURN rows""");

    // c1 -> e1 -> a1, c2 -> e1 -> a1, c2 -> e2 -> a2, c3 -> e3 -> a3: the join on n0 is what makes it 4 and
    // not 3 x 4, and the two anonymous hops of the single pattern part are always distinct edges.
    // This asserts the two forms AGREE; the pre-fix 4-vs-2 divergence itself is demonstrated in
    // CypherWithStarScopeIssue6311Test, which pins the WITH * Cartesian product that caused it.
    assertThat(direct).isEqualTo(4);
    assertThat(materialized).isEqualTo(direct);
  }

  // ---------------------------------------------------------------------------------------------------------
  // Relationship uniqueness spans the comma: the answer may not depend on which plan ran the clause.
  // ---------------------------------------------------------------------------------------------------------

  /**
   * The only {@code (:E)-[]->(n0:A)} edge for a given {@code n0} is the one {@code r1} already holds, so
   * {@code r2} can never be a different relationship and the clause matches nothing at all.
   */
  private static final String COLLIDING = "MATCH (n0:A)<-[r1]-(:E), (n3:C)-[]->(:E)-[r2]->(n0)";

  @Test
  void aRelationshipIsNotSharedBetweenTwoCommaSeparatedPatternParts() {
    assertThat(rows(COLLIDING + " RETURN n0.n AS a, n3.n AS c")).isEmpty();
  }

  @Test
  void optionalMatchScopesUniquenessToTheWholeClause() {
    // An OPTIONAL MATCH that matches nothing still yields its one all-null row.
    assertThat(rows(COLLIDING.replace("MATCH", "OPTIONAL MATCH") + " RETURN n0.n AS a, n3.n AS c"))
        .containsExactly("null|null");
  }

  @Test
  void aCallSubqueryScopesUniquenessToTheWholeClause() {
    assertThat(rows("CALL { " + COLLIDING + " RETURN n0.n AS a, n3.n AS c } RETURN a, c")).isEmpty();
    assertThat(count("CALL { " + COLLIDING + " WITH * UNWIND [1] AS k RETURN count(*) AS rows } RETURN rows"))
        .isEqualTo(0);
  }

  @Test
  void aNamedPathOnOnePartScopesUniquenessToTheWholeClause() {
    assertThat(rows("MATCH (n0:A)<-[r1]-(:E), p = (n3:C)-[]->(:E)-[r2]->(n0) RETURN n0.n AS a, n3.n AS c"))
        .isEmpty();
  }

  @Test
  void anExistsSubqueryScopesUniquenessToTheWholeClause() {
    assertThat(rows("""
        MATCH (n3:C)
        WHERE EXISTS { MATCH (n0:A)<-[r1]-(:E), (n3)-[]->(:E)-[r2]->(n0) }
        RETURN n3.n AS a, '' AS c""")).isEmpty();
  }

  @Test
  void anonymousHopsAreHeldToTheSameRule() {
    // Nothing names the relationships, so the plan has to bind them itself to be able to compare them.
    assertThat(rows("CALL { MATCH (n0:A)<-[]-(:E), (n3:C)-[]->(:E)-[]->(n0) RETURN n0.n AS a, n3.n AS c } RETURN a, c"))
        .isEmpty();
  }

  /**
   * A variable-length hop's declared endpoints are the endpoints of the whole path, not of any one edge it
   * binds: everything between the first and the last edge starts and ends at a node the pattern says nothing
   * about. So {@code (x:C)-[*1..2]->(y:A)} and {@code (p:E)-[]->(q:A)} look endpoint-disjoint on their written
   * ends - no vertex is both a {@code :C} and an {@code :E} - while the second edge of every path the first
   * one walks is exactly an edge the second part matches.
   */
  @Test
  void aVariableLengthHopMayNotBorrowAnEdgeFromAnotherPart() {
    // 4 paths x 3 (:E)-[]->(:A) edges, less the 4 pairings in which they are the same edge.
    assertThat(rows("MATCH (x:C)-[*1..2]->(y:A), (p:E)-[]->(q:A) RETURN x.n AS a, p.n AS c")).hasSize(8);
    assertThat(rows(
        "CALL { MATCH (x:C)-[*1..2]->(y:A), (p:E)-[]->(q:A) RETURN x.n AS a, p.n AS c } RETURN a, c")).hasSize(8);
  }

  @Test
  void aShortestPathPartIsHeldToTheSameRule() {
    assertThat(rows("MATCH p1 = shortestPath((x:C)-[*1..2]->(y:A)), (p:E)-[]->(q:A) RETURN x.n AS a, p.n AS c"))
        .hasSize(8);
  }

  @Test
  void aSingleEdgeVariableLengthHopKeepsTheEndpointProof() {
    // Capped at one edge, so its written endpoints ARE the edge's and the proof applies again - here it
    // proves nothing (both ends are :E -> :A), and uniqueness still removes the 3 same-edge pairings.
    assertThat(rows("MATCH (x:E)-[*1..1]->(y:A), (p:E)-[]->(q:A) RETURN x.n AS a, p.n AS c")).hasSize(6);
  }

  // ---------------------------------------------------------------------------------------------------------
  // The rule stops at the clause boundary, and the fast path it costs must not be paid where it is not owed.
  // ---------------------------------------------------------------------------------------------------------

  @Test
  void twoMatchClausesMayShareTheSameRelationship() {
    // Uniqueness is per MATCH clause: split across two clauses the same edge is allowed in both.
    assertThat(rows("MATCH (n0:A)<-[r1]-(:E) MATCH (n3:C)-[]->(:E)-[r2]->(n0) RETURN n0.n AS a, n3.n AS c"))
        .containsExactlyInAnyOrder("a1|c1", "a1|c2", "a2|c2", "a3|c3");
  }

  @Test
  void typeDisjointPartsStillMatch() {
    // No shared edge is possible between an :S hop and an :R hop, so the clause matches normally.
    database.command("opencypher", "MATCH (a:A {n:'a1'}) CREATE (a)-[:S]->(:D {n:'d1'})");
    assertThat(rows("MATCH (n0:A)-[s:S]->(:D), (n3:C)-[]->(:E)-[r:R]->(n0) RETURN n0.n AS a, n3.n AS c"))
        .containsExactlyInAnyOrder("a1|c1", "a1|c2");
  }

  @Test
  void endpointDisjointPartsStillMatch() {
    // Same edge type on both parts, but no vertex is both an :E and a :C, so the two hops cannot be the
    // same edge and the clause matches normally.
    // c2 twice per :A - it has two outgoing :R edges into :E - for the full 3 x 4 product.
    assertThat(rows("MATCH (:E)-[r1:R]->(n0:A), (n3:C)-[r2:R]->(:E) RETURN n0.n AS a, n3.n AS c"))
        .containsExactlyInAnyOrder("a1|c1", "a1|c2", "a1|c2", "a1|c3", "a2|c1", "a2|c2", "a2|c2", "a2|c3",
            "a3|c1", "a3|c2", "a3|c2", "a3|c3");
  }

  @Test
  void aSupertypeHopAndASubtypeHopCanBeTheSameEdge() {
    // An edge of a sub-type matches the super-type pattern too, so [:BASE] and [:SUB] are not disjoint and
    // the two parts may not both bind the one edge that exists.
    database.command("sql", "CREATE VERTEX TYPE V");
    database.command("sql", "CREATE EDGE TYPE BASE");
    database.command("sql", "CREATE EDGE TYPE SUB EXTENDS BASE");
    database.command("opencypher", "CREATE (:V {n:'v1'})-[:SUB]->(:V {n:'v2'})");

    assertThat(rows("CALL { MATCH (x:V)-[r1:BASE]->(:V), (y:V)-[r2:SUB]->(:V) RETURN x.n AS a, y.n AS c } RETURN a, c"))
        .isEmpty();
    assertThat(rows("MATCH (x:V)-[r1:BASE]->(:V), (y:V)-[r2:SUB]->(:V) RETURN x.n AS a, y.n AS c")).isEmpty();
  }

  /**
   * An undirected hop has no OUT and no IN end to compare, so the endpoint proof has to decline outright.
   * Were it to guess - reading {@code (x:E)-[]-(y:A)} as if it were written {@code <-}, so that :A is the
   * edge's OUT vertex - it would find :A disjoint from the other part's :E and "prove" the two hops
   * distinct. They are not: undirected, this hop walks the very edge the other part matches.
   */
  @Test
  void aBothDirectionHopGetsNoEndpointProof() {
    // 3 undirected :E-:A edges x 3 (:E)-[]->(:A) edges, less the 3 pairings that are the same edge.
    assertThat(rows("MATCH (x:E)-[]-(y:A), (p:E)-[]->(q:A) RETURN x.n AS a, p.n AS c")).hasSize(6);
  }

  @Test
  void aZeroLengthHopSharesNoEdgeWithAnybody() {
    // [*0..0] binds no edge, so it neither collides with the other part nor costs it its fast path:
    // 3 zero-length :C bindings x the 3 (:E)-[]->(:A) edges, nothing removed.
    assertThat(rows("MATCH (x:C)-[*0..0]->(y), (p:E)-[]->(q:A) RETURN x.n AS a, p.n AS c")).hasSize(9);
  }

  @Test
  void aTypeDisjointVariableLengthHopStillMatches() {
    // The type proof survives the variable length - every edge of a [:R*1..2] hop is an :R - so an :S hop
    // next to it keeps the fast path and the clause matches in full.
    database.command("opencypher", "MATCH (a:A {n:'a1'}) CREATE (a)-[:S]->(:D {n:'d1'})");
    assertThat(rows("MATCH (n0:A)-[:S]->(:D), (x:C)-[:R*1..2]->(y:A) RETURN n0.n AS a, x.n AS c"))
        .containsExactlyInAnyOrder("a1|c1", "a1|c2", "a1|c2", "a1|c3");
  }

  @Test
  void aSelfContainedSinglePartIsUnaffected() {
    assertThat(rows("MATCH (n3:C)-[]->(:E)-[]->(n0:A) RETURN n0.n AS a, n3.n AS c"))
        .containsExactlyInAnyOrder("a1|c1", "a1|c2", "a2|c2", "a3|c3");
  }

  /**
   * Which plan runs a clause decides which of the two implementations its uniqueness comes from, so it is
   * worth pinning rather than assuming. The cost-based optimizer declines every variable-length hop, comma
   * or no comma - so there is no optimizer-side variable-length path for the guards above to be wrong on,
   * and a test claiming to cover one would in fact be exercising the traditional plan. A comma clause of
   * fixed-length hops IS optimized, which is what makes the rest of this class cover both implementations.
   * <p>
   * A tripwire, not a preference: when the optimizer does grow variable-length support, this fails, and
   * whoever adds it is made to answer the uniqueness question on that path too.
   */
  @Test
  void theOptimizerDeclinesVariableLengthHopsButNotCommas() {
    assertThat(planOf("MATCH (x:C)-[*1..2]->(y:A), (p:E)-[]->(q:A) RETURN x.n AS a"))
        .contains("Traditional Execution");
    assertThat(planOf("MATCH p1 = shortestPath((x:C)-[*1..2]->(y:A)), (p:E)-[]->(q:A) RETURN x.n AS a"))
        .contains("Traditional Execution");
    assertThat(planOf("MATCH (x:C)-[*1..2]->(y:A) RETURN x.n AS a"))
        .contains("Traditional Execution");
    assertThat(planOf("MATCH (n0:A)<-[r1]-(:E), (n3:C)-[]->(:E)-[r2]->(n0) RETURN n0.n AS a"))
        .doesNotContain("Traditional Execution");
  }

  private String planOf(final String query) {
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query)) {
      assertThat(rs.hasNext()).isTrue();
      return String.valueOf(rs.next().<Object>getProperty("executionPlan"));
    }
  }

  private List<String> rows(final String query) {
    final List<String> out = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        out.add(r.<Object>getProperty("a") + "|" + r.<Object>getProperty("c"));
      }
    }
    return out;
  }

  private long count(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      assertThat(rs.hasNext()).isTrue();
      return ((Number) rs.next().getProperty("rows")).longValue();
    }
  }
}
