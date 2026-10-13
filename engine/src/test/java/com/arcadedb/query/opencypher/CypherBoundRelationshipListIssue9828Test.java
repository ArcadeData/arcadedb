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

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9828 (openCypher TCK Match4 [8]): a variable-length pattern whose relationship variable an earlier clause bound
 * to a list matches that list, in that order, and nothing else. The fix is shared by every evaluator of a
 * variable-length pattern, so each one is pinned here: the traditional {@code ExpandPathStep}, the cost-based
 * {@code VarLengthExpand}, the inline pattern predicate, the pattern comprehension and the {@code EXISTS}/{@code COUNT}
 * subqueries. Also pins the two pattern-predicate defects found alongside (path mode and zero-length paths) and the
 * pattern comprehension binding a variable-length relationship variable to a list.
 * <p>
 * Graph: {@code (a:A)-[:Y {w:1}]->(b:B)-[:Y {w:2}]->(c:C)}, {@code (a)-[:OTHER]->(d:D)-[:OTHER]->(c)} and
 * {@code (c)-[:BACK]->(a)} closing a cycle.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherBoundRelationshipListIssue9828Test {
  private static final String BOUND_YY = "MATCH (:A)-[r1:Y]->(:B)-[r2:Y]->(:C) WITH [r1, r2] AS rs ";

  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/cypher-bound-relationship-list-9828").create();
    database.transaction(() -> database.command("opencypher", """
        CREATE (a:A {n: 'a'}), (b:B {n: 'b'}), (c:C {n: 'c'}), (d:D {n: 'd'})
        CREATE (a)-[:Y {w: 1}]->(b), (b)-[:Y {w: 2}]->(c), (a)-[:OTHER]->(d), (d)-[:OTHER]->(c), (c)-[:BACK]->(a)"""));
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void costBasedPlanMatchesTheBoundList() {
    final String query = "MATCH (a:A)-[r:Y*]->(c:C) MATCH (a)-[r*]->(y:C) RETURN a.n AS f, y.n AS s";
    assertThat(explain(query)).as("the shape must reach the physical operator, or this test is vacuous")
        .contains("Cost-Based").contains("VarLengthExpand(a)-[r*");
    // The OTHER walk a->d->c also ends on a C: it is not the bound list and must not come back
    assertThat(pairs(query)).containsExactly("a->c");
  }

  @Test
  void costBasedPlanReadsTheListInWrittenOrder() {
    // r = [a->b, b->c] in written order from (a): read from (c) its first relationship does not touch c
    assertThat(pairs("MATCH (a:A)-[r:Y*2]->(c:C) MATCH (c)<-[r*]-(y:A) RETURN y.n AS f, c.n AS s")).isEmpty();

    // Written from (c), the same chain binds r = [b->c, a->b], which the second pattern walks back to (a)
    final String query = "MATCH (c:C)<-[r:Y*2]-(a:A) MATCH (c)<-[r*]-(y:A) RETURN y.n AS f, c.n AS s";
    assertThat(explain(query)).contains("Cost-Based").contains("VarLengthExpand");
    assertThat(pairs(query)).containsExactly("a->c");
  }

  @Test
  void costBasedPlanReversingTheConsumingPatternReadsTheListFromItsEnd() {
    // The optimizer expands the second clause first, from (y:A), binding r; the first clause's pattern then runs
    // reversed from the bound (c), so it must read r from its last relationship. a->d->c binds an OTHER list the
    // :Y filter refuses, and the longer trails through the BACK edge are refused by the *2 bound.
    final String query = "MATCH (a:A)-[r:Y*2]->(c:C) MATCH (y:A)-[r*]->(c) RETURN y.n AS f, c.n AS s";
    assertThat(explain(query)).contains("Cost-Based").contains("VarLengthExpand(c)<-[r:Y*2..2]-(a");
    assertThat(pairs(query)).containsExactly("a->c");
  }

  @Test
  void traditionalPlanWithABoundTargetMatchesTheListFromTheSource() {
    // (c) bound and (first) not: every vertex is tried as (first), and only the one the list leaves from reaches (c)
    final String bound = "MATCH (:A)-[r1:Y]->(:B)-[r2:Y]->(c:C) WITH [r1, r2] AS rs, c ";
    assertThat(pairs(bound + "MATCH (first)-[rs*]->(c) RETURN first.n AS f, c.n AS s")).containsExactly("a->c");
    assertThat(pairs(bound + "MATCH (c)<-[rs*]-(first) RETURN first.n AS f, c.n AS s")).isEmpty();
  }

  @Test
  void inlinePatternPredicateMatchesTheBoundList() {
    assertThat(pairs(BOUND_YY + "MATCH (first:A), (second:C) WHERE (first)-[rs*]->(second) RETURN first.n AS f, second.n AS s"))
        .containsExactly("a->c");
    // b->c is a path of the graph, but not the bound one
    assertThat(pairs(BOUND_YY + "MATCH (first:B), (second:C) WHERE (first)-[rs*]->(second) RETURN first.n AS f, second.n AS s"))
        .isEmpty();
    assertThat(pairs(BOUND_YY + "MATCH (first:B), (second:C) WHERE NOT (first)-[rs*]->(second) RETURN first.n AS f, second.n AS s"))
        .containsExactly("b->c");
  }

  @Test
  void patternComprehensionMatchesTheBoundList() {
    assertThat(single(BOUND_YY + "MATCH (first:A) RETURN [(first)-[rs*]->(x) | x.n] AS v")).isEqualTo(List.of("c"));
    assertThat(single(BOUND_YY + "MATCH (first:B) RETURN [(first)-[rs*]->(x) | x.n] AS v")).isEqualTo(List.of());
    assertThat(String.valueOf(single(BOUND_YY + "MATCH (first:A) RETURN [p = (first)-[rs*]->(x) | length(p)] AS v"))).isEqualTo("[2]");
    assertThat(single(BOUND_YY + "MATCH (first:A) RETURN [(first)-[rs:OTHER*]->(x) | x.n] AS v")).isEqualTo(List.of());
  }

  @Test
  void subqueriesMatchTheBoundList() {
    assertThat(names(BOUND_YY + "MATCH (first) WHERE EXISTS { MATCH (first)-[rs*]->(x) } RETURN first.n AS n"))
        .containsExactly("a");
    assertThat(((Number) single(BOUND_YY + "MATCH (first:A) RETURN COUNT { MATCH (first)-[rs*]->(x) } AS v")).longValue()).isEqualTo(1L);
  }

  @Test
  void boundListObeysRelationshipUniquenessAndPathMode() {
    final String sameEdgeTwice = "MATCH (:A)-[r1:Y]->(:B) WITH [r1, r1] AS rs ";
    // TRAIL, the default: a path never traverses one relationship twice
    assertThat(pairs(sameEdgeTwice + "MATCH (first)-[rs*]-(second) RETURN first.n AS f, second.n AS s")).isEmpty();
    // WALK lifts that rule, so the edge out and back again is a walk from either of its ends
    assertThat(pairs(sameEdgeTwice + "MATCH WALK (first)-[rs*..2]-(second) RETURN first.n AS f, second.n AS s"))
        .containsExactlyInAnyOrder("a->a", "b->b");

    final String cycle = "MATCH (:A)-[r1:Y]->(:B)-[r2:Y]->(:C)-[r3:BACK]->(:A) WITH [r1, r2, r3] AS rs ";
    assertThat(pairs(cycle + "MATCH (first)-[rs*]->(second) RETURN first.n AS f, second.n AS s")).containsExactly("a->a");
    // ACYCLIC refuses the return to the start vertex
    assertThat(pairs(cycle + "MATCH ACYCLIC (first)-[rs*]->(second) RETURN first.n AS f, second.n AS s")).isEmpty();
  }

  @Test
  void boundListObeysInlineRelationshipFilters() {
    assertThat(pairs(BOUND_YY + "MATCH (first)-[rs* {w: 1}]->(second) RETURN first.n AS f, second.n AS s")).isEmpty();
    assertThat(pairs(BOUND_YY + "MATCH (first)-[rs* WHERE rs.w > 0]->(second) RETURN first.n AS f, second.n AS s"))
        .containsExactly("a->c");
    assertThat(pairs(BOUND_YY + "MATCH (first)-[rs* WHERE rs.w > 1]->(second) RETURN first.n AS f, second.n AS s")).isEmpty();
  }

  @Test
  void aValueThatIsNotAListOfRelationshipsIsATypeError() {
    assertThatThrownBy(() -> pairs("MATCH (:A)-[r1:Y]->(:B) WITH r1 AS rs MATCH (first)-[rs*]->(second) "
        + "RETURN first.n AS f, second.n AS s")).hasStackTraceContaining("TypeError");
    assertThatThrownBy(() -> pairs("MATCH (:A)-[r1:Y]->(:B) WITH [r1, 1] AS rs MATCH (first)-[rs*]->(second) "
        + "RETURN first.n AS f, second.n AS s")).hasStackTraceContaining("TypeError");
    // A null element is not a type error, it just matches nothing, like a null list
    assertThat(pairs("MATCH (:A)-[r1:Y]->(:B) WITH [r1, null] AS rs MATCH (first)-[rs*]->(second) "
        + "RETURN first.n AS f, second.n AS s")).isEmpty();
  }

  @Test
  void inlinePatternPredicateUsesThePatternPathModeAndKeepsZeroLengthPaths() {
    // Found alongside #9828: the predicate walked ACYCLIC, so the cycle back to (a) was false while MATCH found it,
    // and it dropped every zero-length path, so *0.. was false while EXISTS { } was true
    assertThat(names("MATCH (a:A) WHERE (a)-[*]->(a) RETURN a.n AS n")).containsExactly("a");
    assertThat(names("MATCH (a:A) WHERE EXISTS { (a)-[*]->(a) } RETURN a.n AS n")).containsExactly("a");
    assertThat(names("MATCH (a:A) WHERE (a)-[*0..]->(a) RETURN a.n AS n")).containsExactly("a");
    assertThat(names("MATCH (a:A) WHERE (a)-[*0..0]->(:A) RETURN a.n AS n")).containsExactly("a");
    assertThat(names("MATCH (a:A) WHERE (a)-[*0..0]->(:B) RETURN a.n AS n")).isEmpty();
  }

  @Test
  void patternComprehensionBindsAVariableLengthRelationshipToAList() {
    // Found alongside #9828: the variable held the last relationship walked, so size(r) failed and r was not a list
    assertThat(String.valueOf(single("MATCH (a:A) RETURN [(a)-[r:Y|OTHER*]->(x:C) | size(r)] AS v"))).isEqualTo("[2, 2]");
    assertThat(String.valueOf(single("MATCH (a:A) RETURN [(a)-[r:Y*]->(x) | [e IN r | e.w]] AS v")))
        .isEqualTo("[[1], [1, 2]]");
    // The zero-length hop binds the empty list, as MATCH does
    final List<Long> sizes = new ArrayList<>();
    for (final Object size : (List<?>) single("MATCH (a:A) RETURN [(a)-[r*0..1]->(x) | size(r)] AS v"))
      sizes.add(((Number) size).longValue());
    assertThat(sizes).containsExactlyInAnyOrder(0L, 1L, 1L);
  }

  private String explain(final String query) {
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query)) {
      return rs.next().toJSON().toString();
    }
  }

  private List<String> pairs(final String query) {
    final List<String> pairs = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        pairs.add(row.getProperty("f") + "->" + row.getProperty("s"));
      }
    }
    return pairs;
  }

  private List<String> names(final String query) {
    final List<String> names = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext())
        names.add(rs.next().getProperty("n"));
    }
    return names;
  }

  private Object single(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      final Object value = rs.next().getProperty("v");
      assertThat(rs.hasNext()).isFalse();
      return value;
    }
  }
}
