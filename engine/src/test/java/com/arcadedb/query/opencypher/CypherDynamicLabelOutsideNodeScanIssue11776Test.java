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
import com.arcadedb.exception.TypeMismatchException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11776: a Cypher 25 dynamic {@code $(expr)} label was honoured by the node scan, the variable-length hop and the
 * write clauses, but every other label consumer received an empty static label list, which means "no constraint": the
 * predicate {@code n:$(expr)} was always true, a single-hop target {@code (b:$(expr))} accepted any label, and a
 * relationship type {@code [:$(expr)]} in MATCH matched every type.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherDynamicLabelOutsideNodeScanIssue11776Test extends TestHelper {

  private List<Object> column(final String query, final String column, final Object... params) {
    final List<Object> out = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query, toMap(params))) {
      while (rs.hasNext())
        out.add(rs.next().getProperty(column));
    }
    return out;
  }

  private static Map<String, Object> toMap(final Object... params) {
    final Map<String, Object> map = new HashMap<>();
    for (int i = 0; i < params.length; i += 2)
      map.put((String) params[i], params[i + 1]);
    return map;
  }

  private void create(final String cypher) {
    database.transaction(() -> database.command("opencypher", cypher).close());
  }

  /** Single-hop target {@code (b:$(expr))} and every pattern consumer built on it. */
  @Nested
  class HopTarget {
    private void setUpGraph() {
      create("CREATE (a:A {name:'a1'}), (b1:B {x:1}), (b2:B {x:2}), (c:C {x:3}), (a)-[:R]->(b1), (a)-[:R]->(b2), (a)-[:R]->(c)");
    }

    @Test
    void singleHopTargetHonoursTheDynamicLabel() {
      setUpGraph();
      assertThat(column("MATCH (a:A)-[:R]->(b:$('B')) RETURN b.x AS x ORDER BY x", "x")).containsExactly(1, 2);
    }

    @Test
    void singleHopTargetHonoursAParameterLabel() {
      setUpGraph();
      assertThat(column("MATCH (a:A)-[:R]->(b:$($label)) RETURN b.x AS x ORDER BY x", "x", "label", "B")).containsExactly(1, 2);
    }

    @Test
    void controlsVariableLengthAndAnchorScanStayCorrect() {
      setUpGraph();
      assertThat(column("MATCH (a:A)-[:R*1..1]->(b:$('B')) RETURN b.x AS x ORDER BY x", "x")).containsExactly(1, 2);
      assertThat(column("MATCH (b:$('B'))<-[:R]-(a:A) RETURN b.x AS x ORDER BY x", "x")).containsExactly(1, 2);
    }

    @Test
    void aLabelNobodyHasMatchesNothing() {
      setUpGraph();
      assertThat(column("MATCH (x:A)-[:R]->(a:$('Nope')) RETURN a.x AS x", "x")).isEmpty();
    }

    @Test
    void aBoundTargetIsCheckedAgainstTheDynamicLabel() {
      setUpGraph();
      assertThat(column("MATCH (x:A), (a:B) MATCH (x)-[:R]->(a:$('C')) RETURN a.x AS x", "x")).isEmpty();
      assertThat(column("MATCH (x:A), (a:B) MATCH (x)-[:R]->(a:$('B')) RETURN a.x AS x ORDER BY x", "x")).containsExactly(1, 2);
    }

    @Test
    void staticAndDynamicConjunctionOnTheTarget() {
      setUpGraph();
      assertThat(column("MATCH (a:A)-[:R]->(b:B:$('C')) RETURN b.x AS x", "x")).isEmpty();
    }

    @Test
    void patternComprehension() {
      setUpGraph();
      final List<Object> xs = column("MATCH (a:A) RETURN [(a)-[:R]->(b:$('B')) | b.x] AS xs", "xs");
      assertThat(xs).hasSize(1);
      assertThat((List<Object>) xs.getFirst()).containsExactlyInAnyOrder(1, 2);
    }

    @Test
    void countSubquery() {
      setUpGraph();
      assertThat(column("MATCH (a:A) RETURN COUNT { (a)-[:R]->(b:$('B')) } AS n", "n")).containsExactly(2L);
    }

    @Test
    void existsSubquery() {
      setUpGraph();
      assertThat(column("MATCH (a:A) WHERE EXISTS { (a)-[:R]->(:$('Nope')) } RETURN a.name AS name", "name")).isEmpty();
      assertThat(column("MATCH (a:A) WHERE EXISTS { (a)-[:R]->(:$('C')) } RETURN a.name AS name", "name")).containsExactly("a1");
    }

    @Test
    void optionalMatch() {
      create("CREATE (b1:B {x:1}), (c:C {x:3}), (a:A {x:0}), (a)-[:R]->(b1), (a)-[:S]->(c)");
      final List<Object> xs = column("MATCH (a:A) OPTIONAL MATCH (a)-[:R|S]->(n:$('Nope')) RETURN n.x AS x", "x");
      assertThat(xs).hasSize(1);
      assertThat(xs.getFirst()).isNull();
    }

    @Test
    void groupedCountDoesNotOverCount() {
      setUpGraph();
      assertThat(column("MATCH (a:A)-[:R]->(:$('B')) RETURN a.name AS name, count(*) AS n", "n")).containsExactly(2L);
      assertThat(column("MATCH (a:A)-[:R]->(:$('B')) RETURN count(*) AS n", "n")).containsExactly(2L);
    }
  }

  /** The predicate {@code n:$(expr)} in WHERE and RETURN. */
  @Nested
  class Predicate {
    @Test
    void whereFiltersOnTheDynamicLabel() {
      create("CREATE (b1:B {x:1}), (c:C {x:3}), (a:A {x:0})");
      assertThat(column("MATCH (n) WHERE n:$($label) RETURN n.x AS x ORDER BY x", "x", "label", "B")).containsExactly(1);
    }

    @Test
    void returnEvaluatesTheDynamicLabelPerRow() {
      create("CREATE (b1:B {x:1}), (c:C {x:3}), (a:A {x:0})");
      try (final ResultSet rs = database.query("opencypher", "MATCH (n) RETURN n.x AS x, n:$($label) AS b ORDER BY x",
          Map.of("label", "B"))) {
        final List<Boolean> flags = new ArrayList<>();
        while (rs.hasNext())
          flags.add(rs.next().getProperty("b"));
        assertThat(flags).containsExactly(false, true, false);
      }
    }

    @Test
    void dynamicAndStaticAgree() {
      create("CREATE (c:C {x:3})");
      try (final ResultSet rs = database.query("opencypher", "MATCH (n:C) RETURN n:$('B') AS isB, n:B AS isB2, n:$('C') AS isC")) {
        final Result r = rs.next();
        assertThat((Boolean) r.getProperty("isB")).isFalse();
        assertThat((Boolean) r.getProperty("isB2")).isFalse();
        assertThat((Boolean) r.getProperty("isC")).isTrue();
      }
    }

    @Test
    void negation() {
      create("CREATE (b1:B {x:1}), (c:C {x:3}), (a:A {x:0})");
      assertThat(column("MATCH (n) WHERE NOT n:$('B') RETURN n.x AS x ORDER BY x", "x")).containsExactly(0, 3);
    }

    @Test
    void staticAndDynamicConjunction() {
      create("CREATE (b1:B {x:1}), (c:C {x:3}), (a:A {x:0})");
      assertThat(column("MATCH (n) WHERE n:B:$('C') RETURN n.x AS x", "x")).isEmpty();
      assertThat(column("MATCH (n) WHERE n:B:$('B') RETURN n.x AS x", "x")).containsExactly(1);
    }

    @Test
    void isSpelling() {
      create("CREATE (b1:B {x:1}), (c:C {x:3})");
      assertThat(column("MATCH (n) WHERE n IS $('B') RETURN n.x AS x", "x")).containsExactly(1);
    }

    @Test
    void aListValueRequiresEveryLabel() {
      create("CREATE (b1:B:C {x:1}), (c:C {x:3})");
      assertThat(column("MATCH (n) WHERE n:$(['B','C']) RETURN n.x AS x", "x")).containsExactly(1);
    }
  }

  /** The relationship type {@code [:$(expr)]} in MATCH. */
  @Nested
  class RelationshipType {
    private void setUpGraph() {
      create("CREATE (b1:B {x:1}), (c:C {x:3}), (a:A {x:0}), (a)-[:R]->(b1), (a)-[:S]->(c)");
    }

    @Test
    void dynamicTypeFiltersTheHop() {
      setUpGraph();
      assertThat(column("MATCH (:A)-[r:$('R')]->(n) RETURN n.x AS x ORDER BY x", "x")).containsExactly(1);
      assertThat(column("MATCH (:A)-[r:$($t)]->(n) RETURN n.x AS x ORDER BY x", "x", "t", "S")).containsExactly(3);
    }

    @Test
    void dynamicTypeInExistsAndComprehension() {
      setUpGraph();
      assertThat(column("MATCH (a:A) RETURN COUNT { (a)-[:$('S')]->() } AS n", "n")).containsExactly(1L);
      final List<Object> xs = column("MATCH (a:A) RETURN [(a)-[:$('R')]->(b) | b.x] AS xs", "xs");
      assertThat((List<Object>) xs.getFirst()).containsExactly(1);
    }

    @Test
    void dynamicTypeOnVariableLength() {
      setUpGraph();
      assertThat(column("MATCH (:A)-[:$('S')*1..2]->(n) RETURN n.x AS x", "x")).containsExactly(3);
    }

    @Test
    void anUnknownTypeMatchesNothing() {
      setUpGraph();
      assertThat(column("MATCH (:A)-[r:$('Nope')]->(n) RETURN n.x AS x", "x")).isEmpty();
    }
  }

  /** The remaining read shapes: shortest path, quantified paths, mixed type lists, pattern predicates, row-bound labels. */
  @Nested
  class OtherReadShapes {
    private void setUpGraph() {
      create("CREATE (a:A {name:'B', x:0}), (b1:B {x:1}), (b2:B:C {x:2}), (c:C {x:3}), "
          + "(a)-[:R]->(b1), (b1)-[:R]->(b2), (a)-[:S]->(c)");
    }

    @Test
    void shortestPathHonoursTheDynamicType() {
      setUpGraph();
      assertThat(column("MATCH p = shortestPath((a:A)-[:$('S')*]->(n:C)) RETURN n.x AS x", "x")).containsExactly(3);
      assertThat(column("MATCH p = shortestPath((a:A)-[:$('R')*]->(n:C)) RETURN n.x AS x", "x")).containsExactly(2);
    }

    @Test
    void quantifiedPathHonoursTheDynamicTypeAndTarget() {
      setUpGraph();
      assertThat(column("MATCH (a:A) ((x)-[:$('R')]->(y)){1,2} (n) RETURN n.x AS x ORDER BY x", "x")).containsExactly(1, 2);
      assertThat(column("MATCH (a:A)-[:R]->{1,2}(n:$('C')) RETURN n.x AS x", "x")).containsExactly(2);
    }

    @Test
    void mixedStaticAndDynamicTypes() {
      setUpGraph();
      assertThat(column("MATCH (:A)-[:R|$('S')]->(n) RETURN n.x AS x ORDER BY x", "x")).containsExactly(1, 3);
      assertThat(column("MATCH (:A)-[:R|$('Nope')]->(n) RETURN n.x AS x", "x")).containsExactly(1);
    }

    @Test
    void patternPredicate() {
      setUpGraph();
      assertThat(column("MATCH (a:A) WHERE (a)-[:$('S')]->(:C) RETURN a.x AS x", "x")).containsExactly(0);
      assertThat(column("MATCH (a:A) WHERE (a)-[:$('R')]->(:C) RETURN a.x AS x", "x")).isEmpty();
      assertThat(column("MATCH (a:A) WHERE (a)-->(:$('Nope')) RETURN a.x AS x", "x")).isEmpty();
    }

    @Test
    void labelReadFromAnEarlierBinding() {
      setUpGraph();
      assertThat(column("MATCH (a:A) MATCH (a)-[:R]->(b:$(a.name)) RETURN b.x AS x", "x")).containsExactly(1);
      assertThat(column("MATCH (a:A) MATCH (a)-[]->(b) WHERE b:$(a.name) RETURN b.x AS x", "x")).containsExactly(1);
    }

    /** Adversarial finding: a node disjunction of a static and a dynamic label dropped the dynamic alternative off the anchor. */
    @Test
    void nodeDisjunctionOfStaticAndDynamicLabels() {
      create("CREATE (a:A {x:0}), (b:B {x:1}), (c:C {x:2}), (a)-[:R]->(b), (a)-[:R]->(c)");
      assertThat(column("MATCH (a:A)-[:R]->(n:C|$('B')) RETURN n.x AS x ORDER BY x", "x")).containsExactly(1, 2);
      assertThat(column("MATCH (a:A)-[:R]->(n:Missing|$('B')) RETURN n.x AS x", "x")).containsExactly(1);
      assertThat(column("MATCH (n:C|$('B')) RETURN n.x AS x ORDER BY x", "x")).containsExactly(1, 2);
      assertThat(column("MATCH (n:C|$('B'))<-[:R]-(:A) RETURN n.x AS x ORDER BY x", "x")).containsExactly(1, 2);
      final List<Object> xs = column("MATCH (a:A) RETURN [(a)-[:R]->(n:C|$('B')) | n.x] AS xs", "xs");
      assertThat((List<Object>) xs.getFirst()).containsExactlyInAnyOrder(1, 2);
      assertThat(column("MATCH (a:A) WHERE (a)-[:R]->(:Missing|$('B')) RETURN a.x AS x", "x")).containsExactly(0);
      assertThat(column("MATCH (a:A) RETURN COUNT { (a)-[:R]->(:C|$('B')) } AS n", "n")).containsExactly(2L);
    }

    @Test
    void mergeWithADynamicLabelStaysIdempotent() {
      create("MERGE (n:$('E') {x:5})");
      create("MERGE (n:$('E') {x:5})");
      assertThat(column("MATCH (n:E) RETURN count(n) AS c", "c")).containsExactly(1L);
    }
  }

  /** Findings from the PR review. */
  @Nested
  class ReviewFindings {
    /** {@code $(list)} is {@code $all(list)}: under {@code |} it is one conjunctive operand, A OR (B AND C). */
    @Test
    void aListValuedDynamicLabelInADisjunctionIsOneConjunctiveOperand() {
      create("CREATE (s:S), (a:A {x:1}), (b:B {x:2}), (bc:B:C {x:3}), (c:C {x:4}), "
          + "(s)-[:R]->(a), (s)-[:R]->(b), (s)-[:R]->(bc), (s)-[:R]->(c)");
      assertThat(column("MATCH (n) WHERE n:A|$(['B','C']) RETURN n.x AS x ORDER BY x", "x")).containsExactly(1, 3);
      assertThat(column("MATCH (n) WHERE n:A|$($l) RETURN n.x AS x ORDER BY x", "x", "l", List.of("B", "C"))).containsExactly(1, 3);
      assertThat(column("MATCH (n:A|$(['B','C'])) RETURN n.x AS x ORDER BY x", "x")).containsExactly(1, 3);
      assertThat(column("MATCH (:S)-[:R]->(n:A|$(['B','C'])) RETURN n.x AS x ORDER BY x", "x")).containsExactly(1, 3);
      // a one-element list and a plain string are the single label
      assertThat(column("MATCH (n) WHERE n:A|$(['B']) RETURN n.x AS x ORDER BY x", "x")).containsExactly(1, 2, 3);
    }

    @Test
    void aListValuedDynamicTypeInADisjunctionCannotMatchAnEdge() {
      create("CREATE (s:S), (t:T), (s)-[:A {x:1}]->(t), (s)-[:B {x:2}]->(t), (s)-[:C {x:3}]->(t)");
      assertThat(column("MATCH (:S)-[r:A|$(['B','C'])]->() RETURN r.x AS x ORDER BY x", "x")).containsExactly(1);
      assertThat(column("MATCH (:S)-[r]->() WHERE r:A|$(['B','C']) RETURN r.x AS x ORDER BY x", "x")).containsExactly(1);
      assertThat(column("MATCH (:S)-[r:A|$(['B'])]->() RETURN r.x AS x ORDER BY x", "x")).containsExactly(1, 2);
    }

    @Test
    void anUndefinedNameInADynamicRelationshipTypeIsRejected() {
      create("CREATE (a:A)-[:R]->(:B)");
      assertThatThrownBy(() -> column("MATCH (a:A)-[r:$(missingType)]->(n) RETURN n", "n"))
          .hasMessageContaining("missingType");
      assertThat(column("WITH 'R' AS t MATCH (:A)-[r:$(t)]->(n) RETURN count(n) AS c", "c")).containsExactly(1L);
    }

    @Test
    void shortestPathExpressionChecksTheEndpointLabels() {
      create("CREATE (a:A {x:0}), (b:B {x:1}), (c:B:C {x:2}), (a)-[:R]->(b), (b)-[:R]->(c)");
      assertThat(column("MATCH (a:A), (b:B) RETURN b.x AS x, shortestPath((a)-[:R*]->(b:$('C'))) IS NULL AS none ORDER BY x",
          "none")).containsExactly(true, false);
      assertThat(column("MATCH (a:A), (b:B) RETURN b.x AS x, shortestPath((a)-[:R*]->(b:C)) IS NULL AS none ORDER BY x",
          "none")).containsExactly(true, false);
    }

    /**
     * A dynamic label that evaluates to null is a type error, on a node and on a relationship alike (issue #12338: it used to
     * add no constraint); the empty list is the empty conjunction, which holds.
     */
    @Test
    void aNullDynamicLabelIsATypeErrorOnNodesAndRelationships() {
      create("CREATE (a:A)-[:R]->(:B)");
      assertThatThrownBy(() -> column("MATCH (n:A) RETURN n:$(null) AS b", "b")).isInstanceOf(TypeMismatchException.class);
      assertThatThrownBy(() -> column("MATCH ()-[r]->() RETURN r:$(null) AS b", "b")).isInstanceOf(TypeMismatchException.class);
      assertThat(column("MATCH (n:A) RETURN n:$([]) AS b", "b")).containsExactly(true);
      assertThat(column("MATCH ()-[r]->() RETURN r:$([]) AS b", "b")).containsExactly(true);
    }
  }
}
