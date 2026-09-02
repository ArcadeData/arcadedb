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
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.traversal.TraversalPath;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #4531 - GQL Quantified Path Patterns, Phase B: the shapes that cannot be lowered onto the
 * variable-length-relationship engine and were rejected with {@code FeatureNotImplemented} before.
 *
 * <p>Covered here: multi-relationship inner patterns, inner {@code WHERE} predicates evaluated once
 * per repetition, inner variables surfacing outside the group as parallel {@code LIST<NODE>} /
 * {@code LIST<RELATIONSHIP>} bindings, and grouped path assignment yielding {@code LIST<PATH>}.
 *
 * <p>Every query here plans on the legacy pipeline: {@code CypherExecutionPlanner} declines a
 * quantified group, so the optimizer never sees one. {@link #quantifiedGroupPlansAsAQuantifiedPathStep()}
 * pins that, since a test that silently drifted onto the other path would prove nothing about this one.
 */
class CypherQuantifiedPathPatternPhaseBIssue4531Test {
  private Database database;

  @BeforeEach
  void setUp(final TestInfo testInfo) {
    final String databasePath = "./target/databases/testopencypher-qpp-phaseb-" + testInfo.getTestMethod().get().getName();
    final DatabaseFactory factory = new DatabaseFactory(databasePath);
    if (factory.exists())
      factory.open().drop();
    database = factory.create();

    database.getSchema().createVertexType("P");
    database.getSchema().createEdgeType("R1");
    database.getSchema().createEdgeType("R2");
    database.getSchema().createEdgeType("KNOWS");
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  /**
   * A→B→C→D→E where the edge types alternate R1, R2, R1, R2, so a repeated two-hop unit
   * {@code (R1 then R2)} lands on C after one repetition and on E after two.
   */
  private void createAlternatingChain() {
    database.transaction(() -> database.command("opencypher",
        "CREATE (a:P {name:'A'})-[:R1 {w:10}]->(b:P {name:'B'})-[:R2 {w:1}]->(c:P {name:'C'})"
            + "-[:R1 {w:20}]->(d:P {name:'D'})-[:R2 {w:2}]->(e:P {name:'E'})"));
  }

  /** A→B→C→D over one edge type, with descending weights so an inner WHERE can cut the chain short. */
  private void createWeightedChain() {
    database.transaction(() -> database.command("opencypher",
        "CREATE (a:P {name:'A'})-[:KNOWS {weight:10}]->(b:P {name:'B'})-[:KNOWS {weight:1}]->(c:P {name:'C'})"
            + "-[:KNOWS {weight:10}]->(d:P {name:'D'})"));
  }

  // ---------------------------------------------------------------------------
  // 1. Multi-relationship inner patterns
  // ---------------------------------------------------------------------------

  // Issue #4531: a two-hop inner unit repeats as a unit, so only the vertices at a multiple of two hops match.
  @Test
  void multiRelationshipInnerPatternRepeatsTheWholeUnit() {
    createAlternatingChain();

    assertThat(namesOf("MATCH (s:P {name:'A'}) ((x:P)-[:R1]->(y:P)-[:R2]->(z:P))+ (t:P) RETURN t.name AS name"))
        .containsExactlyInAnyOrder("C", "E");
  }

  // Issue #4531: an exact quantifier on a two-hop inner unit matches exactly 2*n hops away.
  @Test
  void multiRelationshipInnerPatternHonoursExactQuantifier() {
    createAlternatingChain();

    assertThat(namesOf("MATCH (s:P {name:'A'}) ((x:P)-[:R1]->(y:P)-[:R2]->(z:P)){2} (t:P) RETURN t.name AS name"))
        .containsExactly("E");
    assertThat(namesOf("MATCH (s:P {name:'A'}) ((x:P)-[:R1]->(y:P)-[:R2]->(z:P)){1} (t:P) RETURN t.name AS name"))
        .containsExactly("C");
  }

  // Issue #4531: the inner unit's edge types are enforced in order - reversing them matches nothing from A.
  @Test
  void multiRelationshipInnerPatternEnforcesHopOrder() {
    createAlternatingChain();

    assertThat(namesOf("MATCH (s:P {name:'A'}) ((x:P)-[:R2]->(y:P)-[:R1]->(z:P))+ (t:P) RETURN t.name AS name"))
        .isEmpty();
  }

  // ---------------------------------------------------------------------------
  // 2. Inner WHERE predicates, evaluated per repetition
  // ---------------------------------------------------------------------------

  // Issue #4531: an inner WHERE constrains each repetition, so the walk stops at the first failing hop.
  @Test
  void innerWhereIsEvaluatedOncePerRepetition() {
    createWeightedChain();

    // A-[10]->B satisfies w>5; B-[1]->C does not, so the repetition cannot continue past B.
    assertThat(namesOf(
        "MATCH (s:P {name:'A'}) ((m:P)-[r:KNOWS]->(n:P) WHERE r.weight > 5)+ (t:P) RETURN t.name AS name"))
        .containsExactly("B");

    // Without the predicate the same group walks the whole chain.
    assertThat(namesOf("MATCH (s:P {name:'A'}) ((m:P)-[r:KNOWS]->(n:P) WHERE r.weight > 0)+ (t:P) RETURN t.name AS name"))
        .containsExactlyInAnyOrder("B", "C", "D");
  }

  // Issue #4531: an inner WHERE can reference the inner node variables, not only the relationship.
  @Test
  void innerWhereCanReferenceInnerNodeVariables() {
    createWeightedChain();

    assertThat(namesOf(
        "MATCH (s:P {name:'A'}) ((m:P)-[r:KNOWS]->(n:P) WHERE n.name <> 'C')+ (t:P) RETURN t.name AS name"))
        .containsExactly("B");
  }

  // ---------------------------------------------------------------------------
  // 3. Group variables: LIST<NODE> and LIST<RELATIONSHIP>
  // ---------------------------------------------------------------------------

  // Issue #4531: inner node and relationship variables surface outside the group as parallel lists,
  // one element per repetition, in repetition order.
  @Test
  void innerVariablesBindAsParallelGroupLists() {
    createWeightedChain();

    try (final ResultSet rs = database.query("opencypher",
        "MATCH (s:P {name:'A'}) ((m:P)-[r:KNOWS]->(n:P)){2} (t:P) RETURN m AS m, n AS n, r AS r, t.name AS name")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();

      assertThat(namesOfVertexList(row.getProperty("m"))).containsExactly("A", "B");
      assertThat(namesOfVertexList(row.getProperty("n"))).containsExactly("B", "C");

      final List<?> relationships = row.getProperty("r");
      assertThat(relationships).hasSize(2);
      assertThat(relationships).allMatch(Edge.class::isInstance);
      assertThat(((Edge) relationships.get(0)).getInteger("weight")).isEqualTo(10);
      assertThat(((Edge) relationships.get(1)).getInteger("weight")).isEqualTo(1);

      assertThat((String) row.getProperty("name")).isEqualTo("C");
      assertThat(rs.hasNext()).isFalse();
    }
  }

  // Issue #4531: a group variable is an ordinary list, so list functions apply to it.
  @Test
  void groupVariablesAreUsableByListFunctions() {
    createWeightedChain();

    try (final ResultSet rs = database.query("opencypher",
        "MATCH (s:P {name:'A'}) ((m:P)-[r:KNOWS]->(n:P)){2} (t:P) "
            + "RETURN size(r) AS hops, size(m) AS starts, head(m).name AS firstStart")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat(((Number) row.getProperty("hops")).intValue()).isEqualTo(2);
      // A node group variable is a list too - Phase A's lowering never bound one at all.
      assertThat(((Number) row.getProperty("starts")).intValue()).isEqualTo(2);
      assertThat((String) row.getProperty("firstStart")).isEqualTo("A");
    }
  }

  // Issue #4531: a {0,n} quantifier admits zero repetitions - both boundaries stay on the same vertex
  // and every group variable binds to an empty list.
  @Test
  void zeroRepetitionsBindEmptyGroupLists() {
    createWeightedChain();

    final List<String> ends = new ArrayList<>();
    int emptyGroups = 0;
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (s:P {name:'A'}) ((m:P)-[r:KNOWS]->(n:P)){0,1} (t:P) RETURN t.name AS name, m AS m, r AS r")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        ends.add(row.getProperty("name"));
        final List<?> nodes = row.getProperty("m");
        final List<?> relationships = row.getProperty("r");
        assertThat(nodes).hasSameSizeAs(relationships);
        if (nodes.isEmpty())
          emptyGroups++;
      }
    }

    assertThat(ends).containsExactlyInAnyOrder("A", "B");
    assertThat(emptyGroups).isEqualTo(1);
  }

  // ---------------------------------------------------------------------------
  // 4. Grouped path assignment: LIST<PATH>
  // ---------------------------------------------------------------------------

  // Issue #4531: a path variable bound to nothing but a quantified group yields one path per repetition
  // (ISO/IEC 39075 §15.4), not a single concatenated path.
  @Test
  void groupedPathAssignmentYieldsOnePathPerRepetition() {
    createWeightedChain();

    final List<String> starts = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher",
        "MATCH p = ((m:P)-[r:KNOWS]->(n:P)){2} RETURN p AS p")) {
      while (rs.hasNext()) {
        final List<?> paths = rs.next().getProperty("p");
        assertThat(paths).hasSize(2);
        assertThat(paths).allMatch(TraversalPath.class::isInstance);
        // Each element is one repetition of the inner pattern: exactly one relationship long,
        // and consecutive elements are joined end-to-start rather than concatenated into one path.
        final TraversalPath first = (TraversalPath) paths.get(0);
        final TraversalPath second = (TraversalPath) paths.get(1);
        assertThat(first.getEdges()).hasSize(1);
        assertThat(second.getEdges()).hasSize(1);
        assertThat(second.getVertices().get(0).getIdentity())
            .isEqualTo(first.getEndVertex().getIdentity());
        starts.add(first.getVertices().get(0).getString("name"));
      }
    }
    // Two-repetition walks exist from A (A->B->C) and from B (B->C->D); C has only one hop left.
    assertThat(starts).containsExactlyInAnyOrder("A", "B");
  }

  // ---------------------------------------------------------------------------
  // 5. Relationship isomorphism and termination
  // ---------------------------------------------------------------------------

  // Issue #4531: no relationship is traversed twice inside a group, which is also what stops an
  // open-ended quantifier from looping forever on a cycle.
  @Test
  void relationshipIsomorphismTerminatesAnOpenEndedGroupOnACycle() {
    database.transaction(() -> database.command("opencypher",
        "CREATE (a:P {name:'A'})-[:KNOWS]->(b:P {name:'B'})-[:KNOWS]->(a)"));

    // A->B (one repetition) and A->B->A (two); a third would have to reuse the A->B edge.
    assertThat(namesOf("MATCH (s:P {name:'A'}) ((m:P)-[r:KNOWS]->(n:P))+ (t:P) RETURN t.name AS name"))
        .containsExactlyInAnyOrder("A", "B");
  }

  // ---------------------------------------------------------------------------
  // 6. Plan shape and remaining rejections
  // ---------------------------------------------------------------------------

  // Issue #4531: a Phase B group is planned as a QuantifiedPathStep on the legacy pipeline. A Phase A
  // group - one relationship, no inner WHERE, and endpoints that constrain nothing - keeps its
  // variable-length lowering.
  @Test
  void quantifiedGroupPlansAsAQuantifiedPathStep() {
    createWeightedChain();

    assertThat(explainOf("MATCH (s:P {name:'A'}) ((m:P)-[r:KNOWS]->(n:P))+ (t:P) RETURN t.name AS name"))
        .contains("QUANTIFIED PATH");
    assertThat(explainOf("MATCH (s:P {name:'A'}) (()-[:KNOWS]->())+ (t:P) RETURN t.name AS name"))
        .doesNotContain("QUANTIFIED PATH");
  }

  // Issue #4531: the Phase A lowering still applies where it is exact, and both spellings agree.
  @Test
  void phaseALoweringStillAgreesWithThePhaseBOperator() {
    createWeightedChain();

    assertThat(namesOf("MATCH (s:P {name:'A'}) (()-[:KNOWS]->())+ (t:P) RETURN t.name AS name"))
        .containsExactlyInAnyOrderElementsOf(
            namesOf("MATCH (s:P {name:'A'}) ((m:P)-[r:KNOWS]->(n:P))+ (t:P) RETURN t.name AS name"));
  }

  // Issue #4531: a zero upper bound is still a syntax error, in both the Phase A and Phase B shapes.
  @Test
  void zeroQuantifierIsStillRejected() {
    createWeightedChain();

    assertThatThrownBy(() -> database.query("opencypher",
        "MATCH (s:P {name:'A'}) ((m:P)-[r:KNOWS]->(n:P)){0} (t:P) RETURN t"))
        .isInstanceOf(CommandParsingException.class);
  }

  // Issue #4531: nesting one quantified group inside another is reported, not silently mis-executed.
  @Test
  void nestedQuantifiedGroupIsReported() {
    createWeightedChain();

    assertThatThrownBy(() -> database.query("opencypher",
        "MATCH (s:P {name:'A'}) (((m:P)-[r:KNOWS]->(n:P))+ (o:P)-[:KNOWS]->(q:P))+ (t:P) RETURN t"))
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("FeatureNotImplemented");
  }

  // Issue #4531: a variable-length relationship inside a group is reported rather than mis-executed.
  @Test
  void variableLengthRelationshipInsideAGroupIsReported() {
    createWeightedChain();

    assertThatThrownBy(() -> database.query("opencypher",
        "MATCH (s:P {name:'A'}) ((m:P)-[r:KNOWS*1..2]->(n:P)-[:KNOWS]->(o:P))+ (t:P) RETURN t"))
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("FeatureNotImplemented");
  }

  // Issue #4531: a whole-map parameter inside a group is reported rather than silently dropped.
  @Test
  void parameterisedPropertyMapInsideAGroupIsReported() {
    createWeightedChain();

    assertThatThrownBy(() -> database.query("opencypher",
        "MATCH (s:P {name:'A'}) ((m:P $props)-[r:KNOWS]->(n:P))+ (t:P) RETURN t",
        Map.of("props", Map.of("name", "A"))))
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("FeatureNotImplemented");
  }

  // ---------------------------------------------------------------------------
  // 7. Interaction with the enclosing pattern
  // ---------------------------------------------------------------------------

  // Issue #4531: the right boundary node's own labels and properties still filter the group's endpoint.
  @Test
  void rightBoundaryConstraintsFilterTheGroupEndpoint() {
    createWeightedChain();

    try (final ResultSet rs = database.query("opencypher",
        "MATCH (s:P {name:'A'}) ((m:P)-[r:KNOWS]->(n:P))+ (t:P {name:'C'}) RETURN size(r) AS hops")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(((Number) rs.next().getProperty("hops")).intValue()).isEqualTo(2);
      assertThat(rs.hasNext()).isFalse();
    }
  }

  // Issue #4531: a group can start from a variable an earlier clause already bound.
  @Test
  void groupStartsFromAVariableBoundByAnEarlierClause() {
    createWeightedChain();

    assertThat(namesOf("MATCH (s:P {name:'A'}) WITH s "
        + "MATCH (s) ((m:P)-[r:KNOWS]->(n:P)){1} (t:P) RETURN t.name AS name"))
        .containsExactly("B");
  }

  // Issue #4531: a relationship the same MATCH already bound outside the group cannot be reused inside it.
  @Test
  void groupCannotReuseARelationshipBoundElsewhereInTheSameMatch() {
    database.transaction(() -> database.command("opencypher",
        "CREATE (a:P {name:'A'})-[:KNOWS]->(b:P {name:'B'})-[:KNOWS]->(a)"));

    // The first hop consumes A->B, so the group starting at B can only take B->A, and never A->B again.
    assertThat(namesOf("MATCH (s:P {name:'A'})-[e:KNOWS]->(mid:P) ((m:P)-[r:KNOWS]->(n:P))+ (t:P) "
        + "RETURN t.name AS name"))
        .containsExactly("A");
  }

  // Issue #4531: a node's own inline WHERE inside a group sees the variables the repetition has
  // already bound, not only the node's own - the same scope the group-level WHERE gets.
  @Test
  void innerNodeInlineWhereSeesEarlierBindingsOfTheSameRepetition() {
    createWeightedChain();

    // n is reached over r, so r is bound when n's inline WHERE runs. A-[10]->B passes, B-[1]->C does not.
    assertThat(namesOf("MATCH (s:P {name:'A'}) ((m:P)-[r:KNOWS]->(n:P WHERE r.weight > 5))+ (t:P) "
        + "RETURN t.name AS name"))
        .containsExactly("B");

    // Referring to the repetition's start node from the end node's inline WHERE.
    assertThat(namesOf("MATCH (s:P {name:'A'}) ((m:P)-[r:KNOWS]->(n:P WHERE n.name <> m.name))+ (t:P) "
        + "RETURN t.name AS name"))
        .containsExactlyInAnyOrder("B", "C", "D");
  }

  // Issue #4531: a quantified group works inside an EXISTS { MATCH ... } subquery, which plans a real
  // execution plan (unlike the inline pattern predicate, which reports it).
  @Test
  void quantifiedGroupWorksInsideExistsSubquery() {
    createWeightedChain();

    assertThat(namesOf("MATCH (s:P) WHERE EXISTS { MATCH (s) ((m:P)-[r:KNOWS]->(n:P)){3} (t:P) } "
        + "RETURN s.name AS name"))
        .containsExactly("A");
  }

  // Issue #4531: the inline pattern-predicate spelling cannot express a group at all - the grammar's
  // pathPatternNonEmpty admits no parenthesized path - so it is refused at parse time rather than
  // silently answered as if the group were one untyped variable-length hop. EXISTS { MATCH ... } is the
  // supported spelling, covered above.
  @Test
  void inlinePatternPredicateCannotExpressAQuantifiedGroup() {
    createWeightedChain();

    assertThatThrownBy(() -> namesOf("MATCH (s:P) WHERE (s) ((m:P)-[r:KNOWS]->(n:P))+ (t:P) RETURN s.name AS name"))
        .isInstanceOf(CommandParsingException.class);
  }

  // Issue #4531: an inner hop may be written right-to-left or undirected, and may list several types.
  @Test
  void innerHopHonoursDirectionAndTypeAlternatives() {
    createAlternatingChain();

    // A reversed two-hop inner unit walks the chain backwards from E: one repetition reaches C, two reach A.
    assertThat(namesOf("MATCH (s:P {name:'E'}) ((m:P)<-[:R2]-(y:P)<-[:R1]-(n:P))+ (t:P) RETURN t.name AS name"))
        .containsExactlyInAnyOrder("C", "A");

    // A type alternative makes the single-hop unit walk the whole mixed-type chain.
    assertThat(namesOf("MATCH (s:P {name:'A'}) ((m:P)-[r:R1|R2]->(n:P))+ (t:P) RETURN t.name AS name"))
        .containsExactlyInAnyOrder("B", "C", "D", "E");

    // An undirected inner hop can also travel against the arrows.
    assertThat(namesOf("MATCH (s:P {name:'C'}) ((m:P)-[r:R1|R2]-(n:P)){1} (t:P) RETURN t.name AS name"))
        .containsExactlyInAnyOrder("B", "D");
  }

  /**
   * Issue #4531: the repetition search recurses once per repetition, so a long chain walks the Java call
   * stack. Pins that a deep but realistic group does not overflow it, since nothing else in the suite
   * would notice the depth budget shrinking. Tagged slow: it builds and walks a 2,000-vertex chain.
   */
  @Test
  @Tag("slow")
  void deepRepetitionDoesNotExhaustTheCallStack() {
    final int chainLength = 2000;
    database.transaction(() -> {
      final StringBuilder create = new StringBuilder("CREATE (v0:P {name:'v0'})");
      for (int i = 1; i < chainLength; i++)
        create.append("-[:KNOWS]->(v").append(i).append(":P {name:'v").append(i).append("'})");
      database.command("opencypher", create.toString());
    });

    try (final ResultSet rs = database.query("opencypher",
        "MATCH (s:P {name:'v0'}) ((m:P)-[r:KNOWS]->(n:P))+ (t:P) RETURN count(t) AS reached")) {
      assertThat(rs.hasNext()).isTrue();
      // Every vertex downstream of v0 is reachable, at one repetition per hop.
      assertThat(((Number) rs.next().getProperty("reached")).intValue()).isEqualTo(chainLength - 1);
    }
  }

  // Issue #4531: Phase A may only claim a group whose endpoints constrain nothing. A label written on an
  // inner endpoint holds for EVERY repetition, including the vertices between two repetitions, which a
  // plain variable-length relationship cannot express - lowering it would silently drop the label.
  @Test
  void innerEndpointLabelsAreNotDroppedByThePhaseALowering() {
    database.getSchema().createVertexType("Q");
    database.transaction(() -> database.command("opencypher",
        "CREATE (a:P {name:'A'})-[:KNOWS]->(x:Q {name:'X'})-[:KNOWS]->(b:P {name:'B'})"));

    // Every repetition must start and end on a :P, and the only route out of A goes through a :Q.
    assertThat(namesOf("MATCH (s:P {name:'A'}) ((:P)-[:KNOWS]->(:P))+ (t:P) RETURN t.name AS name"))
        .isEmpty();
    // An inline property map on an endpoint is the same kind of per-repetition constraint.
    assertThat(namesOf("MATCH (s:P {name:'A'}) ((:P {name:'A'})-[:KNOWS]->(:P))+ (t:P) RETURN t.name AS name"))
        .isEmpty();

    // The unconstrained spelling is the one Phase A still lowers, and it does reach B over the :Q.
    assertThat(namesOf("MATCH (s:P {name:'A'}) (()-[:KNOWS]->())+ (t:P) RETURN t.name AS name"))
        .containsExactly("B");
  }

  // Issue #4531: the step consumes every input row, not just the first batch its previous step returns.
  @Test
  void everyInputRowIsExpanded() {
    final int pairs = 500;
    database.transaction(() -> {
      final StringBuilder create = new StringBuilder();
      for (int i = 0; i < pairs; i++)
        create.append("CREATE (:P {name:'s").append(i).append("'})-[:KNOWS]->(:P {name:'e").append(i).append("'})\n");
      for (final String statement : create.toString().split("\n"))
        database.command("opencypher", statement);
    });

    try (final ResultSet rs = database.query("opencypher",
        "MATCH (s:P) ((m:P)-[r:KNOWS]->(n:P)){1} (t:P) RETURN count(t) AS reached")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(((Number) rs.next().getProperty("reached")).intValue()).isEqualTo(pairs);
    }
  }

  // ---------------------------------------------------------------------------
  // helpers
  // ---------------------------------------------------------------------------

  private List<String> namesOf(final String query) {
    final List<String> names = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext())
        names.add(rs.next().getProperty("name"));
    }
    return names;
  }

  private static List<String> namesOfVertexList(final Object value) {
    assertThat(value).isInstanceOf(List.class);
    final List<String> names = new ArrayList<>();
    for (final Object item : (List<?>) value) {
      assertThat(item).isInstanceOf(Vertex.class);
      names.add(((Vertex) item).getString("name"));
    }
    return names;
  }

  /** The rendered execution plan of {@code EXPLAIN <query>}. */
  private String explainOf(final String query) {
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query)) {
      assertThat(rs.hasNext()).as(query).isTrue();
      return rs.next().getProperty("executionPlanAsString");
    }
  }
}
