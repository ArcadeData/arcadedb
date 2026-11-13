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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.query.opencypher.ast.CorrelatedSubqueryRewriter;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * CALL { } subqueries: scope, leading clauses, update guards, ORDER BY/LIMIT, edge labels and evaluation order.
 * <p>
 * Each nested class holds the regression tests of the GitHub issue its name carries.
 */
class CypherCallSubqueryTest {
  /**
   * Regression guard for issue #5005: CALL subquery with ORDER BY ... LIMIT must return the
   * correct (minimum) value, matching Neo4j semantics. The variable-length path endpoint bound
   * via the importing WITH must remain constrained even when ORDER BY + LIMIT are present.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue5005SubqueryOrderByLimit {
    private Database database;

    @BeforeEach
    void setUp() {
      database = new DatabaseFactory("./target/databases/testopencypher-issue5005").create();
      database.command("opencypher",
          "CREATE (alice:Person {name: 'Alice'}), (bob:Person {name: 'Bob'}), "
              + "(carol:Person {name: 'Carol'}), (dave:Person {name: 'Dave'}), "
              + "(eve:Person {name: 'Eve'}), "
              + "(alice)-[:FOLLOWS]->(bob), "
              + "(bob)-[:FOLLOWS]->(carol), "
              + "(alice)-[:FOLLOWS]->(dave), "
              + "(dave)-[:FOLLOWS]->(carol), "
              + "(carol)-[:FOLLOWS]->(eve)");
    }

    @AfterEach
    void tearDown() {
      if (database != null) {
        database.drop();
        database = null;
      }
    }

    private long single(final String query, final String column) {
      try (final ResultSet rs = database.query("opencypher", query)) {
        assertThat(rs.hasNext()).isTrue();
        final Result row = rs.next();
        return ((Number) row.getProperty(column)).longValue();
      }
    }

    @Test
    void directOrderByLimit() {
      final long len = single(
          "MATCH (a:Person {name: 'Alice'}), (c:Person {name: 'Carol'}) "
              + "MATCH p = (a)-[:FOLLOWS*1..4]->(c) "
              + "RETURN length(p) AS len ORDER BY len LIMIT 1", "len");
      assertThat(len).isEqualTo(2);
    }

    @Test
    void subqueryMinAggregation() {
      final long distance = single(
          "MATCH (a:Person {name: 'Alice'}), (c:Person {name: 'Carol'}) "
              + "CALL { WITH a, c MATCH p = (a)-[:FOLLOWS*1..4]->(c) RETURN p, length(p) AS len } "
              + "RETURN min(len) AS distance", "distance");
      assertThat(distance).isEqualTo(2);
    }

    @Test
    void subqueryOrderByLimit() {
      final long distance = single(
          "MATCH (a:Person {name: 'Alice'}), (c:Person {name: 'Carol'}) "
              + "CALL { WITH a, c MATCH p = (a)-[:FOLLOWS*1..4]->(c) RETURN length(p) AS len ORDER BY len LIMIT 1 } "
              + "RETURN len AS distance", "distance");
      assertThat(distance).isEqualTo(2);
    }
  }

  /**
   * Regression tests for issue #5213: a nested {@code CALL} subquery that references a variable
   * from an outer scope without importing it (neither via a scope list {@code CALL (v) { ... }}
   * nor via an importing {@code WITH}) must be rejected with an undefined-variable scope error,
   * matching Neo4j ({@code Variable `b` not defined}) instead of silently binding the variable
   * to {@code null}.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue5213NestedCallScope extends TestHelper {
    @Override
    protected void beginTest() {
      database.command("opencypher", "CREATE (a:C1 {id: 1}), (b:C2 {v: 21}), (a)-[:R]->(b)");
    }

    /**
     * The reporter's failing query: the inner {@code CALL} references {@code b} from the enclosing
     * subquery without importing it. Neo4j raises "Variable `b` not defined".
     */
    @Test
    void nestedCallReferencingUnimportedOuterVariableThrows() {
      assertThatThrownBy(() -> database.query("opencypher",
          """
          MATCH (a:C1 {id: 1}) \
          CALL (a) { \
            MATCH (a)-[:R]->(b:C2) \
            CALL { \
              RETURN b.v * 2 AS doubled \
            } \
            RETURN doubled \
          } \
          RETURN doubled""").close())
          .isInstanceOf(CommandParsingException.class)
          .hasMessageContaining("b");
    }

    /**
     * Top-level (non-nested) variant: a plain {@code CALL} referencing an unimported outer variable
     * must also be rejected.
     */
    @Test
    void topLevelCallReferencingUnimportedOuterVariableThrows() {
      assertThatThrownBy(() -> database.query("opencypher",
          "MATCH (a:C1 {id: 1}) CALL { RETURN a.id AS x } RETURN x").close())
          .isInstanceOf(CommandParsingException.class)
          .hasMessageContaining("a");
    }

    /** Control: importing the variable via the scope list makes the nested CALL valid. */
    @Test
    void nestedCallWithScopeImportIsValid() {
      final ResultSet rs = database.query("opencypher",
          """
          MATCH (a:C1 {id: 1}) \
          CALL (a) { \
            MATCH (a)-[:R]->(b:C2) \
            CALL (b) { \
              RETURN b.v * 2 AS doubled \
            } \
            RETURN doubled \
          } \
          RETURN doubled""");
      assertThat(rs.hasNext()).isTrue();
      assertThat(((Number) rs.next().getProperty("doubled")).intValue()).isEqualTo(42);
      assertThat(rs.hasNext()).isFalse();
    }

    /** Control: importing the variable via an importing WITH makes the nested CALL valid. */
    @Test
    void nestedCallWithImportingWithIsValid() {
      final ResultSet rs = database.query("opencypher",
          """
          MATCH (a:C1 {id: 1}) \
          CALL (a) { \
            MATCH (a)-[:R]->(b:C2) \
            CALL { \
              WITH b \
              RETURN b.v * 2 AS doubled \
            } \
            RETURN doubled \
          } \
          RETURN doubled""");
      assertThat(rs.hasNext()).isTrue();
      assertThat(((Number) rs.next().getProperty("doubled")).intValue()).isEqualTo(42);
      assertThat(rs.hasNext()).isFalse();
    }

    /** Control: a fully self-contained inner CALL (binds its own variables) stays valid. */
    @Test
    void selfContainedInnerCallIsValid() {
      final ResultSet rs = database.query("opencypher",
          """
          MATCH (a:C1 {id: 1}) \
          CALL (a) { \
            MATCH (a)-[:R]->(b:C2) \
            CALL { \
              MATCH (c:C2) \
              RETURN c.v AS val \
            } \
            RETURN b.v + val AS total \
          } \
          RETURN total""");
      assertThat(rs.hasNext()).isTrue();
      assertThat(((Number) rs.next().getProperty("total")).intValue()).isEqualTo(42);
      assertThat(rs.hasNext()).isFalse();
    }
  }

  /**
   * Regression tests for issue #5226: in the new scoped subquery syntax
   * {@code CALL () { ... }} / {@code CALL (vars) { ... }}, an inner
   * {@code MATCH (n:Label)} must match only vertices. When the label name collides with an
   * existing edge type, the O(1) type-count optimization ({@code TypeCountStep}) wrongly counted
   * edges, so {@code count(n)} returned the edge count instead of {@code 0}. Labels and
   * relationship types are separate namespaces in Cypher; Neo4j returns {@code 0} for all variants
   * below.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue5226CallMatchEdgeLabel extends TestHelper {
    @Override
    protected void beginTest() {
      database.command("opencypher", "CREATE (a:A {v: 1}), (b:B {v: 2}), (a)-[:EdgeLabel]->(b)");
    }

    private long count(final String cypher) {
      try (final ResultSet rs = database.query("opencypher", cypher)) {
        assertThat(rs.hasNext()).isTrue();
        final Result row = rs.next();
        return ((Number) row.getProperty("cnt")).longValue();
      }
    }

    /** Control: direct MATCH on an edge-type label matches no vertices. */
    @Test
    void directMatchOnEdgeLabelReturnsZero() {
      assertThat(count("MATCH (b:EdgeLabel) RETURN count(b) AS cnt")).isEqualTo(0);
    }

    /** Control: COUNT subquery on an edge-type label returns zero. */
    @Test
    void countSubqueryOnEdgeLabelReturnsZero() {
      assertThat(count("RETURN COUNT { MATCH (b:EdgeLabel) } AS cnt")).isEqualTo(0);
    }

    /** The reporter's failing query: {@code CALL ()} with zero imported variables. */
    @Test
    void scopedCallNoImportOnEdgeLabelReturnsZero() {
      assertThat(count(
          "CALL () { MATCH (b:EdgeLabel) RETURN count(b) AS cnt } RETURN cnt")).isEqualTo(0);
    }

    /** {@code CALL (a)} importing an outer variable, inner var name matches the count arg. */
    @Test
    void scopedCallWithImportOnEdgeLabelReturnsZero() {
      assertThat(count(
          "MATCH (a:A) CALL (a) { MATCH (b:EdgeLabel) RETURN count(b) AS cnt } RETURN cnt"))
          .isEqualTo(0);
    }

    /** Different inner variable name must not change the result. */
    @Test
    void scopedCallWithImportDifferentInnerVarReturnsZero() {
      assertThat(count(
          "MATCH (a:A) CALL (a) { MATCH (x:EdgeLabel) RETURN count(x) AS cnt } RETURN cnt"))
          .isEqualTo(0);
    }

    /** Control: deprecated {@code CALL { WITH ... }} syntax already returns zero. */
    @Test
    void importingWithCallOnEdgeLabelReturnsZero() {
      assertThat(count(
          "MATCH (a:A) CALL { WITH a MATCH (b:EdgeLabel) RETURN count(b) AS cnt } RETURN cnt"))
          .isEqualTo(0);
    }

    /** Control: OPTIONAL MATCH in the scoped CALL already returns zero. */
    @Test
    void scopedCallOptionalMatchOnEdgeLabelReturnsZero() {
      assertThat(count(
          "MATCH (a:A) CALL (a) { OPTIONAL MATCH (b:EdgeLabel) RETURN count(b) AS cnt } RETURN cnt"))
          .isEqualTo(0);
    }

    /** The labels() probe confirms no vertex is matched inside the scoped CALL. */
    @Test
    void scopedCallMatchOnEdgeLabelYieldsNoRows() {
      try (final ResultSet rs = database.query("opencypher",
          "CALL () { MATCH (b:EdgeLabel) RETURN labels(b) AS lbls } RETURN lbls")) {
        assertThat(rs.hasNext()).isFalse();
      }
    }

    /** Sanity: the same optimization still counts real vertex labels correctly inside a scoped CALL. */
    @Test
    void scopedCallMatchOnVertexLabelCountsVertices() {
      assertThat(count(
          "CALL () { MATCH (n:A) RETURN count(n) AS cnt } RETURN cnt")).isEqualTo(1);
    }
  }

  /**
   * Regression tests for issue #5228: in the new scoped subquery syntax {@code CALL () { ... }} /
   * {@code CALL (vars) { ... }}, an inner {@code MATCH (b:Label)} must match only vertices, never
   * relationships. When the label name collides with an existing edge type, the reporter observed
   * that the pattern <em>materialized the edge entity</em> ({@code labels(b) = []},
   * {@code type(b) = "EdgeLabel"}) so {@code count(b)} returned the edge count instead of {@code 0}.
   * <p>
   * This is the same defect as issue #5226 (the O(1) type-count fast path counted edges) combined
   * with the general MATCH kind guard of issue #5194; both are fixed on main. Where the #5226 suite
   * asserts the aggregated {@code count(b)} and a {@code labels(b)} probe, this suite locks down the
   * reporter's remaining diagnostic angle: returning the raw entity {@code b} must yield <em>no rows
   * at all</em>, for every {@code CALL} variant reported. Labels and relationship types are separate
   * namespaces in Cypher; Neo4j returns no rows here.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue5228CallMatchEdgeLabelEntity extends TestHelper {
    @Override
    protected void beginTest() {
      // Two vertices (A, B) and one edge whose type name is "EdgeLabel"; zero vertices are labeled EdgeLabel.
      database.command("opencypher", "CREATE (a:A {v: 1}), (b:B {v: 2}), (a)-[:EdgeLabel]->(b)");
    }

    private int rowCount(final String cypher) {
      try (final ResultSet rs = database.query("opencypher", cypher)) {
        int rows = 0;
        while (rs.hasNext()) {
          rs.next();
          rows++;
        }
        return rows;
      }
    }

    private long count(final String cypher) {
      try (final ResultSet rs = database.query("opencypher", cypher)) {
        assertThat(rs.hasNext()).isTrue();
        final Result row = rs.next();
        return ((Number) row.getProperty("cnt")).longValue();
      }
    }

    /** Returning the raw entity must yield no rows (nothing to materialize). */
    @Test
    void scopedCallNoImportReturningEntityYieldsNoRows() {
      assertThat(rowCount("CALL () { MATCH (b:EdgeLabel) RETURN b AS b } RETURN b")).isEqualTo(0);
    }

    /** The reporter's diagnostic probe: labels() must not surface any relationship. */
    @Test
    void scopedCallNoImportLabelsProbeYieldsNoRows() {
      assertThat(rowCount(
          "CALL () { MATCH (b:EdgeLabel) RETURN labels(b) AS lbls } RETURN lbls")).isEqualTo(0);
    }

    /**
     * The other half of the reporter's probe, {@code type(b)}, asks a node for a relationship type and so is a type
     * error before the query runs - the answer it was written to produce cannot exist. It reads the same inside the
     * subquery as it does outside it: the clause an expression sits in has no bearing on whether the call is valid
     * (issue #5626).
     */
    @Test
    void typeProbeOnANodeIsRejectedInsideAndOutsideTheSubquery() {
      assertThatThrownBy(() -> rowCount("CALL () { MATCH (b:EdgeLabel) RETURN type(b) AS t } RETURN t"))
          .isInstanceOf(CommandParsingException.class)
          .hasMessageContaining("type() requires a relationship argument");
      assertThatThrownBy(() -> rowCount("MATCH (b:EdgeLabel) RETURN type(b) AS t"))
          .isInstanceOf(CommandParsingException.class)
          .hasMessageContaining("type() requires a relationship argument");
    }

    /** {@code CALL (a)} importing an outer variable, returning the entity. */
    @Test
    void scopedCallWithImportReturningEntityYieldsNoRows() {
      assertThat(rowCount(
          "MATCH (a:A) CALL (a) { MATCH (b:EdgeLabel) RETURN b AS b } RETURN b")).isEqualTo(0);
    }

    /** Renamed inner variable must not change the result. */
    @Test
    void scopedCallWithImportRenamedVarReturningEntityYieldsNoRows() {
      assertThat(rowCount(
          "MATCH (a:A) CALL (a) { MATCH (x:EdgeLabel) RETURN x AS x } RETURN x")).isEqualTo(0);
    }

    /** Sanity: a real vertex label still materializes vertices inside a scoped CALL. */
    @Test
    void scopedCallOnVertexLabelStillReturnsVertices() {
      assertThat(rowCount("CALL () { MATCH (n:A) RETURN n AS n } RETURN n")).isEqualTo(1);
      assertThat(count("CALL () { MATCH (n:A) RETURN count(n) AS cnt } RETURN cnt")).isEqualTo(1);
    }
  }

  /**
   * Regression tests for issue #5257: {@code CREATE} and {@code MERGE} patterns inside a {@code CALL}
   * subquery must obey the same variable-scope rules as {@code SET} and {@code DELETE}. Binding an outer
   * variable that was never imported used to silently mint fresh anonymous vertices, persist the edge
   * between those orphans, and still report {@code relationshipsCreated: 1}. Neo4j and Memgraph both
   * reject such queries.
   */
  @Nested
  class Issue5257CallSubqueryRelationshipScope extends TestHelper {
    @Override
    protected void beginTest() {
      database.command("opencypher", "CREATE (:A {id: 1}), (:B {id: 2})");
    }

    /** The reporter's case 1: CREATE of a relationship between two unimported outer variables. */
    @Test
    void createRelationshipWithUnimportedOuterVariablesThrows() {
      assertThatThrownBy(() -> database.command("opencypher",
          """
          MATCH (a:A {id: 1}), (b:B {id: 2}) \
          CALL { \
            CREATE (a)-[:R]->(b) \
            RETURN 1 AS ok \
          } \
          RETURN ok"""))
          .isInstanceOf(CommandParsingException.class)
          .hasMessageContaining("UndefinedVariable")
          .hasMessageContaining("a");

      assertNoOrphanWrites("R");
    }

    /** The reporter's case 2: MERGE has the same false-success behaviour. */
    @Test
    void mergeRelationshipWithUnimportedOuterVariablesThrows() {
      assertThatThrownBy(() -> database.command("opencypher",
          """
          MATCH (a:A {id: 1}), (b:B {id: 2}) \
          CALL { \
            MERGE (a)-[:MR]->(b) \
            RETURN 1 AS ok \
          } \
          RETURN ok"""))
          .isInstanceOf(CommandParsingException.class)
          .hasMessageContaining("UndefinedVariable")
          .hasMessageContaining("a");

      assertNoOrphanWrites("MR");
    }

    /** A partially importing WITH must still flag the endpoint it did not import. */
    @Test
    void createRelationshipWithPartiallyImportedOuterVariablesThrows() {
      assertThatThrownBy(() -> database.command("opencypher",
          """
          MATCH (a:A {id: 1}), (b:B {id: 2}) \
          CALL { \
            WITH a \
            CREATE (a)-[:R]->(b) \
            RETURN 1 AS ok \
          } \
          RETURN ok"""))
          .isInstanceOf(CommandParsingException.class)
          .hasMessageContaining("UndefinedVariable")
          .hasMessageContaining("b");

      assertNoOrphanWrites("R");
    }

    /** A relationship variable bound in the outer scope but not imported must also be rejected. */
    @Test
    void createWithUnimportedOuterRelationshipVariableThrows() {
      database.command("opencypher", "MATCH (a:A {id: 1}), (b:B {id: 2}) CREATE (a)-[:R]->(b)");

      assertThatThrownBy(() -> database.command("opencypher",
          """
          MATCH (a:A)-[r:R]->(b:B) \
          CALL { \
            CREATE (x:X)-[r:R2]->(y:Y) \
            RETURN 1 AS ok \
          } \
          RETURN ok"""))
          .isInstanceOf(CommandParsingException.class)
          .hasMessageContaining("UndefinedVariable")
          .hasMessageContaining("r");
    }

    /**
     * The guard is not limited to relationship patterns: a single-node CREATE re-binding an unimported
     * outer variable is the same shadowing violation and must be rejected too.
     */
    @Test
    void createSingleNodeWithUnimportedOuterVariableThrows() {
      assertThatThrownBy(() -> database.command("opencypher",
          """
          MATCH (a:A {id: 1}) \
          CALL { \
            CREATE (a:Shadow) \
            RETURN 1 AS ok \
          } \
          RETURN ok"""))
          .isInstanceOf(CommandParsingException.class)
          .hasMessageContaining("a");

      assertThat(countVertices()).isEqualTo(2);
    }

    /** Nested subqueries inherit the shadowed names of every enclosing scope. */
    @Test
    void nestedCallCreateRelationshipWithUnimportedOuterVariablesThrows() {
      assertThatThrownBy(() -> database.command("opencypher",
          """
          MATCH (a:A {id: 1}), (b:B {id: 2}) \
          CALL (a, b) { \
            CALL { \
              CREATE (a)-[:R]->(b) \
              RETURN 1 AS ok \
            } \
            RETURN ok \
          } \
          RETURN ok"""))
          .isInstanceOf(CommandParsingException.class)
          .hasMessageContaining("UndefinedVariable");

      assertNoOrphanWrites("R");
    }

    /** Control: an importing WITH makes the relationship CREATE valid and it really persists. */
    @Test
    void createRelationshipWithImportingWithPersists() {
      final ResultSet rs = database.command("opencypher",
          """
          MATCH (a:A {id: 1}), (b:B {id: 2}) \
          CALL { \
            WITH a, b \
            CREATE (a)-[:CR]->(b) \
            RETURN 1 AS ok \
          } \
          RETURN ok""");
      assertThat(rs.hasNext()).isTrue();
      rs.close();

      assertThat(countRelationships("CR")).isEqualTo(1);
      assertThat(countVertices()).isEqualTo(2);
    }

    /** Control: an explicit scope list makes the relationship MERGE valid and it really persists. */
    @Test
    void mergeRelationshipWithExplicitScopeListPersists() {
      final ResultSet rs = database.command("opencypher",
          """
          MATCH (a:A {id: 1}), (b:B {id: 2}) \
          CALL (a, b) { \
            MERGE (a)-[:MR]->(b) \
            RETURN 1 AS ok \
          } \
          RETURN ok""");
      assertThat(rs.hasNext()).isTrue();
      rs.close();

      assertThat(countRelationships("MR")).isEqualTo(1);
      assertThat(countVertices()).isEqualTo(2);
    }

    /** Control: a subquery that declares its own pattern variables stays valid. */
    @Test
    void selfContainedCreateInsideSubqueryPersists() {
      final ResultSet rs = database.command("opencypher",
          """
          MATCH (a:A {id: 1}) \
          CALL { \
            CREATE (x:X {id: 10})-[:XR]->(y:Y {id: 11}) \
            RETURN 1 AS ok \
          } \
          RETURN ok""");
      assertThat(rs.hasNext()).isTrue();
      rs.close();

      assertThat(countRelationships("XR")).isEqualTo(1);
    }

    /** Control: top-level CREATE of a fresh relationship pattern is unaffected. */
    @Test
    void topLevelCreateRelationshipPersists() {
      final ResultSet rs = database.command("opencypher",
          "MATCH (a:A {id: 1}), (b:B {id: 2}) CREATE (a)-[:TR]->(b) RETURN 1 AS ok");
      assertThat(rs.hasNext()).isTrue();
      rs.close();

      assertThat(countRelationships("TR")).isEqualTo(1);
      assertThat(countVertices()).isEqualTo(2);
    }

    private void assertNoOrphanWrites(final String relType) {
      assertThat(countRelationships(relType)).isZero();
      assertThat(countVertices()).isEqualTo(2);
    }

    private long countRelationships(final String relType) {
      // An absent type is the strongest form of "nothing was created"; querying it would throw.
      if (!database.getSchema().existsType(relType))
        return 0;

      try (final ResultSet rs = database.query("opencypher",
          "MATCH ()-[r:" + relType + "]->() RETURN count(r) AS cnt")) {
        return rs.hasNext() ? ((Number) rs.next().getProperty("cnt")).longValue() : 0;
      }
    }

    private long countVertices() {
      try (final ResultSet rs = database.query("opencypher", "MATCH (n) RETURN count(n) AS cnt")) {
        return ((Number) rs.next().getProperty("cnt")).longValue();
      }
    }
  }

  /**
   * Regression test for GitHub issue #5461.
   * <p>
   * {@code COUNT { }} and {@code EXISTS { }} bodies are re-executed as standalone queries, and a body
   * that is a bare pattern has to be wrapped into a {@code MATCH}. The wrapper used to recognize only
   * {@code MATCH}, {@code WITH} and {@code RETURN} as clause keywords, so a body opening with any other
   * clause - {@code UNWIND}, {@code CALL}, {@code OPTIONAL MATCH} - was mistaken for a pattern and
   * spliced into an unparseable {@code MATCH UNWIND [1,2] AS y RETURN y RETURN 1}. The parse failure was
   * absorbed into the expression's neutral value, so {@code COUNT} silently returned 0 and
   * {@code EXISTS} silently returned false.
   */
  @Nested
  class Issue5461SubqueryLeadingClause {
    private Database database;

    @BeforeEach
    void setUp() {
      database = new DatabaseFactory("./target/databases/issue-5461-subquery-leading-clause").create();
    }

    @AfterEach
    void tearDown() {
      if (database != null) {
        database.drop();
        database = null;
      }
    }

    private Object scalar(final String query) {
      try (final ResultSet rs = database.query("opencypher", query)) {
        assertThat(rs.hasNext()).isTrue();
        return rs.next().getProperty("v");
      }
    }

    private List<Result> rows(final String query) {
      final List<Result> collected = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher", query)) {
        while (rs.hasNext())
          collected.add(rs.next());
      }
      return collected;
    }

    @Test
    void countSubqueryStartingWithUnwind() {
      assertThat(scalar("RETURN COUNT { UNWIND [1, 2] AS y RETURN y } AS v")).isEqualTo(2L);
      assertThat(scalar("RETURN COUNT { UNWIND [1] AS y RETURN y } AS v")).isEqualTo(1L);
      assertThat(scalar("RETURN COUNT { UNWIND [1, 2, 3] AS y RETURN y } AS v")).isEqualTo(3L);
      assertThat(scalar("RETURN COUNT { UNWIND [] AS y RETURN y } AS v")).isEqualTo(0L);
    }

    @Test
    void countSubqueryStartingWithUnwindAndNoReturn() {
      assertThat(scalar("RETURN COUNT { UNWIND [1, 2] AS y } AS v")).isEqualTo(2L);
    }

    @Test
    void countSubqueryLeadingWithControlsStillWork() {
      assertThat(scalar("RETURN COUNT { WITH 1 AS dummy UNWIND [1, 2] AS y RETURN y } AS v")).isEqualTo(2L);
      assertThat(scalar("RETURN COUNT { RETURN 1 } AS v")).isEqualTo(1L);
    }

    @Test
    void countSubqueryStartingWithUnwindIsEvaluatedPerOuterRow() {
      final List<Result> results = rows(
          "UNWIND [1, 2, 3] AS x RETURN x, COUNT { UNWIND [1, 2] AS y RETURN y } AS c ORDER BY x");

      assertThat(results).hasSize(3);
      for (int i = 0; i < 3; i++) {
        assertThat(results.get(i).<Number>getProperty("x").intValue()).isEqualTo(i + 1);
        assertThat(results.get(i).<Number>getProperty("c").longValue()).as("count for outer row %d", i + 1).isEqualTo(2L);
      }
    }

    @Test
    void countSubqueryStartingWithCallOrOptionalMatch() {
      assertThat(scalar("RETURN COUNT { CALL { RETURN 1 AS z } RETURN z } AS v")).isEqualTo(1L);
      assertThat(scalar("RETURN COUNT { OPTIONAL MATCH (n:NoSuchLabel) RETURN n } AS v")).isEqualTo(1L);
    }

    @Test
    void existsSubqueryStartingWithUnwind() {
      assertThat(scalar("RETURN EXISTS { UNWIND [1, 2] AS y RETURN y } AS v")).isEqualTo(true);
      assertThat(scalar("RETURN EXISTS { UNWIND [] AS y RETURN y } AS v")).isEqualTo(false);
      assertThat(scalar("RETURN EXISTS { CALL { RETURN 1 AS z } RETURN z } AS v")).isEqualTo(true);
    }

    @Test
    void collectSubqueryStartingWithUnwind() {
      assertThat(scalar("RETURN COLLECT { UNWIND [1, 2] AS y RETURN y } AS v")).isEqualTo(List.of(1L, 2L));
    }

    @Test
    void returningCallSubqueryStartingWithUnwind() {
      final List<Result> results = rows(
          "UNWIND [1, 2, 3] AS x CALL { UNWIND [1, 2] AS y RETURN y } RETURN x, y ORDER BY x, y");

      assertThat(results).hasSize(6);
      final List<String> pairs = new ArrayList<>(6);
      for (final Result row : results)
        pairs.add(row.<Number>getProperty("x").intValue() + "," + row.<Number>getProperty("y").intValue());
      assertThat(pairs).containsExactly("1,1", "1,2", "2,1", "2,2", "3,1", "3,2");
    }

    @Test
    void clauseKeywordTestClassifiesBodiesWithoutThrowing() {
      // The clause-keyword test indexes the first character, so a blank body must not throw.
      assertThatCode(() -> CorrelatedSubqueryRewriter.startsWithClauseKeyword("")).doesNotThrowAnyException();
      assertThat(CorrelatedSubqueryRewriter.startsWithClauseKeyword("")).isFalse();
      assertThat(CorrelatedSubqueryRewriter.startsWithClauseKeyword("   ")).isFalse();

      assertThat(CorrelatedSubqueryRewriter.startsWithClauseKeyword("UNWIND [1] AS y RETURN y")).isTrue();
      assertThat(CorrelatedSubqueryRewriter.startsWithClauseKeyword("unwind [1] AS y RETURN y")).isTrue();
      assertThat(CorrelatedSubqueryRewriter.startsWithClauseKeyword("OPTIONAL MATCH (n) RETURN n")).isTrue();

      // Bare patterns must stay classified as patterns, including one bound to a keyword-prefixed name
      assertThat(CorrelatedSubqueryRewriter.startsWithClauseKeyword("(a)-[:KNOWS]->(b)")).isFalse();
      assertThat(CorrelatedSubqueryRewriter.startsWithClauseKeyword("matches = (a)-->(b)")).isFalse();
      assertThat(CorrelatedSubqueryRewriter.startsWithClauseKeyword("returns = (a)-->(b)")).isFalse();
    }

    @Test
    void correlatedSubqueryStartingWithUnwind() {
      database.transaction(() -> database.command("opencypher", "CREATE (:T {tags: ['a', 'b', 'c']})"));

      assertThat(scalar("MATCH (n:T) RETURN COUNT { UNWIND n.tags AS t RETURN t } AS v")).isEqualTo(3L);
      assertThat(scalar("MATCH (n:T) RETURN EXISTS { UNWIND n.tags AS t RETURN t } AS v")).isEqualTo(true);
      assertThat(scalar("MATCH (n:T) RETURN COLLECT { UNWIND n.tags AS t RETURN t } AS v")).isEqualTo(List.of("a", "b", "c"));
    }

    @Test
    void patternOnlySubqueriesAreStillWrappedIntoMatch() {
      database.transaction(() -> database.command("opencypher", "CREATE (a:P {n:'a'})-[:KNOWS]->(b:P {n:'b'})"));

      assertThat(scalar("MATCH (a:P {n:'a'}) RETURN COUNT { (a)-[:KNOWS]->(:P) } AS v")).isEqualTo(1L);
      assertThat(scalar("MATCH (a:P {n:'a'}) RETURN EXISTS { (a)-[:KNOWS]->(:P) } AS v")).isEqualTo(true);
      assertThat(scalar("MATCH (a:P {n:'b'}) RETURN EXISTS { (a)-[:KNOWS]->(:P) } AS v")).isEqualTo(false);
    }
  }

  /**
   * Regression test for GitHub issue #5541.
   * <p>
   * {@code COUNT { }}, {@code EXISTS { }} and {@code COLLECT { }} must reject a body that contains an
   * update clause. The guard used to be a plain {@code toUpperCase().contains("SET ")} scan, so any
   * body whose <em>text</em> mentioned an update keyword was rejected - including a keyword sitting
   * inside a string literal, where it is ordinary user data rather than a clause. A read-only filter
   * such as {@code WHERE n.name = 'SET x'} therefore failed to parse.
   */
  @Nested
  class Issue5541SubqueryUpdateClauseGuard {
    private Database database;

    @BeforeEach
    void setUp() {
      database = new DatabaseFactory("./target/databases/issue-5541-subquery-update-clause-guard").create();
      database.transaction(() -> database.command("opencypher",
          "CREATE (:T {name:'SET x'}), (:T {name:'CREATE z'}), (:T {name:'DELETE me'}), (:T {name:'plain'})"));
    }

    @AfterEach
    void tearDown() {
      if (database != null) {
        database.drop();
        database = null;
      }
    }

    private Object scalar(final String query) {
      try (final ResultSet rs = database.query("opencypher", query)) {
        assertThat(rs.hasNext()).isTrue();
        return rs.next().getProperty("v");
      }
    }

    @Test
    void countSubqueryWithUpdateKeywordInsideStringLiteral() {
      assertThat(scalar("RETURN COUNT { MATCH (n:T) WHERE n.name = 'SET x' RETURN n } AS v")).isEqualTo(1L);
      assertThat(scalar("RETURN COUNT { MATCH (n:T) WHERE n.name = 'CREATE z' RETURN n } AS v")).isEqualTo(1L);
      assertThat(scalar("RETURN COUNT { MATCH (n:T) WHERE n.name = 'DELETE me' RETURN n } AS v")).isEqualTo(1L);
    }

    @Test
    void countSubqueryWithUpdateKeywordAsSubstringOfLiteral() {
      // The literal need not start with the keyword for the naive scan to trip
      assertThat(scalar("RETURN COUNT { MATCH (n:T) WHERE n.name = 'a SET b' RETURN n } AS v")).isEqualTo(0L);
    }

    @Test
    void existsSubqueryWithUpdateKeywordInsideStringLiteral() {
      assertThat(scalar("RETURN EXISTS { MATCH (n:T) WHERE n.name = 'SET x' RETURN n } AS v")).isEqualTo(true);
      assertThat(scalar("RETURN EXISTS { MATCH (n:T) WHERE n.name = 'MERGE nothing' RETURN n } AS v")).isEqualTo(false);
    }

    @Test
    void collectSubqueryWithUpdateKeywordInsideStringLiteral() {
      assertThat(scalar("RETURN COLLECT { MATCH (n:T) WHERE n.name = 'SET x' RETURN n.name } AS v"))
          .isEqualTo(List.of("SET x"));
    }

    @Test
    void doubleQuotedAndBacktickQuotedTextIsAlsoData() {
      assertThat(scalar("RETURN COUNT { MATCH (n:T) WHERE n.name = \"REMOVE this\" RETURN n } AS v")).isEqualTo(0L);
      assertThat(scalar("RETURN COUNT { MATCH (n:T) WHERE n.`name` = 'SET x' RETURN n } AS v")).isEqualTo(1L);
    }

    @Test
    void genuineUpdateClausesAreStillRejected() {
      assertThatThrownBy(() -> database.query("opencypher", "MATCH (n:T) RETURN COUNT { MATCH (m:T) SET m.x = 1 RETURN m } AS v"))
          .hasMessageContaining("InvalidClauseComposition");
      assertThatThrownBy(() -> database.query("opencypher", "MATCH (n:T) RETURN EXISTS { MATCH (m:T) SET m.x = 1 } AS v"))
          .hasMessageContaining("InvalidClauseComposition");
      assertThatThrownBy(
          () -> database.query("opencypher", "MATCH (n:T) RETURN COLLECT { MATCH (m:T) SET m.x = 1 RETURN m } AS v"))
          .hasMessageContaining("InvalidClauseComposition");
      assertThatThrownBy(() -> database.query("opencypher", "RETURN COUNT { MATCH (m:T) DETACH DELETE m RETURN m } AS v"))
          .hasMessageContaining("InvalidClauseComposition");
      assertThatThrownBy(() -> database.query("opencypher", "RETURN COUNT { CREATE (m:T) RETURN m } AS v"))
          .hasMessageContaining("InvalidClauseComposition");
      assertThatThrownBy(() -> database.query("opencypher", "RETURN COUNT { MERGE (m:T {name:'q'}) RETURN m } AS v"))
          .hasMessageContaining("InvalidClauseComposition");
      assertThatThrownBy(() -> database.query("opencypher", "RETURN COUNT { MATCH (m:T) REMOVE m:T RETURN m } AS v"))
          .hasMessageContaining("InvalidClauseComposition");
    }

    /**
     * INSERT is a synonym of CREATE in this engine, so a body opening with it is a write. It used to
     * slip past the guard and reach the executor, which absorbed it into the expression's neutral
     * value - a silent 0 rather than an error. No row was ever written, but the answer was wrong.
     */
    @Test
    void insertIsRejectedAsAnUpdateClause() {
      assertThatThrownBy(() -> database.query("opencypher", "RETURN COUNT { INSERT (m:T {name:'x'}) RETURN m } AS v"))
          .hasMessageContaining("InvalidClauseComposition");
      assertThatThrownBy(() -> database.query("opencypher", "RETURN EXISTS { INSERT (m:T {name:'x'}) RETURN m } AS v"))
          .hasMessageContaining("InvalidClauseComposition");
      assertThatThrownBy(() -> database.query("opencypher", "RETURN COLLECT { INSERT (m:T {name:'x'}) RETURN m } AS v"))
          .hasMessageContaining("InvalidClauseComposition");
      assertThat(CorrelatedSubqueryRewriter.containsUpdateClause("INSERT (m:T)")).isTrue();

      // ... but INSERT inside a literal is still just data
      assertThat(scalar("RETURN COUNT { MATCH (n:T) WHERE n.name = 'INSERT here' RETURN n } AS v")).isEqualTo(0L);
      assertThat(scalar("MATCH (n:T) RETURN count(n) AS v")).isEqualTo(4L);
    }

    /**
     * A map key spelled like an update clause is data, not a clause. The grammar accepts
     * {@code MATCH (n:T {set : 1}) RETURN n} standalone, so the guard must accept it too.
     */
    @Test
    void mapKeyNamedLikeAnUpdateClauseIsNotAClause() {
      assertThat(scalar("RETURN COUNT { MATCH (n:T {set : 1}) RETURN n } AS v")).isEqualTo(0L);
      assertThat(scalar("RETURN COUNT { MATCH (n:T {create : 1}) RETURN n } AS v")).isEqualTo(0L);
      assertThat(scalar("RETURN EXISTS { MATCH (n:T {set : 1}) RETURN n } AS v")).isEqualTo(false);

      // The lookahead must not swallow a genuine clause that merely has extra whitespace
      assertThat(CorrelatedSubqueryRewriter.containsUpdateClause("MATCH (n)   SET   n.x = 1")).isTrue();
      assertThat(CorrelatedSubqueryRewriter.containsUpdateClause("MATCH (n) REMOVE n:T")).isTrue();
    }

    /**
     * None of the update keywords is reserved in this grammar - {@code RETURN 1 AS set} parses - so an
     * alias spelled like one must not be read as a clause. A scan that accepts any word boundary flags
     * it; the guard requires the keyword to be followed by whitespace or {@code (}, as a clause is.
     */
    @Test
    void anAliasSpelledLikeAnUpdateClauseIsNotAClause() {
      for (final String keyword : new String[] { "set", "create", "delete", "merge", "remove", "insert" }) {
        assertThat(scalar("RETURN COUNT { MATCH (n:T) RETURN 1 AS " + keyword + " } AS v"))
            .as("alias named %s", keyword).isEqualTo(4L);
        assertThat(CorrelatedSubqueryRewriter.containsUpdateClause("MATCH (n) RETURN 1 AS " + keyword))
            .as("alias named %s", keyword).isFalse();
        assertThat(CorrelatedSubqueryRewriter.containsUpdateClause("MATCH (n) RETURN 1 AS " + keyword + ", 2 AS b"))
            .as("alias named %s before a comma", keyword).isFalse();
      }

      // A space-less write is still a write, which the original trailing-space scan missed
      assertThat(CorrelatedSubqueryRewriter.containsUpdateClause("CREATE(n:T)")).isTrue();
      assertThat(CorrelatedSubqueryRewriter.containsUpdateClause("MATCH (n) DELETE n")).isTrue();
    }

    /**
     * The scan uppercases the body to compare against ASCII keywords, so it must not depend on the
     * default locale. In a Turkish locale {@code "insert".toUpperCase()} yields a dotted capital I,
     * which does not match {@code INSERT} and let a lowercase write through.
     */
    @Test
    void keywordMatchingIsIndependentOfTheDefaultLocale() {
      final Locale original = Locale.getDefault();
      try {
        Locale.setDefault(Locale.forLanguageTag("tr-TR"));
        assertThat(CorrelatedSubqueryRewriter.containsUpdateClause("insert (n:T)")).isTrue();
        assertThat(CorrelatedSubqueryRewriter.containsUpdateClause("match (n) set n.x = 1")).isTrue();
        assertThat(CorrelatedSubqueryRewriter.startsWithClauseKeyword("unwind [1] AS y RETURN y")).isTrue();
      } finally {
        Locale.setDefault(original);
      }
    }

    @Test
    void backtickIdentifiersEscapeByDoublingNotByBackslash() {
      // A doubled backtick closes and reopens the quote, so the SET after it is still inside data
      assertThat(CorrelatedSubqueryRewriter.containsUpdateClause("MATCH (n) WHERE n.`a``b SET c` = 1 RETURN n")).isFalse();
      // A backslash is not an escape inside a backtick identifier, so it must not swallow the closer
      assertThat(CorrelatedSubqueryRewriter.containsUpdateClause("MATCH (n) WHERE n.`a\\` = 1 SET n.x = 2")).isTrue();
    }

    @Test
    void rejectedUpdateSubqueryLeavesTheDataUntouched() {
      assertThatThrownBy(() -> database.query("opencypher", "RETURN COUNT { CREATE (m:T {name:'leaked'}) RETURN m } AS v"))
          .hasMessageContaining("InvalidClauseComposition");
      assertThat(scalar("MATCH (n:T) RETURN count(n) AS v")).isEqualTo(4L);
    }

    @Test
    void updateClauseTestSkipsLiteralsAndRespectsWordBoundaries() {
      assertThat(CorrelatedSubqueryRewriter.containsUpdateClause("MATCH (n) WHERE n.name = 'SET x' RETURN n")).isFalse();
      assertThat(CorrelatedSubqueryRewriter.containsUpdateClause("MATCH (n) WHERE n.name = \"CREATE z\" RETURN n")).isFalse();
      assertThat(CorrelatedSubqueryRewriter.containsUpdateClause("MATCH (n) WHERE n.`DELETE` = 1 RETURN n")).isFalse();
      assertThat(CorrelatedSubqueryRewriter.containsUpdateClause("MATCH (n) WHERE n.name = 'it\\'s SET' RETURN n")).isFalse();

      // Not clause keywords: property access, labels/types, and longer identifiers
      assertThat(CorrelatedSubqueryRewriter.containsUpdateClause("MATCH (n) RETURN n.createdAt")).isFalse();
      assertThat(CorrelatedSubqueryRewriter.containsUpdateClause("MATCH (n) RETURN n.set")).isFalse();
      assertThat(CorrelatedSubqueryRewriter.containsUpdateClause("MATCH (n)-[r:SET_BY]->(m) RETURN r")).isFalse();
      assertThat(CorrelatedSubqueryRewriter.containsUpdateClause("MATCH (n {create: 1}) RETURN n")).isFalse();
      // ... including when whitespace separates the key from its colon
      assertThat(CorrelatedSubqueryRewriter.containsUpdateClause("MATCH (n {set : 1}) RETURN n")).isFalse();
      assertThat(CorrelatedSubqueryRewriter.containsUpdateClause("MATCH (n {create  :  1}) RETURN n")).isFalse();

      // Genuine update clauses
      assertThat(CorrelatedSubqueryRewriter.containsUpdateClause("MATCH (n) SET n.x = 1")).isTrue();
      assertThat(CorrelatedSubqueryRewriter.containsUpdateClause("MATCH (n) WHERE n.name = 'SET x' SET n.x = 1")).isTrue();
      assertThat(CorrelatedSubqueryRewriter.containsUpdateClause("CREATE (n:T)")).isTrue();
      assertThat(CorrelatedSubqueryRewriter.containsUpdateClause("MATCH (n) DETACH DELETE n")).isTrue();
      assertThat(CorrelatedSubqueryRewriter.containsUpdateClause("MERGE (n:T {x: 1})")).isTrue();
      assertThat(CorrelatedSubqueryRewriter.containsUpdateClause("MATCH (n) REMOVE n:T")).isTrue();
      assertThat(CorrelatedSubqueryRewriter.containsUpdateClause("")).isFalse();
    }

    @Test
    void correlatedSubqueryWithUpdateKeywordInsideStringLiteral() {
      assertThat(scalar("MATCH (n:T {name:'plain'}) RETURN COUNT { MATCH (m:T) WHERE m.name = 'SET x' RETURN m } AS v"))
          .isEqualTo(1L);
    }
  }

  /**
   * Regression guard for issue #6665: on ArcadeDB 26.8.1, a read-only {@code CALL (...) { ... RETURN count(*) }}
   * subquery reported a different {@code count(*)} depending on whether its inner {@code MATCH}/{@code WHERE}
   * result flowed straight into {@code RETURN count(*)} or was first materialized through {@code collect()}/
   * {@code UNWIND} before the same {@code count(*)} - two forms that must be equivalent for a read-only subquery.
   * <p>
   * This exact repro (verbatim from the issue) no longer reproduces on this branch: both forms agree with each
   * other and with a ground-truth standalone count of the same pattern. This test pins that down so a future
   * regression in CALL-subquery / {@code count(*)} evaluation is caught.
   */
  @Nested
  class Issue6665CallSubqueryCountEquivalence {

    @Test
    void directCountAndMaterializedCountAgreeInsideCallSubquery() {
      final Database database = new DatabaseFactory("./target/databases/issue6665-call-count-equivalence").create();
      try {
        database.transaction(() -> database.command("cypher", """
            CREATE (oa:V {klist: [1167157462, -165975848, -164122097]}),
                   (ob:V {k10: 254370155}),
                   (oc:V),
                   (od:V {k2: "CILs3s"}),
                   (oe:V {k10: -866288821}),
                   (n0:V {k7: 1994572697, k2: "D5ZqpQB"}),
                   (n1:V),
                   (x:V {k10: -1192051958}),
                   (y:V {k9: "5sm", id: 85}),
                   (ob)-[:rt6 {k8: "G", k3: true}]->(oa),
                   (ob)-[:rt8 {k8: "nPemIkbA", k10: -1906562515}]->(oc),
                   (od)-[:rt9 {k7: -51638479}]->(oe),
                   (n1)-[:R]->(x)-[:rt8]->(y)-[:rt2 {k11: [1]}]->(n0)
            """));

        // Query A: the CALL subquery's inner MATCH/WHERE result flows straight into RETURN count(*).
        final String queryA = """
            MATCH DIFFERENT RELATIONSHIPS
              ({klist: [1167157462, -165975848, -164122097]})
                <-[r0:rt6 {k8: "G"}]-({k10: 254370155})
                -[r1:rt8 {k8: "nPemIkbA", k10: -1906562515}]->(),
              p0 = ({k2: "CILs3s"})-[:rt9 {k7: -51638479}]->({k10: -866288821})
            WHERE r0.k3 = true
            CALL (r0, r1, p0) {
              MATCH (n0 {k7: 1994572697, k2: "D5ZqpQB"}),
                    (n1)-[r3]->({k10: -1192051958})-[:rt8]->
                      (:V {k9: "5sm", id: 85})-[r2:rt2|rt10|rt9]->(n0)
              WHERE single(item IN r2.k11 WHERE item IS NOT NULL)
              RETURN count(*) AS __expr_count
            }
            RETURN __expr_count AS __v
            """;

        // Query B: identical, but the matched bindings are first materialized through collect()/UNWIND before the
        // same count(*) - a read-only subquery must report the same answer either way.
        final String queryB = """
            MATCH DIFFERENT RELATIONSHIPS
              ({klist: [1167157462, -165975848, -164122097]})
                <-[r0:rt6 {k8: "G"}]-({k10: 254370155})
                -[r1:rt8 {k8: "nPemIkbA", k10: -1906562515}]->(),
              p0 = ({k2: "CILs3s"})-[:rt9 {k7: -51638479}]->({k10: -866288821})
            WHERE r0.k3 = true
            CALL (r0, r1, p0) {
              MATCH (n0 {k7: 1994572697, k2: "D5ZqpQB"}),
                    (n1)-[r3]->({k10: -1192051958})-[:rt8]->
                      (:V {k9: "5sm", id: 85})-[r2:rt2|rt10|rt9]->(n0)
              WHERE single(item IN r2.k11 WHERE item IS NOT NULL)
              WITH {r0: r0, r1: r1, p0: p0, n0: n0, n1: n1, r3: r3, r2: r2} AS __row
              WITH collect(__row) AS __rows
              UNWIND __rows AS __layer_row
              RETURN count(*) AS __expr_count
            }
            RETURN __expr_count AS __v
            """;

        final long resultA;
        try (ResultSet rs = database.query("cypher", queryA)) {
          assertThat(rs.hasNext()).isTrue();
          resultA = ((Number) rs.next().getProperty("__v")).longValue();
        }
        final long resultB;
        try (ResultSet rs = database.query("cypher", queryB)) {
          assertThat(rs.hasNext()).isTrue();
          resultB = ((Number) rs.next().getProperty("__v")).longValue();
        }

        // Ground truth: the same inner pattern, matched standalone (bound to r0/r1/p0 via WITH instead of CALL) and
        // counted directly, independent of the CALL-subquery machinery under test.
        final String groundTruth = """
            MATCH DIFFERENT RELATIONSHIPS
              ({klist: [1167157462, -165975848, -164122097]})
                <-[r0:rt6 {k8: "G"}]-({k10: 254370155})
                -[r1:rt8 {k8: "nPemIkbA", k10: -1906562515}]->(),
              p0 = ({k2: "CILs3s"})-[:rt9 {k7: -51638479}]->({k10: -866288821})
            WHERE r0.k3 = true
            WITH r0, r1, p0
            MATCH (n0 {k7: 1994572697, k2: "D5ZqpQB"}),
                  (n1)-[r3]->({k10: -1192051958})-[:rt8]->
                    (:V {k9: "5sm", id: 85})-[r2:rt2|rt10|rt9]->(n0)
            WHERE single(item IN r2.k11 WHERE item IS NOT NULL)
            RETURN count(*) AS c
            """;
        final long groundTruthCount;
        try (ResultSet rs = database.query("cypher", groundTruth)) {
          assertThat(rs.hasNext()).isTrue();
          final Result row = rs.next();
          groundTruthCount = ((Number) row.getProperty("c")).longValue();
        }

        assertThat(groundTruthCount).as("the inner pattern matches exactly one row").isEqualTo(1L);
        assertThat(resultA).as("direct count(*) must match the ground-truth count").isEqualTo(groundTruthCount);
        assertThat(resultB).as("collect()/UNWIND-materialized count(*) must match the ground-truth count").isEqualTo(groundTruthCount);
        assertThat(resultA).as("direct and materialized count(*) must agree").isEqualTo(resultB);
      } finally {
        database.drop();
      }
    }
  }

  /**
   * Regression test for issues #9579 and #9580: an expensive subquery expression ({@code COUNT { }}) was evaluated although
   * a cheap sibling operand had already decided the result.
   * <ul>
   *   <li>#9580: {@code coalesce(null, 0, COUNT { ... })} evaluated the COUNT after the non-null constant {@code 0}.</li>
   *   <li>#9579: {@code COUNT { ... } > 0 AND cheap} ran the COUNT for rows the cheap operand rejects.</li>
   * </ul>
   * The "work" the COUNT would do is observed through a probe that fails when it runs: the body divides by zero for every
   * neighbour it visits, so a COUNT that was evaluated over a node with neighbours raises an error.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue9579And9580SubqueryEvaluationOrder {
    private Database database;

    @BeforeEach
    void setup() {
      final DatabaseFactory factory = new DatabaseFactory("./target/databases/issue9579-9580");
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
      database.transaction(() -> {
        database.command("sql", "CREATE VERTEX TYPE Anchor");
        database.command("sql", "CREATE PROPERTY Anchor.uid LONG");
        database.command("sql", "CREATE PROPERTY Anchor.keep BOOLEAN");
        database.command("sql", "CREATE VERTEX TYPE Leaf");
        database.command("sql", "CREATE PROPERTY Leaf.uid LONG");
        database.command("sql", "CREATE PROPERTY Leaf.mark BOOLEAN");
        database.command("sql", "CREATE EDGE TYPE EXPAND");
      });
      database.transaction(() -> {
        database.command("opencypher", "CREATE (:Anchor {uid: 1, keep: false})");
        database.command("opencypher", "CREATE (:Anchor {uid: 2, keep: true})-[:EXPAND]->(:Leaf {uid: 3, mark: true})");
        database.command("opencypher",
            "MATCH (u:Anchor {uid: 1}) UNWIND range(1, 50) AS i CREATE (u)-[:EXPAND]->(:Leaf {uid: 100 + i, mark: false})");
      });
    }

    @AfterEach
    void teardown() {
      if (database != null)
        database.drop();
    }

    // #9580
    @Test
    void coalesceStopsAtTheFirstNonNullArgument() {
      // Every anchor has at least one neighbour, so evaluating the COUNT would hit the division by zero.
      final List<Result> rows = collect("""
          MATCH (u:Anchor {uid: 1})
          RETURN coalesce(null, 0, COUNT { MATCH (u)-[:EXPAND]->(l:Leaf) WHERE l.uid / 0 > 0 }) AS c0""");

      assertThat(rows).hasSize(1);
      assertThat(((Number) rows.get(0).getProperty("c0")).longValue()).isZero();
    }

    @Test
    void coalesceStillEvaluatesLaterArgumentsWhenEarlierOnesAreNull() {
      final List<Result> rows = collect("""
          MATCH (u:Anchor {uid: 1})
          RETURN coalesce(null, u.missing, COUNT { MATCH (u)-[:EXPAND]->(l:Leaf) }) AS c0""");

      assertThat(rows).hasSize(1);
      assertThat(((Number) rows.get(0).getProperty("c0")).longValue()).isEqualTo(50L);
    }

    @Test
    void coalesceInWhereStopsAtTheFirstNonNullArgument() {
      final List<Result> rows = collect("""
          MATCH (u:Anchor {uid: 1})
          WHERE coalesce(u.uid, COUNT { MATCH (u)-[:EXPAND]->(l:Leaf) WHERE l.uid / 0 > 0 }) = 1
          RETURN u.uid AS c0""");

      assertThat(rows).hasSize(1);
    }

    // #9579
    @Test
    void cheapOperandOnTheRightOfAndIsEvaluatedBeforeTheCount() {
      final List<Result> rows = collect("""
          MATCH (u:Anchor)
          WHERE (COUNT { MATCH (u)-[:EXPAND]->(l:Leaf) WHERE (l.mark = false AND l.uid / 0 > 0) OR l.mark = true } > 0)
            AND (coalesce(u.keep, false) = true) AND u.uid = 2
          RETURN u.uid AS c0""");

      // uid 2 is the only anchor passing the cheap operands, and its only leaf has mark = true.
      assertThat(rows).hasSize(1);
      assertThat(((Number) rows.get(0).getProperty("c0")).longValue()).isEqualTo(2L);
    }

    @Test
    void bothOperandOrdersGiveTheSameRowsAsTheIssue() {
      final String count = "COUNT { MATCH (u)-[:EXPAND]->(l:Leaf) WHERE coalesce(l.mark, false) = true } > 0";
      final String cheap = "coalesce(u.keep, false) = true";

      final List<Result> q1 = collect("MATCH (u:Anchor) WHERE (" + cheap + ") AND (" + count + ") RETURN u.uid AS c0");
      final List<Result> q2 = collect("MATCH (u:Anchor) WHERE (" + count + ") AND (" + cheap + ") RETURN u.uid AS c0");

      assertThat(q1).hasSize(1);
      assertThat(q2).hasSize(1);
      assertThat(((Number) q1.get(0).getProperty("c0")).longValue()).isEqualTo(2L);
      assertThat(((Number) q2.get(0).getProperty("c0")).longValue()).isEqualTo(2L);
    }

    @Test
    void cheapTrueOperandOnTheRightOfOrSkipsTheCount() {
      final List<Result> rows = collect("""
          MATCH (u:Anchor {uid: 1})
          WHERE (COUNT { MATCH (u)-[:EXPAND]->(l:Leaf) WHERE l.uid / 0 > 0 } > 0) OR u.uid = 1
          RETURN u.uid AS c0""");

      assertThat(rows).hasSize(1);
    }

    @Test
    void returnedBooleanExpressionKeepsThreeValuedSemantics() {
      // keep is false: the cheap operand decides, regardless of what the COUNT would do.
      final List<Result> rows = collect("""
          MATCH (u:Anchor {uid: 1})
          RETURN (COUNT { MATCH (u)-[:EXPAND]->(l:Leaf) WHERE l.uid / 0 > 0 } > 0) AND (u.keep = true) AS c0""");

      assertThat(rows).hasSize(1);
      assertThat(rows.get(0).<Boolean>getProperty("c0")).isFalse();
    }

    @Test
    void costlyOperandOnTheLeftOfOrWithNullOrFalseCheapOperandStillEvaluatesIt() {
      // keep is null on the missing property: null OR <count result> must be decided by the COUNT, in both orders.
      final List<Result> rows = collect("""
          MATCH (u:Anchor)
          WHERE (COUNT { MATCH (u)-[:EXPAND]->(l:Leaf) WHERE l.mark = true } > 0) OR u.missing = 1
          RETURN u.uid AS c0""");
      final List<Result> reversed = collect("""
          MATCH (u:Anchor)
          WHERE u.missing = 1 OR (COUNT { MATCH (u)-[:EXPAND]->(l:Leaf) WHERE l.mark = true } > 0)
          RETURN u.uid AS c0""");

      assertThat(rows).hasSize(1);
      assertThat(((Number) rows.get(0).getProperty("c0")).longValue()).isEqualTo(2L);
      assertThat(reversed).hasSize(1);
      assertThat(((Number) reversed.get(0).getProperty("c0")).longValue()).isEqualTo(2L);
    }

    @Test
    void existsSubqueryAfterFalseCheapOperandIsNotEvaluated() {
      final List<Result> rows = collect("""
          MATCH (u:Anchor)
          WHERE EXISTS { MATCH (u)-[:EXPAND]->(l:Leaf) WHERE (l.mark = false AND l.uid / 0 > 0) OR l.mark = true }
            AND u.uid = 2
          RETURN u.uid AS c0""");

      assertThat(rows).hasSize(1);
      assertThat(((Number) rows.get(0).getProperty("c0")).longValue()).isEqualTo(2L);
    }

    @Test
    void patternPredicateAfterFalseCheapOperandKeepsTheSameRows() {
      final List<Result> rows = collect("""
          MATCH (u:Anchor)
          WHERE (u)-[:EXPAND]->(:Leaf {mark: true}) AND coalesce(u.keep, false) = true
          RETURN u.uid AS c0""");

      assertThat(rows).hasSize(1);
      assertThat(((Number) rows.get(0).getProperty("c0")).longValue()).isEqualTo(2L);
    }

    @Test
    void nullAndFalseIsFalseInBothOrdersWithACostlyOperand() {
      final List<Result> rows = collect("""
          MATCH (u:Anchor {uid: 2})
          RETURN (COUNT { MATCH (u)-[:EXPAND]->(l:Leaf) } > 0 AND null) AS a,
                 (null AND COUNT { MATCH (u)-[:EXPAND]->(l:Leaf) } > 0) AS b,
                 (COUNT { MATCH (u)-[:EXPAND]->(l:Leaf) } > 0 AND false) AS c,
                 (false AND COUNT { MATCH (u)-[:EXPAND]->(l:Leaf) } > 0) AS d,
                 (COUNT { MATCH (u)-[:EXPAND]->(l:Leaf) } > 0 OR null) AS e,
                 (null OR COUNT { MATCH (u)-[:EXPAND]->(l:Leaf) } > 0) AS f""");

      assertThat(rows).hasSize(1);
      final Result row = rows.get(0);
      assertThat(row.<Object>getProperty("a")).isNull();
      assertThat(row.<Object>getProperty("b")).isNull();
      assertThat(row.<Boolean>getProperty("c")).isFalse();
      assertThat(row.<Boolean>getProperty("d")).isFalse();
      assertThat(row.<Boolean>getProperty("e")).isTrue();
      assertThat(row.<Boolean>getProperty("f")).isTrue();
    }

    @Test
    void coalesceInAnAggregatingProjectionStopsAtTheFirstNonNullArgument() {
      // An aggregation in the projection routes evaluation through ExpressionEvaluator rather than the AST's own path.
      final List<Result> rows = collect("""
          MATCH (u:Anchor {uid: 1})
          RETURN count(u) AS n, coalesce(1, COUNT { MATCH (u)-[:EXPAND]->(l:Leaf) WHERE l.uid / 0 > 0 }) AS c0""");

      assertThat(rows).hasSize(1);
      assertThat(((Number) rows.get(0).getProperty("c0")).longValue()).isEqualTo(1L);
    }

    @Test
    void coalesceDoesNotEvaluateAnArgumentAfterTheFirstNonNullOne() {
      final List<Result> rows = collect("RETURN coalesce(1, 1 / 0) AS c0");

      assertThat(rows).hasSize(1);
      assertThat(((Number) rows.get(0).getProperty("c0")).longValue()).isEqualTo(1L);
    }

    @Test
    void nestedLogicalChainKeepsTheCheapOperandsFirst() {
      // (costly AND cheap-false) OR cheap-false: the COUNT must never run for anchor 1, which fails both cheap operands
      final List<Result> rows = collect("""
          MATCH (u:Anchor)
          WHERE ((COUNT { MATCH (u)-[:EXPAND]->(l:Leaf) WHERE (l.mark = false AND l.uid / 0 > 0) OR l.mark = true } > 0
                  AND u.keep = true) OR u.uid = 99)
          RETURN u.uid AS c0""");

      assertThat(rows).hasSize(1);
      assertThat(((Number) rows.get(0).getProperty("c0")).longValue()).isEqualTo(2L);
    }

    @Test
    void notOverACostlyOperandStillEvaluatesIt() {
      final List<Result> rows = collect("""
          MATCH (u:Anchor)
          WHERE NOT (COUNT { MATCH (u)-[:EXPAND]->(l:Leaf) WHERE l.mark = true } > 0) AND u.uid = 1
          RETURN u.uid AS c0""");

      assertThat(rows).hasSize(1);
      assertThat(((Number) rows.get(0).getProperty("c0")).longValue()).isEqualTo(1L);
    }

    private List<Result> collect(final String query) {
      final List<Result> rows = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher", query)) {
        while (rs.hasNext())
          rows.add(rs.next());
      }
      return rows;
    }
  }
}
