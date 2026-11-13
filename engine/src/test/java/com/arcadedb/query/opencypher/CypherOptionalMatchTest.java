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
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.traversal.TraversalPath;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * OPTIONAL MATCH: identity batches, zero-length paths, barriers before CALL and aggregation scope after UNWIND.
 * <p>
 * Each nested class holds the regression tests of the GitHub issue its name carries.
 */
class CypherOptionalMatchTest {
  /**
   * Reproduces issue #6378: {@code OPTIONAL MATCH p = ({k: 1}) RETURN p} raised
   * "ZeroLengthPathStep requires a previous step" instead of returning the matching node as a
   * zero-length path, even though the equivalent non-optional {@code MATCH p = ({k: 1}) RETURN p}
   * worked correctly.
   */
  @Nested
  class Issue6378OptionalMatchSingleNodeZeroLengthPath {
    private Database database;

    @BeforeEach
    void setUp() {
      database = new DatabaseFactory("./target/databases/testissue6378").create();
      database.getSchema().createVertexType("N");
      database.transaction(() -> database.newVertex("N").set("k", 1).save());
    }

    @AfterEach
    void tearDown() {
      if (database != null) {
        database.drop();
        database = null;
      }
    }

    @Test
    void controlMatchWithSingleNodeZeroLengthPathReturnsOneRow() {
      // Sanity check: the non-optional form from the issue must keep working.
      final ResultSet rs = database.query("opencypher", "MATCH p = ({k: 1}) RETURN p");

      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      final Object path = row.getProperty("p");
      assertThat(path).isInstanceOf(TraversalPath.class);
      assertThat(((TraversalPath) path).getStartVertex()).isNotNull();
      assertThat(rs.hasNext()).isFalse();
      rs.close();
    }

    @Test
    void standaloneOptionalMatchWithSingleNodeZeroLengthPathReturnsOneRow() {
      // Exact query from the issue report.
      final ResultSet rs = database.query("opencypher", "OPTIONAL MATCH p = ({k: 1}) RETURN p");

      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      final Object path = row.getProperty("p");
      assertThat(path).isInstanceOf(TraversalPath.class);

      final Vertex startNode = ((TraversalPath) path).getStartVertex();
      assertThat(startNode).isNotNull();
      assertThat((Integer) startNode.get("k")).isEqualTo(1);

      assertThat(rs.hasNext()).isFalse();
      rs.close();
    }

    @Test
    void standaloneOptionalMatchWithNoMatchReturnsNullPath() {
      // No vertex has k: 999, so OPTIONAL MATCH must fall back to a single NULL row rather than
      // throwing or returning zero rows.
      final ResultSet rs = database.query("opencypher", "OPTIONAL MATCH p = ({k: 999}) RETURN p");

      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat((Object) row.getProperty("p")).isNull();
      assertThat(rs.hasNext()).isFalse();
      rs.close();
    }

    @Test
    void chainedOptionalMatchWithSingleNodeZeroLengthPathReturnsOneRow() {
      // Covers the branch where matchChainStart was already set by an earlier (non-optional)
      // MATCH clause before the OPTIONAL MATCH single-node pattern is processed.
      final ResultSet rs = database.query("opencypher",
          "MATCH (n:N {k: 1}) OPTIONAL MATCH p = (n) RETURN p");

      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      final Object path = row.getProperty("p");
      assertThat(path).isInstanceOf(TraversalPath.class);
      assertThat((Integer) ((TraversalPath) path).getStartVertex().get("k")).isEqualTo(1);
      assertThat(rs.hasNext()).isFalse();
      rs.close();
    }
  }

  /**
   * Issue #6629: {@code OPTIONAL MATCH (x)-[:TYPE]->(y) ... WITH y, count(x) AS cnt} is fused by the planner into
   * {@code CountEdgesStep} (and, for two consecutive OPTIONAL MATCHes, {@code CountChainedEdgesStep}), which counts
   * edges directly instead of materializing the OPTIONAL MATCH rows. Both steps used to emit one output row per
   * INPUT row instead of aggregating, on the (previously unstated) assumption that exactly one input row reaches
   * them per distinct value of the bound vertex. UNWIND breaks that assumption - it fans a single bound vertex out
   * into several input rows - so the WITH aggregation boundary was silently lost: N UNWIND rows produced N output
   * rows, each carrying a per-row count, instead of one grouped row carrying the total.
   * <p>
   * Both steps now group their input rows by the WITH clause's non-aggregated (grouping-key) values and sum the
   * per-row edge count within each group, which is correct both with and without a row-multiplying clause upstream.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue6629UnwindOptionalMatchAggregationScope {
    private Database database;

    @BeforeEach
    void setUp() {
      database = new DatabaseFactory("./target/databases/testopencypher-unwind-optional-count-6629").create();
      database.transaction(() -> {
        database.getSchema().createVertexType("X");
        database.getSchema().createEdgeType("L");

        final MutableVertex x1 = database.newVertex("X").set("_id", "x1").save();
        final MutableVertex x2 = database.newVertex("X").set("_id", "x2").save();
        x1.newEdge("L", x2);
      });
    }

    @AfterEach
    void tearDown() {
      if (database != null)
        database.drop();
    }

    @Test
    void unwindThenOptionalMatchCollapsesToOneGroupWithSummedCount() {
      // The reported witness: UNWIND fans x1 out into 2 rows; each row's OPTIONAL MATCH finds the
      // same edge, so the WITH boundary must collapse them into ONE row with c=2 (matching Neo4j/
      // Memgraph/FalkorDB), not two rows with c=1 each.
      final ResultSet rs = database.query("opencypher",
          "MATCH (a:X {_id:'x1'}) UNWIND [1, 2] AS u OPTIONAL MATCH (a)-[:L]->(m:X) WITH a, count(m) AS c RETURN a, c");

      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat(row.<Long>getProperty("c")).isEqualTo(2L);
      assertThat(rs.hasNext()).isFalse();
      rs.close();
    }

    @Test
    void unwindThenOptionalMatchWithNoMatchesCollapsesToOneGroupWithZeroCount() {
      // Control 3 from the issue: zero optional matches must still be ONE group with c=0, not one
      // zero-count row per UNWIND element.
      final ResultSet rs = database.query("opencypher",
          "MATCH (a:X {_id:'x2'}) UNWIND [1, 2] AS u OPTIONAL MATCH (a)-[:L]->(m:X) WITH a, count(m) AS c RETURN a, c");

      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat(row.<Long>getProperty("c")).isEqualTo(0L);
      assertThat(rs.hasNext()).isFalse();
      rs.close();
    }

    @Test
    void optionalMatchWithoutUnwindStillAggregatesToOneRow() {
      // Control 1 from the issue: the same aggregation without UNWIND already collapsed correctly -
      // must keep doing so.
      final ResultSet rs = database.query("opencypher",
          "MATCH (a:X {_id:'x1'}) OPTIONAL MATCH (a)-[:L]->(m:X) WITH a, count(m) AS c RETURN a, c");

      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat(row.<Long>getProperty("c")).isEqualTo(1L);
      assertThat(rs.hasNext()).isFalse();
      rs.close();
    }

    @Test
    void unwindWithoutOptionalMatchStillAggregatesToOneRow() {
      // Control 2 from the issue: UNWIND followed by a plain count(*) (no OPTIONAL MATCH, so no
      // CountEdgesStep fusion at all) already collapsed correctly - must keep doing so.
      final ResultSet rs = database.query("opencypher",
          "MATCH (a:X {_id:'x1'}) UNWIND [1, 2] AS u WITH a, count(*) AS c RETURN a, c");

      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat(row.<Long>getProperty("c")).isEqualTo(2L);
      assertThat(rs.hasNext()).isFalse();
      rs.close();
    }

    @Test
    void unwindBeforeChainedOptionalMatchCountAggregatesAcrossUnwoundRows() {
      // Same collapse-of-grouping-boundary bug, but through CountChainedEdgesStep (two consecutive
      // OPTIONAL MATCH clauses fused into one chained edge count) rather than CountEdgesStep.
      database.transaction(() -> {
        database.getSchema().createVertexType("Question");
        database.getSchema().createVertexType("Answer");
        database.getSchema().createVertexType("Comment");
        database.getSchema().createEdgeType("HAS_ANSWER");
        database.getSchema().createEdgeType("HAS_COMMENT");

        final MutableVertex q1 = database.newVertex("Question").set("id", 1).save();
        final MutableVertex a1 = database.newVertex("Answer").set("id", 1).save();
        q1.newEdge("HAS_ANSWER", a1);
        for (int i = 0; i < 3; i++) {
          final MutableVertex c = database.newVertex("Comment").set("id", 100 + i).save();
          a1.newEdge("HAS_COMMENT", c);
        }
      });

      // UNWIND fans q1 out into 2 rows before the chained OPTIONAL MATCH; each row finds all 3
      // comments through the HAS_ANSWER -> HAS_COMMENT chain, so the total must be 2 * 3 = 6, in
      // ONE row per question.
      final ResultSet rs = database.query("opencypher",
          """
          MATCH (q:Question) UNWIND [1, 2] AS u \
          OPTIONAL MATCH (q)-[:HAS_ANSWER]->(a) \
          OPTIONAL MATCH (a)-[:HAS_COMMENT]->(x) \
          WITH q, count(x) AS cnt \
          RETURN q.id AS qid, cnt""");

      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat(row.<Integer>getProperty("qid")).isEqualTo(1);
      assertThat(row.<Long>getProperty("cnt")).isEqualTo(6L);
      assertThat(rs.hasNext()).isFalse();
      rs.close();
    }
  }

  /**
   * Issue #9205: an {@code OPTIONAL MATCH ... WHERE false WITH *} barrier in front of a {@code CALL (*)} subquery
   * changed the number of rows the query returned.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue9205OptionalMatchBarrierCallStar {
    private static final String PATH = "./target/databases/issue-9205";
    private Database database;

    @BeforeEach
    void setUp() {
      final DatabaseFactory factory = new DatabaseFactory(PATH);
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
      database.getSchema().createVertexType("Seed");
      database.getSchema().createVertexType("Person");
      database.getSchema().createVertexType("NoMatch");
      database.transaction(() -> database.command("opencypher", "CREATE (:Seed {id: 1})"));
    }

    @AfterEach
    void tearDown() {
      if (database != null && database.isOpen())
        database.drop();
    }

    private int count(final String query) {
      int rows = 0;
      database.begin();
      try (final ResultSet rs = database.command("opencypher", query)) {
        while (rs.hasNext()) {
          rs.next();
          rows++;
        }
      }
      database.commit();
      return rows;
    }

    private static final String CALL = """
        CALL (*) {
          OPTIONAL MATCH () WHERE EXISTS { MATCH (m) }
          RETURN 0 AS marker
        }
        RETURN alias0""";

    /** Control: this shape already passed before the fix. */
    @Test
    void callStarWithoutOptionalMatchBarrier() {
      assertThat(count("CREATE (alias0:Person) " + CALL)).isEqualTo(2);
    }

    @Test
    void callStarAfterOptionalMatchBarrier() {
      assertThat(count("CREATE (alias0:Person) OPTIONAL MATCH (:NoMatch) WHERE false WITH * " + CALL)).isEqualTo(2);
    }

    @Test
    void returnsTheCreatedNodeAndKeepsUserVariablesVisibleInsideTheBody() {
      database.begin();
      final List<Object[]> rows = new ArrayList<>();
      try (final ResultSet rs = database.command("opencypher", """
          CREATE (alias0:Person {name: 'p'})
          OPTIONAL MATCH (:NoMatch) WHERE false
          WITH *
          CALL (*) {
            OPTIONAL MATCH () WHERE EXISTS { MATCH (m) }
            RETURN alias0.name AS seen
          }
          RETURN alias0.name AS name, seen""")) {
        while (rs.hasNext()) {
          final Result r = rs.next();
          rows.add(new Object[] { r.getProperty("name"), r.getProperty("seen") });
        }
      }
      database.commit();
      assertThat(rows).hasSize(2);
      for (final Object[] row : rows) {
        assertThat(row[0]).isEqualTo("p");
        assertThat(row[1]).isEqualTo("p");
      }
    }

    @Test
    void generatedBindingsOfTheBodyDoNotReplaceTheOuterOnes() {
      // the outer MATCH () and the inner MATCH () both use generated names; the outer rows must survive the call
      assertThat(count("MATCH () WITH * CALL (*) { MATCH () RETURN 1 AS x } RETURN x")).isEqualTo(1);
      database.begin();
      try (final ResultSet rs = database.command("opencypher", "MATCH (a) WITH * CALL (*) { MATCH (b) RETURN b.id AS bid } RETURN a.id AS aid, bid")) {
        assertThat(rs.hasNext()).isTrue();
        final Result r = rs.next();
        assertThat((Object) r.getProperty("aid")).isEqualTo(1);
        assertThat((Object) r.getProperty("bid")).isEqualTo(1);
      } finally {
        database.commit();
      }
      assertThat(count("MATCH () OPTIONAL MATCH (:NoMatch) WHERE false WITH * CALL (*) { MATCH () RETURN 1 AS x } MATCH () RETURN x")).isEqualTo(1);
    }

    @Test
    void explicitImportAfterTheBarrierIsUnchanged() {
      assertThat(count("""
          CREATE (alias0:Person) OPTIONAL MATCH (:NoMatch) WHERE false WITH *
          CALL (alias0) { OPTIONAL MATCH () WHERE EXISTS { MATCH (m) } RETURN 0 AS marker }
          RETURN alias0""")).isEqualTo(2);
    }

    @Test
    void unitSubqueryAfterTheBarrierKeepsTheOuterRow() {
      assertThat(count("CREATE (alias0:Person) OPTIONAL MATCH (:NoMatch) WHERE false WITH * CALL (*) { CREATE (:Seed) } RETURN alias0")).isEqualTo(1);
    }

    @Test
    void theGraphIsLeftUntouched() {
      count("CREATE (alias0:Person) OPTIONAL MATCH (:NoMatch) WHERE false WITH * " + CALL);
      assertThat(database.countType("Person", true) + database.countType("Seed", true)).isEqualTo(2);
    }
  }

  /**
   * #10512, #10513, #10515: a no-match {@code OPTIONAL MATCH ... WHERE false} followed by {@code WITH *} is the identity on the
   * rows that reach it, yet it changed what a write placed after it did to a result of more than 100 rows. The pipeline pulls
   * its rows in batches of 100, and the query with the identity OPTIONAL MATCH and the one without it disagreed from the 101st
   * row on, so each pair is pinned to the same answer, and to the right one:
   * <ul>
   *   <li>#10512: a correlated {@code CALL} subquery that deletes the node a variable-length path starts from;</li>
   *   <li>#10513: a path endpoint and the variable bound to the same node, read after a {@code REMOVE} of that node's property;</li>
   *   <li>#10515: an {@code OPTIONAL MATCH} that follows a {@code MERGE ... ON CREATE SET}, and must find the relationship the
   *       MERGE created for the current row.</li>
   * </ul>
   * The three have one cause in common, a clause that reads what an earlier clause of the same statement wrote or is about to
   * write while the pipeline hands the rows over a hundred at a time. The tests after the pinned queries cover the neighbours
   * of each fix: a body that only deletes and where it does not need to wait for the rows, a CREATE in place of the MERGE, a
   * path returned after the write.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue10512IdentityOptionalMatchBatchBoundary extends TestHelper {

    private static final String IDENTITY_OPTIONAL = "OPTIONAL MATCH (:NoMatch)\nWHERE false\nWITH *\n";

    private List<Result> run(final String cypher) {
      final List<Result> rows = new ArrayList<>();
      database.transaction(() -> {
        try (final ResultSet rs = database.command("opencypher", cypher)) {
          while (rs.hasNext())
            rows.add(rs.next());
        }
      });
      return rows;
    }

    private long count(final String cypher) {
      final List<Result> rows = run(cypher);
      return ((Number) rows.get(0).getProperty("c")).longValue();
    }

    // ---- #10512 ----

    private static final String TETRAHEDRON = "CREATE (a:N)-[:R]->(b:N), (a)-[:R]->(c:N), (b)-[:R]->(c), (b)-[:R]->(d:N), "
        + "(c)-[:R]->(d), (c)-[:R]->(a), (d)-[:R]->(a), (d)-[:R]->(b)";

    private static String correlatedDelete(final String identity) {
      return "MATCH p=(n0:N)-[*1..5]->()\n" + identity + "CALL (n0, p) {\n  DETACH DELETE n0\n  UNWIND [1] AS x\n  CREATE (:X)\n"
          + "  RETURN collect(x) AS rows\n}\nUNWIND rows AS r\nRETURN count(*) AS resultRows";
    }

    private void assertCorrelatedDelete(final String identity) {
      run(TETRAHEDRON);
      final long paths = count("MATCH p=(n0:N)-[*1..5]->() RETURN count(*) AS c");
      assertThat(paths).isGreaterThan(100L);

      final List<Result> rows = run(correlatedDelete(identity));

      assertThat(rows).hasSize(1);
      assertThat(((Number) rows.get(0).getProperty("resultRows")).longValue()).isEqualTo(paths);
      assertThat(count("MATCH (x:X) RETURN count(x) AS c")).isEqualTo(paths);
      assertThat(count("MATCH (n:N) RETURN count(n) AS c")).isZero();
      assertThat(count("MATCH ()-[r]->() RETURN count(r) AS c")).isZero();
    }

    @Test
    void correlatedDeleteOverVariableLengthPathsSeesEveryPath() {
      assertCorrelatedDelete("");
    }

    @Test
    void correlatedDeleteOverVariableLengthPathsWithIdentityOptionalMatchSeesEveryPath() {
      assertCorrelatedDelete(IDENTITY_OPTIONAL);
    }

    @Test
    void correlatedDeleteWithNothingElseInTheBodySeesEveryPath() {
      run(TETRAHEDRON);
      final long paths = count("MATCH p=(n0:N)-[*1..5]->() RETURN count(*) AS c");

      final long rows = count("MATCH p=(n0:N)-[*1..5]->() CALL (n0) { DETACH DELETE n0 RETURN 1 AS x } RETURN count(*) AS c");

      assertThat(rows).isEqualTo(paths);
      assertThat(count("MATCH (n:N) RETURN count(n) AS c")).isZero();
    }

    @Test
    void correlatedCreateInTheBodyDoesNotFeedTheMatchBehindIt() {
      run("UNWIND range(1, 5) AS i CREATE (:N {i: i})");

      // The MATCH is enumerated again for every row of the UNWIND and must not meet what the body created for the one before.
      // Pins the openCypher answer; it holds without a barrier too, the chained scan being a snapshot (issue #6602)
      final long rows = count("UNWIND range(1, 3) AS u MATCH (n:N) CALL (n) { CREATE (:N) RETURN 1 AS x } RETURN count(*) AS c");

      assertThat(rows).isEqualTo(15L);
      assertThat(count("MATCH (n:N) RETURN count(n) AS c")).isEqualTo(20L);
    }

    private String explain(final String cypher) {
      try (final ResultSet plan = database.command("opencypher", "EXPLAIN " + cypher)) {
        return plan.next().getProperty("executionPlanAsString");
      }
    }

    @Test
    void aBodyThatDeletesIsReadAheadOnlyBehindAnEnumerationItCanBreak() {
      final String body = " CALL (n) { DETACH DELETE n RETURN 1 AS x } RETURN count(*) AS c";

      // a variable-length walk keeps edge cursors open across the rows it still produces: the rows are drained first
      assertThat(explain("MATCH p=(n:N)-[*1..3]->()" + body)).contains("EAGER");
      // a plain scan is not broken by a delete of the row it just handed over: it keeps streaming
      assertThat(explain("MATCH (n:N)" + body)).doesNotContain("EAGER");
      // a body that does not delete never asks for it
      assertThat(explain("MATCH p=(n:N)-[*1..3]->() CALL (n) { CREATE (:X) RETURN 1 AS x } RETURN count(*) AS c")).doesNotContain("EAGER");
    }

    @Test
    void aDeleteBuriedInTheBodyIsFoundAtAnyDepth() {
      final String walk = "MATCH p=(n:N)-[*1..3]->() ";
      final String tail = " RETURN count(*) AS c";

      // in a FOREACH of the body
      assertThat(explain(walk + "CALL (n) { FOREACH (m IN [n] | DETACH DELETE m) RETURN 1 AS x }" + tail)).contains("EAGER");
      // in a subquery of the body
      assertThat(explain(walk + "CALL (n) { CALL (n) { DETACH DELETE n RETURN 1 AS y } RETURN 1 AS x }" + tail)).contains("EAGER");
      // in one branch of a UNION body
      assertThat(explain(walk + "CALL (n) { DETACH DELETE n RETURN 1 AS x UNION RETURN 2 AS x }" + tail)).contains("EAGER");
      // and none of them asks for it behind a plain scan
      assertThat(explain("MATCH (n:N) CALL (n) { CALL (n) { DETACH DELETE n RETURN 1 AS y } RETURN 1 AS x }" + tail)).doesNotContain("EAGER");
    }

    @Test
    void correlatedDeleteInANestedSubqueryOverVariableLengthPathsSeesEveryPath() {
      run(TETRAHEDRON);
      final long paths = count("MATCH p=(n0:N)-[*1..5]->() RETURN count(*) AS c");

      final long rows = count("MATCH p=(n0:N)-[*1..5]->() CALL (n0) { CALL (n0) { DETACH DELETE n0 RETURN 1 AS y } RETURN 1 AS x } RETURN count(*) AS c");

      assertThat(rows).isEqualTo(paths);
      assertThat(count("MATCH (n:N) RETURN count(n) AS c")).isZero();
    }

    // ---- #10513 ----

    private static String removeThenCompare(final String identity) {
      return "UNWIND range(1, 101) AS row\nMATCH p=(a)<-[:R]-(b)\n" + identity + "REMOVE b.k10\n"
          + "RETURN count(*) AS rows,\n       count(CASE WHEN properties(b) <> properties(last(nodes(p))) THEN 1 END) AS inconsistentAliasRows";
    }

    private void assertAliasAgrees(final String identity) {
      run("CREATE (a {k10: false})<-[:R]-(b {k10: true})");

      final List<Result> rows = run(removeThenCompare(identity));

      assertThat(rows).hasSize(1);
      assertThat(((Number) rows.get(0).getProperty("rows")).longValue()).isEqualTo(101L);
      assertThat(((Number) rows.get(0).getProperty("inconsistentAliasRows")).longValue()).isZero();
    }

    @Test
    void pathEndpointAgreesWithTheBoundNodeAfterRemove() {
      assertAliasAgrees("");
    }

    @Test
    void pathEndpointAgreesWithTheBoundNodeAfterRemoveWithIdentityOptionalMatch() {
      assertAliasAgrees(IDENTITY_OPTIONAL);
    }

    // ---- #10515 ----

    private static String mergeThenOptionalMatch(final String identity) {
      return "UNWIND range(1, 101) AS i\nMERGE (:A)-[r:rt8 {k11: 'before'}]->(n:N)\nON CREATE SET r.k11 = 'after'\n" + identity
          + "OPTIONAL MATCH ()-[r3:rt8]->(n)\nRETURN i, properties(r3) AS relationshipProperties ORDER BY i";
    }

    private void assertMergedRelationshipIsFound(final String identity) {
      final List<Result> rows = run(mergeThenOptionalMatch(identity));

      assertThat(rows).hasSize(101);
      for (final Result row : rows) {
        final Object properties = row.getProperty("relationshipProperties");
        assertThat(properties).as("row %s", row.<Object>getProperty("i")).isEqualTo(Map.of("k11", "after"));
      }
    }

    @Test
    void optionalMatchFindsTheRelationshipMergeCreatedForTheRow() {
      assertMergedRelationshipIsFound("");
    }

    @Test
    void optionalMatchFindsTheRelationshipMergeCreatedForTheRowWithIdentityOptionalMatch() {
      assertMergedRelationshipIsFound(IDENTITY_OPTIONAL);
    }

    @Test
    void optionalMatchFindsTheRelationshipCreateMadeForTheRow() {
      final List<Result> rows = run("UNWIND range(1, 101) AS i\nCREATE (:A)-[r:rt8 {k11: 'before'}]->(n:N)\nSET r.k11 = 'after'\n"
          + "OPTIONAL MATCH ()-[r3:rt8]->(n)\nRETURN i, properties(r3) AS relationshipProperties ORDER BY i");

      assertThat(rows).hasSize(101);
      for (final Result row : rows)
        assertThat(row.<Object>getProperty("relationshipProperties")).as("row %s", row.<Object>getProperty("i")).isEqualTo(Map.of("k11", "after"));
    }

    @Test
    void matchFindsTheRelationshipMergeCreatedForTheRow() {
      final List<Result> rows = run("UNWIND range(1, 101) AS i\nMERGE (:A)-[r:rt8 {k11: 'before'}]->(n:N)\nON CREATE SET r.k11 = 'after'\n"
          + "MATCH ()-[r3:rt8]->(n)\nRETURN i, r3");

      assertThat(rows).hasSize(101);
    }

    // ---- a path returned after the write ----

    @Test
    void pathReturnedAfterSetCarriesTheWrittenNode() {
      run("CREATE (:P {n: 1})-[:R]->(:P {n: 2})");

      final List<Result> rows = run("MATCH p = (a:P {n: 1})-[:R]->(b) SET b.v = 1 RETURN properties(last(nodes(p))) AS end");

      assertThat(rows).hasSize(1);
      assertThat(rows.get(0).<Object>getProperty("end")).isEqualTo(Map.of("n", 2, "v", 1L));
    }

    @Test
    void pathReturnedAfterSetOfARelationshipCarriesTheWrittenRelationship() {
      run("CREATE (:P {n: 1})-[:R]->(:P {n: 2})");

      final List<Result> rows = run("MATCH p = (a:P {n: 1})-[r:R]->(b) SET r.w = 5 RETURN properties(head(relationships(p))) AS rel");

      assertThat(rows).hasSize(1);
      assertThat(rows.get(0).<Object>getProperty("rel")).isEqualTo(Map.of("w", 5L));
    }

    @Test
    void pathReturnedAfterSetOfTheStartNodeCarriesItAndLeavesTheOtherNodeAlone() {
      run("CREATE (:P {n: 1})-[:R]->(:P {n: 2})");

      final List<Result> rows = run("MATCH p = (a:P {n: 1})-[:R]->(b) SET a.v = 1 RETURN properties(last(nodes(p))) AS end, properties(head(nodes(p))) AS start");

      assertThat(rows).hasSize(1);
      assertThat(rows.get(0).<Object>getProperty("end")).isEqualTo(Map.of("n", 2));
      assertThat(rows.get(0).<Object>getProperty("start")).isEqualTo(Map.of("n", 1, "v", 1L));
    }
  }
}
