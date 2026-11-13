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
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.graph.olap.GraphAnalyticalViewRegistry;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * count(*) over MATCH and OPTIONAL MATCH: multi-label counts, fan-out, cartesian products and pass-through WITH.
 * <p>
 * Each nested class holds the regression tests of the GitHub issue its name carries.
 */
class CypherCountStarTest {
  /**
   * Reproduces issue #5071: count(*) with inline-filtered start node and anonymous
   * endpoint returns the global relationship count instead of the count of matching rows.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue5071CountStar {
    private Database database;

    @BeforeEach
    void setUp() {
      database = new DatabaseFactory("./target/databases/testissue5071").create();
      database.transaction(() -> {
        database.getSchema().createVertexType("Node");
        database.getSchema().createEdgeType("LINK");

        final MutableVertex n1 = database.newVertex("Node").set("id", 1).save();
        final MutableVertex n2 = database.newVertex("Node").set("id", 2).save();
        final MutableVertex n3 = database.newVertex("Node").set("id", 3).save();

        // 3 LINK relationships: n1->n2, n1->n3, n2->n3
        n1.newEdge("LINK", n2);
        n1.newEdge("LINK", n3);
        n2.newEdge("LINK", n3);
      });
    }

    @AfterEach
    void tearDown() {
      if (database != null) {
        database.drop();
        database = null;
      }
    }

    @Test
    void countStarInlineFilterAnonymousEndpoint() {
      // Node 1 has 2 outgoing LINK edges -> expected 2, not the global count of 3.
      final ResultSet rs = database.query("opencypher",
          "MATCH (a:Node {id: 1})-[:LINK]->(:Node) RETURN count(*) AS cnt");

      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Long>getProperty("cnt")).isEqualTo(2L);
      assertThat(rs.hasNext()).isFalse();
      rs.close();
    }

    @Test
    void countStarNode2() {
      // Node 2 has 1 outgoing LINK edge.
      final ResultSet rs = database.query("opencypher",
          "MATCH (a:Node {id: 2})-[:LINK]->(:Node) RETURN count(*) AS cnt");

      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Long>getProperty("cnt")).isEqualTo(1L);
      rs.close();
    }

    @Test
    void countStarNoMatch() {
      // Node 3 has 0 outgoing LINK edges.
      final ResultSet rs = database.query("opencypher",
          "MATCH (a:Node {id: 3})-[:LINK]->(:Node) RETURN count(*) AS cnt");

      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Long>getProperty("cnt")).isEqualTo(0L);
      rs.close();
    }

    @Test
    void countBoundRelationshipStillWorks() {
      final ResultSet rs = database.query("opencypher",
          "MATCH (a:Node {id: 1})-[r:LINK]->(:Node) RETURN count(r) AS cnt");

      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Long>getProperty("cnt")).isEqualTo(2L);
      rs.close();
    }

    @Test
    void countNamedEndNodeStillWorks() {
      final ResultSet rs = database.query("opencypher",
          "MATCH (a:Node {id: 1})-[:LINK]->(b:Node) RETURN count(*) AS cnt");

      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Long>getProperty("cnt")).isEqualTo(2L);
      rs.close();
    }
  }

  /**
   * Reproduces issue #5084: {@code count(n)} on a multi-label node pattern
   * {@code (n:BugA:BugB)} returns an inflated count because the O(1) type-count
   * optimization only considered the first label. Row materialization for the
   * same pattern was already correct.
   * <p>
   * Label distribution:
   * <ul>
   *   <li>id = 1: BugA + BugB</li>
   *   <li>id = 2: BugA only</li>
   *   <li>id = 3: BugB only</li>
   *   <li>id = 4: BugA + BugB</li>
   * </ul>
   * Only nodes id = 1 and id = 4 have both labels, so the conjunction count must be 2.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue5084MultiLabelCount {
    private Database database;

    @BeforeEach
    void setUp() {
      database = new DatabaseFactory("./target/databases/testissue5084").create();
      database.command("opencypher", "CREATE "
          + "(n1:BugNode:BugA:BugB {id: 1}), "
          + "(n2:BugNode:BugA      {id: 2}), "
          + "(n3:BugNode:BugB      {id: 3}), "
          + "(n4:BugNode:BugA:BugB {id: 4})");
    }

    @AfterEach
    void tearDown() {
      if (database != null) {
        database.drop();
        database = null;
      }
    }

    @Test
    void multiLabelConjunctionCount() {
      // Only id = 1 and id = 4 have both BugA and BugB.
      final ResultSet rs = database.query("opencypher",
          "MATCH (n:BugA:BugB) RETURN count(n) AS cnt");

      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Long>getProperty("cnt")).isEqualTo(2L);
      assertThat(rs.hasNext()).isFalse();
      rs.close();
    }

    @Test
    void multiLabelConjunctionRowMaterializationStillCorrect() {
      // Control: row materialization was already returning the correct set.
      final ResultSet rs = database.query("opencypher",
          "MATCH (n:BugA:BugB) RETURN n.id AS id ORDER BY id");

      assertThat(rs.next().<Number>getProperty("id").intValue()).isEqualTo(1);
      assertThat(rs.next().<Number>getProperty("id").intValue()).isEqualTo(4);
      assertThat(rs.hasNext()).isFalse();
      rs.close();
    }

    @Test
    void singleLabelPlusWherePredicateCount() {
      // Control: equivalent single-label pattern + WHERE predicate was already correct.
      final ResultSet rs = database.query("opencypher",
          "MATCH (n:BugA) WHERE n:BugB RETURN count(n) AS cnt");

      assertThat(rs.next().<Long>getProperty("cnt")).isEqualTo(2L);
      rs.close();
    }

    @Test
    void singleLabelCountStillUsesOptimization() {
      // (n:BugA) matches all nodes carrying BugA (id 1,2,4) as in Neo4j.
      final ResultSet rs = database.query("opencypher",
          "MATCH (n:BugA) RETURN count(n) AS cnt");

      assertThat(rs.next().<Long>getProperty("cnt")).isEqualTo(3L);
      rs.close();
    }

    @Test
    void singleLabelBugBCount() {
      // (n:BugB) matches all nodes carrying BugB (id 1,3,4).
      final ResultSet rs = database.query("opencypher",
          "MATCH (n:BugB) RETURN count(n) AS cnt");

      assertThat(rs.next().<Long>getProperty("cnt")).isEqualTo(3L);
      rs.close();
    }
  }

  /**
   * Reproduces issue #5094: count(*) after OPTIONAL MATCH drops the null-preserving rows,
   * returning count(m) instead of the total number of rows.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue5094OptionalMatchCountStar {
    private Database database;

    @BeforeEach
    void setUp() {
      database = new DatabaseFactory("./target/databases/testissue5094").create();
      database.transaction(() -> {
        database.getSchema().createVertexType("BugNode");
        database.getSchema().createEdgeType("LINK");

        final MutableVertex n1 = database.newVertex("BugNode").set("id", 1).save();
        final MutableVertex n2 = database.newVertex("BugNode").set("id", 2).save();
        final MutableVertex n3 = database.newVertex("BugNode").set("id", 3).save();
        final MutableVertex n4 = database.newVertex("BugNode").set("id", 4).save();
        final MutableVertex n5 = database.newVertex("BugNode").set("id", 5).save();

        // n1 has 2 outgoing LINK edges, n2 and n4 have 1 each, n3 and n5 have none
        n1.newEdge("LINK", n2);
        n1.newEdge("LINK", n3);
        n4.newEdge("LINK", n5);
        n2.newEdge("LINK", n4);
      });
    }

    @AfterEach
    void tearDown() {
      if (database != null) {
        database.drop();
        database = null;
      }
    }

    @Test
    void countStarAfterOptionalMatchCountsNullRows() {
      // 4 matched rows (n1->n2, n1->n3, n2->n4, n4->n5) + 2 null rows (n3, n5) = 6
      final ResultSet rs = database.query("opencypher",
          "MATCH (n:BugNode) OPTIONAL MATCH (n)-[:LINK]->(m:BugNode) RETURN count(*) AS cnt");

      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Long>getProperty("cnt")).isEqualTo(6L);
      assertThat(rs.hasNext()).isFalse();
      rs.close();
    }

    @Test
    void countStarAndCountVariableTogether() {
      final ResultSet rs = database.query("opencypher",
          "MATCH (n:BugNode) OPTIONAL MATCH (n)-[:LINK]->(m:BugNode) RETURN count(*) AS rowCount, count(m) AS matchedCount");

      assertThat(rs.hasNext()).isTrue();
      final var row = rs.next();
      assertThat(row.<Long>getProperty("rowCount")).isEqualTo(6L);
      assertThat(row.<Long>getProperty("matchedCount")).isEqualTo(4L);
      rs.close();
    }

    @Test
    void countOptionalVariableExcludesNulls() {
      final ResultSet rs = database.query("opencypher",
          "MATCH (n:BugNode) OPTIONAL MATCH (n)-[:LINK]->(m:BugNode) RETURN count(m) AS cnt");

      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Long>getProperty("cnt")).isEqualTo(4L);
      rs.close();
    }

    @Test
    void rowMaterializationPreservesNullRows() {
      final ResultSet rs = database.query("opencypher",
          "MATCH (n:BugNode) OPTIONAL MATCH (n)-[:LINK]->(m:BugNode) RETURN n.id AS nId, m.id AS mId");

      int rows = 0;
      int nullRows = 0;
      while (rs.hasNext()) {
        final var row = rs.next();
        rows++;
        if (row.getProperty("mId") == null)
          nullRows++;
      }
      assertThat(rows).isEqualTo(6);
      assertThat(nullRows).isEqualTo(2);
      rs.close();
    }

    @Test
    void countStarWithoutOptionalMatchStillWorks() {
      final ResultSet rs = database.query("opencypher",
          "MATCH (n:BugNode) RETURN count(*) AS cnt");

      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Long>getProperty("cnt")).isEqualTo(5L);
      rs.close();
    }
  }

  /**
   * Reproduces issue #5790: {@code count(*)} after an {@code OPTIONAL MATCH} over a relationship
   * type that is not declared in the schema returned HTTP 500 ({@code SchemaException: Type with
   * name '...' was not found}) instead of the correct aggregated count.
   * <p>
   * Root cause: the star-join count-push-down ({@code CypherExecutionPlan#tryDetectStarCountStar})
   * builds a {@code DegreeProductOp} arm for the OPTIONAL MATCH relationship without checking
   * whether the schema actually declares that edge type. {@code DegreeProductOp#executeOLTPDegreeMap}
   * then called {@code Database#iterateType(String, boolean)} unconditionally, which resolves the
   * type via {@code Schema#getType(String)} and throws for an undeclared name. Every other
   * aggregation form ({@code count(m)}, {@code sum(1)}, {@code collect(m)}) and the un-aggregated
   * query are answered by the ordinary materialization pipeline, which already tolerates an
   * undeclared relationship type via vertex-local edge-list filtering ({@code EdgeLinkedList#count},
   * fixed for issue #4199) - only the {@code count(*)} degree-product fast path skipped that guard.
   *
   * @see <a href="https://github.com/ArcadeData/arcadedb/issues/5790">Issue #5790</a>
   */
  @Nested
  class Issue5790OptionalMatchCountStarUndeclaredRelType {
    private Database database;

    @BeforeEach
    void setUp() {
      database = new DatabaseFactory("./target/databases/testissue5790").create();
      database.getSchema().createVertexType("T");
    }

    @AfterEach
    void tearDown() {
      if (database != null) {
        database.drop();
        database = null;
      }
    }

    @Test
    void countStarAfterOptionalMatchOverUndeclaredRelTypeCountsAnchorRow() {
      database.transaction(() -> database.newVertex("T").set("id", 1).save());

      final ResultSet rs = database.query("opencypher",
          "MATCH (n:T) OPTIONAL MATCH (n)-[:NO_SUCH]->(m:T) RETURN count(*) AS c");

      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Long>getProperty("c")).isEqualTo(1L);
      assertThat(rs.hasNext()).isFalse();
      rs.close();
    }

    @Test
    void countStarAfterOptionalMatchOverUndeclaredRelTypeOnEmptyGraphIsZero() {
      final ResultSet rs = database.query("opencypher",
          "MATCH (n:T) OPTIONAL MATCH (n)-[:NO_SUCH]->(m:T) RETURN count(*) AS c");

      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Long>getProperty("c")).isEqualTo(0L);
      rs.close();
    }

    @Test
    void countStarAfterOptionalMatchOverUndeclaredRelTypeWithMultipleAnchors() {
      database.transaction(() -> {
        database.newVertex("T").set("id", 1).save();
        database.newVertex("T").set("id", 2).save();
        database.newVertex("T").set("id", 3).save();
      });

      final ResultSet rs = database.query("opencypher",
          "MATCH (n:T) OPTIONAL MATCH (n)-[:NO_SUCH]->(m:T) RETURN count(*) AS c");

      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Long>getProperty("c")).isEqualTo(3L);
      rs.close();
    }

    @Test
    void controlFormsAlreadyWorkedAndKeepWorking() {
      database.transaction(() -> database.newVertex("T").set("id", 1).save());

      try (ResultSet rs = database.query("opencypher",
          "MATCH (n:T) OPTIONAL MATCH (n)-[:NO_SUCH]->(m:T) RETURN count(m) AS c")) {
        assertThat(rs.next().<Long>getProperty("c")).isEqualTo(0L);
      }

      try (ResultSet rs = database.query("opencypher",
          "MATCH (n:T) OPTIONAL MATCH (n)-[:NO_SUCH]->(m:T) RETURN sum(1) AS c")) {
        assertThat(rs.next().<Number>getProperty("c").longValue()).isEqualTo(1L);
      }

      try (ResultSet rs = database.query("opencypher",
          "MATCH (n:T) OPTIONAL MATCH (n)-[:NO_SUCH]->(m:T) RETURN n.id AS id")) {
        assertThat(rs.next().<Number>getProperty("id").longValue()).isEqualTo(1L);
      }
    }

    @Test
    void countStarWithOneDeclaredAndOneUndeclaredOptionalArm() {
      // Star-join with two arms sharing the central variable n: a declared LINK arm and an
      // undeclared NO_SUCH arm. Only the undeclared arm's degree must be treated as always 0
      // (max(1, 0) = 1 multiplier), the declared arm must still be counted correctly.
      database.getSchema().createEdgeType("LINK");
      database.transaction(() -> {
        final MutableVertex n1 = database.newVertex("T").set("id", 1).save();
        final MutableVertex n2 = database.newVertex("T").set("id", 2).save();
        n1.newEdge("LINK", n2);
      });

      final ResultSet rs = database.query("opencypher",
          "MATCH (n:T) OPTIONAL MATCH (n)-[:LINK]->(x:T) OPTIONAL MATCH (n)-[:NO_SUCH]->(y:T) RETURN count(*) AS c");

      // n1 contributes 1 row (LINK degree 1 * NO_SUCH max(1,0)=1), n2 contributes 1 row
      // (LINK degree 0 -> optional null row * NO_SUCH max(1,0)=1) = 2 total.
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Long>getProperty("c")).isEqualTo(2L);
      rs.close();
    }
  }

  /**
   * Regression tests for issue #6629: the OPTIONAL MATCH + WITH count() fast-path optimization
   * (CountEdgesStep) assumes the bound vertex appears exactly once per input row. Any preceding
   * clause that fans a single input row out into several rows for the same bound vertex breaks
   * that assumption, since CountEdgesStep emits one output row per input row instead of collapsing
   * into the WITH aggregation's group.
   * <p>
   * PR #6630 guards against UNWIND / LOAD CSV preceding the OPTIONAL MATCH. This test also covers
   * other row-fanout sources (cartesian-product MATCH, variable-length paths) that are not excluded
   * by that guard, to check whether the same bug survives through them.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue6629FanoutBeforeOptionalMatchCount {
    private Database database;

    @BeforeEach
    void setUp() {
      database = new DatabaseFactory("./target/databases/testissue6629fanout").create();
      database.transaction(() -> {
        database.getSchema().createVertexType("X");
        database.getSchema().createEdgeType("L");
        database.command("sql", "CREATE VERTEX X SET _id = 'x1'");
        database.command("sql", "CREATE VERTEX X SET _id = 'x2'");
        database.command("sql",
            "CREATE EDGE L FROM (SELECT FROM X WHERE _id = 'x1') TO (SELECT FROM X WHERE _id = 'x2')");
      });
    }

    @AfterEach
    void tearDown() {
      if (database != null) {
        database.drop();
        database = null;
      }
    }

    @Test
    void unwindBeforeOptionalMatchCollapsesToOneGroup() {
      final ResultSet result = database.query("opencypher",
          "MATCH (a:X {_id:'x1'}) UNWIND [1, 2] AS u OPTIONAL MATCH (a)-[:L]->(m:X) WITH a, count(m) AS c RETURN c");

      assertThat(result.hasNext()).isTrue();
      final Result row = result.next();
      assertThat(row.<Long>getProperty("c")).isEqualTo(2L);
      assertThat(result.hasNext()).isFalse();
      result.close();
    }

    @Test
    void cartesianProductBeforeOptionalMatchCollapsesToOneGroup() {
      // (b) is an unrelated pattern that cartesian-products with (a), fanning out the input
      // stream to 2 rows per (a) before the OPTIONAL MATCH runs - the same fan-out shape as
      // UNWIND, just produced by a MATCH clause instead.
      final ResultSet result = database.query("opencypher",
          "MATCH (a:X {_id:'x1'}), (b:X) OPTIONAL MATCH (a)-[:L]->(m:X) WITH a, count(m) AS c RETURN c");

      assertThat(result.hasNext()).isTrue();
      final Result row = result.next();
      assertThat(row.<Long>getProperty("c")).isEqualTo(2L);
      assertThat(result.hasNext()).isFalse();
      result.close();
    }

    @Test
    void zeroMatchesStillCollapseToOneGroupWithUnwind() {
      final ResultSet result = database.query("opencypher",
          "MATCH (a:X {_id:'x2'}) UNWIND [1, 2] AS u OPTIONAL MATCH (a)-[:L]->(m:X) WITH a, count(m) AS c RETURN c");

      assertThat(result.hasNext()).isTrue();
      final Result row = result.next();
      assertThat(row.<Long>getProperty("c")).isEqualTo(0L);
      assertThat(result.hasNext()).isFalse();
      result.close();
    }

    @Test
    void controlWithoutUnwindStillOptimized() {
      final ResultSet result = database.query("opencypher",
          "MATCH (a:X {_id:'x1'}) OPTIONAL MATCH (a)-[:L]->(m:X) WITH a, count(m) AS c RETURN c");

      assertThat(result.hasNext()).isTrue();
      final Result row = result.next();
      assertThat(row.<Long>getProperty("c")).isEqualTo(1L);
      assertThat(result.hasNext()).isFalse();
      result.close();
    }

    @Test
    void anonymousPredecessorFanoutBeforeOptionalMatchCollapsesToOneGroup() {
      // Two anonymous, unnamed R-edges into (a) fan the input stream out to 2 rows for the same
      // a=x1 before the OPTIONAL MATCH runs, exactly like UNWIND does - but through pattern
      // elements that have no name to check against the WITH's grouping keys.
      database.transaction(() -> {
        database.getSchema().createEdgeType("R");
        database.newVertex("X").set("_id", "x3").save();
        database.command("sql",
            "CREATE EDGE R FROM (SELECT FROM X WHERE _id = 'x2') TO (SELECT FROM X WHERE _id = 'x1')");
        database.command("sql",
            "CREATE EDGE R FROM (SELECT FROM X WHERE _id = 'x3') TO (SELECT FROM X WHERE _id = 'x1')");
      });

      final ResultSet result = database.query("opencypher",
          "MATCH ()-[:R]->(a:X {_id:'x1'}) OPTIONAL MATCH (a)-[:L]->(m:X) WITH a, count(m) AS c RETURN c");

      assertThat(result.hasNext()).isTrue();
      final Result row = result.next();
      assertThat(row.<Long>getProperty("c")).isEqualTo(2L);
      assertThat(result.hasNext()).isFalse();
      result.close();
    }

    @Test
    void mergeMatchingMultipleExistingNodesBeforeOptionalMatchCollapsesToOneGroup() {
      // MERGE returns one row PER EXISTING MATCH when its pattern matches more than one element
      // (MergeStep), fanning the input stream out to 2 rows for the same a=x1 before the OPTIONAL
      // MATCH runs, the same shape as UNWIND but through a clause the guard must not assume safe.
      database.transaction(() -> {
        database.getSchema().createVertexType("Y");
        database.newVertex("Y").set("foo", 1).save();
        database.newVertex("Y").set("foo", 1).save();
      });

      final ResultSet result = database.command("opencypher",
          "MATCH (a:X {_id:'x1'}) MERGE (b:Y {foo: 1}) OPTIONAL MATCH (a)-[:L]->(m:X) WITH a, count(m) AS c RETURN c");

      assertThat(result.hasNext()).isTrue();
      final Result row = result.next();
      assertThat(row.<Long>getProperty("c")).isEqualTo(2L);
      assertThat(result.hasNext()).isFalse();
      result.close();
    }
  }

  /**
   * Issue #9596: when a MATCH is made of parts that share no variable and nothing but {@code count(*)} reads the rows, the
   * count is the product of the counts of the parts. The rows used to be built, so the time grew with the answer:
   * {@code MATCH (a:Person), (b:Person), (c:Person) RETURN count(*)} over 1,700 persons did not finish in 60 seconds. Each
   * part is now counted on its own, by the cheapest path it has, and the counts are multiplied.
   * <p>
   * Every count is checked against the row pipeline, reached through {@code RETURN sum(1)} which no count push-down
   * answers.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue9596CountCartesianProduct extends TestHelper {
    private static final String PRODUCT = "COUNT CARTESIAN PRODUCT";

    @Override
    protected void beginTest() {
      database.command("sql", "CREATE VERTEX TYPE Person");
      database.command("sql", "CREATE VERTEX TYPE Tag");
      database.command("sql", "CREATE VERTEX TYPE Empty");
      database.command("sql", "CREATE EDGE TYPE KNOWS");
      database.command("sql", "CREATE EDGE TYPE CLOSE_TO EXTENDS KNOWS");
      database.command("sql", "CREATE EDGE TYPE HAS_INTEREST");
      final Random random = new Random(9596);
      database.transaction(() -> {
        final List<Vertex> persons = new ArrayList<>();
        final List<Vertex> tags = new ArrayList<>();
        for (int i = 0; i < 40; i++)
          persons.add(database.newVertex("Person").set("id", i).save());
        for (int i = 0; i < 12; i++)
          tags.add(database.newVertex("Tag").set("id", i).save());
        for (int i = 0; i < 90; i++)
          persons.get(random.nextInt(persons.size())).modify()
              .newEdge(i % 7 == 0 ? "CLOSE_TO" : "KNOWS", persons.get(random.nextInt(persons.size())));
        for (int i = 0; i < 50; i++)
          persons.get(random.nextInt(persons.size())).modify().newEdge("HAS_INTEREST", tags.get(random.nextInt(tags.size())));
      });
    }

    @Test
    void disconnectedNodesMultiplyTheirCounts() {
      assertProduct("MATCH (a:Person), (b:Person)", 40L * 40);
      assertProduct("MATCH (a:Person), (b:Person), (c:Person)", 40L * 40 * 40);
      assertProduct("MATCH (a:Person) MATCH (t:Tag)", 40L * 12);
      assertProduct("MATCH (a:Person), (t:Tag), (e:Empty)", 0L);
      assertProduct("MATCH (a:Person {id: 3}), (t:Tag)", 12L);
    }

    @Test
    void disconnectedChainsMultiplyTheirCounts() {
      assertProductMatchesPipeline("MATCH (a:Person)-[:KNOWS]-(b), (t:Tag)");
      assertProductMatchesPipeline("MATCH (a:Person)-[:KNOWS]->(b:Person), (p:Person)-[:HAS_INTEREST]->(t:Tag)");
      assertProductMatchesPipeline("MATCH (a:Person)-[:HAS_INTEREST]->(t:Tag) MATCH (b:Person)-[:KNOWS]->(c) WHERE c.id > 10");
      assertProductMatchesPipeline("MATCH (a:Person)-[:KNOWS]->(b), (c:Person) WHERE a.id < 20 AND c.id > 30");
      // two parts that a WHERE joins are one part, and the third one still multiplies
      assertProductMatchesPipeline("MATCH (a:Person), (b:Person), (t:Tag) WHERE a.id = b.id + 1");
      // a star next to a lone node: the star count used to skip the lone node's pattern and leave its count out
      assertProductMatchesPipeline("MATCH (a:Person)-[:KNOWS]->(b), (a)-[:HAS_INTEREST]->(t), (x:Tag)");
      // an inline WHERE on a relationship that reads the other part joins the two
      final String relationshipWhere = "MATCH (a:Person), (c:Person)-[r:HAS_INTEREST WHERE c.id < a.id]->(t:Tag)";
      assertThat(plan(relationshipWhere + " RETURN count(*) AS n")).doesNotContain(PRODUCT);
      assertThat(count(relationshipWhere + " RETURN count(*) AS n"))
          .isEqualTo(count(relationshipWhere + " RETURN sum(1) AS n"));
      assertProductMatchesPipeline("MATCH (a:Person), (c:Person)-[r:HAS_INTEREST WHERE c.id < 20]->(t:Tag)");
      // a conjunct of a later clause that reads an earlier part only
      assertThat(count("MATCH (a:Person) MATCH (t:Tag) WHERE a.id > 30 RETURN count(*) AS n"))
          .isEqualTo(count("MATCH (a:Person) MATCH (t:Tag) WHERE a.id > 30 RETURN sum(1) AS n"));
    }

    @Test
    void optionalPartsCountAtLeastOneRow() {
      assertProductMatchesPipeline("MATCH (a:Person) OPTIONAL MATCH (e:Empty)");
      assertProductMatchesPipeline("MATCH (a:Person) OPTIONAL MATCH (t:Tag)");
      assertProductMatchesPipeline("MATCH (a:Person) OPTIONAL MATCH (t:Tag), (e:Empty)");
      assertProductMatchesPipeline("MATCH (a:Person) OPTIONAL MATCH (a)-[:HAS_INTEREST]->(t:Tag) MATCH (b:Tag)");
      assertProductMatchesPipeline("OPTIONAL MATCH (e:Empty) MATCH (t:Tag)");
    }

    @Test
    void partsThatMayBindTheSameRelationshipAreNotMultiplied() {
      // one MATCH binds every relationship once, so two KNOWS parts of it can not be the same edge
      for (final String match : new String[] { "MATCH (a)-[:KNOWS]->(b), (c)-[:KNOWS]->(d)",
          "MATCH (a)-[:KNOWS]->(b), (c)-[:CLOSE_TO]->(d)", "MATCH (a)-[]->(b), (c:Person)-[:HAS_INTEREST]->(t)" }) {
        assertThat(plan(match + " RETURN count(*) AS n")).as(match).doesNotContain(PRODUCT);
        assertThat(count(match + " RETURN count(*) AS n")).as(match).isEqualTo(count(match + " RETURN sum(1) AS n"));
      }
      // in two MATCH clauses they can
      assertProductMatchesPipeline("MATCH (a)-[:KNOWS]->(b) MATCH (c)-[:KNOWS]->(d)");
      // a sub-type and a type of another hierarchy cannot be the same edge, so one clause multiplies them
      assertProductMatchesPipeline("MATCH (a:Person)-[:CLOSE_TO]->(b), (c:Person)-[:HAS_INTEREST]->(t:Tag)");
      // the parent type matches the sub-type's edges too: refused in either order
      final String parentAndChild = "MATCH (a)-[:CLOSE_TO]->(b), (c)-[:KNOWS]->(d)";
      assertThat(plan(parentAndChild + " RETURN count(*) AS n")).doesNotContain(PRODUCT);
      assertThat(count(parentAndChild + " RETURN count(*) AS n")).isEqualTo(count(parentAndChild + " RETURN sum(1) AS n"));
    }

    @Test
    void skipAndLimitApplyToTheCountRow() {
      assertThat(count("MATCH (a:Person), (b:Tag) RETURN count(*) AS n LIMIT 1")).isEqualTo(40L * 12);
      try (final ResultSet rs = database.query("opencypher", "MATCH (a:Person), (b:Tag) RETURN count(*) AS n SKIP 1")) {
        assertThat(rs.hasNext()).isFalse();
      }
    }

    @Test
    void aCountSubqueryOfDisconnectedPartsIsAProductToo() {
      final String query = "RETURN COUNT { MATCH (a:Person), (t:Tag) } AS n";
      assertThat(count(query)).isEqualTo(40L * 12);
    }

    @Test
    void aCountThatOverflowsALongIsAnError() {
      final StringBuilder match = new StringBuilder("MATCH ");
      for (int i = 0; i < 13; i++)
        match.append(i > 0 ? ", " : "").append("(p").append(i).append(":Person)");
      // 40^13 is about 6.7e20, beyond what a long holds: the rows could never be counted either
      assertThatThrownBy(() -> count(match + " RETURN count(*) AS n")).isInstanceOf(CommandExecutionException.class)
          .hasMessageContaining("overflow");

      // an empty part makes the product 0 whatever the others hold: after the overflow, and before it
      assertThat(count(match + ", (e:Empty) RETURN count(*) AS n")).isZero();
      assertThat(count("MATCH (e:Empty), " + match.substring("MATCH ".length()) + " RETURN count(*) AS n")).isZero();
    }

    @Test
    void theSameAnswersOverAGraphAnalyticalView() throws InterruptedException {
      database.command("sql", "CREATE GRAPH ANALYTICAL VIEW people VERTEX TYPES (Person, Tag, Empty) EDGE TYPES "
          + "(KNOWS, CLOSE_TO, HAS_INTEREST) UPDATE MODE OFF");
      final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "people");
      final long deadline = System.currentTimeMillis() + 60_000;
      while (!view.isReady() && System.currentTimeMillis() < deadline)
        Thread.sleep(20);
      assertThat(view.isReady()).isTrue();
      try {
        disconnectedNodesMultiplyTheirCounts();
        disconnectedChainsMultiplyTheirCounts();
        optionalPartsCountAtLeastOneRow();
      } finally {
        database.command("sql", "DROP GRAPH ANALYTICAL VIEW people");
      }
    }

    private void assertProduct(final String match, final long expected) {
      final String query = match + " RETURN count(*) AS n";
      assertThat(plan(query)).as("plan of %s", query).contains(PRODUCT);
      assertThat(count(query)).as(query).isEqualTo(expected);
      assertThat(count(match + " RETURN sum(1) AS n")).as("pipeline of %s", query).isEqualTo(expected);
    }

    private void assertProductMatchesPipeline(final String match) {
      final String query = match + " RETURN count(*) AS n";
      assertThat(plan(query)).as("plan of %s", query).contains(PRODUCT);
      assertThat(count(query)).as(query).isEqualTo(count(match + " RETURN sum(1) AS n"));
    }

    private String plan(final String query) {
      try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query)) {
        return rs.getExecutionPlan().map(p -> p.prettyPrint(0, 2)).orElse("");
      }
    }

    private long count(final String query) {
      try (final ResultSet rs = database.query("opencypher", query, Map.of())) {
        final Object value = rs.next().getProperty("n");
        return value == null ? 0L : ((Number) value).longValue();
      }
    }
  }

  /**
   * Issue #9597: a {@code WITH} that only passes variables on - no aggregation, {@code DISTINCT}, {@code ORDER BY},
   * {@code SKIP}, {@code LIMIT} or {@code WHERE} - leaves the number of rows alone, but it took a count-only statement off
   * every count push-down: LSQB Q7 with {@code WITH message, creator} went from 0.008 s to 0.875 s. Such a {@code WITH} is
   * now transparent to the push-downs, aliases included.
   * <p>
   * Every count is checked against the row pipeline, reached through {@code RETURN sum(1)} which no count push-down
   * answers.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue9597PassThroughWithCount extends TestHelper {
    private static final String PUSH_DOWN = "Using Count Push-Down";

    private static final String Q7_HEAD = "MATCH (:Tag)<-[:HAS_TAG]-(message:Message)-[:HAS_CREATOR]->(creator:Person) ";
    private static final String Q7_TAIL =
        "OPTIONAL MATCH (message)<-[:LIKES]-(liker:Person) OPTIONAL MATCH (message)<-[:REPLY_OF]-(comment:Message)";

    @Override
    protected void beginTest() {
      database.command("sql", "CREATE VERTEX TYPE Tag");
      database.command("sql", "CREATE VERTEX TYPE Person");
      database.command("sql", "CREATE VERTEX TYPE Message");
      database.command("sql", "CREATE VERTEX TYPE Comment EXTENDS Message");
      database.command("sql", "CREATE EDGE TYPE HAS_TAG");
      database.command("sql", "CREATE EDGE TYPE HAS_CREATOR");
      database.command("sql", "CREATE EDGE TYPE LIKES");
      database.command("sql", "CREATE EDGE TYPE REPLY_OF");
      final Random random = new Random(9597);
      database.transaction(() -> {
        final List<Vertex> tags = new ArrayList<>();
        final List<Vertex> persons = new ArrayList<>();
        for (int i = 0; i < 20; i++)
          tags.add(database.newVertex("Tag").save());
        for (int i = 0; i < 30; i++)
          persons.add(database.newVertex("Person").set("id", i).save());
        final List<Vertex> messages = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
          final MutableVertex m = database.newVertex(i % 3 == 0 ? "Comment" : "Message").set("id", i).save();
          messages.add(m);
          for (int k = random.nextInt(3); k > 0; k--)
            m.newEdge("HAS_TAG", tags.get(random.nextInt(tags.size())));
          if (random.nextInt(6) > 0)
            m.newEdge("HAS_CREATOR", persons.get(random.nextInt(persons.size())));
          for (int k = random.nextInt(4); k > 0; k--)
            persons.get(random.nextInt(persons.size())).modify().newEdge("LIKES", m);
          if (i > 0 && random.nextBoolean())
            m.newEdge("REPLY_OF", messages.get(random.nextInt(i)));
        }
      });
    }

    @Test
    void aPassThroughWithKeepsTheStarCount() {
      assertPushedDownAndExact(Q7_HEAD + "WITH message, creator " + Q7_TAIL);
      assertPushedDownAndExact(Q7_HEAD + "WITH * " + Q7_TAIL);
      assertPushedDownAndExact(Q7_HEAD + "WITH message " + Q7_TAIL);
      assertPushedDownAndExact(Q7_HEAD + "WITH message AS m, creator "
          + "OPTIONAL MATCH (m)<-[:LIKES]-(liker:Person) OPTIONAL MATCH (m)<-[:REPLY_OF]-(comment:Comment)");
      assertPushedDownAndExact("MATCH (:Tag)<-[:HAS_TAG]-(message:Message) WITH message AS m WITH m AS x "
          + "MATCH (x)-[:HAS_CREATOR]->(:Person)");
    }

    @Test
    void aPassThroughWithKeepsTheChainCount() {
      assertPushedDownAndExact("MATCH (t:Tag)<-[:HAS_TAG]-(m:Message) WITH m MATCH (m)-[:HAS_CREATOR]->(p:Person)");
      assertPushedDownAndExact("MATCH (t:Tag)<-[:HAS_TAG]-(m:Message) WITH t, m RETURN count(*) AS n", false);
    }

    @Test
    void aWithThatChangesTheRowsIsNotTransparent() {
      for (final String with : new String[] { "WITH DISTINCT message, creator ", "WITH message, creator LIMIT 5 ",
          "WITH message, creator WHERE creator.id > 10 ", "WITH message, creator ORDER BY creator.id SKIP 3 ",
          "WITH message, count(*) AS c ", "WITH message, creator, 1 AS one ", "WITH message, creator.id AS cid " }) {
        final String match = Q7_HEAD + with + Q7_TAIL;
        assertThat(count(match + " RETURN count(*) AS n")).as(match).isEqualTo(count(match + " RETURN sum(1) AS n"));
      }
    }

    @Test
    void aNameTheWithDropsIsAFreshVariableAfterIt() {
      // after WITH message, creator the name 'creator2' below is new, and so is a name the WITH dropped
      final String reused = "MATCH (p:Person)<-[:HAS_CREATOR]-(message:Message)-[:HAS_TAG]->(t:Tag) WITH message "
          + "MATCH (message)<-[:LIKES]-(p:Person)";
      assertThat(count(reused + " RETURN count(*) AS n")).isEqualTo(count(reused + " RETURN sum(1) AS n"));
      // concatenated, the two 'a' would be one variable and the star count would answer the likes alone
      final String fresh = "MATCH (a:Person)-[:LIKES]->(m:Message) WITH m MATCH (a:Person)";
      assertThat(count(fresh + " RETURN count(*) AS n")).isEqualTo(count(fresh + " RETURN sum(1) AS n"))
          .isEqualTo(count("MATCH (:Person)-[:LIKES]->(:Message) RETURN count(*) AS n") * 30);

      // an alias onto a name the WITH dropped binds that name to the projected value only
      final String swapped = "MATCH (message:Message)-[:HAS_CREATOR]->(creator:Person) WITH message AS creator "
          + "MATCH (creator)<-[:LIKES]-(liker:Person)";
      assertThat(count(swapped + " RETURN count(*) AS n")).isEqualTo(count(swapped + " RETURN sum(1) AS n"));
    }

    @Test
    void theSameAnswersOverAGraphAnalyticalView() throws InterruptedException {
      database.command("sql", "CREATE GRAPH ANALYTICAL VIEW social VERTEX TYPES (Tag, Person, Message, Comment) EDGE TYPES "
          + "(HAS_TAG, HAS_CREATOR, LIKES, REPLY_OF) UPDATE MODE OFF");
      final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "social");
      final long deadline = System.currentTimeMillis() + 60_000;
      while (!view.isReady() && System.currentTimeMillis() < deadline)
        Thread.sleep(20);
      assertThat(view.isReady()).isTrue();
      try {
        aPassThroughWithKeepsTheStarCount();
        aPassThroughWithKeepsTheChainCount();
        aWithThatChangesTheRowsIsNotTransparent();
        aNameTheWithDropsIsAFreshVariableAfterIt();
      } finally {
        database.command("sql", "DROP GRAPH ANALYTICAL VIEW social");
      }
    }

    private void assertPushedDownAndExact(final String match) {
      assertPushedDownAndExact(match, true);
    }

    private void assertPushedDownAndExact(final String statement, final boolean appendReturn) {
      final String query = appendReturn ? statement + " RETURN count(*) AS n" : statement;
      final String pipeline = appendReturn ? statement + " RETURN sum(1) AS n" : statement.replace("count(*)", "sum(1)");
      assertThat(plan(query)).as("plan of %s", query).contains(PUSH_DOWN);
      assertThat(count(query)).as(query).isEqualTo(count(pipeline));
    }

    private String plan(final String query) {
      try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query)) {
        return rs.getExecutionPlan().map(p -> p.prettyPrint(0, 2)).orElse("");
      }
    }

    private long count(final String query) {
      try (final ResultSet rs = database.query("opencypher", query)) {
        final Object value = rs.next().getProperty("n");
        return value == null ? 0L : ((Number) value).longValue();
      }
    }
  }
}
