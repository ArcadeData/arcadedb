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
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * DISTINCT and grouping-key equality: numeric equality, lists, hidden path variables and key delimiters.
 * <p>
 * Each nested class holds the regression tests of the GitHub issue its name carries.
 */
class CypherDistinctAndGroupingTest {
  /**
   * Issue #4141 (ISO GQL / Cypher 25 cleanup): grouping keys in aggregating RETURN must be explicit.
   * <p>
   * GQL forbids ambiguous (implicit) grouping: an aggregating projection that also references a
   * non-grouping-key variable/property is an error. This verifies that explicit grouping works as
   * expected and that ambiguous grouping is rejected with {@code AmbiguousAggregationExpression}.
   */
  @Nested
  class Issue4141GroupingKeys {
    private Database database;

    @BeforeEach
    void setUp() {
      final DatabaseFactory factory = new DatabaseFactory("./target/databases/testIssue4141GroupingKeys");
      if (factory.exists())
        factory.open().drop(); // defend against a leftover db from a previously interrupted run
      database = factory.create();
      database.getSchema().createVertexType("Person");
      database.transaction(() -> {
        database.command("opencypher", "CREATE (p:Person {name: 'Alice', dept: 'eng', team: 'core', age: 30})");
        database.command("opencypher", "CREATE (p:Person {name: 'Bob',   dept: 'eng', team: 'core', age: 40})");
        database.command("opencypher", "CREATE (p:Person {name: 'Carol', dept: 'sales', team: 'west', age: 50})");
      });
    }

    @AfterEach
    void tearDown() {
      if (database != null) {
        database.drop();
        database = null;
      }
    }

    // ---- Explicit grouping keys work ------------------------------------------------------------

    @Test
    void singleGroupingKey() {
      final Map<String, Long> counts = new HashMap<>();
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (p:Person) RETURN p.dept AS dept, count(*) AS c")) {
        while (rs.hasNext()) {
          final Result r = rs.next();
          counts.put(r.getProperty("dept"), ((Number) r.getProperty("c")).longValue());
        }
      }
      assertThat(counts).containsEntry("eng", 2L).containsEntry("sales", 1L);
    }

    @Test
    void multipleGroupingKeys() {
      int rows = 0;
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (p:Person) RETURN p.dept AS dept, p.team AS team, count(*) AS c")) {
        while (rs.hasNext()) {
          rs.next();
          rows++;
        }
      }
      // (eng,core) and (sales,west) -> 2 distinct groups
      assertThat(rows).isEqualTo(2);
    }

    @Test
    void aggregationMixedWithAGroupingKeyIsAllowed() {
      // p.age is a grouping key here (first item), so 'count(*) + p.age' is unambiguous.
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (p:Person) RETURN p.age AS age, count(*) + p.age AS expr ORDER BY age")) {
        assertThat(rs.hasNext()).isTrue();
        final Result first = rs.next();
        // age 30 appears once -> 1 + 30 = 31
        assertThat(((Number) first.getProperty("age")).longValue()).isEqualTo(30L);
        assertThat(((Number) first.getProperty("expr")).longValue()).isEqualTo(31L);
      }
    }

    // ---- Ambiguous (implicit) grouping is rejected ----------------------------------------------

    @Test
    void aggregationMixedWithNonGroupingKeyIsRejected() {
      // p.age is NOT a grouping key (no non-aggregating item references it), so mixing it into an
      // aggregating projection is an ambiguous implicit grouping.
      assertThatThrownBy(() ->
          database.query("opencypher", "MATCH (p:Person) RETURN count(*) + p.age AS expr").close())
          .isInstanceOf(CommandParsingException.class)
          .hasMessageContaining("AmbiguousAggregationExpression");
    }

    @Test
    void aggregationInWhereIsRejected() {
      assertThatThrownBy(() ->
          database.query("opencypher", "MATCH (p:Person) WHERE count(*) > 1 RETURN p").close())
          .isInstanceOf(CommandParsingException.class)
          .hasMessageContaining("InvalidAggregation: Aggregation functions are not allowed in WHERE");
    }
  }

  /**
   * Issue #5789: ArcadeDB evaluates {@code 1 = 1.0} as {@code true}, but its duplicate-elimination
   * paths (UNION, RETURN DISTINCT, count(DISTINCT ...), collect(DISTINCT ...)) treated the same
   * finite, non-null numeric values as distinct when they had different boxed numeric types
   * (INTEGER vs FLOAT). This left UNION and DISTINCT out of sync with expression equality.
   */
  @Nested
  class Issue5789NumericDistinctEquality {
    private Database database;

    @BeforeEach
    void setUp() {
      database = new DatabaseFactory("./target/databases/issue5789-numeric-distinct").create();
    }

    @AfterEach
    void tearDown() {
      if (database != null) {
        database.drop();
        database = null;
      }
    }

    @Test
    void unionCollapsesNumericallyEqualCrossTypeValues() {
      try (final ResultSet rs = database.query("opencypher", "RETURN 1 AS x UNION RETURN 1.0 AS x")) {
        assertThat(rs.stream().count()).isEqualTo(1);
      }
    }

    @Test
    void unionStillDeduplicatesSameTypeValues() {
      try (final ResultSet rs = database.query("opencypher", "RETURN 1 AS x UNION RETURN 1 AS x")) {
        assertThat(rs.stream().count()).isEqualTo(1);
      }
    }

    @Test
    void unionAllStillPreservesCrossTypeDuplicates() {
      try (final ResultSet rs = database.query("opencypher", "RETURN 1 AS x UNION ALL RETURN 1.0 AS x")) {
        assertThat(rs.stream().count()).isEqualTo(2);
      }
    }

    @Test
    void returnDistinctCollapsesNumericallyEqualCrossTypeValues() {
      try (final ResultSet rs = database.query("opencypher", "UNWIND [1, 1.0] AS x RETURN DISTINCT x")) {
        assertThat(rs.stream().count()).isEqualTo(1);
      }
    }

    @Test
    void returnDistinctStillKeepsNumericallyDifferentValues() {
      try (final ResultSet rs = database.query("opencypher", "UNWIND [1, 2.0] AS x RETURN DISTINCT x")) {
        assertThat(rs.stream().count()).isEqualTo(2);
      }
    }

    @Test
    void countDistinctCollapsesNumericallyEqualCrossTypeValues() {
      try (final ResultSet rs = database.query("opencypher", "UNWIND [1, 1.0] AS x RETURN count(DISTINCT x) AS c")) {
        assertThat(rs.hasNext()).isTrue();
        final Result row = rs.next();
        assertThat(((Number) row.getProperty("c")).longValue()).isEqualTo(1L);
      }
    }

    @Test
    void collectDistinctCollapsesNumericallyEqualCrossTypeValues() {
      try (final ResultSet rs = database.query("opencypher", "UNWIND [1, 1.0] AS x RETURN collect(DISTINCT x) AS c")) {
        assertThat(rs.hasNext()).isTrue();
        final Result row = rs.next();
        @SuppressWarnings("unchecked")
        final List<Object> collected = (List<Object>) row.getProperty("c");
        assertThat(collected).hasSize(1);
      }
    }

    @Test
    void minDistinctAndMaxDistinctCollapseNumericallyEqualCrossTypeValues() {
      // A regression guard for the shared DistinctAggregationWrapper used by min/max/avg DISTINCT.
      try (final ResultSet rs = database.query("opencypher",
          "UNWIND [1, 1.0, 2] AS x RETURN min(DISTINCT x) AS mn, max(DISTINCT x) AS mx, avg(DISTINCT x) AS av")) {
        assertThat(rs.hasNext()).isTrue();
        final Result row = rs.next();
        assertThat(((Number) row.getProperty("mn")).doubleValue()).isEqualTo(1.0);
        assertThat(((Number) row.getProperty("mx")).doubleValue()).isEqualTo(2.0);
        // Only two distinct values (1 and 2) should be averaged: (1 + 2) / 2 = 1.5
        assertThat(((Number) row.getProperty("av")).doubleValue()).isEqualTo(1.5);
      }
    }

    @Test
    void equalityOperatorAndDistinctPathsAgree() {
      try (final ResultSet rs = database.query("opencypher", "RETURN 1 = 1.0 AS eq")) {
        assertThat(rs.hasNext()).isTrue();
        final Result row = rs.next();
        assertThat((Boolean) row.getProperty("eq")).isTrue();
      }
    }
  }

  /**
   * Regression test for issue #6488: {@code RETURN DISTINCT} (and {@code WITH DISTINCT}) must collapse
   * rows whose projected variables are equal, even when one of the two matching rows binds its variable
   * to a graph-element reference whose property buffer has not yet been deserialized.
   * <p>
   * On a single self-looping node, the quantified relationship {@code [*0..1]} enumerates two paths
   * that both bind {@code n0} and {@code n1} to the same node: the zero-length path (whose {@code n1}
   * reference is the already-loaded start vertex) and the one-edge self-loop path (whose {@code n1}
   * reference is freshly obtained from the edge's endpoint and not yet deserialized). Both references
   * carry the same RID, but {@code Document.toString()} renders a not-yet-loaded reference as a
   * placeholder (e.g. {@code #1:0[?]}) instead of the actual property values - the root cause was
   * DISTINCT building its deduplication key by rendering values with {@code toString()} instead of by
   * their stable identity. {@code RETURN DISTINCT n1, n0} (and {@code WITH DISTINCT n1, n0}) must
   * collapse the two rows to one - matching Neo4j, the openCypher reference implementation.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue6488DistinctHiddenPathVariable {
    private Database database;

    @BeforeEach
    void setUp() {
      final DatabaseFactory factory = new DatabaseFactory("./target/databases/issue6488");
      if (factory.exists())
        factory.open().drop();
      database = factory.create();

      database.transaction(() -> {
        database.command("cypher", "CREATE (a:A {name: 'a'})");
        database.command("cypher", "MATCH (a:A {name: 'a'}) CREATE (a)-[:R]->(a)");
      });
    }

    @AfterEach
    void tearDown() {
      if (database != null && database.isOpen())
        database.drop();
    }

    /**
     * The reported case: an unreturned path variable {@code p} must not defeat {@code DISTINCT}.
     */
    @Test
    void distinctCollapsesSelfLoopZeroAndOneHopPaths() {
      try (ResultSet rs = database.query("cypher", """
          MATCH p = WALK (n0)-[*0..1]->(n1)
          RETURN DISTINCT n1, n0""")) {
        assertThat(rs.hasNext()).isTrue();
        rs.next();
        assertThat(rs.hasNext()).as("DISTINCT must collapse the zero-length and self-loop paths into a single row").isFalse();
      }
    }

    /**
     * Same query, but with an identity {@code CALL (*)} boundary interposed between the MATCH and the
     * final RETURN. Both forms must agree.
     */
    @Test
    void distinctCollapsesSelfLoopPathsAcrossIdentitySubquery() {
      try (ResultSet rs = database.query("cypher", """
          MATCH p = WALK (n0)-[*0..1]->(n1)
          WITH n0, n1
          CALL (*) {
            RETURN 0 AS marker
          }
          WITH n0, n1
          RETURN DISTINCT n1, n0""")) {
        assertThat(rs.hasNext()).isTrue();
        rs.next();
        assertThat(rs.hasNext()).isFalse();
      }
    }

    /**
     * {@code WITH DISTINCT} hit the same root cause via a different code path: it built its
     * deduplication key from the projected row's {@code toString()} instead of canonicalizing each
     * value, so it was equally susceptible to the {@code Document.toString()} load-state instability.
     */
    @Test
    void withDistinctCollapsesSelfLoopZeroAndOneHopPaths() {
      try (ResultSet rs = database.query("cypher", """
          MATCH p = WALK (n0)-[*0..1]->(n1)
          WITH DISTINCT n1, n0
          RETURN n1, n0""")) {
        assertThat(rs.hasNext()).isTrue();
        rs.next();
        assertThat(rs.hasNext()).as("WITH DISTINCT must collapse the zero-length and self-loop paths into a single row").isFalse();
      }
    }

    /**
     * {@code UNION} (implicit DISTINCT) deduplicates through the very same
     * {@code DistinctNumericKey.canonicalize()} utility as {@code RETURN DISTINCT}. Union the
     * already-loaded reference from a plain match with the freshly-fetched, not-yet-deserialized
     * reference from the self-loop traversal, and confirm they still collapse to one row.
     */
    @Test
    void unionCollapsesLoadedAndUnloadedReferencesToSameNode() {
      try (ResultSet rs = database.query("cypher", """
          MATCH p = WALK (n0)-[*0..1]->(n1)
          RETURN n1 AS x
          UNION
          MATCH (a:A)
          RETURN a AS x""")) {
        assertThat(rs.hasNext()).isTrue();
        rs.next();
        assertThat(rs.hasNext()).as("UNION must collapse references to the same node regardless of load state").isFalse();
      }
    }

    /**
     * {@code collect(DISTINCT ...)} and {@code count(DISTINCT ...)} go through
     * {@code DistinctAggregationWrapper}, which also canonicalizes via {@code DistinctNumericKey}.
     */
    @Test
    void collectAndCountDistinctCollapseSelfLoopZeroAndOneHopPaths() {
      try (ResultSet rs = database.query("cypher", """
          MATCH p = WALK (n0)-[*0..1]->(n1)
          RETURN collect(DISTINCT n1) AS xs, count(DISTINCT n1) AS c""")) {
        assertThat(rs.hasNext()).isTrue();
        final Result row = rs.next();
        assertThat((List<?>) row.getProperty("xs")).hasSize(1);
        assertThat(row.<Number>getProperty("c").intValue()).isEqualTo(1);
      }
    }
  }

  /**
   * Issue #6540: {@code ProjectReturnStep}, {@code UnionStep} and {@code WithStep} built their
   * DISTINCT/UNION deduplication key by concatenating {@code name=value|} for every projected property
   * into one string. A projected value whose rendering itself contains {@code =} or {@code |} could make
   * two genuinely different rows concatenate to the same key string and get wrongly collapsed - e.g. a
   * single property {@code a = "1|b=2"} and two properties {@code a=1, b=2} both used to render as
   * {@code "a=1|b=2|"}.
   * <p>
   * {@code UNION} is the clearest way to put two differently-shaped rows (one column vs. two columns)
   * through the same dedup key, since a plain {@code RETURN}/{@code WITH} fixes its column set for every
   * row of a single branch.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue6540DistinctKeyDelimiterCollision {
    private Database database;

    @BeforeEach
    void setUp() {
      final DatabaseFactory factory = new DatabaseFactory("./target/databases/issue-6540-distinct-key-collision");
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
    }

    @AfterEach
    void tearDown() {
      if (database != null && database.isOpen())
        database.drop();
    }

    /**
     * The reported case, verbatim: a single string property that happens to look like a rendered
     * {@code name=value|name=value|} key must not collide with an unrelated two-property row.
     */
    @Test
    void unionDoesNotCollapseDifferentlyShapedRowsThatRenderToTheSameDelimitedString() {
      // Both branches write "RETURN *", so the parse-time column-name check that UNION otherwise
      // enforces (all branches must share the same return column names) passes trivially - but the
      // *actual* per-row property sets still differ at runtime, exactly like the reported example.
      final List<Result> rows = new ArrayList<>();
      try (ResultSet rs = database.query("cypher", """
          WITH '1|b=2' AS a RETURN *
          UNION
          WITH 1 AS a, 2 AS b RETURN *""")) {
        while (rs.hasNext())
          rows.add(rs.next());
      }

      assertThat(rows).hasSize(2);
      assertThat(rows).anySatisfy(row -> {
        assertThat(row.getPropertyNames()).containsExactly("a");
        assertThat(row.<String>getProperty("a")).isEqualTo("1|b=2");
      });
      assertThat(rows).anySatisfy(row -> {
        assertThat(row.getPropertyNames()).containsExactlyInAnyOrder("a", "b");
        assertThat(row.<Number>getProperty("a").intValue()).isEqualTo(1);
        assertThat(row.<Number>getProperty("b").intValue()).isEqualTo(2);
      });
    }

    /**
     * Sanity check: two rows that really do carry equal names/values must still collapse under UNION's
     * implicit DISTINCT.
     */
    @Test
    void unionStillCollapsesTrueDuplicates() {
      final List<Result> rows = new ArrayList<>();
      try (ResultSet rs = database.query("cypher", """
          RETURN 1 AS a, 2 AS b
          UNION
          RETURN 1 AS a, 2 AS b""")) {
        while (rs.hasNext())
          rows.add(rs.next());
      }
      assertThat(rows).hasSize(1);
    }
  }

  /**
   * Issue #6676: Cypher {@code GROUP BY} (the implicit grouping {@link
   * com.arcadedb.query.opencypher.executor.steps.GroupByAggregationStep} performs when a RETURN/WITH clause mixes
   * aggregations with non-aggregated expressions) keyed its groups on the raw boxed grouping value instead of
   * canonicalizing numeric types the way its sibling DISTINCT paths do (issue #5789, {@link
   * com.arcadedb.function.DistinctNumericKey}). A value present as different numeric runtime types (Integer 1 vs Long 1
   * vs Double 1.0) therefore split into separate groups, even though {@code 1 = 1.0} evaluates to {@code true}.
   */
  @Nested
  class Issue6676NumericGroupByEquality {
    private Database database;

    @BeforeEach
    void setUp() {
      database = new DatabaseFactory("./target/databases/issue6676-numeric-groupby").create();
    }

    @AfterEach
    void tearDown() {
      if (database != null) {
        database.drop();
        database = null;
      }
    }

    @Test
    void singleKeyGroupByCollapsesNumericallyEqualCrossTypeValues() {
      try (final ResultSet rs = database.query("opencypher", "UNWIND [1, 1.0, 1] AS x RETURN x, count(*) AS c")) {
        assertThat(rs.hasNext()).isTrue();
        final Result row = rs.next();
        assertThat(((Number) row.getProperty("x")).doubleValue()).isEqualTo(1.0);
        assertThat(((Number) row.getProperty("c")).longValue()).isEqualTo(3L);
        assertThat(rs.hasNext()).isFalse();
      }
    }

    @Test
    void singleKeyGroupByStillSeparatesNumericallyDifferentValues() {
      try (final ResultSet rs = database.query("opencypher", "UNWIND [1, 1.0, 2] AS x RETURN x, count(*) AS c ORDER BY c DESC")) {
        assertThat(rs.hasNext()).isTrue();
        final Result first = rs.next();
        assertThat(((Number) first.getProperty("c")).longValue()).isEqualTo(2L);
        assertThat(rs.hasNext()).isTrue();
        final Result second = rs.next();
        assertThat(((Number) second.getProperty("c")).longValue()).isEqualTo(1L);
        assertThat(rs.hasNext()).isFalse();
      }
    }

    @Test
    void multiKeyGroupByCollapsesNumericallyEqualCrossTypeValues() {
      try (final ResultSet rs = database.query("opencypher",
          "UNWIND [[1, 'a'], [1.0, 'a'], [2, 'b']] AS pair RETURN pair[0] AS n, pair[1] AS label, count(*) AS c")) {
        long total = 0;
        int rows = 0;
        while (rs.hasNext()) {
          final Result row = rs.next();
          total += ((Number) row.getProperty("c")).longValue();
          rows++;
        }
        assertThat(rows).isEqualTo(2); // (1/1.0, 'a') collapses into one group, (2, 'b') is the other
        assertThat(total).isEqualTo(3);
      }
    }
  }

  /**
   * The {@code coll} list functions must compare elements the way Cypher's {@code =} and {@code IN} do, so {@code 1}
   * and {@code 1.0} are the same element (issue #8561).
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue8561CollNumericEquality extends TestHelper {

    private Object one(final String query) {
      try (final ResultSet rs = database.query("opencypher", query)) {
        return rs.next().getProperty("r");
      }
    }

    @Test
    void indexOfMatchesAcrossNumericTypes() {
      assertThat(((Number) one("RETURN coll.indexOf([1.0, 2], 1) AS r")).longValue()).isEqualTo(0L);
      assertThat(((Number) one("RETURN coll.indexOf([1, 2.0], 2) AS r")).longValue()).isEqualTo(1L);
      assertThat(((Number) one("RETURN coll.indexOf([1, 2], 3) AS r")).longValue()).isEqualTo(-1L);
    }

    @Test
    void distinctCollapsesNumericTwins() {
      assertThat((List<?>) one("RETURN coll.distinct([1, 1.0, 2, 'a', 'a']) AS r")).hasSize(3);
      assertThat(((Number) one("RETURN size(coll.toSet([1, 1.0])) AS r")).longValue()).isEqualTo(1L);
    }

    @Test
    void nestedNumericsAreComparedLikeCypherEquality() {
      assertThat((List<?>) one("RETURN coll.distinct([[1], [1.0], {a: 1}, {a: 1.0}]) AS r")).hasSize(2);
      assertThat(((Number) one("RETURN coll.indexOf([[1.0, 2]], [1, 2]) AS r")).longValue()).isEqualTo(0L);
    }

    @Test
    void rangeIndexOfAcceptsAnIntegralFloat() {
      assertThat(((Number) one("RETURN coll.indexOf(range(1, 5), 2.0) AS r")).longValue()).isEqualTo(1L);
      assertThat(((Number) one("RETURN coll.indexOf(range(1, 5), 2.5) AS r")).longValue()).isEqualTo(-1L);
    }

    @Test
    void distinctAndGroupByFollowNestedCypherEquality() {
      try (final ResultSet rs = database.query("opencypher", "UNWIND [[1], [1.0], [2]] AS x RETURN count(DISTINCT x) AS r")) {
        assertThat(((Number) rs.next().getProperty("r")).longValue()).isEqualTo(2L);
      }
      try (final ResultSet rs = database.query("opencypher", "UNWIND [[1], [1.0], [2]] AS x RETURN x, count(*) AS c ORDER BY c DESC")) {
        assertThat(((Number) rs.next().getProperty("c")).longValue()).isEqualTo(2L);
        assertThat(((Number) rs.next().getProperty("c")).longValue()).isEqualTo(1L);
        assertThat(rs.hasNext()).isFalse();
      }
    }

    @Test
    void unionCollapsesNumericTwins() {
      assertThat((List<?>) one("RETURN coll.union([1], [1.0, 2]) AS r")).hasSize(2);
    }
  }

  /**
   * Issue #10152: {@code collect(DISTINCT x)} over a variable-length hop answers the same SET of values whether the hop is walked path
   * by path or with a visited set, but its list carries the order the walk met the values in, which is depth first for the first and
   * breadth first for the second. Cypher promises no order for {@code collect} without an {@code ORDER BY} (the reference
   * documentation says so), so the shortcut stays - dropping it would bring back the walk over every path for the most common way to
   * ask "everything this reaches" - and what is pinned is what the shortcut does promise:
   * <ul>
   *   <li>the values, each once, are those of the path enumeration;</li>
   *   <li>a statement that reads the ORDER of the list is not one of the shapes the shortcut takes, so it keeps the enumeration order.</li>
   * </ul>
   * The differential fuzz of issue #10062 compares the lists of a {@code collect} as sets over random multigraphs.
   * <p>
   * The graph has four vertices 0..3 and the edges 0-&gt;1, 0-&gt;2, 2-&gt;0: the list was [2, 1, 0] without a named path and [2, 0, 1] with one.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue10152CollectDistinctEndpointsOrder extends TestHelper {
    private static final String ENDPOINTS = "DISTINCT ENDPOINTS";

    @Override
    protected void beginTest() {
      database.transaction(() -> {
        database.getSchema().createVertexType("V").createProperty("id", Type.INTEGER);
        database.getSchema().createEdgeType("E");
        final List<MutableVertex> vertices = new ArrayList<>();
        for (int i = 0; i < 4; i++)
          vertices.add(database.newVertex("V").set("id", i).save());
        vertices.get(0).newEdge("E", vertices.get(1)).save();
        vertices.get(0).newEdge("E", vertices.get(2)).save();
        vertices.get(2).newEdge("E", vertices.get(0)).save();
      });
    }

    @Test
    void theListHoldsTheValuesOfThePathEnumerationEachOnce() {
      final String walked = "MATCH (a:V {id: 0})-[:E*1..2]->(b:V) RETURN a.id AS s, collect(DISTINCT b.id) AS l";
      final String enumerated = "MATCH p = (a:V {id: 0})-[:E*1..2]->(b:V) RETURN a.id AS s, collect(DISTINCT b.id) AS l";
      assertThat(planOf(walked)).contains(ENDPOINTS);
      assertThat(planOf(enumerated)).doesNotContain(ENDPOINTS);

      final List<Object> fromWalk = list(walked);
      assertThat(fromWalk).containsExactlyInAnyOrder(0, 1, 2);
      assertThat(fromWalk).containsExactlyInAnyOrderElementsOf(list(enumerated));
    }

    @Test
    void aStatementThatReadsTheOrderOfTheListKeepsTheEnumerationOrder() {
      for (final String projection : new String[] { "head(collect(DISTINCT b.id))", "collect(DISTINCT b.id)[0]",
          "collect(DISTINCT b.id)[0..1]", "last(collect(DISTINCT b.id))" }) {
        final String walked = "MATCH (a:V {id: 0})-[:E*1..2]->(b:V) RETURN a.id AS s, " + projection + " AS x";
        final String enumerated = "MATCH p = (a:V {id: 0})-[:E*1..2]->(b:V) RETURN a.id AS s, " + projection + " AS x";
        assertThat(planOf(walked)).as(walked).doesNotContain(ENDPOINTS);
        assertThat(rows(walked)).as(walked).isEqualTo(rows(enumerated));
      }
    }

    private String planOf(final String query) {
      try (final ResultSet resultSet = database.query("opencypher", "EXPLAIN " + query)) {
        return String.valueOf(resultSet.next().<Object>getProperty("executionPlan"));
      }
    }

    private List<Object> list(final String query) {
      try (final ResultSet resultSet = database.query("opencypher", query)) {
        final Result row = resultSet.next();
        final List<Object> values = new ArrayList<>(row.<List<Object>>getProperty("l"));
        assertThat(resultSet.hasNext()).isFalse();
        return values;
      }
    }

    private List<String> rows(final String query) {
      final List<String> rows = new ArrayList<>();
      try (final ResultSet resultSet = database.query("opencypher", query)) {
        while (resultSet.hasNext()) {
          final Result row = resultSet.next();
          final List<String> cells = new ArrayList<>();
          for (final String name : row.getPropertyNames())
            cells.add(name + "=" + row.<Object>getProperty(name));
          rows.add(String.join(",", cells));
        }
      }
      Collections.sort(rows);
      return rows;
    }
  }

  /**
   * Issue #10254: a list kept next to an aggregate in a WITH ({@code WITH r1, collect(DISTINCT b) AS r2}) is the very same
   * object on every row, and the grouping canonicalized and hashed it again for each of them, so the query was quadratic.
   * The cost is pinned by counting the element reads of a list passed as a parameter, which reaches every row as the same
   * object: a per-row walk reads it once per row, the memory of the row before reads it a bounded number of times.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue10254ListGroupingKey extends TestHelper {
    private static final int ROWS   = 1_000;
    private static final int LENGTH = 500;

    @Override
    protected void beginTest() {
      database.command("sql", "CREATE VERTEX TYPE A");
      database.transaction(() -> {
        for (int i = 0; i < ROWS; i++)
          database.newVertex("A").set("i", i).set("g", i % 3).save();
      });
    }

    @Test
    void issueShapesAnswerTheSame() {
      final String list = "MATCH (a:A) WITH collect(a) AS r1 MATCH (b:A) WITH r1, collect(DISTINCT b) AS r2 RETURN size(r1) AS s1, size(r2) AS s2";
      final String scalar = "MATCH (a:A) WITH count(a) AS c1 MATCH (b:A) WITH c1, collect(DISTINCT b) AS r2 RETURN c1 AS s1, size(r2) AS s2";
      for (final String query : new String[] { list, scalar }) {
        final Result row = single(query, Map.of());
        assertThat(row.<Number>getProperty("s1").intValue()).isEqualTo(ROWS);
        assertThat(row.<Number>getProperty("s2").intValue()).isEqualTo(ROWS);
      }

      // the chain of the discussion that found it: each WITH keeps the lists of the WITH before as keys
      final Result chained = single("MATCH (a:A) WITH collect(a) AS r1 MATCH (b:A) WITH r1, collect(DISTINCT b) AS r2 "
          + "MATCH (c:A) WITH r1, r2, collect(DISTINCT c) AS r3 RETURN size(r1) AS s1, size(r2) AS s2, size(r3) AS s3", Map.of());
      assertThat(chained.<Number>getProperty("s3").intValue()).isEqualTo(ROWS);
    }

    @Test
    void singleListKeyIsNotWalkedPerRow() {
      final CountingList key = new CountingList(LENGTH);
      final Result row = single("MATCH (b:A) WITH $l AS r1, b WITH r1, collect(DISTINCT b) AS r2 RETURN size(r1) AS s1, size(r2) AS s2",
          Map.of("l", key));
      assertThat(row.<Number>getProperty("s1").intValue()).isEqualTo(LENGTH);
      assertThat(row.<Number>getProperty("s2").intValue()).isEqualTo(ROWS);
      assertBounded(key);
    }

    @Test
    void listKeyNextToAVaryingKeyIsNotWalkedPerRow() {
      final CountingList key = new CountingList(LENGTH);
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (b:A) WITH $l AS r1, b WITH r1, b.g AS g, count(b) AS n RETURN size(r1) AS s, g, n ORDER BY g", Map.of("l", key))) {
        final List<Long> counts = new ArrayList<>();
        while (rs.hasNext()) {
          final Result row = rs.next();
          assertThat(row.<Number>getProperty("s").intValue()).isEqualTo(LENGTH);
          counts.add(row.<Number>getProperty("n").longValue());
        }
        assertThat(counts).containsExactly(334L, 333L, 333L);
      }
      assertBounded(key);
    }

    @Test
    void listKeyOfAReturnAggregationIsNotWalkedPerRow() {
      // grouped straight off the label scan, which can aggregate in the workers of a parallel scan
      final CountingList key = new CountingList(LENGTH);
      final Result row = single("MATCH (b:A) RETURN $l AS r1, count(b) AS n", Map.of("l", key));
      assertThat(row.<Number>getProperty("n").longValue()).isEqualTo(ROWS);
      assertBounded(key);
    }

    @Test
    void distinctOverAListIsNotWalkedPerRow() {
      final CountingList key = new CountingList(LENGTH);
      final Result with = single("MATCH (b:A) WITH DISTINCT $l AS r1, b.g AS g RETURN count(*) AS n", Map.of("l", key));
      assertThat(with.<Number>getProperty("n").longValue()).isEqualTo(3L);
      assertBounded(key);

      final CountingList returned = new CountingList(LENGTH);
      try (final ResultSet rs = database.query("opencypher", "MATCH (b:A) RETURN DISTINCT $l AS r1, b.g AS g", Map.of("l", returned))) {
        int rows = 0;
        while (rs.hasNext()) {
          rs.next();
          rows++;
        }
        assertThat(rows).isEqualTo(3);
      }
      assertBounded(returned);
    }

    @Test
    void equalListsOfDifferentObjectsStillShareTheirGroup() {
      // [1] and [1.0] are equal under Cypher's = (issue #8561), and two literals are two objects
      try (final ResultSet rs = database.query("opencypher",
          "UNWIND [[1], [1.0], [2], [1], [2, 3], [2]] AS l RETURN l, count(*) AS n ORDER BY n DESC, size(l)")) {
        final List<Long> counts = new ArrayList<>();
        while (rs.hasNext())
          counts.add(rs.next().<Number>getProperty("n").longValue());
        assertThat(counts).containsExactly(3L, 2L, 1L);
      }
      try (final ResultSet rs = database.query("opencypher",
          "UNWIND [null, 1, null, 1.0, null] AS x WITH x, 'k' AS k RETURN x, k, count(*) AS n ORDER BY n DESC")) {
        final List<Long> counts = new ArrayList<>();
        while (rs.hasNext())
          counts.add(rs.next().<Number>getProperty("n").longValue());
        assertThat(counts).containsExactly(3L, 2L);
      }
      try (final ResultSet rs = database.query("opencypher", "UNWIND [[1], [1.0], [1], [2]] AS l RETURN DISTINCT l")) {
        int rows = 0;
        while (rs.hasNext()) {
          rs.next();
          rows++;
        }
        assertThat(rows).isEqualTo(2);
      }
    }

    /** A per-row walk reads the list ROWS times over; a bounded number of walks is what the memory of the row before costs. */
    private static void assertBounded(final CountingList key) {
      assertThat(key.reads).as("element reads of a %d-long list over %d rows", LENGTH, ROWS).isLessThan(20L * LENGTH);
    }

    private Result single(final String query, final Map<String, Object> parameters) {
      try (final ResultSet rs = database.query("opencypher", query, parameters)) {
        return rs.next();
      }
    }

    /** A list of distinct longs that counts how many times its elements are read. */
    private static final class CountingList extends AbstractList<Object> {
      private final int  size;
      private       long reads;

      CountingList(final int size) {
        this.size = size;
      }

      @Override
      public Object get(final int index) {
        if (index < 0 || index >= size)
          throw new IndexOutOfBoundsException(index);
        reads++;
        return (long) index;
      }

      @Override
      public int size() {
        return size;
      }
    }
  }
}
