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
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.JsonSerializer;
import com.arcadedb.serializer.json.JSONObject;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * null handling in writes, property reads, nullIf and boolean functions.
 * <p>
 * Each nested class holds the regression tests of the GitHub issue its name carries.
 */
class CypherNullHandlingTest {
  /**
   * Regression test for issue #5207: nullIf() on a node property returned the value instead of null
   * because the stored property (Integer) was compared to the literal (Long) with type-strict equality.
   */
  @Nested
  class Issue5207 extends TestHelper {
    @Test
    void nullIfOnPropertyValueMatchesLiteral() {
      database.command("opencypher", "CREATE (:A {v:1}), (:A {v:2})");

      final List<Object> results = new ArrayList<>();
      try (final ResultSet rs = database.command("opencypher",
          "MATCH (a:A) RETURN a.v AS v, nullIf(a.v, 1) AS result ORDER BY v")) {
        while (rs.hasNext()) {
          final Result r = rs.next();
          results.add(r.getProperty("result"));
        }
      }

      assertThat(results).hasSize(2);
      assertThat(results.get(0)).isNull();                            // v=1 -> nullIf(1,1)=null
      assertThat(((Number) results.get(1)).intValue()).isEqualTo(2);  // v=2 -> 2
    }

    @Test
    void nullIfCrossNumericTypeEquality() {
      // Integer property vs Double literal: Cypher value equality treats 1 = 1.0 as equal.
      database.command("opencypher", "CREATE (:B {v:1})");
      try (final ResultSet rs = database.command("opencypher", "MATCH (b:B) RETURN nullIf(b.v, 1.0) AS result")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().<Object>getProperty("result")).isNull();
      }
    }

    @Test
    void nullIfOnPropertyValueDiffers() {
      database.command("opencypher", "CREATE (:C {v:5})");
      try (final ResultSet rs = database.command("opencypher", "MATCH (c:C) RETURN nullIf(c.v, 9) AS result")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(((Number) rs.next().getProperty("result")).intValue()).isEqualTo(5);
      }
    }

    @Test
    void nullIfLiteralsStillWork() {
      try (final ResultSet rs = database.command("opencypher", "RETURN nullIf(1, 1) AS a, nullIf(1, 2) AS b")) {
        assertThat(rs.hasNext()).isTrue();
        final Result r = rs.next();
        assertThat(r.<Object>getProperty("a")).isNull();
        assertThat(((Number) r.getProperty("b")).intValue()).isEqualTo(1);
      }
    }

    @Test
    void nullIfWithStringProperty() {
      database.command("opencypher", "CREATE (:D {name:'Brown'})");
      try (final ResultSet rs = database.command("opencypher",
          "MATCH (d:D) RETURN coalesce(nullIf(d.name, 'Brown'), 'Hazel') AS color")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().<String>getProperty("color")).isEqualTo("Hazel");
      }
    }
  }

  /**
   * Regression for issue #5383: {@code true OR null} returns {@code null} when the null
   * operand is derived from a function call.
   */
  @Nested
  class Issue5383BooleanNullFunction {
    private Database database;

    @BeforeEach
    void setUp() {
      final DatabaseFactory factory = new DatabaseFactory("./target/databases/testIssue5383");
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
    }

    @AfterEach
    void tearDown() {
      if (database != null) {
        database.drop();
        database = null;
      }
    }

    @Test
    void literalNullOrWorks() {
      assertThat(scalar("RETURN (null IS NULL OR null = null) AS v")).isEqualTo(true);
    }

    @Test
    void toStringNullOr() {
      assertThat(scalar("RETURN (toString(null) IS NULL OR toString(null) = '') AS v")).isEqualTo(true);
    }

    @Test
    void lTrimNullOr() {
      assertThat(scalar("RETURN (lTrim(null) IS NULL OR lTrim(null) = '') AS v")).isEqualTo(true);
    }

    @Test
    void substringNullOr() {
      assertThat(scalar("RETURN (substring(null, 0, 3) IS NULL OR substring(null, 0, 3) = '') AS v")).isEqualTo(true);
    }

    @Test
    void parenthesizedFunctionIsNullYieldsConcreteBoolean() {
      // The parenthesized wrapper must not collapse the whole expression to just the inner
      // function call: IS NULL over a non-null function result is a concrete false.
      assertThat(scalar("RETURN (toString('x') IS NULL) AS v")).isEqualTo(false);
      assertThat(scalar("RETURN (toString(null) IS NULL) AS v")).isEqualTo(true);
    }

    @Test
    void parenthesizedTrueOrFunctionIsTrue() {
      // true OR <anything> must short-circuit to true even when the right operand contains a
      // null-returning function call inside the parentheses.
      assertThat(scalar("RETURN (true OR toString(null) = '') AS v")).isEqualTo(true);
    }

    @Test
    void nestedParenthesesFunctionOr() {
      assertThat(scalar("RETURN ((toString(null) IS NULL OR toString(null) = '')) AS v")).isEqualTo(true);
    }

    @Test
    void withProjectionParenthesizedFunctionOr() {
      assertThat(scalar(
          "WITH (toString(null) IS NULL OR toString(null) = '') AS v RETURN v")).isEqualTo(true);
    }

    private Object scalar(final String cypher) {
      try (final ResultSet rs = database.query("opencypher", cypher)) {
        assertThat(rs.hasNext()).as("query returned no rows: %s", cypher).isTrue();
        final Result r = rs.next();
        return r.getProperty("v");
      }
    }
  }

  /**
   * Reproduces issue #5613: returning a whole vertex from openCypher also emitted every declared
   * property of the vertex's type as a top-level column, always null. `RETURN f` yielded five columns
   * where four were null, while the equivalent SQL MATCH yielded one.
   * <p>
   * The row carried both a backing element (the vertex) and content ({@code {"f": vertex}}).
   * {@code getPropertyNames()} returned the union of both, while {@code getProperty()} deliberately
   * ignores the element once content is present, so every element-only name resolved to null.
   */
  @Nested
  class Issue5613WholeVertexNullColumns {
    private Database database;

    @BeforeEach
    void setUp() {
      // create() throws when the directory survives, so a crash before tearDown would break every later run.
      final DatabaseFactory factory = new DatabaseFactory("./target/databases/testissue5613");
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
      database.command("sql", "CREATE VERTEX TYPE Person");
      database.command("sql", "CREATE PROPERTY Person.id LONG");
      database.command("sql", "CREATE PROPERTY Person.name STRING");
      database.command("sql", "CREATE PROPERTY Person.age INTEGER");
      database.command("sql", "CREATE PROPERTY Person.city STRING");
      database.command("sql", "CREATE INDEX ON Person (id) UNIQUE");
      database.command("sql", "CREATE EDGE TYPE KNOWS");

      database.transaction(() -> {
        for (int i = 1; i <= 3; i++)
          database.command("sql", "INSERT INTO Person SET id = ?, name = ?, age = ?, city = ?", i, "n" + i, 20 + i, "c" + i);
        database.command("sql", "CREATE EDGE KNOWS FROM (SELECT FROM Person WHERE id = 1) TO (SELECT FROM Person WHERE id = 2)");
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
    void returningWholeVertexEmitsOnlyTheAliasColumn() {
      final ResultSet rs = database.query("opencypher", "MATCH (f:Person) WHERE f.id = 1 RETURN f");

      assertThat(rs.hasNext()).isTrue();
      final Result result = rs.next();

      assertThat(result.getPropertyNames()).containsExactly("f");
      assertThat(result.<Object>getProperty("f")).isNotNull();
      assertThat(rs.hasNext()).isFalse();
    }

    @Test
    void returningWholeVertexOverATraversalEmitsOnlyTheAliasColumn() {
      final ResultSet rs = database.query("opencypher",
          "MATCH (p:Person)-[:KNOWS]->(f:Person) WHERE p.id = 1 RETURN DISTINCT f");

      assertThat(rs.hasNext()).isTrue();
      final Result result = rs.next();

      assertThat(result.getPropertyNames()).containsExactly("f");
      assertThat(rs.hasNext()).isFalse();
    }

    @Test
    void returningAScalarProjectionStaysClean() {
      final ResultSet rs = database.query("opencypher",
          "MATCH (p:Person)-[:KNOWS]->(f:Person) WHERE p.id = 1 RETURN f.name");

      assertThat(rs.hasNext()).isTrue();
      final Result result = rs.next();

      assertThat(result.getPropertyNames()).containsExactly("f.name");
      assertThat(result.<String>getProperty("f.name")).isEqualTo("n2");
    }

    /**
     * The whole-vertex row keeps its backing element so {@code isElement()} / {@code toElement()} and
     * the JSON flattening added for issue #3391 keep working.
     */
    @Test
    void wholeVertexRowStillExposesTheBackingElement() {
      final ResultSet rs = database.query("opencypher", "MATCH (f:Person) WHERE f.id = 1 RETURN f");
      final Result result = rs.next();

      assertThat(result.isElement()).isTrue();
      assertThat(result.toElement().getString("name")).isEqualTo("n1");

      final JSONObject json = JsonSerializer.createJsonSerializer().serializeResult(database, result);
      assertThat(json.getString("name")).isEqualTo("n1");
      assertThat(json.getLong("id")).isEqualTo(1L);
      assertThat(json.getInt("age")).isEqualTo(21);
      assertThat(json.getString("city")).isEqualTo("c1");
      // The vertex is flattened, never nested under the alias, so no stray column survives on the
      // surfaces that build their columns from the row itself (Postgres, gRPC, Bolt).
      assertThat(json.has("f")).isFalse();
    }

    /**
     * The invariant the fix establishes, asserted directly on {@link ResultInternal} so it holds
     * independently of the Cypher engine: {@code getPropertyNames()} must never advertise a column that
     * {@code getProperty()} cannot resolve. Its guard is the exact complement of the content-precedence
     * branch in {@code getProperty()}, and this test fails if a future edit desyncs the two.
     */
    @Test
    void everyListedColumnIsResolvableByGetProperty() {
      final Document person = database.query("sql", "SELECT FROM Person WHERE id = 1").next().getElement().get();

      final ResultInternal row = new ResultInternal();
      row.setProperty("f", person);
      row.setElement(person);

      assertThat(row.getPropertyNames()).containsExactly("f");
      for (final String name : row.getPropertyNames())
        assertThat(row.<Object>getProperty(name)).as("column %s is listed so it must resolve", name).isNotNull();
    }

    /**
     * {@code toMap()} keeps the opposite (element-first) precedence on purpose: the JSON flattening of a
     * whole-entity projection goes through it. Aligning it with {@code getPropertyNames()} would nest the
     * vertex under its alias and bring the null columns back from the other side.
     */
    @Test
    void wholeVertexRowStillFlattensThroughToMap() {
      final Result result = database.query("opencypher", "MATCH (f:Person) WHERE f.id = 1 RETURN f").next();

      assertThat(result.getPropertyNames()).containsExactly("f");
      assertThat(result.toMap()).containsKeys("id", "name", "age", "city").doesNotContainKey("f");
      assertThat(result.toMap()).containsEntry("name", "n1");
    }

    /**
     * The point of the issue: openCypher and the equivalent SQL MATCH must describe the same answer
     * with the same columns.
     */
    @Test
    void openCypherAndSqlMatchAgreeOnTheColumnList() {
      final Result cypher = database.query("opencypher",
          "MATCH (p:Person)-[:KNOWS]->(f:Person) WHERE p.id = 1 RETURN DISTINCT f").next();
      final Result sql = database.query("sql",
          "MATCH {type: Person, as: p, where: (id = 1)}.out('KNOWS'){as: f} RETURN DISTINCT f").next();

      assertThat(cypher.getPropertyNames()).isEqualTo(sql.getPropertyNames());
    }

    /**
     * The fix narrows {@code ResultInternal.getPropertyNames()}, which SQL shares. A plain SELECT *
     * seeds content with every element property, so its column list must be unaffected.
     */
    @Test
    void sqlSelectStarStillListsEveryColumn() {
      final Result result = database.query("sql", "SELECT * FROM Person WHERE id = 1").next();

      assertThat(result.getPropertyNames()).contains("id", "name", "age", "city");
      assertThat(result.<String>getProperty("name")).isEqualTo("n1");
      assertThat(result.<Integer>getProperty("age")).isEqualTo(21);
    }

    /**
     * RETURN * takes the sibling branch in FinalProjectionStep and must not leak the columns either.
     */
    @Test
    void returnStarOverAWholeVertexEmitsOnlyTheAliasColumn() {
      final ResultSet rs = database.query("opencypher", "MATCH (f:Person) WHERE f.id = 1 RETURN *");

      assertThat(rs.hasNext()).isTrue();
      final Result result = rs.next();

      assertThat(result.getPropertyNames()).containsExactly("f");
    }
  }

  /**
   * Issue #10191: null values in Cypher write patterns. Each expected outcome was checked on Neo4j 2026.09:
   * <ul>
   * <li>a relationship endpoint bound to null, or to something that is not a node, fails CREATE and MERGE alike ("Expected
   * the value NULL to be of type NODE") - it used to create an anonymous node in its place (CREATE) or silently skip the row
   * (MERGE);</li>
   * <li>a null property in a MERGE pattern fails, whether it is written as a literal, comes through a variable or a
   * parameter, or reads a property that is missing, on a node or on a relationship - only the literal used to be refused,
   * and the others created a node without the property, a new one per row;</li>
   * <li>a null property in a CREATE pattern is no error: the property is simply not set.</li>
   * </ul>
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue10191NullWritePatterns extends TestHelper {

    @Override
    protected void beginTest() {
      database.transaction(() -> {
        database.getSchema().createVertexType("A");
        database.getSchema().createVertexType("B");
        database.getSchema().createVertexType("M");
        database.getSchema().createEdgeType("R");
        database.command("opencypher", "CREATE (:A {k: 0})").close();
      });
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "OPTIONAL MATCH (a:A {k: 99}) CREATE (a)-[:R]->(b:B {k: 5})",
        "OPTIONAL MATCH (a:A {k: 99}) CREATE (b:B {k: 5})-[:R]->(a)",
        "OPTIONAL MATCH (a:A {k: 99}) CREATE (a)-[:R]->(a)",
        "OPTIONAL MATCH (a:A {k: 0}) OPTIONAL MATCH (z:A {k: 99}) CREATE (a)-[:R]->(m:M)-[:R]->(z)",
        "OPTIONAL MATCH (a:A {k: 99}) MERGE (a)-[:R]->(b:B {k: 5})",
        "OPTIONAL MATCH (a:A {k: 99}) MERGE (b:B {k: 5})<-[:R]-(a)",
        "OPTIONAL MATCH (a:A {k: 99}) WITH a MERGE (a)-[:R]->(b:B {k: 5})" })
    void aNullEndpointFailsTheStatement(final String cypher) {
      assertThatThrownBy(() -> runInTransaction(cypher)).hasMessageContaining("Expected the value NULL to be of type NODE");
      assertUnchanged();
    }

    /** The validator refuses a variable it knows to be a scalar; one that arrives through a parameter is refused at run time. */
    @ParameterizedTest
    @ValueSource(strings = { "WITH 1 AS a CREATE (a)-[:R]->(b:B)", "WITH 'x' AS a MERGE (a)-[:R]->(b:B)",
        "WITH $p AS a CREATE (a)-[:R]->(b:B)", "WITH $p AS a MERGE (a)-[:R]->(b:B)" })
    void anEndpointThatIsNotANodeFailsTheStatement(final String cypher) {
      assertThatThrownBy(() -> database.transaction(() -> database.command("opencypher", cypher, Map.of("p", 1)).close()))
          .hasMessageContaining("NODE");
      assertUnchanged();
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "MERGE (n:A {k: null})",
        "UNWIND [null, null] AS x MERGE (n:A {k: x})",
        "MATCH (a:A) MERGE (n:B {k: a.missing})",
        "MATCH (a:A) MERGE (n:B {k: 1, j: a.missing})",
        "MATCH (a:A) MERGE (a)-[:R]->(b:B {w: null})",
        "MATCH (a:A) MERGE (a)-[:R {w: null}]->(b:B)",
        "MATCH (a:A) WITH a, null AS x MERGE (a)-[:R {w: x}]->(b:B)" })
    void aNullMergePropertyFailsTheStatement(final String cypher) {
      assertThatThrownBy(() -> runInTransaction(cypher)).hasMessageContaining("cannot be used with a graph element property value that is null");
      assertUnchanged();
    }

    @Test
    void aNullMergePropertyFromAParameterFailsTheStatement() {
      final Map<String, Object> parameters = new HashMap<>();
      parameters.put("p", null);
      assertThatThrownBy(() -> database.transaction(() -> database.command("opencypher", "MERGE (n:A {k: $p})", parameters).close()))
          .hasMessageContaining("cannot be used with a graph element property value that is null");
      assertUnchanged();
    }

    /** An endpoint bound to a bare RID is the vertex it names: the pattern connects it rather than a new node. */
    @Test
    void anEndpointBoundToARidIsTheVertexItNames() {
      final RID a;
      try (final ResultSet rs = database.query("sql", "SELECT FROM A WHERE k = 0")) {
        a = rs.next().getIdentity().get();
      }
      final Map<String, Object> parameters = Map.of("a", a);

      database.transaction(() -> database.command("opencypher", "WITH $a AS a MERGE (a)-[:R]->(b:B {k: 9})", parameters).close());
      database.transaction(() -> database.command("opencypher", "WITH $a AS a MERGE (a)-[:R]->(b:B {k: 9})", parameters).close());
      database.transaction(() -> database.command("opencypher", "WITH $a AS a CREATE (a)-[:R]->(:M {k: 1})", parameters).close());

      assertThat(graph()).containsExactly("[A]{k=0}", "[B]{k=9}", "[M]{k=1}");
      try (final ResultSet rs = database.query("opencypher", "MATCH (x:A {k: 0})-[r:R]->() RETURN count(r) AS c")) {
        assertThat(rs.next().<Number>getProperty("c").longValue()).isEqualTo(2L);
      }
    }

    @Test
    void aNullCreatePropertyIsJustNotSet() {
      final List<Result> rows = runInTransaction("MATCH (a:A) CREATE (n:B {k: a.missing, j: 1}) RETURN n");
      assertThat(rows).hasSize(1);
      assertThat(graph()).containsExactly("[A]{k=0}", "[B]{j=1}");
    }

    @Test
    void aNullSetAfterTheMergeIsNoError() {
      runInTransaction("MATCH (a:A) MERGE (a)-[:R]->(b:B) ON CREATE SET b.k = null RETURN b");
      assertThat(graph()).containsExactly("[A]{k=0}", "[B]{}");
    }

    @Test
    void aMergePropertyThatIsNotNullStillMerges() {
      runInTransaction("UNWIND [1, 1, 2] AS x MERGE (n:B {k: x})");
      assertThat(graph()).containsExactly("[A]{k=0}", "[B]{k=1}", "[B]{k=2}");
    }

    private void assertUnchanged() {
      assertThat(graph()).containsExactly("[A]{k=0}");
      try (final ResultSet rs = database.query("opencypher", "MATCH ()-[r]->() RETURN count(r) AS c")) {
        assertThat(rs.next().<Number>getProperty("c").longValue()).isZero();
      }
    }

    private List<Result> runInTransaction(final String cypher) {
      final List<Result> rows = new ArrayList<>();
      database.transaction(() -> {
        try (final ResultSet rs = database.command("opencypher", cypher)) {
          while (rs.hasNext())
            rows.add(rs.next());
        }
      });
      return rows;
    }

    private List<String> graph() {
      final List<String> out = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (n) RETURN labels(n) AS l, properties(n) AS p ORDER BY labels(n)[0], n.k")) {
        while (rs.hasNext()) {
          final Result r = rs.next();
          out.add(r.getProperty("l") + "" + new TreeMap<>(r.<Map<String, Object>>getProperty("p")));
        }
      }
      return out;
    }
  }

  /**
   * #10194: an explicit NULL on a SQL INSERT is stored as NULL, but in Cypher a null in a property map means "no property" (Neo4j
   * never stores a null), so a property left null in {@code CREATE (n:T {df: null})} is absent and the schema default still fills it.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue10194CypherNullPropertyDefault extends TestHelper {

    @BeforeEach
    void createType() {
      database.command("sql", "CREATE VERTEX TYPE T").close();
      database.command("sql", "CREATE PROPERTY T.id LONG").close();
      database.command("sql", "CREATE PROPERTY T.df INTEGER (default 7)").close();
    }

    private Document only() {
      try (final ResultSet rs = database.query("sql", "SELECT FROM T")) {
        final Document doc = rs.next().getElement().orElseThrow();
        assertThat(rs.hasNext()).isFalse();
        return doc;
      }
    }

    @Test
    void aNullInTheCreatePatternIsNoPropertyAndTheDefaultFillsIt() {
      database.transaction(() -> database.command("opencypher", "CREATE (n:T {id: 1, df: null})").close());

      assertThat(only().getInteger("df")).isEqualTo(7);
    }

    @Test
    void anOmittedPropertyGetsTheDefault() {
      database.transaction(() -> database.command("opencypher", "CREATE (n:T {id: 1})").close());

      assertThat(only().getInteger("df")).isEqualTo(7);
    }

    @Test
    void aNullParameterIsNoPropertyEither() {
      final Map<String, Object> parameters = new HashMap<>();
      parameters.put("df", null);

      database.transaction(() -> database.command("opencypher", "CREATE (n:T {id: 1, df: $df})", parameters).close());

      assertThat(only().getInteger("df")).isEqualTo(7);
    }
  }
}
