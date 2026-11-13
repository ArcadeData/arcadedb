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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.query.OpenCypherQueryEngine;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * RETURN * / WITH * / YIELD *: hidden internal variables, subquery scope and declared columns.
 * <p>
 * Each nested class holds the regression tests of the GitHub issue its name carries.
 */
class CypherReturnStarTest {
  /**
   * Issue #5444: {@code RETURN *} projects the variables the query wrote. The executor also binds
   * anonymous pattern elements to generated variables ({@code   __anon0}, {@code   src0}, ...), and
   * those were passed straight through to the caller, who never named them and cannot reference them.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue5444ReturnStarInternalVariables {
    private static final String DATABASE_PATH = "./target/databases/issue-5444-return-star";

    private Database database;

    @BeforeEach
    void setUp() {
      database = new DatabaseFactory(DATABASE_PATH).create();
      database.getSchema().createVertexType("Node").createProperty("id", Type.STRING);
      database.getSchema().createEdgeType("LINK");

      database.transaction(() -> {
        final MutableVertex hub = database.newVertex("Node").set("id", "hub").save();
        for (final String id : List.of("a", "b")) {
          final MutableVertex leaf = database.newVertex("Node").set("id", id).save();
          leaf.newEdge("LINK", hub, true, (Object[]) null).save();
        }
      });
    }

    @AfterEach
    void tearDown() {
      if (database != null)
        database.drop();
    }

    @Test
    void returnStarHidesTheAnonymousNodeOfAFixedLengthPattern() {
      assertThat(projectedVariables("MATCH (:Node)-[:LINK]->(hub:Node) WHERE hub.id = 'hub' RETURN *"))
          .hasSize(2)
          .allSatisfy(names -> assertThat(names).noneMatch(InternalVariables::isInternal).contains("hub"));
    }

    @Test
    void returnStarHidesTheAnonymousNodeOfAVariableLengthPattern() {
      assertThat(projectedVariables("MATCH (:Node)-[:LINK*1..2]->(hub:Node) WHERE hub.id = 'hub' RETURN *"))
          .hasSize(2)
          .allSatisfy(names -> assertThat(names).noneMatch(InternalVariables::isInternal).contains("hub"));
    }

    @Test
    void returnStarHidesTheAnonymousRelationshipOfAPattern() {
      assertThat(projectedVariables("MATCH (n:Node)-[:LINK]->(hub:Node) WHERE hub.id = 'hub' RETURN *"))
          .containsExactly(List.of("hub", "n"), List.of("hub", "n"));
    }

    @Test
    void returnDistinctStarIgnoresTheHiddenVariables() {
      // the two anonymous sources differ, so counting them would keep both rows
      try (final ResultSet resultSet = database.query("opencypher",
          "MATCH (:Node)-[:LINK]->(hub:Node) WHERE hub.id = 'hub' RETURN DISTINCT *")) {
        final List<String> ids = new ArrayList<>();
        while (resultSet.hasNext())
          ids.add(resultSet.next().<Vertex>getProperty("hub").getString("id"));
        assertThat(ids).containsExactly("hub");
      }
    }

    @Test
    void returnStarStillProjectsANamedSingleVariable() {
      try (final ResultSet resultSet = database.query("opencypher",
          "MATCH (hub:Node) WHERE hub.id = 'hub' RETURN *")) {
        assertThat(resultSet.hasNext()).isTrue();
        assertThat(resultSet.next().<Vertex>getProperty("hub").getString("id")).isEqualTo("hub");
      }
    }

    private List<List<String>> projectedVariables(final String query) {
      final List<List<String>> rows = new ArrayList<>();
      try (final ResultSet resultSet = database.query("opencypher", query)) {
        while (resultSet.hasNext())
          rows.add(resultSet.next().getPropertyNames().stream().sorted().toList());
      }
      return rows;
    }
  }

  /**
   * Issue #6135: {@code RETURN *} rejected a variable that was in scope only because it was exported by a preceding
   * {@code CALL { }} subquery, a {@code WITH} projection, or a {@code CALL ... YIELD}. The check backing
   * {@code RETURN *} scanned only the statement's own {@code MATCH} patterns and {@code UNWIND} variables, so any
   * other way of putting a name into scope was invisible to it - not because those names are not in scope (the
   * executor projects them correctly once {@code RETURN *} is allowed to run at all), but because the parse-time
   * guard that decides whether {@code RETURN *} is legal never looked at them.
   * <p>
   * Neo4j (the openCypher reference implementation) accepts every query below: a {@code CALL { }} subquery's
   * {@code RETURN} items become ordinary variables of the enclosing scope, exactly like a {@code WITH}.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue6135ReturnStarSubqueryScope extends TestHelper {

    @Override
    protected void beginTest() {
      database.command("opencypher", "CREATE (:P {name: 'a'})-[:KNOWS]->(:P {name: 'b'})");
    }

    @Test
    void returnStarSeesAVariableExportedByACallSubquery() {
      try (final ResultSet rs = database.query("opencypher", "CALL { MATCH (n:P) RETURN n } RETURN *")) {
        assertThat(rs.hasNext()).isTrue();
        final Result row = rs.next();
        assertThat(row.getPropertyNames()).containsExactly("n");
        assertThat(row.<Object>getProperty("n")).isNotNull();
      }
    }

    @Test
    void returnStarSeesAVariableExportedByACallSubqueryAlongsideOuterVariables() {
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (a:P {name: 'a'}) CALL { MATCH (m:P {name: 'b'}) RETURN m } RETURN *")) {
        assertThat(rs.hasNext()).isTrue();
        final Result row = rs.next();
        assertThat(row.getPropertyNames()).containsExactlyInAnyOrder("a", "m");
      }
    }

    @Test
    void returnStarSeesAWithProjectedAlias() {
      try (final ResultSet rs = database.query("opencypher", "WITH 1 AS x RETURN *")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().<Number>getProperty("x").intValue()).isEqualTo(1);
      }
    }

    @Test
    void returnStarSeesACallYieldVariable() {
      try (final ResultSet rs = database.query("opencypher", "CALL db.labels() YIELD label RETURN *")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().getPropertyNames()).containsExactly("label");
      }
    }

    @Test
    void returnStarWithNothingInScopeIsStillRejected() {
      assertThatThrownBy(() -> database.query("opencypher", "RETURN *").close())
          .isInstanceOf(CommandParsingException.class)
          .hasMessageContaining("NoVariablesInScope");
    }

    @Test
    void returnStarInsideACallSubqueryBodyStillFollowsTheSameRule() {
      assertThatCode(() -> database.query("opencypher", "CALL { MATCH (n:P) WITH n CALL { MATCH (m:P) RETURN m } "
          + "RETURN * } RETURN *").close()).doesNotThrowAnyException();
    }
  }

  /**
   * Issue #6541: same class of bug as #5444 ({@link CypherReturnStarTest.Issue5444ReturnStarInternalVariables}), but for
   * {@code WITH *} instead of {@code RETURN *}. The executor binds anonymous pattern elements to
   * generated variables ({@code   __anon0}, ...) that a query never named; {@code WithStep}'s
   * {@code WITH *} branch forwards them unfiltered, and its {@code DISTINCT} key-building loop counted
   * them too, so two rows that only differ by such a hidden binding wrongly stayed distinct.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue6541WithDistinctStarInternalVariables {
    private static final String DATABASE_PATH = "./target/databases/issue-6541-with-distinct-star";

    private Database database;

    @BeforeEach
    void setUp() {
      database = new DatabaseFactory(DATABASE_PATH).create();
      database.getSchema().createVertexType("Node").createProperty("id", Type.STRING);
      database.getSchema().createEdgeType("LINK");

      database.transaction(() -> {
        final MutableVertex hub = database.newVertex("Node").set("id", "hub").save();
        for (final String id : List.of("a", "b")) {
          final MutableVertex leaf = database.newVertex("Node").set("id", id).save();
          leaf.newEdge("LINK", hub, true, (Object[]) null).save();
        }
      });
    }

    @AfterEach
    void tearDown() {
      if (database != null)
        database.drop();
    }

    /**
     * The anonymous source node differs between the two matched paths, but only {@code hub} is ever
     * returned: {@code WITH DISTINCT *} must collapse the two rows to one, the same way
     * {@code RETURN DISTINCT *} already does for #5444.
     */
    @Test
    void withDistinctStarIgnoresTheHiddenVariables() {
      final List<String> ids = new ArrayList<>();
      try (ResultSet resultSet = database.query("opencypher",
          "MATCH (:Node)-[:LINK]->(hub:Node) WHERE hub.id = 'hub' WITH DISTINCT * RETURN hub.id AS id")) {
        while (resultSet.hasNext())
          ids.add(resultSet.next().getProperty("id"));
      }
      assertThat(ids).containsExactly("hub");
    }

    /**
     * {@code WITH *} still forwards every in-scope variable downstream (only the DISTINCT key ignores
     * the internal ones), so a plain, non-distinct {@code WITH *} keeps behaving exactly as before.
     */
    @Test
    void withStarStillForwardsNamedVariables() {
      try (ResultSet resultSet = database.query("opencypher",
          "MATCH (hub:Node) WHERE hub.id = 'hub' WITH * RETURN hub.id AS id")) {
        assertThat(resultSet.hasNext()).isTrue();
        assertThat(resultSet.next().<String>getProperty("id")).isEqualTo("hub");
      }
    }
  }

  /**
   * Issue #11543: a wire protocol announces the columns of a zero-row result from the statement, since there is no row to read
   * them from (issue #11386). Two shapes declare their columns somewhere other than in RETURN items: {@code RETURN *}, whose
   * columns are the variables in scope at the RETURN, and a standalone {@code CALL ... YIELD} (or a {@code CALL} with no YIELD,
   * or {@code YIELD *}), whose columns are the yielded fields. Both used to declare nothing, so a zero-row result of them
   * announced no column at all.
   * <p>
   * Every shape is checked against the keys of a row the same statement answers when it does match, so the columns a zero-row
   * result announces are the ones a non-empty result of it carries, in the same order.
   */
  @Nested
  class Issue11543DeclaredColumnsOfStarAndCall extends TestHelper {

    @Override
    protected void beginTest() {
      database.transaction(() -> {
        database.getSchema().createVertexType("A");
        database.getSchema().createVertexType("B");
        database.getSchema().createEdgeType("R");
        database.command("opencypher", "CREATE (:A {i: 1})-[:R]->(:B {j: 2})");
      });
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "MATCH (n:A) RETURN *",
        "MATCH (z:A)-[r:R]->(b:B) RETURN *",
        "MATCH (z)-[r]->(b:B {j: 2}) RETURN *",
        "MATCH (z:A), (b:B) RETURN *",
        "MATCH (z:A) WITH z, 1 AS a RETURN *",
        "MATCH (z:A) WITH z.i AS i RETURN *",
        "MATCH p = (z:A)-->() RETURN *",
        "MATCH (z:A)-->() RETURN *",
        "UNWIND [1] AS y MATCH (z:A) RETURN *",
        "UNWIND [1, 2] AS x RETURN *",
        "MATCH (z:A) WITH * MATCH (b:B) RETURN *",
        "MATCH (z:A) CALL { WITH z RETURN 5 AS k } RETURN *",
        "MATCH (z:A) OPTIONAL MATCH (z)-[q:X]->(m) RETURN *",
        "MATCH (z:A) RETURN * ORDER BY z.i LIMIT 1",
        "MATCH (z:A) FOREACH (x IN [1] | CREATE (:B {j: x})) RETURN *",
        "MATCH (z:A) WITH DISTINCT * RETURN *",
        "MATCH (z:A) WITH * OPTIONAL MATCH (z)-[r:R]->(b:B) RETURN *",
        "MATCH (z:A) MATCH (z)-[r:R]->(b:B) RETURN *",
        "CREATE (c:A {i: 3}) RETURN *",
        "MERGE (c:A {i: 1}) RETURN *",
        "MATCH (n:A) RETURN * UNION MATCH (n:B) RETURN *",
        "CALL db.labels() YIELD label AS l",
        "CALL db.labels() YIELD label",
        "CALL db.labels() YIELD label WHERE label = 'A'",
        "CALL db.labels() YIELD *",
        "CALL db.labels()",
        "CALL db.labels() YIELD label MATCH (n:A) RETURN *"
    })
    void theDeclaredColumnsAreTheKeysOfARowOfTheStatement(final String query) {
      database.transaction(() -> {
        try (final ResultSet rs = database.command("opencypher", query)) {
          assertThat(rs.hasNext()).as("the control statement answers a row: %s", query).isTrue();
          assertThat(declaredColumns(query)).as(query).isEqualTo(new ArrayList<>(rs.next().getPropertyNames()));
        }
      });
    }

    /**
     * A pattern walked from an end bound by an earlier clause binds its elements in the plan's direction ({@code b} then
     * {@code r} then {@code z}), not in the written one: the same names, in an order only the plan knows. Neo4j sorts the
     * columns of {@code RETURN *}, which removes the difference; #11557 tracks doing that for the rows and the declared columns
     * together.
     */
    @Test
    void aPatternWalkedFromABoundEndDeclaresTheSameNames() {
      final String query = "MATCH (b:B) MATCH (z:A)-[r:R]->(b) RETURN *";
      try (final ResultSet rs = database.query("opencypher", query)) {
        assertThat(declaredColumns(query)).containsExactlyInAnyOrderElementsOf(rs.next().getPropertyNames());
      }
    }

    @Test
    void aZeroRowReturnStarDeclaresTheVariablesInScope() {
      assertThat(declaredColumns("MATCH (n:Missing) RETURN *")).containsExactly("n");
      assertThat(declaredColumns("MATCH (a:A)-[r:R]->(b:B) WHERE a.i > 100 RETURN *")).containsExactly("a", "r", "b");
      assertThat(declaredColumns("MATCH (a:A) WHERE a.i > 100 WITH a, a.i AS i RETURN *")).containsExactly("a", "i");
      assertThat(declaredColumns("MATCH (a:A) WHERE a.i > 100 WITH a.i AS i RETURN *")).containsExactly("i");
      assertThat(declaredColumns("EXPLAIN MATCH (n:Missing) RETURN *")).containsExactly("n");
      assertThat(declaredColumns("PROFILE MATCH (n:Missing) RETURN *")).containsExactly("n");
    }

    @Test
    void aZeroRowCallDeclaresItsYieldedFields() {
      assertThat(declaredColumns("CALL db.labels() YIELD label WHERE label = 'Missing'")).containsExactly("label");
      assertThat(declaredColumns("CALL db.labels() YIELD label AS l WHERE l = 'Missing'")).containsExactly("l");
      assertThat(declaredColumns("CALL algo.wcc() YIELD componentId")).containsExactly("componentId");
      assertThat(declaredColumns("CALL algo.wcc()")).containsExactly("node", "componentId");
      assertThat(declaredColumns("CALL algo.wcc() YIELD *")).containsExactly("node", "componentId");
    }

    /** A CALL whose procedure is not one the engine knows still declares what it explicitly yields, and nothing otherwise. */
    @Test
    void anUnknownProcedureDeclaresOnlyItsExplicitYield() {
      assertThat(declaredColumns("CALL my.unknown() YIELD a, b AS c")).containsExactly("a", "c");
      assertThat(declaredColumns("CALL my.unknown()")).isNull();
    }

    /** A statement with neither a RETURN nor a trailing CALL still declares nothing: it answers no rows. */
    @Test
    void aWriteWithoutReturnDeclaresNoColumns() {
      assertThat(declaredColumns("CREATE (:A {i: 99})")).isNull();
      assertThat(declaredColumns("MATCH (n:A) SET n.x = 1")).isNull();
    }

    private List<String> declaredColumns(final String query) {
      return OpenCypherQueryEngine.declaredColumns((DatabaseInternal) database, query);
    }
  }

  /**
   * Issue #11556: a RETURN that mixes the star with explicit items ({@code RETURN *, z.i AS i}) answered only the variables
   * in scope, and silently dropped every item written after the star. {@code WITH *, expr AS alias} already kept both.
   */
  @Nested
  class Issue11556ReturnStarWithExplicitItems extends TestHelper {

    @Override
    protected void beginTest() {
      database.transaction(() -> {
        database.getSchema().createVertexType("A");
        database.getSchema().createVertexType("B");
        database.getSchema().createEdgeType("R");
        database.command("opencypher", "CREATE (:A {i: 1})-[:R]->(:B {j: 2})");
        database.command("opencypher", "CREATE (:A {i: 2})");
        database.command("opencypher", "CREATE (:A {i: 2})");
      });
    }

    @Test
    void theItemAfterTheStarIsAColumnOfEveryRow() {
      final List<Result> rows = rows("MATCH (z:A) RETURN *, z.i AS i ORDER BY i");
      assertThat(rows).hasSize(3);
      for (final Result row : rows) {
        assertThat(row.getPropertyNames()).containsExactly("z", "i");
        assertThat(row.<Integer>getProperty("i")).isEqualTo(row.<Document>getProperty("z").getInteger("i"));
      }
      assertThat(rows.stream().map(r -> r.<Integer>getProperty("i")).toList()).containsExactly(1, 2, 2);
    }

    @Test
    void severalItemsAfterTheStarKeepTheirWrittenOrder() {
      final Result row = single("MATCH (z:A {i: 1})-[r:R]->(b:B) RETURN *, b.j AS j, z.i + b.j AS s");
      assertThat(row.getPropertyNames()).containsExactly("z", "r", "b", "j", "s");
      assertThat(row.<Integer>getProperty("j")).isEqualTo(2);
      assertThat(row.<Number>getProperty("s").longValue()).isEqualTo(3L);
    }

    @Test
    void anUnaliasedItemAfterTheStarIsNamedByItsText() {
      final Result row = single("MATCH (z:A {i: 1}) RETURN *, z.i");
      assertThat(row.getPropertyNames()).containsExactly("z", "z.i");
      assertThat(row.<Integer>getProperty("z.i")).isEqualTo(1);
    }

    @Test
    void anAggregateAfterTheStarGroupsByTheVariablesInScope() {
      final List<Result> rows = rows("MATCH (z:A) WITH z.i AS k RETURN *, count(*) AS c ORDER BY k");
      assertThat(rows).hasSize(2);
      assertThat(rows.get(0).getPropertyNames()).containsExactly("k", "c");
      assertThat(rows.get(0).<Integer>getProperty("k")).isEqualTo(1);
      assertThat(rows.get(0).<Long>getProperty("c")).isEqualTo(1L);
      assertThat(rows.get(1).<Integer>getProperty("k")).isEqualTo(2);
      assertThat(rows.get(1).<Long>getProperty("c")).isEqualTo(2L);
    }

    @Test
    void distinctIsOverTheStarAndTheItems() {
      final List<Result> rows = rows("MATCH (z:A) WITH z.i AS k RETURN DISTINCT *, k * 10 AS t ORDER BY k");
      assertThat(rows).hasSize(2);
      assertThat(rows.get(0).getPropertyNames()).containsExactly("k", "t");
      assertThat(rows.get(0).<Number>getProperty("t").longValue()).isEqualTo(10L);
      assertThat(rows.get(1).<Number>getProperty("t").longValue()).isEqualTo(20L);
    }

    @Test
    void orderByAndLimitCanUseTheItemAfterTheStar() {
      final List<Result> rows = rows("UNWIND [3, 1, 2] AS x RETURN *, x * 10 AS y ORDER BY y DESC LIMIT 2");
      assertThat(rows).hasSize(2);
      assertThat(rows.get(0).getPropertyNames()).containsExactly("x", "y");
      assertThat(rows.get(0).<Number>getProperty("y").longValue()).isEqualTo(30L);
      assertThat(rows.get(1).<Number>getProperty("y").longValue()).isEqualTo(20L);
    }

    @Test
    void aLiteralAfterTheStarOfAWriteIsReturned() {
      database.transaction(() -> {
        try (final ResultSet rs = database.command("opencypher", "CREATE (c:B {j: 9}) RETURN *, c.j AS j, 'x' AS tag")) {
          final Result row = rs.next();
          assertThat(row.getPropertyNames()).containsExactly("c", "j", "tag");
          assertThat(row.<Integer>getProperty("j")).isEqualTo(9);
          assertThat(row.<String>getProperty("tag")).isEqualTo("x");
        }
      });
    }

    /**
     * A scope the parser cannot read without running (a {@code YIELD *} of a {@code DEFINE FUNCTION}, whose output no signature
     * declares) keeps the star, which the projection resolves against the row instead: the items after it are still columns.
     */
    @Test
    void aStarOverAScopeKnownOnlyAtRunTimeKeepsTheItems() {
      defineTwice();
      final List<Result> rows = commandRows("MATCH (z:A {i: 1}) CALL math.twice(z.i) YIELD * RETURN *, z.i AS i, z.i + 1 AS n");
      assertThat(rows).hasSize(1);
      final Result row = rows.getFirst();
      assertThat(row.getPropertyNames()).containsExactlyInAnyOrder("z", "value", "i", "n");
      assertThat(row.<Number>getProperty("value").longValue()).isEqualTo(2L);
      assertThat(row.<Integer>getProperty("i")).isEqualTo(1);
      assertThat(row.<Number>getProperty("n").longValue()).isEqualTo(2L);
    }

    /** The same unknown scope under DISTINCT: the rows are told apart by the variables and the items. */
    @Test
    void aDistinctStarOverAScopeKnownOnlyAtRunTimeKeepsTheItems() {
      defineTwice();
      final List<Result> rows = commandRows("MATCH (z:A) CALL math.twice(z.i) YIELD * RETURN DISTINCT *, z.i AS i ORDER BY i");
      assertThat(rows).hasSize(3);
      assertThat(rows.getFirst().getPropertyNames()).containsExactlyInAnyOrder("z", "value", "i");
    }

    /** Grouping by keys only known at run time is refused, rather than answered without its grouping keys. */
    @Test
    void anAggregateOverAScopeKnownOnlyAtRunTimeIsRefused() {
      defineTwice();
      assertThatThrownBy(() -> rows("MATCH (z:A) CALL math.twice(z.i) YIELD * RETURN *, count(*) AS c"))
          .isInstanceOf(CommandParsingException.class).hasMessageContaining("RETURN *");
    }

    /**
     * A CALL subquery that itself returns {@code *} has a scope known without running since #11571, so the star expands at
     * parse time and an aggregation groups by its variables.
     */
    @Test
    void anAggregateOverASubqueryReturningStarGroupsByItsScope() {
      final List<Result> rows = rows("MATCH (z:A) CALL { UNWIND [5, 5] AS k RETURN * } RETURN *, count(*) AS c");
      assertThat(rows).hasSize(3);
      for (final Result row : rows) {
        assertThat(row.getPropertyNames()).containsExactly("z", "k", "c");
        assertThat(row.<Number>getProperty("c").longValue()).isEqualTo(2L);
      }
    }

    private void defineTwice() {
      database.command("sql", "DEFINE FUNCTION math.twice \"SELECT :x * 2\" PARAMETERS [x] LANGUAGE sql");
    }

    /** A RETURN *, expr inside a CALL subquery answers its items to the enclosing query too. */
    @Test
    void theItemsAfterTheStarOfASubqueryReachTheEnclosingQuery() {
      final Result row = single("MATCH (z:A {i: 1}) CALL { UNWIND [5] AS k RETURN *, k * 2 AS d } RETURN z.i AS i, k, d");
      assertThat(row.<Number>getProperty("k").longValue()).isEqualTo(5L);
      assertThat(row.<Number>getProperty("d").longValue()).isEqualTo(10L);
    }

    /** Each branch of a UNION expands its own star. */
    @Test
    void eachBranchOfAUnionKeepsItsItems() {
      final List<Result> rows = rows("MATCH (z:A {i: 1}) RETURN *, z.i AS i UNION MATCH (z:B) RETURN *, z.j AS i");
      assertThat(rows).hasSize(2);
      for (final Result row : rows)
        assertThat(row.getPropertyNames()).containsExactly("z", "i");
      assertThat(rows.stream().map(r -> r.<Integer>getProperty("i")).toList()).containsExactlyInAnyOrder(1, 2);
    }

    /** An item named like a variable the star already returns defines that column twice, which Neo4j refuses too. */
    @Test
    void anItemNamedLikeAVariableOfTheStarIsAColumnConflict() {
      assertThatThrownBy(() -> rows("MATCH (z:A) RETURN *, z.i AS z")).isInstanceOf(CommandParsingException.class)
          .hasMessageContaining("ColumnNameConflict").hasMessageContaining("'z'");
      assertThatThrownBy(() -> rows("MATCH (z:A) RETURN *, z")).isInstanceOf(CommandParsingException.class)
          .hasMessageContaining("ColumnNameConflict");
    }

    /** What a zero-row result announces (#11543) is what a row of the same statement carries, item after the star included. */
    @ParameterizedTest
    @ValueSource(strings = {
        "MATCH (z:A {i: 1}) RETURN *, z.i AS i",
        "MATCH (z:A {i: 1})-[r:R]->(b:B) RETURN *, b.j AS j, z.i + b.j AS s",
        "MATCH (z:A {i: 1}) RETURN *, z.i",
        "UNWIND [1] AS x RETURN *, x * 10 AS y",
        "MATCH (z:A) WITH z.i AS k RETURN *, count(*) AS c",
        "MATCH (z:A) WITH z.i AS k RETURN DISTINCT *, k * 10 AS t"
    })
    void theDeclaredColumnsAreTheKeysOfARow(final String query) {
      final List<Result> rows = rows(query);
      assertThat(rows).as("the control statement answers a row: %s", query).isNotEmpty();
      assertThat(OpenCypherQueryEngine.declaredColumns((DatabaseInternal) database, query)).as(query)
          .isEqualTo(new ArrayList<>(rows.getFirst().getPropertyNames()));
    }

    private Result single(final String query) {
      final List<Result> rows = rows(query);
      assertThat(rows).as(query).hasSize(1);
      return rows.getFirst();
    }

    /** A call that may reach a DEFINE FUNCTION body is not idempotent, so it runs as a command. */
    private List<Result> commandRows(final String query) {
      try (final ResultSet rs = database.command("opencypher", query)) {
        final List<Result> rows = new ArrayList<>();
        while (rs.hasNext())
          rows.add(rs.next());
        return rows;
      }
    }

    private List<Result> rows(final String query) {
      try (final ResultSet rs = database.query("opencypher", query)) {
        final List<Result> rows = new ArrayList<>();
        while (rs.hasNext())
          rows.add(rs.next());
        return rows;
      }
    }
  }

  /**
   * Issue #11571, follow-up of #11543: a {@code RETURN *} whose scope runs through a {@code CALL { ... }} subquery that itself
   * returns {@code *} declared no columns, so a zero-row result of it announced {@code fields=[]} over Bolt. The scope walk now
   * follows the subquery's own {@code RETURN *} scope.
   * <p>
   * Every shape is checked against the keys of a row the same statement answers when it does match, so the columns a zero-row
   * result announces are the ones a non-empty result of it carries, in the same order.
   */
  @Nested
  class Issue11571DeclaredColumnsOfSubqueryStar extends TestHelper {

    @Override
    protected void beginTest() {
      database.transaction(() -> {
        database.getSchema().createVertexType("A");
        database.getSchema().createVertexType("B");
        database.getSchema().createEdgeType("R");
        database.command("opencypher", "CREATE (:A {i: 1})-[:R]->(:B {j: 2})");
      });
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "CALL { MATCH (n:A) RETURN * } RETURN *",
        "CALL { MATCH (n:A)-[r:R]->(m:B) RETURN * } RETURN *",
        "MATCH (z:A) CALL { MATCH (b:B) RETURN * } RETURN *",
        "MATCH (z:A) CALL { MATCH (z:A) RETURN * } RETURN *",
        "MATCH (z:A) CALL (z) { MATCH (z)-[r:R]->(b) RETURN * } RETURN *",
        "MATCH (z:A) CALL { WITH z MATCH (z)-[r:R]->(b) RETURN * } RETURN *",
        "MATCH (z:A) CALL (*) { MATCH (z)-[r:R]->(b) RETURN * } RETURN *",
        "MATCH (z:A) CALL { WITH * MATCH (z)-[r:R]->(b) RETURN * } RETURN *",
        "MATCH (z:A) CALL (z) { WITH z MATCH (b:B) WITH b RETURN * } RETURN *",
        "CALL { MATCH (n:A) RETURN *, 1 AS one } RETURN *",
        "CALL { CALL { MATCH (n:A) RETURN * } RETURN * } RETURN *",
        "CALL { MATCH (n:A) RETURN * UNION MATCH (n:B) RETURN * } RETURN *",
        "CALL { CALL db.labels() YIELD * RETURN * } RETURN *",
        "CALL { MATCH (n:A) RETURN * } WITH * MATCH (b:B) RETURN *",
        "CALL { MATCH (n:A) RETURN * } RETURN *, 2 AS two"
    })
    void theDeclaredColumnsAreTheKeysOfARowOfTheStatement(final String query) {
      database.transaction(() -> {
        try (final ResultSet rs = database.command("opencypher", query)) {
          assertThat(rs.hasNext()).as("the control statement answers a row: %s", query).isTrue();
          assertThat(declaredColumns(query)).as(query).isEqualTo(new ArrayList<>(rs.next().getPropertyNames()));
        }
      });
    }

    /** The reported shape: a zero-row {@code RETURN *} through a subquery returning {@code *}. */
    @Test
    void aZeroRowReturnStarThroughASubqueryStarDeclaresTheSubqueryVariables() {
      assertThat(declaredColumns("CALL { MATCH (n:Missing) RETURN * } RETURN *")).containsExactly("n");
      assertThat(declaredColumns("MATCH (z:A) WHERE z.i > 100 CALL { MATCH (b:B) RETURN * } RETURN *")).containsExactly("z", "b");
      assertThat(declaredColumns("MATCH (z:A) CALL (z) { MATCH (z)-[r:R]->(b:Missing) RETURN * } RETURN *"))
          .containsExactly("z", "r", "b");
      assertThat(declaredColumns("PROFILE CALL { MATCH (n:Missing) RETURN * } RETURN *")).containsExactly("n");
    }

    /**
     * An OPTIONAL CALL declares the columns its body returns, as Neo4j binds them to null when the body matches nothing. The null
     * row the executor produces leaves them out today (#11632), so this is pinned on the declared columns alone.
     */
    @Test
    void anOptionalSubqueryStarDeclaresTheBodyColumns() {
      assertThat(declaredColumns("MATCH (z:A) OPTIONAL CALL (z) { MATCH (z)-[r:X]->(b) RETURN * } RETURN *"))
          .containsExactly("z", "r", "b");
    }

    /**
     * A subquery returning {@code *} through a CALL of a procedure with no static signature still declares nothing: its columns
     * are not known before it runs.
     */
    @Test
    void aSubqueryStarOverAnUnknownProcedureStillDeclaresNothing() {
      assertThat(declaredColumns("CALL { MATCH (n:A) CALL my.unknown() YIELD * RETURN * } RETURN *")).isNull();
      assertThat(declaredColumns("CALL my.unknown() YIELD *")).isNull();
    }

    private List<String> declaredColumns(final String query) {
      return OpenCypherQueryEngine.declaredColumns((DatabaseInternal) database, query);
    }
  }
}
