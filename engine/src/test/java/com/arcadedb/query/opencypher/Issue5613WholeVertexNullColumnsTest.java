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
import com.arcadedb.database.Document;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.JsonSerializer;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reproduces issue #5613: returning a whole vertex from openCypher also emitted every declared
 * property of the vertex's type as a top-level column, always null. `RETURN f` yielded five columns
 * where four were null, while the equivalent SQL MATCH yielded one.
 * <p>
 * The row carried both a backing element (the vertex) and content ({@code {"f": vertex}}).
 * {@code getPropertyNames()} returned the union of both, while {@code getProperty()} deliberately
 * ignores the element once content is present, so every element-only name resolved to null.
 */
class Issue5613WholeVertexNullColumnsTest {
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
