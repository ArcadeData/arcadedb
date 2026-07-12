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
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5107: a {@code MATCH (p:Person) WHERE p.id = <const>} predicate inside
 * a write statement ({@code MATCH ... CREATE ...}) must use the unique index on {@code Person.id}
 * for the leading MATCH instead of full-scanning the vertex type.
 * <p>
 * Before the fix the leading MATCH full-scanned because its WHERE-based index shortcut only fired when
 * an input row was present; the leading/seed MATCH has none. These tests assert the index is used via
 * the PROFILE plan text and that results are still correct.
 * <p>
 * Note: since issue #5136 relaxed the {@code createCount > 1} guard, the two-CREATE write now runs
 * through the cost-based optimizer (which emits {@code NodeIndexSeek}) rather than the legacy path
 * (which prints {@code [index: Person[id]]}). Both are index-backed, so these tests accept either
 * marker.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue5107MatchCreateIndexTest {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./databases/test-issue5107").create();

    database.transaction(() -> {
      final var personType = database.getSchema().createVertexType("Person");
      personType.createProperty("id", Integer.class);
      personType.createProperty("name", String.class);
      personType.createProperty("age", Integer.class);
      personType.createProperty("city", String.class);

      final var knows = database.getSchema().createEdgeType("KNOWS");
      knows.createProperty("since", Integer.class);

      // Unique index on Person.id (the index the write's MATCH must use).
      personType.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");
    });

    // Populate 100 Person vertices with sequential ids.
    database.transaction(() -> {
      for (int i = 0; i < 100; i++)
        database.command("opencypher",
            "CREATE (p:Person {name: 'Person" + i + "', id: " + i + ", age: " + (20 + (i % 50)) + ", city: 'c" + i + "'})");
    });
  }

  @AfterEach
  void teardown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  /**
   * The exact issue query: a leading MATCH with a constant WHERE equality followed by two CREATE
   * clauses. The MATCH must use the unique index, and the write side-effects must be correct.
   */
  @Test
  void matchCreateUsesIndexForConstantPredicate() {
    final String write = "MATCH (p:Person) WHERE p.id = 42 "
        + "CREATE (q:Person {id: 900001, name: 'w', age: 33, city: 'c'}) "
        + "CREATE (p)-[:KNOWS {since: 2026}]->(q)";

    // PROFILE surfaces the executed plan. Since #5136 the two-CREATE write runs through the optimizer
    // (NodeIndexSeek); the legacy path prints [index: Person[id]]. Either proves the index was used
    // instead of a full scan.
    final String plan = profilePlan(write);

    assertThat(plan)
        .as("leading MATCH with constant WHERE equality must use the unique index, not a full scan\n%s", plan)
        .containsAnyOf("NodeIndexSeek", "[index: Person[id]]");

    // Verify the write actually happened and connected the correct source vertex.
    database.transaction(() -> {
      final ResultSet check = database.query("opencypher",
          "MATCH (p:Person)-[:KNOWS]->(q:Person) WHERE p.id = 42 RETURN p.id AS src, q.id AS dst, q.name AS name");
      assertThat(check.hasNext()).isTrue();
      final Result row = check.next();
      assertThat((Integer) row.getProperty("src")).isEqualTo(42);
      assertThat((Integer) row.getProperty("dst")).isEqualTo(900001);
      assertThat((String) row.getProperty("name")).isEqualTo("w");
      assertThat(check.hasNext()).isFalse();
      check.close();
    });
  }

  /**
   * Same shape, but the predicate value comes from a query parameter. Parameters resolve without an
   * input row, so the index must still be used.
   */
  @Test
  void matchCreateUsesIndexForParameterPredicate() {
    final Map<String, Object> params = new HashMap<>();
    params.put("id", 7);

    final String write = "MATCH (p:Person) WHERE p.id = $id "
        + "CREATE (q:Person {id: 900002, name: 'param', age: 40, city: 'x'}) "
        + "CREATE (p)-[:KNOWS {since: 2027}]->(q)";

    final String plan = profilePlan(write, params);

    assertThat(plan)
        .as("leading MATCH with parameter WHERE equality must use the unique index\n%s", plan)
        .containsAnyOf("NodeIndexSeek", "[index: Person[id]]");

    database.transaction(() -> {
      final ResultSet check = database.query("opencypher",
          "MATCH (p:Person)-[:KNOWS]->(q:Person) WHERE p.id = 7 RETURN q.id AS dst", params);
      assertThat(check.hasNext()).isTrue();
      assertThat((Integer) check.next().getProperty("dst")).isEqualTo(900002);
      check.close();
    });
  }

  /**
   * A single CREATE keeps the cost-based optimizer path, which already emits NodeIndexSeek. Guard
   * that the common case is not regressed and stays index-backed.
   */
  @Test
  void matchSingleCreateStillUsesIndex() {
    final String write = "MATCH (p:Person) WHERE p.id = 13 "
        + "CREATE (q:Person {id: 900003, name: 'single', age: 21, city: 'y'})";

    final String plan = profilePlan(write);

    assertThat(plan)
        .as("single-CREATE write must use the index (optimizer NodeIndexSeek or legacy index)\n%s", plan)
        .containsAnyOf("NodeIndexSeek", "[index: Person[id]]");

    database.transaction(() -> {
      final ResultSet check = database.query("opencypher", "MATCH (q:Person) WHERE q.id = 900003 RETURN q.name AS name");
      assertThat(check.hasNext()).isTrue();
      assertThat((String) check.next().getProperty("name")).isEqualTo("single");
      check.close();
    });
  }

  /**
   * A leading MATCH whose WHERE predicate cannot use an index (no index on {@code age}) must still
   * execute correctly and must not throw - it simply falls back to a full scan + row-level filter.
   */
  @Test
  void matchCreateNonIndexedPredicateFallsBackGracefully() {
    // 'name' is not indexed; the constant equality on it cannot use an index and must fall back to a
    // full scan + row-level filter without throwing. 'Person5' matches exactly one vertex.
    final String write = "MATCH (p:Person) WHERE p.name = 'Person5' "
        + "CREATE (q:Person {id: 900004, name: 'fallback', age: 99, city: 'z'}) "
        + "CREATE (p)-[:KNOWS {since: 2028}]->(q)";

    // Must not throw and must not falsely claim an index (there is none on 'name').
    final String plan = profilePlan(write);
    assertThat(plan).doesNotContain("[index:");

    database.transaction(() -> {
      final ResultSet check = database.query("opencypher",
          "MATCH (p:Person)-[:KNOWS]->(q:Person) WHERE q.name = 'fallback' RETURN p.id AS src, p.name AS srcName");
      assertThat(check.hasNext()).isTrue();
      final Result row = check.next();
      assertThat((Integer) row.getProperty("src")).isEqualTo(5);
      assertThat((String) row.getProperty("srcName")).isEqualTo("Person5");
      assertThat(check.hasNext()).isFalse();
      check.close();
    });
  }

  private String profilePlan(final String cypher) {
    return profilePlan(cypher, null);
  }

  private String profilePlan(final String cypher, final Map<String, Object> params) {
    final StringBuilder plan = new StringBuilder();
    // The PROFILE run executes the write; wrap it in a transaction so the side-effects commit.
    database.transaction(() -> {
      final ResultSet rs = params != null
          ? database.command("opencypher", "PROFILE " + cypher, params)
          : database.command("opencypher", "PROFILE " + cypher);
      while (rs.hasNext())
        rs.next();
      plan.append(rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2));
      rs.close();
    });
    return plan.toString();
  }
}
