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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for https://github.com/ArcadeData/arcadedb/issues/3761
 * Cypher: unable to parse query when using WITH *
 * <p>
 * Root cause: CypherASTBuilder.visitWithClause used an if/else pattern, so when
 * TIMES (*) was present, additional return items (e.g. "t2 AS out2") were silently
 * dropped. The RETURN clause then referenced an undefined alias, causing a parse error.
 */
class Issue3761Test {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/testopencypher-issue3761").create();

    database.getSchema().createVertexType("Thing1");
    database.getSchema().createVertexType("Thing2");
    database.getSchema().createEdgeType("basedOn");

    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Thing1 {id: 'A'})");
      database.command("opencypher", "CREATE (:Thing2 {name: 'B'})");
      database.command("opencypher",
          "MATCH (t1:Thing1 {id: 'A'}), (t2:Thing2 {name: 'B'}) CREATE (t1)-[:basedOn]->(t2)");
    });
  }

  @AfterEach
  void cleanup() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  /**
   * Exact query from the issue: OPTIONAL MATCH followed by WITH *, alias.
   * Previously threw "Error parsing OpenCypher query" due to undefined alias.
   */
  @Test
  void withStarAndAliasAfterOptionalMatch() {
    database.transaction(() -> {
      try (final ResultSet rs = database.query("opencypher", """
          MATCH (t1:Thing1 {id: 'A'})
          OPTIONAL MATCH (t1)-[:basedOn]->(t2:Thing2)
          WITH *, t2 AS out2
          RETURN t1 AS out1, out2
          """)) {
        final List<Result> results = new ArrayList<>();
        while (rs.hasNext())
          results.add(rs.next());

        assertThat(results).hasSize(1);
        final Result row = results.get(0);
        assertThat(row.getPropertyNames()).contains("out1", "out2");
        assertThat((Object) row.getProperty("out1")).isNotNull();
        assertThat((Object) row.getProperty("out2")).isNotNull();
      }
    });
  }

  /**
   * WITH * alone (without extra items) should still pass all variables through.
   */
  @Test
  void withStarAlonePassesAllVariables() {
    database.transaction(() -> {
      try (final ResultSet rs = database.query("opencypher", """
          MATCH (t1:Thing1 {id: 'A'})
          OPTIONAL MATCH (t1)-[:basedOn]->(t2:Thing2)
          WITH *
          RETURN t1, t2
          """)) {
        final List<Result> results = new ArrayList<>();
        while (rs.hasNext())
          results.add(rs.next());

        assertThat(results).hasSize(1);
        final Result row = results.get(0);
        assertThat(row.getPropertyNames()).contains("t1", "t2");
      }
    });
  }

  /**
   * WITH *, computed expression as alias — the expression should be evaluated.
   */
  @Test
  void withStarAndComputedAlias() {
    database.transaction(() -> {
      try (final ResultSet rs = database.query("opencypher", """
          MATCH (t1:Thing1 {id: 'A'})
          WITH *, t1.id AS itemId
          RETURN t1, itemId
          """)) {
        final List<Result> results = new ArrayList<>();
        while (rs.hasNext())
          results.add(rs.next());

        assertThat(results).hasSize(1);
        final Result row = results.get(0);
        assertThat(row.getPropertyNames()).contains("t1", "itemId");
        assertThat(row.<String>getProperty("itemId")).isEqualTo("A");
      }
    });
  }

  /**
   * WITH *, expr1 AS a, expr2 AS b — multiple extra items after the wildcard.
   */
  @Test
  void withStarAndMultipleExtraAliases() {
    database.transaction(() -> {
      try (final ResultSet rs = database.query("opencypher", """
          MATCH (t1:Thing1 {id: 'A'})
          OPTIONAL MATCH (t1)-[:basedOn]->(t2:Thing2)
          WITH *, t1.id AS srcId, t2.name AS tgtName
          RETURN srcId, tgtName
          """)) {
        final List<Result> results = new ArrayList<>();
        while (rs.hasNext())
          results.add(rs.next());

        assertThat(results).hasSize(1);
        final Result row = results.get(0);
        assertThat(row.<String>getProperty("srcId")).isEqualTo("A");
        assertThat(row.<String>getProperty("tgtName")).isEqualTo("B");
      }
    });
  }

  /**
   * WITH *, alias used in WHERE clause filtering.
   */
  @Test
  void withStarAndAliasInWhereClause() {
    database.transaction(() -> {
      // Add a second Thing1 node that should be filtered out
      database.command("opencypher", "CREATE (:Thing1 {id: 'X'})");

      try (final ResultSet rs = database.query("opencypher", """
          MATCH (t1:Thing1)
          WITH *, t1.id AS tid
          WHERE tid = 'A'
          RETURN t1, tid
          """)) {
        final List<Result> results = new ArrayList<>();
        while (rs.hasNext())
          results.add(rs.next());

        assertThat(results).hasSize(1);
        assertThat(results.get(0).<String>getProperty("tid")).isEqualTo("A");
      }
    });
  }
}
