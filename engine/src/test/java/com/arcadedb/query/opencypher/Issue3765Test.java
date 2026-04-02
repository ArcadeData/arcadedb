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
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for https://github.com/ArcadeData/arcadedb/issues/3765
 * OpenCypher: parameterized queries fail to match in property maps
 * when there is an edge and the parameter is on the right-hand side node.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue3765Test {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/testopencypher-issue3765").create();

    database.getSchema().createVertexType("A");
    database.getSchema().createVertexType("B");
    database.getSchema().createEdgeType("LINK");

    database.transaction(() -> {
      database.command("opencypher",
          "CREATE (a:A {f: 'a'}), (b:B {f: 'x'}), (a)-[:LINK]->(b)");
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
   * Baseline: literal values on both sides work fine.
   */
  @Test
  void matchWithLiteralValuesOnBothSides() {
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (a:A {f: 'a'})-[:LINK]->(b:B {f: 'x'}) RETURN a.f, b.f")) {
      final List<Result> results = new ArrayList<>();
      while (rs.hasNext())
        results.add(rs.next());

      assertThat(results).hasSize(1);
      assertThat(results.get(0).<String>getProperty("a.f")).isEqualTo("a");
      assertThat(results.get(0).<String>getProperty("b.f")).isEqualTo("x");
    }
  }

  /**
   * Parameter on the left-hand side node works.
   */
  @Test
  void matchWithParameterOnLeftSideNode() {
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (a:A {f: $param})-[:LINK]->(b:B {f: 'x'}) RETURN a.f, b.f",
        Map.of("param", "a"))) {
      final List<Result> results = new ArrayList<>();
      while (rs.hasNext())
        results.add(rs.next());

      assertThat(results).hasSize(1);
      assertThat(results.get(0).<String>getProperty("a.f")).isEqualTo("a");
      assertThat(results.get(0).<String>getProperty("b.f")).isEqualTo("x");
    }
  }

  /**
   * This is the exact failing scenario from issue #3765:
   * Parameter on the right-hand side node returns empty results.
   */
  @Test
  void matchWithParameterOnRightSideNode() {
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (a:A {f: 'a'})-[:LINK]->(b:B {f: $param}) RETURN a.f, b.f",
        Map.of("param", "x"))) {
      final List<Result> results = new ArrayList<>();
      while (rs.hasNext())
        results.add(rs.next());

      assertThat(results).hasSize(1);
      assertThat(results.get(0).<String>getProperty("a.f")).isEqualTo("a");
      assertThat(results.get(0).<String>getProperty("b.f")).isEqualTo("x");
    }
  }

  /**
   * Parameters on both sides of the relationship.
   */
  @Test
  void matchWithParametersOnBothSides() {
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (a:A {f: $p1})-[:LINK]->(b:B {f: $p2}) RETURN a.f, b.f",
        Map.of("p1", "a", "p2", "x"))) {
      final List<Result> results = new ArrayList<>();
      while (rs.hasNext())
        results.add(rs.next());

      assertThat(results).hasSize(1);
      assertThat(results.get(0).<String>getProperty("a.f")).isEqualTo("a");
      assertThat(results.get(0).<String>getProperty("b.f")).isEqualTo("x");
    }
  }

  /**
   * Parameter on right-hand side that does not match should return empty.
   */
  @Test
  void matchWithParameterOnRightSideNoMatch() {
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (a:A {f: 'a'})-[:LINK]->(b:B {f: $param}) RETURN a.f, b.f",
        Map.of("param", "nonexistent"))) {
      assertThat(rs.hasNext()).isFalse();
    }
  }
}
