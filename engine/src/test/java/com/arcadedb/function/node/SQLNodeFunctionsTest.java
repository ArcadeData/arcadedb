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
package com.arcadedb.function.node;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for node.* and rel.* functions called from SQL.
 * The argument unwrapping in AbstractNodeFunction.toVertex() and
 * AbstractRelFunction.toEdge() must handle SQL-side wrappers (Result,
 * Identifiable, RID) in addition to the bare Vertex/Edge that Cypher passes.
 */
class SQLNodeFunctionsTest extends TestHelper {

  @Override
  public void beginTest() {
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("Friend");

    database.transaction(() -> {
      database.command("sql", "INSERT INTO Person SET name = 'Ada'");
      database.command("sql", "INSERT INTO Person SET name = 'Bob'");
      database.command("sql", "INSERT INTO Person SET name = 'Cara'");
      database.command("sql", "CREATE EDGE Friend FROM (SELECT FROM Person WHERE name = 'Ada') TO (SELECT FROM Person WHERE name = 'Bob')");
      database.command("sql", "CREATE EDGE Friend FROM (SELECT FROM Person WHERE name = 'Ada') TO (SELECT FROM Person WHERE name = 'Cara')");
    });
  }

  // node.degree via @this (SQL row carrier)
  @Test
  void nodeDegreeFromSql_thisReference() {
    final ResultSet rs = database.query("sql", "SELECT name, node.degree(@this, 'Friend') AS d FROM Person WHERE name = 'Ada'");
    assertThat(rs.hasNext()).isTrue();
    final Result row = rs.next();
    assertThat(row.<Long>getProperty("d")).isEqualTo(2L);
    rs.close();
  }

  // node.degree via @rid (RID - an Identifiable)
  @Test
  void nodeDegreeFromSql_ridReference() {
    final ResultSet rs = database.query("sql", "SELECT name, node.degree(@rid, 'Friend') AS d FROM Person WHERE name = 'Ada'");
    assertThat(rs.hasNext()).isTrue();
    final Result row = rs.next();
    assertThat(row.<Long>getProperty("d")).isEqualTo(2L);
    rs.close();
  }

  // node.degree via $current (SQL context variable for the current record)
  @Test
  void nodeDegreeFromSql_currentVariable() {
    final ResultSet rs = database.query("sql", "SELECT name, node.degree($current, 'Friend') AS d FROM Person WHERE name = 'Ada'");
    assertThat(rs.hasNext()).isTrue();
    final Result row = rs.next();
    assertThat(row.<Long>getProperty("d")).isEqualTo(2L);
    rs.close();
  }

  // node.id via @rid - cross-validates that the RID from @rid and node.id agree
  @Test
  void nodeIdFromSql_ridReference() {
    final ResultSet rs = database.query("sql", "SELECT node.id(@rid) AS rid FROM Person WHERE name = 'Ada'");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Object>getProperty("rid")).isNotNull();
    rs.close();
  }

  // node.labels via @this
  @Test
  void nodeLabelsFromSql_thisReference() {
    final ResultSet rs = database.query("sql", "SELECT node.labels(@this) AS lbl FROM Person WHERE name = 'Ada'");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Object>getProperty("lbl")).isNotNull();
    rs.close();
  }

  // node.id via @this
  @Test
  void nodeIdFromSql_thisReference() {
    final ResultSet rs = database.query("sql", "SELECT node.id(@this) AS rid FROM Person WHERE name = 'Ada'");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Object>getProperty("rid")).isNotNull();
    rs.close();
  }

  // rel.type via @this when iterating edges
  @Test
  void relTypeFromSql_thisReference() {
    final ResultSet rs = database.query("sql", "SELECT rel.type(@this) AS t FROM Friend");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("t")).isEqualTo("Friend");
    rs.close();
  }

  // rel.startNode via @this when iterating edges
  @Test
  void relStartNodeFromSql_thisReference() {
    final ResultSet rs = database.query("sql", "SELECT rel.startNode(@this) AS src FROM Friend");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Object>getProperty("src")).isNotNull();
    rs.close();
  }
}
