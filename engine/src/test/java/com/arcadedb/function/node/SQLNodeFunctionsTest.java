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
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Test;

import java.util.Collection;

import static org.assertj.core.api.Assertions.assertThat;

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

  @Test
  void nodeDegreeFromSql_thisReference() {
    try (final ResultSet rs = database.query("sql", "SELECT name, node.degree(@this, 'Friend') AS d FROM Person WHERE name = 'Ada'")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Long>getProperty("d")).isEqualTo(2L);
    }
  }

  @Test
  void nodeDegreeFromSql_ridReference() {
    try (final ResultSet rs = database.query("sql", "SELECT name, node.degree(@rid, 'Friend') AS d FROM Person WHERE name = 'Ada'")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Long>getProperty("d")).isEqualTo(2L);
    }
  }

  @Test
  void nodeDegreeFromSql_currentVariable() {
    try (final ResultSet rs = database.query("sql", "SELECT name, node.degree($current, 'Friend') AS d FROM Person WHERE name = 'Ada'")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Long>getProperty("d")).isEqualTo(2L);
    }
  }

  @Test
  void nodeLabelsFromSql_thisReference() {
    try (final ResultSet rs = database.query("sql", "SELECT node.labels(@this) AS lbl FROM Person WHERE name = 'Ada'")) {
      assertThat(rs.hasNext()).isTrue();
      final Collection<String> labels = rs.next().getProperty("lbl");
      assertThat(labels).contains("Person");
    }
  }

  @Test
  void nodeIdFromSql_thisReference() {
    try (final ResultSet rs = database.query("sql", "SELECT @rid AS expected, node.id(@this) AS rid FROM Person WHERE name = 'Ada'")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat(row.<String>getProperty("rid")).isEqualTo(row.getProperty("expected").toString());
    }
  }

  @Test
  void nodeIdFromSql_ridReference() {
    try (final ResultSet rs = database.query("sql", "SELECT @rid AS expected, node.id(@rid) AS rid FROM Person WHERE name = 'Ada'")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat(row.<String>getProperty("rid")).isEqualTo(row.getProperty("expected").toString());
    }
  }

  @Test
  void relTypeFromSql_thisReference() {
    try (final ResultSet rs = database.query("sql", "SELECT rel.type(@this) AS t FROM Friend")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<String>getProperty("t")).isEqualTo("Friend");
    }
  }

  @Test
  void relTypeFromSql_ridReference() {
    try (final ResultSet rs = database.query("sql", "SELECT rel.type(@rid) AS t FROM Friend")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<String>getProperty("t")).isEqualTo("Friend");
    }
  }

  @Test
  void relStartNodeFromSql_thisReference() {
    try (final ResultSet rs = database.query("sql", "SELECT rel.startNode(@this) AS src FROM Friend")) {
      assertThat(rs.hasNext()).isTrue();
      int count = 0;
      while (rs.hasNext()) {
        final Result row = rs.next();
        final Vertex src = row.getProperty("src");
        assertThat(src).isNotNull();
        assertThat(src.getString("name")).isEqualTo("Ada");
        count++;
      }
      assertThat(count).isEqualTo(2);
    }
  }

  @Test
  void relEndNodeFromSql_thisReference() {
    try (final ResultSet rs = database.query("sql", "SELECT rel.endNode(@this) AS dst FROM Friend")) {
      assertThat(rs.hasNext()).isTrue();
      int count = 0;
      while (rs.hasNext()) {
        final Result row = rs.next();
        final Vertex dst = row.getProperty("dst");
        assertThat(dst).isNotNull();
        assertThat(dst.getString("name")).isIn("Bob", "Cara");
        count++;
      }
      assertThat(count).isEqualTo(2);
    }
  }
}
