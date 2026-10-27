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
package com.arcadedb.query.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10684: on an edge, SQL reads the endpoints as {@code @out} / {@code @in}. Reading the plain names {@code out} and
 * {@code in} - the OrientDB spelling, or a guess - silently answered null for every path through them. On an edge that has
 * no property of that name they now resolve to the edge's endpoints; a property of that name still wins, and nothing
 * changes on a vertex or a document.
 */
class Issue10684EdgeOutInEndpointsTest extends TestHelper {

  private RID aRid;
  private RID bRid;

  @BeforeEach
  void setUpGraph() {
    database.command("sql", "CREATE VERTEX TYPE A");
    database.command("sql", "CREATE VERTEX TYPE B");
    database.command("sql", "CREATE EDGE TYPE E");
    database.command("sql", "CREATE DOCUMENT TYPE D");
    database.transaction(() -> {
      aRid = database.command("sql", "INSERT INTO A SET id = 1").next().getIdentity().orElseThrow();
      bRid = database.command("sql", "INSERT INTO B SET name = 'x'").next().getIdentity().orElseThrow();
      database.command("sql", "CREATE EDGE E FROM (SELECT FROM A) TO (SELECT FROM B) SET qty = 7");
      database.command("sql", "INSERT INTO D SET name = 'doc'");
    });
  }

  @Test
  void propertyPathThroughOutAndInReachesTheEndpoints() {
    final Result row = single("SELECT out.id AS oid, in.name AS iname, @out.id AS aoid, outV().id AS voi FROM E");
    assertThat(row.<Object>getProperty("oid")).isEqualTo(1);
    assertThat(row.<Object>getProperty("iname")).isEqualTo("x");
    assertThat(row.<Object>getProperty("aoid")).isEqualTo(1);
    assertThat(row.<Object>getProperty("voi")).isEqualTo(1);
  }

  @Test
  void bareOutAndInProjectTheEndpointRids() {
    final Result row = single("SELECT out, in FROM E");
    assertThat(row.<Object>getProperty("out")).isEqualTo(aRid);
    assertThat(row.<Object>getProperty("in")).isEqualTo(bRid);
  }

  @Test
  void whereOnOutAndInFiltersByTheEndpoints() {
    assertThat(count("SELECT FROM E WHERE out = " + aRid)).isEqualTo(1);
    assertThat(count("SELECT FROM E WHERE in = " + bRid)).isEqualTo(1);
    assertThat(count("SELECT FROM E WHERE out = " + bRid)).isZero();
    assertThat(count("SELECT FROM E WHERE out.id = 1 AND in.name = 'x'")).isEqualTo(1);
  }

  @Test
  void edgesReachedThroughATraversalResolveTheirEndpointsToo() {
    final Result row = single("SELECT outE().in.name AS names, outE('E')[0].out.id AS firstOut FROM A");
    assertThat(row.<List<Object>>getProperty("names")).containsExactly("x");
    assertThat(row.<Object>getProperty("firstOut")).isEqualTo(1);
  }

  @Test
  void orderByAndGroupByOnOutUseTheEndpoints() {
    database.transaction(() -> {
      database.command("sql", "INSERT INTO A SET id = 2");
      database.command("sql", "CREATE EDGE E FROM (SELECT FROM A WHERE id = 2) TO (SELECT FROM B) SET qty = 8");
    });

    try (final ResultSet rs = database.query("sql", "SELECT FROM E ORDER BY out DESC")) {
      assertThat(rs.stream().map(r -> r.<Integer>getProperty("qty")).toList()).containsExactly(8, 7);
    }
    try (final ResultSet rs = database.query("sql", "SELECT FROM E ORDER BY out ASC")) {
      assertThat(rs.stream().map(r -> r.<Integer>getProperty("qty")).toList()).containsExactly(7, 8);
    }
    try (final ResultSet rs = database.query("sql", "SELECT qty FROM E ORDER BY out DESC")) {
      assertThat(rs.stream().map(r -> r.<Integer>getProperty("qty")).toList()).containsExactly(8, 7);
    }
    try (final ResultSet rs = database.query("sql", "SELECT in.name AS n, count(*) AS c FROM E GROUP BY in.name")) {
      final Result row = rs.next();
      assertThat(row.<Object>getProperty("n")).isEqualTo("x");
      assertThat(row.<Long>getProperty("c")).isEqualTo(2L);
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void matchWhereOnAnEdgeAliasReadsTheEndpoints() {
    final Result row = single("MATCH {type: A, as: a}.outE('E'){as: e, where: (out.id = 1 AND in.name = 'x')} RETURN e.qty AS qty");
    assertThat(row.<Object>getProperty("qty")).isEqualTo(7);
  }

  @Test
  void aRealPropertyNamedOutStillWins() {
    database.transaction(() -> database.command("sql", "UPDATE E SET out = 'mine'"));
    final Result row = single("SELECT out, in.name AS iname, @out AS endpoint FROM E");
    assertThat(row.<Object>getProperty("out")).isEqualTo("mine");
    assertThat(row.<Object>getProperty("iname")).isEqualTo("x");
    assertThat(row.<Object>getProperty("endpoint")).isEqualTo(aRid);
  }

  @Test
  void aPropertyNamedInDeclaredOnTheEdgeTypeWinsEvenWhenUnset() {
    database.command("sql", "CREATE PROPERTY E.in STRING");
    final Result row = single("SELECT in AS i, out.id AS oid FROM E");
    assertThat(row.<Object>getProperty("i")).isNull();
    assertThat(row.<Object>getProperty("oid")).isEqualTo(1);
  }

  @Test
  void verticesAndDocumentsAreUnchanged() {
    final Result vertex = single("SELECT out AS o, in AS i FROM A");
    assertThat(vertex.<Object>getProperty("o")).isNull();
    assertThat(vertex.<Object>getProperty("i")).isNull();

    final Result doc = single("SELECT out AS o, in AS i FROM D");
    assertThat(doc.<Object>getProperty("o")).isNull();
    assertThat(doc.<Object>getProperty("i")).isNull();
  }

  private Result single(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      assertThat(rs.hasNext()).as(sql).isTrue();
      final Result row = rs.next();
      assertThat(rs.hasNext()).as(sql).isFalse();
      return row;
    }
  }

  private long count(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return rs.stream().count();
    }
  }
}
