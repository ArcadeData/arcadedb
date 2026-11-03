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
package com.arcadedb.postgres;

import com.arcadedb.database.Database;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11026: the simple query protocol named the columns of a result from the rows that came back, so a declared
 * property that is NULL on all of them was dropped from a wildcard projection, and one projected explicitly was announced as
 * varchar instead of its declared type, while Describe('S') of the same text answered both from the schema. The announced
 * columns of a statement naming a schema type are a property of the schema, not of the data.
 */
class Issue11026DeclaredNullColumnsIT extends PostgresWireProtocolTestBase {

  @BeforeEach
  void createData() {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.transaction(() -> {
      final VertexType type = database.getSchema().createVertexType("NullCol26");
      type.createProperty("n", Type.INTEGER);
      type.createProperty("k", Type.STRING);
      type.createProperty("d", Type.DECIMAL);
    });
    database.transaction(() -> database.newVertex("NullCol26").set("k", "a").save());
  }

  @Test
  void simpleQueryTypesANullDeclaredColumnFromTheSchema() throws Exception {
    final Properties properties = new Properties();
    properties.setProperty("preferQueryMode", "simple");
    try (final Connection connection = connect(properties); final Statement statement = connection.createStatement();
        final ResultSet rs = statement.executeQuery("SELECT n FROM NullCol26")) {
      assertThat(rs.getMetaData().getColumnTypeName(1)).isEqualTo("int4");
      assertThat(rs.next()).isTrue();
      rs.getInt(1);
      assertThat(rs.wasNull()).isTrue();
    }
  }

  @Test
  void simpleQueryWildcardHoldsEveryDeclaredColumn() throws Exception {
    final Properties properties = new Properties();
    properties.setProperty("preferQueryMode", "simple");
    try (final Connection connection = connect(properties); final Statement statement = connection.createStatement();
        final ResultSet rs = statement.executeQuery("SELECT * FROM NullCol26")) {
      assertWildcardShape(rs);
    }
  }

  @Test
  void simpleQueryAgreesWithDescribeStatement() throws Exception {
    final Properties simple = new Properties();
    simple.setProperty("preferQueryMode", "simple");
    final Properties prepared = new Properties();
    prepared.setProperty("prepareThreshold", "-1");
    for (final String sql : List.of("SELECT * FROM NullCol26", "SELECT n FROM NullCol26", "SELECT n, k FROM NullCol26")) {
      final List<String> viaSimple = columnsOf(simple, sql);
      final List<String> viaPrepared = columnsOf(prepared, sql);
      assertThat(viaSimple).as(sql).containsExactlyInAnyOrderElementsOf(viaPrepared);
    }
  }

  @Test
  void describePortalWildcardHoldsEveryDeclaredColumn() throws Exception {
    final Properties properties = new Properties();
    properties.setProperty("prepareThreshold", "0");
    try (final Connection connection = connect(properties); final PreparedStatement ps = connection.prepareStatement(
        "SELECT * FROM NullCol26"); final ResultSet rs = ps.executeQuery()) {
      assertWildcardShape(rs);
    }
  }

  /** Issue #11164: an aliased bare property is typed from the property it reads, like Describe('S') does. */
  @Test
  void simpleQueryTypesAnAliasedNullDeclaredColumnFromTheSchema() throws Exception {
    final Properties simple = new Properties();
    simple.setProperty("preferQueryMode", "simple");
    assertThat(columnsOf(simple, "SELECT n AS x FROM NullCol26")).containsExactly("x:int4");
    // the alias repeats the name of ANOTHER declared property: the type is the source's, not the namesake's
    assertThat(columnsOf(simple, "SELECT n AS k FROM NullCol26")).containsExactly("k:int4");
    assertThat(columnsOf(simple, "SELECT n AS k, k AS n FROM NullCol26")).containsExactly("k:int4", "n:varchar");
  }

  @Test
  void aliasedSimpleQueryAgreesWithDescribeStatement() throws Exception {
    final Properties simple = new Properties();
    simple.setProperty("preferQueryMode", "simple");
    final Properties prepared = new Properties();
    prepared.setProperty("prepareThreshold", "-1");
    for (final String sql : List.of("SELECT n AS x FROM NullCol26", "SELECT n AS k FROM NullCol26", "SELECT n AS x, k FROM NullCol26"))
      assertThat(columnsOf(simple, sql)).as(sql).containsExactlyElementsOf(columnsOf(prepared, sql));
  }

  /** A computed item whose output name repeats a declared property is not that property: it keeps its own type. */
  @Test
  void computedItemNamedLikeADeclaredPropertyIsNotTypedFromIt() throws Exception {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.transaction(() -> database.newVertex("NullCol26").set("n", 1).save());
    database.transaction(() -> database.command("sql", "UPDATE NullCol26 SET k = null"));
    final Properties simple = new Properties();
    simple.setProperty("preferQueryMode", "simple");
    final Properties prepared = new Properties();
    prepared.setProperty("prepareThreshold", "-1");
    final String sql = "SELECT k.toUpperCase() AS n FROM NullCol26";
    assertThat(columnsOf(simple, sql)).isEqualTo(columnsOf(prepared, sql)).containsExactly("n:varchar");
  }

  /** A projection must not gain declared columns it does not name. */
  @Test
  void projectionDoesNotGainUnnamedColumns() throws Exception {
    final Properties properties = new Properties();
    properties.setProperty("preferQueryMode", "simple");
    assertThat(columnsOf(properties, "SELECT k FROM NullCol26")).containsExactly("k:varchar");
  }

  private static void assertWildcardShape(final ResultSet rs) throws Exception {
    final ResultSetMetaData meta = rs.getMetaData();
    final List<String> columns = new ArrayList<>();
    for (int i = 1; i <= meta.getColumnCount(); i++)
      columns.add(meta.getColumnName(i) + ":" + meta.getColumnTypeName(i));
    assertThat(columns).contains("n:int4", "k:varchar", "d:numeric");
    assertThat(rs.next()).isTrue();
    assertThat(rs.getString("k")).isEqualTo("a");
    rs.getInt("n");
    assertThat(rs.wasNull()).isTrue();
    assertThat(rs.getBigDecimal("d")).isNull();
  }

  private List<String> columnsOf(final Properties properties, final String sql) throws Exception {
    try (final Connection connection = connect(properties); final PreparedStatement ps = connection.prepareStatement(sql);
        final ResultSet rs = ps.executeQuery()) {
      final ResultSetMetaData meta = rs.getMetaData();
      final List<String> columns = new ArrayList<>();
      for (int i = 1; i <= meta.getColumnCount(); i++)
        columns.add(meta.getColumnName(i) + ":" + meta.getColumnTypeName(i));
      return columns;
    }
  }

  private Connection connect(final Properties properties) throws Exception {
    properties.setProperty("user", "root");
    properties.setProperty("password", DEFAULT_PASSWORD_FOR_TESTS);
    properties.setProperty("sslmode", "disable");
    return DriverManager.getConnection(getServerPostgresJdbcUrl(), properties);
  }
}
