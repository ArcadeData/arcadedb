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

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #10228 over the wire, through pgjdbc's extended protocol: the catalog shapes psql and
 * psqlodbc send are answered, a projection naming two columns alike announces both, and a set operation the
 * catalog cannot evaluate still reaches the SQL engine rather than being swallowed into an empty answer.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10228PsqlOdbcCatalogIT extends PostgresWireProtocolTestBase {

  @Test
  void odbcAllTableTypesAnnouncesThreeColumns() throws Exception {
    try (final Connection connection = openJdbcConnection(); final Statement statement = connection.createStatement();
        final ResultSet resultSet = statement.executeQuery(
            "select NULL, NULL, relkind from (select 'r' as relkind union select 'v' union select 'm' union select 'f' "
                + "union select 'p') as a")) {
      final ResultSetMetaData metaData = resultSet.getMetaData();
      assertThat(metaData.getColumnCount()).as("psqlodbc reads the third column").isEqualTo(3);
      assertThat(metaData.getColumnName(1)).isEqualTo("?column?");
      assertThat(metaData.getColumnName(2)).isEqualTo("?column?");
      assertThat(metaData.getColumnName(3)).isEqualTo("relkind");

      final List<String> kinds = new ArrayList<>();
      while (resultSet.next()) {
        assertThat(resultSet.getString(1)).isNull();
        kinds.add(resultSet.getString(3));
      }
      assertThat(kinds).containsExactly("r", "v", "m", "f", "p");
    }
  }

  /** Describe on the prepared statement, before it runs: the duplicate names are announced the same way. */
  @Test
  void aDescribedStatementAnnouncesDuplicateColumnsByName() throws Exception {
    try (final Connection connection = openJdbcConnection(); final PreparedStatement statement = connection.prepareStatement(
        "select NULL, NULL, relkind from (select 'r' as relkind union select 'v') as a")) {
      final ResultSetMetaData metaData = statement.getMetaData();
      assertThat(metaData.getColumnCount()).isEqualTo(3);
      assertThat(metaData.getColumnName(1)).isEqualTo("?column?");
      assertThat(metaData.getColumnName(2)).isEqualTo("?column?");
      assertThat(metaData.getColumnName(3)).isEqualTo("relkind");

      final List<String> kinds = new ArrayList<>();
      try (final ResultSet resultSet = statement.executeQuery()) {
        while (resultSet.next())
          kinds.add(resultSet.getString(3));
      }
      assertThat(kinds).containsExactly("r", "v");
    }
  }

  @Test
  void theFiveShapesOfTheIssue() throws Exception {
    try (final Connection connection = openJdbcConnection()) {
      createSchema(connection);

      final int tables = count(connection, "select c.relname from pg_catalog.pg_class c");
      assertThat(tables).isGreaterThanOrEqualTo(3);

      assertThat(count(connection,
          "select 1 from (pg_catalog.pg_class c inner join pg_catalog.pg_namespace n on n.oid = c.relnamespace)"))
          .isEqualTo(tables);
      assertThat(count(connection,
          "select c.relname from pg_catalog.pg_class c left join pg_catalog.pg_am am on am.oid = c.relam"))
          .isEqualTo(tables);
      assertThat(count(connection,
          "select relname from pg_catalog.pg_class where relname operator(pg_catalog.=) 'zzz'")).isZero();
      assertThat(strings(connection, "select 1 union select 2")).containsExactly("1", "2");
      assertThat(strings(connection, "select count(*) from pg_catalog.pg_class c")).containsExactly(String.valueOf(tables));
    }
  }

  /** psqlodbc's SQLColumns("Person"), whose table filter is in a join condition of a parenthesised join. */
  @Test
  void odbcColumnsOfOneTable() throws Exception {
    try (final Connection connection = openJdbcConnection()) {
      createSchema(connection);

      final List<String> columns = strings(connection,
          "select a.attname from (((pg_catalog.pg_class c inner join pg_catalog.pg_namespace n "
              + "on n.oid = c.relnamespace and c.relname like 'Person10228') inner join pg_catalog.pg_attribute a "
              + "on (not a.attisdropped) and a.attnum operator(pg_catalog.>) 0 and a.attrelid operator(pg_catalog.=) c.oid) "
              + "inner join pg_catalog.pg_type t on t.oid operator(pg_catalog.=) a.atttypid) left outer join pg_attrdef d "
              + "on a.atthasdef and d.adrelid operator(pg_catalog.=) a.attrelid and d.adnum operator(pg_catalog.=) a.attnum "
              + "order by n.nspname, c.relname, attnum");
      assertThat(columns).containsExactly("id", "name");
    }
  }

  /**
   * A catalog query that spends more work than one statement may is refused with PostgreSQL's
   * program_limit_exceeded, over both protocols, rather than answered with an empty catalog - and the connection
   * goes on working.
   */
  @Test
  void aCatalogQueryOverItsWorkBudgetIsRefusedWithSqlState54000() throws Exception {
    for (final String mode : List.of("extended", "simple")) {
      try (final Connection connection = openJdbcConnection(mode)) {
        try (final Statement statement = connection.createStatement()) {
          statement.execute("CREATE DOCUMENT TYPE " + "x".repeat(40) + " IF NOT EXISTS");
        }

        assertThatThrownBy(() -> strings(connection,
            "select relname from pg_catalog.pg_class where relname ~ '^(.*)(.*)(.*)(.*)(.*)Y$'"))
            .isInstanceOf(SQLException.class)
            .satisfies(e -> assertThat(((SQLException) e).getSQLState()).as(mode).isEqualTo("54000"));

        assertThat(strings(connection, "select relname from pg_catalog.pg_class where relname = 'Person10228'")).as(mode)
            .isEmpty();
      }
    }
  }

  /** A set operation over the user's own types is not a catalog query: the SQL engine still answers it. */
  @Test
  void aUnionOverAUserTypeIsStillTheSqlEnginesToAnswer() throws Exception {
    try (final Connection connection = openJdbcConnection()) {
      createSchema(connection);

      assertThatThrownBy(() -> strings(connection, "select name from Person10228 union select 'x'"))
          .isInstanceOf(SQLException.class);
      // and the connection is still usable afterwards
      assertThat(strings(connection, "select 1 union all select 1")).containsExactly("1", "1");
    }
  }

  private static int count(final Connection connection, final String query) throws SQLException {
    return strings(connection, query).size();
  }

  private static List<String> strings(final Connection connection, final String query) throws SQLException {
    final List<String> values = new ArrayList<>();
    try (final Statement statement = connection.createStatement(); final ResultSet resultSet = statement.executeQuery(query)) {
      while (resultSet.next())
        values.add(resultSet.getString(1));
    }
    return values;
  }

  private void createSchema(final Connection connection) throws Exception {
    try (final Statement statement = connection.createStatement()) {
      statement.execute("CREATE DOCUMENT TYPE Person10228 IF NOT EXISTS");
      statement.execute("CREATE PROPERTY Person10228.id IF NOT EXISTS INTEGER");
      statement.execute("CREATE PROPERTY Person10228.name IF NOT EXISTS STRING");
    }
  }

  private Connection openJdbcConnection() throws Exception {
    return openJdbcConnection("extended");
  }

  private Connection openJdbcConnection(final String queryMode) throws Exception {
    Class.forName("org.postgresql.Driver");
    final Properties properties = new Properties();
    properties.setProperty("preferQueryMode", queryMode);
    properties.setProperty("user", "root");
    properties.setProperty("password", DEFAULT_PASSWORD_FOR_TESTS);
    properties.setProperty("ssl", "false");
    properties.setProperty("sslMode", "disable");
    return DriverManager.getConnection(getServerPostgresJdbcUrl(), properties);
  }
}
