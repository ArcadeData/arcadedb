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
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10842 through the PostgreSQL JDBC driver: {@code pg_depend} lists the dependencies of the indexes of a table, typed as
 * PostgreSQL types them, while the {@code DatabaseMetaData} calls that join it still answer no foreign key; and a catalog relation
 * this server does not model is still refused with 0A000.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10842PgDependIT extends PostgresWireProtocolTestBase {
  private static final String TABLE = "Dep10842";

  @Test
  void pgDependListsTheDependenciesOfTheIndexesOfATable() throws Exception {
    for (final String mode : List.of("extended", "simple")) {
      try (final Connection connection = openJdbcConnection(mode)) {
        createType(connection);
        try (final Statement statement = connection.createStatement();
            final ResultSet resultSet = statement.executeQuery("""
                SELECT c.relname, d.refobjsubid, d.deptype FROM pg_catalog.pg_depend d
                JOIN pg_catalog.pg_class c ON c.oid = d.objid
                JOIN pg_catalog.pg_class t ON t.oid = d.refobjid
                WHERE d.classid = 1259 AND d.refclassid = 1259 AND t.relname = '%s'""".formatted(TABLE))) {
          assertThat(resultSet.getMetaData().getColumnType(2)).as(mode).isEqualTo(Types.INTEGER);
          final Set<String> rows = new TreeSet<>();
          while (resultSet.next()) {
            assertThat(resultSet.getInt(2)).as("the column the index depends on").isPositive();
            rows.add(resultSet.getString(1) + ":" + resultSet.getString(3));
          }
          // the plain index depends on its column; the unique one depends on its constraint, not on the table
          assertThat(rows).as(mode).containsExactly(TABLE + "[age]:a");
        }

        try (final Statement statement = connection.createStatement();
            final ResultSet resultSet = statement.executeQuery(
                "SELECT count(*) FROM pg_catalog.pg_depend WHERE refclassid = 2606 AND deptype = 'i'")) {
          assertThat(resultSet.next()).isTrue();
          assertThat(resultSet.getLong(1)).as(mode).isGreaterThanOrEqualTo(1L);
        }

        final DatabaseMetaData metadata = connection.getMetaData();
        assertNoRow(mode, metadata.getImportedKeys(null, null, TABLE));
        assertNoRow(mode, metadata.getExportedKeys(null, null, TABLE));
      }
    }
  }

  @Test
  void anUnmodelledRelationIsStillRefused() throws Exception {
    try (final Connection connection = openJdbcConnection("extended"); final Statement statement = connection.createStatement()) {
      assertThatThrownBy(() -> statement.executeQuery("SELECT extname FROM pg_catalog.pg_extension")).isInstanceOf(SQLException.class)
          .satisfies(e -> assertThat(((SQLException) e).getSQLState()).isEqualTo("0A000"))
          .hasMessageContaining("pg_extension");
    }
  }

  private static void assertNoRow(final String mode, final ResultSet resultSet) throws SQLException {
    try (resultSet) {
      assertThat(resultSet.next()).as(mode).isFalse();
    }
  }

  private static void createType(final Connection connection) throws SQLException {
    try (final Statement statement = connection.createStatement()) {
      statement.execute("CREATE DOCUMENT TYPE " + TABLE + " IF NOT EXISTS");
      statement.execute("CREATE PROPERTY " + TABLE + ".name IF NOT EXISTS STRING");
      statement.execute("CREATE PROPERTY " + TABLE + ".age IF NOT EXISTS INTEGER");
      statement.execute("CREATE INDEX IF NOT EXISTS ON " + TABLE + " (name) UNIQUE");
      statement.execute("CREATE INDEX IF NOT EXISTS ON " + TABLE + " (age) NOTUNIQUE");
    }
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
