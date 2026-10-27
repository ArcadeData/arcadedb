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
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10691 over the wire: the JDBC driver's own {@code getTables} classifies a relation by its {@code relkind}, so a
 * materialized view it used to report as a {@code TABLE} is now a {@code MATERIALIZED VIEW}, and the report's
 * {@code pg_matviews} is answered rather than refused.
 */
class Issue10691MaterializedViewCatalogIT extends PostgresWireProtocolTestBase {

  @Test
  void theDriverTellsAMaterializedViewFromATable() throws Exception {
    try (final Connection connection = openJdbcConnection()) {
      createSchema(connection);

      assertThat(tables(connection, "TABLE")).contains("Person10691").doesNotContain("PersonView10691");
      assertThat(tables(connection, "MATERIALIZED VIEW")).containsExactly("PersonView10691");
    }
  }

  @Test
  void pgMatviewsAnswersTheViewAndItsQuery() throws Exception {
    try (final Connection connection = openJdbcConnection()) {
      createSchema(connection);

      try (final Statement statement = connection.createStatement();
          final ResultSet resultSet = statement.executeQuery("select matviewname, definition from pg_matviews")) {
        assertThat(resultSet.next()).isTrue();
        assertThat(resultSet.getString("matviewname")).isEqualTo("PersonView10691");
        assertThat(resultSet.getString("definition")).isEqualTo("SELECT name FROM Person10691");
        assertThat(resultSet.next()).isFalse();
      }
    }
  }

  private static List<String> tables(final Connection connection, final String type) throws Exception {
    final List<String> tables = new ArrayList<>();
    try (final ResultSet resultSet = connection.getMetaData().getTables(null, null, "%", new String[] { type })) {
      while (resultSet.next()) {
        assertThat(resultSet.getString("TABLE_TYPE")).isEqualTo(type);
        tables.add(resultSet.getString("TABLE_NAME"));
      }
    }
    return tables;
  }

  private void createSchema(final Connection connection) throws Exception {
    try (final Statement statement = connection.createStatement()) {
      statement.execute("CREATE VERTEX TYPE Person10691 IF NOT EXISTS");
      statement.execute("CREATE PROPERTY Person10691.name IF NOT EXISTS STRING");
      statement.execute("INSERT INTO Person10691 SET name = 'a'");
      statement.execute("CREATE MATERIALIZED VIEW IF NOT EXISTS PersonView10691 AS SELECT name FROM Person10691");
    }
  }

  private Connection openJdbcConnection() throws Exception {
    Class.forName("org.postgresql.Driver");
    final Properties properties = new Properties();
    properties.setProperty("user", "root");
    properties.setProperty("password", DEFAULT_PASSWORD_FOR_TESTS);
    properties.setProperty("ssl", "false");
    properties.setProperty("sslMode", "disable");
    return DriverManager.getConnection(getServerPostgresJdbcUrl(), properties);
  }
}
