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
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10473 through the PostgreSQL JDBC driver: {@code DatabaseMetaData.getIndexInfo()} and {@code getPrimaryKeys()}, which
 * Hibernate, Flyway, Liquibase and the BI tools call, are written with {@code information_schema._pg_expandarray()} - a set-returning
 * function - and read its members as {@code (expression).n}. The statements are the driver's own, so these tests hold the shape
 * this catalog has to read; when a release of the driver rewrites them they fail first.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10473JdbcIndexMetadataIT extends PostgresWireProtocolTestBase {
  @Test
  void getIndexInfoListsTheKeyColumnsOfEveryIndex() throws Exception {
    try (final Connection connection = openJdbcConnection()) {
      createIndexedType(connection);

      final DatabaseMetaData metadata = connection.getMetaData();
      final List<String> rows = new ArrayList<>();
      try (final ResultSet resultSet = metadata.getIndexInfo(null, null, "Meta10473", false, false)) {
        while (resultSet.next())
          rows.add(resultSet.getString("INDEX_NAME") + "|" + resultSet.getInt("ORDINAL_POSITION") + "|"
              + resultSet.getString("COLUMN_NAME") + "|" + resultSet.getBoolean("NON_UNIQUE") + "|"
              + resultSet.getString("ASC_OR_DESC") + "|" + resultSet.getString("TABLE_NAME"));
      }

      // unique first (NON_UNIQUE false), then by name and position, as the driver orders them
      assertThat(rows).containsExactly("Meta10473[id]|1|id|false|A|Meta10473", "Meta10473[name,tag]|1|name|true|A|Meta10473",
          "Meta10473[name,tag]|2|tag|true|A|Meta10473");

      // and only the unique ones when the caller asks for them
      try (final ResultSet resultSet = metadata.getIndexInfo(null, null, "Meta10473", true, false)) {
        assertThat(resultSet.next()).isTrue();
        assertThat(resultSet.getString("INDEX_NAME")).isEqualTo("Meta10473[id]");
        assertThat(resultSet.next()).isFalse();
      }
    }
  }

  @Test
  void getPrimaryKeysOfATypeWithoutOneAreNone() throws Exception {
    try (final Connection connection = openJdbcConnection()) {
      createIndexedType(connection);

      // ArcadeDB has no primary key: the unique index is not one, and the driver is told so instead of being refused
      try (final ResultSet resultSet = connection.getMetaData().getPrimaryKeys(null, null, "Meta10473")) {
        assertThat(resultSet.next()).isFalse();
      }
    }
  }

  @Test
  void theTablesAndColumnsStillAnswer() throws Exception {
    try (final Connection connection = openJdbcConnection()) {
      createIndexedType(connection);

      try (final ResultSet resultSet = connection.getMetaData().getTables(null, null, "Meta10473", null)) {
        assertThat(resultSet.next()).isTrue();
        assertThat(resultSet.getString("TABLE_NAME")).isEqualTo("Meta10473");
      }
      try (final ResultSet resultSet = connection.getMetaData().getColumns(null, null, "Meta10473", null)) {
        final List<String> columns = new ArrayList<>();
        while (resultSet.next())
          columns.add(resultSet.getString("COLUMN_NAME"));
        assertThat(columns).containsExactlyInAnyOrder("id", "name", "tag");
      }
    }
  }

  private static void createIndexedType(final Connection connection) throws Exception {
    try (final Statement statement = connection.createStatement()) {
      statement.execute("CREATE DOCUMENT TYPE Meta10473 IF NOT EXISTS");
      statement.execute("CREATE PROPERTY Meta10473.id IF NOT EXISTS INTEGER");
      statement.execute("CREATE PROPERTY Meta10473.name IF NOT EXISTS STRING");
      statement.execute("CREATE PROPERTY Meta10473.tag IF NOT EXISTS STRING");
      statement.execute("CREATE INDEX IF NOT EXISTS ON Meta10473 (id) UNIQUE");
      statement.execute("CREATE INDEX IF NOT EXISTS ON Meta10473 (name, tag) NOTUNIQUE");
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
