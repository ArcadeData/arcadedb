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
 * Regression tests for the second half of issue #6412: the catalog answers a client needs to render a
 * database - the schema list, the table list, the columns of a table - were matched by string equality
 * against one tool's exact spelling, several of them only when {@code application_name} was literally
 * {@code "dbvis"}. The same questions from any other client fell through and were answered with nothing.
 * <p>
 * Every test here goes through the JDBC driver's own {@link DatabaseMetaData} calls, with no
 * {@code application_name} set at all: the queries are the driver's, not this test's, so what is being
 * asserted is that the answer follows from the shape of the question rather than from the name of the asker.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue6412CatalogIT extends PostgresWireProtocolTestBase {

  @Test
  void theSchemaListNamesTheConnectedDatabase() throws Exception {
    try (final Connection connection = openJdbcConnection()) {
      createSchema(connection);

      final List<String> schemas = new ArrayList<>();
      try (final ResultSet resultSet = connection.getMetaData().getSchemas()) {
        while (resultSet.next())
          schemas.add(resultSet.getString("TABLE_SCHEM"));
      }

      assertThat(schemas).contains(getDatabaseName());
    }
  }

  @Test
  void theTableListNamesEveryType() throws Exception {
    try (final Connection connection = openJdbcConnection()) {
      createSchema(connection);

      final List<String> tables = new ArrayList<>();
      final List<String> types = new ArrayList<>();
      try (final ResultSet resultSet = connection.getMetaData().getTables(null, null, "%", new String[] { "TABLE" })) {
        while (resultSet.next()) {
          tables.add(resultSet.getString("TABLE_NAME"));
          types.add(resultSet.getString("TABLE_TYPE"));
        }
      }

      assertThat(tables).contains("Article6412", "Author6412");
      // The driver's own CASE over relkind produced this string: the catalog never spells it itself.
      assertThat(types).containsOnly("TABLE");
    }
  }

  @Test
  void theTableListHonoursTheNameTheClientAskedFor() throws Exception {
    try (final Connection connection = openJdbcConnection()) {
      createSchema(connection);

      final List<String> tables = new ArrayList<>();
      try (final ResultSet resultSet = connection.getMetaData().getTables(null, null, "Article6412", null)) {
        while (resultSet.next())
          tables.add(resultSet.getString("TABLE_NAME"));
      }

      assertThat(tables).containsExactly("Article6412");
    }
  }

  @Test
  void theColumnListDescribesTheTypesProperties() throws Exception {
    try (final Connection connection = openJdbcConnection()) {
      createSchema(connection);

      final List<String> columns = new ArrayList<>();
      final List<String> typeNames = new ArrayList<>();
      try (final ResultSet resultSet = connection.getMetaData().getColumns(null, null, "Article6412", "%")) {
        while (resultSet.next()) {
          columns.add(resultSet.getString("COLUMN_NAME"));
          typeNames.add(resultSet.getString("TYPE_NAME"));
        }
      }

      assertThat(columns).containsExactlyInAnyOrder("id", "published", "title");
      assertThat(typeNames).contains("int4", "varchar");
      // Columns of the other type must not leak into the answer: the client asked about one table.
      assertThat(columns).doesNotContain("name");
    }
  }

  @Test
  void aCatalogQuerySpelledByHandIsAnsweredToo() throws Exception {
    try (final Connection connection = openJdbcConnection(); final Statement statement = connection.createStatement()) {
      createSchema(connection);

      // information_schema, in a spelling no tool in the old allow-list ever sent.
      final List<String> tables = new ArrayList<>();
      try (final ResultSet resultSet = statement.executeQuery(
          "SELECT table_name FROM information_schema.tables WHERE table_schema = '" + getDatabaseName()
              + "' ORDER BY table_name")) {
        while (resultSet.next())
          tables.add(resultSet.getString("table_name"));
      }

      assertThat(tables).contains("Article6412", "Author6412");
    }
  }

  @Test
  void theUserListIsAnsweredForEveryClientNotJustTheAllowListedOne() throws Exception {
    try (final Connection connection = openJdbcConnection(); final Statement statement = connection.createStatement()) {
      // This exact query used to be answered only when application_name was "dbvis"; this connection sets none.
      try (final ResultSet resultSet = statement.executeQuery(
          "select distinct GRANTEE as USER_NAME, 'N' as IS_EXPIRED, 'N' as IS_LOCKED from INFORMATION_SCHEMA.USAGE_PRIVILEGES order by GRANTEE asc")) {
        assertThat(resultSet.next()).isTrue();
        assertThat(resultSet.getString("USER_NAME")).isEqualTo("root");
        assertThat(resultSet.getString("IS_EXPIRED")).isEqualTo("N");
        assertThat(resultSet.next()).isFalse();
      }
    }
  }

  @Test
  void theCharacterSetListIsAnswered() throws Exception {
    try (final Connection connection = openJdbcConnection(); final Statement statement = connection.createStatement()) {
      try (final ResultSet resultSet = statement.executeQuery(
          "select CHARACTER_SET_NAME as CHARSET_NAME, -1 as MAX_LENGTH from INFORMATION_SCHEMA.CHARACTER_SETS order by CHARACTER_SET_NAME asc")) {
        assertThat(resultSet.next()).isTrue();
        assertThat(resultSet.getString("CHARSET_NAME")).isEqualTo("UTF8");
        assertThat(resultSet.getInt("MAX_LENGTH")).isEqualTo(-1);
      }
    }
  }

  @Test
  void theSchemaListSpelledWithCaseExpressionsIsAnswered() throws Exception {
    try (final Connection connection = openJdbcConnection(); final Statement statement = connection.createStatement()) {
      // The DbVisualizer spelling, which used to be one of the exact-match arms. Its CASE expressions are
      // evaluated rather than recognised, so any variation on them works as well.
      try (final ResultSet resultSet = statement.executeQuery(
          "select NSPNAME as SCHEMA_NAME, case when lower(NSPNAME)='pg_catalog' then 'Y' else 'N' end as IS_PUBLIC, "
              + "case when lower(NSPNAME)='information_schema' then 'Y' else 'N' end as IS_SYSTEM, 'N' as IS_EMPTY "
              + "from PG_CATALOG.PG_NAMESPACE order by NSPNAME asc")) {
        assertThat(resultSet.next()).isTrue();
        assertThat(resultSet.getString("SCHEMA_NAME")).isEqualTo(getDatabaseName());
        assertThat(resultSet.getString("IS_PUBLIC")).isEqualTo("N");
        assertThat(resultSet.getString("IS_SYSTEM")).isEqualTo("N");
        assertThat(resultSet.next()).isFalse();
      }
    }
  }

  @Test
  void anUnmodelledCatalogRelationIsDeclinedRatherThanGuessedAt() throws Exception {
    try (final Connection connection = openJdbcConnection(); final Statement statement = connection.createStatement()) {
      // pg_index has no ArcadeDB equivalent this catalog models. The client gets the empty answer the rest of
      // pg_catalog has always given, not an error and not an invented row.
      try (final ResultSet resultSet = statement.executeQuery("SELECT indexrelid FROM pg_catalog.pg_index")) {
        assertThat(resultSet.next()).isFalse();
      }
    }
  }

  @Test
  void aUserTypeCalledLikeACatalogRelationIsStillQueryable() throws Exception {
    try (final Connection connection = openJdbcConnection(); final Statement statement = connection.createStatement()) {
      // The pre-filter looks for "pg_" anywhere in the query, so a type whose name starts with it has to
      // survive the catalog recogniser and reach the SQL engine.
      statement.execute("CREATE DOCUMENT TYPE pg_notes6412 IF NOT EXISTS");
      statement.execute("CREATE PROPERTY pg_notes6412.note IF NOT EXISTS STRING");
      statement.execute("INSERT INTO pg_notes6412 SET note = 'mine'");

      try (final ResultSet resultSet = statement.executeQuery("SELECT note FROM pg_notes6412")) {
        assertThat(resultSet.next()).isTrue();
        assertThat(resultSet.getString("note")).isEqualTo("mine");
      }
    }
  }

  private void createSchema(final Connection connection) throws Exception {
    try (final Statement statement = connection.createStatement()) {
      statement.execute("CREATE DOCUMENT TYPE Article6412 IF NOT EXISTS");
      statement.execute("CREATE PROPERTY Article6412.id IF NOT EXISTS INTEGER");
      statement.execute("CREATE PROPERTY Article6412.title IF NOT EXISTS STRING");
      statement.execute("CREATE PROPERTY Article6412.published IF NOT EXISTS DATETIME");
      statement.execute("CREATE DOCUMENT TYPE Author6412 IF NOT EXISTS");
      statement.execute("CREATE PROPERTY Author6412.name IF NOT EXISTS STRING");
    }
  }

  private Connection openJdbcConnection() throws Exception {
    Class.forName("org.postgresql.Driver");
    final Properties properties = new Properties();
    properties.setProperty("user", "root");
    properties.setProperty("password", DEFAULT_PASSWORD_FOR_TESTS);
    properties.setProperty("ssl", "false");
    properties.setProperty("sslMode", "disable");
    return DriverManager.getConnection("jdbc:postgresql://localhost:5432/" + getDatabaseName(), properties);
  }
}
