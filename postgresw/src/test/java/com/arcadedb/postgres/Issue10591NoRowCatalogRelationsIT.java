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
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10591 through the PostgreSQL JDBC driver: the {@code DatabaseMetaData} calls whose honest answer is "there are none" -
 * the foreign keys of a table, the functions and procedures of the database - read relations ArcadeDB never holds a row in
 * ({@code pg_proc}, {@code pg_depend}) and {@code pg_settings}, and were refused with 0A000 since #10371. The statements are the
 * driver's own, so a release of the driver that reads another relation fails these first.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10591NoRowCatalogRelationsIT extends PostgresWireProtocolTestBase {
  private static final String TABLE = "Meta10591";

  @Test
  void theKeyMetadataCallsAnswerNoForeignKey() throws Exception {
    for (final String mode : List.of("extended", "simple")) {
      try (final Connection connection = openJdbcConnection(mode)) {
        createType(connection);
        final DatabaseMetaData metadata = connection.getMetaData();

        assertNoRow(mode, metadata.getImportedKeys(null, null, TABLE));
        assertNoRow(mode, metadata.getExportedKeys(null, null, TABLE));
        assertNoRow(mode, metadata.getCrossReference(null, null, TABLE, null, null, TABLE));
      }
    }
  }

  @Test
  void theFunctionMetadataCallsAnswerNoFunction() throws Exception {
    for (final String mode : List.of("extended", "simple")) {
      try (final Connection connection = openJdbcConnection(mode)) {
        final DatabaseMetaData metadata = connection.getMetaData();

        assertNoRow(mode, metadata.getProcedures(null, null, "%"));
        assertNoRow(mode, metadata.getProcedureColumns(null, null, "%", "%"));
        assertNoRow(mode, metadata.getFunctions(null, null, "%"));
        assertNoRow(mode, metadata.getFunctionColumns(null, null, "%", "%"));
      }
    }
  }

  /** The control: the calls that already answered still do. */
  @Test
  void theCallsThatAnsweredBeforeStillDo() throws Exception {
    try (final Connection connection = openJdbcConnection("extended")) {
      createType(connection);
      final DatabaseMetaData metadata = connection.getMetaData();

      try (final ResultSet resultSet = metadata.getTables(null, null, TABLE, null)) {
        assertThat(resultSet.next()).isTrue();
        assertThat(resultSet.getString("TABLE_NAME")).isEqualTo(TABLE);
      }
      try (final ResultSet resultSet = metadata.getIndexInfo(null, null, TABLE, false, false)) {
        assertThat(resultSet.next()).isTrue();
      }
      assertNoRow("extended", metadata.getPrimaryKeys(null, null, TABLE));
    }
  }

  /** pgjdbc reads max_index_keys from pg_settings; SHOW and pg_settings answer the same value for every parameter they share. */
  @Test
  void pgSettingsAnswersWhatShowAnswers() throws Exception {
    for (final String mode : List.of("extended", "simple")) {
      try (final Connection connection = openJdbcConnection(mode); final Statement statement = connection.createStatement()) {
        try (final ResultSet resultSet = statement.executeQuery(
            "SELECT setting FROM pg_catalog.pg_settings WHERE name='max_index_keys'")) {
          assertThat(resultSet.next()).as(mode).isTrue();
          assertThat(resultSet.getInt(1)).as(mode).isEqualTo(32);
          assertThat(resultSet.next()).as(mode).isFalse();
        }
        try (final ResultSet resultSet = statement.executeQuery("SHOW max_index_keys")) {
          assertThat(resultSet.next()).as(mode).isTrue();
          assertThat(resultSet.getString(1)).as(mode).isEqualTo("32");
        }

        // a value this connection set is the one pg_settings lists
        statement.execute("SET application_name = 'app10591'");
        try (final ResultSet resultSet = statement.executeQuery(
            "select setting, context from pg_catalog.pg_settings where name = 'application_name'")) {
          assertThat(resultSet.next()).as(mode).isTrue();
          assertThat(resultSet.getString("setting")).as(mode).isEqualTo("app10591");
          assertThat(resultSet.getString("context")).as(mode).isEqualTo("user");
        }
        try (final ResultSet resultSet = statement.executeQuery(
            "select setting from pg_catalog.pg_settings where name = 'default_transaction_isolation'")) {
          assertThat(resultSet.next()).as(mode).isTrue();
          assertThat(resultSet.getString(1)).as(mode).isEqualTo("read committed");
        }
      }
    }
  }

  /** psql's {@code \df} and {@code \du}, verbatim from describe.c: the listing a fresh database can produce is produced. */
  @Test
  void psqlListingsOverNoRowRelationsAreAnswered() throws Exception {
    for (final String mode : List.of("extended", "simple")) {
      try (final Connection connection = openJdbcConnection(mode); final Statement statement = connection.createStatement()) {
        try (final ResultSet resultSet = statement.executeQuery(Issue10591NoRowCatalogRelationsTest.PSQL_DF)) {
          assertThat(resultSet.getMetaData().getColumnLabel(2)).as(mode).isEqualTo("Name");
          assertThat(resultSet.next()).as(mode).isFalse();
        }
        try (final ResultSet resultSet = statement.executeQuery(Issue10591NoRowCatalogRelationsTest.PSQL_DU)) {
          assertThat(resultSet.next()).as(mode).isTrue();
          assertThat(resultSet.getString("rolname")).as(mode).isEqualTo("root");
          assertThat(resultSet.getArray("memberof").getArray()).as(mode).isEqualTo(new String[0]);
          assertThat(resultSet.next()).as(mode).isFalse();
        }
        // pg_depend is not among them since #10842: it lists the dependencies of the indexes, which this database has
        for (final String relation : List.of("pg_proc", "pg_trigger", "pg_auth_members", "pg_sequence", "pg_rewrite"))
          try (final ResultSet resultSet = statement.executeQuery("select count(*) from pg_catalog." + relation)) {
            assertThat(resultSet.next()).as("%s %s", mode, relation).isTrue();
            assertThat(resultSet.getLong(1)).as("%s %s", mode, relation).isZero();
          }
      }
    }
  }

  /** The refusal of #10371 stands for a relation whose absence would be a lie. */
  @Test
  void aRelationThatHoldsRowsInPostgresqlIsStillRefused() throws Exception {
    try (final Connection connection = openJdbcConnection("extended"); final Statement statement = connection.createStatement()) {
      assertThatThrownBy(() -> {
        try (final ResultSet resultSet = statement.executeQuery("select count(*) from pg_catalog.pg_stat_activity")) {
          resultSet.next();
        }
      }).isInstanceOf(SQLException.class).satisfies(e -> assertThat(((SQLException) e).getSQLState()).isEqualTo("0A000"))
          .hasMessageContaining("pg_stat_activity");
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
