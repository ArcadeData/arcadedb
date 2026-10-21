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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10371 over the wire: a catalog query over a relation the emulation does not model is refused with
 * {@code feature_not_supported} (0A000) on both protocols, instead of being answered with an empty result set that reads as an
 * empty catalog. {@code select count(*)} over such a relation used to send no row at all, where PostgreSQL always sends one, and
 * psql's {@code \di}, psqlodbc's {@code SQLPrimaryKeys} and {@code SQLStatistics} reported nothing. The connection goes on
 * working after the refusal, and a statement whose filters are bound parameters is refused at Execute, once the values are in.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10371DeclinedCatalogQueryIT extends PostgresWireProtocolTestBase {

  /** psql's {@code \di} lists indexes through pg_index; psqlodbc's SQLPrimaryKeys and SQLStatistics read it the same way. */
  private static final String INDEX_LISTING = "SELECT c.relname, i.indisunique FROM pg_catalog.pg_class c, pg_catalog.pg_index i "
      + "WHERE c.oid = i.indexrelid";

  @Test
  void anUngroupedCountOverAnUnmodelledRelationIsRefusedNotAnsweredWithNoRow() throws Exception {
    for (final String mode : List.of("extended", "simple")) {
      try (final Connection connection = openJdbcConnection(mode)) {
        assertRefused(mode, connection, "select count(*) from pg_catalog.pg_proc where proname = 'gen_random_uuid'", "pg_proc");
        assertRefused(mode, connection, "select count(*) from pg_catalog.pg_trigger", "pg_trigger");
        assertRefused(mode, connection, "select count(*) from pg_catalog.pg_extension", "pg_extension");

        // the connection is still usable, and a modelled relation answers PostgreSQL's one row holding 0
        try (final Statement statement = connection.createStatement(); final ResultSet resultSet = statement.executeQuery(
            "select count(*) from pg_catalog.pg_class where relname = 'Nope10371'")) {
          assertThat(resultSet.next()).as(mode).isTrue();
          assertThat(resultSet.getLong(1)).as(mode).isZero();
          assertThat(resultSet.next()).as(mode).isFalse();
        }
      }
    }
  }

  /** The index listings were refused while pg_index was not modelled; they are answered now (issue #10473). */
  @Test
  void theIndexListingsPsqlAndOdbcSendAreAnswered() throws Exception {
    for (final String mode : List.of("extended", "simple")) {
      try (final Connection connection = openJdbcConnection(mode)) {
        try (final Statement statement = connection.createStatement()) {
          statement.execute("CREATE DOCUMENT TYPE Listed10473 IF NOT EXISTS");
          statement.execute("CREATE PROPERTY Listed10473.name IF NOT EXISTS STRING");
          statement.execute("CREATE INDEX IF NOT EXISTS ON Listed10473 (name) UNIQUE");
        }

        try (final Statement statement = connection.createStatement(); final ResultSet resultSet = statement.executeQuery(
            INDEX_LISTING + " and c.relname like 'Listed10473%'")) {
          assertThat(resultSet.next()).as(mode).isTrue();
          assertThat(resultSet.getString("relname")).as(mode).isEqualTo("Listed10473[name]");
          assertThat(resultSet.getBoolean("indisunique")).as(mode).isTrue();
          assertThat(resultSet.next()).as(mode).isFalse();
        }
        try (final Statement statement = connection.createStatement(); final ResultSet resultSet = statement.executeQuery(
            "select constraint_type, table_name from information_schema.table_constraints where table_name = 'Listed10473'")) {
          assertThat(resultSet.next()).as(mode).isTrue();
          assertThat(resultSet.getString("constraint_type")).as(mode).isEqualTo("UNIQUE");
          assertThat(resultSet.next()).as(mode).isFalse();
        }
        try (final Statement statement = connection.createStatement(); final ResultSet resultSet = statement.executeQuery(
            "select column_name from information_schema.key_column_usage where table_name = 'Listed10473'")) {
          assertThat(resultSet.next()).as(mode).isTrue();
          assertThat(resultSet.getString("column_name")).as(mode).isEqualTo("name");
          assertThat(resultSet.next()).as(mode).isFalse();
        }
      }
    }
  }

  /** The extended protocol sees the query at Parse, before the values of its filters arrive: the refusal is final at Execute. */
  @Test
  void aRefusedCatalogQueryWithBoundParametersIsRefusedAtExecute() throws Exception {
    try (final Connection connection = openJdbcConnection("extended");
        final PreparedStatement statement = connection.prepareStatement("select count(*) from pg_catalog.pg_proc where proname = ?")) {
      statement.setString(1, "gen_random_uuid");

      assertThatThrownBy(statement::executeQuery).isInstanceOf(SQLException.class)
          .satisfies(e -> assertThat(((SQLException) e).getSQLState()).isEqualTo("0A000"))
          .hasMessageContaining("pg_proc");

      // the same statement, run again on the same connection, is refused again rather than hanging or answering empty
      assertThatThrownBy(statement::executeQuery).isInstanceOf(SQLException.class)
          .satisfies(e -> assertThat(((SQLException) e).getSQLState()).isEqualTo("0A000"));
    }
  }

  /** {@code \d}'s header step reads relhasindex, which follows the indexes the catalog lists (issue #10473). */
  @Test
  void aTableWithAnIndexClaimsOneAndTheCatalogShowsIt() throws Exception {
    try (final Connection connection = openJdbcConnection("extended")) {
      try (final Statement statement = connection.createStatement()) {
        statement.execute("CREATE DOCUMENT TYPE Person10371 IF NOT EXISTS");
        statement.execute("CREATE PROPERTY Person10371.name IF NOT EXISTS STRING");
        statement.execute("CREATE INDEX IF NOT EXISTS ON Person10371 (name) UNIQUE");
      }

      try (final Statement statement = connection.createStatement(); final ResultSet resultSet = statement.executeQuery(
          "select relname, relhasindex from pg_catalog.pg_class where relname = 'Person10371'")) {
        assertThat(resultSet.next()).isTrue();
        assertThat(resultSet.getBoolean("relhasindex")).isTrue();
      }
    }
  }

  private static void assertRefused(final String mode, final Connection connection, final String query, final String relation) {
    assertThatThrownBy(() -> {
      try (final Statement statement = connection.createStatement(); final ResultSet resultSet = statement.executeQuery(query)) {
        resultSet.next();
      }
    }).as("%s: %s", mode, query).isInstanceOf(SQLException.class)
        .satisfies(e -> assertThat(((SQLException) e).getSQLState()).as(mode).isEqualTo("0A000"))
        .hasMessageContaining(relation);
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
