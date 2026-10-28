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
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10695: a statement whose TEXT contained "pg_catalog" or "pg_type" anywhere - a string literal, a column name, a word
 * inside a longer one - was never executed on the Postgres wire and was acknowledged as a success: an INSERT answered
 * {@code INSERT 0 0} and stored nothing, a SELECT answered no rows. Every value of a corpus holding each relation name the
 * catalog knows is written, updated, read back and deleted through both protocols, and must round-trip.
 */
class Issue10695CatalogWordInDataIT extends PostgresWireProtocolTestBase {

  private static final List<String> CORPUS = List.of("plain", "pg_catalog", "PG_CATALOG", "xxpg_typexx", "pg_type", "pg_class",
      "information_schema", "pg_", "see the pg_catalog docs", "pg_catalog.pg_type");

  @Test
  void aValueNamingACatalogRelationRoundTripsThroughEveryStatement() throws Exception {
    for (final String mode : List.of("extended", "simple")) {
      final String type = "Note10695" + mode;
      try (final Connection connection = openJdbcConnection(mode); final Statement statement = connection.createStatement()) {
        statement.execute("CREATE DOCUMENT TYPE " + type);
        statement.execute("CREATE PROPERTY " + type + ".body STRING");

        // ArcadeDB's INSERT returns the record it stored, so the evidence of the write is reading it back
        for (final String value : CORPUS)
          statement.execute("INSERT INTO " + type + " SET body = '" + value + "'");
        assertThat(bodies(statement, "SELECT body FROM " + type + " ORDER BY body")).as(mode)
            .containsExactlyInAnyOrderElementsOf(CORPUS);

        // a read whose filter holds the word reaches the rows
        assertThat(bodies(statement, "SELECT body FROM " + type + " WHERE body LIKE '%pg_catalog%'")).as(mode)
            .containsExactlyInAnyOrder("pg_catalog", "see the pg_catalog docs", "pg_catalog.pg_type");

        // an UPDATE and a DELETE whose literals hold the words are executed
        statement.execute("UPDATE " + type + " SET body = 'pg_catalog rules' WHERE body = 'plain'");
        statement.execute("DELETE FROM " + type + " WHERE body = 'pg_type'");
        assertThat(bodies(statement, "SELECT body FROM " + type + " WHERE body = 'pg_catalog rules'")).as(mode)
            .containsExactly("pg_catalog rules");
        assertThat(bodies(statement, "SELECT body FROM " + type + " WHERE body = 'pg_type'")).as(mode).isEmpty();
      }
    }
  }

  /** A column the user called pg_type is theirs: creating, writing and reading it is not a catalog query. */
  @Test
  void aColumnCalledLikeACatalogRelationIsTheUsers() throws Exception {
    for (final String mode : List.of("extended", "simple")) {
      final String type = "Typed10695" + mode;
      try (final Connection connection = openJdbcConnection(mode); final Statement statement = connection.createStatement()) {
        statement.execute("CREATE DOCUMENT TYPE " + type);
        statement.execute("CREATE PROPERTY " + type + ".pg_type STRING");
        statement.execute("INSERT INTO " + type + " SET pg_type = 'v'");
        try (final ResultSet resultSet = statement.executeQuery("SELECT pg_type FROM " + type)) {
          assertThat(resultSet.next()).as(mode).isTrue();
          assertThat(resultSet.getString(1)).as(mode).isEqualTo("v");
          assertThat(resultSet.next()).as(mode).isFalse();
        }
      }
    }
  }

  /** The value bound as a parameter was never at risk; it still is not, and a prepared INSERT with an inlined literal stores too. */
  @Test
  void aPreparedInsertStoresWhetherTheWordIsBoundOrInlined() throws Exception {
    try (final Connection connection = openJdbcConnection("extended")) {
      try (final Statement statement = connection.createStatement()) {
        statement.execute("CREATE DOCUMENT TYPE Prepared10695");
        statement.execute("CREATE PROPERTY Prepared10695.body STRING");
      }
      try (final PreparedStatement insert = connection.prepareStatement("INSERT INTO Prepared10695 SET body = ?")) {
        insert.setString(1, "bound pg_catalog");
        insert.execute();
      }
      try (final PreparedStatement insert = connection.prepareStatement("INSERT INTO Prepared10695 SET body = 'jdbc pg_catalog inline'")) {
        insert.execute();
      }
      try (final Statement statement = connection.createStatement()) {
        assertThat(bodies(statement, "SELECT body FROM Prepared10695")).containsExactlyInAnyOrder("bound pg_catalog",
            "jdbc pg_catalog inline");
      }
    }
  }

  /** A FROM-less probe the client qualifies with pg_catalog answers PostgreSQL's one row, not the empty set it used to get. */
  @Test
  void aQualifiedSessionProbeAnswersOneRow() throws Exception {
    for (final String mode : List.of("extended", "simple")) {
      try (final Connection connection = openJdbcConnection(mode); final Statement statement = connection.createStatement()) {
        final int pid;
        try (final ResultSet resultSet = statement.executeQuery("select pg_backend_pid()")) {
          assertThat(resultSet.next()).as(mode).isTrue();
          pid = resultSet.getInt(1);
        }
        try (final ResultSet resultSet = statement.executeQuery("select pg_catalog.pg_backend_pid()")) {
          assertThat(resultSet.next()).as(mode).isTrue();
          assertThat(resultSet.getInt(1)).as(mode).isEqualTo(pid);
          assertThat(resultSet.next()).as(mode).isFalse();
        }
        try (final ResultSet resultSet = statement.executeQuery("SELECT pg_catalog.set_config('search_path', '', false)")) {
          assertThat(resultSet.next()).as(mode).isTrue();
          assertThat(resultSet.getString(1)).as(mode).isEmpty();
          assertThat(resultSet.next()).as(mode).isFalse();
        }
        try (final ResultSet resultSet = statement.executeQuery("/* app */ SELECT pg_catalog.set_config('search_path', '', false)")) {
          assertThat(resultSet.next()).as(mode).isTrue();
          assertThat(resultSet.getString(1)).as(mode).isEmpty();
          assertThat(resultSet.next()).as(mode).isFalse();
        }
        try (final ResultSet resultSet = statement.executeQuery("SELECT pg_catalog.pg_is_in_recovery()")) {
          assertThat(resultSet.next()).as(mode).isTrue();
          assertThat(resultSet.getBoolean(1)).as(mode).isFalse();
          assertThat(resultSet.next()).as(mode).isFalse();
        }
      }
    }
  }

  private static List<String> bodies(final Statement statement, final String query) throws Exception {
    final List<String> bodies = new ArrayList<>();
    try (final ResultSet resultSet = statement.executeQuery(query)) {
      while (resultSet.next())
        bodies.add(resultSet.getString(1));
    }
    return bodies;
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
