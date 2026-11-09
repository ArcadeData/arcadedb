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
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * Issue #11392 through the PostgreSQL JDBC driver: {@code prepareStatement(sql, RETURN_GENERATED_KEYS)} and
 * {@code prepareStatement(sql, new String[] {...})} append {@code RETURNING *} / {@code RETURNING "col"} to the statement, which
 * the SQL grammar refused with {@code 42601}, so Hibernate IDENTITY, Spring {@code KeyHolder} and jOOQ could not insert. And an
 * index equivalent to one under another name answered {@code 22023} where the same-name duplicate answers {@code 42P07}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11392PostgresReturningIT extends PostgresWireProtocolTestBase {

  @Test
  void returnGeneratedKeysAnswersTheInsertedRow() throws Exception {
    try (final Connection connection = open()) {
      try (final Statement statement = connection.createStatement()) {
        statement.execute("CREATE DOCUMENT TYPE Gk");
      }

      try (final PreparedStatement insert = connection.prepareStatement("INSERT INTO Gk (name, n) VALUES (?, ?)",
          Statement.RETURN_GENERATED_KEYS)) {
        insert.setString(1, "first");
        insert.setInt(2, 1);
        assertThat(insert.executeUpdate()).isEqualTo(1);
        try (final ResultSet keys = insert.getGeneratedKeys()) {
          assertThat(keys.next()).isTrue();
          assertThat(keys.getString("name")).isEqualTo("first");
        }
      }

      try (final PreparedStatement insert = connection.prepareStatement("INSERT INTO Gk (name, n) VALUES (?, ?)",
          new String[] { "n" })) {
        insert.setString(1, "second");
        insert.setInt(2, 2);
        assertThat(insert.executeUpdate()).isEqualTo(1);
        try (final ResultSet keys = insert.getGeneratedKeys()) {
          assertThat(keys.next()).isTrue();
          assertThat(keys.getInt("n")).isEqualTo(2);
        }
      }

      // The identity ArcadeDB generates for a record is its RID: asked for by name, it is the RID of the record inserted
      final String rid;
      try (final PreparedStatement insert = connection.prepareStatement("INSERT INTO Gk (name, n) VALUES (?, ?)",
          new String[] { "@rid" })) {
        insert.setString(1, "third");
        insert.setInt(2, 3);
        assertThat(insert.executeUpdate()).isEqualTo(1);
        try (final ResultSet keys = insert.getGeneratedKeys()) {
          assertThat(keys.next()).isTrue();
          rid = keys.getString("@rid");
        }
      }
      assertThat(rid).startsWith("#");
      try (final Statement statement = connection.createStatement();
          final ResultSet byRid = statement.executeQuery("SELECT name FROM " + rid)) {
        assertThat(byRid.next()).isTrue();
        assertThat(byRid.getString("name")).isEqualTo("third");
      }

      // the same quoted attribute in a query is the attribute too, not the string "@rid"
      try (final Statement statement = connection.createStatement();
          final ResultSet byQuotedRid = statement.executeQuery("SELECT \"@rid\" FROM Gk WHERE name = 'third'")) {
        assertThat(byQuotedRid.next()).isTrue();
        assertThat(byQuotedRid.getString(1)).isEqualTo(rid);
      }

      try (final Statement statement = connection.createStatement();
          final ResultSet count = statement.executeQuery("SELECT count(*) AS c FROM Gk")) {
        assertThat(count.next()).isTrue();
        assertThat(count.getLong("c")).isEqualTo(3L);
      }
    }
  }

  @Test
  void anEquivalentIndexUnderAnotherNameIsDuplicateTable() throws Exception {
    try (final Connection connection = open(); final Statement statement = connection.createStatement()) {
      statement.execute("CREATE DOCUMENT TYPE Eqv");
      statement.execute("CREATE PROPERTY Eqv.a STRING");
      statement.execute("CREATE INDEX eqvA ON Eqv (a) NOTUNIQUE");

      final SQLException refused = catchThrowableOfType(SQLException.class,
          () -> statement.execute("CREATE INDEX eqvB ON Eqv (a) NOTUNIQUE"));
      assertThat(refused != null).as("the equivalent index is refused").isTrue();
      assertThat(refused.getSQLState()).isEqualTo("42P07");
    }
  }

  private Connection open() throws Exception {
    Class.forName("org.postgresql.Driver");
    final Properties properties = new Properties();
    properties.setProperty("user", "root");
    properties.setProperty("password", DEFAULT_PASSWORD_FOR_TESTS);
    properties.setProperty("ssl", "false");
    properties.setProperty("sslMode", "disable");
    return DriverManager.getConnection(getServerPostgresJdbcUrl(), properties);
  }
}
