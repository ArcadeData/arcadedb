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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12711: a text parameter of unspecified type (OID 0, what psycopg 3 sends for every Python {@code str}, and pgjdbc with
 * {@code stringtype=unspecified}) that looks like an array literal ({@code {name}}, {@code {a,b}}, {@code {}}) was read as a list,
 * so a STRING property stored the list's Java text ({@code [name]}, {@code [a, b]}, {@code []}). It now stores the text that was
 * sent, while a schemaless property keeps reading the literal as the list psycopg sends a Python list as (issue #2559).
 */
class Issue12711UnspecifiedTextParameterIT extends PostgresWireProtocolTestBase {
  private static final List<String> TEXTS = List.of("{name}", "{a,b}", "{}", "Hello {name}!", "{\"k\": 1}", "{\"a\",NULL,\"\"}");

  @BeforeEach
  void createSchema() {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType("T12711");
      type.createProperty("k", Type.INTEGER);
      type.createProperty("s", Type.STRING);
      // the same text in an indexed property: an equality on it is answered by an index lookup, not by the filter
      type.createProperty("x", Type.STRING).createIndex(Schema.INDEX_TYPE.LSM_TREE, false);
    });
  }

  @ParameterizedTest
  @ValueSource(strings = { "0", "-1" })
  void aStringPropertyStoresExactlyTheTextSent(final String prepareThreshold) throws Exception {
    try (final Connection connection = connect(prepareThreshold);
        final PreparedStatement insert = connection.prepareStatement("INSERT INTO T12711 SET k = ?, s = ?")) {
      for (int i = 0; i < TEXTS.size(); i++) {
        insert.setInt(1, i);
        insert.setString(2, TEXTS.get(i));
        insert.execute();
      }
    }

    final Database database = getServerDatabase(0, getDatabaseName());
    for (int i = 0; i < TEXTS.size(); i++)
      try (final ResultSet rs = database.query("sql", "SELECT s, s.type() AS t FROM T12711 WHERE k = ?", i)) {
        final Result row = rs.next();
        assertThat(row.<Object>getProperty("s")).as(TEXTS.get(i)).isEqualTo(TEXTS.get(i));
        assertThat(row.<String>getProperty("t")).as(TEXTS.get(i)).isEqualTo("STRING");
      }
  }

  @ParameterizedTest
  @ValueSource(strings = { "0", "-1" })
  void anUpdateOfAStringPropertyStoresExactlyTheTextSent(final String prepareThreshold) throws Exception {
    getServerDatabase(0, getDatabaseName()).command("sql", "INSERT INTO T12711 SET k = 1, s = 'before'");
    try (final Connection connection = connect(prepareThreshold);
        final PreparedStatement update = connection.prepareStatement("UPDATE T12711 SET s = ? WHERE k = 1")) {
      update.setString(1, "{a,b}");
      update.execute();
    }
    try (final ResultSet rs = getServerDatabase(0, getDatabaseName()).query("sql", "SELECT s FROM T12711 WHERE k = 1")) {
      assertThat(rs.next().<Object>getProperty("s")).isEqualTo("{a,b}");
    }
  }

  @ParameterizedTest
  @ValueSource(strings = { "0", "-1" })
  void aStringPropertyComparesWithTheTextSent(final String prepareThreshold) throws Exception {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.command("sql", "INSERT INTO T12711 SET k = 1, s = '{a,b}', x = '{a,b}'");
    database.command("sql", "INSERT INTO T12711 SET k = 2, s = '{name}', x = '{name}'");
    database.command("sql", "INSERT INTO T12711 SET k = 3, s = '{}', x = '{}'");

    try (final Connection connection = connect(prepareThreshold)) {
      assertThat(keys(connection, "SELECT k FROM T12711 WHERE s = ?", "{a,b}")).containsExactly(1);
      assertThat(keys(connection, "SELECT k FROM T12711 WHERE s = ?", "{name}")).containsExactly(2);
      assertThat(keys(connection, "SELECT k FROM T12711 WHERE s = ?", "{}")).containsExactly(3);
      assertThat(keys(connection, "SELECT k FROM T12711 WHERE x = ?", "{a,b}")).containsExactly(1);
      assertThat(keys(connection, "SELECT k FROM T12711 WHERE x = ?", "{name}")).containsExactly(2);
      assertThat(keys(connection, "SELECT k FROM T12711 WHERE x = ?", "{}")).containsExactly(3);
    }
  }

  /** The trade-off kept on purpose: psycopg sends a Python list exactly as it sends this text, so an undeclared property reads it as a list. */
  @ParameterizedTest
  @ValueSource(strings = { "0", "-1" })
  void aSchemalessPropertyStillReadsAnArrayLiteralAsAList(final String prepareThreshold) throws Exception {
    try (final Connection connection = connect(prepareThreshold);
        final PreparedStatement insert = connection.prepareStatement("INSERT INTO T12711 SET k = 1, u = ?")) {
      insert.setString(1, "{a,b}");
      insert.execute();
    }
    try (final ResultSet rs = getServerDatabase(0, getDatabaseName()).query("sql", "SELECT u, u.type() AS t FROM T12711 WHERE k = 1")) {
      final Result row = rs.next();
      assertThat(row.<String>getProperty("t")).isEqualTo("LIST");
      assertThat(row.<List<Object>>getProperty("u")).containsExactly("a", "b");
    }
  }

  private static List<Integer> keys(final Connection connection, final String query, final String parameter) throws Exception {
    final List<Integer> keys = new ArrayList<>();
    try (final PreparedStatement statement = connection.prepareStatement(query)) {
      statement.setString(1, parameter);
      try (final java.sql.ResultSet rs = statement.executeQuery()) {
        while (rs.next())
          keys.add(rs.getInt(1));
      }
    }
    return keys;
  }

  private Connection connect(final String prepareThreshold) throws Exception {
    final Properties properties = new Properties();
    properties.setProperty("user", "root");
    properties.setProperty("password", DEFAULT_PASSWORD_FOR_TESTS);
    properties.setProperty("sslmode", "disable");
    properties.setProperty("prepareThreshold", prepareThreshold);
    // a String parameter goes out with the unspecified type (OID 0), as psycopg 3 sends a Python str
    properties.setProperty("stringtype", "unspecified");
    return DriverManager.getConnection(getServerPostgresJdbcUrl(), properties);
  }
}
