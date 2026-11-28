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
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A double-quoted name that starts with {@code @} is read with PostgreSQL's rules ({@code ""} is a quote, a backslash is an
 * ordinary character) and reaches the engine as one name, whatever it holds: an application that quotes a name for PostgreSQL
 * gets the same token boundaries on this server, on both query protocols.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class PostgresQuotedAtNameIT extends PostgresWireProtocolTestBase {

  @Test
  void aQuotedAtNameIsOneNameWhateverItHolds() throws Exception {
    createDoc();

    final String[] names = { "@\" , 1 AS marker FROM Doc -- ", "@\\\" , 1 AS marker FROM Doc -- ", "@\\\" , 1 AS marker FROM Doc /* ",
        "@\\\"\" , 1 AS marker FROM Doc -- " };

    for (final String mode : new String[] { "simple", "extended" })
      try (final Connection connection = connect(mode)) {
        for (final String name : names)
          try (final Statement statement = connection.createStatement(); final ResultSet resultSet = statement.executeQuery(
              "SELECT " + quote(name) + " AS c FROM Doc")) {
            // the name is one projection, whatever it holds
            assertThat(labelsOf(resultSet)).as("preferQueryMode=%s, name %s", mode, name).doesNotContain("marker");
            int rows = 0;
            while (resultSet.next()) {
              assertThat(resultSet.getString("c")).as("preferQueryMode=%s, name %s", mode, name).isNull();
              rows++;
            }
            assertThat(rows).isEqualTo(3);
          }
      }
  }

  @Test
  void aQuotedAtNameInAnOrderingIsOneSortKey() throws Exception {
    createDoc();

    for (final String mode : new String[] { "simple", "extended" })
      try (final Connection connection = connect(mode); final Statement statement = connection.createStatement()) {
        // one sort key naming nothing, so every record is answered
        final String name = "@\\\" , title DESC LIMIT 1 -- ";
        final List<String> titles = new ArrayList<>();
        try (final ResultSet resultSet = statement.executeQuery("SELECT title FROM Doc ORDER BY " + quote(name))) {
          while (resultSet.next())
            titles.add(resultSet.getString("title"));
        }
        assertThat(titles).as("preferQueryMode=%s", mode).containsExactlyInAnyOrder("first", "second", "third");
      }
  }

  @Test
  void aQuotedRecordAttributeStillNamesTheAttribute() throws Exception {
    createDoc();

    for (final String mode : new String[] { "simple", "extended" })
      try (final Connection connection = connect(mode); final Statement statement = connection.createStatement();
          final ResultSet resultSet = statement.executeQuery("SELECT \"@rid\" AS r, \"@type\" AS t, \"title\" FROM Doc ORDER BY \"title\"")) {
        final List<String> titles = new ArrayList<>();
        while (resultSet.next()) {
          assertThat(resultSet.getString("r")).as("preferQueryMode=%s", mode).startsWith("#");
          assertThat(resultSet.getString("t")).isEqualTo("Doc");
          titles.add(resultSet.getString("title"));
        }
        assertThat(titles).containsExactly("first", "second", "third");
      }
  }

  /** The name quoted the way PostgreSQL quotes an identifier: a double quote is doubled and nothing else is escaped. */
  private static String quote(final String name) {
    return "\"" + name.replace("\"", "\"\"") + "\"";
  }

  private static List<String> labelsOf(final ResultSet resultSet) throws SQLException {
    final ResultSetMetaData metaData = resultSet.getMetaData();
    final List<String> labels = new ArrayList<>();
    for (int i = 1; i <= metaData.getColumnCount(); i++)
      labels.add(metaData.getColumnLabel(i));
    return labels;
  }

  private void createDoc() throws SQLException {
    try (final Connection connection = connect("simple"); final Statement statement = connection.createStatement()) {
      statement.execute("CREATE DOCUMENT TYPE Doc");
      statement.execute("INSERT INTO Doc CONTENT {\"title\": 'first'}");
      statement.execute("INSERT INTO Doc CONTENT {\"title\": 'second'}");
      statement.execute("INSERT INTO Doc CONTENT {\"title\": 'third'}");
    }
  }

  private Connection connect(final String queryMode) throws SQLException {
    final Properties properties = new Properties();
    properties.setProperty("user", "root");
    properties.setProperty("password", DEFAULT_PASSWORD_FOR_TESTS);
    properties.setProperty("ssl", "false");
    properties.setProperty("preferQueryMode", queryMode);
    return DriverManager.getConnection(getServerPostgresJdbcUrl(), properties);
  }
}
