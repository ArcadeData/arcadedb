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
 * Issue #10882: the recogniser of the JDBC driver's pg_type element self-join decided on the raw text of the query, string
 * literals included, so a query over the user's own type whose literal spelled the join out was answered with the element of
 * array 1007 instead of being executed. It also answered a column of the array alias as if it were the element's.
 */
class Issue10882ElementJoinInDataIT extends PostgresWireProtocolTestBase {

  private static final String LITERAL = "pg_ typelem = e.oid and t.oid = 1007";

  @Test
  void aLiteralSpellingTheJoinOutIsExecutedOverTheUsersType() throws Exception {
    for (final String mode : List.of("extended", "simple")) {
      final String type = "Note10882" + mode;
      try (final Connection connection = openJdbcConnection(mode); final Statement statement = connection.createStatement()) {
        statement.execute("CREATE DOCUMENT TYPE " + type);
        statement.execute("CREATE PROPERTY " + type + ".typname STRING");
        statement.execute("INSERT INTO " + type + " SET typname = '" + LITERAL + "'");
        statement.execute("INSERT INTO " + type + " SET typname = 'other'");

        assertThat(rows(statement, "SELECT typname FROM " + type + " WHERE typname = '" + LITERAL + "'")).as(mode)
            .containsExactly(List.of(LITERAL));
      }
    }
  }

  @Test
  void theDriverSelfJoinStillDescribesTheElementAndTheArrayAliasStillTheArray() throws Exception {
    for (final String mode : List.of("extended", "simple")) {
      try (final Connection connection = openJdbcConnection(mode); final Statement statement = connection.createStatement()) {
        assertThat(rows(statement,
            "SELECT e.typdelim, e.typname FROM pg_catalog.pg_type t, pg_catalog.pg_type e WHERE t.oid = 1007 AND t.typelem = e.oid"))
            .as(mode).containsExactly(List.of(",", "int4"));

        // a column of the array alias is the array's, not the element's
        assertThat(rows(statement, "SELECT t.typname, e.typname FROM pg_type t, pg_type e WHERE t.oid = 1007 AND t.typelem = e.oid"))
            .as(mode).containsExactly(List.of("_int4", "int4"));
      }
    }
  }

  private static List<List<String>> rows(final Statement statement, final String query) throws Exception {
    final List<List<String>> rows = new ArrayList<>();
    try (final ResultSet resultSet = statement.executeQuery(query)) {
      final int columns = resultSet.getMetaData().getColumnCount();
      while (resultSet.next()) {
        final List<String> row = new ArrayList<>(columns);
        for (int i = 1; i <= columns; i++)
          row.add(resultSet.getString(i));
        rows.add(row);
      }
    }
    return rows;
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
