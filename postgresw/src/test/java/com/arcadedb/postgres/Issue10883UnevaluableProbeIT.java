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
 * Issue #10883 over the wire: a FROM-less SELECT naming {@code pg_catalog} that the catalog cannot evaluate is refused with
 * {@code feature_not_supported} (0A000) on both protocols, instead of being acknowledged as a success with no row. The connection
 * goes on working after the refusal, the probes the catalog evaluates still answer their one row, and a probe whose argument is a
 * bound parameter is refused at Execute, once the value is in.
 */
class Issue10883UnevaluableProbeIT extends PostgresWireProtocolTestBase {

  @Test
  void anUnevaluableProbeIsRefusedAndTheConnectionGoesOn() throws Exception {
    for (final String mode : List.of("extended", "simple")) {
      try (final Connection connection = openJdbcConnection(mode)) {
        assertRefused(mode, connection, "SELECT pg_catalog.no_such_function_10883()");
        assertRefused(mode, connection, "SELECT pg_catalog.pg_postmaster_start_time()");

        try (final Statement statement = connection.createStatement();
            final ResultSet resultSet = statement.executeQuery("SELECT pg_catalog.set_config('search_path', '', false)")) {
          assertThat(resultSet.next()).as(mode).isTrue();
          assertThat(resultSet.getString(1)).as(mode).isEmpty();
          assertThat(resultSet.next()).as(mode).isFalse();
        }
      }
    }
  }

  @Test
  void aProbeWithABoundArgumentIsRefusedAtExecute() throws Exception {
    try (final Connection connection = openJdbcConnection("extended");
        final PreparedStatement statement = connection.prepareStatement("SELECT pg_catalog.no_such_function_10883(?)")) {
      statement.setString(1, "x");
      assertThatThrownBy(statement::executeQuery).isInstanceOf(SQLException.class)
          .satisfies(e -> assertThat(((SQLException) e).getSQLState()).isEqualTo("0A000"));
    }

    try (final Connection connection = openJdbcConnection("extended");
        final PreparedStatement statement = connection.prepareStatement("SELECT pg_catalog.set_config(?, ?, false)")) {
      statement.setString(1, "search_path");
      statement.setString(2, "public");
      try (final ResultSet resultSet = statement.executeQuery()) {
        assertThat(resultSet.next()).isTrue();
        assertThat(resultSet.getString(1)).isEqualTo("public");
      }
    }
  }

  private static void assertRefused(final String mode, final Connection connection, final String query) {
    assertThatThrownBy(() -> {
      try (final Statement statement = connection.createStatement(); final ResultSet resultSet = statement.executeQuery(query)) {
        resultSet.next();
      }
    }).as("%s: %s", mode, query).isInstanceOf(SQLException.class)
        .satisfies(e -> assertThat(((SQLException) e).getSQLState()).as(mode).isEqualTo("0A000"))
        .hasMessageContaining("session probe");
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
