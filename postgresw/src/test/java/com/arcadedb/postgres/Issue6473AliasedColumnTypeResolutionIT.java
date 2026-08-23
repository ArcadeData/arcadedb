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
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #6473, the follow-up to #6447/#5289: {@code PostgresNetworkExecutor.getDeclaredProperty()}
 * looked a projected column's schema property up by the ROW's own column name, which for an aliased projection
 * ({@code SELECT amount AS x FROM Type}) is the alias, not the source property - so both schema-context
 * fallbacks it backs (the empty-LIST element type, #5289; the DATE/DATETIME java.util.Date disambiguation,
 * #6447) missed whenever the projected column carried an alias, and fell back to a value-only guess instead.
 * <p>
 * Both scenarios below need a real ROW - not an empty result set - with a narrow (non-whole-entity) aliased
 * projection: that is exactly the shape that reaches {@code getDeclaredProperty}'s {@code queryTargetType}
 * fallback. A zero-row probe would instead be answered by {@code getColumnsFromQuerySchema}, an unrelated,
 * already alias-aware static resolution that does not exercise this code path at all.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue6473AliasedColumnTypeResolutionIT extends PostgresWireProtocolTestBase {

  @Test
  void anAliasedEmptyListPropertyReportsTheDeclaredElementTypeInsteadOfDefaultingToText() throws Exception {
    try (final Connection connection = openJdbcConnection()) {
      createTestType(connection);

      try (final Statement insert = connection.createStatement()) {
        // An empty list carries no element to infer the type from - the value alone cannot tell int4[] from
        // text[], only the schema's "LIST OF INTEGER" declaration can (issue #5289).
        insert.execute("INSERT INTO Types6473 SET id = 1, tags = []");
      }

      final String typeFromRow;
      try (final PreparedStatement select = connection.prepareStatement("SELECT tags AS x FROM Types6473 WHERE id = 1");
          final ResultSet resultSet = select.executeQuery()) {
        assertThat(resultSet.next()).isTrue();
        typeFromRow = resultSet.getMetaData().getColumnTypeName(1);
      }

      assertThat(typeFromRow)
          .as("an alias must not defeat the declared-LIST-element-type fallback: without resolving 'x' back to "
              + "'tags', an empty list reports the default text[] instead of the declared int4[]")
          .isEqualTo("_int4");
    }
  }

  @Test
  void anAliasedDatetimePropertyRoundTripsAsTimestampWhenDateTimeImplementationIsJavaUtilDate() throws Exception {
    // Same java.util.Date/DATE ambiguity #6447 disambiguates via the schema (see Issue6447TypeResolutionIT),
    // but through an alias: "SELECT whenHappened AS x" - x is not a schema property, so getDeclaredProperty
    // must resolve it back to "whenHappened" before isDeclaredAsDatetime can even run.
    try (final Connection connection = openJdbcConnection()) {
      try (final Statement configure = connection.createStatement()) {
        configure.execute("ALTER DATABASE dateTimeImplementation `java.util.Date`");
      }
      createTestType(connection);

      try (final Statement insert = connection.createStatement()) {
        insert.execute("INSERT INTO Types6473 SET id = 1, whenHappened = '2026-08-19 12:34:56'");
      }

      final String typeFromRow;
      try (final PreparedStatement select = connection.prepareStatement(
          "SELECT whenHappened AS x FROM Types6473 WHERE id = 1");
          final ResultSet resultSet = select.executeQuery()) {
        assertThat(resultSet.next()).isTrue();
        typeFromRow = resultSet.getMetaData().getColumnTypeName(1);
      }

      assertThat(typeFromRow)
          .as("without resolving the alias back to its source property, a java.util.Date-backed DATETIME "
              + "sample resolves to plain date instead of timestamp")
          .isEqualTo("timestamp");
    }
  }

  private void createTestType(final Connection connection) throws Exception {
    try (final Statement statement = connection.createStatement()) {
      statement.execute("CREATE DOCUMENT TYPE Types6473 IF NOT EXISTS");
      statement.execute("CREATE PROPERTY Types6473.id IF NOT EXISTS INTEGER");
      statement.execute("CREATE PROPERTY Types6473.tags IF NOT EXISTS LIST OF INTEGER");
      statement.execute("CREATE PROPERTY Types6473.whenHappened IF NOT EXISTS DATETIME");
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
