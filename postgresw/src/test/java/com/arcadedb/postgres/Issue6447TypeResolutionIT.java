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

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #6447, the follow-up to #6411 (BYTEA): a column's announced OID depended on
 * whether the result set happened to be empty (typed from the declared schema, {@link PostgresType#getTypeFromArcade})
 * or had a sampled row (typed from the value, {@link PostgresType#getTypeForValue}), for three more types
 * ({@code SHORT}/{@code BYTE}, {@code DATETIME} and {@code DECIMAL}) beyond the one #6411 already fixed.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue6447TypeResolutionIT extends PostgresWireProtocolTestBase {

  @Test
  void aDecimalPropertyRoundTripsAsNumericWhetherOrNotARowWasSampled() throws Exception {
    try (final Connection connection = openJdbcConnection()) {
      createTestType(connection);

      final BigDecimal payload = new BigDecimal("12345.6789");
      try (final PreparedStatement insert = connection.prepareStatement("INSERT INTO Types6447 SET id = 1, amount = ?")) {
        insert.setBigDecimal(1, payload);
        insert.execute();
      }

      final String typeFromRow;
      try (final PreparedStatement select = connection.prepareStatement("SELECT amount FROM Types6447 WHERE id = 1");
          final ResultSet resultSet = select.executeQuery()) {
        assertThat(resultSet.next()).isTrue();
        typeFromRow = resultSet.getMetaData().getColumnTypeName(1);
        assertThat(resultSet.getBigDecimal("amount")).isEqualByComparingTo(payload);
      }

      final String typeFromSchema;
      try (final PreparedStatement select = connection.prepareStatement("SELECT amount FROM Types6447 WHERE id = 2");
          final ResultSet resultSet = select.executeQuery()) {
        assertThat(resultSet.next()).isFalse();
        final ResultSetMetaData metaData = resultSet.getMetaData();
        typeFromSchema = metaData.getColumnTypeName(1);
      }

      assertThat(typeFromRow).isEqualTo("numeric");
      assertThat(typeFromSchema)
          .as("the column's OID must not depend on whether the result set happens to be empty")
          .isEqualTo(typeFromRow);
    }
  }

  @Test
  void aShortPropertyRoundTripsAsSmallintWhetherOrNotARowWasSampled() throws Exception {
    try (final Connection connection = openJdbcConnection()) {
      createTestType(connection);

      try (final PreparedStatement insert = connection.prepareStatement("INSERT INTO Types6447 SET id = 1, aShort = ?")) {
        insert.setShort(1, (short) 1234);
        insert.execute();
      }

      final String typeFromRow;
      try (final PreparedStatement select = connection.prepareStatement("SELECT aShort FROM Types6447 WHERE id = 1");
          final ResultSet resultSet = select.executeQuery()) {
        assertThat(resultSet.next()).isTrue();
        typeFromRow = resultSet.getMetaData().getColumnTypeName(1);
        assertThat(resultSet.getShort("aShort")).isEqualTo((short) 1234);
      }

      final String typeFromSchema;
      try (final PreparedStatement select = connection.prepareStatement("SELECT aShort FROM Types6447 WHERE id = 2");
          final ResultSet resultSet = select.executeQuery()) {
        assertThat(resultSet.next()).isFalse();
        typeFromSchema = resultSet.getMetaData().getColumnTypeName(1);
      }

      assertThat(typeFromRow).isEqualTo("int2");
      assertThat(typeFromSchema)
          .as("the column's OID must not depend on whether the result set happens to be empty")
          .isEqualTo(typeFromRow);
    }
  }

  @Test
  void aBytePropertyRoundTripsAsSmallintWhetherOrNotARowWasSampled() throws Exception {
    try (final Connection connection = openJdbcConnection()) {
      createTestType(connection);

      try (final PreparedStatement insert = connection.prepareStatement("INSERT INTO Types6447 SET id = 1, aByte = ?")) {
        insert.setByte(1, (byte) 42);
        insert.execute();
      }

      final String typeFromRow;
      try (final PreparedStatement select = connection.prepareStatement("SELECT aByte FROM Types6447 WHERE id = 1");
          final ResultSet resultSet = select.executeQuery()) {
        assertThat(resultSet.next()).isTrue();
        typeFromRow = resultSet.getMetaData().getColumnTypeName(1);
        assertThat(resultSet.getByte("aByte")).isEqualTo((byte) 42);
      }

      final String typeFromSchema;
      try (final PreparedStatement select = connection.prepareStatement("SELECT aByte FROM Types6447 WHERE id = 2");
          final ResultSet resultSet = select.executeQuery()) {
        assertThat(resultSet.next()).isFalse();
        typeFromSchema = resultSet.getMetaData().getColumnTypeName(1);
      }

      assertThat(typeFromRow).isEqualTo("int2");
      assertThat(typeFromSchema)
          .as("the column's OID must not depend on whether the result set happens to be empty")
          .isEqualTo(typeFromRow);
    }
  }

  @Test
  void aDatetimePropertyRoundTripsAsTimestampWhetherOrNotARowWasSampled() throws Exception {
    try (final Connection connection = openJdbcConnection()) {
      createTestType(connection);

      final LocalDateTime payload = LocalDateTime.of(2026, 8, 19, 12, 34, 56);
      try (final Statement insert = connection.createStatement()) {
        insert.execute("INSERT INTO Types6447 SET id = 1, whenHappened = '2026-08-19 12:34:56'");
      }

      final String typeFromRow;
      try (final PreparedStatement select = connection.prepareStatement("SELECT whenHappened FROM Types6447 WHERE id = 1");
          final ResultSet resultSet = select.executeQuery()) {
        assertThat(resultSet.next()).isTrue();
        typeFromRow = resultSet.getMetaData().getColumnTypeName(1);
        // Timestamp.toString() formats in the JVM default timezone, so compare via toLocalDateTime() instead.
        assertThat(resultSet.getTimestamp("whenHappened").toLocalDateTime()).isEqualTo(payload);
      }

      final String typeFromSchema;
      try (final PreparedStatement select = connection.prepareStatement("SELECT whenHappened FROM Types6447 WHERE id = 2");
          final ResultSet resultSet = select.executeQuery()) {
        assertThat(resultSet.next()).isFalse();
        typeFromSchema = resultSet.getMetaData().getColumnTypeName(1);
      }

      assertThat(typeFromRow)
          .as("a DATETIME property must not be reported as plain date - it has a time component")
          .isEqualTo("timestamp");
      assertThat(typeFromSchema)
          .as("the column's OID must not depend on whether the result set happens to be empty")
          .isEqualTo(typeFromRow);
    }
  }

  @Test
  void aDatetimePropertyRoundTripsAsTimestampWhenDateTimeImplementationIsJavaUtilDate() throws Exception {
    // Type.DATETIME's runtime representation defaults to LocalDateTime, which the value path already told
    // apart from DATE before this fix (that's what the previous test exercises). java.util.Date is the
    // *other* supported representation, and it is ALSO Type.DATE's default - so with this configuration a
    // sampled value alone genuinely cannot disambiguate, and PostgresNetworkExecutor.isDeclaredAsDatetime
    // (the mechanism this PR adds) is what makes the populated-row case answer correctly instead of "date".
    try (final Connection connection = openJdbcConnection()) {
      try (final Statement configure = connection.createStatement()) {
        configure.execute("ALTER DATABASE dateTimeImplementation `java.util.Date`");
      }
      createTestType(connection);

      try (final Statement insert = connection.createStatement()) {
        insert.execute("INSERT INTO Types6447 SET id = 1, whenHappened = '2026-08-19 12:34:56'");
      }

      final String typeFromRow;
      try (final PreparedStatement select = connection.prepareStatement("SELECT whenHappened FROM Types6447 WHERE id = 1");
          final ResultSet resultSet = select.executeQuery()) {
        assertThat(resultSet.next()).isTrue();
        typeFromRow = resultSet.getMetaData().getColumnTypeName(1);
      }

      final String typeFromSchema;
      try (final PreparedStatement select = connection.prepareStatement("SELECT whenHappened FROM Types6447 WHERE id = 2");
          final ResultSet resultSet = select.executeQuery()) {
        assertThat(resultSet.next()).isFalse();
        typeFromSchema = resultSet.getMetaData().getColumnTypeName(1);
      }

      assertThat(typeFromRow)
          .as("without isDeclaredAsDatetime, a java.util.Date-backed DATETIME sample resolves to plain date")
          .isEqualTo("timestamp");
      assertThat(typeFromSchema).isEqualTo(typeFromRow);
    }
  }

  @Test
  void aDatetimePropertyRoundTripsAsTimestampOverTheSimpleQueryProtocolToo() throws Exception {
    // The previous two tests go through the extended query protocol (PreparedStatement), which is what every
    // real JDBC/ORM client uses. This forces pgjdbc's simple query protocol instead - the path
    // PostgresNetworkExecutor.queryCommand() implements, where a review round found the query-target-type
    // parse being computed unconditionally (before the SET/SHOW/SAVEPOINT/BEGIN dispatch) was both a
    // performance regression and something worth a dedicated regression test. Same narrow column projection
    // ("SELECT col FROM Type", not "SELECT * FROM Type") that requires resolveQueryTargetType's fallback.
    try (final Connection connection = openJdbcConnection(true)) {
      try (final Statement configure = connection.createStatement()) {
        configure.execute("ALTER DATABASE dateTimeImplementation `java.util.Date`");
      }
      createTestType(connection);

      try (final Statement insert = connection.createStatement()) {
        insert.execute("INSERT INTO Types6447 SET id = 1, whenHappened = '2026-08-19 12:34:56'");
      }

      try (final Statement select = connection.createStatement();
          final ResultSet resultSet = select.executeQuery("SELECT whenHappened FROM Types6447 WHERE id = 1")) {
        assertThat(resultSet.next()).isTrue();
        assertThat(resultSet.getMetaData().getColumnTypeName(1)).isEqualTo("timestamp");
      }
    }
  }

  private void createTestType(final Connection connection) throws Exception {
    try (final Statement statement = connection.createStatement()) {
      statement.execute("CREATE DOCUMENT TYPE Types6447 IF NOT EXISTS");
      statement.execute("CREATE PROPERTY Types6447.id IF NOT EXISTS INTEGER");
      statement.execute("CREATE PROPERTY Types6447.amount IF NOT EXISTS DECIMAL");
      statement.execute("CREATE PROPERTY Types6447.aShort IF NOT EXISTS SHORT");
      statement.execute("CREATE PROPERTY Types6447.aByte IF NOT EXISTS BYTE");
      statement.execute("CREATE PROPERTY Types6447.whenHappened IF NOT EXISTS DATETIME");
    }
  }

  private Connection openJdbcConnection() throws Exception {
    return openJdbcConnection(false);
  }

  private Connection openJdbcConnection(final boolean simpleQueryProtocol) throws Exception {
    Class.forName("org.postgresql.Driver");
    final Properties properties = new Properties();
    properties.setProperty("user", "root");
    properties.setProperty("password", DEFAULT_PASSWORD_FOR_TESTS);
    properties.setProperty("ssl", "false");
    properties.setProperty("sslMode", "disable");
    if (simpleQueryProtocol)
      properties.setProperty("preferQueryMode", "simple");
    return DriverManager.getConnection("jdbc:postgresql://localhost:5432/" + getDatabaseName(), properties);
  }
}
