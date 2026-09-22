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
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8090 end to end, over the wire the client that reported it speaks.
 * <p>
 * The engine tests pin the conversion layer directly; this pins the route the report actually took. A BOUND parameter
 * never reached the broken path - {@code PostgresType.parseTimestampText} rewrites the wire's space separator into
 * the ISO 'T' before handing the text on - so the value that vanished was a literal embedded in the statement text,
 * which reaches {@code Type.convert} exactly as the user typed it. Both routes are driven here, and they have to
 * agree.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8090SqlTimestampLiteralIT extends PostgresWireProtocolTestBase {

  private static final String LITERAL  = "2024-02-29 13:45:10.123456";
  private static final Timestamp EXPECTED = Timestamp.valueOf(LocalDateTime.of(2024, 2, 29, 13, 45, 10, 123_456_000));

  @Test
  void aSqlTimestampLiteralInTheStatementTextIsStoredRatherThanEmptied() throws Exception {
    try (final Connection connection = openJdbcConnection()) {
      createType(connection);

      try (final Statement statement = connection.createStatement()) {
        statement.execute("INSERT INTO Ts8090 SET id = 1, ts = '" + LITERAL + "'");
      }

      assertThat(readTimestamp(connection, 1))//
          .as("the INSERT reported success, so the column cannot be empty")//
          .isEqualTo(EXPECTED);
    }
  }

  @Test
  void theLiteralAndTheBoundParameterAgree() throws Exception {
    try (final Connection connection = openJdbcConnection()) {
      createType(connection);

      try (final Statement statement = connection.createStatement()) {
        statement.execute("INSERT INTO Ts8090 SET id = 2, ts = '" + LITERAL + "'");
      }

      // The bound route normalises the separator before the engine sees it, so it was never broken. It is here to
      // pin that the repaired literal route answers the SAME instant rather than merely a non-null one.
      try (final PreparedStatement insert = connection.prepareStatement("INSERT INTO Ts8090 SET id = 3, ts = ?")) {
        insert.setTimestamp(1, EXPECTED);
        insert.execute();
      }

      assertThat(readTimestamp(connection, 2)).isEqualTo(readTimestamp(connection, 3));
    }
  }

  @Test
  void aLiteralThatCannotBeReadFailsTheInsertRatherThanStoringNull() throws Exception {
    try (final Connection connection = openJdbcConnection()) {
      createType(connection);

      try (final Statement statement = connection.createStatement()) {
        assertThatThrownBy(() -> statement.execute("INSERT INTO Ts8090 SET id = 4, ts = 'not-a-timestamp'"))//
            .isInstanceOf(SQLException.class);
      }

      // ...and nothing was written under that id: the refusal took the whole record, not just the column.
      try (final PreparedStatement select = connection.prepareStatement("SELECT id FROM Ts8090 WHERE id = 4");
          final ResultSet resultSet = select.executeQuery()) {
        assertThat(resultSet.next()).isFalse();
      }
    }
  }

  private Timestamp readTimestamp(final Connection connection, final int id) throws Exception {
    try (final PreparedStatement select = connection.prepareStatement("SELECT ts FROM Ts8090 WHERE id = " + id);
        final ResultSet resultSet = select.executeQuery()) {
      assertThat(resultSet.next()).as("no record with id %d", id).isTrue();
      return resultSet.getTimestamp("ts");
    }
  }

  private void createType(final Connection connection) throws Exception {
    try (final Statement statement = connection.createStatement()) {
      statement.execute("CREATE DOCUMENT TYPE Ts8090 IF NOT EXISTS");
      statement.execute("CREATE PROPERTY Ts8090.id IF NOT EXISTS INTEGER");
      statement.execute("CREATE PROPERTY Ts8090.ts IF NOT EXISTS DATETIME_MICROS");
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
