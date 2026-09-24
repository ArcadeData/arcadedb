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
 * Issue #8285: a client that describes a statement before running it (Parse + {@code Describe('S')}) got every
 * projected column that is not a plain schema property announced as {@code varchar} (OID 1043) - {@code count(*)},
 * {@code sum(x)}, {@code max(n)}, arithmetic - even though the RowDescription of an executed query reports the
 * real type. Because a statement Describe is the contract later Executes of that statement honor (issue #6725),
 * pgjdbc's {@link PreparedStatement#getMetaData()}, called before the statement ever runs, is exactly that
 * pre-execution Describe, and it returned these aggregates and expressions as text for good.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8285DescribeComputedColumnTypeIT extends PostgresWireProtocolTestBase {

  @Test
  void countStarDescribesAsInt8BeforeExecution() throws Exception {
    try (final Connection connection = openJdbcConnection()) {
      createAndPopulateTestType(connection);

      try (final PreparedStatement statement = connection.prepareStatement("SELECT count(*) AS c FROM Items8285")) {
        assertThat(statement.getMetaData().getColumnTypeName(1)).isEqualTo("int8");

        try (final ResultSet resultSet = statement.executeQuery()) {
          assertThat(resultSet.next()).isTrue();
          assertThat(resultSet.getObject(1)).isInstanceOf(Long.class);
        }
      }
    }
  }

  @Test
  void sumOfADoublePropertyDescribesAsFloat8BeforeExecution() throws Exception {
    try (final Connection connection = openJdbcConnection()) {
      createAndPopulateTestType(connection);

      try (final PreparedStatement statement = connection.prepareStatement("SELECT sum(x) AS s FROM Items8285")) {
        assertThat(statement.getMetaData().getColumnTypeName(1)).isEqualTo("float8");

        try (final ResultSet resultSet = statement.executeQuery()) {
          assertThat(resultSet.next()).isTrue();
          assertThat(resultSet.getObject(1)).isInstanceOf(Double.class);
        }
      }
    }
  }

  @Test
  void maxOfALongPropertyDescribesAsInt8BeforeExecution() throws Exception {
    try (final Connection connection = openJdbcConnection()) {
      createAndPopulateTestType(connection);

      try (final PreparedStatement statement = connection.prepareStatement("SELECT max(n) AS m FROM Items8285")) {
        assertThat(statement.getMetaData().getColumnTypeName(1)).isEqualTo("int8");

        try (final ResultSet resultSet = statement.executeQuery()) {
          assertThat(resultSet.next()).isTrue();
          assertThat(resultSet.getObject(1)).isInstanceOf(Long.class);
        }
      }
    }
  }

  @Test
  void arithmeticOverALongPropertyDescribesAsInt8BeforeExecution() throws Exception {
    try (final Connection connection = openJdbcConnection()) {
      createAndPopulateTestType(connection);

      try (final PreparedStatement statement = connection.prepareStatement(
          "SELECT n * 2 AS d FROM Items8285 WHERE n = 1")) {
        assertThat(statement.getMetaData().getColumnTypeName(1)).isEqualTo("int8");

        try (final ResultSet resultSet = statement.executeQuery()) {
          assertThat(resultSet.next()).isTrue();
          assertThat(resultSet.getObject(1)).isInstanceOf(Long.class);
        }
      }
    }
  }

  @Test
  void aPlainDeclaredPropertyIsUnaffected() throws Exception {
    try (final Connection connection = openJdbcConnection()) {
      createAndPopulateTestType(connection);

      try (final PreparedStatement statement = connection.prepareStatement("SELECT n FROM Items8285 WHERE n = 1")) {
        assertThat(statement.getMetaData().getColumnTypeName(1)).isEqualTo("int8");
      }
    }
  }

  private void createAndPopulateTestType(final Connection connection) throws Exception {
    try (final Statement statement = connection.createStatement()) {
      statement.execute("CREATE DOCUMENT TYPE Items8285 IF NOT EXISTS");
      statement.execute("CREATE PROPERTY Items8285.n IF NOT EXISTS LONG");
      statement.execute("CREATE PROPERTY Items8285.x IF NOT EXISTS DOUBLE");
      for (int i = 0; i < 3; i++)
        statement.execute("INSERT INTO Items8285 SET n = " + i + ", x = " + (i * 0.5));
    }
  }

  private Connection openJdbcConnection() throws Exception {
    Class.forName("org.postgresql.Driver");
    final Properties properties = new Properties();
    properties.setProperty("user", "root");
    properties.setProperty("password", DEFAULT_PASSWORD_FOR_TESTS);
    properties.setProperty("ssl", "false");
    properties.setProperty("sslMode", "disable");
    return DriverManager.getConnection(getServerPostgresJdbcUrl(), properties);
  }
}
