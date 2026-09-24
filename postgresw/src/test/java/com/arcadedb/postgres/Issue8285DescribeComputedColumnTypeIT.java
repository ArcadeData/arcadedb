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
  void aModifierAppliedToAnAggregateIsNotDescribedAsTheAggregatesOwnType() throws Exception {
    // count(*) is int8, but .asString() runs AFTER it and turns the result into a String - inferring from the
    // un-modified count() alone would describe the modifier's input, not its actual output (review of #8285).
    try (final Connection connection = openJdbcConnection()) {
      createAndPopulateTestType(connection);

      try (final PreparedStatement statement = connection.prepareStatement(
          "SELECT count(*).asString() AS c FROM Items8285")) {
        assertThat(statement.getMetaData().getColumnTypeName(1)).isNotEqualTo("int8");

        try (final ResultSet resultSet = statement.executeQuery()) {
          assertThat(resultSet.next()).isTrue();
          assertThat(resultSet.getObject(1)).isInstanceOf(String.class);
        }
      }
    }
  }

  @Test
  void nonExactDivisionIsNotDescribedAsAnIntegerType() throws Exception {
    // MathExpression.Operator.SLASH returns a Double whenever the division isn't exact - which value ends up in
    // the row depends on n's value, not on n's declared type, so this can never be decided before execution
    // (review of #8285). A statement Describe pins the contract later Executes of it honor (#6725), so once this
    // is correctly left undecided (varchar), the value round-trips as text rather than a native binary type -
    // safe, since text always decodes correctly, unlike a wrong binary int8 would have.
    try (final Connection connection = openJdbcConnection()) {
      createAndPopulateTestType(connection);

      try (final PreparedStatement statement = connection.prepareStatement(
          "SELECT n / 2 AS d FROM Items8285 WHERE n = 1")) {
        assertThat(statement.getMetaData().getColumnTypeName(1)).isNotEqualTo("int8");

        try (final ResultSet resultSet = statement.executeQuery()) {
          assertThat(resultSet.next()).isTrue();
          assertThat(resultSet.getDouble(1)).isEqualTo(0.5);
        }
      }
    }
  }

  @Test
  void sumOfTwoIntegerPropertiesThatOverflowsDescribesAsInt8NotInt4BeforeExecution() throws Exception {
    // MathExpression.Operator.PLUS.apply(Integer, Integer) silently widens to a Long on overflow (no exception,
    // unlike the Long,Long overload), so an INTEGER + INTEGER whose sum does not fit in int4 executes to a Long
    // regardless of the declared operand types - the same "depends on the row's values" problem SLASH has, so
    // describing this as int4 would make a binary-format client decode the real Long as a truncated int4
    // (review of #8285).
    try (final Connection connection = openJdbcConnection()) {
      try (final Statement statement = connection.createStatement()) {
        statement.execute("CREATE DOCUMENT TYPE Items8285Overflow IF NOT EXISTS");
        statement.execute("CREATE PROPERTY Items8285Overflow.a IF NOT EXISTS INTEGER");
        statement.execute("CREATE PROPERTY Items8285Overflow.b IF NOT EXISTS INTEGER");
        statement.execute("INSERT INTO Items8285Overflow SET a = " + Integer.MAX_VALUE + ", b = " + Integer.MAX_VALUE);
      }

      try (final PreparedStatement statement = connection.prepareStatement("SELECT a + b AS s FROM Items8285Overflow")) {
        assertThat(statement.getMetaData().getColumnTypeName(1)).isEqualTo("int8");

        try (final ResultSet resultSet = statement.executeQuery()) {
          assertThat(resultSet.next()).isTrue();
          assertThat(resultSet.getLong(1)).isEqualTo(2L * Integer.MAX_VALUE);
        }
      }
    }
  }

  @Test
  void sumOfADecimalPropertyDescribesAsNumericNotFloat8BeforeExecution() throws Exception {
    // SQLFunctionSum/Type#increment keep a DECIMAL accumulator as BigDecimal: describing it as float8 would make
    // binary encoding call doubleValue() and lose precision (review of #8285).
    try (final Connection connection = openJdbcConnection()) {
      try (final Statement statement = connection.createStatement()) {
        statement.execute("CREATE DOCUMENT TYPE Items8285Decimal IF NOT EXISTS");
        statement.execute("CREATE PROPERTY Items8285Decimal.amount IF NOT EXISTS DECIMAL");
        statement.execute("INSERT INTO Items8285Decimal SET amount = 1.1");
        statement.execute("INSERT INTO Items8285Decimal SET amount = 2.2");
      }

      try (final PreparedStatement statement = connection.prepareStatement(
          "SELECT sum(amount) AS s FROM Items8285Decimal")) {
        assertThat(statement.getMetaData().getColumnTypeName(1)).isEqualTo("numeric");

        try (final ResultSet resultSet = statement.executeQuery()) {
          assertThat(resultSet.next()).isTrue();
          assertThat(resultSet.getObject(1)).isInstanceOf(java.math.BigDecimal.class);
        }
      }
    }
  }

  @Test
  void maxOfADecimalPropertyDescribesAsNumericBeforeExecution() throws Exception {
    // min()/max() pass the operand's own type through unchanged (unlike sum(), which widens): symmetry check for
    // the sum(DECIMAL) case above, since a NUMERIC-typed min()/max() had no coverage (review of #8285).
    try (final Connection connection = openJdbcConnection()) {
      try (final Statement statement = connection.createStatement()) {
        statement.execute("CREATE DOCUMENT TYPE Items8285DecimalMinMax IF NOT EXISTS");
        statement.execute("CREATE PROPERTY Items8285DecimalMinMax.amount IF NOT EXISTS DECIMAL");
        statement.execute("INSERT INTO Items8285DecimalMinMax SET amount = 1.1");
        statement.execute("INSERT INTO Items8285DecimalMinMax SET amount = 2.2");
      }

      try (final PreparedStatement statement = connection.prepareStatement(
          "SELECT max(amount) AS m FROM Items8285DecimalMinMax")) {
        assertThat(statement.getMetaData().getColumnTypeName(1)).isEqualTo("numeric");

        try (final ResultSet resultSet = statement.executeQuery()) {
          assertThat(resultSet.next()).isTrue();
          assertThat(resultSet.getObject(1)).isInstanceOf(java.math.BigDecimal.class);
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
