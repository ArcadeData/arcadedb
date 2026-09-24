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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;

import static com.arcadedb.postgres.PostgresWireMessages.WireMessage;
import static com.arcadedb.postgres.PostgresWireMessages.firstDataRowValue;
import static com.arcadedb.postgres.PostgresWireMessages.messageTypesOf;
import static com.arcadedb.postgres.PostgresWireMessages.readUntilReadyForQuery;
import static com.arcadedb.postgres.PostgresWireMessages.sendBind;
import static com.arcadedb.postgres.PostgresWireMessages.sendDescribe;
import static com.arcadedb.postgres.PostgresWireMessages.sendExecute;
import static com.arcadedb.postgres.PostgresWireMessages.sendParse;
import static com.arcadedb.postgres.PostgresWireMessages.sendSimpleQuery;
import static com.arcadedb.postgres.PostgresWireMessages.sendSync;
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
  void nullCoalescingIsNotDescribedAsTheOtherOperandsWiderType() throws Exception {
    // MathExpression.Operator.NULL_COALESCING (??) returns whichever operand is non-null UNCHANGED - never
    // widened to a common type - so `n ?? x` (n LONG, x DOUBLE) returns n's own Long when n is non-null, not a
    // Double. Describing it as the wider operand type (float8) would make binary encoding call doubleValue() on
    // that Long and risk losing precision (review of #8285, CodeRabbit).
    //
    // Sent over the raw wire protocol rather than through pgjdbc: a PreparedStatement's client-side parameter
    // parsing treats "??" as an escaped literal "?" and rewrites it away before the query ever reaches the
    // server ("mismatched input '?'"), so this shape cannot be reproduced through pgjdbc's own API.
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      socket.setSoTimeout(30_000); // a stalled response fails the test instead of blocking it indefinitely
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());

      sendStartupMessage(out, "root", getDatabaseName());
      readMessage(in); // AuthenticationCleartextPassword
      sendPasswordMessage(out, DEFAULT_PASSWORD_FOR_TESTS);
      readMessageOfType(in, 'Z'); // drain AuthenticationOk/BackendKeyData/ParameterStatus.../ReadyForQuery

      sendSimpleQuery(out, "CREATE DOCUMENT TYPE Items8285NullCoalescing IF NOT EXISTS");
      readUntilReadyForQuery(in);
      sendSimpleQuery(out, "CREATE PROPERTY Items8285NullCoalescing.n IF NOT EXISTS LONG");
      readUntilReadyForQuery(in);
      sendSimpleQuery(out, "CREATE PROPERTY Items8285NullCoalescing.x IF NOT EXISTS DOUBLE");
      readUntilReadyForQuery(in);
      sendSimpleQuery(out, "INSERT INTO Items8285NullCoalescing SET n = 1, x = 2.5");
      readUntilReadyForQuery(in);

      sendParse(out, "s", "SELECT n ?? x AS c FROM Items8285NullCoalescing WHERE n = 1");
      sendDescribe(out, 'S', "s");
      sendSync(out);
      final List<WireMessage> described = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(described)).as("Parse and Describe both succeed").doesNotContain('E');

      final WireMessage rowDescription = described.stream().filter(m -> m.type() == 'T').findFirst()
          .orElseThrow(() -> new AssertionError("no RowDescription among " + messageTypesOf(described)));
      assertThat(firstColumnTypeOid(rowDescription))
          .as("must not commit to float8 (OID 701), the wider operand's type, before execution")
          .isNotEqualTo(701);

      sendBind(out, "p", "s");
      sendExecute(out, "p");
      sendSync(out);
      final List<WireMessage> executed = readUntilReadyForQuery(in);
      assertThat(firstDataRowValue(executed)).as("n is non-null, so the value is n's own Long, unchanged")
          .isEqualTo("1");
    }
  }

  /**
   * The data type OID of a RowDescription's first (and, for these tests, only) column.
   */
  private static int firstColumnTypeOid(final WireMessage rowDescription) {
    final ByteBuffer buffer = ByteBuffer.wrap(rowDescription.body());
    buffer.getShort(); // field count
    while (buffer.get() != 0) {
      // skip the null-terminated field name
    }
    buffer.getInt(); // table OID
    buffer.getShort(); // column attribute number
    return buffer.getInt(); // data type OID
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
