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
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;
import org.postgresql.util.PGInterval;
import org.postgresql.util.PGobject;

import java.sql.Array;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * How values of each type travel over the PostgreSQL wire protocol, in both directions: {@code bytea}, timestamps
 * written as SQL literals and bound as parameters, arrays in the binary format and lists of mixed element types,
 * string literals under {@code standard_conforming_strings}, the {@code name} type, and a wide row read in one pass.
 */
class PostgresTypeEncodingIT extends PostgresWireProtocolTestBase {

  /*
   * Regression tests for issue #6411: a BINARY property was announced as {@code varchar} when the OID came
   * from the schema (empty result set) and as {@code "char"[]} when it came from a sampled row, so a client
   * that prepared against an empty result and then re-executed got two different types for one column. Both
   * answers were wrong anyway - PostgreSQL's type for a blob is {@code bytea} (OID 17), and decoding arbitrary
   * bytes as text loses data for any non-UTF-8 payload.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  /** Every byte value, so that anything the text encoding mangles shows up. */
  private static final byte[] PAYLOAD = allByteValues();

  // Regression: #6411
  @Test
  void aBinaryPropertyRoundTripsThroughSetBytesAndGetBytes() throws Exception {
    try (final Connection connection = openJdbcConnection()) {
      createBlobType(connection);

      try (final PreparedStatement insert = connection.prepareStatement("INSERT INTO Blob6411 SET id = 1, payload = ?")) {
        insert.setBytes(1, PAYLOAD);
        insert.execute();
      }

      try (final PreparedStatement select = connection.prepareStatement("SELECT payload FROM Blob6411 WHERE id = 1");
          final ResultSet resultSet = select.executeQuery()) {
        assertThat(resultSet.next()).isTrue();
        assertThat(resultSet.getBytes("payload")).isEqualTo(PAYLOAD);
      }
    }
  }

  // Regression: #6411
  @Test
  void theColumnIsAnnouncedAsByteaWhetherOrNotARowWasSampled() throws Exception {
    try (final Connection connection = openJdbcConnection()) {
      createBlobType(connection);

      try (final PreparedStatement insert = connection.prepareStatement("INSERT INTO Blob6411 SET id = 1, payload = ?")) {
        insert.setBytes(1, PAYLOAD);
        insert.execute();
      }

      final String typeFromRow;
      try (final PreparedStatement select = connection.prepareStatement("SELECT payload FROM Blob6411 WHERE id = 1");
          final ResultSet resultSet = select.executeQuery()) {
        assertThat(resultSet.next()).isTrue();
        typeFromRow = resultSet.getMetaData().getColumnTypeName(1);
      }

      final String typeFromSchema;
      try (final PreparedStatement select = connection.prepareStatement("SELECT payload FROM Blob6411 WHERE id = 2");
          final ResultSet resultSet = select.executeQuery()) {
        assertThat(resultSet.next()).isFalse();
        final ResultSetMetaData metaData = resultSet.getMetaData();
        typeFromSchema = metaData.getColumnTypeName(1);
      }

      assertThat(typeFromRow).isEqualTo("bytea");
      assertThat(typeFromSchema)
          .as("the column's OID must not depend on whether the result set happens to be empty")
          .isEqualTo(typeFromRow);
    }
  }

  // Regression: #6411
  @Test
  void anEmptyBlobRoundTrips() throws Exception {
    try (final Connection connection = openJdbcConnection()) {
      createBlobType(connection);

      try (final PreparedStatement insert = connection.prepareStatement("INSERT INTO Blob6411 SET id = 3, payload = ?")) {
        insert.setBytes(1, new byte[0]);
        insert.execute();
      }

      try (final PreparedStatement select = connection.prepareStatement("SELECT payload FROM Blob6411 WHERE id = 3");
          final ResultSet resultSet = select.executeQuery()) {
        assertThat(resultSet.next()).isTrue();
        assertThat(resultSet.getBytes("payload")).isEmpty();
      }
    }
  }

  private void createBlobType(final Connection connection) throws Exception {
    try (final Statement statement = connection.createStatement()) {
      statement.execute("CREATE DOCUMENT TYPE Blob6411 IF NOT EXISTS");
      statement.execute("CREATE PROPERTY Blob6411.id IF NOT EXISTS INTEGER");
      statement.execute("CREATE PROPERTY Blob6411.payload IF NOT EXISTS BINARY");
    }
  }

  private static byte[] allByteValues() {
    final byte[] bytes = new byte[256];
    for (int i = 0; i < bytes.length; i++)
      bytes[i] = (byte) i;
    return bytes;
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

  /*
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
  private static final String LITERAL  = "2024-02-29 13:45:10.123456";
  private static final Timestamp EXPECTED = Timestamp.valueOf(LocalDateTime.of(2024, 2, 29, 13, 45, 10, 123_456_000));
  /** The instant the literal names, as the engine reads a text with no offset: a UTC wall clock. */
  private static final Instant LITERAL_INSTANT = LocalDateTime.of(2024, 2, 29, 13, 45, 10, 123_456_000).toInstant(ZoneOffset.UTC);

  // Regression: #8090
  @Test
  void aSqlTimestampLiteralInTheStatementTextIsStoredRatherThanEmptied() throws Exception {
    try (final Connection connection = openJdbcConnection()) {
      createTimestampType(connection);

      try (final Statement statement = connection.createStatement()) {
        statement.execute("INSERT INTO Ts8090 SET id = 1, ts = '" + LITERAL + "'");
      }

      assertThat(readTimestamp(connection, 1))//
          .as("the INSERT reported success, so the column cannot be empty")//
          .isEqualTo(EXPECTED);
    }
  }

  // Regression: #8090
  @Test
  void theLiteralAndTheBoundParameterAgree() throws Exception {
    try (final Connection connection = openJdbcConnection()) {
      createTimestampType(connection);

      try (final Statement statement = connection.createStatement()) {
        statement.execute("INSERT INTO Ts8090 SET id = 2, ts = '" + LITERAL + "'");
      }

      // The bound route normalises the separator before the engine sees it, so it was never broken. It is here to
      // pin that the repaired literal route answers the SAME instant rather than merely a non-null one. The literal has no offset,
      // so it is the UTC wall clock; the parameter is bound as the same instant in UTC, whatever the zone of the JVM running this
      // (the driver writes the offset of the calendar it is given, and the engine stores the UTC wall clock of an offset, #10489)
      try (final PreparedStatement insert = connection.prepareStatement("INSERT INTO Ts8090 SET id = 3, ts = ?")) {
        insert.setTimestamp(1, Timestamp.from(LITERAL_INSTANT), Calendar.getInstance(TimeZone.getTimeZone(ZoneOffset.UTC)));
        insert.execute();
      }

      assertThat(readTimestamp(connection, 2)).isEqualTo(readTimestamp(connection, 3));
    }
  }

  // Regression: #8090
  @Test
  void aLiteralThatCannotBeReadFailsTheInsertRatherThanStoringNull() throws Exception {
    try (final Connection connection = openJdbcConnection()) {
      createTimestampType(connection);

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

  private void createTimestampType(final Connection connection) throws Exception {
    try (final Statement statement = connection.createStatement()) {
      statement.execute("CREATE DOCUMENT TYPE Ts8090 IF NOT EXISTS");
      statement.execute("CREATE PROPERTY Ts8090.id IF NOT EXISTS INTEGER");
      statement.execute("CREATE PROPERTY Ts8090.ts IF NOT EXISTS DATETIME_MICROS");
    }
  }

  /*
   * Regression test for issue #9007: pgjdbc asks for the binary format of an array column once a PreparedStatement is prepared on
   * the server (the 5th execution by default), and the plugin answered with the text literal, so every execution from the 6th on
   * failed with BufferUnderflowException.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  // Regression: #9007
  @Test
  void aListColumnSurvivesServerPreparedExecutions() throws Exception {
    try (final Connection connection = connectWithProperties(new Properties())) {
      createArrayType();
      try (final PreparedStatement ps = connection.prepareStatement("SELECT lst, txt FROM T9007 WHERE id = ?")) {
        for (int execution = 1; execution <= 8; execution++) {
          ps.setInt(1, 1);
          try (final ResultSet rs = ps.executeQuery()) {
            assertThat(rs.next()).isTrue();
            assertThat((Object[]) rs.getArray(1).getArray()).as("execution %d", execution).containsExactly(1, 2, 3);
            final Array text = rs.getArray(2);
            assertThat((Object[]) text.getArray()).as("execution %d", execution).containsExactly("a", "b\"c", "d,e");
          }
        }
      }
    }
  }

  // Regression: #9007
  @Test
  void binaryResultsFromTheFirstExecution() throws Exception {
    final Properties properties = new Properties();
    properties.setProperty("prepareThreshold", "-1");
    try (final Connection connection = connectWithProperties(properties)) {
      createArrayType();
      try (final PreparedStatement ps = connection.prepareStatement("SELECT lst FROM T9007 WHERE id = ?")) {
        ps.setInt(1, 1);
        try (final ResultSet rs = ps.executeQuery()) {
          assertThat(rs.next()).isTrue();
          assertThat((Object[]) rs.getArray(1).getArray()).containsExactly(1, 2, 3);
        }
      }
    }
  }

  private Connection connectWithProperties(final Properties properties) throws Exception {
    properties.setProperty("user", "root");
    properties.setProperty("password", DEFAULT_PASSWORD_FOR_TESTS);
    properties.setProperty("sslmode", "disable");
    return DriverManager.getConnection(getServerPostgresJdbcUrl(), properties);
  }

  private void createArrayType() {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.transaction(() -> {
      final DocumentType type = database.getSchema().getOrCreateDocumentType("T9007");
      type.getOrCreateProperty("id", Type.INTEGER);
      type.getOrCreateProperty("lst", Type.LIST);
      type.getOrCreateProperty("txt", Type.LIST);
      database.newDocument("T9007").set("id", 1).set("lst", List.of(1, 2, 3)).set("txt", List.of("a", "b\"c", "d,e")).save();
    });
  }

  /*
   * Issues #9008 and #9009: a value must never be announced under a type that cannot hold it.
   * <ul>
   *   <li>#9008: a LIST column was typed from its first element, so {@code [1, 2.5]} was announced as {@code int4[]}
   *   and read back as {@code [1, 2]}, and {@code [1, 3000000000]} and {@code [1, "a"]} could not be read at all.</li>
   *   <li>#9009: a prepared SELECT on a type with undeclared properties described its columns from one row and then
   *   serialized every row in that row's own layout, so pgjdbc read 3000000000 as 0 or -1294967296, 1.5 as a bit pattern
   *   or 1, and {@code @rid} in the column of another property.</li>
   * </ul>
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  // Regression: #9008, #9009
  @Test
  void aMixedListIsAnnouncedUnderAnArrayTypeThatHoldsEveryElement() throws Exception {
    try (final Connection connection = openJdbcConnection(""); final Statement statement = connection.createStatement()) {
      statement.execute("CREATE DOCUMENT TYPE List9008");
      statement.execute("INSERT INTO List9008 SET id = 1, lst = [1, 2.5]");
      statement.execute("INSERT INTO List9008 SET id = 2, lst = [1, 3000000000]");
      statement.execute("INSERT INTO List9008 SET id = 3, lst = [1, 'a']");
      statement.execute("INSERT INTO List9008 SET id = 4, lst = [1, 2, 3]");

      assertThat(listOf(statement, 1)).containsExactly("1.0", "2.5");
      assertThat(listOf(statement, 2)).containsExactly("1", "3000000000");
      assertThat(listOf(statement, 3)).containsExactly("1", "a");
      assertThat(listOf(statement, 4)).containsExactly("1", "2", "3");

      try (final ResultSet resultSet = statement.executeQuery("SELECT lst FROM List9008 WHERE id = 4")) {
        resultSet.next();
        assertThat(resultSet.getMetaData().getColumnTypeName(1)).as("a list of integers keeps the narrow type").isEqualTo("_int4");
      }
    }
  }

  // Regression: #9008, #9009
  @Test
  void aNumericListRoundTripsOverTheWire() throws Exception {
    // default settings only: an array column described before its first execution is read as binary by pgjdbc on
    // the later executions whatever the format announced, an array limitation that predates this change
    for (final String options : new String[] { "" }) {
      try (final Connection connection = openJdbcConnection(options)) {
        final Database database = getServerDatabase(0, getDatabaseName());
        database.transaction(() -> {
          if (!database.getSchema().existsType("Numeric9008")) {
            database.getSchema().createDocumentType("Numeric9008");
            database.newDocument("Numeric9008").set("id", 1)
                .set("lst", java.util.List.of(new java.math.BigDecimal("1.25"), 2, new java.math.BigDecimal("10000000000"))).save();
          }
        });
        try (final PreparedStatement select = connection.prepareStatement("SELECT lst FROM Numeric9008 WHERE id = ?")) {
          for (int i = 0; i < 3; i++) {
            select.setInt(1, 1);
            try (final ResultSet resultSet = select.executeQuery()) {
              assertThat(resultSet.next()).isTrue();
              assertThat(resultSet.getMetaData().getColumnTypeName(1)).isIn("_numeric", "_text");
              final String text = resultSet.getString(1);
              assertThat(text).as("options=" + options).isNotNull();
              final String[] values = text.replaceAll("[{}\"]", "").split(",");
              assertThat(values).hasSize(3);
              assertThat(new java.math.BigDecimal(values[0]).compareTo(new java.math.BigDecimal("1.25"))).isZero();
              assertThat(new java.math.BigDecimal(values[2]).compareTo(new java.math.BigDecimal("10000000000"))).isZero();
            }
          }
        }
      }
    }
  }

  // Regression: #9008, #9009
  @Test
  void aPreparedStatementKeepsOneLayoutForRowsOfDifferentTypes() throws Exception {
    // default pgjdbc settings: executions 1-4 are unnamed, the 5th is described and prepared on the server, 6+ reuse it
    verifyRows("", new int[] { 1, 1, 1, 1, 1, 2, 3, 1 });
  }

  // Regression: #9008, #9009
  @Test
  void aStatementDescribedBeforeItsFirstExecutionKeepsOneLayoutToo() throws Exception {
    verifyRows("prepareThreshold=-1", new int[] { 1, 2, 3 });
  }

  // Regression: #9008, #9009
  @Test
  void aProjectionOfAnUndeclaredPropertyKeepsOneLayoutToo() throws Exception {
    for (final String options : new String[] { "", "prepareThreshold=-1" }) {
      try (final Connection connection = openJdbcConnection(options)) {
        final Database database = getServerDatabase(0, getDatabaseName());
        database.transaction(() -> {
          if (!database.getSchema().existsType("Projected9009")) {
            database.getSchema().createDocumentType("Projected9009").createProperty("id", Type.INTEGER);
            database.newDocument("Projected9009").set("id", 1).set("u", 1).save();
            database.newDocument("Projected9009").set("id", 2).set("u", 3000000000L).save();
            database.newDocument("Projected9009").set("id", 3).set("u", 1.5).save();
          }
        });
        final int[] ids = options.isEmpty() ? new int[] { 1, 1, 1, 1, 1, 2, 3, 1 } : new int[] { 1, 2, 3 };
        try (final PreparedStatement select = connection.prepareStatement("SELECT u FROM Projected9009 WHERE id = ?")) {
          for (final int id : ids) {
            select.setInt(1, id);
            try (final ResultSet resultSet = select.executeQuery()) {
              assertThat(resultSet.next()).isTrue();
              assertThat(resultSet.getString("u")).as("u of id " + id + " with " + options)
                  .isEqualTo(switch (id) { case 1 -> "1"; case 2 -> "3000000000"; default -> "1.5"; });
            }
          }
        }
      }
    }
  }

  private void verifyRows(final String options, final int[] ids) throws Exception {
    try (final Connection connection = openJdbcConnection(options); final Statement statement = connection.createStatement()) {
      final Database database = getServerDatabase(0, getDatabaseName());
      database.transaction(() -> {
        database.getSchema().createDocumentType("Schemaless9009").createProperty("id", Type.INTEGER);
        database.newDocument("Schemaless9009").set("id", 1).set("u", 1).set("a", "x").save();
        database.newDocument("Schemaless9009").set("id", 2).set("u", 3000000000L).save();
        database.newDocument("Schemaless9009").set("id", 3).set("u", 1.5).save();
      });

      try (final PreparedStatement select = connection.prepareStatement("SELECT FROM Schemaless9009 WHERE id = ?")) {
        for (final int id : ids) {
          select.setInt(1, id);
          try (final ResultSet resultSet = select.executeQuery()) {
            assertThat(resultSet.next()).isTrue();
            assertThat(resultSet.getInt("id")).isEqualTo(id);
            assertThat(resultSet.getMetaData().getColumnTypeName(1)).as("a declared property keeps its native type").isEqualTo("int4");
            final String expectedU = switch (id) {
              case 1 -> "1";
              case 2 -> "3000000000";
              default -> "1.5";
            };
            assertThat(resultSet.getString("u")).as("u of id " + id).isEqualTo(expectedU);
            assertThat(resultSet.getString("a")).as("a of id " + id).isEqualTo(id == 1 ? "x" : null);
            assertThat(resultSet.getString("@rid")).as("@rid of id " + id).startsWith("#");
            assertThat(resultSet.getString("@type")).isEqualTo("Schemaless9009");
            assertThat(resultSet.getString("@cat")).isEqualTo("d");
          }
        }
      }
    }
  }

  private static String[] listOf(final Statement statement, final int id) throws Exception {
    try (final ResultSet resultSet = statement.executeQuery("SELECT lst FROM List9008 WHERE id = " + id)) {
      resultSet.next();
      final Object[] values = (Object[]) resultSet.getArray(1).getArray();
      return Arrays.stream(values).map(String::valueOf).toArray(String[]::new);
    }
  }

  private Connection openJdbcConnection(final String options) throws Exception {
    Class.forName("org.postgresql.Driver");
    final Properties properties = new Properties();
    properties.setProperty("user", "root");
    properties.setProperty("password", DEFAULT_PASSWORD_FOR_TESTS);
    properties.setProperty("ssl", "false");
    properties.setProperty("sslMode", "disable");
    return DriverManager.getConnection(getServerPostgresJdbcUrl() + (options.isEmpty() ? "" : "?" + options), properties);
  }

  /*
   * Issue #10871: every connection is told {@code standard_conforming_strings = on}, which is a client's licence to put a
   * backslash in a plain literal unescaped. The literal was then escape-processed by the engine lexer: {@code 'a\b'} was
   * stored as {@code ab}, {@code 'C:\'} was a syntax error, {@code E'...'} did not parse, and a {@code SET} of the
   * parameter to {@code off} was accepted and changed nothing.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  private static final String[] VALUES = { "a\\b", "C:\\", "C:\\tmp\\x", "it's", "x\\ty", "line\nbreak", "\\'" };

  private Connection connect(final String queryMode) throws SQLException {
    return connect(queryMode, null);
  }

  private Connection connect(final String queryMode, final String options) throws SQLException {
    final Properties props = new Properties();
    props.setProperty("user", "root");
    props.setProperty("password", DEFAULT_PASSWORD_FOR_TESTS);
    props.setProperty("ssl", "false");
    if (queryMode != null)
      props.setProperty("preferQueryMode", queryMode);
    if (options != null)
      props.setProperty("options", options);
    return DriverManager.getConnection(getServerPostgresJdbcUrl(), props);
  }

  // Regression: #10871
  @Test
  void theConnectionCanStartWithTheParameterOff() throws Exception {
    try (final Connection conn = connect(null, "-c standard_conforming_strings=off"); final Statement st = conn.createStatement()) {
      assertThat(show(st)).isEqualTo("off");
      assertThat(selectOne(st, "SELECT 'x\\\\ty' AS v")).isEqualTo("x\\ty");
      assertThat(selectOne(st, "SELECT 'x\\ty' AS v")).isEqualTo("x\ty");
    }
  }

  // Regression: #10871
  @Test
  void aPlainLiteralKeepsItsBackslashes() throws Exception {
    try (final Connection conn = connect(null); final Statement st = conn.createStatement()) {
      assertThat(show(st)).isEqualTo("on");
      assertThat(selectOne(st, "SELECT 'a\\b' AS v")).isEqualTo("a\\b");
      assertThat(selectOne(st, "SELECT 'x\\ty' AS v")).isEqualTo("x\\ty");
      assertThat(selectOne(st, "SELECT 'C:\\' AS v")).isEqualTo("C:\\");
      assertThat(selectOne(st, "SELECT 'C:\\tmp\\x' AS v")).isEqualTo("C:\\tmp\\x");
      assertThat(selectOne(st, "SELECT 'it''s' AS v")).isEqualTo("it's");
      // the literal after one that ends in a backslash is a literal of its own
      try (final ResultSet rs = st.executeQuery("SELECT 'a\\' AS v, 'b' AS w")) {
        assertThat(rs.next()).isTrue();
        assertThat(rs.getString("v")).isEqualTo("a\\");
        assertThat(rs.getString("w")).isEqualTo("b");
      }
    }
  }

  // Regression: #10871
  @Test
  void anEscapeStringProcessesPostgresEscapes() throws Exception {
    try (final Connection conn = connect(null); final Statement st = conn.createStatement()) {
      assertThat(selectOne(st, "SELECT E'x\\ty' AS v")).isEqualTo("x\ty");
      assertThat(selectOne(st, "SELECT E'a\\\\b' AS v")).isEqualTo("a\\b");
      assertThat(selectOne(st, "SELECT E'it\\'s' AS v")).isEqualTo("it's");
      assertThat(selectOne(st, "SELECT E'a\\bc' AS v")).isEqualTo("a\bc");
    }
  }

  // Regression: #10871
  @Test
  void aSetOfTheParameterIsHonoured() throws Exception {
    try (final Connection conn = connect(null); final Statement st = conn.createStatement()) {
      st.execute("SET standard_conforming_strings = off");
      assertThat(show(st)).isEqualTo("off");
      assertThat(selectOne(st, "SELECT 'a\\\\b' AS v")).isEqualTo("a\\b");
      assertThat(selectOne(st, "SELECT 'x\\ty' AS v")).isEqualTo("x\ty");

      st.execute("SET standard_conforming_strings = on");
      assertThat(show(st)).isEqualTo("on");
      assertThat(selectOne(st, "SELECT 'a\\b' AS v")).isEqualTo("a\\b");
    }
  }

  // Regression: #10871
  @Test
  void everyQueryModeStoresTheBoundValueAsItIs() throws Exception {
    try (final Connection conn = connect(null); final Statement st = conn.createStatement()) {
      st.execute("CREATE DOCUMENT TYPE Bs");
    }

    // simple: the driver composes the literal itself, following the reported setting; extended: the value is bound
    for (final String mode : new String[] { "simple", "extended" })
      for (final boolean standardConformingStrings : new boolean[] { true, false })
        try (final Connection conn = connect(mode)) {
          if (!standardConformingStrings)
            try (final Statement st = conn.createStatement()) {
              st.execute("SET standard_conforming_strings = off");
            }
          try (final Statement st = conn.createStatement()) {
            st.execute("DELETE FROM Bs");
          }
          for (final String value : VALUES)
            try (final PreparedStatement ps = conn.prepareStatement("INSERT INTO Bs (v) VALUES (?)")) {
              ps.setString(1, value);
              ps.execute();
            }
          final List<String> stored = new ArrayList<>();
          try (final Statement st = conn.createStatement(); final ResultSet rs = st.executeQuery("SELECT v FROM Bs")) {
            while (rs.next())
              stored.add(rs.getString("v"));
          }
          assertThat(stored).as("preferQueryMode=%s, standard_conforming_strings=%s", mode, standardConformingStrings)
              .containsExactlyInAnyOrder(VALUES);

          for (final String value : VALUES)
            try (final PreparedStatement ps = conn.prepareStatement("SELECT v FROM Bs WHERE v = ?")) {
              ps.setString(1, value);
              try (final ResultSet rs = ps.executeQuery()) {
                assertThat(rs.next()).as("lookup of %s in mode %s", value, mode).isTrue();
                assertThat(rs.getString("v")).isEqualTo(value);
              }
            }
        }
  }

  private static String show(final Statement st) throws SQLException {
    try (final ResultSet rs = st.executeQuery("SHOW standard_conforming_strings")) {
      assertThat(rs.next()).isTrue();
      return rs.getString(1);
    }
  }

  private static String selectOne(final Statement st, final String sql) throws SQLException {
    try (final ResultSet rs = st.executeQuery(sql)) {
      assertThat(rs.next()).as(sql).isTrue();
      return rs.getString("v");
    }
  }

  /*
   * Issue #11027 through the PostgreSQL JDBC driver: {@code pg_type} had no row for {@code name} (OID 19), which pgjdbc reads the
   * typlen of to answer {@code getMax*NameLength()} and {@code getClientInfoProperties()}, so each of them threw.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  // Regression: #11027
  @Test
  void everyMaxNameLengthAccessorAnswersPostgreSqlsValue() throws Exception {
    try (final Connection connection = open()) {
      final DatabaseMetaData meta = connection.getMetaData();
      assertThat(meta.getMaxColumnNameLength()).isEqualTo(63);
      assertThat(meta.getMaxTableNameLength()).isEqualTo(63);
      assertThat(meta.getMaxSchemaNameLength()).isEqualTo(63);
      assertThat(meta.getMaxCatalogNameLength()).isEqualTo(63);
      assertThat(meta.getMaxCursorNameLength()).isEqualTo(63);
      assertThat(meta.getMaxUserNameLength()).isEqualTo(63);
      assertThat(meta.getMaxProcedureNameLength()).isEqualTo(63);
    }
  }

  // Regression: #11027
  @Test
  void getClientInfoPropertiesAnswersItsRows() throws Exception {
    try (final Connection connection = open();
        final ResultSet rows = connection.getMetaData().getClientInfoProperties()) {
      // the rows pgjdbc builds depend on the server version it is told; what matters is that it no longer throws
      assertThat(rows.next()).isTrue();
      assertThat(rows.getString("NAME")).isEqualTo("ApplicationName");
    }
  }

  // Regression: #11027
  @Test
  void theTypesAClientLooksUpByNameAreThere() throws Exception {
    try (final Connection connection = open(); final Statement statement = connection.createStatement()) {
      for (final String name : new String[] { "name", "oid", "int2vector" })
        try (final ResultSet rows = statement.executeQuery("SELECT oid, typname FROM pg_catalog.pg_type WHERE typname = '" + name + "'")) {
          assertThat(rows.next()).as(name).isTrue();
          assertThat(rows.getString("typname")).isEqualTo(name);
        }
    }
  }

  private Connection open() throws Exception {
    Class.forName("org.postgresql.Driver");
    final Properties properties = new Properties();
    properties.setProperty("user", "root");
    properties.setProperty("password", DEFAULT_PASSWORD_FOR_TESTS);
    properties.setProperty("ssl", "false");
    properties.setProperty("sslMode", "disable");
    return DriverManager.getConnection(getServerPostgresJdbcUrl(), properties);
  }

  /*
   * Issue #11960: pgjdbc binds an {@code OffsetDateTime} as timestamptz, a {@code LocalTime} as time, an {@code OffsetTime} as timetz,
   * a {@code PGInterval} as interval and a jsonb {@code PGobject} as jsonb. Every one of them failed the Bind with XX000; each is now
   * stored as the engine value of the matching type.
   */
  // Regression: #11960
  @Test
  void driverTemporalAndJsonbParametersAreStored() throws Exception {
    final OffsetDateTime zoned = OffsetDateTime.of(2024, 2, 29, 13, 14, 15, 123_456_000, ZoneOffset.ofHours(2));
    try (final Connection connection = openJdbcConnection()) {
      try (final var statement = connection.createStatement()) {
        statement.execute("CREATE DOCUMENT TYPE W11960 IF NOT EXISTS");
        statement.execute("CREATE PROPERTY W11960.micros IF NOT EXISTS DATETIME_MICROS");
      }
      try (final PreparedStatement insert = connection.prepareStatement(
          "INSERT INTO W11960 SET id = 1, z = ?, micros = ?, t = ?, ttz = ?, d = ?, j = ?")) {
        insert.setObject(1, zoned);
        insert.setObject(2, zoned);
        insert.setObject(3, LocalTime.of(10, 15, 30));
        insert.setObject(4, OffsetTime.of(10, 15, 30, 0, ZoneOffset.ofHours(2)));
        insert.setObject(5, new PGInterval(0, 0, 1, 0, 0, 5));
        final PGobject jsonb = new PGobject();
        jsonb.setType("jsonb");
        jsonb.setValue("{\"a\": 1}");
        insert.setObject(6, jsonb);
        insert.executeUpdate();
      }
    }

    try (final var rs = getServerDatabase(0, getDatabaseName()).query("sql", "SELECT FROM W11960 WHERE id = 1")) {
      final var row = rs.next();
      assertThat(((ZonedDateTime) row.getProperty("z")).toInstant()).isEqualTo(zoned.toInstant());
      // the instant, read into a DATETIME_MICROS property: every microsecond kept
      assertThat(row.<LocalDateTime>getProperty("micros")).isEqualTo(zoned.withOffsetSameInstant(ZoneOffset.UTC).toLocalDateTime());
      assertThat(row.<Object>getProperty("t")).isEqualTo(LocalTime.of(10, 15, 30));
      assertThat(((OffsetTime) row.getProperty("ttz")).toLocalTime()).isEqualTo(LocalTime.of(10, 15, 30));
      assertThat(row.<Object>getProperty("d").toString()).isEqualTo("P1DT5S");
      assertThat(row.<Map<String, Object>>getProperty("j")).containsEntry("a", 1);
    }
  }

  /*
   * A functional check of the one-pass row read over the Postgres wire (#12558): a wide row keeps every column, a null one
   * included, with its value. The header walks themselves are counted by {@code PostgresOnePassRowReadTest}.
   */
  private static final int PROPERTIES = 40;

  // Regression: #12558
  @Test
  void aWideRowIsReadOnceOverTheRecordAndKeepsEveryColumn() throws Exception {
    try (final Connection connection = openJdbcConnection(); final Statement statement = connection.createStatement()) {
      statement.execute("CREATE DOCUMENT TYPE Issue12558Wide IF NOT EXISTS");
      final StringBuilder insert = new StringBuilder("INSERT INTO Issue12558Wide SET nothing = null");
      for (int i = 0; i < PROPERTIES; i++)
        insert.append(", p").append(i).append(" = 'value-").append(i).append('\'');
      statement.execute(insert.toString());

      // the row over the wire
      try (final ResultSet rs = statement.executeQuery("SELECT * FROM Issue12558Wide")) {
        assertThat(rs.next()).isTrue();
        for (int i = 0; i < PROPERTIES; i++)
          assertThat(rs.getString("p" + i)).as("column p%d", i).isEqualTo("value-" + i);
        assertThat(rs.getObject("nothing")).as("a null column").isNull();
      }
    }
  }
}
