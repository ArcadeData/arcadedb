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
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The type a result column is announced with over the PostgreSQL wire protocol (RowDescription), and the values sent for it:
 * whether it comes from the schema or from the rows that happened to come back, through an alias, for a declared column that
 * is NULL on every row, for an excluded projection item, for a graph traversal and for a LIST OF BINARY, on the simple and
 * the extended query protocol alike.
 */
class PostgresColumnTypeResolutionIT extends PostgresWireProtocolTestBase {

  /*
   * Regression tests for issue #6447, the follow-up to #6411 (BYTEA): a column's announced OID depended on
   * whether the result set happened to be empty (typed from the declared schema, {@link PostgresType#getTypeFromArcade})
   * or had a sampled row (typed from the value, {@link PostgresType#getTypeForValue}), for three more types
   * ({@code SHORT}/{@code BYTE}, {@code DATETIME} and {@code DECIMAL}) beyond the one #6411 already fixed.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  // Regression: #6447
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

  // Regression: #6447
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

  // Regression: #6447
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

  // Regression: #6447
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

  // Regression: #6447
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

  // Regression: #6447
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
    return DriverManager.getConnection(getServerPostgresJdbcUrl(), properties);
  }

  /*
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
  // Regression: #6473
  @Test
  void anAliasedEmptyListPropertyReportsTheDeclaredElementTypeInsteadOfDefaultingToText() throws Exception {
    try (final Connection connection = openJdbcConnection()) {
      createAliasTestType(connection);

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

  // Regression: #6473
  @Test
  void anAliasedDatetimePropertyRoundTripsAsTimestampWhenDateTimeImplementationIsJavaUtilDate() throws Exception {
    // Same java.util.Date/DATE ambiguity #6447 disambiguates via the schema (see the #6447 tests above),
    // but through an alias: "SELECT whenHappened AS x" - x is not a schema property, so getDeclaredProperty
    // must resolve it back to "whenHappened" before isDeclaredAsDatetime can even run.
    try (final Connection connection = openJdbcConnection()) {
      try (final Statement configure = connection.createStatement()) {
        configure.execute("ALTER DATABASE dateTimeImplementation `java.util.Date`");
      }
      createAliasTestType(connection);

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

  private void createAliasTestType(final Connection connection) throws Exception {
    try (final Statement statement = connection.createStatement()) {
      statement.execute("CREATE DOCUMENT TYPE Types6473 IF NOT EXISTS");
      statement.execute("CREATE PROPERTY Types6473.id IF NOT EXISTS INTEGER");
      statement.execute("CREATE PROPERTY Types6473.tags IF NOT EXISTS LIST OF INTEGER");
      statement.execute("CREATE PROPERTY Types6473.whenHappened IF NOT EXISTS DATETIME");
    }
  }

  /**
   * Issue #11026: the simple query protocol named the columns of a result from the rows that came back, so a declared
   * property that is NULL on all of them was dropped from a wildcard projection, and one projected explicitly was announced as
   * varchar instead of its declared type, while Describe('S') of the same text answered both from the schema. The announced
   * columns of a statement naming a schema type are a property of the schema, not of the data.
   */
  @Nested
  class DeclaredNullColumns {
    @BeforeEach
    void createData() {
      final Database database = getServerDatabase(0, getDatabaseName());
      database.transaction(() -> {
        final VertexType type = database.getSchema().createVertexType("NullCol26");
        type.createProperty("n", Type.INTEGER);
        type.createProperty("k", Type.STRING);
        type.createProperty("d", Type.DECIMAL);
      });
      database.transaction(() -> database.newVertex("NullCol26").set("k", "a").save());
    }

    // Regression: #11026
    @Test
    void simpleQueryTypesANullDeclaredColumnFromTheSchema() throws Exception {
      final Properties properties = new Properties();
      properties.setProperty("preferQueryMode", "simple");
      try (final Connection connection = connect(properties); final Statement statement = connection.createStatement();
          final ResultSet rs = statement.executeQuery("SELECT n FROM NullCol26")) {
        assertThat(rs.getMetaData().getColumnTypeName(1)).isEqualTo("int4");
        assertThat(rs.next()).isTrue();
        rs.getInt(1);
        assertThat(rs.wasNull()).isTrue();
      }
    }

    // Regression: #11026
    @Test
    void simpleQueryWildcardHoldsEveryDeclaredColumn() throws Exception {
      final Properties properties = new Properties();
      properties.setProperty("preferQueryMode", "simple");
      try (final Connection connection = connect(properties); final Statement statement = connection.createStatement();
          final ResultSet rs = statement.executeQuery("SELECT * FROM NullCol26")) {
        assertWildcardShape(rs);
      }
    }

    // Regression: #11026
    @Test
    void simpleQueryAgreesWithDescribeStatement() throws Exception {
      final Properties simple = new Properties();
      simple.setProperty("preferQueryMode", "simple");
      final Properties prepared = new Properties();
      prepared.setProperty("prepareThreshold", "-1");
      for (final String sql : List.of("SELECT * FROM NullCol26", "SELECT n FROM NullCol26", "SELECT n, k FROM NullCol26")) {
        final List<String> viaSimple = columnsOf(simple, sql);
        final List<String> viaPrepared = columnsOf(prepared, sql);
        assertThat(viaSimple).as(sql).containsExactlyInAnyOrderElementsOf(viaPrepared);
      }
    }

    // Regression: #11026
    @Test
    void describePortalWildcardHoldsEveryDeclaredColumn() throws Exception {
      final Properties properties = new Properties();
      properties.setProperty("prepareThreshold", "0");
      try (final Connection connection = connect(properties); final PreparedStatement ps = connection.prepareStatement(
          "SELECT * FROM NullCol26"); final ResultSet rs = ps.executeQuery()) {
        assertWildcardShape(rs);
      }
    }

    // Regression: #11026
    /** Issue #11164: an aliased bare property is typed from the property it reads, like Describe('S') does. */
    @Test
    void simpleQueryTypesAnAliasedNullDeclaredColumnFromTheSchema() throws Exception {
      final Properties simple = new Properties();
      simple.setProperty("preferQueryMode", "simple");
      assertThat(columnsOf(simple, "SELECT n AS x FROM NullCol26")).containsExactly("x:int4");
      // the alias repeats the name of ANOTHER declared property: the type is the source's, not the namesake's
      assertThat(columnsOf(simple, "SELECT n AS k FROM NullCol26")).containsExactly("k:int4");
      assertThat(columnsOf(simple, "SELECT n AS k, k AS n FROM NullCol26")).containsExactly("k:int4", "n:varchar");
    }

    // Regression: #11026
    @Test
    void aliasedSimpleQueryAgreesWithDescribeStatement() throws Exception {
      final Properties simple = new Properties();
      simple.setProperty("preferQueryMode", "simple");
      final Properties prepared = new Properties();
      prepared.setProperty("prepareThreshold", "-1");
      for (final String sql : List.of("SELECT n AS x FROM NullCol26", "SELECT n AS k FROM NullCol26", "SELECT n AS x, k FROM NullCol26"))
        assertThat(columnsOf(simple, sql)).as(sql).containsExactlyElementsOf(columnsOf(prepared, sql));
    }

    // Regression: #11026
    /** A computed item whose output name repeats a declared property is not that property: it keeps its own type. */
    @Test
    void computedItemNamedLikeADeclaredPropertyIsNotTypedFromIt() throws Exception {
      final Database database = getServerDatabase(0, getDatabaseName());
      database.transaction(() -> database.newVertex("NullCol26").set("n", 1).save());
      database.transaction(() -> database.command("sql", "UPDATE NullCol26 SET k = null"));
      final Properties simple = new Properties();
      simple.setProperty("preferQueryMode", "simple");
      final Properties prepared = new Properties();
      prepared.setProperty("prepareThreshold", "-1");
      final String sql = "SELECT k.toUpperCase() AS n FROM NullCol26";
      assertThat(columnsOf(simple, sql)).isEqualTo(columnsOf(prepared, sql)).containsExactly("n:varchar");
    }

    // Regression: #11026
    /** A projection must not gain declared columns it does not name. */
    @Test
    void projectionDoesNotGainUnnamedColumns() throws Exception {
      final Properties properties = new Properties();
      properties.setProperty("preferQueryMode", "simple");
      assertThat(columnsOf(properties, "SELECT k FROM NullCol26")).containsExactly("k:varchar");
    }

    private static void assertWildcardShape(final ResultSet rs) throws Exception {
      final ResultSetMetaData meta = rs.getMetaData();
      final List<String> columns = new ArrayList<>();
      for (int i = 1; i <= meta.getColumnCount(); i++)
        columns.add(meta.getColumnName(i) + ":" + meta.getColumnTypeName(i));
      assertThat(columns).contains("n:int4", "k:varchar", "d:numeric");
      assertThat(rs.next()).isTrue();
      assertThat(rs.getString("k")).isEqualTo("a");
      rs.getInt("n");
      assertThat(rs.wasNull()).isTrue();
      assertThat(rs.getBigDecimal("d")).isNull();
    }

    private List<String> columnsOf(final Properties properties, final String sql) throws Exception {
      try (final Connection connection = connect(properties); final PreparedStatement ps = connection.prepareStatement(sql);
          final ResultSet rs = ps.executeQuery()) {
        final ResultSetMetaData meta = rs.getMetaData();
        final List<String> columns = new ArrayList<>();
        for (int i = 1; i <= meta.getColumnCount(); i++)
          columns.add(meta.getColumnName(i) + ":" + meta.getColumnTypeName(i));
        return columns;
      }
    }
  }

  /**
   * Issue #11941: {@code SELECT *, !secret} over the PostgreSQL wire protocol announced a {@code secret} column and sent its stored
   * value, because the protocol read the row's backing record directly instead of the row, so the projection's exclusion never applied.
   * The same held for {@code !@rid} and {@code !@type}.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ExcludedColumn {
    @BeforeEach
    void createData() {
      final Database database = getServerDatabase(0, getDatabaseName());
      database.transaction(() -> {
        database.command("sql", "CREATE DOCUMENT TYPE P11941");
        database.command("sql", "INSERT INTO P11941 SET name = 'a', secret = 's3cr3t'");
      });
    }

    // Regression: #11941
    @Test
    void anExcludedPropertyIsNeitherAnnouncedNorSent() throws Exception {
      for (final Properties properties : modes()) {
        final String mode = properties.toString();
        try (final Connection connection = connectWithCopy(properties); final Statement statement = connection.createStatement();
            final ResultSet rs = statement.executeQuery("SELECT *, !secret FROM P11941")) {
          final List<String> columns = columnsOf(rs.getMetaData());
          assertThat(columns).as(mode).contains("name").doesNotContain("secret");
          assertThat(rs.next()).isTrue();
          for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++)
            assertThat(rs.getString(i)).as(mode + " column " + columns.get(i - 1)).isNotEqualTo("s3cr3t");
        }
      }
    }

    // Regression: #11941
    @Test
    void excludedRecordAttributesAreNotAnnounced() throws Exception {
      for (final Properties properties : modes()) {
        final String mode = properties.toString();
        try (final Connection connection = connectWithCopy(properties); final Statement statement = connection.createStatement();
            final ResultSet rs = statement.executeQuery("SELECT *, !@rid, !@type FROM P11941")) {
          final List<String> columns = columnsOf(rs.getMetaData());
          assertThat(columns).as(mode).contains("name", "secret").doesNotContain("@rid", "@type");
          assertThat(rs.next()).isTrue();
          assertThat(rs.getString("secret")).isEqualTo("s3cr3t");
        }
      }
    }

    // Regression: #11941
    @Test
    void anExcludedPropertyRewrittenByTheProjectionAnswersTheNewValue() throws Exception {
      for (final Properties properties : modes()) {
        final String mode = properties.toString();
        try (final Connection connection = connectWithCopy(properties); final Statement statement = connection.createStatement();
            final ResultSet rs = statement.executeQuery("SELECT *, !secret, 'hidden' AS secret FROM P11941")) {
          assertThat(rs.next()).isTrue();
          assertThat(rs.getString("secret")).as(mode).isEqualTo("hidden");
        }
      }
    }

    // Regression: #11941
    @Test
    void withoutExclusionEveryColumnIsStillThere() throws Exception {
      for (final Properties properties : modes()) {
        try (final Connection connection = connectWithCopy(properties); final Statement statement = connection.createStatement();
            final ResultSet rs = statement.executeQuery("SELECT * FROM P11941")) {
          assertThat(columnsOf(rs.getMetaData())).contains("name", "secret", "@rid", "@type");
        }
      }
    }

    private static List<Properties> modes() {
      final Properties simple = new Properties();
      simple.setProperty("preferQueryMode", "simple");
      final Properties extended = new Properties();
      extended.setProperty("prepareThreshold", "-1");
      return List.of(simple, extended);
    }

    private static List<String> columnsOf(final ResultSetMetaData meta) throws Exception {
      final List<String> columns = new ArrayList<>();
      for (int i = 1; i <= meta.getColumnCount(); i++)
        columns.add(meta.getColumnName(i));
      return columns;
    }

    private Connection connectWithCopy(final Properties properties) throws Exception {
      final Properties copy = new Properties();
      copy.putAll(properties);
      copy.setProperty("user", "root");
      copy.setProperty("password", DEFAULT_PASSWORD_FOR_TESTS);
      copy.setProperty("sslmode", "disable");
      return DriverManager.getConnection(getServerPostgresJdbcUrl(), copy);
    }
  }

  /**
   * Issue #11957: {@code out()}, {@code outE()} and {@code in()} answer a lazy engine iterable, which the Postgres wire typed as
   * a json column and then sent as the iterable's identity string ({@code com.arcadedb.graph.GraphEngine$10@72d8f43f}), so a
   * client decoding the json column (psycopg does it for every row) failed the whole fetch. The column now holds the records
   * as a JSON array, the neighbours HTTP and gRPC send.
   */
  @Nested
  class GraphTraversalColumn {
    private static final String QUERY = "SELECT k, out('E') AS o, outE('E') AS oe, in() AS i FROM V WHERE k = 1";

    private RID target;
    private RID edge;

    @BeforeEach
    void createGraph() {
      final Database database = getServerDatabase(0, getDatabaseName());
      database.transaction(() -> {
        database.getSchema().createVertexType("V");
        database.getSchema().createEdgeType("E");
      });
      database.transaction(() -> {
        final MutableVertex source = database.newVertex("V").set("k", 1).save();
        final MutableVertex destination = database.newVertex("V").set("k", 2).save();
        target = destination.getIdentity();
        edge = source.newEdge("E", destination).set("w", 1.5).save().getIdentity();
      });
    }

    // Regression: #11957
    @Test
    void simpleQuerySendsTheNeighboursAsJson() throws Exception {
      final Properties properties = new Properties();
      properties.setProperty("preferQueryMode", "simple");
      try (final Connection connection = connect(properties); final Statement statement = connection.createStatement();
          final ResultSet rs = statement.executeQuery(QUERY)) {
        assertNeighboursAsJson(rs);
      }
    }

    // Regression: #11957
    @Test
    void extendedQuerySendsTheNeighboursAsJson() throws Exception {
      final Properties properties = new Properties();
      properties.setProperty("prepareThreshold", "0");
      try (final Connection connection = connect(properties); final PreparedStatement statement = connection.prepareStatement(QUERY)) {
        // twice: pgjdbc switches to a named statement on a later execution
        for (int i = 0; i < 2; i++)
          try (final ResultSet rs = statement.executeQuery()) {
            assertNeighboursAsJson(rs);
          }
      }
    }

    // Regression: #11957
    /** A server-prepared statement in binary: the values never carry the identity string of the iterable. */
    @Test
    void serverPreparedBinaryQueryNeverSendsTheIterableIdentity() throws Exception {
      final Properties properties = new Properties();
      properties.setProperty("prepareThreshold", "-1");
      try (final Connection connection = connect(properties); final PreparedStatement statement = connection.prepareStatement(QUERY)) {
        for (int i = 0; i < 2; i++)
          try (final ResultSet rs = statement.executeQuery()) {
            assertThat(rs.next()).isTrue();
            for (final String column : new String[] { "o", "oe", "i" })
              assertThat(rs.getString(column)).as(column).doesNotContain("GraphEngine");
            assertThat(rs.getString("o")).contains(target.toString());
            assertThat(rs.getString("oe")).contains(edge.toString());
          }
      }
    }

    private void assertNeighboursAsJson(final ResultSet rs) throws Exception {
      assertThat(rs.next()).isTrue();
      assertThat(rs.getInt("k")).isEqualTo(1);

      // what a json-decoding client does with the column: it must parse
      assertThat(rs.getMetaData().getColumnTypeName(rs.findColumn("o"))).isEqualTo("json");
      final JSONArray neighbours = new JSONArray(rs.getString("o"));
      assertThat(neighbours.length()).isEqualTo(1);
      final JSONObject neighbour = neighbours.getJSONObject(0);
      assertThat(neighbour.getString("@rid")).isEqualTo(target.toString());
      assertThat(neighbour.getInt("k")).isEqualTo(2);

      assertThat(rs.getMetaData().getColumnTypeName(rs.findColumn("oe"))).isEqualTo("json");
      final JSONArray edges = new JSONArray(rs.getString("oe"));
      assertThat(edges.length()).isEqualTo(1);
      assertThat(edges.getJSONObject(0).getString("@rid")).isEqualTo(edge.toString());
      assertThat(edges.getJSONObject(0).getDouble("w")).isEqualTo(1.5);

      // no incoming edge: an empty array, not the identity string of an empty iterable
      assertThat(rs.getString("i")).doesNotContain("GraphEngine");
      assertThat((Object[]) rs.getArray("i").getArray()).isEmpty();

      assertThat(rs.next()).isFalse();
    }
  }

  /**
   * Issue #10927: a column declared LIST OF BINARY was announced as {@code _text} when the matched rows held empty lists, as
   * {@code json} when they held populated ones, as {@code varchar} when they held both, and as {@code _text} by a statement
   * Describe whatever the rows held, so the same row travelled as {@code [[1,2,3]]} or {@code {"[1,2,3]"}} depending on the
   * protocol. A BINARY is a {@code bytea}, so a list of them is a {@code bytea[]}, announced from the declaration alone, and
   * the client reads back the bytes stored.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ListOfBinaryColumn {
    private static final String[] FILTERS = { "WHERE n = 1", "WHERE n = 2", "WHERE n = 3", "", "WHERE n = 99" };

    @BeforeEach
    void createData() {
      final Database database = getServerDatabase(0, getDatabaseName());
      database.transaction(() -> {
        final VertexType type = database.getSchema().createVertexType("BinList");
        type.createProperty("b", Type.LIST, "BINARY");
        type.createProperty("n", Type.INTEGER);
      });
      database.transaction(() -> {
        database.newVertex("BinList").set("n", 1).set("b", new ArrayList<>()).save();
        database.newVertex("BinList").set("n", 2).set("b", List.of(new byte[] { 1, 2, 3 })).save();
        database.newVertex("BinList").set("n", 3).set("b", List.of(new byte[] { 1, 2, 3 }, new byte[] { (byte) 0xff, 0 })).save();
      });
    }

    // Regression: #10927
    @Test
    void simpleQueryAnnouncesByteaArrayWhateverTheRowsHold() throws Exception {
      final Properties properties = new Properties();
      properties.setProperty("preferQueryMode", "simple");
      assertEveryShapeIsByteaArray(properties);
    }

    // Regression: #10927
    @Test
    void extendedQueryAnnouncesByteaArrayWhateverTheRowsHold() throws Exception {
      final Properties properties = new Properties();
      properties.setProperty("prepareThreshold", "0");
      assertEveryShapeIsByteaArray(properties);
    }

    // Regression: #10927
    /** A server-prepared statement: Describe('S') types the columns from the schema and the values come in binary. */
    @Test
    void serverPreparedBinaryQueryAnnouncesByteaArrayWhateverTheRowsHold() throws Exception {
      final Properties properties = new Properties();
      properties.setProperty("prepareThreshold", "-1");
      assertEveryShapeIsByteaArray(properties);
    }

    private void assertEveryShapeIsByteaArray(final Properties properties) throws Exception {
      try (final Connection connection = connect(properties)) {
        for (final String filter : FILTERS) {
          final String sql = "SELECT n, b FROM BinList " + filter + " ORDER BY n";
          try (final Statement statement = connection.createStatement(); final ResultSet rs = statement.executeQuery(sql)) {
            assertRows(sql, rs, filter);
          }
          try (final PreparedStatement ps = connection.prepareStatement(sql)) {
            // twice: pgjdbc switches to a named statement on a later execution
            for (int i = 0; i < 2; i++)
              try (final ResultSet rs = ps.executeQuery()) {
                assertRows(sql, rs, filter);
              }
          }
        }
      }
    }

    private static void assertRows(final String sql, final ResultSet rs, final String filter) throws Exception {
      assertThat(rs.getMetaData().getColumnTypeName(2)).as(sql).isEqualTo("_bytea");
      while (rs.next()) {
        final int n = rs.getInt(1);
        final Array array = rs.getArray(2);
        final Object[] elements = (Object[]) array.getArray();
        switch (n) {
        case 1 -> assertThat(elements).as(sql).isEmpty();
        case 2 -> {
          assertThat(elements).as(sql).hasSize(1);
          assertThat((byte[]) elements[0]).as(sql).containsExactly(1, 2, 3);
        }
        case 3 -> {
          assertThat(elements).as(sql).hasSize(2);
          assertThat((byte[]) elements[0]).as(sql).containsExactly(1, 2, 3);
          assertThat((byte[]) elements[1]).as(sql).containsExactly((byte) 0xff, 0);
        }
        default -> throw new AssertionError("Unexpected row " + n + " for " + filter);
        }
      }
    }
  }

  private Connection connect(final Properties properties) throws Exception {
    properties.setProperty("user", "root");
    properties.setProperty("password", DEFAULT_PASSWORD_FOR_TESTS);
    properties.setProperty("sslmode", "disable");
    return DriverManager.getConnection(getServerPostgresJdbcUrl(), properties);
  }
}
