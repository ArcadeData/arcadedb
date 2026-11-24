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

import com.arcadedb.schema.DocumentType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What SQL tools and client libraries ask a PostgreSQL server at connect time, before they show anything: session and
 * system-information functions, the {@code pg_type} enumeration, and the catalog queries Arrow ADBC sends to bootstrap
 * its type map and to read a table's schema.
 */
class PostgresClientBootstrapIT extends PostgresWireProtocolTestBase {

  /*
   * Regression tests for issue #5290: SQL tools speaking the Postgres wire protocol ask a handful of
   * system-information questions before they will show anything, and ArcadeDB matched each of them against one
   * exact spelling. Anything a client wrote around the call missed the match and fell through to ArcadeDB's own
   * SQL engine, which either has no such function or - worse - has a different one under the same name.
   * <p>
   * Note on the two failures the report also lists, {@code 'list' object has no attribute 'encode'} for a LIST
   * column and all-NULL cells for EMBEDDED ones: those come from the client. They are a long-standing bug in
   * Microsoft's pgtoolsservice (the backend shared by the VS Code and Azure Data Studio PostgreSQL extensions),
   * which cannot render an array or json column against genuine PostgreSQL either - see
   * microsoft/azuredatastudio-postgresql#487 and #497. The OIDs ArcadeDB announces for those columns are the
   * correct ones and every other client decodes them, so they are deliberately left alone; the last test here
   * pins that down so a future "fix" for that report does not quietly downgrade them to text.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  // Regression: #5290
  @ParameterizedTest
  @ValueSource(strings = {
      // The query Beekeeper sends at connect time. "schema" is a reserved word in ArcadeDB SQL, so this
      // used to fail to parse and the tool never got past opening the connection.
      "SELECT CURRENT_SCHEMA() AS schema",
      "SELECT CURRENT_SCHEMA() AS \"schema\"",
      "select current_schema() as schema" })
  void currentSchemaAliasedToAReservedWord(final String query) throws Exception {
    try (final Connection connection = openConnection(); final Statement statement = connection.createStatement();
        final ResultSet resultSet = statement.executeQuery(query)) {
      assertThat(resultSet.getMetaData().getColumnName(1)).isEqualTo("schema");
      assertThat(resultSet.next()).isTrue();
      assertThat(resultSet.getString(1)).isEqualTo(getDatabaseName());
    }
  }

  // Regression: #5290
  @Test
  void aliasingVersionStillMeansThePostgresVersion() throws Exception {
    // ArcadeDB has a version() function of its own that answers the build string. Aliasing the call was
    // enough to reach it, so a client parsing the server version out of the answer read a version
    // PostgreSQL never had.
    try (final Connection connection = openConnection(); final Statement statement = connection.createStatement()) {
      for (final String query : new String[] { "SELECT version()", "SELECT version() AS v", "SELECT pg_catalog.version() AS v" })
        try (final ResultSet resultSet = statement.executeQuery(query)) {
          assertThat(resultSet.next()).as(query).isTrue();
          assertThat(resultSet.getString(1)).as(query).startsWith("PostgreSQL " + PostgresNetworkExecutor.PG_SERVER_VERSION);
        }
    }
  }

  // Regression: #5290
  @Test
  void aTrailingSemicolonSeparatedByASpaceIsStillTheSameQuery() throws Exception {
    // The simple-query path strips a semicolon only when it is the very last character, which left a
    // trailing space behind and broke the exact-string match.
    try (final Connection connection = openConnection(); final Statement statement = connection.createStatement();
        final ResultSet resultSet = statement.executeQuery("SELECT current_schema() ;")) {
      assertThat(resultSet.next()).isTrue();
      assertThat(resultSet.getString("current_schema")).isEqualTo(getDatabaseName());
    }
  }

  // Regression: #5290
  @Test
  void currentDatabaseAndCurrentCatalogAnswerTheDatabaseName() throws Exception {
    try (final Connection connection = openConnection(); final Statement statement = connection.createStatement()) {
      for (final String query : new String[] { "SELECT current_database()", "SELECT current_catalog" })
        try (final ResultSet resultSet = statement.executeQuery(query)) {
          assertThat(resultSet.next()).as(query).isTrue();
          assertThat(resultSet.getString(1)).as(query).isEqualTo(getDatabaseName());
        }
    }
  }

  // Regression: #5290
  @Test
  void currentUserAnswersTheAuthenticatedUser() throws Exception {
    // It used to answer NULL, which a tool showing "connected as" renders as an empty identity.
    try (final Connection connection = openConnection(); final Statement statement = connection.createStatement()) {
      for (final String query : new String[] { "SELECT current_user", "SELECT session_user", "SELECT current_role AS role" })
        try (final ResultSet resultSet = statement.executeQuery(query)) {
          assertThat(resultSet.next()).as(query).isTrue();
          assertThat(resultSet.getString(1)).as(query).isEqualTo("root");
        }
    }
  }

  // Regression: #5290
  @Test
  void enumeratingPgTypeAnswersTheTypesThisProtocolProduces() throws Exception {
    // This query was on an ignore-list and answered with zero rows, so a client that builds its whole
    // OID-to-name map up front built an empty one and then could not name a single column's type.
    final Map<Integer, String> catalog = new HashMap<>();
    try (final Connection connection = openConnection(); final Statement statement = connection.createStatement();
        final ResultSet resultSet = statement.executeQuery("SELECT oid, typname FROM pg_type")) {
      while (resultSet.next())
        catalog.put(resultSet.getInt("oid"), resultSet.getString("typname"));
    }

    // the types this protocol produces, and the row type of every type of the database (issue #10873)
    final Collection<? extends DocumentType> types = getServerDatabase(0, getDatabaseName()).getSchema().getTypes();
    assertThat(catalog).hasSize(PostgresType.values().length + types.size());
    for (final PostgresType type : PostgresType.values())
      assertThat(catalog).as(type.name()).containsEntry(type.code, type.typeName);
    for (final DocumentType type : types)
      assertThat(catalog).as(type.getName()).containsValue(type.getName());
  }

  // Regression: #5290
  @Test
  void enumeratingPgTypeKeepsTheProjectionOrderTheClientAskedFor() throws Exception {
    try (final Connection connection = openConnection(); final Statement statement = connection.createStatement();
        final ResultSet resultSet = statement.executeQuery("SELECT typname, oid FROM pg_catalog.pg_type")) {
      final ResultSetMetaData metaData = resultSet.getMetaData();
      assertThat(metaData.getColumnName(1)).isEqualTo("typname");
      assertThat(metaData.getColumnName(2)).isEqualTo("oid");
      assertThat(resultSet.next()).isTrue();
    }
  }

  // Regression: #5290
  @Test
  void listAndEmbeddedColumnsKeepTheirRealOids() throws Exception {
    // The schema from the report. The client that could not render these has the same trouble with genuine
    // PostgreSQL; downgrading the OIDs to text to work around it would break every client that can.
    try (final Connection connection = openConnection(); final Statement statement = connection.createStatement()) {
      statement.execute("CREATE DOCUMENT TYPE Product IF NOT EXISTS");
      statement.execute("CREATE PROPERTY Product.sku IF NOT EXISTS STRING");
      statement.execute("CREATE PROPERTY Product.name IF NOT EXISTS STRING");
      statement.execute("CREATE VERTEX TYPE Supplier IF NOT EXISTS");
      statement.execute("CREATE PROPERTY Supplier.name IF NOT EXISTS STRING");
      statement.execute("CREATE PROPERTY Supplier.certifications IF NOT EXISTS LIST");
      statement.execute("CREATE PROPERTY Supplier.embedded IF NOT EXISTS EMBEDDED OF Product");
      statement.execute("CREATE PROPERTY Supplier.embedded_list IF NOT EXISTS LIST OF Product");
      statement.execute("INSERT INTO Supplier (name, certifications, embedded, embedded_list) VALUES "
          + "('Berlin Sensors GmbH', 'ISO-9001,RoHS', { \"@type\": \"Product\", \"sku\": \"1234\", \"name\": \"CPU\"}, "
          + "[{ \"@type\": \"Product\", \"sku\": \"1234\", \"name\": \"CPU\"}])");

      try (final ResultSet resultSet = statement.executeQuery(
          "SELECT name, certifications, embedded, embedded_list FROM `Supplier`")) {
        final ResultSetMetaData metaData = resultSet.getMetaData();
        assertThat(metaData.getColumnTypeName(1)).isEqualTo("varchar");
        assertThat(metaData.getColumnTypeName(2)).isEqualTo("_text");
        assertThat(metaData.getColumnTypeName(3)).isEqualTo("json");
        assertThat(metaData.getColumnTypeName(4)).isEqualTo("json");

        assertThat(resultSet.next()).isTrue();
        assertThat(resultSet.getString("name")).isEqualTo("Berlin Sensors GmbH");
        assertThat(resultSet.getString("certifications")).contains("ISO-9001,RoHS");
        assertThat(resultSet.getString("embedded")).contains("\"sku\":\"1234\"");
        assertThat(resultSet.getString("embedded_list")).contains("\"sku\":\"1234\"");
      }
    }
  }

  private Connection openConnection() throws Exception {
    Class.forName("org.postgresql.Driver");
    final Properties properties = new Properties();
    properties.setProperty("user", "root");
    properties.setProperty("password", DEFAULT_PASSWORD_FOR_TESTS);
    properties.setProperty("ssl", "false");
    properties.setProperty("sslMode", "disable");
    properties.setProperty("preferQueryMode", "simple");
    return DriverManager.getConnection(getServerPostgresJdbcUrl(), properties);
  }

  /*
   * Regression test for issue #7178, reported in discussion #6888: the Apache Arrow native PostgreSQL ADBC
   * driver cannot connect to ArcadeDB, failing with
   * {@code Expected 5 or 6 columns from type resolver pg_type query but got 0}.
   * <p>
   * The driver's {@code PostgresDatabase::RebuildTypeResolver} runs three queries before it hands the caller
   * a connection, and it validates the <i>shape</i> of each answer rather than its content - zero rows are
   * fine, zero columns are fatal:
   * <ol>
   * <li>{@code SELECT version();} - exactly one row and one column;</li>
   * <li>{@code SELECT attrelid, attname, atttypid FROM pg_catalog.pg_attribute ORDER BY attrelid, attnum} -
   * exactly three columns;</li>
   * <li>{@code SELECT oid, typname, typreceive, typbasetype, typrelid, typarray FROM pg_catalog.pg_type WHERE
   * (typreceive != 0 OR typsend != 0) AND typtype != 'r' AND typreceive::TEXT != 'array_recv'} - five or six
   * columns.</li>
   * </ol>
   * The third one is the one that was declined: {@link PostgresTypeCatalog} knew neither {@code typreceive}
   * nor {@code typsend}, and its WHERE clause parser accepted only a single {@code oid = N} or
   * {@code typname = '...'} equality; {@link PostgresCatalog} then declined it too, because {@code pg_type}
   * was registered there only as a relation that decorates a {@code pg_attribute} join and never as one a
   * query could be about. The query fell through to the empty-result fallback, which announces a
   * RowDescription with no fields at all.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  /** Verbatim from arrow-adbc {@code c/driver/postgresql/database.cc}, {@code RebuildTypeResolver}. */
  private static final String ADBC_PG_TYPE_QUERY = //
      "SELECT oid, typname, typreceive, typbasetype, typrelid, typarray FROM "
          + "pg_catalog.pg_type WHERE (typreceive != 0 OR typsend != 0) AND typtype != 'r' AND "
          + "typreceive::TEXT != 'array_recv'";

  /** Verbatim from the same function, {@code kColumnsQuery}. */
  private static final String ADBC_PG_ATTRIBUTE_QUERY = //
      "SELECT attrelid, attname, atttypid FROM pg_catalog.pg_attribute ORDER BY attrelid, attnum";

  // Regression: #7178
  @Test
  void theArrowAdbcTypeResolverQueryAnswersWithSixColumns() throws Exception {
    try (final Connection connection = openConnection();
        final Statement statement = connection.createStatement();
        final ResultSet resultSet = statement.executeQuery(ADBC_PG_TYPE_QUERY)) {

      final ResultSetMetaData metaData = resultSet.getMetaData();
      assertThat(metaData.getColumnCount())
          .as("the ADBC driver rejects anything but 5 or 6 columns from its pg_type bootstrap")
          .isEqualTo(6);
      assertThat(metaData.getColumnName(1)).isEqualTo("oid");
      assertThat(metaData.getColumnName(2)).isEqualTo("typname");
      assertThat(metaData.getColumnName(3)).isEqualTo("typreceive");
      assertThat(metaData.getColumnName(4)).isEqualTo("typbasetype");
      assertThat(metaData.getColumnName(5)).isEqualTo("typrelid");
      assertThat(metaData.getColumnName(6)).isEqualTo("typarray");

      final Map<Integer, String[]> byOid = new HashMap<>();
      while (resultSet.next())
        byOid.put(resultSet.getInt("oid"), new String[] { resultSet.getString("typname"),
            resultSet.getString("typreceive"), resultSet.getString("typarray") });

      assertThat(byOid).as("an empty catalog leaves the resolver unable to name any column's type").isNotEmpty();

      // int4: the driver reads typreceive to pick a decoder, so it has to be the real receive function.
      assertThat(byOid.get(23)).as("int4").isNotNull();
      assertThat(byOid.get(23)[0]).isEqualTo("int4");
      assertThat(byOid.get(23)[1]).isEqualTo("int4recv");
      assertThat(byOid.get(23)[2]).as("int4's typarray, from which the driver synthesises _int4").isEqualTo("1007");

      assertThat(byOid.get(25)[1]).as("text").isEqualTo("textrecv");
      assertThat(byOid.get(16)[1]).as("bool").isEqualTo("boolrecv");
      assertThat(byOid.get(1114)[1]).as("timestamp").isEqualTo("timestamp_recv");

      assertThat(byOid)
          .as("typreceive::TEXT != 'array_recv' excludes the array types: the driver derives them from typarray")
          .doesNotContainKey(1007);
    }
  }

  // Regression: #7178
  @Test
  void theArrowAdbcPgAttributeQueryAnswersWithThreeColumns() throws Exception {
    try (final Connection connection = openConnection();
        final Statement statement = connection.createStatement();
        final ResultSet resultSet = statement.executeQuery(ADBC_PG_ATTRIBUTE_QUERY)) {

      assertThat(resultSet.getMetaData().getColumnCount())
          .as("the ADBC driver rejects anything but 3 columns from its pg_attribute bootstrap")
          .isEqualTo(3);
    }
  }

  // Regression: #7178
  @Test
  void theArrowAdbcVersionQueryAnswersWithOneRowAndOneColumn() throws Exception {
    try (final Connection connection = openConnection();
        final Statement statement = connection.createStatement();
        final ResultSet resultSet = statement.executeQuery("SELECT version();")) {

      assertThat(resultSet.getMetaData().getColumnCount()).isEqualTo(1);
      assertThat(resultSet.next()).isTrue();
      assertThat(resultSet.getString(1)).startsWith("PostgreSQL ");
      assertThat(resultSet.next()).as("exactly one row").isFalse();
    }
  }

  // Regression: #7178
  @Test
  void aPgTypeColumnThisCatalogDoesNotModelAnswersNullRatherThanNothing() throws Exception {
    // The two recognisers disagree about this shape on purpose, and only the end-to-end answer settles it.
    // PostgresTypeCatalog declines a projection naming a column it cannot value, which used to end the
    // query with an empty result set because pg_type had no family in PostgresCatalog either. Now it falls
    // through to the generic catalog, where an unmodelled column of a modelled relation answers NULL - the
    // same treatment pg_class and pg_attribute have always given one. A client that asks for typacl gets
    // the rest of its row instead of being told the whole query matched nothing.
    try (final Connection connection = openConnection();
        final Statement statement = connection.createStatement();
        final ResultSet resultSet = statement.executeQuery("SELECT oid, typname, typacl FROM pg_catalog.pg_type")) {

      assertThat(resultSet.getMetaData().getColumnCount()).isEqualTo(3);

      int rows = 0;
      while (resultSet.next()) {
        ++rows;
        assertThat(resultSet.getInt("oid")).isPositive();
        assertThat(resultSet.getString("typname")).isNotBlank();
        assertThat(resultSet.getString("typacl")).as("no built-in type carries an ACL").isNull();
      }

      assertThat(rows).as("an unmodelled column must not empty the result set").isPositive();
    }
  }

  /*
   * Regression test for issue #7180, the follow-up to #7178: with the type-resolver bootstrap answered, the
   * Apache Arrow native PostgreSQL ADBC driver (1.12.0) connects, but {@code GetTableSchema} comes back with
   * zero fields for a table that exists.
   * <p>
   * {@code PostgresConnection::GetTableSchema} (arrow-adbc {@code c/driver/postgresql/connection.cc}, tag
   * {@code apache-arrow-adbc-24}) selects a table's columns by casting the table's name to {@code regclass}:
   *
   * <pre>
   * SELECT attname, atttypid
   * FROM pg_catalog.pg_class AS cls
   * INNER JOIN pg_catalog.pg_attribute AS attr ON cls.oid = attr.attrelid
   * INNER JOIN pg_catalog.pg_type AS typ ON attr.atttypid = typ.oid
   * WHERE attr.attnum &gt;= 0 AND cls.oid = $1::regclass::oid
   * ORDER BY attr.attnum
   * </pre>
   * <p>
   * The one text parameter is the table name after {@code PQescapeIdentifier}, so the double quotes are part
   * of the value. Every cast used to be transparent in the catalog's expression evaluator - which is right for
   * the casts that only steer PostgreSQL's own type resolution, and wrong for the OID-alias types, where the
   * cast <i>is</i> the lookup. {@code cls.oid = '"adbc_big"'} compared a number against a name, and the
   * predicate quietly selected nothing, so the driver built a schema with no fields and reported none.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  /** Verbatim from arrow-adbc {@code c/driver/postgresql/connection.cc}, {@code GetTableSchema}. */
  private static final String ADBC_GET_TABLE_SCHEMA = //
      "SELECT attname, atttypid FROM pg_catalog.pg_class AS cls "
          + "INNER JOIN pg_catalog.pg_attribute AS attr ON cls.oid = attr.attrelid "
          + "INNER JOIN pg_catalog.pg_type AS typ ON attr.atttypid = typ.oid "
          + "WHERE attr.attnum >= 0 AND cls.oid = $1::regclass::oid "
          + "ORDER BY attr.attnum";

  // Regression: #7180
  @Test
  void theDriversTableSchemaQueryDescribesTheTableItNames() throws Exception {
    try (final Connection connection = openJdbcConnection()) {
      createSchema(connection);

      // pgjdbc writes its own placeholder, and the driver's text is written for PQexecParams: the query goes
      // out with $1 either way, so it is sent as a literal with the escaped identifier already in it, which
      // is the string PQescapeIdentifier hands the driver.
      final List<String> columns = tableSchemaOf(connection, "\"adbc_big7180\"");

      assertThat(columns).as("GetTableSchema built a schema with no fields at all")
          .containsExactly("count", "created", "id", "name", "price");
    }
  }

  // Regression: #7180
  @Test
  void theDriversTableSchemaQueryNamesTheColumnTypesTheDriverDecodesWith() throws Exception {
    try (final Connection connection = openJdbcConnection()) {
      createSchema(connection);

      try (final Statement statement = connection.createStatement();
          final ResultSet resultSet = statement
              .executeQuery(ADBC_GET_TABLE_SCHEMA.replace("$1", "'\"adbc_big7180\"'"))) {

        final List<Integer> oids = new ArrayList<>();
        while (resultSet.next())
          oids.add(resultSet.getInt("atttypid"));

        // count, created, id, name, price - the properties in the order this catalog reports them.
        assertThat(oids).containsExactly(PostgresType.LONG.code, PostgresType.DATE.code,
            PostgresType.INTEGER.code, PostgresType.VARCHAR.code, PostgresType.DOUBLE.code);
      }
    }
  }

  // Regression: #7180
  @Test
  void aTableTheDriverAsksAboutThatDoesNotExistDescribesNoColumnsRatherThanEveryColumn() throws Exception {
    try (final Connection connection = openJdbcConnection()) {
      createSchema(connection);

      assertThat(tableSchemaOf(connection, "\"no_such_table_7180\""))
          .as("an unresolvable regclass name must not fall back to describing every column of every table")
          .isEmpty();
    }
  }

  // Regression: #7180
  @Test
  void theSameQuestionThroughTheExtendedProtocolIsAnsweredToo() throws Exception {
    // PQexecParams is the extended protocol, so the parameter reaches the catalog at Execute time rather
    // than being inlined. The answer has to be the same one the simple protocol gives.
    try (final Connection connection = openJdbcConnection()) {
      createSchema(connection);

      try (final PreparedStatement statement = connection.prepareStatement(ADBC_GET_TABLE_SCHEMA.replace("$1", "?"))) {
        statement.setString(1, "\"adbc_big7180\"");

        try (final ResultSet resultSet = statement.executeQuery()) {
          final List<String> columns = new ArrayList<>();
          while (resultSet.next())
            columns.add(resultSet.getString("attname"));

          assertThat(columns).containsExactly("count", "created", "id", "name", "price");
        }
      }
    }
  }

  private List<String> tableSchemaOf(final Connection connection, final String escapedIdentifier) throws Exception {
    try (final Statement statement = connection.createStatement();
        final ResultSet resultSet = statement
            .executeQuery(ADBC_GET_TABLE_SCHEMA.replace("$1", "'" + escapedIdentifier + "'"))) {

      final List<String> columns = new ArrayList<>();
      while (resultSet.next())
        columns.add(resultSet.getString("attname"));
      return columns;
    }
  }

  private void createSchema(final Connection connection) throws Exception {
    try (final Statement statement = connection.createStatement()) {
      statement.execute("CREATE DOCUMENT TYPE adbc_big7180 IF NOT EXISTS");
      statement.execute("CREATE PROPERTY adbc_big7180.id IF NOT EXISTS INTEGER");
      statement.execute("CREATE PROPERTY adbc_big7180.name IF NOT EXISTS STRING");
      statement.execute("CREATE PROPERTY adbc_big7180.price IF NOT EXISTS DOUBLE");
      statement.execute("CREATE PROPERTY adbc_big7180.created IF NOT EXISTS DATE");
      statement.execute("CREATE PROPERTY adbc_big7180.count IF NOT EXISTS LONG");
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
