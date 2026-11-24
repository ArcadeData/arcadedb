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
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.postgresql.core.TypeInfo;
import org.postgresql.jdbc.PgConnection;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static com.arcadedb.postgres.PostgresWireMessages.WireMessage;
import static com.arcadedb.postgres.PostgresWireMessages.readUntilReadyForQuery;
import static com.arcadedb.postgres.PostgresWireMessages.sendBind;
import static com.arcadedb.postgres.PostgresWireMessages.sendExecute;
import static com.arcadedb.postgres.PostgresWireMessages.sendParse;
import static com.arcadedb.postgres.PostgresWireMessages.sendSimpleQuery;
import static com.arcadedb.postgres.PostgresWireMessages.sendSync;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * The PostgreSQL system catalog ({@code pg_catalog}, {@code information_schema}) and the JDBC {@code DatabaseMetaData}
 * calls built on it, exercised over the wire through the JDBC driver or raw protocol messages: the relations SQL tools and
 * drivers query before they show anything, the ones the catalog declines rather than guessing at, and the user data that
 * merely looks like a catalog query.
 */
class PostgresCatalogIT extends PostgresWireProtocolTestBase {

  /*
   * Regression tests for the second half of issue #6412: the catalog answers a client needs to render a
   * database - the schema list, the table list, the columns of a table - were matched by string equality
   * against one tool's exact spelling, several of them only when {@code application_name} was literally
   * {@code "dbvis"}. The same questions from any other client fell through and were answered with nothing.
   * <p>
   * Every test here goes through the JDBC driver's own {@link DatabaseMetaData} calls, with no
   * {@code application_name} set at all: the queries are the driver's, not this test's, so what is being
   * asserted is that the answer follows from the shape of the question rather than from the name of the asker.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  // Regression: #6412
  @Test
  void theSchemaListNamesTheConnectedDatabase() throws Exception {
    try (final Connection connection = openJdbcConnection()) {
      createArticleSchema(connection);

      final List<String> schemas = new ArrayList<>();
      try (final ResultSet resultSet = connection.getMetaData().getSchemas()) {
        while (resultSet.next())
          schemas.add(resultSet.getString("TABLE_SCHEM"));
      }

      assertThat(schemas).contains(getDatabaseName());
    }
  }

  // Regression: #6412
  @Test
  void theTableListNamesEveryType() throws Exception {
    try (final Connection connection = openJdbcConnection()) {
      createArticleSchema(connection);

      final List<String> tables = new ArrayList<>();
      final List<String> types = new ArrayList<>();
      try (final ResultSet resultSet = connection.getMetaData().getTables(null, null, "%", new String[] { "TABLE" })) {
        while (resultSet.next()) {
          tables.add(resultSet.getString("TABLE_NAME"));
          types.add(resultSet.getString("TABLE_TYPE"));
        }
      }

      assertThat(tables).contains("Article6412", "Author6412");
      // The driver's own CASE over relkind produced this string: the catalog never spells it itself.
      assertThat(types).containsOnly("TABLE");
    }
  }

  // Regression: #6412
  @Test
  void theTableListHonoursTheNameTheClientAskedFor() throws Exception {
    try (final Connection connection = openJdbcConnection()) {
      createArticleSchema(connection);

      final List<String> tables = new ArrayList<>();
      try (final ResultSet resultSet = connection.getMetaData().getTables(null, null, "Article6412", null)) {
        while (resultSet.next())
          tables.add(resultSet.getString("TABLE_NAME"));
      }

      assertThat(tables).containsExactly("Article6412");
    }
  }

  // Regression: #6412
  @Test
  void theColumnListDescribesTheTypesProperties() throws Exception {
    try (final Connection connection = openJdbcConnection()) {
      createArticleSchema(connection);

      final List<String> columns = new ArrayList<>();
      final List<String> typeNames = new ArrayList<>();
      try (final ResultSet resultSet = connection.getMetaData().getColumns(null, null, "Article6412", "%")) {
        while (resultSet.next()) {
          columns.add(resultSet.getString("COLUMN_NAME"));
          typeNames.add(resultSet.getString("TYPE_NAME"));
        }
      }

      assertThat(columns).containsExactlyInAnyOrder("id", "published", "title");
      assertThat(typeNames).contains("int4", "varchar");
      // Columns of the other type must not leak into the answer: the client asked about one table.
      assertThat(columns).doesNotContain("name");
    }
  }

  // Regression: #6412
  @Test
  void aCatalogQuerySpelledByHandIsAnsweredToo() throws Exception {
    try (final Connection connection = openJdbcConnection(); final Statement statement = connection.createStatement()) {
      createArticleSchema(connection);

      // information_schema, in a spelling no tool in the old allow-list ever sent.
      final List<String> tables = new ArrayList<>();
      try (final ResultSet resultSet = statement.executeQuery(
          "SELECT table_name FROM information_schema.tables WHERE table_schema = '" + getDatabaseName()
              + "' ORDER BY table_name")) {
        while (resultSet.next())
          tables.add(resultSet.getString("table_name"));
      }

      assertThat(tables).contains("Article6412", "Author6412");
    }
  }

  // Regression: #6412
  @Test
  void theUserListIsAnsweredForEveryClientNotJustTheAllowListedOne() throws Exception {
    try (final Connection connection = openJdbcConnection(); final Statement statement = connection.createStatement()) {
      // This exact query used to be answered only when application_name was "dbvis"; this connection sets none.
      try (final ResultSet resultSet = statement.executeQuery(
          "select distinct GRANTEE as USER_NAME, 'N' as IS_EXPIRED, 'N' as IS_LOCKED from INFORMATION_SCHEMA.USAGE_PRIVILEGES order by GRANTEE asc")) {
        assertThat(resultSet.next()).isTrue();
        assertThat(resultSet.getString("USER_NAME")).isEqualTo("root");
        assertThat(resultSet.getString("IS_EXPIRED")).isEqualTo("N");
        assertThat(resultSet.next()).isFalse();
      }
    }
  }

  // Regression: #6412
  @Test
  void theCharacterSetListIsAnswered() throws Exception {
    try (final Connection connection = openJdbcConnection(); final Statement statement = connection.createStatement()) {
      try (final ResultSet resultSet = statement.executeQuery(
          "select CHARACTER_SET_NAME as CHARSET_NAME, -1 as MAX_LENGTH from INFORMATION_SCHEMA.CHARACTER_SETS order by CHARACTER_SET_NAME asc")) {
        assertThat(resultSet.next()).isTrue();
        assertThat(resultSet.getString("CHARSET_NAME")).isEqualTo("UTF8");
        assertThat(resultSet.getInt("MAX_LENGTH")).isEqualTo(-1);
      }
    }
  }

  // Regression: #6412
  @Test
  void theSchemaListSpelledWithCaseExpressionsIsAnswered() throws Exception {
    try (final Connection connection = openJdbcConnection(); final Statement statement = connection.createStatement()) {
      // The DbVisualizer spelling, which used to be one of the exact-match arms. Its CASE expressions are
      // evaluated rather than recognised, so any variation on them works as well.
      try (final ResultSet resultSet = statement.executeQuery(
          "select NSPNAME as SCHEMA_NAME, case when lower(NSPNAME)='pg_catalog' then 'Y' else 'N' end as IS_PUBLIC, "
              + "case when lower(NSPNAME)='information_schema' then 'Y' else 'N' end as IS_SYSTEM, 'N' as IS_EMPTY "
              + "from PG_CATALOG.PG_NAMESPACE order by NSPNAME asc")) {
        // the database's schema and pg_catalog, the schema of the built-in types (issue #10745), each as the client's CASE reads it
        final Map<String, String> isPublic = new TreeMap<>();
        while (resultSet.next()) {
          isPublic.put(resultSet.getString("SCHEMA_NAME"), resultSet.getString("IS_PUBLIC"));
          assertThat(resultSet.getString("IS_SYSTEM")).isEqualTo("N");
        }
        assertThat(isPublic).containsOnly(entry(getDatabaseName(), "N"), entry("pg_catalog", "Y"));
      }
    }
  }

  // Regression: #6412
  @Test
  void anUnmodelledCatalogRelationIsDeclinedRatherThanGuessedAt() throws Exception {
    try (final Connection connection = openJdbcConnection(); final Statement statement = connection.createStatement()) {
      // pg_extension has rows in every PostgreSQL, and this catalog models none of them. The client is told so, with
      // feature_not_supported (issue #10371), and not handed an empty answer that reads as "no extension here" nor an invented row.
      assertThatThrownBy(() -> statement.executeQuery("SELECT oid FROM pg_catalog.pg_extension")).isInstanceOf(SQLException.class)
          .satisfies(e -> assertThat(((SQLException) e).getSQLState()).isEqualTo("0A000"))
          .hasMessageContaining("pg_extension");

      // and the connection goes on
      try (final ResultSet resultSet = statement.executeQuery("SELECT 1")) {
        assertThat(resultSet.next()).isTrue();
      }
    }
  }

  // Regression: #6412
  @Test
  void aUserTypeCalledLikeACatalogRelationIsStillQueryable() throws Exception {
    try (final Connection connection = openJdbcConnection(); final Statement statement = connection.createStatement()) {
      // The pre-filter looks for "pg_" anywhere in the query, so a type whose name starts with it has to
      // survive the catalog recogniser and reach the SQL engine.
      statement.execute("CREATE DOCUMENT TYPE pg_notes6412 IF NOT EXISTS");
      statement.execute("CREATE PROPERTY pg_notes6412.note IF NOT EXISTS STRING");
      statement.execute("INSERT INTO pg_notes6412 SET note = 'mine'");

      try (final ResultSet resultSet = statement.executeQuery("SELECT note FROM pg_notes6412")) {
        assertThat(resultSet.next()).isTrue();
        assertThat(resultSet.getString("note")).isEqualTo("mine");
      }
    }
  }

  private void createArticleSchema(final Connection connection) throws Exception {
    try (final Statement statement = connection.createStatement()) {
      statement.execute("CREATE DOCUMENT TYPE Article6412 IF NOT EXISTS");
      statement.execute("CREATE PROPERTY Article6412.id IF NOT EXISTS INTEGER");
      statement.execute("CREATE PROPERTY Article6412.title IF NOT EXISTS STRING");
      statement.execute("CREATE PROPERTY Article6412.published IF NOT EXISTS DATETIME");
      statement.execute("CREATE DOCUMENT TYPE Author6412 IF NOT EXISTS");
      statement.execute("CREATE PROPERTY Author6412.name IF NOT EXISTS STRING");
    }
  }

  /*
   * Regression test for issue #7224 through the PostgreSQL JDBC driver itself.
   * <p>
   * {@link DatabaseMetaData#getTypeInfo()} joins pg_type to pg_namespace, which the catalog answered as a question
   * about SCHEMAS: one fabricated row whose type columns were all NULL. The driver reported a single type with no
   * name, silently - any tool populating a type picker or validating a column type against the catalog got that.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  // Regression: #7224
  @Test
  void theTypeListIsTheTypeListAndNotOneNamelessRow() throws Exception {
    try (final Connection connection = openJdbcConnection()) {
      final List<String> names = new ArrayList<>();
      try (final ResultSet resultSet = connection.getMetaData().getTypeInfo()) {
        while (resultSet.next())
          names.add(resultSet.getString("TYPE_NAME"));
      }

      assertThat(names).hasSizeGreaterThan(1).doesNotContainNull();
      // The names PostgreSQL uses, which is what a client resolves an OID against.
      assertThat(names).contains("int4", "int8", "varchar", "bool");
    }
  }

  /*
   * Regression test for issue #10228 over the wire, through pgjdbc's extended protocol: the catalog shapes psql and
   * psqlodbc send are answered, a projection naming two columns alike announces both, and a set operation the
   * catalog cannot evaluate still reaches the SQL engine rather than being swallowed into an empty answer.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  // Regression: #10228
  @Test
  void odbcAllTableTypesAnnouncesThreeColumns() throws Exception {
    try (final Connection connection = openJdbcConnection("extended"); final Statement statement = connection.createStatement();
        final ResultSet resultSet = statement.executeQuery(
            "select NULL, NULL, relkind from (select 'r' as relkind union select 'v' union select 'm' union select 'f' "
                + "union select 'p') as a")) {
      final ResultSetMetaData metaData = resultSet.getMetaData();
      assertThat(metaData.getColumnCount()).as("psqlodbc reads the third column").isEqualTo(3);
      assertThat(metaData.getColumnName(1)).isEqualTo("?column?");
      assertThat(metaData.getColumnName(2)).isEqualTo("?column?");
      assertThat(metaData.getColumnName(3)).isEqualTo("relkind");

      final List<String> kinds = new ArrayList<>();
      while (resultSet.next()) {
        assertThat(resultSet.getString(1)).isNull();
        kinds.add(resultSet.getString(3));
      }
      assertThat(kinds).containsExactly("r", "v", "m", "f", "p");
    }
  }

  // Regression: #10228
  /** Describe on the prepared statement, before it runs: the duplicate names are announced the same way. */
  @Test
  void aDescribedStatementAnnouncesDuplicateColumnsByName() throws Exception {
    try (final Connection connection = openJdbcConnection("extended"); final PreparedStatement statement = connection.prepareStatement(
        "select NULL, NULL, relkind from (select 'r' as relkind union select 'v') as a")) {
      final ResultSetMetaData metaData = statement.getMetaData();
      assertThat(metaData.getColumnCount()).isEqualTo(3);
      assertThat(metaData.getColumnName(1)).isEqualTo("?column?");
      assertThat(metaData.getColumnName(2)).isEqualTo("?column?");
      assertThat(metaData.getColumnName(3)).isEqualTo("relkind");

      final List<String> kinds = new ArrayList<>();
      try (final ResultSet resultSet = statement.executeQuery()) {
        while (resultSet.next())
          kinds.add(resultSet.getString(3));
      }
      assertThat(kinds).containsExactly("r", "v");
    }
  }

  // Regression: #10228
  @Test
  void theFiveShapesOfTheIssue() throws Exception {
    try (final Connection connection = openJdbcConnection("extended")) {
      createPersonSchema(connection);

      final int tables = count(connection, "select c.relname from pg_catalog.pg_class c");
      assertThat(tables).isGreaterThanOrEqualTo(3);

      assertThat(count(connection,
          "select 1 from (pg_catalog.pg_class c inner join pg_catalog.pg_namespace n on n.oid = c.relnamespace)"))
          .isEqualTo(tables);
      assertThat(count(connection,
          "select c.relname from pg_catalog.pg_class c left join pg_catalog.pg_am am on am.oid = c.relam"))
          .isEqualTo(tables);
      assertThat(count(connection,
          "select relname from pg_catalog.pg_class where relname operator(pg_catalog.=) 'zzz'")).isZero();
      assertThat(strings(connection, "select 1 union select 2")).containsExactly("1", "2");
      assertThat(strings(connection, "select count(*) from pg_catalog.pg_class c")).containsExactly(String.valueOf(tables));
    }
  }

  // Regression: #10228
  /** psqlodbc's SQLColumns("Person"), whose table filter is in a join condition of a parenthesised join. */
  @Test
  void odbcColumnsOfOneTable() throws Exception {
    try (final Connection connection = openJdbcConnection("extended")) {
      createPersonSchema(connection);

      final List<String> columns = strings(connection,
          "select a.attname from (((pg_catalog.pg_class c inner join pg_catalog.pg_namespace n "
              + "on n.oid = c.relnamespace and c.relname like 'Person10228') inner join pg_catalog.pg_attribute a "
              + "on (not a.attisdropped) and a.attnum operator(pg_catalog.>) 0 and a.attrelid operator(pg_catalog.=) c.oid) "
              + "inner join pg_catalog.pg_type t on t.oid operator(pg_catalog.=) a.atttypid) left outer join pg_attrdef d "
              + "on a.atthasdef and d.adrelid operator(pg_catalog.=) a.attrelid and d.adnum operator(pg_catalog.=) a.attnum "
              + "order by n.nspname, c.relname, attnum");
      assertThat(columns).containsExactly("id", "name");
    }
  }

  // Regression: #10228
  /**
   * A catalog query that spends more work than one statement may is refused with PostgreSQL's
   * program_limit_exceeded, over both protocols, rather than answered with an empty catalog - and the connection
   * goes on working.
   */
  @Test
  void aCatalogQueryOverItsWorkBudgetIsRefusedWithSqlState54000() throws Exception {
    for (final String mode : List.of("extended", "simple")) {
      try (final Connection connection = openJdbcConnection(mode)) {
        try (final Statement statement = connection.createStatement()) {
          statement.execute("CREATE DOCUMENT TYPE " + "x".repeat(40) + " IF NOT EXISTS");
        }

        assertThatThrownBy(() -> strings(connection,
            "select relname from pg_catalog.pg_class where relname ~ '^(.*)(.*)(.*)(.*)(.*)Y$'"))
            .isInstanceOf(SQLException.class)
            .satisfies(e -> assertThat(((SQLException) e).getSQLState()).as(mode).isEqualTo("54000"));

        assertThat(strings(connection, "select relname from pg_catalog.pg_class where relname = 'Person10228'")).as(mode)
            .isEmpty();
      }
    }
  }

  /**
   * A catalog pattern that can take many steps without reading a character is refused the same way, over both protocols and
   * whether it filters a relation or is evaluated on its own, and the connection goes on working.
   */
  @Test
  void aCatalogPatternThatStepsWithoutReadingIsRefusedWithSqlState54000() throws Exception {
    for (final String mode : List.of("extended", "simple")) {
      try (final Connection connection = openJdbcConnection(mode)) {
        for (final String pattern : List.of("((){1000}){1000}", "(|)".repeat(12) + "(?!)", "^?".repeat(12) + "(?!)")) {
          assertThatThrownBy(() -> strings(connection, "select relname from pg_catalog.pg_class where relname ~ '" + pattern + "'"))
              .isInstanceOf(SQLException.class)
              .satisfies(e -> assertThat(((SQLException) e).getSQLState()).as("%s: %s", mode, pattern).isEqualTo("54000"));
          assertThatThrownBy(() -> strings(connection, "select '' ~ '" + pattern + "', pg_catalog.version()"))
              .isInstanceOf(SQLException.class)
              .satisfies(e -> assertThat(((SQLException) e).getSQLState()).as("%s: %s", mode, pattern).isEqualTo("54000"));
        }

        assertThat(strings(connection, "select relname from pg_catalog.pg_class where relname ~ '^Person10228$'")).as(mode)
            .isEmpty();
      }
    }
  }

  // Regression: #10228
  /** A set operation over the user's own types is not a catalog query: the SQL engine still answers it. */
  @Test
  void aUnionOverAUserTypeIsStillTheSqlEnginesToAnswer() throws Exception {
    try (final Connection connection = openJdbcConnection("extended")) {
      createPersonSchema(connection);

      assertThatThrownBy(() -> strings(connection, "select name from Person10228 union select 'x'"))
          .isInstanceOf(SQLException.class);
      // and the connection is still usable afterwards
      assertThat(strings(connection, "select 1 union all select 1")).containsExactly("1", "1");
    }
  }

  private static int count(final Connection connection, final String query) throws SQLException {
    return strings(connection, query).size();
  }

  private static List<String> strings(final Connection connection, final String query) throws SQLException {
    final List<String> values = new ArrayList<>();
    try (final Statement statement = connection.createStatement(); final ResultSet resultSet = statement.executeQuery(query)) {
      while (resultSet.next())
        values.add(resultSet.getString(1));
    }
    return values;
  }

  private void createPersonSchema(final Connection connection) throws Exception {
    try (final Statement statement = connection.createStatement()) {
      statement.execute("CREATE DOCUMENT TYPE Person10228 IF NOT EXISTS");
      statement.execute("CREATE PROPERTY Person10228.id IF NOT EXISTS INTEGER");
      statement.execute("CREATE PROPERTY Person10228.name IF NOT EXISTS STRING");
    }
  }

  /*
   * Issue #10371 over the wire: a catalog query over a relation the emulation does not model is refused with
   * {@code feature_not_supported} (0A000) on both protocols, instead of being answered with an empty result set that reads as an
   * empty catalog. {@code select count(*)} over such a relation used to send no row at all, where PostgreSQL always sends one, and
   * psql's {@code \di}, psqlodbc's {@code SQLPrimaryKeys} and {@code SQLStatistics} reported nothing. The connection goes on
   * working after the refusal, and a statement whose filters are bound parameters is refused at Execute, once the values are in.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  /** psql's {@code \di} lists indexes through pg_index; psqlodbc's SQLPrimaryKeys and SQLStatistics read it the same way. */
  private static final String INDEX_LISTING = "SELECT c.relname, i.indisunique FROM pg_catalog.pg_class c, pg_catalog.pg_index i "
      + "WHERE c.oid = i.indexrelid";

  // Regression: #10371
  @Test
  void anUngroupedCountOverAnUnmodelledRelationIsRefusedNotAnsweredWithNoRow() throws Exception {
    for (final String mode : List.of("extended", "simple")) {
      try (final Connection connection = openJdbcConnection(mode)) {
        assertRefused(mode, connection, "select count(*) from pg_catalog.pg_extension where extname = 'pgcrypto'", "pg_extension");
        assertRefused(mode, connection, "select count(*) from pg_catalog.pg_stat_activity", "pg_stat_activity");
        assertRefused(mode, connection, "select count(*) from pg_catalog.pg_locks", "pg_locks");

        // the connection is still usable, and a modelled relation answers PostgreSQL's one row holding 0
        try (final Statement statement = connection.createStatement(); final ResultSet resultSet = statement.executeQuery(
            "select count(*) from pg_catalog.pg_class where relname = 'Nope10371'")) {
          assertThat(resultSet.next()).as(mode).isTrue();
          assertThat(resultSet.getLong(1)).as(mode).isZero();
          assertThat(resultSet.next()).as(mode).isFalse();
        }
      }
    }
  }

  // Regression: #10371
  /** The index listings were refused while pg_index was not modelled; they are answered now (issue #10473). */
  @Test
  void theIndexListingsPsqlAndOdbcSendAreAnswered() throws Exception {
    for (final String mode : List.of("extended", "simple")) {
      try (final Connection connection = openJdbcConnection(mode)) {
        try (final Statement statement = connection.createStatement()) {
          statement.execute("CREATE DOCUMENT TYPE Listed10473 IF NOT EXISTS");
          statement.execute("CREATE PROPERTY Listed10473.name IF NOT EXISTS STRING");
          statement.execute("CREATE INDEX IF NOT EXISTS ON Listed10473 (name) UNIQUE");
        }

        try (final Statement statement = connection.createStatement(); final ResultSet resultSet = statement.executeQuery(
            INDEX_LISTING + " and c.relname like 'Listed10473%'")) {
          assertThat(resultSet.next()).as(mode).isTrue();
          assertThat(resultSet.getString("relname")).as(mode).isEqualTo("Listed10473[name]");
          assertThat(resultSet.getBoolean("indisunique")).as(mode).isTrue();
          assertThat(resultSet.next()).as(mode).isFalse();
        }
        try (final Statement statement = connection.createStatement(); final ResultSet resultSet = statement.executeQuery(
            "select constraint_type, table_name from information_schema.table_constraints where table_name = 'Listed10473'")) {
          assertThat(resultSet.next()).as(mode).isTrue();
          assertThat(resultSet.getString("constraint_type")).as(mode).isEqualTo("UNIQUE");
          assertThat(resultSet.next()).as(mode).isFalse();
        }
        try (final Statement statement = connection.createStatement(); final ResultSet resultSet = statement.executeQuery(
            "select column_name from information_schema.key_column_usage where table_name = 'Listed10473'")) {
          assertThat(resultSet.next()).as(mode).isTrue();
          assertThat(resultSet.getString("column_name")).as(mode).isEqualTo("name");
          assertThat(resultSet.next()).as(mode).isFalse();
        }
      }
    }
  }

  // Regression: #10371
  /** The extended protocol sees the query at Parse, before the values of its filters arrive: the refusal is final at Execute. */
  @Test
  void aRefusedCatalogQueryWithBoundParametersIsRefusedAtExecute() throws Exception {
    try (final Connection connection = openJdbcConnection("extended");
        final PreparedStatement statement = connection.prepareStatement("select count(*) from pg_catalog.pg_extension where extname = ?")) {
      statement.setString(1, "pgcrypto");

      assertThatThrownBy(statement::executeQuery).isInstanceOf(SQLException.class)
          .satisfies(e -> assertThat(((SQLException) e).getSQLState()).isEqualTo("0A000"))
          .hasMessageContaining("pg_extension");

      // the same statement, run again on the same connection, is refused again rather than hanging or answering empty
      assertThatThrownBy(statement::executeQuery).isInstanceOf(SQLException.class)
          .satisfies(e -> assertThat(((SQLException) e).getSQLState()).isEqualTo("0A000"));
    }
  }

  // Regression: #10371
  /** {@code \d}'s header step reads relhasindex, which follows the indexes the catalog lists (issue #10473). */
  @Test
  void aTableWithAnIndexClaimsOneAndTheCatalogShowsIt() throws Exception {
    try (final Connection connection = openJdbcConnection("extended")) {
      try (final Statement statement = connection.createStatement()) {
        statement.execute("CREATE DOCUMENT TYPE Person10371 IF NOT EXISTS");
        statement.execute("CREATE PROPERTY Person10371.name IF NOT EXISTS STRING");
        statement.execute("CREATE INDEX IF NOT EXISTS ON Person10371 (name) UNIQUE");
      }

      try (final Statement statement = connection.createStatement(); final ResultSet resultSet = statement.executeQuery(
          "select relname, relhasindex from pg_catalog.pg_class where relname = 'Person10371'")) {
        assertThat(resultSet.next()).isTrue();
        assertThat(resultSet.getBoolean("relhasindex")).isTrue();
      }
    }
  }

  private static void assertRefused(final String mode, final Connection connection, final String query, final String relation) {
    assertThatThrownBy(() -> {
      try (final Statement statement = connection.createStatement(); final ResultSet resultSet = statement.executeQuery(query)) {
        resultSet.next();
      }
    }).as("%s: %s", mode, query).isInstanceOf(SQLException.class)
        .satisfies(e -> assertThat(((SQLException) e).getSQLState()).as(mode).isEqualTo("0A000"))
        .hasMessageContaining(relation);
  }

  /*
   * Issue #10473 through the PostgreSQL JDBC driver: {@code DatabaseMetaData.getIndexInfo()} and {@code getPrimaryKeys()}, which
   * Hibernate, Flyway, Liquibase and the BI tools call, are written with {@code information_schema._pg_expandarray()} - a set-returning
   * function - and read its members as {@code (expression).n}. The statements are the driver's own, so these tests hold the shape
   * this catalog has to read; when a release of the driver rewrites them they fail first.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  // Regression: #10473
  @Test
  void getIndexInfoListsTheKeyColumnsOfEveryIndex() throws Exception {
    try (final Connection connection = openJdbcConnection()) {
      createIndexedType(connection);

      final DatabaseMetaData metadata = connection.getMetaData();
      final List<String> rows = new ArrayList<>();
      try (final ResultSet resultSet = metadata.getIndexInfo(null, null, "Meta10473", false, false)) {
        while (resultSet.next())
          rows.add(resultSet.getString("INDEX_NAME") + "|" + resultSet.getInt("ORDINAL_POSITION") + "|"
              + resultSet.getString("COLUMN_NAME") + "|" + resultSet.getBoolean("NON_UNIQUE") + "|"
              + resultSet.getString("ASC_OR_DESC") + "|" + resultSet.getString("TABLE_NAME"));
      }

      // unique first (NON_UNIQUE false), then by name and position, as the driver orders them
      assertThat(rows).containsExactly("Meta10473[id]|1|id|false|A|Meta10473", "Meta10473[name,tag]|1|name|true|A|Meta10473",
          "Meta10473[name,tag]|2|tag|true|A|Meta10473");

      // and only the unique ones when the caller asks for them
      try (final ResultSet resultSet = metadata.getIndexInfo(null, null, "Meta10473", true, false)) {
        assertThat(resultSet.next()).isTrue();
        assertThat(resultSet.getString("INDEX_NAME")).isEqualTo("Meta10473[id]");
        assertThat(resultSet.next()).isFalse();
      }
    }
  }

  // Regression: #10473
  @Test
  void getPrimaryKeysOfATypeWithoutOneAreNone() throws Exception {
    try (final Connection connection = openJdbcConnection()) {
      createIndexedType(connection);

      // ArcadeDB has no primary key: the unique index is not one, and the driver is told so instead of being refused
      try (final ResultSet resultSet = connection.getMetaData().getPrimaryKeys(null, null, "Meta10473")) {
        assertThat(resultSet.next()).isFalse();
      }
    }
  }

  // Regression: #10473
  @Test
  void theTablesAndColumnsStillAnswer() throws Exception {
    try (final Connection connection = openJdbcConnection()) {
      createIndexedType(connection);

      try (final ResultSet resultSet = connection.getMetaData().getTables(null, null, "Meta10473", null)) {
        assertThat(resultSet.next()).isTrue();
        assertThat(resultSet.getString("TABLE_NAME")).isEqualTo("Meta10473");
      }
      try (final ResultSet resultSet = connection.getMetaData().getColumns(null, null, "Meta10473", null)) {
        final List<String> columns = new ArrayList<>();
        while (resultSet.next())
          columns.add(resultSet.getString("COLUMN_NAME"));
        assertThat(columns).containsExactlyInAnyOrder("id", "name", "tag");
      }
    }
  }

  private static void createIndexedType(final Connection connection) throws Exception {
    try (final Statement statement = connection.createStatement()) {
      statement.execute("CREATE DOCUMENT TYPE Meta10473 IF NOT EXISTS");
      statement.execute("CREATE PROPERTY Meta10473.id IF NOT EXISTS INTEGER");
      statement.execute("CREATE PROPERTY Meta10473.name IF NOT EXISTS STRING");
      statement.execute("CREATE PROPERTY Meta10473.tag IF NOT EXISTS STRING");
      statement.execute("CREATE INDEX IF NOT EXISTS ON Meta10473 (id) UNIQUE");
      statement.execute("CREATE INDEX IF NOT EXISTS ON Meta10473 (name, tag) NOTUNIQUE");
    }
  }

  /*
   * Issue #10495 through the PostgreSQL JDBC driver itself: the lookups its {@code TypeInfoCache} sends to name a type it has not met
   * (the OID of a schema-qualified name, the array of a type, the element of an array) are answered from {@link PostgresType} with the
   * values the driver bound, where they were declined and kept on an empty "no such type" answer.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  // Regression: #10495
  @Test
  void theDriverFindsTheOidOfAQualifiedTypeNameThisServerHas() throws Exception {
    try (final Connection connection = openJdbcConnection()) {
      final TypeInfo types = connection.unwrap(PgConnection.class).getTypeInfo();

      // a name the driver has no entry for is looked up in the catalog, with the schema it was given
      assertThat(types.getPGType(types.getPGType("pg_catalog.int4"))).isEqualTo("int4");
      assertThat(types.getPGType("pg_catalog.int4")).isEqualTo(PostgresType.INTEGER.code);
      assertThat(types.getPGType("pg_catalog.text")).isEqualTo(PostgresType.TEXT.code);
    }
  }

  // Regression: #10495
  @Test
  void theDriverFindsTheArrayTypeOfAnElementType() throws Exception {
    try (final Connection connection = openJdbcConnection()) {
      final TypeInfo types = connection.unwrap(PgConnection.class).getTypeInfo();

      assertThat(types.getPGType("pg_catalog.int4[]")).isEqualTo(PostgresType.ARRAY_INT.code);
      assertThat(types.getPGType("pg_catalog.float8[]")).isEqualTo(PostgresType.ARRAY_DOUBLE.code);
    }
  }

  // Regression: #10495
  @Test
  void aTypeNoServerTypeHasIsNotFoundAndNothingFails() throws Exception {
    try (final Connection connection = openJdbcConnection()) {
      final TypeInfo types = connection.unwrap(PgConnection.class).getTypeInfo();

      // "no such type" is the driver's answer for a name the catalog does not have, an unspecified OID, and not an error
      assertThat(types.getPGType("pg_catalog.hstore")).isZero();
      assertThat(types.getPGType("hstore")).isZero();
      assertThat(types.getPGType("nowhere.int4")).isZero();
    }
  }

  // Regression: #10495
  @Test
  void theDriverNamesATypeFromItsOidAndFindsTheElementOfAnArray() throws Exception {
    try (final Connection connection = openJdbcConnection()) {
      final TypeInfo types = connection.unwrap(PgConnection.class).getTypeInfo();

      for (final PostgresType type : PostgresTypeCatalog.types()) {
        assertThat(types.getPGType(type.code)).as("name of oid %d", type.code).isEqualTo(type.typeName);
        if (type.isArrayType())
          assertThat(types.getPGArrayElement(type.code)).as("element of %s", type.typeName).isEqualTo(type.elementCode);
      }
    }
  }

  /*
   * Issue #10591 through the PostgreSQL JDBC driver: the {@code DatabaseMetaData} calls whose honest answer is "there are none" -
   * the foreign keys of a table, the functions and procedures of the database - read relations ArcadeDB never holds a row in
   * ({@code pg_proc}, {@code pg_depend}) and {@code pg_settings}, and were refused with 0A000 since #10371. The statements are the
   * driver's own, so a release of the driver that reads another relation fails these first.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  private static final String NO_ROW_TABLE = "Meta10591";

  // Regression: #10591
  @Test
  void theKeyMetadataCallsAnswerNoForeignKey() throws Exception {
    for (final String mode : List.of("extended", "simple")) {
      try (final Connection connection = openJdbcConnection(mode)) {
        createNoRowType(connection);
        final DatabaseMetaData metadata = connection.getMetaData();

        assertNoRow(mode, metadata.getImportedKeys(null, null, NO_ROW_TABLE));
        assertNoRow(mode, metadata.getExportedKeys(null, null, NO_ROW_TABLE));
        assertNoRow(mode, metadata.getCrossReference(null, null, NO_ROW_TABLE, null, null, NO_ROW_TABLE));
      }
    }
  }

  // Regression: #10591
  @Test
  void theFunctionMetadataCallsAnswerNoFunction() throws Exception {
    for (final String mode : List.of("extended", "simple")) {
      try (final Connection connection = openJdbcConnection(mode)) {
        final DatabaseMetaData metadata = connection.getMetaData();

        assertNoRow(mode, metadata.getProcedures(null, null, "%"));
        assertNoRow(mode, metadata.getProcedureColumns(null, null, "%", "%"));
        assertNoRow(mode, metadata.getFunctions(null, null, "%"));
        assertNoRow(mode, metadata.getFunctionColumns(null, null, "%", "%"));
      }
    }
  }

  // Regression: #10591
  /** The control: the calls that already answered still do. */
  @Test
  void theCallsThatAnsweredBeforeStillDo() throws Exception {
    try (final Connection connection = openJdbcConnection("extended")) {
      createNoRowType(connection);
      final DatabaseMetaData metadata = connection.getMetaData();

      try (final ResultSet resultSet = metadata.getTables(null, null, NO_ROW_TABLE, null)) {
        assertThat(resultSet.next()).isTrue();
        assertThat(resultSet.getString("TABLE_NAME")).isEqualTo(NO_ROW_TABLE);
      }
      try (final ResultSet resultSet = metadata.getIndexInfo(null, null, NO_ROW_TABLE, false, false)) {
        assertThat(resultSet.next()).isTrue();
      }
      assertNoRow("extended", metadata.getPrimaryKeys(null, null, NO_ROW_TABLE));
    }
  }

  // Regression: #10591
  /** pgjdbc reads max_index_keys from pg_settings; SHOW and pg_settings answer the same value for every parameter they share. */
  @Test
  void pgSettingsAnswersWhatShowAnswers() throws Exception {
    for (final String mode : List.of("extended", "simple")) {
      try (final Connection connection = openJdbcConnection(mode); final Statement statement = connection.createStatement()) {
        try (final ResultSet resultSet = statement.executeQuery(
            "SELECT setting FROM pg_catalog.pg_settings WHERE name='max_index_keys'")) {
          assertThat(resultSet.next()).as(mode).isTrue();
          assertThat(resultSet.getInt(1)).as(mode).isEqualTo(32);
          assertThat(resultSet.next()).as(mode).isFalse();
        }
        try (final ResultSet resultSet = statement.executeQuery("SHOW max_index_keys")) {
          assertThat(resultSet.next()).as(mode).isTrue();
          assertThat(resultSet.getString(1)).as(mode).isEqualTo("32");
        }

        // a value this connection set is the one pg_settings lists
        statement.execute("SET application_name = 'app10591'");
        try (final ResultSet resultSet = statement.executeQuery(
            "select setting, context from pg_catalog.pg_settings where name = 'application_name'")) {
          assertThat(resultSet.next()).as(mode).isTrue();
          assertThat(resultSet.getString("setting")).as(mode).isEqualTo("app10591");
          assertThat(resultSet.getString("context")).as(mode).isEqualTo("user");
        }
        try (final ResultSet resultSet = statement.executeQuery(
            "select setting from pg_catalog.pg_settings where name = 'default_transaction_isolation'")) {
          assertThat(resultSet.next()).as(mode).isTrue();
          assertThat(resultSet.getString(1)).as(mode).isEqualTo("read committed");
        }
      }
    }
  }

  // Regression: #10591
  /** psql's {@code \df} and {@code \du}, verbatim from describe.c: the listing a fresh database can produce is produced. */
  @Test
  void psqlListingsOverNoRowRelationsAreAnswered() throws Exception {
    for (final String mode : List.of("extended", "simple")) {
      try (final Connection connection = openJdbcConnection(mode); final Statement statement = connection.createStatement()) {
        try (final ResultSet resultSet = statement.executeQuery(PostgresCatalogRelationsTest.NoRowRelations.PSQL_DF)) {
          assertThat(resultSet.getMetaData().getColumnLabel(2)).as(mode).isEqualTo("Name");
          assertThat(resultSet.next()).as(mode).isFalse();
        }
        try (final ResultSet resultSet = statement.executeQuery(PostgresCatalogRelationsTest.NoRowRelations.PSQL_DU)) {
          assertThat(resultSet.next()).as(mode).isTrue();
          assertThat(resultSet.getString("rolname")).as(mode).isEqualTo("root");
          assertThat(resultSet.getArray("memberof").getArray()).as(mode).isEqualTo(new String[0]);
          assertThat(resultSet.next()).as(mode).isFalse();
        }
        // pg_depend is not among them since #10842: it lists the dependencies of the indexes, which this database has
        for (final String relation : List.of("pg_proc", "pg_trigger", "pg_auth_members", "pg_sequence", "pg_rewrite"))
          try (final ResultSet resultSet = statement.executeQuery("select count(*) from pg_catalog." + relation)) {
            assertThat(resultSet.next()).as("%s %s", mode, relation).isTrue();
            assertThat(resultSet.getLong(1)).as("%s %s", mode, relation).isZero();
          }
      }
    }
  }

  // Regression: #10591
  /** The refusal of #10371 stands for a relation whose absence would be a lie. */
  @Test
  void aRelationThatHoldsRowsInPostgresqlIsStillRefused() throws Exception {
    try (final Connection connection = openJdbcConnection("extended"); final Statement statement = connection.createStatement()) {
      assertThatThrownBy(() -> {
        try (final ResultSet resultSet = statement.executeQuery("select count(*) from pg_catalog.pg_stat_activity")) {
          resultSet.next();
        }
      }).isInstanceOf(SQLException.class).satisfies(e -> assertThat(((SQLException) e).getSQLState()).isEqualTo("0A000"))
          .hasMessageContaining("pg_stat_activity");
    }
  }

  private static void createNoRowType(final Connection connection) throws SQLException {
    try (final Statement statement = connection.createStatement()) {
      statement.execute("CREATE DOCUMENT TYPE " + NO_ROW_TABLE + " IF NOT EXISTS");
      statement.execute("CREATE PROPERTY " + NO_ROW_TABLE + ".name IF NOT EXISTS STRING");
      statement.execute("CREATE PROPERTY " + NO_ROW_TABLE + ".age IF NOT EXISTS INTEGER");
      statement.execute("CREATE INDEX IF NOT EXISTS ON " + NO_ROW_TABLE + " (name) UNIQUE");
    }
  }

  /*
   * Issue #10691 over the wire: the JDBC driver's own {@code getTables} classifies a relation by its {@code relkind}, so a
   * materialized view it used to report as a {@code TABLE} is now a {@code MATERIALIZED VIEW}, and the report's
   * {@code pg_matviews} is answered rather than refused.
   */
  // Regression: #10691
  @Test
  void theDriverTellsAMaterializedViewFromATable() throws Exception {
    try (final Connection connection = openJdbcConnection()) {
      createViewSchema(connection);

      assertThat(tables(connection, "TABLE")).contains("Person10691").doesNotContain("PersonView10691");
      assertThat(tables(connection, "MATERIALIZED VIEW")).containsExactly("PersonView10691");
    }
  }

  // Regression: #10691
  @Test
  void pgMatviewsAnswersTheViewAndItsQuery() throws Exception {
    try (final Connection connection = openJdbcConnection()) {
      createViewSchema(connection);

      try (final Statement statement = connection.createStatement();
          final ResultSet resultSet = statement.executeQuery("select matviewname, definition from pg_matviews")) {
        assertThat(resultSet.next()).isTrue();
        assertThat(resultSet.getString("matviewname")).isEqualTo("PersonView10691");
        assertThat(resultSet.getString("definition")).isEqualTo("SELECT name FROM Person10691");
        assertThat(resultSet.next()).isFalse();
      }
    }
  }

  private static List<String> tables(final Connection connection, final String type) throws Exception {
    final List<String> tables = new ArrayList<>();
    try (final ResultSet resultSet = connection.getMetaData().getTables(null, null, "%", new String[] { type })) {
      while (resultSet.next()) {
        assertThat(resultSet.getString("TABLE_TYPE")).isEqualTo(type);
        tables.add(resultSet.getString("TABLE_NAME"));
      }
    }
    return tables;
  }

  private void createViewSchema(final Connection connection) throws Exception {
    try (final Statement statement = connection.createStatement()) {
      statement.execute("CREATE VERTEX TYPE Person10691 IF NOT EXISTS");
      statement.execute("CREATE PROPERTY Person10691.name IF NOT EXISTS STRING");
      statement.execute("INSERT INTO Person10691 SET name = 'a'");
      statement.execute("CREATE MATERIALIZED VIEW IF NOT EXISTS PersonView10691 AS SELECT name FROM Person10691");
    }
  }

  /*
   * Issue #10695: a statement whose TEXT contained "pg_catalog" or "pg_type" anywhere - a string literal, a column name, a word
   * inside a longer one - was never executed on the Postgres wire and was acknowledged as a success: an INSERT answered
   * {@code INSERT 0 0} and stored nothing, a SELECT answered no rows. Every value of a corpus holding each relation name the
   * catalog knows is written, updated, read back and deleted through both protocols, and must round-trip.
   */
  private static final List<String> CORPUS = List.of("plain", "pg_catalog", "PG_CATALOG", "xxpg_typexx", "pg_type", "pg_class",
      "information_schema", "pg_", "see the pg_catalog docs", "pg_catalog.pg_type");

  // Regression: #10695
  @Test
  void aValueNamingACatalogRelationRoundTripsThroughEveryStatement() throws Exception {
    for (final String mode : List.of("extended", "simple")) {
      final String type = "Note10695" + mode;
      try (final Connection connection = openJdbcConnection(mode); final Statement statement = connection.createStatement()) {
        statement.execute("CREATE DOCUMENT TYPE " + type);
        statement.execute("CREATE PROPERTY " + type + ".body STRING");

        // ArcadeDB's INSERT returns the record it stored, so the evidence of the write is reading it back
        for (final String value : CORPUS)
          statement.execute("INSERT INTO " + type + " SET body = '" + value + "'");
        assertThat(bodies(statement, "SELECT body FROM " + type + " ORDER BY body")).as(mode)
            .containsExactlyInAnyOrderElementsOf(CORPUS);

        // a read whose filter holds the word reaches the rows
        assertThat(bodies(statement, "SELECT body FROM " + type + " WHERE body LIKE '%pg_catalog%'")).as(mode)
            .containsExactlyInAnyOrder("pg_catalog", "see the pg_catalog docs", "pg_catalog.pg_type");

        // an UPDATE and a DELETE whose literals hold the words are executed
        statement.execute("UPDATE " + type + " SET body = 'pg_catalog rules' WHERE body = 'plain'");
        statement.execute("DELETE FROM " + type + " WHERE body = 'pg_type'");
        assertThat(bodies(statement, "SELECT body FROM " + type + " WHERE body = 'pg_catalog rules'")).as(mode)
            .containsExactly("pg_catalog rules");
        assertThat(bodies(statement, "SELECT body FROM " + type + " WHERE body = 'pg_type'")).as(mode).isEmpty();
      }
    }
  }

  // Regression: #10695
  /** A column the user called pg_type is theirs: creating, writing and reading it is not a catalog query. */
  @Test
  void aColumnCalledLikeACatalogRelationIsTheUsers() throws Exception {
    for (final String mode : List.of("extended", "simple")) {
      final String type = "Typed10695" + mode;
      try (final Connection connection = openJdbcConnection(mode); final Statement statement = connection.createStatement()) {
        statement.execute("CREATE DOCUMENT TYPE " + type);
        statement.execute("CREATE PROPERTY " + type + ".pg_type STRING");
        statement.execute("INSERT INTO " + type + " SET pg_type = 'v'");
        try (final ResultSet resultSet = statement.executeQuery("SELECT pg_type FROM " + type)) {
          assertThat(resultSet.next()).as(mode).isTrue();
          assertThat(resultSet.getString(1)).as(mode).isEqualTo("v");
          assertThat(resultSet.next()).as(mode).isFalse();
        }
      }
    }
  }

  // Regression: #10695
  /** The value bound as a parameter was never at risk; it still is not, and a prepared INSERT with an inlined literal stores too. */
  @Test
  void aPreparedInsertStoresWhetherTheWordIsBoundOrInlined() throws Exception {
    try (final Connection connection = openJdbcConnection("extended")) {
      try (final Statement statement = connection.createStatement()) {
        statement.execute("CREATE DOCUMENT TYPE Prepared10695");
        statement.execute("CREATE PROPERTY Prepared10695.body STRING");
      }
      try (final PreparedStatement insert = connection.prepareStatement("INSERT INTO Prepared10695 SET body = ?")) {
        insert.setString(1, "bound pg_catalog");
        insert.execute();
      }
      try (final PreparedStatement insert = connection.prepareStatement("INSERT INTO Prepared10695 SET body = 'jdbc pg_catalog inline'")) {
        insert.execute();
      }
      try (final Statement statement = connection.createStatement()) {
        assertThat(bodies(statement, "SELECT body FROM Prepared10695")).containsExactlyInAnyOrder("bound pg_catalog",
            "jdbc pg_catalog inline");
      }
    }
  }

  // Regression: #10695
  /** A FROM-less probe the client qualifies with pg_catalog answers PostgreSQL's one row, not the empty set it used to get. */
  @Test
  void aQualifiedSessionProbeAnswersOneRow() throws Exception {
    for (final String mode : List.of("extended", "simple")) {
      try (final Connection connection = openJdbcConnection(mode); final Statement statement = connection.createStatement()) {
        final int pid;
        try (final ResultSet resultSet = statement.executeQuery("select pg_backend_pid()")) {
          assertThat(resultSet.next()).as(mode).isTrue();
          pid = resultSet.getInt(1);
        }
        try (final ResultSet resultSet = statement.executeQuery("select pg_catalog.pg_backend_pid()")) {
          assertThat(resultSet.next()).as(mode).isTrue();
          assertThat(resultSet.getInt(1)).as(mode).isEqualTo(pid);
          assertThat(resultSet.next()).as(mode).isFalse();
        }
        try (final ResultSet resultSet = statement.executeQuery("SELECT pg_catalog.set_config('search_path', '', false)")) {
          assertThat(resultSet.next()).as(mode).isTrue();
          assertThat(resultSet.getString(1)).as(mode).isEmpty();
          assertThat(resultSet.next()).as(mode).isFalse();
        }
        try (final ResultSet resultSet = statement.executeQuery("/* app */ SELECT pg_catalog.set_config('search_path', '', false)")) {
          assertThat(resultSet.next()).as(mode).isTrue();
          assertThat(resultSet.getString(1)).as(mode).isEmpty();
          assertThat(resultSet.next()).as(mode).isFalse();
        }
        try (final ResultSet resultSet = statement.executeQuery("SELECT pg_catalog.pg_is_in_recovery()")) {
          assertThat(resultSet.next()).as(mode).isTrue();
          assertThat(resultSet.getBoolean(1)).as(mode).isFalse();
          assertThat(resultSet.next()).as(mode).isFalse();
        }
      }
    }
  }

  private static List<String> bodies(final Statement statement, final String query) throws Exception {
    final List<String> bodies = new ArrayList<>();
    try (final ResultSet resultSet = statement.executeQuery(query)) {
      while (resultSet.next())
        bodies.add(resultSet.getString(1));
    }
    return bodies;
  }

  /*
   * Issue #10711 over the wire, with pgjdbc.
   * <ul>
   * <li>#10592: {@code createArrayOf("varchar", ...)} binds a parameter of type 1015, {@code varchar[]}, which this protocol neither
   * listed in pg_type nor could read: the statement failed on a type the catalog itself named as varchar's array type.</li>
   * <li>#10745: a client that reads pg_namespace on its own finds the schema every type names in {@code typnamespace}.</li>
   * </ul>
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  // Regression: #10711
  @Test
  void aVarcharArrayParameterIsBound() throws Exception {
    try (final Connection connection = openJdbcConnection()) {
      try (final Statement statement = connection.createStatement()) {
        statement.execute("CREATE DOCUMENT TYPE Item10711 IF NOT EXISTS");
        statement.execute("CREATE PROPERTY Item10711.name IF NOT EXISTS STRING");
        statement.execute("INSERT INTO Item10711 SET name = 'a'");
        statement.execute("INSERT INTO Item10711 SET name = 'b'");
        statement.execute("INSERT INTO Item10711 SET name = 'c'");
      }

      final Array names = connection.createArrayOf("varchar", new String[] { "a", "c" });
      try (final PreparedStatement statement = connection.prepareStatement(
          "SELECT name FROM Item10711 WHERE name IN ? ORDER BY name")) {
        statement.setArray(1, names);
        final List<String> found = new ArrayList<>();
        try (final ResultSet resultSet = statement.executeQuery()) {
          while (resultSet.next())
            found.add(resultSet.getString("name"));
        }
        assertThat(found).containsExactly("a", "c");
      }
    }
  }

  // Regression: #10711
  @Test
  void theNamespaceOfTheTypesIsASchemaOfTheCatalog() throws Exception {
    try (final Connection connection = openJdbcConnection();
        final Statement statement = connection.createStatement()) {
      final List<String> schemas = new ArrayList<>();
      try (final ResultSet resultSet = statement.executeQuery("select nspname from pg_namespace where oid = "
          + "(select typnamespace from pg_type where typname = 'int4')")) {
        while (resultSet.next())
          schemas.add(resultSet.getString(1));
      }
      assertThat(schemas).containsExactly("pg_catalog");
    }
  }

  /*
   * Issue #10842 through the PostgreSQL JDBC driver: {@code pg_depend} lists the dependencies of the indexes of a table, typed as
   * PostgreSQL types them, while the {@code DatabaseMetaData} calls that join it still answer no foreign key; and a catalog relation
   * this server does not model is still refused with 0A000.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  private static final String DEPEND_TABLE = "Dep10842";

  // Regression: #10842
  @Test
  void pgDependListsTheDependenciesOfTheIndexesOfATable() throws Exception {
    for (final String mode : List.of("extended", "simple")) {
      try (final Connection connection = openJdbcConnection(mode)) {
        createDependType(connection);
        try (final Statement statement = connection.createStatement();
            final ResultSet resultSet = statement.executeQuery("""
                SELECT c.relname, d.refobjsubid, d.deptype FROM pg_catalog.pg_depend d
                JOIN pg_catalog.pg_class c ON c.oid = d.objid
                JOIN pg_catalog.pg_class t ON t.oid = d.refobjid
                WHERE d.classid = 1259 AND d.refclassid = 1259 AND t.relname = '%s'""".formatted(DEPEND_TABLE))) {
          assertThat(resultSet.getMetaData().getColumnType(2)).as(mode).isEqualTo(Types.INTEGER);
          final Set<String> rows = new TreeSet<>();
          while (resultSet.next()) {
            assertThat(resultSet.getInt(2)).as("the column the index depends on").isPositive();
            rows.add(resultSet.getString(1) + ":" + resultSet.getString(3));
          }
          // the plain index depends on its column; the unique one depends on its constraint, not on the table
          assertThat(rows).as(mode).containsExactly(DEPEND_TABLE + "[age]:a");
        }

        try (final Statement statement = connection.createStatement();
            final ResultSet resultSet = statement.executeQuery(
                "SELECT count(*) FROM pg_catalog.pg_depend WHERE refclassid = 2606 AND deptype = 'i'")) {
          assertThat(resultSet.next()).isTrue();
          assertThat(resultSet.getLong(1)).as(mode).isGreaterThanOrEqualTo(1L);
        }

        final DatabaseMetaData metadata = connection.getMetaData();
        assertNoRow(mode, metadata.getImportedKeys(null, null, DEPEND_TABLE));
        assertNoRow(mode, metadata.getExportedKeys(null, null, DEPEND_TABLE));
      }
    }
  }

  // Regression: #10842
  @Test
  void anUnmodelledRelationIsStillRefused() throws Exception {
    try (final Connection connection = openJdbcConnection("extended"); final Statement statement = connection.createStatement()) {
      assertThatThrownBy(() -> statement.executeQuery("SELECT extname FROM pg_catalog.pg_extension")).isInstanceOf(SQLException.class)
          .satisfies(e -> assertThat(((SQLException) e).getSQLState()).isEqualTo("0A000"))
          .hasMessageContaining("pg_extension");
    }
  }

  private static void createDependType(final Connection connection) throws SQLException {
    try (final Statement statement = connection.createStatement()) {
      statement.execute("CREATE DOCUMENT TYPE " + DEPEND_TABLE + " IF NOT EXISTS");
      statement.execute("CREATE PROPERTY " + DEPEND_TABLE + ".name IF NOT EXISTS STRING");
      statement.execute("CREATE PROPERTY " + DEPEND_TABLE + ".age IF NOT EXISTS INTEGER");
      statement.execute("CREATE INDEX IF NOT EXISTS ON " + DEPEND_TABLE + " (name) UNIQUE");
      statement.execute("CREATE INDEX IF NOT EXISTS ON " + DEPEND_TABLE + " (age) NOTUNIQUE");
    }
  }

  /*
   * Issues #10869 and #10870 through the PostgreSQL JDBC driver:
   * <ul>
   *   <li>a catalog query that matches no row announces the columns it projected, typed as when it matches (#10869);</li>
   *   <li>an array parameter sent in text format keeps its empty-string and NULL elements, and an untyped JSON document is not read
   *   as a list (#10870).</li>
   * </ul>
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  // Regression: #10869, #10870
  @Test
  void aTypeLookupThatFindsNothingStillHasItsColumns() throws Exception {
    try (final Connection connection = connect(new Properties()); final Statement statement = connection.createStatement()) {
      for (final String filter : List.of("typname = 'nosuchtype'", "oid = 999999", "typname = 'int4'")) {
        try (final ResultSet rs = statement.executeQuery("SELECT oid, typname, typlen FROM pg_type WHERE " + filter)) {
          final ResultSetMetaData metaData = rs.getMetaData();
          assertThat(metaData.getColumnCount()).as(filter).isEqualTo(3);
          assertThat(metaData.getColumnTypeName(1)).as(filter).isEqualTo("int4");
          assertThat(metaData.getColumnTypeName(2)).as(filter).isEqualTo("varchar");
          assertThat(metaData.getColumnTypeName(3)).as(filter).isEqualTo("int4");
          assertThat(rs.findColumn("typname")).isEqualTo(2);
        }
      }
    }
  }

  // Regression: #10869, #10870
  @Test
  void anEmptyRelationAnswerIsTypedLikeAFullOne() throws Exception {
    createTagsType();
    try (final Connection connection = connect(new Properties()); final Statement statement = connection.createStatement()) {
      for (final String relname : List.of("T10870", "NoSuchRelation"))
        try (final ResultSet rs = statement.executeQuery(
            "SELECT oid, relnatts, relhasindex, relkind FROM pg_class WHERE relname = '" + relname + "'")) {
          final ResultSetMetaData metaData = rs.getMetaData();
          assertThat(metaData.getColumnTypeName(1)).as(relname).isEqualTo("int4");
          assertThat(metaData.getColumnTypeName(2)).as(relname).isEqualTo("int4");
          assertThat(metaData.getColumnTypeName(3)).as(relname).isEqualTo("bool");
          assertThat(metaData.getColumnTypeName(4)).as(relname).isEqualTo("varchar");
        }
    }
  }

  // Regression: #10869, #10870
  @Test
  void aTextFormatArrayParameterKeepsEveryElement() throws Exception {
    createTagsType();
    final Properties properties = new Properties();
    // text format for every parameter, which is what a client that does not negotiate binary sends
    properties.setProperty("binaryTransfer", "false");
    try (final Connection connection = connect(properties)) {
      try (final PreparedStatement ps = connection.prepareStatement("INSERT INTO T10870 SET id = ?, tags = ?")) {
        ps.setInt(1, 1);
        ps.setArray(2, connection.createArrayOf("text", new String[] { "a", "", "b", null, "NULL" }));
        ps.execute();

        // an untyped parameter is a list only when it is an array literal
        ps.setInt(1, 2);
        ps.setObject(2, "{\"a\",\"\",NULL}", Types.OTHER);
        ps.execute();

        ps.setInt(1, 3);
        ps.setObject(2, "{\"k\": 1}", Types.OTHER);
        ps.execute();
      }
    }

    final Database database = getServerDatabase(0, getDatabaseName());
    assertThat(tagsOf(database, 1)).containsExactly("a", "", "b", null, "NULL");
    assertThat(tagsOf(database, 2)).containsExactly("a", "", null);
    try (final com.arcadedb.query.sql.executor.ResultSet rs = database.query("sql", "SELECT tags FROM T10870 WHERE id = 3")) {
      assertThat(rs.next().<Object>getProperty("tags")).isEqualTo("{\"k\": 1}");
    }
  }

  private static List<Object> tagsOf(final Database database, final int id) {
    try (final com.arcadedb.query.sql.executor.ResultSet rs = database.query("sql", "SELECT tags FROM T10870 WHERE id = ?", id)) {
      return rs.next().getProperty("tags");
    }
  }

  private Connection connect(final Properties properties) throws Exception {
    properties.setProperty("user", "root");
    properties.setProperty("password", DEFAULT_PASSWORD_FOR_TESTS);
    properties.setProperty("sslmode", "disable");
    return DriverManager.getConnection(getServerPostgresJdbcUrl(), properties);
  }

  private void createTagsType() {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.transaction(() -> database.getSchema().getOrCreateDocumentType("T10870").getOrCreateProperty("id", Type.INTEGER));
  }

  /*
   * Issue #10882: the recogniser of the JDBC driver's pg_type element self-join decided on the raw text of the query, string
   * literals included, so a query over the user's own type whose literal spelled the join out was answered with the element of
   * array 1007 instead of being executed. It also answered a column of the array alias as if it were the element's.
   */
  private static final String LITERAL = "pg_ typelem = e.oid and t.oid = 1007";

  // Regression: #10882
  @Test
  void aLiteralSpellingTheJoinOutIsExecutedOverTheUsersType() throws Exception {
    for (final String mode : List.of("extended", "simple")) {
      final String type = "Note10882" + mode;
      try (final Connection connection = openJdbcConnection(mode); final Statement statement = connection.createStatement()) {
        statement.execute("CREATE DOCUMENT TYPE " + type);
        statement.execute("CREATE PROPERTY " + type + ".typname STRING");
        statement.execute("INSERT INTO " + type + " SET typname = '" + LITERAL + "'");
        statement.execute("INSERT INTO " + type + " SET typname = 'other'");

        assertThat(rows(statement, "SELECT typname FROM " + type + " WHERE typname = '" + LITERAL + "'")).as(mode)
            .containsExactly(List.of(LITERAL));
      }
    }
  }

  // Regression: #10882
  @Test
  void theDriverSelfJoinStillDescribesTheElementAndTheArrayAliasStillTheArray() throws Exception {
    for (final String mode : List.of("extended", "simple")) {
      try (final Connection connection = openJdbcConnection(mode); final Statement statement = connection.createStatement()) {
        assertThat(rows(statement,
            "SELECT e.typdelim, e.typname FROM pg_catalog.pg_type t, pg_catalog.pg_type e WHERE t.oid = 1007 AND t.typelem = e.oid"))
            .as(mode).containsExactly(List.of(",", "int4"));

        // a column of the array alias is the array's, not the element's
        assertThat(rows(statement, "SELECT t.typname, e.typname FROM pg_type t, pg_type e WHERE t.oid = 1007 AND t.typelem = e.oid"))
            .as(mode).containsExactly(List.of("_int4", "int4"));
      }
    }
  }

  private static List<List<String>> rows(final Statement statement, final String query) throws Exception {
    final List<List<String>> rows = new ArrayList<>();
    try (final ResultSet resultSet = statement.executeQuery(query)) {
      final int columns = resultSet.getMetaData().getColumnCount();
      while (resultSet.next()) {
        final List<String> row = new ArrayList<>(columns);
        for (int i = 1; i <= columns; i++)
          row.add(resultSet.getString(i));
        rows.add(row);
      }
    }
    return rows;
  }

  /*
   * Issue #10883 over the wire: a FROM-less SELECT naming {@code pg_catalog} that the catalog cannot evaluate is refused with
   * {@code feature_not_supported} (0A000) on both protocols, instead of being acknowledged as a success with no row. The connection
   * goes on working after the refusal, the probes the catalog evaluates still answer their one row, and a probe whose argument is a
   * bound parameter is refused at Execute, once the value is in.
   */
  // Regression: #10883
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

  // Regression: #10883
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

  /*
   * Issue #10922: a named prepared catalog query with no bound parameters kept answering the schema as it stood at Parse,
   * so a type created after Parse was missing and a type dropped after Parse was still listed.
   */
  private static final String PREPARED_QUERY = "SELECT relname FROM pg_catalog.pg_class";
  private static final String PREPARED_TYPE  = "Zzz10922New";

  // Regression: #10922
  @Test
  @DisplayName("[#10922] a named prepared catalog statement sees a type created and dropped after Parse")
  void namedStatementTracksSchemaChanges() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      sendStartupMessage(out, "root", getDatabaseName());
      readMessage(in);
      sendPasswordMessage(out, DEFAULT_PASSWORD_FOR_TESTS);
      readMessageOfType(in, 'Z');

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        sendParse(out, "S1", PREPARED_QUERY);
        sendBind(out, "P1", "S1");
        sendExecute(out, "P1");
        sendSync(out);
        assertThat(listsType(readUntilReadyForQuery(in))).isFalse();

        sendSimpleQuery(out, "CREATE VERTEX TYPE " + PREPARED_TYPE);
        readUntilReadyForQuery(in);

        // Bind + Execute of the already-parsed statement, no re-Parse
        sendBind(out, "P2", "S1");
        sendExecute(out, "P2");
        sendSync(out);
        assertThat(listsType(readUntilReadyForQuery(in))).as("type created after Parse").isTrue();

        sendSimpleQuery(out, "DROP TYPE " + PREPARED_TYPE);
        readUntilReadyForQuery(in);

        sendBind(out, "P3", "S1");
        sendExecute(out, "P3");
        sendSync(out);
        assertThat(listsType(readUntilReadyForQuery(in))).as("type dropped after Parse").isFalse();
      });
    }
  }

  // Regression: #10922
  @Test
  @DisplayName("[#10922] a partially fetched catalog portal keeps slicing its own answer across a schema change")
  void partiallyFetchedPortalKeepsItsAnswer() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      sendStartupMessage(out, "root", getDatabaseName());
      readMessage(in);
      sendPasswordMessage(out, DEFAULT_PASSWORD_FOR_TESTS);
      readMessageOfType(in, 'Z');

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        sendSimpleQuery(out, "BEGIN");
        readUntilReadyForQuery(in);
        sendParse(out, "S2", PREPARED_QUERY);
        sendBind(out, "P1", "S2");
        sendExecute(out, "P1", 2);
        sendSync(out);
        final List<WireMessage> first = readUntilReadyForQuery(in);
        assertThat(first.stream().filter(m -> m.type() == 'D').count()).isEqualTo(2);

        sendSimpleQuery(out, "CREATE VERTEX TYPE " + PREPARED_TYPE + "B");
        readUntilReadyForQuery(in);

        // The resumed cursor continues the answer it started on: it neither restarts nor re-reads
        sendExecute(out, "P1", 0);
        sendSync(out);
        final List<WireMessage> rest = readUntilReadyForQuery(in);
        assertThat(rest.stream().filter(m -> m.type() == 'D').count()).isGreaterThan(0);
        assertThat(listsType(rest)).isFalse();

        sendSimpleQuery(out, "ROLLBACK");
        readUntilReadyForQuery(in);
        sendSimpleQuery(out, "DROP TYPE " + PREPARED_TYPE + "B");
        readUntilReadyForQuery(in);
      });
    }
  }

  private static boolean listsType(final List<WireMessage> messages) {
    for (final WireMessage message : messages)
      if (message.type() == 'D' && new String(message.body(), StandardCharsets.UTF_8).contains(PREPARED_TYPE))
        return true;
    return false;
  }

  private static void assertNoRow(final String mode, final ResultSet resultSet) throws SQLException {
    try (resultSet) {
      assertThat(resultSet.next()).as(mode).isFalse();
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
