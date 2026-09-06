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
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
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
class Issue7180ArrowAdbcTableSchemaIT extends PostgresWireProtocolTestBase {

  /** Verbatim from arrow-adbc {@code c/driver/postgresql/connection.cc}, {@code GetTableSchema}. */
  private static final String ADBC_GET_TABLE_SCHEMA = //
      "SELECT attname, atttypid FROM pg_catalog.pg_class AS cls "
          + "INNER JOIN pg_catalog.pg_attribute AS attr ON cls.oid = attr.attrelid "
          + "INNER JOIN pg_catalog.pg_type AS typ ON attr.atttypid = typ.oid "
          + "WHERE attr.attnum >= 0 AND cls.oid = $1::regclass::oid "
          + "ORDER BY attr.attnum";

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

  @Test
  void aTableTheDriverAsksAboutThatDoesNotExistDescribesNoColumnsRatherThanEveryColumn() throws Exception {
    try (final Connection connection = openJdbcConnection()) {
      createSchema(connection);

      assertThat(tableSchemaOf(connection, "\"no_such_table_7180\""))
          .as("an unresolvable regclass name must not fall back to describing every column of every table")
          .isEmpty();
    }
  }

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
    return DriverManager.getConnection("jdbc:postgresql://localhost:5432/" + getDatabaseName(), properties);
  }
}
