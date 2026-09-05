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
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
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
class Issue7178ArrowAdbcBootstrapIT extends PostgresWireProtocolTestBase {

  /** Verbatim from arrow-adbc {@code c/driver/postgresql/database.cc}, {@code RebuildTypeResolver}. */
  private static final String ADBC_PG_TYPE_QUERY = //
      "SELECT oid, typname, typreceive, typbasetype, typrelid, typarray FROM "
          + "pg_catalog.pg_type WHERE (typreceive != 0 OR typsend != 0) AND typtype != 'r' AND "
          + "typreceive::TEXT != 'array_recv'";

  /** Verbatim from the same function, {@code kColumnsQuery}. */
  private static final String ADBC_PG_ATTRIBUTE_QUERY = //
      "SELECT attrelid, attname, atttypid FROM pg_catalog.pg_attribute ORDER BY attrelid, attnum";

  @Test
  void theArrowAdbcTypeResolverQueryAnswersWithSixColumns() throws Exception {
    try (final Connection connection = openJdbcConnection();
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

  @Test
  void theArrowAdbcPgAttributeQueryAnswersWithThreeColumns() throws Exception {
    try (final Connection connection = openJdbcConnection();
        final Statement statement = connection.createStatement();
        final ResultSet resultSet = statement.executeQuery(ADBC_PG_ATTRIBUTE_QUERY)) {

      assertThat(resultSet.getMetaData().getColumnCount())
          .as("the ADBC driver rejects anything but 3 columns from its pg_attribute bootstrap")
          .isEqualTo(3);
    }
  }

  @Test
  void theArrowAdbcVersionQueryAnswersWithOneRowAndOneColumn() throws Exception {
    try (final Connection connection = openJdbcConnection();
        final Statement statement = connection.createStatement();
        final ResultSet resultSet = statement.executeQuery("SELECT version();")) {

      assertThat(resultSet.getMetaData().getColumnCount()).isEqualTo(1);
      assertThat(resultSet.next()).isTrue();
      assertThat(resultSet.getString(1)).startsWith("PostgreSQL ");
      assertThat(resultSet.next()).as("exactly one row").isFalse();
    }
  }

  private Connection openJdbcConnection() throws Exception {
    Class.forName("org.postgresql.Driver");
    final Properties properties = new Properties();
    properties.setProperty("user", "root");
    properties.setProperty("password", DEFAULT_PASSWORD_FOR_TESTS);
    properties.setProperty("ssl", "false");
    properties.setProperty("sslMode", "disable");
    properties.setProperty("preferQueryMode", "simple");
    return DriverManager.getConnection("jdbc:postgresql://localhost:5432/" + getDatabaseName(), properties);
  }
}
