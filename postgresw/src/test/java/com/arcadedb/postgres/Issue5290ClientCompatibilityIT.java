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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

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
public class Issue5290ClientCompatibilityIT extends BaseGraphServerTest {

  private static final int POSTGRES_PORT = 5432;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("Postgres:com.arcadedb.postgres.PostgresProtocolPlugin");
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  @Override
  protected String getDatabaseName() {
    return "postgresdb";
  }

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

    assertThat(catalog).hasSize(PostgresType.values().length);
    for (final PostgresType type : PostgresType.values())
      assertThat(catalog).as(type.name()).containsEntry(type.code, type.typeName);
  }

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
    return DriverManager.getConnection("jdbc:postgresql://localhost:" + POSTGRES_PORT + "/" + getDatabaseName(), properties);
  }
}
