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

import java.sql.Array;
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
 * Issue #10711 over the wire, with pgjdbc.
 * <ul>
 * <li>#10592: {@code createArrayOf("varchar", ...)} binds a parameter of type 1015, {@code varchar[]}, which this protocol neither
 * listed in pg_type nor could read: the statement failed on a type the catalog itself named as varchar's array type.</li>
 * <li>#10745: a client that reads pg_namespace on its own finds the schema every type names in {@code typnamespace}.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10711CatalogReferencesIT extends PostgresWireProtocolTestBase {

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
