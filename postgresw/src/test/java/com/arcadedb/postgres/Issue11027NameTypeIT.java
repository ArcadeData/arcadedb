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
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11027 through the PostgreSQL JDBC driver: {@code pg_type} had no row for {@code name} (OID 19), which pgjdbc reads the
 * typlen of to answer {@code getMax*NameLength()} and {@code getClientInfoProperties()}, so each of them threw.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11027NameTypeIT extends PostgresWireProtocolTestBase {

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

  @Test
  void getClientInfoPropertiesAnswersItsRows() throws Exception {
    try (final Connection connection = open();
        final ResultSet rows = connection.getMetaData().getClientInfoProperties()) {
      // the rows pgjdbc builds depend on the server version it is told; what matters is that it no longer throws
      assertThat(rows.next()).isTrue();
      assertThat(rows.getString("NAME")).isEqualTo("ApplicationName");
    }
  }

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
}
