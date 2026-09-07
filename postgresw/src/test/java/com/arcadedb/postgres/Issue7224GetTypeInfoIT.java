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
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #7224 through the PostgreSQL JDBC driver itself.
 * <p>
 * {@link DatabaseMetaData#getTypeInfo()} joins pg_type to pg_namespace, which the catalog answered as a question
 * about SCHEMAS: one fabricated row whose type columns were all NULL. The driver reported a single type with no
 * name, silently - any tool populating a type picker or validating a column type against the catalog got that.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7224GetTypeInfoIT extends PostgresWireProtocolTestBase {

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
