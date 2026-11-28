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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The columns a {@code select from <type>} announces (RowDescription) for rows that are plain records, which take the key set of
 * the properties already read for their values as their column list (issue #13012): their set and their order are the record's,
 * the union of the rows' for a type whose records do not all hold the same properties, and the values sent under them are the
 * record's own.
 */
class PostgresRecordRowColumnsIT extends PostgresWireProtocolTestBase {
  private static final String TYPE = "Plain13012";

  @BeforeEach
  void populate() {
    final Database database = getServerDatabase(0, getDatabaseName());
    // schemaless: the columns are the properties the records happen to hold, in the order each record stores them
    database.getSchema().createDocumentType(TYPE, 1);
    database.transaction(() -> {
      database.newDocument(TYPE).set("zeta", 1).set("alpha", "a").set("mid", true).set("nothing", null).save();
      database.newDocument(TYPE).set("zeta", 2).set("alpha", "b").set("mid", false).set("late", 3.5d).save();
    });
  }

  // Regression: #13012
  @Test
  void selectFromATypeAnnouncesTheColumnsOfItsRecordsInTheOrderTheRecordsStoreThem() throws Exception {
    try (final Connection connection = openJdbcConnection(); final Statement statement = connection.createStatement();
        final ResultSet resultSet = statement.executeQuery("select from " + TYPE)) {
      // the first record's properties in its order, then the property only the second one holds, then the system columns
      assertThat(columnNames(resultSet.getMetaData())).containsExactly("zeta", "alpha", "mid", "nothing", "late", "@rid", "@type", "@cat");

      assertThat(resultSet.next()).isTrue();
      assertThat(resultSet.getInt("zeta")).isEqualTo(1);
      assertThat(resultSet.getString("alpha")).isEqualTo("a");
      assertThat(resultSet.getBoolean("mid")).isTrue();
      assertThat(resultSet.getString("nothing")).isNull();
      assertThat(resultSet.getString("late")).isNull();

      assertThat(resultSet.next()).isTrue();
      assertThat(resultSet.getInt("zeta")).isEqualTo(2);
      assertThat(resultSet.getString("alpha")).isEqualTo("b");
      assertThat(resultSet.getBoolean("mid")).isFalse();
      assertThat(resultSet.getString("nothing")).isNull();
      assertThat(resultSet.getDouble("late")).isEqualTo(3.5d);
      assertThat(resultSet.next()).isFalse();
    }
  }

  // Regression: #13012
  @Test
  void aProjectionKeepsThePropertiesItNamedInTheOrderItNamedThem() throws Exception {
    try (final Connection connection = openJdbcConnection(); final Statement statement = connection.createStatement();
        final ResultSet resultSet = statement.executeQuery("select mid, zeta from " + TYPE)) {
      assertThat(columnNames(resultSet.getMetaData())).startsWith("mid", "zeta").doesNotContain("alpha", "nothing", "late");
    }
  }

  // Regression: #13012
  @Test
  void aPropertyAProjectionExcludedIsNotAnnounced() throws Exception {
    try (final Connection connection = openJdbcConnection(); final Statement statement = connection.createStatement();
        final ResultSet resultSet = statement.executeQuery("select *, !alpha from " + TYPE)) {
      // a projection row is not a plain record row: it takes the union path, so only the set of columns is asserted here
      assertThat(columnNames(resultSet.getMetaData())).containsExactlyInAnyOrder("zeta", "mid", "nothing", "late", "@rid", "@type", "@cat");
    }
  }

  private static List<String> columnNames(final ResultSetMetaData metaData) throws Exception {
    final List<String> names = new ArrayList<>(metaData.getColumnCount());
    for (int i = 1; i <= metaData.getColumnCount(); i++)
      names.add(metaData.getColumnName(i));
    return names;
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
