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
 * Issue #11941: {@code SELECT *, !secret} over the PostgreSQL wire protocol announced a {@code secret} column and sent its stored
 * value, because the protocol read the row's backing record directly instead of the row, so the projection's exclusion never applied.
 * The same held for {@code !@rid} and {@code !@type}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11941ExcludedColumnIT extends PostgresWireProtocolTestBase {

  @BeforeEach
  void createData() {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE P11941");
      database.command("sql", "INSERT INTO P11941 SET name = 'a', secret = 's3cr3t'");
    });
  }

  @Test
  void anExcludedPropertyIsNeitherAnnouncedNorSent() throws Exception {
    for (final Properties properties : modes()) {
      final String mode = properties.toString();
      try (final Connection connection = connect(properties); final Statement statement = connection.createStatement();
          final ResultSet rs = statement.executeQuery("SELECT *, !secret FROM P11941")) {
        final List<String> columns = columnsOf(rs.getMetaData());
        assertThat(columns).as(mode).contains("name").doesNotContain("secret");
        assertThat(rs.next()).isTrue();
        for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++)
          assertThat(rs.getString(i)).as(mode + " column " + columns.get(i - 1)).isNotEqualTo("s3cr3t");
      }
    }
  }

  @Test
  void excludedRecordAttributesAreNotAnnounced() throws Exception {
    for (final Properties properties : modes()) {
      final String mode = properties.toString();
      try (final Connection connection = connect(properties); final Statement statement = connection.createStatement();
          final ResultSet rs = statement.executeQuery("SELECT *, !@rid, !@type FROM P11941")) {
        final List<String> columns = columnsOf(rs.getMetaData());
        assertThat(columns).as(mode).contains("name", "secret").doesNotContain("@rid", "@type");
        assertThat(rs.next()).isTrue();
        assertThat(rs.getString("secret")).isEqualTo("s3cr3t");
      }
    }
  }

  @Test
  void anExcludedPropertyRewrittenByTheProjectionAnswersTheNewValue() throws Exception {
    for (final Properties properties : modes()) {
      final String mode = properties.toString();
      try (final Connection connection = connect(properties); final Statement statement = connection.createStatement();
          final ResultSet rs = statement.executeQuery("SELECT *, !secret, 'hidden' AS secret FROM P11941")) {
        assertThat(rs.next()).isTrue();
        assertThat(rs.getString("secret")).as(mode).isEqualTo("hidden");
      }
    }
  }

  @Test
  void withoutExclusionEveryColumnIsStillThere() throws Exception {
    for (final Properties properties : modes()) {
      try (final Connection connection = connect(properties); final Statement statement = connection.createStatement();
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

  private Connection connect(final Properties properties) throws Exception {
    final Properties copy = new Properties();
    copy.putAll(properties);
    copy.setProperty("user", "root");
    copy.setProperty("password", DEFAULT_PASSWORD_FOR_TESTS);
    copy.setProperty("sslmode", "disable");
    return DriverManager.getConnection(getServerPostgresJdbcUrl(), copy);
  }
}
