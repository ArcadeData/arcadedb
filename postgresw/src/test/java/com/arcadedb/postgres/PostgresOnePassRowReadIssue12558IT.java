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
import java.sql.Statement;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A functional check of the one-pass row read over the Postgres wire (#12558): a wide row keeps every column, a null one
 * included, with its value. The header walks themselves are counted by {@code PostgresOnePassRowReadIssue12558Test}.
 */
class PostgresOnePassRowReadIssue12558IT extends PostgresWireProtocolTestBase {

  private static final int PROPERTIES = 40;

  @Test
  void aWideRowIsReadOnceOverTheRecordAndKeepsEveryColumn() throws Exception {
    try (final Connection connection = openJdbcConnection(); final Statement statement = connection.createStatement()) {
      statement.execute("CREATE DOCUMENT TYPE Issue12558Wide IF NOT EXISTS");
      final StringBuilder insert = new StringBuilder("INSERT INTO Issue12558Wide SET nothing = null");
      for (int i = 0; i < PROPERTIES; i++)
        insert.append(", p").append(i).append(" = 'value-").append(i).append('\'');
      statement.execute(insert.toString());

      // the row over the wire
      try (final ResultSet rs = statement.executeQuery("SELECT * FROM Issue12558Wide")) {
        assertThat(rs.next()).isTrue();
        for (int i = 0; i < PROPERTIES; i++)
          assertThat(rs.getString("p" + i)).as("column p%d", i).isEqualTo("value-" + i);
        assertThat(rs.getObject("nothing")).as("a null column").isNull();
      }
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
