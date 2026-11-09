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
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.Properties;
import java.util.TimeZone;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11389: pgjdbc binds a {@code java.sql.Date} as untyped text with a zone suffix ({@code '2024-05-06 +01'}). An INSERT
 * into a DATE property failed and a predicate on it silently matched nothing.
 */
class Issue11389SetDateZoneSuffixIT extends PostgresWireProtocolTestBase {

  @Test
  void setDateInsertsAndMatchesADeclaredDateProperty() throws Exception {
    final TimeZone previous = TimeZone.getDefault();
    // a zone ahead of UTC makes the driver render a non-trivial suffix; the day must survive whatever it is
    TimeZone.setDefault(TimeZone.getTimeZone("Europe/Lisbon"));
    try (final Connection connection = openJdbcConnection()) {
      try (final Statement statement = connection.createStatement()) {
        statement.execute("CREATE DOCUMENT TYPE D11389 IF NOT EXISTS");
        statement.execute("CREATE PROPERTY D11389.id IF NOT EXISTS INTEGER");
        statement.execute("CREATE PROPERTY D11389.d IF NOT EXISTS DATE");
      }

      final Date day = Date.valueOf(LocalDate.of(2024, 5, 6));
      try (final PreparedStatement insert = connection.prepareStatement("INSERT INTO D11389 (id, d) VALUES (?, ?)")) {
        insert.setInt(1, 1);
        insert.setDate(2, day);
        insert.execute();
      }

      for (final String[] predicate : new String[][] { { "d = ?", "1" }, { "d >= ?", "1" }, { "d <= ?", "1" }, { "d < ?", "0" },
          { "d > ?", "0" } }) {
        try (final PreparedStatement select = connection.prepareStatement("SELECT id FROM D11389 WHERE " + predicate[0])) {
          select.setDate(1, day);
          try (final ResultSet rs = select.executeQuery()) {
            int count = 0;
            while (rs.next())
              count++;
            assertThat(count).as(predicate[0]).isEqualTo(Integer.parseInt(predicate[1]));
          }
        }
      }
    } finally {
      TimeZone.setDefault(previous);
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
