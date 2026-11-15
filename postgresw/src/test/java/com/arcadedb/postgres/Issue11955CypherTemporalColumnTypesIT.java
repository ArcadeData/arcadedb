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
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11955: a Cypher date or datetime (a function result, or a DATE / DATETIME property read through Cypher) was announced as
 * varchar and sent as its text, where SQL announces the same property as date / timestamp. It is now typed as the java.time value
 * it wraps.
 */
class Issue11955CypherTemporalColumnTypesIT extends PostgresWireProtocolTestBase {

  @Test
  void cypherTemporalsAreTypedAsTheirSqlCounterparts() throws Exception {
    try (final Connection connection = openJdbcConnection(); final Statement statement = connection.createStatement()) {
      statement.execute("CREATE VERTEX TYPE T11955 IF NOT EXISTS");
      statement.execute("CREATE PROPERTY T11955.da IF NOT EXISTS DATE");
      statement.execute("CREATE PROPERTY T11955.dt IF NOT EXISTS DATETIME");
      statement.execute("INSERT INTO T11955 SET da = '2024-02-29', dt = '2024-02-29 13:14:15.123'");

      try (final ResultSet sql = statement.executeQuery("SELECT da, dt FROM T11955")) {
        assertThat(sql.next()).isTrue();
        assertThat(sql.getMetaData().getColumnType(1)).isEqualTo(Types.DATE);
        assertThat(sql.getMetaData().getColumnType(2)).isEqualTo(Types.TIMESTAMP);
      }

      try (final ResultSet rs = statement.executeQuery("{cypher}MATCH (n:T11955) RETURN n.da AS da, n.dt AS dt, "
          + "date('2024-01-02') AS d, localdatetime('2024-01-02T03:04:05') AS ldt")) {
        assertThat(rs.next()).isTrue();
        final ResultSetMetaData meta = rs.getMetaData();
        assertThat(meta.getColumnType(1)).as("n.da").isEqualTo(Types.DATE);
        assertThat(meta.getColumnType(2)).as("n.dt").isEqualTo(Types.TIMESTAMP);
        assertThat(meta.getColumnType(3)).as("date()").isEqualTo(Types.DATE);
        assertThat(meta.getColumnType(4)).as("localdatetime()").isEqualTo(Types.TIMESTAMP);
        assertThat(rs.getObject(1, LocalDate.class)).isEqualTo(LocalDate.of(2024, 2, 29));
        assertThat(rs.getObject(2, LocalDateTime.class)).isEqualTo(LocalDateTime.of(2024, 2, 29, 13, 14, 15, 123_000_000));
        assertThat(rs.getObject(3, LocalDate.class)).isEqualTo(LocalDate.of(2024, 1, 2));
        assertThat(rs.getObject(4, LocalDateTime.class)).isEqualTo(LocalDateTime.of(2024, 1, 2, 3, 4, 5));
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
