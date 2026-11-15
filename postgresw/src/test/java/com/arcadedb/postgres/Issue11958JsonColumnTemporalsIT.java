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

import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11958: a record or a map in a json column carried its temporals as epoch numbers, a DATE as days in a record and as
 * milliseconds in a map, and a map lost the zone of a ZONED_DATETIME and the sub-millisecond digits of a DATETIME_NANOS. They now
 * travel as the text the HTTP API sends, the same wherever the value sits.
 */
class Issue11958JsonColumnTemporalsIT extends PostgresWireProtocolTestBase {

  @BeforeEach
  void createVertex() throws Exception {
    try (final Connection connection = openJdbcConnection(); final Statement statement = connection.createStatement()) {
      statement.execute("CREATE VERTEX TYPE T11958 IF NOT EXISTS");
      statement.execute("CREATE PROPERTY T11958.da IF NOT EXISTS DATE");
      statement.execute("CREATE PROPERTY T11958.dt IF NOT EXISTS DATETIME");
      statement.execute("CREATE PROPERTY T11958.dtn IF NOT EXISTS DATETIME_NANOS");
      statement.execute("CREATE PROPERTY T11958.zdt IF NOT EXISTS ZONED_DATETIME");
      statement.execute("INSERT INTO T11958 SET da = '2024-02-29', dt = '2024-02-29 13:14:15.123', "
          + "dtn = '2024-02-29 13:14:15.123456789', zdt = '2024-03-10T03:30-04:00[America/New_York]'");
    }
  }

  @Test
  void sqlRecordAndMapCarryTheSameTemporalText() throws Exception {
    try (final Connection connection = openJdbcConnection(); final Statement statement = connection.createStatement();
        final ResultSet rs = statement.executeQuery(
            "SELECT @this AS rec, {'da': da, 'dt': dt, 'dtn': dtn, 'zdt': zdt} AS m FROM T11958")) {
      assertThat(rs.next()).isTrue();
      assertTemporals(new JSONObject(rs.getString("rec")));
      assertTemporals(new JSONObject(rs.getString("m")));
    }
  }

  @Test
  void cypherNodeAndPropertiesCarryTheSameTemporalText() throws Exception {
    try (final Connection connection = openJdbcConnection(); final Statement statement = connection.createStatement();
        final ResultSet rs = statement.executeQuery("{cypher}MATCH (n:T11958) RETURN n, properties(n) AS props")) {
      assertThat(rs.next()).isTrue();
      assertTemporals(new JSONObject(rs.getString("n")));
      assertTemporals(new JSONObject(rs.getString("props")));
    }
  }

  private static void assertTemporals(final JSONObject json) {
    assertThat(json.get("da")).isEqualTo("2024-02-29");
    assertThat(json.get("dt")).isEqualTo("2024-02-29 13:14:15.123");
    assertThat(json.get("dtn")).isEqualTo("2024-02-29 13:14:15.123456789");
    assertThat(json.get("zdt")).isEqualTo("2024-03-10T03:30-04:00[America/New_York]");
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
