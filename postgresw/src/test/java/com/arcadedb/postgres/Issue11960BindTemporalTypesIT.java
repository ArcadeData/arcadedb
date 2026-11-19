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

import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.postgresql.util.PGInterval;
import org.postgresql.util.PGobject;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11960: pgjdbc binds an {@code OffsetDateTime} as timestamptz, a {@code LocalTime} as time, an {@code OffsetTime} as timetz,
 * a {@code PGInterval} as interval and a jsonb {@code PGobject} as jsonb. Every one of them failed the Bind with XX000; each is now
 * stored as the engine value of the matching type.
 */
class Issue11960BindTemporalTypesIT extends PostgresWireProtocolTestBase {

  @Test
  void driverTemporalAndJsonbParametersAreStored() throws Exception {
    final OffsetDateTime zoned = OffsetDateTime.of(2024, 2, 29, 13, 14, 15, 123_456_000, ZoneOffset.ofHours(2));
    try (final Connection connection = openJdbcConnection()) {
      try (final var statement = connection.createStatement()) {
        statement.execute("CREATE DOCUMENT TYPE W11960 IF NOT EXISTS");
        statement.execute("CREATE PROPERTY W11960.micros IF NOT EXISTS DATETIME_MICROS");
      }
      try (final PreparedStatement insert = connection.prepareStatement(
          "INSERT INTO W11960 SET id = 1, z = ?, micros = ?, t = ?, ttz = ?, d = ?, j = ?")) {
        insert.setObject(1, zoned);
        insert.setObject(2, zoned);
        insert.setObject(3, LocalTime.of(10, 15, 30));
        insert.setObject(4, OffsetTime.of(10, 15, 30, 0, ZoneOffset.ofHours(2)));
        insert.setObject(5, new PGInterval(0, 0, 1, 0, 0, 5));
        final PGobject jsonb = new PGobject();
        jsonb.setType("jsonb");
        jsonb.setValue("{\"a\": 1}");
        insert.setObject(6, jsonb);
        insert.executeUpdate();
      }
    }

    try (final ResultSet rs = getServerDatabase(0, getDatabaseName()).query("sql", "SELECT FROM W11960 WHERE id = 1")) {
      final var row = rs.next();
      assertThat(((ZonedDateTime) row.getProperty("z")).toInstant()).isEqualTo(zoned.toInstant());
      // the instant, read into a DATETIME_MICROS property: every microsecond kept
      assertThat(row.<LocalDateTime>getProperty("micros")).isEqualTo(zoned.withOffsetSameInstant(ZoneOffset.UTC).toLocalDateTime());
      assertThat(row.<Object>getProperty("t")).isEqualTo(LocalTime.of(10, 15, 30));
      assertThat(((OffsetTime) row.getProperty("ttz")).toLocalTime()).isEqualTo(LocalTime.of(10, 15, 30));
      assertThat(row.<Object>getProperty("d").toString()).isEqualTo("P1DT5S");
      assertThat(row.<Map<String, Object>>getProperty("j")).containsEntry("a", 1);
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
