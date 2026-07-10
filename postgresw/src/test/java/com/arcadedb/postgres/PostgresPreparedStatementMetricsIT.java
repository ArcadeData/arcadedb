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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.BaseGraphServerTest;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that SQL queries executed via the Postgres extended protocol (PreparedStatement /
 * PARSE+BIND+EXECUTE) are timed and emitted as {@code arcadedb.query.duration} with
 * {@code protocol=postgres} and {@code language=sql}.
 */
public class PostgresPreparedStatementMetricsIT extends BaseGraphServerTest {

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("Postgres:com.arcadedb.postgres.PostgresProtocolPlugin");
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  private Connection getConnection() throws Exception {
    Class.forName("org.postgresql.Driver");
    final String url = "jdbc:postgresql://localhost/" + getDatabaseName();
    final Properties props = new Properties();
    props.setProperty("user", "root");
    props.setProperty("password", DEFAULT_PASSWORD_FOR_TESTS);
    props.setProperty("ssl", "false");
    return DriverManager.getConnection(url, props);
  }

  @Override
  protected String getDatabaseName() {
    return "postgresdb";
  }

  @Test
  void extendedProtocolSqlQueryIsTimedWithPostgresTag() throws Exception {
    try (final Connection conn = getConnection()) {
      // Force extended protocol: PreparedStatement sends PARSE + BIND + EXECUTE
      try (final PreparedStatement ps = conn.prepareStatement("SELECT * FROM schema:types")) {
        final ResultSet rs = ps.executeQuery();
        rs.close();
      }
    }

    final Timer timer = Metrics.globalRegistry.find("arcadedb.query.duration")
        .tag("protocol", "postgres")
        .tag("language", "sql")
        .timer();
    assertThat(timer).isNotNull();
    assertThat(timer.count()).isGreaterThanOrEqualTo(1L);
  }
}
