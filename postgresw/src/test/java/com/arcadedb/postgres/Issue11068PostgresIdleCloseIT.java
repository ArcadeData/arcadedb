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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.DatabaseIdleCloser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11068, through a real driver: a database the server closed for idleness is reopened by the next statement of a
 * connection that stayed open across the close - a pooled connection does not keep its database open - while a connection
 * with a transaction open does keep it open, so its transaction is never rolled back by an idle close.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue11068PostgresIdleCloseIT extends PostgresWireProtocolTestBase {

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    // Driven by the test: the scheduled check waits an hour
    config.setValue(GlobalConfiguration.SERVER_DATABASE_IDLE_CHECK_INTERVAL, TimeUnit.HOURS.toMillis(1));
  }

  @AfterEach
  void reopen() {
    final ArcadeDBServer server = getServer(0);
    if (server != null && server.isStarted()) {
      server.getConfiguration().setValue(GlobalConfiguration.SERVER_DATABASE_IDLE_TIMEOUT, 0L);
      server.getDatabaseForOperator(getDatabaseName());
    }
  }

  @Test
  void aConnectionHeldAcrossAnIdleCloseReopensTheDatabase() throws Exception {
    try (final Connection connection = openConnection()) {
      execute(connection, "create document type IdlePg");
      execute(connection, "insert into IdlePg set i = 1");

      assertThat(closeIdle()).isEqualTo(1);
      assertThat(getServer(0).existsDatabase(getDatabaseName())).isFalse();

      // The same connection, not a new one: its next statement opens the database again
      assertThat(count(connection)).isEqualTo(1);
      assertThat(getServer(0).existsDatabase(getDatabaseName())).isTrue();
      execute(connection, "insert into IdlePg set i = 2");
      assertThat(count(connection)).isEqualTo(2);
    }
  }

  @Test
  void aConnectionWithATransactionOpenKeepsTheDatabaseOpen() throws Exception {
    try (final Connection connection = openConnection()) {
      execute(connection, "create document type IdlePgTx");
      connection.setAutoCommit(false);
      execute(connection, "insert into IdlePgTx set i = 1");

      assertThat(closeIdle()).as("a quiet transaction is still a transaction").isZero();
      execute(connection, "insert into IdlePgTx set i = 2");
      connection.commit();
      connection.setAutoCommit(true);

      try (final Statement statement = connection.createStatement();
          final ResultSet resultSet = statement.executeQuery("select count(*) as c from IdlePgTx")) {
        assertThat(resultSet.next()).isTrue();
        assertThat(resultSet.getLong(1)).isEqualTo(2);
      }
    }
  }

  /**
   * Checks a little apart, until one closes something: the first samples the database, a later one finds it unused past a 1 ms
   * timeout. Several, so that background work landing between two of them delays the close instead of failing the test.
   */
  private int closeIdle() throws InterruptedException {
    getServer(0).getConfiguration().setValue(GlobalConfiguration.SERVER_DATABASE_IDLE_TIMEOUT, 1L);
    final DatabaseIdleCloser closer = getServer(0).getDatabaseIdleCloser();
    int closed = closer.check();
    for (int attempt = 0; attempt < 10 && closed == 0; attempt++) {
      Thread.sleep(20);
      closed += closer.check();
    }
    return closed;
  }

  private static void execute(final Connection connection, final String sql) throws SQLException {
    try (final Statement statement = connection.createStatement()) {
      statement.execute(sql);
    }
  }

  private static long count(final Connection connection) throws SQLException {
    try (final Statement statement = connection.createStatement();
        final ResultSet resultSet = statement.executeQuery("select count(*) as c from IdlePg")) {
      assertThat(resultSet.next()).isTrue();
      return resultSet.getLong(1);
    }
  }

  private Connection openConnection() throws Exception {
    Class.forName("org.postgresql.Driver");
    final Properties properties = new Properties();
    properties.setProperty("user", "root");
    properties.setProperty("password", DEFAULT_PASSWORD_FOR_TESTS);
    properties.setProperty("ssl", "false");
    properties.setProperty("sslMode", "disable");
    return DriverManager.getConnection("jdbc:postgresql://localhost:" + getServerPostgresPort() + "/" + getDatabaseName(),
        properties);
  }
}
