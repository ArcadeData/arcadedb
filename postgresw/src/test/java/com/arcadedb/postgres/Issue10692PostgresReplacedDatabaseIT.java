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
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerControlPlane;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10692, through a real driver: a Postgres connection resolved its database once, at connect, and kept that instance for
 * life. When the server replaced the instance behind the name - a {@code close database} followed by {@code open database}, a
 * {@code drop database} followed by a {@code create database}, a {@code restore backup} with {@code overwrite} - every later
 * statement on the connection failed with {@code XX000} internal_error, while a fresh connection worked. A connection held across
 * the swap now serves the new instance; one used while nothing serves the name is closed with the {@code 57P03} the connect path
 * gives, never with the server-fault code; one with a transaction open on the closed instance is terminated with {@code 57P01}.
 */
public class Issue10692PostgresReplacedDatabaseIT extends PostgresWireProtocolTestBase {
  private static final String NAME = "pgswap10692";

  private ServerControlPlane controlPlane;

  @BeforeEach
  @Override
  public void beginTest() {
    super.beginTest();
    controlPlane = new ServerControlPlane(getServer(0));
    controlPlane.createDatabase(NAME);
    getServer(0).getDatabaseForOperator(NAME).getSchema().createDocumentType("S");
  }

  @AfterEach
  @Override
  public void endTest() {
    try {
      final ArcadeDBServer server = getServer(0);
      server.unmarkClosedByCommand(NAME);
      if (server.existsDatabase(NAME))
        server.dropLocalDatabase(server.getDatabaseForOperator(NAME).getEmbedded(), NAME);
      FileUtils.deleteRecursively(
          new File(server.getConfiguration().getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY), NAME));
    } finally {
      super.endTest();
    }
  }

  @Test
  void aConnectionHeldAcrossACloseAndOpenServesTheReopenedDatabase() throws Exception {
    try (final Connection connection = openConnection()) {
      insert(connection, 5);
      assertThat(count(connection)).isEqualTo(5);

      controlPlane.closeDatabase(NAME);
      controlPlane.openDatabase(NAME);

      assertThat(count(connection)).as("the reopened instance, not the closed one").isEqualTo(5);
      insert(connection, 2);
      assertThat(count(connection)).isEqualTo(7);
    }
  }

  @Test
  void aConnectionHeldAcrossADropAndCreateServesTheNewDatabase() throws Exception {
    try (final Connection connection = openConnection()) {
      insert(connection, 5);

      controlPlane.dropDatabase(NAME);
      controlPlane.createDatabase(NAME);

      try (final Statement statement = connection.createStatement()) {
        statement.execute("create document type S");
      }
      insert(connection, 3);
      assertThat(count(connection)).as("the database created again, holding only what was written to it").isEqualTo(3);
    }
  }

  @Test
  void aConnectionHeldAcrossARestoreServesTheRestoredDatabase() throws Exception {
    try (final Connection connection = openConnection()) {
      insert(connection, 5);
      // THE FILE NAME, RESOLVED UNDER THE SERVER'S BACKUP DIRECTORY
      final String backupFile = new File(controlPlane.triggerBackup(NAME).getString("backupFile")).getName();
      insert(connection, 2);
      assertThat(count(connection)).isEqualTo(7);

      try {
        controlPlane.restoreBackup(NAME, backupFile, NAME, true, ServerControlPlane.ProgressListener.NOOP);

        assertThat(count(connection)).as("the restored copy, taken before the last two inserts").isEqualTo(5);
        insert(connection, 1);
        assertThat(count(connection)).isEqualTo(6);
      } finally {
        FileUtils.deleteRecursively(controlPlane.resolveBackupFile(NAME, backupFile).getParent().toFile());
      }
    }
  }

  @Test
  void aConnectionUsedWhileTheDatabaseIsClosedIsRefusedAsNotAvailableNotAsAServerFault() throws Exception {
    try (final Connection connection = openConnection()) {
      insert(connection, 1);

      controlPlane.closeDatabase(NAME);

      assertThatThrownBy(() -> count(connection)).isInstanceOf(SQLException.class)
          .satisfies(e -> assertThat(((SQLException) e).getSQLState()).isEqualTo(PostgresNetworkExecutor.SQLSTATE_CANNOT_CONNECT_NOW))
          .hasMessageContaining("not available");
      assertThat(connection.isValid(5)).as("closed as PostgreSQL closes the backends of a database taken away").isFalse();

      // a new connection is refused the same way until the name is served again, and then works
      assertThatThrownBy(() -> openConnection().close()).isInstanceOf(SQLException.class)
          .satisfies(e -> assertThat(((SQLException) e).getSQLState()).isEqualTo(PostgresNetworkExecutor.SQLSTATE_CANNOT_CONNECT_NOW));
    }

    controlPlane.openDatabase(NAME);
    try (final Connection connection = openConnection()) {
      assertThat(count(connection)).isEqualTo(1);
    }
  }

  @Test
  void aTransactionOpenOnTheClosedInstanceTerminatesTheConnection() throws Exception {
    try (final Connection connection = openConnection()) {
      insert(connection, 1);
      connection.setAutoCommit(false);
      insert(connection, 4);

      controlPlane.closeDatabase(NAME);
      controlPlane.openDatabase(NAME);

      // carrying on against the reopened instance would let the client commit a block of which only the later statements exist
      assertThatThrownBy(() -> insert(connection, 1)).isInstanceOf(SQLException.class)
          .satisfies(e -> assertThat(((SQLException) e).getSQLState()).isEqualTo(PostgresNetworkExecutor.SQLSTATE_ADMIN_SHUTDOWN));
      assertThat(connection.isValid(5)).isFalse();
    }

    try (final Connection connection = openConnection()) {
      assertThat(count(connection)).as("the block open at the close is gone, nothing of it committed").isEqualTo(1);
    }
  }

  private static void insert(final Connection connection, final int rows) throws SQLException {
    try (final Statement statement = connection.createStatement()) {
      for (int i = 0; i < rows; i++)
        statement.execute("insert into S set i = " + i);
    }
  }

  private static long count(final Connection connection) throws SQLException {
    try (final Statement statement = connection.createStatement();
        final ResultSet resultSet = statement.executeQuery("select count(*) as c from S")) {
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
    return DriverManager.getConnection("jdbc:postgresql://localhost:" + getServerPostgresPort() + "/" + NAME, properties);
  }
}
