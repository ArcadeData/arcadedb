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
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerDatabase;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10533, through a real connection: a Postgres client that names a database an {@code overwrite} restore holds, or that a
 * {@code close database} closed, is refused with {@code 57P03} cannot_connect_now - the database exists, so it is not told that it
 * does not - and a name that is not a database is {@code 3D000} invalid_catalog_name (issue #10795). Once nothing holds the name the
 * same connection opens it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue10533PostgresHeldDatabaseIT extends PostgresWireProtocolTestBase {
  private static final String HELD = "pgheld10533";

  @Test
  void aConnectionToADatabaseARestoreHoldsIsRefusedAsNotAvailable() throws Exception {
    final ArcadeDBServer server = getServer(0);
    onDiskNotOpen(server);

    server.reserveDatabaseNameForRestore(HELD);
    try {
      assertThatThrownBy(() -> openConnection(HELD).close()).isInstanceOf(SQLException.class)
          .satisfies(e -> assertThat(((SQLException) e).getSQLState()).isEqualTo(PostgresNetworkExecutor.SQLSTATE_CANNOT_CONNECT_NOW))
          .hasMessageContaining("not available");
      assertThat(server.existsDatabase(HELD)).as("the connection opened nothing").isFalse();
    } finally {
      server.releaseDatabaseNameReservedForRestore(HELD);
    }

    try (final Connection connection = openConnection(HELD)) {
      assertThat(connection.isValid(5)).as("a released reservation holds nothing").isTrue();
    } finally {
      dropIfPresent(server);
    }
  }

  @Test
  void aConnectionToADatabaseACloseCommandClosedIsRefusedAsNotAvailable() throws Exception {
    final ArcadeDBServer server = getServer(0);
    onDiskNotOpen(server);

    server.markClosedByCommand(HELD);
    try {
      assertThatThrownBy(() -> openConnection(HELD).close()).isInstanceOf(SQLException.class)
          .satisfies(e -> assertThat(((SQLException) e).getSQLState()).isEqualTo(PostgresNetworkExecutor.SQLSTATE_CANNOT_CONNECT_NOW));
    } finally {
      server.unmarkClosedByCommand(HELD);
      dropIfPresent(server);
    }
  }

  /** Issue #10795: PostgreSQL's own answer to a connection that names a database it does not have, 3D000 invalid_catalog_name. */
  @Test
  void aNameThatIsNotADatabaseIsInvalidCatalogName() {
    assertThatThrownBy(() -> openConnection("pgnothing10533").close()).isInstanceOf(SQLException.class)
        .satisfies(e -> assertThat(((SQLException) e).getSQLState()).isEqualTo(PostgresNetworkExecutor.SQLSTATE_INVALID_CATALOG_NAME))
        .hasMessageContaining("does not exist");
  }

  /** Leaves {@link #HELD} on disk, closed and deregistered: what an {@code overwrite} restore finds on its target. */
  private static void onDiskNotOpen(final ArcadeDBServer server) {
    final ServerDatabase created = server.createDatabase(HELD, ComponentFile.MODE.READ_WRITE);
    created.getEmbedded().close();
    server.removeDatabase(HELD);
    assertThat(server.existsDatabase(HELD)).isFalse();
  }

  private static void dropIfPresent(final ArcadeDBServer server) {
    if (server.existsDatabase(HELD))
      server.dropLocalDatabase(server.getDatabase(HELD).getEmbedded(), HELD);
    FileUtils.deleteRecursively(
        new File(server.getConfiguration().getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY), HELD));
  }

  private Connection openConnection(final String database) throws Exception {
    Class.forName("org.postgresql.Driver");
    final Properties properties = new Properties();
    properties.setProperty("user", "root");
    properties.setProperty("password", DEFAULT_PASSWORD_FOR_TESTS);
    properties.setProperty("ssl", "false");
    properties.setProperty("sslMode", "disable");
    return DriverManager.getConnection("jdbc:postgresql://localhost:" + getServerPostgresPort() + "/" + database, properties);
  }
}
