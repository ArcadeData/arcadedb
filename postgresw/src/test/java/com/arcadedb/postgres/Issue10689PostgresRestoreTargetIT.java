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

import com.arcadedb.server.ArcadeDBServer;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10689, through a real connection: a Postgres session resolves its database once, when it connects, so a session opened
 * before an {@code overwrite} restore claimed the database never meets a check made at resolution. Its writes inside the restore
 * window were acknowledged and then destroyed by the swap. They are now refused with {@code 57P03} cannot_connect_now, the answer a
 * held database gets, while its reads go on until the swap.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue10689PostgresRestoreTargetIT extends PostgresWireProtocolTestBase {
  private static final String TYPE = "Issue10689Pg";

  @Test
  void aSessionOpenedBeforeTheRestoreReadsAndCannotWrite() throws Exception {
    final ArcadeDBServer server = getServer(0);
    server.getDatabaseForOperator(getDatabaseName()).getSchema().createDocumentType(TYPE);

    try (final Connection connection = openConnection(); final Statement statement = connection.createStatement()) {
      statement.execute("INSERT INTO " + TYPE + " SET v = 1");

      server.reserveDatabaseNameForRestore(getDatabaseName());
      try {
        assertThat(count(statement)).as("the session still reads the database the restore will replace").isEqualTo(1L);

        assertThatThrownBy(() -> statement.execute("INSERT INTO " + TYPE + " SET v = 2")).isInstanceOf(SQLException.class)
            .satisfies(e -> assertThat(((SQLException) e).getSQLState()).isEqualTo(PostgresNetworkExecutor.SQLSTATE_CANNOT_CONNECT_NOW))
            .hasMessageContaining("restore");
        assertThat(count(statement)).as("nothing was written").isEqualTo(1L);
      } finally {
        server.releaseDatabaseNameReservedForRestore(getDatabaseName());
      }

      statement.execute("INSERT INTO " + TYPE + " SET v = 2");
      assertThat(count(statement)).as("the end of the restore gives the writes back").isEqualTo(2L);
    }
  }

  private static long count(final Statement statement) throws SQLException {
    try (final ResultSet resultSet = statement.executeQuery("SELECT count(@rid) AS c FROM " + TYPE)) {
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
    return DriverManager.getConnection("jdbc:postgresql://localhost:" + getServerPostgresPort() + "/" + getDatabaseName(), properties);
  }
}
