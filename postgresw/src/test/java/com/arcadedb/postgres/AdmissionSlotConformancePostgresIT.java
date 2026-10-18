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

import com.arcadedb.database.Database;
import com.arcadedb.server.AdmissionSlotConformance;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10235 over the Postgres protocol: a statement that gives its admission slot back before a wait finds the slot on
 * the thread that runs it, on the simple and the extended protocol, in auto-commit and inside a client transaction. See
 * {@link AdmissionSlotConformance}.
 */
class AdmissionSlotConformancePostgresIT extends PostgresWireProtocolTestBase {
  private static final String TYPE = "Conformance10235";

  @Test
  void aStatementGivesItsSlotBackFromTheThreadThatRunsIt() throws Exception {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.getSchema().getOrCreateDocumentType(TYPE);

    int id = 0;
    for (final String mode : new String[] { "simple", "extended" })
      for (final boolean autoCommit : new boolean[] { true, false }) {
        final String surface = "Postgres " + mode + (autoCommit ? "" : " in a transaction");
        final String sql = "INSERT INTO " + TYPE + " SET id = " + (++id);
        try (final Connection conn = getConnection(mode); final Statement st = conn.createStatement()) {
          conn.setAutoCommit(autoCommit);
          AdmissionSlotConformance.assertGivesItsSlotBack(database, TYPE, surface, () -> st.execute(sql));
          if (!autoCommit)
            conn.commit();
        }
      }
    assertThat(database.countType(TYPE, false)).isEqualTo(4);
  }

  private Connection getConnection(final String mode) throws SQLException, ClassNotFoundException {
    Class.forName("org.postgresql.Driver");
    final Properties props = new Properties();
    props.setProperty("user", "root");
    props.setProperty("password", DEFAULT_PASSWORD_FOR_TESTS);
    props.setProperty("ssl", "false");
    // simple: every statement is a 'Q' message; extended: Parse/Bind/Describe/Execute
    props.setProperty("preferQueryMode", mode);
    return DriverManager.getConnection(getServerPostgresJdbcUrl(), props);
  }
}
