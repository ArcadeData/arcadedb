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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.postgresql.util.PSQLException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #7846: {@code ROLLBACK TO <savepoint>} used to be accepted as a silent no-op,
 * the same way {@code SAVEPOINT} and {@code RELEASE} harmlessly are, even though this server has no
 * savepoint checkpoint to roll back to. That let a client believe a rollback-to-savepoint had discarded
 * some of its pending writes when every one of them was still going to be persisted by the next
 * {@code COMMIT}. {@code ROLLBACK TO} must now fail instead, aborting the transaction so nothing commits
 * silently.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7846RollbackToNoLongerSucceedsIT extends BaseGraphServerTest {

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("Postgres:com.arcadedb.postgres.PostgresProtocolPlugin");
    GlobalConfiguration.POSTGRES_DEBUG.setValue("false");
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    GlobalConfiguration.POSTGRES_DEBUG.setValue("false");
    super.endTest();
  }

  @Override
  protected String getDatabaseName() {
    return "postgresdb";
  }

  private Connection getConnection() throws SQLException, ClassNotFoundException {
    Class.forName("org.postgresql.Driver");
    final var url = "jdbc:postgresql://localhost/" + getDatabaseName();
    final var props = new Properties();
    props.setProperty("user", "root");
    props.setProperty("password", DEFAULT_PASSWORD_FOR_TESTS);
    return DriverManager.getConnection(url, props);
  }

  @Test
  void rollbackToSavepointFailsAndAbortsTheTransaction() throws Exception {
    try (var conn = getConnection()) {
      try (var st = conn.createStatement()) {
        st.execute("CREATE VERTEX TYPE Issue7846Test IF NOT EXISTS");
      }

      conn.setAutoCommit(false);
      try (var st = conn.createStatement()) {
        st.execute("INSERT INTO Issue7846Test SET name = 'beforeSavepoint'");
        final Savepoint sp = conn.setSavepoint("issue7846sp");
        st.execute("INSERT INTO Issue7846Test SET name = 'afterSavepoint'");

        // A real PostgreSQL server would discard only 'afterSavepoint' here. This server cannot, so it must
        // refuse the rollback rather than pretend it happened: pgjdbc's Savepoint API runs over the extended
        // query protocol, where refusing a statement aborts the whole pipelined request and the transaction
        // it belongs to (mirrored by the simple-query-protocol case below) - so neither write survives, which
        // is the safe outcome given this server cannot undo only the one the client actually asked to discard.
        assertThatThrownBy(() -> conn.rollback(sp)).isInstanceOf(PSQLException.class);
      } catch (final SQLException ignored) {
        // pgjdbc surfaces the aborted transaction to further statements on this Connection as an exception
        // too, depending on how much of the Execute pipeline it had already queued - either way is fine here.
      }

      try {
        conn.rollback();
      } catch (final SQLException ignored) {
        // Already rolled back server-side by the failed statement above; a client-side ROLLBACK on top of
        // that is a no-op it's fine for pgjdbc to reject.
      }
      conn.setAutoCommit(true);

      try (var st = conn.createStatement(); ResultSet rs = st.executeQuery("SELECT FROM Issue7846Test")) {
        assertThat(rs.next()).as("neither the pre- nor the post-savepoint write may survive a rollback this server could not honor")
            .isFalse();
      }
    }
  }

  @Test
  void savepointAndReleaseStillSucceedAsHarmlessNoOps() throws Exception {
    try (var conn = getConnection()) {
      try (var st = conn.createStatement()) {
        st.execute("CREATE VERTEX TYPE Issue7846ReleaseTest IF NOT EXISTS");
      }

      conn.setAutoCommit(false);
      try (var st = conn.createStatement()) {
        st.execute("INSERT INTO Issue7846ReleaseTest SET name = 'row'");
        final Savepoint sp = conn.setSavepoint("issue7846releaseSp");
        conn.releaseSavepoint(sp);
        conn.commit();
      }

      try (var st = conn.createStatement(); ResultSet rs = st.executeQuery("SELECT FROM Issue7846ReleaseTest")) {
        assertThat(rs.next()).isTrue();
      }
    }
  }

  @Test
  void rollbackToOnTheSimpleQueryProtocolReportsTheFeatureNotSupportedSqlState() throws Exception {
    try (var conn = getConnection()) {
      conn.setAutoCommit(false);
      try (var st = conn.createStatement()) {
        st.execute("BEGIN");
        st.execute("SAVEPOINT plainsp");
        assertThatThrownBy(() -> st.execute("ROLLBACK TO plainsp"))
            .isInstanceOf(PSQLException.class)
            .satisfies(e -> assertThat(((PSQLException) e).getSQLState()).isEqualTo("0A000"));
        st.execute("ROLLBACK");
      }
    }
  }
}
