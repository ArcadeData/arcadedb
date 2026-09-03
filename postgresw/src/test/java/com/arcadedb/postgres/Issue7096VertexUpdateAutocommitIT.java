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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #7096: on the Postgres wire port an {@code UPDATE} against a vertex type failed with
 * "Transaction not active" unless the client had opened an explicit transaction, while the same statement against a
 * document or an edge type, and every {@code INSERT}/{@code CREATE VERTEX}/{@code DELETE}, ran fine in autocommit.
 * Autocommit is the JDBC default and what Spark's Postgres connector uses for one-shot DML, so the asymmetry made
 * vertex updates unusable from those clients.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7096VertexUpdateAutocommitIT extends BaseGraphServerTest {

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
    final Properties props = new Properties();
    props.setProperty("user", "root");
    props.setProperty("password", DEFAULT_PASSWORD_FOR_TESTS);
    props.setProperty("ssl", "false");
    return DriverManager.getConnection("jdbc:postgresql://localhost/" + getDatabaseName(), props);
  }

  @Test
  @DisplayName("[#7096] a scalar vertex UPDATE runs in autocommit, exactly like a document UPDATE")
  void scalarVertexUpdateInAutocommit() throws Exception {
    try (final Connection conn = getConnection(); final Statement st = conn.createStatement()) {
      assertThat(conn.getAutoCommit()).isTrue();
      st.execute("CREATE VERTEX TYPE Character7096 IF NOT EXISTS");
      st.execute("CREATE VERTEX Character7096 SET name = 'Napoleon'");

      st.execute("UPDATE Character7096 SET name = 'Bonaparte' WHERE name = 'Napoleon'");

      try (final ResultSet rs = st.executeQuery("SELECT name FROM Character7096")) {
        assertThat(rs.next()).isTrue();
        assertThat(rs.getString("name")).isEqualTo("Bonaparte");
        assertThat(rs.next()).isFalse();
      }
    }
  }

  @Test
  @DisplayName("[#7096] a vertex UPDATE assigning LIST and MAP literals runs in autocommit")
  void containerVertexUpdateInAutocommit() throws Exception {
    try (final Connection conn = getConnection(); final Statement st = conn.createStatement()) {
      st.execute("CREATE VERTEX TYPE Character7096 IF NOT EXISTS");
      st.execute("CREATE VERTEX Character7096 SET name = 'Napoleon'");

      st.execute("UPDATE Character7096 SET aliases = ['Emperor','Bonaparte'], attrs = {'title':'Emperor','nation':'France'} "
          + "WHERE name = 'Napoleon'");

      try (final ResultSet rs = st.executeQuery("SELECT aliases, attrs FROM Character7096 WHERE name = 'Napoleon'")) {
        assertThat(rs.next()).isTrue();
        assertThat(rs.getString("aliases")).contains("Emperor").contains("Bonaparte");
        assertThat(rs.getString("attrs")).contains("France");
      }
    }
  }

  @Test
  @DisplayName("[#7096] the same vertex UPDATE through the extended protocol (prepared statement) in autocommit")
  void preparedVertexUpdateInAutocommit() throws Exception {
    try (final Connection conn = getConnection(); final Statement st = conn.createStatement()) {
      st.execute("CREATE VERTEX TYPE Character7096 IF NOT EXISTS");
      st.execute("CREATE VERTEX Character7096 SET name = 'Napoleon'");

      try (final PreparedStatement ps = conn.prepareStatement("UPDATE Character7096 SET name = ? WHERE name = ?")) {
        ps.setString(1, "Bonaparte");
        ps.setString(2, "Napoleon");
        // execute(), not executeUpdate(): ArcadeDB answers a DML statement with its count row, which pgJDBC's
        // executeUpdate() refuses as "a result was returned when none was expected" on every wire path, not just this one.
        ps.execute();
      }

      try (final ResultSet rs = st.executeQuery("SELECT name FROM Character7096")) {
        assertThat(rs.next()).isTrue();
        assertThat(rs.getString("name")).isEqualTo("Bonaparte");
      }
    }
  }

  @Test
  @DisplayName("[#7096] a vertex UPDATE followed by an edge UPDATE and a DELETE, all in autocommit on one connection")
  void mixedDmlInAutocommit() throws Exception {
    try (final Connection conn = getConnection(); final Statement st = conn.createStatement()) {
      st.execute("CREATE VERTEX TYPE Character7096 IF NOT EXISTS");
      st.execute("CREATE EDGE TYPE Appearance7096 IF NOT EXISTS");
      st.execute("CREATE VERTEX Character7096 SET name = 'Napoleon'");
      st.execute("CREATE VERTEX Character7096 SET name = 'Myriel'");
      st.execute("CREATE EDGE Appearance7096 FROM (SELECT FROM Character7096 WHERE name = 'Napoleon') "
          + "TO (SELECT FROM Character7096 WHERE name = 'Myriel') SET weight = 1");

      st.execute("UPDATE Character7096 SET rank = 1 WHERE name = 'Napoleon'");
      st.execute("UPDATE Appearance7096 SET weight = 2");
      st.execute("UPDATE Character7096 SET rank = 2");
      st.execute("DELETE FROM Character7096 WHERE name = 'Myriel'");

      try (final ResultSet rs = st.executeQuery("SELECT name, rank FROM Character7096")) {
        assertThat(rs.next()).isTrue();
        assertThat(rs.getString("name")).isEqualTo("Napoleon");
        assertThat(rs.getInt("rank")).isEqualTo(2);
        assertThat(rs.next()).isFalse();
      }
    }
  }
}
