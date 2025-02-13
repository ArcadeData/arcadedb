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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.postgresql.util.PSQLException;

import java.sql.*;
import java.time.LocalDate;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class PostgresWJdbcTest extends BaseGraphServerTest {
  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue(
        "Postgres:com.arcadedb.postgres.PostgresProtocolPlugin,GremlinServer:com.arcadedb.server.gremlin.GremlinServerPlugin");
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  @Test
  public void testTypeNotExistsErrorManagement() throws Exception {
    try (final Connection conn = getConnection()) {
      try (final Statement st = conn.createStatement()) {
        try {
          st.executeQuery("SELECT * FROM V");
          fail("The query should go in error");
        } catch (final PSQLException e) {
        }
      }
    }
  }

  @Test
  public void testParsingErrorMgmt() throws Exception {
    try (final Connection conn = getConnection()) {
      try (final Statement st = conn.createStatement()) {
        try {
          st.executeQuery("SELECT 'abc \\u30 def';");
          fail("The query should go in error");
        } catch (final PSQLException e) {
          assertThat(e.toString().contains("Syntax error")).isTrue();
        }
      }
    }
  }

  @Test
  void testGremlinQuery() throws Exception {
    try (final Connection conn = getConnection()) {
      conn.setAutoCommit(false);
      try (final Statement st = conn.createStatement()) {
        st.execute("create vertex type V");
        for (int i = 0; i < 11; i++) {
          st.execute("create vertex V set id = " + i + ", name = 'Jay', lastName = 'Miner'");
        }

        final ResultSet rs = st.executeQuery("{gremlin}g.V().hasLabel('V').order().by('id').limit(10)");
        assertThat(rs.next()).isTrue();
        assertThat(rs.getInt("id")).isEqualTo(0);
        assertThat(rs.getString("name")).isEqualTo("Jay");
        assertThat(rs.getString("lastName")).isEqualTo("Miner");
      }
    }
  }

  @Test
  void testScript() throws Exception {
    try (final Connection conn = getConnection()) {
      conn.setAutoCommit(false);
      try (final Statement st = conn.createStatement()) {
        st.execute("""
            {sqlscript}create vertex type V IF NOT EXISTS;
            create vertex V set id = 0, name = 'Jay', lastName = 'Miner';
            create vertex V set id = 1, name = 'Jay', lastName = 'Miner';
            create vertex V set id = 2, name = 'Jay', lastName = 'Miner';
            create vertex V set id = 3, name = 'Jay', lastName = 'Miner';
            create vertex V set id = 4, name = 'Jay', lastName = 'Miner';
            create vertex V set id = 5, name = 'Jay', lastName = 'Miner';
            create vertex V set id = 6, name = 'Jay', lastName = 'Miner';
            create vertex V set id = 7, name = 'Jay', lastName = 'Miner';
            create vertex V set id = 8, name = 'Jay', lastName = 'Miner';
            create vertex V set id = 9, name = 'Jay', lastName = 'Miner';
            create vertex V set id = 10, name = 'Jay', lastName = 'Miner';
            """);

        final ResultSet rs = st.executeQuery("{gremlin}g.V().hasLabel('V').order().by('id').limit(10)");
        assertThat(rs.next()).isTrue();
        assertThat(rs.getInt("id")).isEqualTo(0);
        assertThat(rs.getString("name")).isEqualTo("Jay");
        assertThat(rs.getString("lastName")).isEqualTo("Miner");
      }
    }
  }

  @Test
  public void queryVertices() throws Exception {
    final int TOTAL = 1000;
    final long now = System.currentTimeMillis();

    try (final Connection conn = getConnection()) {
      try (final Statement st = conn.createStatement()) {
        st.execute("create vertex type V");
        for (int i = 0; i < TOTAL; i++) {
          st.execute("create vertex V set id = " + i + ", name = 'Jay', lastName = 'Miner'");
        }

        final PreparedStatement pst = conn.prepareStatement(
            "create vertex V set name = ?, lastName = ?, short = ?, int = ?, long = ?, float = ?, double = ?, boolean = ?, date = ?, timestamp = ?");
        pst.setString(1, "Rocky");
        pst.setString(2, "Balboa");
        pst.setShort(3, (short) 3);
        pst.setInt(4, 4);
        pst.setLong(5, 5L);
        pst.setFloat(6, 6F);
        pst.setDouble(7, 7D);
        pst.setBoolean(8, false);
        pst.setDate(9, new java.sql.Date(now));
        pst.setTimestamp(10, new java.sql.Timestamp(now));
        pst.execute();
        pst.close();

        ResultSet rs = st.executeQuery(
            "SELECT name, lastName, short, int, long, float, double, boolean, date, timestamp FROM V order by id");

        assertThat(rs.isAfterLast()).isFalse();

        int i = 0;
        while (rs.next()) {
          if (rs.getString(1).equalsIgnoreCase("Jay")) {
            assertThat(rs.getString(1)).isEqualTo("Jay");
            assertThat(rs.getString(2)).isEqualTo("Miner");
            ++i;
          } else if (rs.getString(1).equalsIgnoreCase("Rocky")) {
            assertThat(rs.getString(2)).isEqualTo("Balboa");
            assertThat(rs.getShort(3)).isEqualTo((short) 3);
            assertThat(rs.getInt(4)).isEqualTo(4);
            assertThat(rs.getLong(5)).isEqualTo(5L);
            assertThat(rs.getFloat(6)).isEqualTo(6F);
            assertThat(rs.getDouble(7)).isEqualTo(7D);
            assertThat(rs.getBoolean(8)).isFalse();
            assertThat(rs.getDate(9).toLocalDate()).isEqualTo(LocalDate.now());
            assertThat(rs.getTimestamp(10).getTime()).isEqualTo(now);
            ++i;
          } else
            fail("Unknown value");
        }

        assertThat(i).isEqualTo(TOTAL + 1);

        rs.close();

        rs = st.executeQuery("SELECT FROM V order by id");

        assertThat(rs.isAfterLast()).isFalse();

        i = 0;
        while (rs.next()) {
          assertThat(rs.findColumn("@rid") > -1).isTrue();
          assertThat(rs.findColumn("@type") > -1).isTrue();
          assertThat(rs.findColumn("@cat") > -1).isTrue();

          assertThat(rs.getString(rs.findColumn("@rid")).startsWith("#")).isTrue();
          assertThat(rs.getString(rs.findColumn("@type"))).isEqualTo("V");
          assertThat(rs.getString(rs.findColumn("@cat"))).isEqualTo("v");

          ++i;
        }

        assertThat(i).isEqualTo(TOTAL + 1);

      }
    }
  }

  @Test
  public void queryTransaction() throws Exception {
    try (final Connection conn = getConnection()) {
      conn.setAutoCommit(false);
      try (final Statement st = conn.createStatement()) {
        st.execute("begin");
        st.execute("create vertex type V");
        st.execute("create vertex V set name = 'Jay', lastName = 'Miner'");

        final PreparedStatement pst = conn.prepareStatement("create vertex V set name = ?, lastName = ?");
        pst.setString(1, "Rocky");
        pst.setString(2, "Balboa");
        pst.execute();
        pst.close();

        final ResultSet rs = st.executeQuery("SELECT * FROM V");

        assertThat(rs.isAfterLast()).isFalse();

        int i = 0;
        while (rs.next()) {
          if (rs.getString(1).equalsIgnoreCase("Jay")) {
            assertThat(rs.getString(1)).isEqualTo("Jay");
            assertThat(rs.getString(2)).isEqualTo("Miner");
            ++i;
          } else if (rs.getString(1).equalsIgnoreCase("Rocky")) {
            assertThat(rs.getString(1)).isEqualTo("Rocky");
            assertThat(rs.getString(2)).isEqualTo("Balboa");
            ++i;
          } else
            fail("Unknown value");
        }

        st.execute("commit");

        assertThat(i).isEqualTo(2);

        rs.close();
      }
    }
  }

  @Test
  void testCypher() throws Exception {
    try (final Connection conn = getConnection()) {
      conn.setAutoCommit(false);

      try (final Statement st = conn.createStatement()) {
        st.execute("CREATE VERTEX TYPE PersonVertex;");

        for (int i = 0; i < 100; i++) {
          st.execute("{cypher} MATCH (n) DETACH DELETE n;");
          st.execute("{cypher} CREATE (james:PersonVertex {name: \"James\", height: 1.9});");
          st.execute("{cypher} CREATE (henry:PersonVertex {name: \"Henry\"});");

          final ResultSet rs = st.executeQuery("{cypher} MATCH (person:PersonVertex) RETURN person.name, person.height;");

          int numberOfPeople = 0;
          while (rs.next()) {
            assertThat(rs.getString(1)).isNotNull();

            if (rs.getString(1).equals("James"))
              assertThat(rs.getFloat(2)).isEqualTo(1.9F);
            else if (rs.getString(1).equals("Henry"))
              assertThat(rs.getString(2)).isNull();
            else
              fail("");

            ++numberOfPeople;
          }

          assertThat(numberOfPeople).isEqualTo(2);
          st.execute("commit");
        }
      }
    }
  }

  /**
   * Issue https://github.com/ArcadeData/arcadedb/issues/1325#issuecomment-1816145926
   *
   * @throws Exception
   */
  @Test
  public void showTxIsolationLevel() throws Exception {
    try (final Connection conn = getConnection()) {
      try (final Statement st = conn.createStatement()) {
        try (final ResultSet rs = st.executeQuery("SHOW TRANSACTION ISOLATION LEVEL")) {
          assertThat(rs.next()).isTrue();
        }
      }
    }
  }

  @Test
  public void testISODateFormat() throws Exception {
    try (final Connection conn = getConnection()) {
      try (final Statement st = conn.createStatement()) {
        st.execute("SET datestyle TO 'ISO'");
      }
    }
  }

  @Test
  @Disabled
  public void testWaitForConnectionFromExternal() throws InterruptedException {
    Thread.sleep(1000000);
  }

  private Connection getConnection() throws ClassNotFoundException, SQLException {
    Class.forName("org.postgresql.Driver");

    final String url = "jdbc:postgresql://localhost/" + getDatabaseName();
    final Properties props = new Properties();
    props.setProperty("user", "root");
    props.setProperty("password", DEFAULT_PASSWORD_FOR_TESTS);
    props.setProperty("ssl", "false");
    final Connection conn = DriverManager.getConnection(url, props);
    return conn;
  }

  protected String getDatabaseName() {
    return "postgresdb";
  }
}
