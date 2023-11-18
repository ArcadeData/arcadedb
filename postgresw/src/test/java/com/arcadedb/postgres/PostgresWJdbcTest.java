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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.postgresql.util.PSQLException;

import java.sql.*;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

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
          Assertions.fail("The query should go in error");
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
          Assertions.fail("The query should go in error");
        } catch (final PSQLException e) {
          Assertions.assertTrue(e.toString().contains("Syntax error"));
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

        final ResultSet rs = st.executeQuery("{gremlin}g.V().limit(10)");
      }
    }
  }

  @Test
  public void queryVertices() throws Exception {
    final int TOTAL = 1000;
    try (final Connection conn = getConnection()) {
      try (final Statement st = conn.createStatement()) {
        st.execute("create vertex type V");
        for (int i = 0; i < TOTAL; i++) {
          st.execute("create vertex V set id = " + i + ", name = 'Jay', lastName = 'Miner'");
        }

        final PreparedStatement pst = conn.prepareStatement(
            "create vertex V set name = ?, lastName = ?, short = ?, int = ?, long = ?, float = ?, double = ?, boolean = ?");
        pst.setString(1, "Rocky");
        pst.setString(2, "Balboa");
        pst.setShort(3, (short) 3);
        pst.setInt(4, 4);
        pst.setLong(5, 5L);
        pst.setFloat(6, 6F);
        pst.setDouble(7, 7D);
        pst.setBoolean(8, false);
        pst.execute();
        pst.close();

        ResultSet rs = st.executeQuery("SELECT name, lastName, short, int, long, float, double, boolean FROM V order by id");

        Assertions.assertTrue(!rs.isAfterLast());

        int i = 0;
        while (rs.next()) {
          if (rs.getString(1).equalsIgnoreCase("Jay")) {
            Assertions.assertEquals("Jay", rs.getString(1));
            Assertions.assertEquals("Miner", rs.getString(2));
            ++i;
          } else if (rs.getString(1).equalsIgnoreCase("Rocky")) {
            Assertions.assertEquals("Balboa", rs.getString(2));
            Assertions.assertEquals((short) 3, rs.getShort(3));
            Assertions.assertEquals(4, rs.getInt(4));
            Assertions.assertEquals(5L, rs.getLong(5));
            Assertions.assertEquals(6F, rs.getFloat(6));
            Assertions.assertEquals(7D, rs.getDouble(7));
            Assertions.assertEquals(false, rs.getBoolean(8));
            ++i;
          } else
            Assertions.fail("Unknown value");
        }

        Assertions.assertEquals(TOTAL + 1, i);

        rs.close();

        rs = st.executeQuery("SELECT FROM V order by id");

        Assertions.assertTrue(!rs.isAfterLast());

        i = 0;
        while (rs.next()) {
          Assertions.assertTrue(rs.findColumn("@rid") > -1);
          Assertions.assertTrue(rs.findColumn("@type") > -1);
          Assertions.assertTrue(rs.findColumn("@cat") > -1);

          Assertions.assertTrue(rs.getString(rs.findColumn("@rid")).startsWith("#"));
          Assertions.assertEquals("V", rs.getString(rs.findColumn("@type")));
          Assertions.assertEquals("v", rs.getString(rs.findColumn("@cat")));

          ++i;
        }

        Assertions.assertEquals(TOTAL + 1, i);

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

        Assertions.assertTrue(!rs.isAfterLast());

        int i = 0;
        while (rs.next()) {
          if (rs.getString(1).equalsIgnoreCase("Jay")) {
            Assertions.assertEquals("Jay", rs.getString(1));
            Assertions.assertEquals("Miner", rs.getString(2));
            ++i;
          } else if (rs.getString(1).equalsIgnoreCase("Rocky")) {
            Assertions.assertEquals("Rocky", rs.getString(1));
            Assertions.assertEquals("Balboa", rs.getString(2));
            ++i;
          } else
            Assertions.fail("Unknown value");
        }

        st.execute("commit");

        Assertions.assertEquals(2, i);

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
            Assertions.assertNotNull(rs.getString(1));

            if (rs.getString(1).equals("James"))
              Assertions.assertEquals(1.9F, rs.getFloat(2));
            else if (rs.getString(1).equals("Henry"))
              Assertions.assertNull(rs.getString(2));
            else
              Assertions.fail();

            ++numberOfPeople;
          }

          Assertions.assertEquals(2, numberOfPeople);
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
