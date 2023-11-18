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
import org.apache.tomcat.jdbc.pool.DataSource;
import org.apache.tomcat.jdbc.pool.PoolProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.postgresql.util.PSQLException;

import java.sql.*;
import java.util.*;

public class TomcatConnectionPoolPostgresWJdbcTest extends BaseGraphServerTest {
  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue(
        "Postgres:com.arcadedb.postgres.PostgresProtocolPlugin,GremlinServer:com.arcadedb.server.gremlin.GremlinServerPlugin");
  }

  @Test
  public void testConnection() throws SQLException {
    PoolProperties p = new PoolProperties();
    p.setUrl("jdbc:postgresql://localhost/" + getDatabaseName());
    p.setDriverClassName("org.postgresql.Driver");
    p.setUsername("root");
    p.setPassword(DEFAULT_PASSWORD_FOR_TESTS);
    p.setJmxEnabled(true);
    p.setTestWhileIdle(false);
    p.setTestOnBorrow(true);
    p.setValidationQuery("SELECT 1");
    p.setTestOnReturn(false);
    p.setValidationInterval(30000);
    p.setTimeBetweenEvictionRunsMillis(30000);
    p.setMaxActive(100);
    p.setInitialSize(10);
    p.setMaxWait(10000);
    p.setRemoveAbandonedTimeout(60);
    p.setMinEvictableIdleTimeMillis(30000);
    p.setMinIdle(10);
    p.setLogAbandoned(true);
    p.setRemoveAbandoned(true);
    p.setJdbcInterceptors(
        "org.apache.tomcat.jdbc.pool.interceptor.ConnectionState;" + "org.apache.tomcat.jdbc.pool.interceptor.StatementFinalizer");
    DataSource datasource = new DataSource();
    datasource.setPoolProperties(p);

    Connection con = null;
    try {
      con = datasource.getConnection();
      Statement st = con.createStatement();
      ResultSet rs = st.executeQuery("select * from schema:database");
      int cnt = 1;
      while (rs.next()) {
        System.out.println((cnt++) + ". name:" + rs.getString("name"));
      }
      rs.close();
      st.close();
    } finally {
      if (con != null)
        try {
          con.close();
        } catch (Exception ignore) {
        }
    }
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
