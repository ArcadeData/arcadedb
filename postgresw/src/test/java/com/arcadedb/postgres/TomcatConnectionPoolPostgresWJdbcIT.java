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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThat;

class TomcatConnectionPoolPostgresWJdbcIT extends BaseGraphServerTest {
  private DataSource datasource;

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

  @Override
  protected String getDatabaseName() {
    return "postgresdb";
  }


  @BeforeEach
  void setUp() {
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
        """
            org.apache.tomcat.jdbc.pool.interceptor.ConnectionState;
            org.apache.tomcat.jdbc.pool.interceptor.StatementFinalizer""");
    datasource = new DataSource();
    datasource.setPoolProperties(p);

  }

  @Test
  public void testConnectionFromPool() throws SQLException {

    try (var conn = datasource.getConnection()) {
      conn.setAutoCommit(false);

      assertThat(datasource.getActive()).isEqualTo(1) ;

      try (var st = conn.createStatement()) {
        st.execute("create vertex type V");
        for (int i = 0; i < 11; i++) {
          st.execute("create vertex V set id = " + i + ", name = 'Jay', lastName = 'Miner'");
        }

        var rs = st.executeQuery("SELECT FROM V order by id");
        assertThat(rs.next()).isTrue();
        assertThat(rs.getInt("id")).isEqualTo(0);
        assertThat(rs.getString("name")).isEqualTo("Jay");
        assertThat(rs.getString("lastName")).isEqualTo("Miner");
      }
    }

    assertThat(datasource.getActive()).isEqualTo(0);
  }

}
