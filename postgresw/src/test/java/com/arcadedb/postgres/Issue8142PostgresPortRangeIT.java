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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8142: the Postgres plugin used to bind the single port 5432, so anything already listening there - a
 * developer's own server, a second concurrent build - failed every IT of the module at {@code @BeforeEach}. The
 * test base now starts the plugin on a range and the tests connect to the port the listener actually bound.
 * <p>
 * This class occupies the FIRST port of the configured range before the server starts, which is exactly the
 * situation a concurrent run creates: the server must still start, on another port, and be reachable there.
 */
class Issue8142PostgresPortRangeIT extends PostgresWireProtocolTestBase {
  private ServerSocket squatter;
  private int          squattedPort;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    // Runs before the servers start, and after the base class configured the range.
    final String range = GlobalConfiguration.POSTGRES_PORT.getValueAsString();
    squattedPort = Integer.parseInt(range.split("[-,]")[0].trim());
    try {
      squatter = new ServerSocket(squattedPort, 0, InetAddress.getByName(GlobalConfiguration.POSTGRES_HOST.getValueAsString()));
    } catch (final IOException e) {
      // Something else holds it already: the port is occupied either way, which is all this test needs.
      squatter = null;
    }
  }

  @AfterEach
  @Override
  public void endTest() {
    try {
      super.endTest();
    } finally {
      if (squatter != null)
        try {
          squatter.close();
        } catch (final IOException e) {
          // IGNORE IT
        }
    }
  }

  @Test
  void serverStartsOnTheNextFreePortOfTheRangeAndIsReachableThere() throws Exception {
    final int port = getServerPostgresPort();
    assertThat(port).isNotEqualTo(squattedPort);
    assertThat(port).isBetween(5433, 5442);

    final Properties properties = new Properties();
    properties.setProperty("user", "root");
    properties.setProperty("password", DEFAULT_PASSWORD_FOR_TESTS);
    properties.setProperty("ssl", "false");
    try (final Connection conn = DriverManager.getConnection(getServerPostgresJdbcUrl(), properties);
        final Statement st = conn.createStatement();
        final ResultSet rs = st.executeQuery("SELECT 1 AS one")) {
      assertThat(rs.next()).isTrue();
      assertThat(rs.getInt("one")).isEqualTo(1);
    }
  }

  @Test
  void testRangeLeavesOutTheProductionDefault() {
    // Clients expect 5432 from a real server, so the production default does not move...
    assertThat(GlobalConfiguration.POSTGRES_PORT.getDefValue()).isEqualTo("5432");
    // ...but a test server never uses it: a stranger listening on localhost:5432 only does not stop a 0.0.0.0 bind
    // on macOS, and would then answer every connection the test makes.
    final String[] limits = GlobalConfiguration.POSTGRES_PORT.getValueAsString().split("-");
    assertThat(Integer.parseInt(limits[0].trim())).isGreaterThan(5432);
    assertThat(Integer.parseInt(limits[1].trim())).isGreaterThan(Integer.parseInt(limits[0].trim()));
  }

  @Test
  void accessorRefusesToGuessWhenNothingIsListening() {
    assertThatThrownBy(() -> getServerPostgresPort(null)).isInstanceOf(IllegalStateException.class);
  }
}
