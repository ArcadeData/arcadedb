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
package com.arcadedb.bolt;

import com.arcadedb.GlobalConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression for issue #8209: the BOLT tests used to start the plugin on the production port 7687 and to connect to
 * that fixed port, so anything already listening there (a developer's own Neo4j or ArcadeDB, a concurrent build,
 * another agent) failed the module or answered the tests' connections.
 * <p>
 * This test holds 7687 itself before the server starts, which is exactly the situation the issue describes: the
 * server must still start, on an operating-system-assigned port, be reachable there, and advertise that port rather
 * than the configured {@code 0}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue8209BoltEphemeralPortIT extends BaseBoltServerTest {
  private static final int PRODUCTION_DEFAULT_PORT = 7687;

  private ServerSocket squatter;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("Bolt:com.arcadedb.bolt.BoltProtocolPlugin");
    try {
      squatter = new ServerSocket(PRODUCTION_DEFAULT_PORT, 1, InetAddress.getLoopbackAddress());
    } catch (final IOException e) {
      // Something already holds 7687, which is the condition this test reproduces anyway
      squatter = null;
    }
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    try {
      super.endTest();
    } finally {
      if (squatter != null) {
        try {
          squatter.close();
        } catch (final IOException e) {
          // IGNORE
        }
      }
    }
  }

  private Driver getDriver() {
    return GraphDatabase.driver(getServerBoltUrl(), AuthTokens.basic("root", DEFAULT_PASSWORD_FOR_TESTS),
        Config.builder().withoutEncryption().build());
  }

  @Test
  void serverStartsOnAnEphemeralPortWhileTheProductionPortIsTaken() {
    assertThat(getServer(0).isStarted()).isTrue();
    assertThat(getServerBoltPort()).isGreaterThan(0);
    assertThat(getServerBoltPort()).isNotEqualTo(PRODUCTION_DEFAULT_PORT);

    try (final Driver driver = getDriver(); final Session session = driver.session(
        SessionConfig.forDatabase(getDatabaseName()))) {
      final List<Record> rows = session.run("RETURN 1 AS one").list();
      assertThat(rows).hasSize(1);
      assertThat(rows.getFirst().get("one").asLong()).isEqualTo(1L);
    }
  }

  /**
   * Only the tests run on an ephemeral port: the production default is untouched.
   */
  @Test
  void productionDefaultIsUnchanged() {
    assertThat(GlobalConfiguration.BOLT_PORT.getDefValue()).isEqualTo(PRODUCTION_DEFAULT_PORT);
  }

  /**
   * The configured port is {@code 0}, which nobody can dial: the advertised address must carry the port the listener
   * actually bound.
   */
  @Test
  void showDatabasesAdvertisesTheBoundPort() {
    try (final Driver driver = getDriver(); final Session session = driver.session(SessionConfig.forDatabase("system"))) {
      final List<Record> rows = session.run("SHOW DATABASES").list();
      assertThat(rows).isNotEmpty();
      for (final Record row : rows)
        assertThat(row.get("address").asString()).endsWith(":" + getServerBoltPort());
    }
  }
}
