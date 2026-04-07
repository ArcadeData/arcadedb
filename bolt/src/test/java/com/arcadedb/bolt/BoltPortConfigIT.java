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
import com.arcadedb.test.BaseGraphServerTest;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Record;

import java.io.IOException;
import java.net.ConnectException;
import java.net.Socket;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that the Bolt protocol server binds to the configured port and that the port
 * is configurable via {@link GlobalConfiguration#BOLT_PORT}.
 * <p>
 * Regression test for https://github.com/ArcadeData/arcadedb/issues/3809
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class BoltPortConfigIT extends BaseGraphServerTest {

  private static final int CUSTOM_BOLT_PORT = 17687;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("Neo4j-Bolt:com.arcadedb.bolt.BoltProtocolPlugin");
    GlobalConfiguration.BOLT_PORT.setValue(CUSTOM_BOLT_PORT);
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    GlobalConfiguration.BOLT_PORT.reset();
    super.endTest();
  }

  @Test
  void boltServerListensOnConfiguredPort() {
    // Verify the custom port is reachable
    assertThat(isPortOpen(CUSTOM_BOLT_PORT)).isTrue();
  }

  @Test
  void boltServerDoesNotListenOnDefaultPort() {
    // Verify the default port 7687 is NOT open (we configured a custom port)
    assertThat(isPortOpen(7687)).isFalse();
  }

  @Test
  void connectionOnConfiguredPort() {
    try (Driver driver = GraphDatabase.driver(
        "bolt://localhost:" + CUSTOM_BOLT_PORT,
        AuthTokens.basic("root", DEFAULT_PASSWORD_FOR_TESTS),
        Config.builder().withoutEncryption().build())) {
      driver.verifyConnectivity();
    }
  }

  @Test
  void queryOnConfiguredPort() {
    try (Driver driver = GraphDatabase.driver(
        "bolt://localhost:" + CUSTOM_BOLT_PORT,
        AuthTokens.basic("root", DEFAULT_PASSWORD_FOR_TESTS),
        Config.builder().withoutEncryption().build())) {
      try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        final Result result = session.run("RETURN 42 AS answer");
        assertThat(result.hasNext()).isTrue();
        final Record record = result.next();
        assertThat(record.get("answer").asLong()).isEqualTo(42L);
      }
    }
  }

  @Test
  void pluginDiscoveredByName() {
    // Verify the plugin was loaded using its getName() ("Neo4j-Bolt")
    // by checking that it actually listens on the configured port.
    // If the name matching didn't work, the plugin wouldn't load and
    // the connection would fail.
    try (Driver driver = GraphDatabase.driver(
        "bolt://localhost:" + CUSTOM_BOLT_PORT,
        AuthTokens.basic("root", DEFAULT_PASSWORD_FOR_TESTS),
        Config.builder().withoutEncryption().build())) {
      driver.verifyConnectivity();
      try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        final Result result = session.run("RETURN 'port-config-works' AS status");
        assertThat(result.hasNext()).isTrue();
        assertThat(result.next().get("status").asString()).isEqualTo("port-config-works");
      }
    }
  }

  private static boolean isPortOpen(final int port) {
    try (Socket socket = new Socket("localhost", port)) {
      return true;
    } catch (final ConnectException e) {
      return false;
    } catch (final IOException e) {
      return false;
    }
  }
}
