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
package com.arcadedb.mongo;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.BaseGraphServerTest;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5796: {@link MongoDBProtocolPlugin#configure(com.arcadedb.server.ArcadeDBServer, ContextConfiguration)}
 * used to drop the server's {@link ContextConfiguration} argument entirely and {@link MongoDBProtocolPlugin#startService()}
 * bound the static {@link GlobalConfiguration#MONGO_HOST}/{@link GlobalConfiguration#MONGO_PORT} defaults instead. This meant
 * a custom port configured on the server was silently discarded and the plugin always bound the hardcoded default (27017).
 */
public class MongoDBPortConfigurationTest extends BaseGraphServerTest {

  private static int customPort;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("MongoDB:com.arcadedb.mongo.MongoDBProtocolPlugin");
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    try (final ServerSocket probe = new ServerSocket(0)) {
      customPort = probe.getLocalPort();
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
    config.setValue(GlobalConfiguration.MONGO_PORT, customPort);
  }

  @Test
  void pluginBindsTheConfiguredPortNotTheDefault() {
    // The plugin must be reachable on the port configured via ContextConfiguration...
    try (final MongoClient client = new MongoClient(new ServerAddress("localhost", customPort),
        MongoCredential.createPlainCredential("root", getDatabaseName(), DEFAULT_PASSWORD_FOR_TESTS.toCharArray()),
        MongoClientOptions.builder().serverSelectionTimeout(5000).build())) {
      final Document pingResult = client.getDatabase(getDatabaseName()).runCommand(new Document("ping", 1));
      assertThat(pingResult.getDouble("ok")).isEqualTo(1.0);
    }

    // ...and must NOT have silently fallen back to the hardcoded default port.
    final int defaultPort = GlobalConfiguration.MONGO_PORT.getValueAsInteger();
    assertThat(isListening(defaultPort))
        .as("MongoDB plugin must not bind the default port %d when a custom port %d is configured", defaultPort, customPort)
        .isFalse();
  }

  private static boolean isListening(final int port) {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("127.0.0.1", port), 500);
      return true;
    } catch (final IOException e) {
      return false;
    }
  }

  @Override
  protected void populateDatabase() {
    // NO NEED FOR TEST DATA, THIS TEST ONLY EXERCISES THE PLUGIN'S NETWORK BINDING
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }
}
