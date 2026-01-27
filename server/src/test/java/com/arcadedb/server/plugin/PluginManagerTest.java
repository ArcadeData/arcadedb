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
package com.arcadedb.server.plugin;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerPlugin;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Collection;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test for PluginManager to verify plugin discovery and loading with isolated class loaders.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class PluginManagerTest {
  private ArcadeDBServer server;
  private PluginManager  pluginManager;

  @BeforeEach
  public void setup() {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.SERVER_ROOT_PATH, "./target/test-server");
    configuration.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, "./target/test-server/databases");

    server = new ArcadeDBServer(configuration);
    pluginManager = new PluginManager(server, configuration);
  }

  @AfterEach
  public void teardown() {
    if (server != null && server.isStarted()) {
      server.stop();
    }
  }

  @Test
  public void testPluginManagerCreation() {
    assertNotNull(pluginManager);
    assertEquals(0, pluginManager.getPluginCount());
  }

  @Test
  public void testDiscoverPluginsWithNoDirectory() {
    // Should handle missing plugins directory gracefully
    pluginManager.discoverPlugins();
    assertEquals(0, pluginManager.getPluginCount());
  }

  @Test
  public void testGetPluginNames() {
    final Collection<String> names = pluginManager.getPluginNames();
    assertNotNull(names);
    assertTrue(names.isEmpty());
  }

  @Test
  public void testGetPlugins() {
    final Collection<ServerPlugin> plugins = pluginManager.getPlugins();
    assertNotNull(plugins);
    assertTrue(plugins.isEmpty());
  }

  @Test
  public void testStopPluginsWhenEmpty() {
    // Should handle stopping with no plugins loaded
    assertDoesNotThrow(() -> pluginManager.stopPlugins());
  }
}
