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
package com.arcadedb.mcp;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.ArcadeDBServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7415: the MCP plugin keeps {@code mcp-config.json} in the server configuration directory
 * ({@code arcadedb.server.configDirectory}), not always under {@code <root>/config}.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue7415MCPConfigDirectoryTest {

  @Test
  void theConfigurationIsWrittenToTheGivenDirectory(@TempDir final Path tempDir) {
    final Path configDir = tempDir.resolve("config7415");

    final MCPConfiguration config = new MCPConfiguration(configDir);
    config.load();

    assertThat(configDir.resolve("mcp-config.json")).exists();
    assertThat(tempDir.resolve("config").resolve("mcp-config.json")).doesNotExist();
  }

  @Test
  void theRootPathConstructorKeepsTheDefaultLayout(@TempDir final Path tempDir) {
    final MCPConfiguration config = new MCPConfiguration(tempDir.toString());
    config.load();

    assertThat(tempDir.resolve("config").resolve("mcp-config.json")).exists();
  }

  @Test
  void thePluginUsesTheServerConfigDirectory(@TempDir final Path tempDir) throws Exception {
    final Path root = tempDir.resolve("root");
    final Path configDir = tempDir.resolve("volume").resolve("config");
    Files.createDirectories(root);

    final ContextConfiguration serverConfig = new ContextConfiguration();
    serverConfig.setValue(GlobalConfiguration.SERVER_ROOT_PATH, root.toString());
    serverConfig.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, root.resolve("databases").toString());
    serverConfig.setValue(GlobalConfiguration.SERVER_CONFIG_DIRECTORY, configDir.toString());

    // Not started: configure() only needs the server's paths.
    final ArcadeDBServer server = new ArcadeDBServer(serverConfig);

    final MCPPlugin plugin = new MCPPlugin();
    plugin.configure(server, serverConfig);

    assertThat(configDir.resolve("mcp-config.json")).exists();
    assertThat(root.resolve("config").resolve("mcp-config.json")).doesNotExist();
  }
}
