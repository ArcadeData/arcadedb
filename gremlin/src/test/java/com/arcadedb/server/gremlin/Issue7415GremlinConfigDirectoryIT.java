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
package com.arcadedb.server.gremlin;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Issue #7415: the Gremlin plugin reads {@code gremlin-server.yaml} from the server configuration directory
 * ({@code arcadedb.server.configDirectory}), not always from {@code <root>/config}.
 * <p>
 * The YAML placed in the configured directory names a graph whose properties point at a database that nothing else
 * creates, so that database existing after startup proves the plugin read that YAML.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue7415GremlinConfigDirectoryIT extends BaseGraphServerTest {
  private static final String DB_NAME    = "graph7415";
  private static final File   CONFIG_DIR = new File("./target/config7415").getAbsoluteFile();

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GremlinServer:com.arcadedb.server.gremlin.GremlinServerPlugin");
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    config.setValue(GlobalConfiguration.SERVER_CONFIG_DIRECTORY, CONFIG_DIR.getPath());
  }

  @Override
  protected void onBeforeStarting(final ArcadeDBServer server) {
    assertThat(server.getConfigPath()).isEqualTo(CONFIG_DIR.getPath());
    CONFIG_DIR.mkdirs();
    try {
      final File properties = new File(CONFIG_DIR, "gremlin-server.properties");
      final String dbDirectory = new File(
          server.getConfiguration().getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY), DB_NAME).getAbsolutePath();
      FileUtils.writeFile(properties,
          "gremlin.graph=com.arcadedb.gremlin.ArcadeGraph\ngremlin.arcadedb.directory=" + dbDirectory + "\n");

      final String yaml = FileUtils.readStreamAsString(getClass().getClassLoader().getResourceAsStream("gremlin-server.yaml"), "utf8")
          .replace("config/gremlin-server.properties", properties.getAbsolutePath());
      FileUtils.writeFile(new File(CONFIG_DIR, "gremlin-server.yaml"), yaml);
    } catch (final IOException e) {
      fail("Failed to write Gremlin config files", e);
    }
  }

  @Override
  protected boolean isCreateDatabases() {
    return false;
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    try {
      super.endTest();
    } finally {
      FileUtils.deleteRecursively(CONFIG_DIR);
    }
  }

  @Test
  void theGremlinYamlIsReadFromTheConfiguredDirectory() {
    assertThat(new File(CONFIG_DIR, "server-users.jsonl")).exists();
    assertThat(getServer(0).existsDatabase(DB_NAME))
        .as("database '%s' is named only by the gremlin-server.yaml in the configured config directory", DB_NAME)
        .isTrue();
  }
}
