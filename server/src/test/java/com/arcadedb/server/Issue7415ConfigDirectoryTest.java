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
package com.arcadedb.server;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.backup.AutoBackupConfig;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;

import static com.arcadedb.GlobalConfiguration.TX_WAL;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7415: the server configuration directory is configurable through {@code arcadedb.server.configDirectory},
 * so it can live on a persistent volume next to the databases instead of always under {@code <root>/config}.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue7415ConfigDirectoryTest extends StaticBaseServerTest {
  private ArcadeDBServer server;

  @AfterEach
  @Override
  public void endTest() {
    if (server != null && server.isStarted())
      server.stop();
    server = null;
    super.endTest();
  }

  @Test
  void theConfigDirectoryDefaultsToConfigUnderTheRootPath() {
    GlobalConfiguration.SERVER_CONFIG_DIRECTORY.reset();

    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, "./target/root7415");
    assertThat(ArcadeDBServer.resolveConfigPath(config, "./target/root7415")).isEqualTo("./target/root7415/config");

    config.setValue(GlobalConfiguration.SERVER_CONFIG_DIRECTORY, "  ");
    assertThat(ArcadeDBServer.resolveConfigPath(config, "./target/root7415"))
        .isEqualTo("./target/root7415" + File.separator + "config");

    config.setValue(GlobalConfiguration.SERVER_CONFIG_DIRECTORY, "/data/arcadedb/config");
    assertThat(ArcadeDBServer.resolveConfigPath(config, "./target/root7415")).isEqualTo("/data/arcadedb/config");
  }

  @Test
  @Timeout(120)
  void theServerReadsAndWritesItsConfigurationFilesInTheConfiguredDirectory(@TempDir final Path tempDir) throws Exception {
    final Path root = tempDir.resolve("root");
    final Path configDir = tempDir.resolve("volume").resolve("config");
    Files.createDirectories(root);

    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, root.toString());
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, root.resolve("databases").toString());
    config.setValue(GlobalConfiguration.SERVER_BACKUP_DIRECTORY, root.resolve("backups").toString());
    config.setValue(GlobalConfiguration.SERVER_CONFIG_DIRECTORY, configDir.toString());
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, DEFAULT_PASSWORD_FOR_TESTS);
    config.setValue(GlobalConfiguration.SERVER_HTTP_IO_THREADS, 2);

    server = new ArcadeDBServer(config);
    assertThat(server.getConfigPath()).isEqualTo(configDir.toString());

    server.start();
    assertThat(server.getStatus()).isEqualTo(ArcadeDBServer.STATUS.ONLINE);

    // Security: the root user created at first start lands in the configured directory, and nothing is written to
    // the default <root>/config.
    assertThat(configDir.resolve("server-users.jsonl")).exists();
    assertThat(root.resolve("config").resolve("server-users.jsonl")).doesNotExist();

    // AI configuration
    server.getAiConfiguration().save();
    assertThat(configDir.resolve("ai.json")).exists();

    // Auto-backup configuration, written and read back through the control plane
    final ServerControlPlane controlPlane = new ServerControlPlane(server);
    controlPlane.setBackupConfig(new JSONObject().put("enabled", false).put("backupDirectory", "backups7415"));
    assertThat(configDir.resolve(AutoBackupConfig.CONFIG_FILE_NAME)).exists();
    assertThat(root.resolve("config").resolve(AutoBackupConfig.CONFIG_FILE_NAME)).doesNotExist();

    final JSONObject backupConfig = controlPlane.getBackupConfig();
    assertThat(backupConfig.getJSONObject("config").getString("backupDirectory")).isEqualTo("backups7415");
    assertThat(controlPlane.resolveBackupDirectory()).isEqualTo(root.resolve("backups7415").toAbsolutePath().normalize());

    assertThat(root.resolve("config")).doesNotExist();
  }

  @Test
  void theServerConfigurationFileIsLoadedFromTheConfiguredDirectory(@TempDir final Path tempDir) throws Exception {
    final Path root = tempDir.resolve("root");
    final Path configDir = tempDir.resolve("config7415");
    Files.createDirectories(root.resolve("config"));
    Files.createDirectories(configDir);

    final ContextConfiguration fileContent = new ContextConfiguration();
    fileContent.setValue(TX_WAL, false);
    FileUtils.writeFile(configDir.resolve(ArcadeDBServer.SERVER_CONFIGURATION_FILE_NAME).toFile(), fileContent.toJSON());

    // A decoy under the default location, which must NOT be the one read.
    final ContextConfiguration decoy = new ContextConfiguration();
    decoy.setValue(TX_WAL, true);
    FileUtils.writeFile(root.resolve("config").resolve(ArcadeDBServer.SERVER_CONFIGURATION_FILE_NAME).toFile(), decoy.toJSON());

    try {
      // The no-argument constructor is the one that reads server-configuration.json, from the JVM-wide settings.
      GlobalConfiguration.SERVER_ROOT_PATH.setValue(root.toString());
      GlobalConfiguration.SERVER_CONFIG_DIRECTORY.setValue(configDir.toString());

      final ArcadeDBServer unstarted = new ArcadeDBServer();
      assertThat(unstarted.getConfigPath()).isEqualTo(configDir.toString());
      assertThat(unstarted.getConfiguration().getValueAsBoolean(TX_WAL)).isFalse();
    } finally {
      GlobalConfiguration.SERVER_CONFIG_DIRECTORY.reset();
      setTestConfiguration();
    }
  }
}
