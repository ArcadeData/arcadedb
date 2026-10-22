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
import com.arcadedb.server.security.SecurityGroupFileRepository;
import com.arcadedb.server.security.ServerSecurity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10554: the defaults a configuration directory is seeded with used to be read from the image's
 * {@code <root>/config} only, so a volume mounted over that very directory (a Kubernetes PersistentVolumeClaim or an
 * {@code emptyDir} at {@code /home/arcadedb/config}) hid them, and the server ran on built-in groups that lacked the
 * {@code readonly} group. The defaults are now also bundled in the server jar, from the single source
 * {@code package/src/main/config}, and the built-in groups are read from the same copy.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue10554BundledConfigDefaultsTest extends StaticBaseServerTest {
  private static final String GROUPS     = SecurityGroupFileRepository.FILE_NAME;
  private static final String CONFIG_DIR = "package/src/main/config";

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
  void everySeededFileIsBundledByteForByteAsTheDistributionShipsIt() throws IOException {
    for (final String file : ConfigDirectorySeeder.SEEDED_FILES)
      try (final InputStream bundled = ConfigDirectorySeeder.openBundledDefault(file)) {
        assertThat(bundled).as("'%s' is not bundled in the server jar", file).isNotNull();
        assertThat(bundled.readAllBytes()).as("the bundled '%s' drifted from %s", file, CONFIG_DIR)
            .isEqualTo(Files.readAllBytes(shippedConfigDirectory().resolve(file)));
      }
    assertThat(ConfigDirectorySeeder.openBundledDefault("arcadedb-log.properties")).as("only the seeded files are bundled").isNull();
  }

  @Test
  void aVolumeMountedOverTheRootConfigIsSeededWithTheBundledDefaults(@TempDir final Path tempDir) throws Exception {
    // The mount hides what the image shipped: <root>/config is the configuration directory, and it is empty
    final Path config = Files.createDirectories(tempDir.resolve("root").resolve("config").resolve("lost+found")).getParent();

    assertThat(ConfigDirectorySeeder.seed(config, config)).containsExactlyElementsOf(ConfigDirectorySeeder.SEEDED_FILES);

    for (final String file : ConfigDirectorySeeder.SEEDED_FILES)
      assertThat(config.resolve(file)).as("'%s' was not seeded", file).hasSameBinaryContentAs(shippedConfigDirectory().resolve(file));
    // Seeded once: from now on the directory holds files, and what is in it is the operator's
    assertThat(ConfigDirectorySeeder.seed(config, config)).isEmpty();
  }

  @Test
  void aFileTheRootConfigLacksIsSeededFromTheBundledCopy(@TempDir final Path tempDir) throws Exception {
    final Path shipped = Files.createDirectories(tempDir.resolve("root").resolve("config"));
    final String customGroups = "{\"databases\":{},\"version\":2}";
    Files.writeString(shipped.resolve(GROUPS), customGroups);
    final Path configDir = tempDir.resolve("volume");

    assertThat(ConfigDirectorySeeder.seed(shipped, configDir)).containsExactlyElementsOf(ConfigDirectorySeeder.SEEDED_FILES);

    // What the image ships wins over the bundled copy, the rest comes from the jar
    assertThat(Files.readString(configDir.resolve(GROUPS))).isEqualTo(customGroups);
    for (final String file : ConfigDirectorySeeder.SEEDED_FILES)
      if (!file.equals(GROUPS))
        assertThat(configDir.resolve(file)).hasSameBinaryContentAs(shippedConfigDirectory().resolve(file));
  }

  @Test
  void theBuiltInGroupsAreTheShippedOnes(@TempDir final Path tempDir) throws IOException {
    final JSONObject shipped = new JSONObject(Files.readString(shippedConfigDirectory().resolve(GROUPS)));
    final SecurityGroupFileRepository repository = new SecurityGroupFileRepository(tempDir.toString(), 600_000);
    try {
      final JSONObject builtIn = repository.createDefault();

      assertThat(builtIn.toMap()).isEqualTo(shipped.toMap());
      // The shipped file is current, so the built-in default never goes through the version migration
      assertThat(builtIn.getInt("version")).isEqualTo(ServerSecurity.LATEST_VERSION);
      assertThat(builtIn.getJSONObject("databases").getJSONObject("*").getJSONObject("groups").has("readonly")).isTrue();
      // A fresh document each call: a caller editing it must not change the next default
      builtIn.getJSONObject("databases").remove("*");
      assertThat(repository.createDefault().getJSONObject("databases").has("*")).isTrue();
    } finally {
      repository.stop();
    }
  }

  @Test
  @Timeout(120)
  void aServerStartedOnAnEmptyRootConfigRunsOnTheShippedGroups(@TempDir final Path tempDir) throws Exception {
    final Path root = tempDir.resolve("root");
    final Path config = Files.createDirectories(root.resolve("config"));

    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.SERVER_ROOT_PATH, root.toString());
    configuration.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, root.resolve("databases").toString());
    configuration.setValue(GlobalConfiguration.SERVER_BACKUP_DIRECTORY, root.resolve("backups").toString());
    configuration.setValue(GlobalConfiguration.SERVER_CONFIG_DIRECTORY, config.toString());
    configuration.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, DEFAULT_PASSWORD_FOR_TESTS);
    configuration.setValue(GlobalConfiguration.SERVER_HTTP_IO_THREADS, 2);

    server = new ArcadeDBServer(configuration);
    server.start();
    assertThat(server.getStatus()).isEqualTo(ArcadeDBServer.STATUS.ONLINE);

    assertThat(config.resolve("gremlin-server.yaml")).hasSameBinaryContentAs(shippedConfigDirectory().resolve("gremlin-server.yaml"));
    assertThat(server.getSecurity().groupsToJSON().getJSONObject("databases").getJSONObject("*").getJSONObject("groups")
        .has("readonly")).isTrue();
  }

  /** Surefire runs in the module directory: walk up to the checkout that holds the distribution's config. */
  private static Path shippedConfigDirectory() {
    Path candidate = new File("").getAbsoluteFile().toPath();
    while (candidate != null) {
      if (Files.isRegularFile(candidate.resolve(CONFIG_DIR).resolve(GROUPS)))
        return candidate.resolve(CONFIG_DIR);
      candidate = candidate.getParent();
    }
    throw new IllegalStateException("Cannot find " + CONFIG_DIR + " above " + new File("").getAbsolutePath());
  }
}
