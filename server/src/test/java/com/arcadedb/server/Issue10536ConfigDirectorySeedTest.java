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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Issue #10536: a configuration directory moved off {@code <root>/config} (for example onto a Kubernetes
 * PersistentVolumeClaim, which is never populated from the image) starts empty, so the server ran without the shipped
 * {@code server-groups.json} (and its {@code readonly} group) and the Gremlin server files. The seeding used to be an
 * init container running {@code /bin/sh}, which the native images do not have. The server now seeds the directory
 * itself at startup, from the files its own distribution ships, so it works on every image.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue10536ConfigDirectorySeedTest extends StaticBaseServerTest {
  private static final String GROUPS = SecurityGroupFileRepository.FILE_NAME;

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
  @Timeout(120)
  void aServerStartedOnAnEmptyConfigDirectorySeedsItWithTheShippedDefaults(@TempDir final Path tempDir) throws Exception {
    final Path root = tempDir.resolve("root");
    final Path shipped = shipDefaults(root);
    final Path configDir = tempDir.resolve("volume").resolve("config");

    server = new ArcadeDBServer(serverConfiguration(root, configDir));
    server.start();
    assertThat(server.getStatus()).isEqualTo(ArcadeDBServer.STATUS.ONLINE);

    // Every default the server reads from its configuration directory is there, byte for byte the shipped one.
    for (final String file : ConfigDirectorySeeder.SEEDED_FILES)
      assertThat(configDir.resolve(file)).as("'%s' was not seeded", file).hasSameBinaryContentAs(shipped.resolve(file));

    // ...so the security policy in force is the shipped one, readonly group included, not the built-in fallback.
    final JSONObject groups = new JSONObject(Files.readString(configDir.resolve(GROUPS)));
    assertThat(groups.getJSONObject("databases").getJSONObject("*").getJSONObject("groups").has("readonly")).isTrue();
    assertThat(server.getSecurity().groupsToJSON().getJSONObject("databases").getJSONObject("*").getJSONObject("groups")
        .has("readonly")).isTrue();

    // The JVM loads logging from the image, never from the configuration directory: a copy on the volume would be a
    // file the operator edits to no effect.
    assertThat(configDir.resolve("arcadedb-log.properties")).doesNotExist();
    assertThat(configDir.resolve("arcadedb-log-all.properties")).doesNotExist();
  }

  @Test
  void aConfigDirectoryAlreadyInUseIsLeftAsItIs(@TempDir final Path tempDir) throws Exception {
    final Path root = tempDir.resolve("root");
    final Path shipped = shipDefaults(root);
    final Path configDir = Files.createDirectories(tempDir.resolve("config"));

    // An operator's edit, or the server's own runtime write, is what the volume exists to keep across restarts: neither
    // is overwritten, and no default is added next to them.
    final String operatorGroups = "{\"databases\":{},\"version\":2}";
    Files.writeString(configDir.resolve(GROUPS), operatorGroups);

    assertThat(ConfigDirectorySeeder.seed(shipped, configDir)).isEmpty();
    assertThat(Files.readString(configDir.resolve(GROUPS))).isEqualTo(operatorGroups);
    assertThat(configDir.resolve("gremlin-server.yaml")).doesNotExist();
  }

  @Test
  void aDefaultDeletedOnPurposeIsNotBroughtBackOnTheNextStart(@TempDir final Path tempDir) throws Exception {
    final Path shipped = shipDefaults(tempDir.resolve("root"));
    final Path configDir = tempDir.resolve("config");

    assertThat(ConfigDirectorySeeder.seed(shipped, configDir)).containsExactlyElementsOf(ConfigDirectorySeeder.SEEDED_FILES);
    Files.delete(configDir.resolve("gremlin-server.yaml"));

    assertThat(ConfigDirectorySeeder.seed(shipped, configDir)).isEmpty();
    assertThat(configDir.resolve("gremlin-server.yaml")).doesNotExist();
  }

  @Test
  void aSeedThatFailsHalfwayLeavesNoFileBehindSoTheNextStartSeedsAgain(@TempDir final Path tempDir) throws Exception {
    final Path shipped = shipDefaults(tempDir.resolve("root"));
    final Path configDir = tempDir.resolve("config");
    // server-groups.json is copied first: an unreadable file after it makes the seed fail halfway
    final Path unreadable = shipped.resolve("gremlin-server.yaml");
    assumeTrue(Files.getFileStore(shipped).supportsFileAttributeView(PosixFileAttributeView.class));
    final Set<PosixFilePermission> permissions = Files.getPosixFilePermissions(unreadable);
    Files.setPosixFilePermissions(unreadable, Set.of());
    try {
      assumeFalse(Files.isReadable(unreadable), "running as root, every file is readable");

      assertThatThrownBy(() -> ConfigDirectorySeeder.seed(shipped, configDir)).isInstanceOf(IOException.class);
      // A half-seeded directory would hold a file, and so would never be seeded again
      try (final Stream<Path> left = Files.list(configDir)) {
        assertThat(left.filter(Files::isRegularFile)).isEmpty();
      }
      // The server logs it and starts on its built-in defaults
      ConfigDirectorySeeder.seedFromRoot(tempDir.resolve("root").toString(), configDir.toString());
    } finally {
      Files.setPosixFilePermissions(unreadable, permissions);
    }

    assertThat(ConfigDirectorySeeder.seed(shipped, configDir)).containsExactlyElementsOf(ConfigDirectorySeeder.SEEDED_FILES);
    try (final Stream<Path> left = Files.list(configDir)) {
      assertThat(left.filter(Files::isDirectory)).as("the staging directory is removed").isEmpty();
    }
  }

  @Test
  void aFreshVolumeHoldingOnlyLostAndFoundIsSeeded(@TempDir final Path tempDir) throws Exception {
    final Path shipped = shipDefaults(tempDir.resolve("root"));
    // An ext4 volume claim mounted as the configuration directory itself starts with this directory in it
    final Path configDir = Files.createDirectories(tempDir.resolve("config").resolve("lost+found")).getParent();

    final List<String> seeded = ConfigDirectorySeeder.seed(shipped, configDir);

    assertThat(seeded).containsExactlyElementsOf(ConfigDirectorySeeder.SEEDED_FILES);
    assertThat(configDir.resolve(GROUPS)).hasSameBinaryContentAs(shipped.resolve(GROUPS));
  }

  @Test
  void onlyTheShippedDefaultsAreSeededNeverOtherFilesFoundUnderTheRootConfig(@TempDir final Path tempDir) throws Exception {
    final Path root = tempDir.resolve("root");
    final Path shipped = shipDefaults(root);
    // Left behind by a server that ran on <root>/config before the directory was moved: users, tokens and settings the
    // operator chose not to carry over must not reappear on the new volume.
    Files.writeString(shipped.resolve("server-users.jsonl"), "{\"name\":\"stale\"}\n");
    Files.writeString(shipped.resolve(ArcadeDBServer.SERVER_CONFIGURATION_FILE_NAME), "{\"configuration\":{}}");
    Files.writeString(shipped.resolve("server-api-tokens.json"), "{\"tokens\":[]}");
    final Path configDir = tempDir.resolve("config");

    ConfigDirectorySeeder.seed(shipped, configDir);

    assertThat(configDir.resolve("server-users.jsonl")).doesNotExist();
    assertThat(configDir.resolve(ArcadeDBServer.SERVER_CONFIGURATION_FILE_NAME)).doesNotExist();
    assertThat(configDir.resolve("server-api-tokens.json")).doesNotExist();
  }

  @Test
  void theDefaultConfigDirectoryIsLeftAsItIs(@TempDir final Path tempDir) throws Exception {
    final Path root = tempDir.resolve("root");
    final Path shipped = shipDefaults(root);
    Files.delete(shipped.resolve(GROUPS));

    // Not moved: the directory IS the shipped one, and a file missing from it was removed by the operator.
    assertThat(ConfigDirectorySeeder.seed(shipped, shipped)).isEmpty();
    assertThat(ConfigDirectorySeeder.seed(shipped, root.resolve("x").resolve("..").resolve("config"))).isEmpty();
    assertThat(shipped.resolve(GROUPS)).doesNotExist();
  }

  @Test
  void anEmbeddedServerWithNoShippedConfigSeedsNothing(@TempDir final Path tempDir) throws Exception {
    final Path configDir = tempDir.resolve("config");

    assertThat(ConfigDirectorySeeder.seed(tempDir.resolve("root").resolve("config"), configDir)).isEmpty();
    assertThat(configDir).doesNotExist();
  }

  /** Lays out {@code <root>/config} the way the distribution ships it, and returns that directory. */
  private static Path shipDefaults(final Path root) throws IOException {
    final Path shipped = Files.createDirectories(root.resolve("config"));
    Files.writeString(shipped.resolve(GROUPS), """
        {"databases":{"*":{"groups":{
          "admin":{"access":["updateSecurity","updateSchema","updateDatabaseSettings"],"resultSetLimit":-1,"readTimeout":-1,
                   "types":{"*":{"access":["createRecord","readRecord","updateRecord","deleteRecord"]}}},
          "readonly":{"access":[],"resultSetLimit":-1,"readTimeout":-1,"types":{"*":{"access":["readRecord"]}}},
          "*":{"access":[],"resultSetLimit":-1,"readTimeout":-1,"types":{"*":{"access":[]}}}}}},
         "version":2}
        """);
    Files.writeString(shipped.resolve("gremlin-server.yaml"), "host: 0.0.0.0\n");
    Files.writeString(shipped.resolve("gremlin-server.groovy"), "// lifecycle hook\n");
    Files.writeString(shipped.resolve("gremlin-server.properties"), "gremlin.graph=com.arcadedb.gremlin.ArcadeGraph\n");
    Files.writeString(shipped.resolve("arcadedb-log.properties"), "handlers=java.util.logging.ConsoleHandler\n");
    Files.writeString(shipped.resolve("arcadedb-log-all.properties"), "handlers=java.util.logging.ConsoleHandler\n");
    return shipped;
  }

  private static ContextConfiguration serverConfiguration(final Path root, final Path configDir) {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, root.toString());
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, root.resolve("databases").toString());
    config.setValue(GlobalConfiguration.SERVER_BACKUP_DIRECTORY, root.resolve("backups").toString());
    config.setValue(GlobalConfiguration.SERVER_CONFIG_DIRECTORY, configDir.toString());
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, DEFAULT_PASSWORD_FOR_TESTS);
    config.setValue(GlobalConfiguration.SERVER_HTTP_IO_THREADS, 2);
    return config;
  }
}
