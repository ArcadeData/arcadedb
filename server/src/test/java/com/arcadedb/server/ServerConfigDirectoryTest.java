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
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.backup.AutoBackupConfig;
import com.arcadedb.server.security.SecurityGroupFileRepository;
import com.arcadedb.server.security.ServerSecurity;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static com.arcadedb.GlobalConfiguration.TX_WAL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The server configuration directory: configurable through {@code arcadedb.server.configDirectory}, seeded with the shipped
 * (bundled) defaults when empty, and the groups it ships.
 */
class ServerConfigDirectoryTest {
  // Regression: #7415 the configuration directory is configurable
  /**
   * Issue #7415: the server configuration directory is configurable through {@code arcadedb.server.configDirectory},
   * so it can live on a persistent volume next to the databases instead of always under {@code <root>/config}.
   *
   * @author Roberto Franchini (r.franchini@arcadedata.com)
   */
  @Nested
  class ConfiguredDirectory extends StaticBaseServerTest {
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

  // Regression: #10536 an empty configuration directory is seeded with the shipped defaults
  /**
   * Issue #10536: a configuration directory moved off {@code <root>/config} (for example onto a Kubernetes
   * PersistentVolumeClaim, which is never populated from the image) starts empty, so the server ran without the shipped
   * {@code server-groups.json} (and its {@code readonly} group) and the Gremlin server files. The seeding used to be an
   * init container running {@code /bin/sh}, which the native images do not have. The server now seeds the directory
   * itself at startup, from the files its own distribution ships, so it works on every image.
   *
   * @author Roberto Franchini (r.franchini@arcadedata.com)
   */
  @Nested
  class Seed extends StaticBaseServerTest {
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

  // Regression: #10554 the seed defaults are bundled in the jar, so a volume over <root>/config is still seeded
  /**
   * Issue #10554: the defaults a configuration directory is seeded with used to be read from the image's
   * {@code <root>/config} only, so a volume mounted over that very directory (a Kubernetes PersistentVolumeClaim or an
   * {@code emptyDir} at {@code /home/arcadedb/config}) hid them, and the server ran on built-in groups that lacked the
   * {@code readonly} group. The defaults are now also bundled in the server jar, from the single source
   * {@code package/src/main/config}, and the built-in groups are read from the same copy.
   *
   * @author Roberto Franchini (r.franchini@arcadedata.com)
   */
  @Nested
  class BundledDefaults extends StaticBaseServerTest {
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

  // Regression: #11276 GET /server/groups lists every declared group field; the shipped crud group had no readTimeout
  /**
   * Issue #11276: {@code GET /server/groups} declares every group with {@code resultSetLimit}, {@code readTimeout}, {@code access}
   * and {@code types} ({@code GroupDefinition}), but the shipped {@code crud} group had no {@code readTimeout} and the listing
   * returned groups as stored, so a client enforcing the required members failed on every call.
   */
  @Nested
  class GroupListingContract extends StaticBaseServerTest {
    private static final List<String> REQUIRED = List.of("resultSetLimit", "readTimeout", "access", "types");

    private ArcadeDBServer server;

    @BeforeEach
    @Override
    public void beginTest() {
      super.beginTest();
      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, "./target");
      config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, DEFAULT_PASSWORD_FOR_TESTS);
      config.setValue(GlobalConfiguration.SERVER_HTTP_IO_THREADS, 2);
      config.setValue(GlobalConfiguration.TYPE_DEFAULT_BUCKETS, 2);
      server = new ArcadeDBServer(config);
      server.start();
    }

    @AfterEach
    @Override
    public void endTest() {
      try {
        if (server != null && server.isStarted())
          server.stop();
      } finally {
        super.endTest();
      }
    }

    @Test
    void shippedGroupsCarryEveryRequiredMember() throws IOException {
      Path root = new File("").getAbsoluteFile().toPath();
      while (root != null && !Files.isRegularFile(root.resolve("package/src/main/config/server-groups.json")))
        root = root.getParent();
      assertThat(root).isNotNull();

      final JSONObject document = new JSONObject(Files.readString(root.resolve("package/src/main/config/server-groups.json")));
      final JSONObject databases = document.getJSONObject("databases");
      for (final String database : databases.keySet()) {
        final JSONObject groups = databases.getJSONObject(database).getJSONObject("groups");
        for (final String name : groups.keySet())
          for (final String member : REQUIRED)
            assertThat(groups.getJSONObject(name).has(member)).as("group '%s' of database '%s' lacks '%s'", name, database, member)
                .isTrue();
      }
    }

    @Test
    void listingNormalizesAGroupStoredWithoutTheDefaultedMembers() {
      // What the 26.11.1 snapshot seeded: no readTimeout
      server.getSecurity().saveGroup("*", "issue11276", new JSONObject().put("access", new JSONArray()).put("types",
          new JSONObject().put("*", new JSONObject().put("access", new JSONArray().put("readRecord")))));

      final JSONObject listed = new ServerControlPlane(server).listGroups().getJSONObject("databases").getJSONObject("*")
          .getJSONObject("groups").getJSONObject("issue11276");

      for (final String member : REQUIRED)
        assertThat(listed.has(member)).as("listed group lacks '%s'", member).isTrue();
      assertThat(listed.getLong("readTimeout")).isEqualTo(-1L);
      assertThat(listed.getLong("resultSetLimit")).isEqualTo(-1L);

      // the normalization is a view: the stored document is untouched
      assertThat(server.getSecurity().groupsToJSON().getJSONObject("databases").getJSONObject("*").getJSONObject("groups")
          .getJSONObject("issue11276").has("readTimeout")).isFalse();
    }
  }
}
