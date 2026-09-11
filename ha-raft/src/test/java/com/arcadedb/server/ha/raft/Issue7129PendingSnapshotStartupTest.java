/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.server.ha.raft;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.exception.DatabaseNotAvailableException;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerPlugin;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Exercises the real boot scans against interrupted snapshot installs (issue #7129). */
class Issue7129PendingSnapshotStartupTest {
  @TempDir
  Path root;

  @Test
  void failedRuntimeSwapReopensWhilePendingMarkerStillExists() throws Exception {
    final Path live = root.resolve("databases").resolve("Universe");
    createDatabase(live, "old");
    final ArcadeDBServer server = newServer();
    try {
      server.start();
      // A missing staging directory makes phase 2 fail after the live files have moved to backup.
      final Path staged = live.resolve(".snapshot-new");
      final Path marker = live.resolve(".snapshot-pending");
      Files.writeString(marker, "");

      assertThatThrownBy(() -> SnapshotInstaller.swapAndReopen("Universe", live, staged,
          live.resolve(".snapshot-backup"), marker, server))
          .isInstanceOf(IOException.class).hasMessageContaining("Snapshot swap failed");
      assertThat(marker).exists();
      assertThat(server.existsDatabase("Universe")).as("rollback reopens the original database").isTrue();
      assertValue(server, "Universe", "old");
    } finally {
      server.stop();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = { false, true })
  void defaultDatabaseMustDeferPendingDirectory(final boolean pending) throws Exception {
    final Path live = root.resolve("databases").resolve("Universe");
    createDatabase(live, "old");
    if (pending)
      Files.writeString(live.resolve(".snapshot-pending"), "");
    final ArcadeDBServer server = newServer();
    server.getConfiguration().setValue(GlobalConfiguration.SERVER_DEFAULT_DATABASES, "Universe[root]");
    try {
      server.start();
      assertThat(server.existsDatabase("Universe")).isEqualTo(!pending);
      if (!pending)
        assertValue(server, "Universe", "old");
    } finally {
      server.stop();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = { false, true })
  void directLoadDuringStartupMustDeferPendingDirectory(final boolean pending) throws Exception {
    final Path live = root.resolve("databases").resolve("Universe");
    createDatabase(live, "old");
    if (pending)
      Files.writeString(live.resolve(".snapshot-pending"), "");
    final ArcadeDBServer server = newServer(StartupLoadProbe.class.getName());
    // HA stays disabled so the marker survives until the AFTER_HTTP_ON probe observes the startup window.
    StartupLoadProbe.probe = running -> {
      assertThat(running.getStatus()).isEqualTo(ArcadeDBServer.STATUS.STARTING);
      if (pending)
        assertThatThrownBy(() -> assertValue(running, "Universe", "old"))
            .as("a concurrent direct load must not open a pending database during STARTING")
            .isInstanceOf(DatabaseNotAvailableException.class);
      else
        assertValue(running, "Universe", "old");
    };
    StartupLoadProbe.executed = false;
    try {
      server.start();
      assertThat(StartupLoadProbe.executed).isTrue();
      assertThat(server.existsDatabase("Universe")).isEqualTo(!pending);
    } finally {
      server.stop();
      StartupLoadProbe.probe = null;
    }
  }

  /** Runs an independent database lookup while HTTP is on and the server is still STARTING. */
  public static class StartupLoadProbe implements ServerPlugin {
    static Consumer<ArcadeDBServer> probe;
    static boolean executed;
    private ArcadeDBServer server;

    @Override
    public void configure(final ArcadeDBServer server, final ContextConfiguration configuration) {
      this.server = server;
    }

    @Override
    public PluginInstallationPriority getInstallationPriority() {
      return PluginInstallationPriority.AFTER_HTTP_ON;
    }

    @Override
    public void startService() {
      try (final var executor = Executors.newSingleThreadExecutor()) {
        executor.submit(() -> probe.accept(server)).get(60, TimeUnit.SECONDS);
        executed = true;
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  @ParameterizedTest
  @CsvSource({ "false,false", "true,false", "false,true", "true,true" })
  @Timeout(90)
  void haStartupRecoversBeforeSecondBootScan(final boolean completeDownload, final boolean defaultDatabase) throws Exception {
    final Path live = root.resolve("databases").resolve("Universe");
    createDatabase(live.resolve(".snapshot-backup"), "old");
    createDatabase(live.resolve(".snapshot-new"), "new");
    Files.writeString(live.resolve(".snapshot-pending"), "");
    if (completeDownload)
      Files.writeString(live.resolve(".snapshot-new/.snapshot-complete"), "");

    final int raftPort;
    final int httpPort;
    try (final ServerSocket raftSocket = new ServerSocket(0); final ServerSocket httpSocket = new ServerSocket(0)) {
      raftPort = raftSocket.getLocalPort();
      httpPort = httpSocket.getLocalPort();
    }
    final ArcadeDBServer server = newServer();
    server.getConfiguration().setValue(GlobalConfiguration.HA_ENABLED, true);
    if (defaultDatabase)
      server.getConfiguration().setValue(GlobalConfiguration.SERVER_DEFAULT_DATABASES, "Universe[root]");
    server.getConfiguration().setValue(GlobalConfiguration.HA_SERVER_LIST, "localhost:" + raftPort + ":" + httpPort);
    server.getConfiguration().setValue(GlobalConfiguration.HA_RAFT_PORT, raftPort);
    server.getConfiguration().setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_HOST, "localhost");
    server.getConfiguration().setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_PORT, String.valueOf(httpPort));
    server.getConfiguration().setValue(GlobalConfiguration.HA_HEALTH_CHECK_INTERVAL, 0L);
    try {
      server.start();
      assertThat(server.getHA()).isNotNull();
      assertThat(live.resolve(".snapshot-pending")).doesNotExist();
      assertThat(server.existsDatabase("Universe")).as("the second boot scan loads the recovered database").isTrue();
      assertValue(server, "Universe", completeDownload ? "new" : "old");
    } finally {
      server.stop();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = { false, true })
  void pendingDatabaseStaysClosedUntilRecoveryCompletes(final boolean completeDownload) throws Exception {
    final Path databases = root.resolve("databases");
    final Path live = databases.resolve("Universe");
    final Path backup = live.resolve(".snapshot-backup");
    final Path staged = live.resolve(".snapshot-new");
    createDatabase(backup, "old");
    createDatabase(staged, "new");
    if (completeDownload)
      Files.writeString(staged.resolve(".snapshot-complete"), "");
    Files.writeString(live.resolve(".snapshot-pending"), "");
    createDatabase(databases.resolve("Healthy"), "healthy");

    // Without HA loaded, BOTH boot scans must leave the torn directory alone. Otherwise startup
    // fails before the HA state machine could run its recovery pass.
    final ArcadeDBServer server = newServer();
    try {
      server.start();
      assertThat(server.existsDatabase("Universe")).isFalse();
      assertValue(server, "Healthy", "healthy");
      assertThat(live.resolve(".snapshot-pending")).exists();

      // Run the same recovery entry point used by ArcadeStateMachine.initialize(), with no open
      // handles on Universe. A second pass must be harmless, including after the database is opened.
      SnapshotInstaller.recoverPendingSnapshotSwaps(databases);
      assertThat(live.resolve(".snapshot-pending")).doesNotExist();
      assertValue(server, "Universe", completeDownload ? "new" : "old");
      SnapshotInstaller.recoverPendingSnapshotSwaps(databases);
      assertValue(server, "Universe", completeDownload ? "new" : "old");
    } finally {
      server.stop();
    }

    final ArcadeDBServer restarted = newServer();
    try {
      restarted.start();
      assertThat(restarted.existsDatabase("Universe")).isTrue();
      assertValue(restarted, "Universe", completeDownload ? "new" : "old");
    } finally {
      restarted.stop();
    }
  }

  @Test
  void evenLoadablePendingDatabaseIsNotRegisteredBeforeRecovery() throws Exception {
    final Path live = root.resolve("databases").resolve("Universe");
    createDatabase(live, "old");
    Files.writeString(live.resolve(".snapshot-pending"), "");
    final ArcadeDBServer server = newServer();
    try {
      server.start();
      assertThat(server.existsDatabase("Universe"))
          .as("a loadable pending directory must not be opened before recovery swaps its files").isFalse();
    } finally {
      server.stop();
    }
  }

  @Test
  void unrecoverableDirectoryRemainsIsolatedAcrossRestarts() throws Exception {
    final Path live = root.resolve("databases").resolve("Universe");
    Files.createDirectories(live);
    Files.writeString(live.resolve(".snapshot-pending"), "");
    Files.writeString(live.resolve("partial.bucket"), "incomplete");
    for (int attempt = 0; attempt < 2; attempt++) {
      SnapshotInstaller.recoverPendingSnapshotSwaps(root.resolve("databases"));
      final ArcadeDBServer server = newServer();
      try {
        server.start();
        assertThat(server.existsDatabase("Universe")).isFalse();
        assertThat(live.resolve(".snapshot-pending")).exists();
        assertThat(Files.readString(live.resolve("partial.bucket"))).isEqualTo("incomplete");
      } finally {
        server.stop();
      }
    }
  }

  @Test
  void pendingDirectoryIsStillRefusedOnceTheServerIsOnline() throws Exception {
    final Path live = root.resolve("databases").resolve("Universe");
    createDatabase(live, "old");
    Files.writeString(live.resolve(".snapshot-pending"), "");
    final ArcadeDBServer server = newServer();
    try {
      server.start();
      assertThat(server.getStatus()).isEqualTo(ArcadeDBServer.STATUS.ONLINE);
      assertThat(server.existsDatabase("Universe")).isFalse();

      // The marker says the directory is mid-install, and nothing about the server reaching ONLINE reconciled it.
      // A guard scoped to STATUS.STARTING would have expired here and served the torn directory to the first
      // request that named it, which is the very failure mode issue #7129 describes.
      assertThatThrownBy(() -> server.getDatabase("Universe"))
          .as("an unrecovered pending directory must stay closed for the whole server lifetime, not just startup")
          .isInstanceOf(DatabaseNotAvailableException.class)
          .hasMessageContaining(".snapshot-pending");
      assertThat(server.existsDatabase("Universe")).isFalse();
      assertThat(live.resolve(".snapshot-pending")).exists();
    } finally {
      server.stop();
    }
  }

  @Test
  void creatingOverAPendingDirectoryIsRefused() throws Exception {
    final Path live = root.resolve("databases").resolve("Universe");
    Files.createDirectories(live);
    Files.writeString(live.resolve(".snapshot-pending"), "");
    // A torn directory whose schema the swap already took away: DatabaseFactory.exists() answers false for it,
    // so "already exists" is not what stops a create from landing on top of the interrupted install.
    Files.writeString(live.resolve("partial.bucket"), "incomplete");
    final ArcadeDBServer server = newServer();
    try {
      server.start();
      assertThatThrownBy(() -> server.createDatabase("Universe", ComponentFile.MODE.READ_WRITE))
          .isInstanceOf(DatabaseNotAvailableException.class).hasMessageContaining(".snapshot-pending");
      assertThat(server.existsDatabase("Universe")).isFalse();
      assertThat(Files.readString(live.resolve("partial.bucket"))).isEqualTo("incomplete");
      assertThat(live.resolve(".snapshot-pending")).exists();
    } finally {
      server.stop();
    }
  }

  @Test
  void unrecoverableDefaultDatabaseIsNotRecreatedOverTheInterruptedInstall() throws Exception {
    final Path live = root.resolve("databases").resolve("Universe");
    Files.createDirectories(live);
    Files.writeString(live.resolve(".snapshot-pending"), "");
    Files.writeString(live.resolve("partial.bucket"), "incomplete");
    final ArcadeDBServer server = newServer();
    server.getConfiguration().setValue(GlobalConfiguration.SERVER_DEFAULT_DATABASES, "Universe[root]");
    try {
      server.start();
      assertThat(server.existsDatabase("Universe")).isFalse();
      // The default-database pass must not read the deferred directory as "absent" and create a fresh database
      // into it: that would overwrite the only evidence snapshot recovery has to reason from.
      assertThat(live.resolve("partial.bucket")).exists();
      assertThat(Files.readString(live.resolve("partial.bucket"))).isEqualTo("incomplete");
      assertThat(live.resolve("schema.json")).doesNotExist();
      assertThat(live.resolve(".snapshot-pending")).exists();
    } finally {
      server.stop();
    }
  }

  @Test
  @Timeout(120)
  void aSnapshotSwapWhileTheServerIsStartingReopensTheInstalledCopy() throws Exception {
    final Path live = root.resolve("databases").resolve("Universe");
    createDatabase(live, "old");
    final ArcadeDBServer server = newServer(StartupLoadProbe.class.getName());
    // A bootstrap-mismatch install is applied during Raft log replay, i.e. while the server is still STARTING.
    // Its swapAndReopen has to reopen the freshly installed copy with the pending marker still on disk - the
    // marker is cleared only after that open proves the snapshot loads. A refusal there is read as "the snapshot
    // will not open" and answered by rolling a perfectly good snapshot back to the previous copy.
    StartupLoadProbe.probe = running -> {
      assertThat(running.getStatus()).isEqualTo(ArcadeDBServer.STATUS.STARTING);
      final Path staged = live.resolve(".snapshot-new");
      final Path backup = live.resolve(".snapshot-backup");
      final Path pendingMarker = live.resolve(".snapshot-pending");
      try {
        createDatabase(staged, "new");
        Files.writeString(pendingMarker, "");
        SnapshotInstaller.swapAndReopen("Universe", live, staged, backup, pendingMarker, running);
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
      assertThat(pendingMarker).as("a successful swap clears its own marker").doesNotExist();
      assertValue(running, "Universe", "new");
    };
    StartupLoadProbe.executed = false;
    try {
      server.start();
      assertThat(StartupLoadProbe.executed).isTrue();
      assertThat(server.existsDatabase("Universe")).isTrue();
      assertValue(server, "Universe", "new");
      assertThat(live.resolve(".snapshot-pending")).doesNotExist();
    } finally {
      server.stop();
      StartupLoadProbe.probe = null;
    }
  }

  private ArcadeDBServer newServer() {
    return newServer("");
  }

  private ArcadeDBServer newServer(final String plugins) {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_PLUGINS, plugins);
    config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, root.toString());
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, root.resolve("databases").toString());
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, "TestPassword7129");
    config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_PORT, "0");
    config.setValue(GlobalConfiguration.SERVER_HTTP_IO_THREADS, 2);
    config.setValue(GlobalConfiguration.SERVER_METRICS, false);
    config.setValue(GlobalConfiguration.SERVER_HEALTH_CHECK_ENABLED, false);
    config.setValue(GlobalConfiguration.SERVER_DATABASE_LOADATSTARTUP, true);
    config.setValue(GlobalConfiguration.SERVER_DEFAULT_DATABASES, "");
    config.setValue(GlobalConfiguration.HA_ENABLED, false);
    config.setValue(GlobalConfiguration.HA_SERVER_LIST, "");
    return new ArcadeDBServer(config);
  }

  private static void createDatabase(final Path path, final String value) {
    try (final DatabaseFactory factory = new DatabaseFactory(path.toString()); final var db = factory.create()) {
      db.transaction(() -> {
        db.getSchema().createDocumentType("Item", 1);
        db.newDocument("Item").set("value", value).save();
      });
    }
  }

  private static void assertValue(final ArcadeDBServer server, final String name, final String expected) {
    try (final var result = server.getDatabase(name).query("sql", "select value from Item")) {
      assertThat(result.hasNext()).isTrue();
      assertThat((String) result.next().getProperty("value")).isEqualTo(expected);
      assertThat(result.hasNext()).isFalse();
    }
  }
}
