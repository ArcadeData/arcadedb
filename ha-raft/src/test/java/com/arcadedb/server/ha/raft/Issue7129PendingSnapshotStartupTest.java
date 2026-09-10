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
import com.arcadedb.server.ArcadeDBServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/** Exercises the real boot scans against interrupted snapshot installs (issue #7129). */
class Issue7129PendingSnapshotStartupTest {
  @TempDir
  Path root;

  @ParameterizedTest
  @ValueSource(booleans = { false, true })
  @Timeout(90)
  void haStartupRecoversBeforeSecondBootScan(final boolean completeDownload) throws Exception {
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

  private ArcadeDBServer newServer() {
    final ContextConfiguration config = new ContextConfiguration();
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
