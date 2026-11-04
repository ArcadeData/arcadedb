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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11200: the validating reopen of an installed snapshot only says "did it throw", which a snapshot of
 * zero-length page files, or one whose {@code schema.json} the engine replaced with {@code schema.prev.json}, satisfies.
 * The install then deleted the retained backup. The installed files are now compared with the account of what the
 * leader sent (name and size of every staged file, taken once the staging was verified against the manifest) before
 * the backup can be deleted, and a mismatch is handled like a failed validating reopen.
 */
class Issue11200InstallVerifiesInstalledFilesTest {
  private static final String DB_NAME = "Universe";

  @TempDir
  Path root;

  private ArcadeDBServer server;

  @AfterEach
  void tearDown() {
    if (server != null && server.isStarted())
      server.stop();
  }

  @Test
  void zeroLengthPageFilesRollBackToThePreviousCopy() throws Exception {
    final Path live = root.resolve("databases").resolve(DB_NAME);
    final Path staged = live.resolve(SnapshotInstaller.SNAPSHOT_NEW_DIR);
    createDatabase(live, "old");
    createDatabase(staged, "new");
    final Map<String, Long> expected = SnapshotInstaller.expectedInstalledFiles(staged);

    int truncated = 0;
    try (final Stream<Path> files = Files.list(staged)) {
      for (final Path f : (Iterable<Path>) files.filter(p -> p.getFileName().toString().contains(".bucket"))::iterator) {
        try (final FileChannel channel = FileChannel.open(f, StandardOpenOption.WRITE)) {
          channel.truncate(0);
        }
        truncated++;
      }
    }
    assertThat(truncated).isPositive();

    assertRolledBack(live, staged, expected);
  }

  @Test
  void corruptSchemaFileRollsBackToThePreviousCopy() throws Exception {
    final Path live = root.resolve("databases").resolve(DB_NAME);
    final Path staged = live.resolve(SnapshotInstaller.SNAPSHOT_NEW_DIR);
    createDatabase(live, "old");
    createDatabase(staged, "new");
    final Map<String, Long> expected = SnapshotInstaller.expectedInstalledFiles(staged);

    Files.writeString(staged.resolve("schema.json"), "{ not json");

    assertRolledBack(live, staged, expected);
  }

  @Test
  void intactSnapshotStillInstalls() throws Exception {
    final Path live = root.resolve("databases").resolve(DB_NAME);
    final Path staged = live.resolve(SnapshotInstaller.SNAPSHOT_NEW_DIR);
    final Path backup = live.resolve(SnapshotInstaller.SNAPSHOT_BACKUP_DIR);
    final Path marker = live.resolve(SnapshotInstaller.SNAPSHOT_PENDING_FILE);
    createDatabase(live, "old");
    createDatabase(staged, "new");
    final Map<String, Long> expected = SnapshotInstaller.expectedInstalledFiles(staged);

    server = newServer();
    server.start();
    Files.writeString(marker, "");
    Files.writeString(staged.resolve(SnapshotInstaller.SNAPSHOT_COMPLETE_FILE), "");
    SnapshotInstaller.swapAndReopen(DB_NAME, live, staged, backup, marker, server, expected);

    assertValue(server, "new");
    assertThat(marker).doesNotExist();
    assertThat(backup).doesNotExist();
  }

  private void assertRolledBack(final Path live, final Path staged, final Map<String, Long> expected) throws Exception {
    final Path backup = live.resolve(SnapshotInstaller.SNAPSHOT_BACKUP_DIR);
    final Path marker = live.resolve(SnapshotInstaller.SNAPSHOT_PENDING_FILE);

    server = newServer();
    server.start();
    assertValue(server, "old");
    Files.writeString(marker, "");
    Files.writeString(staged.resolve(SnapshotInstaller.SNAPSHOT_COMPLETE_FILE), "");

    assertThatThrownBy(() -> SnapshotInstaller.swapAndReopen(DB_NAME, live, staged, backup, marker, server, expected))
        .isInstanceOf(IOException.class).hasMessageContaining("do not match what the leader sent");

    assertValue(server, "old");
    server.stop();
    SnapshotInstaller.recoverPendingSnapshotSwaps(root.resolve("databases"));
    assertDatabaseValue(live, "old");
  }

  private ArcadeDBServer newServer() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_PLUGINS, "");
    config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, root.toString());
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, root.resolve("databases").toString());
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, "TestPassword11200");
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

  private static void assertValue(final ArcadeDBServer server, final String expected) {
    final var db = server.getDatabaseForOperator(DB_NAME);
    assertThat(db.iterateType("Item", false).next().asDocument().getString("value")).isEqualTo(expected);
  }

  private static void assertDatabaseValue(final Path path, final String value) {
    try (final DatabaseFactory factory = new DatabaseFactory(path.toString()); final var db = factory.open()) {
      assertThat(db.countType("Item", false)).isEqualTo(1);
      assertThat(db.iterateType("Item", false).next().asDocument().getString("value")).isEqualTo(value);
    }
  }
}
