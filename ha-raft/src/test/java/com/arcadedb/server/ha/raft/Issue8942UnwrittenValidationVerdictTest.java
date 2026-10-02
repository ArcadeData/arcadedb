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
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8942: when the validating reopen of an installed snapshot fails and the VALIDATION_FAILED phase cannot be
 * written (a full volume: the condition the install path exists for), the verdict used to be lost. The phase stayed
 * INSTALLED, the next recovery pass rolled the unopenable snapshot forward on the strength of its schema file, and
 * deleted {@code .snapshot-backup} - the only copy that opens.
 * <p>
 * The full volume is modelled the way the rest of the swap tests model it: a non-empty directory in place of
 * {@code .snapshot-swap-state.tmp}, which makes every fresh phase write fail while renames and deletes still work.
 */
class Issue8942UnwrittenValidationVerdictTest {
  private static final String DB_NAME = "Universe";

  @TempDir
  Path root;

  private ArcadeDBServer server;

  @AfterEach
  void tearDown() {
    SnapshotInstaller.swapProgressForTesting = null;
    if (server != null && server.isStarted())
      server.stop();
  }

  /** The issue's repro, driven through the real install entry point rather than through its pieces. */
  @Test
  void failedValidationThatCannotWriteItsPhaseKeepsTheBackup() throws Exception {
    final Path databases = root.resolve("databases");
    final Path live = databases.resolve(DB_NAME);
    final Path staged = live.resolve(SnapshotInstaller.SNAPSHOT_NEW_DIR);
    final Path backup = live.resolve(SnapshotInstaller.SNAPSHOT_BACKUP_DIR);
    final Path marker = live.resolve(SnapshotInstaller.SNAPSHOT_PENDING_FILE);
    final Path phaseTemporary = live.resolve(SnapshotInstaller.SNAPSHOT_SWAP_STATE_TMP_FILE);
    createDatabase(live, "old");
    createUnopenableDatabase(staged);

    server = newServer();
    server.start();
    assertValue(server, "old");

    Files.writeString(marker, "");
    Files.writeString(staged.resolve(SnapshotInstaller.SNAPSHOT_COMPLETE_FILE), "");

    // The volume fills once the snapshot is installed: every later phase write fails.
    SnapshotInstaller.swapProgressForTesting = point -> {
      if (point.equals("INSTALLED"))
        blockPhaseWrites(phaseTemporary);
    };
    assertThatThrownBy(() -> SnapshotInstaller.swapAndReopen(DB_NAME, live, staged, backup, marker, server))
        .isInstanceOf(IOException.class).hasMessageContaining("failed to open");
    SnapshotInstaller.swapProgressForTesting = null;
    server.stop();

    assertThat(backup).as("the rollback could not start, so the previous copy is still in the backup").isDirectory();
    assertThat(live.resolve(SnapshotInstaller.SNAPSHOT_SWAP_STATE_FILE))
        .as("the verdict survives the full volume").hasContent("VALIDATION_FAILED");

    // Restarts while the volume is still full: recovery cannot roll back either, and must not roll forward.
    SnapshotInstaller.recoverPendingSnapshotSwaps(databases);
    SnapshotInstaller.recoverPendingSnapshotSwaps(databases);

    assertDatabaseValue(backup, "old");
    assertThat(marker).exists();

    // Space is freed: the next pass finishes the rollback the verdict asked for.
    unblockPhaseWrites(phaseTemporary);
    SnapshotInstaller.recoverPendingSnapshotSwaps(databases);

    assertDatabaseValue(live, "old");
    assertThat(marker).doesNotExist();
    assertThat(backup).doesNotExist();
    assertThat(live.resolve(SnapshotInstaller.SNAPSHOT_SWAP_STATE_FILE)).doesNotExist();
    assertThat(live.resolve(SnapshotInstaller.SNAPSHOT_VALIDATION_FAILED_FILE)).doesNotExist();
  }

  /** A verdict that cannot be prepared refuses the install before a single file moves, and leaves the database open. */
  @Test
  void unpreparableVerdictRefusesTheSwapBeforeAnythingMoves() throws Exception {
    final Path databases = root.resolve("databases");
    final Path live = databases.resolve(DB_NAME);
    final Path staged = live.resolve(SnapshotInstaller.SNAPSHOT_NEW_DIR);
    final Path backup = live.resolve(SnapshotInstaller.SNAPSHOT_BACKUP_DIR);
    final Path marker = live.resolve(SnapshotInstaller.SNAPSHOT_PENDING_FILE);
    final Path verdict = live.resolve(SnapshotInstaller.SNAPSHOT_VALIDATION_FAILED_FILE);
    createDatabase(live, "old");
    createDatabase(staged, "new");

    server = newServer();
    server.start();

    Files.writeString(marker, "");
    Files.writeString(staged.resolve(SnapshotInstaller.SNAPSHOT_COMPLETE_FILE), "");
    blockPhaseWrites(verdict);

    assertThatThrownBy(() -> SnapshotInstaller.swapAndReopen(DB_NAME, live, staged, backup, marker, server))
        .isInstanceOf(IOException.class);

    assertThat(backup).as("nothing was moved").doesNotExist();
    assertThat(live.resolve(SnapshotInstaller.SNAPSHOT_SWAP_STATE_FILE)).doesNotExist();
    assertThat(server.existsDatabase(DB_NAME)).isTrue();
    assertValue(server, "old");
  }

  /**
   * A crash during the validating reopen leaves a prepared but unpublished verdict next to INSTALLED. No verdict was
   * reached, so recovery rolls forward as before and removes the prepared file with the rest of the swap state.
   */
  @Test
  void preparedButUnpublishedVerdictIsIgnoredAndCleanedUp() throws Exception {
    final Path databases = root.resolve("databases");
    final Path live = databases.resolve(DB_NAME);
    final Path staged = live.resolve(SnapshotInstaller.SNAPSHOT_NEW_DIR);
    final Path backup = live.resolve(SnapshotInstaller.SNAPSHOT_BACKUP_DIR);
    final Path marker = live.resolve(SnapshotInstaller.SNAPSHOT_PENDING_FILE);
    createDatabase(live, "old");
    createDatabase(staged, "new");
    Files.writeString(marker, "");
    Files.writeString(staged.resolve(SnapshotInstaller.SNAPSHOT_COMPLETE_FILE), "");

    SnapshotInstaller.prepareValidationFailedVerdict(live);
    SnapshotInstaller.atomicSwap(live, staged, backup);

    SnapshotInstaller.recoverPendingSnapshotSwaps(databases);

    assertDatabaseValue(live, "new");
    assertThat(marker).doesNotExist();
    assertThat(backup).doesNotExist();
    assertThat(live.resolve(SnapshotInstaller.SNAPSHOT_VALIDATION_FAILED_FILE)).doesNotExist();
  }

  private static void blockPhaseWrites(final Path file) {
    try {
      Files.createDirectories(file.resolve("blocker"));
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static void unblockPhaseWrites(final Path file) throws IOException {
    Files.delete(file.resolve("blocker"));
    Files.delete(file);
  }

  private ArcadeDBServer newServer() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_PLUGINS, "");
    config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, root.toString());
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, root.resolve("databases").toString());
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, "TestPassword8942");
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

  /**
   * A real database whose schema file is intact - enough for recovery's "looks like a database" check - but which
   * the engine refuses to open: a second component file carries the file id of an existing one.
   */
  private static void createUnopenableDatabase(final Path path) throws IOException {
    createDatabase(path, "new");
    final Path component;
    try (final Stream<Path> files = Files.list(path)) {
      component = files.filter(f -> f.getFileName().toString().startsWith("Item_0.") && f.getFileName().toString()
          .endsWith(".bucket")).findFirst().orElseThrow();
    }
    Files.copy(component, path.resolve("Duplicate" + component.getFileName().toString().substring("Item_0".length())));
    try (final DatabaseFactory factory = new DatabaseFactory(path.toString())) {
      assertThat(factory.exists()).isTrue();
      assertThatThrownBy(factory::open).as("the snapshot must not open").isInstanceOf(RuntimeException.class);
    }
  }

  private static void assertValue(final ArcadeDBServer server, final String expected) {
    final var db = server.getDatabase(DB_NAME);
    assertThat(db.iterateType("Item", false).next().asDocument().getString("value")).isEqualTo(expected);
  }

  private static void assertDatabaseValue(final Path path, final String value) {
    try (final DatabaseFactory factory = new DatabaseFactory(path.toString()); final var db = factory.open()) {
      assertThat(db.countType("Item", false)).isEqualTo(1);
      assertThat(db.iterateType("Item", false).next().asDocument().getString("value")).isEqualTo(value);
    }
  }
}
