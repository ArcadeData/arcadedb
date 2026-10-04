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
package com.arcadedb.server.ha.raft;

import com.arcadedb.database.DatabaseFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8950: recovery rolled a swap forward and deleted {@code .snapshot-backup} on the strength of a schema file
 * being present, without proving the snapshot opens. Where no verdict was ever reached - a crash during the validating
 * reopen leaves the phase at INSTALLED, a resumed BACKING_UP or INSTALLING, the legacy layouts - an unopenable snapshot
 * therefore cost the only copy that opens.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8950RollForwardProvesTheSnapshotOpensTest {
  @TempDir
  Path root;

  @Test
  void installedSnapshotThatDoesNotOpenRestoresTheBackup() throws Exception {
    final Path live = installedLayout("INSTALLED", true);

    SnapshotInstaller.recoverPendingSnapshotSwaps(root.resolve("databases"));

    assertDatabaseValue(live, "old");
    assertRecovered(live);
  }

  @Test
  void installedSnapshotThatOpensIsRolledForward() throws Exception {
    final Path live = installedLayout("INSTALLED", false);

    SnapshotInstaller.recoverPendingSnapshotSwaps(root.resolve("databases"));

    assertDatabaseValue(live, "new");
    assertRecovered(live);
  }

  @Test
  void resumedInstallingSwapWhoseSnapshotDoesNotOpenRestoresTheBackup() throws Exception {
    final Path live = installedLayout("INSTALLING", true);

    SnapshotInstaller.recoverPendingSnapshotSwaps(root.resolve("databases"));

    assertDatabaseValue(live, "old");
    assertRecovered(live);
  }

  @Test
  void aSnapshotWithNoBackupToFallBackToIsKept() throws Exception {
    final Path live = installedLayout("INSTALLED", true);
    deleteTree(live.resolve(SnapshotInstaller.SNAPSHOT_BACKUP_DIR));

    SnapshotInstaller.recoverPendingSnapshotSwaps(root.resolve("databases"));

    assertThat(live.resolve("schema.json")).exists();
    assertThat(live.resolve(SnapshotInstaller.SNAPSHOT_PENDING_FILE)).doesNotExist();
  }

  @Test
  void anInconclusiveProofKeepsTheMarkerAndBothCopies() throws Exception {
    final Path live = installedLayout("INSTALLED", false);
    SnapshotInstaller.snapshotOpensForTesting = path -> {
      throw new UncheckedIOException(new IOException("simulated I/O error"));
    };
    try {
      SnapshotInstaller.recoverPendingSnapshotSwaps(root.resolve("databases"));
    } finally {
      SnapshotInstaller.snapshotOpensForTesting = null;
    }

    assertThat(live.resolve(SnapshotInstaller.SNAPSHOT_PENDING_FILE)).exists();
    assertThat(live.resolve(SnapshotInstaller.SNAPSHOT_BACKUP_DIR)).exists();
    assertThat(live.resolve("schema.json")).exists();
  }

  /** A live directory holding the installed snapshot, with the previous copy retained in {@code .snapshot-backup}. */
  private Path installedLayout(final String phase, final boolean snapshotIsUnopenable) throws IOException {
    final Path live = root.resolve("databases").resolve("Universe");
    final Path previous = root.resolve("previous");
    createDatabase(previous, "old");
    createDatabase(live, "new");
    if (snapshotIsUnopenable)
      corrupt(live);

    final Path backup = live.resolve(SnapshotInstaller.SNAPSHOT_BACKUP_DIR);
    Files.move(previous, backup);
    Files.writeString(live.resolve(SnapshotInstaller.SNAPSHOT_PENDING_FILE), "");
    Files.writeString(live.resolve(SnapshotInstaller.SNAPSHOT_SWAP_STATE_FILE), phase);
    if (phase.equals("INSTALLING")) {
      // every snapshot file was already moved into place, so only the completed, empty staging directory is left
      final Path staged = live.resolve(SnapshotInstaller.SNAPSHOT_NEW_DIR);
      Files.createDirectories(staged);
      Files.writeString(staged.resolve(SnapshotInstaller.SNAPSHOT_COMPLETE_FILE), "");
    }
    return live;
  }

  private void assertRecovered(final Path live) {
    assertThat(live.resolve(SnapshotInstaller.SNAPSHOT_PENDING_FILE)).doesNotExist();
    assertThat(live.resolve(SnapshotInstaller.SNAPSHOT_SWAP_STATE_FILE)).doesNotExist();
    assertThat(live.resolve(SnapshotInstaller.SNAPSHOT_BACKUP_DIR)).doesNotExist();
    assertThat(live.resolve(SnapshotInstaller.SNAPSHOT_NEW_DIR)).doesNotExist();
  }

  private static void createDatabase(final Path path, final String value) {
    try (final DatabaseFactory factory = new DatabaseFactory(path.toString()); final var db = factory.create()) {
      db.transaction(() -> {
        db.getSchema().createDocumentType("Item", 1);
        db.newDocument("Item").set("value", value).save();
      });
    }
  }

  /** The schema file stays intact, so the directory still looks like a database, but a duplicated file id stops the open. */
  private static void corrupt(final Path path) throws IOException {
    final Path component;
    try (final Stream<Path> files = Files.list(path)) {
      component = files.filter(f -> f.getFileName().toString().startsWith("Item_0.") && f.getFileName().toString().endsWith(".bucket"))
          .findFirst().orElseThrow();
    }
    Files.copy(component, path.resolve("Duplicate" + component.getFileName().toString().substring("Item_0".length())));
    try (final DatabaseFactory factory = new DatabaseFactory(path.toString())) {
      assertThat(factory.exists()).isTrue();
      assertThatThrownBy(factory::open).as("the snapshot must not open").isInstanceOf(RuntimeException.class);
    }
  }

  private static void assertDatabaseValue(final Path path, final String value) {
    try (final DatabaseFactory factory = new DatabaseFactory(path.toString()); final var db = factory.open()) {
      assertThat(db.countType("Item", false)).isEqualTo(1);
      assertThat(db.iterateType("Item", false).next().asDocument().getString("value")).isEqualTo(value);
    }
  }

  private static void deleteTree(final Path path) throws IOException {
    try (final Stream<Path> files = Files.walk(path)) {
      files.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
    }
  }
}
