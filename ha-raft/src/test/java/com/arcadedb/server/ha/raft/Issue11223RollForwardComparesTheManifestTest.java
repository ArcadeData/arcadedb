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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11223: the crash-recovery roll-forward deleted {@code .snapshot-backup} with nothing to compare the installed
 * files against, because the account of the leader's files was held in memory for the live install only. The account is
 * now persisted as {@code .snapshot-manifest} and verified on every roll-forward arm before the backup is deleted.
 * The open proof is forced to succeed here, so the manifest alone decides.
 */
class Issue11223RollForwardComparesTheManifestTest {
  @TempDir
  Path root;

  @AfterEach
  void resetTheOpenProofOverride() {
    SnapshotInstaller.snapshotOpensForTesting = null;
  }

  @Test
  void manifestRoundTripsAndSkipsTheMachineryFiles() throws Exception {
    final Path staged = root.resolve("staged");
    createDatabase(staged, "new");
    Files.writeString(staged.resolve(SnapshotInstaller.SNAPSHOT_COMPLETE_FILE), "");
    final Map<String, Long> expected = SnapshotInstaller.expectedInstalledFiles(staged);
    SnapshotInstaller.writeInstalledFilesManifest(staged, expected);

    assertThat(SnapshotInstaller.readInstalledFilesManifest(staged)).isEqualTo(expected);
    assertThat(SnapshotInstaller.expectedInstalledFiles(staged)).isEqualTo(expected)
        .doesNotContainKey(SnapshotInstaller.SNAPSHOT_MANIFEST_FILE);
    assertThat(SnapshotInstaller.isSnapshotMachineryFileName(SnapshotInstaller.SNAPSHOT_MANIFEST_FILE)).isTrue();
  }

  @Test
  void installedFilesMatchingTheManifestAreRolledForward() throws Exception {
    for (final String phase : new String[] { "INSTALLED", "INSTALLING" }) {
      final Path live = installedLayout(phase, false);

      SnapshotInstaller.recoverPendingSnapshotSwaps(root.resolve("databases"));

      assertDatabaseValue(live, "new");
      assertRecovered(live);
      deleteTree(root);
      Files.createDirectories(root);
    }
  }

  @Test
  void aTruncatedInstalledFileRestoresTheBackup() throws Exception {
    for (final String phase : new String[] { "INSTALLED", "INSTALLING" }) {
      final Path live = installedLayout(phase, true);

      SnapshotInstaller.recoverPendingSnapshotSwaps(root.resolve("databases"));

      assertDatabaseValue(live, "old");
      assertRecovered(live);
      deleteTree(root);
      Files.createDirectories(root);
    }
  }

  @Test
  void aMissingInstalledFileRestoresTheBackup() throws Exception {
    final Path live = installedLayout("INSTALLED", false);
    final Path victim = firstBucketFile(live);
    Files.delete(victim);

    SnapshotInstaller.recoverPendingSnapshotSwaps(root.resolve("databases"));

    assertDatabaseValue(live, "old");
    assertRecovered(live);
  }

  @Test
  void anInstallWithoutAManifestStillRollsForwardOnTheOpenProof() throws Exception {
    final Path live = installedLayout("INSTALLED", true);
    Files.delete(live.resolve(SnapshotInstaller.SNAPSHOT_MANIFEST_FILE));

    SnapshotInstaller.recoverPendingSnapshotSwaps(root.resolve("databases"));

    assertRecovered(live);
    assertThat(live.resolve("schema.json")).exists();
  }

  /**
   * A live directory holding the installed snapshot and its manifest, the previous copy retained in the backup.
   * With {@code damage} a page file is truncated after the manifest was taken, the zero-length file #11200 is about.
   */
  private Path installedLayout(final String phase, final boolean damage) throws IOException {
    SnapshotInstaller.snapshotOpensForTesting = path -> true;
    final Path live = root.resolve("databases").resolve("Universe");
    final Path previous = root.resolve("previous");
    createDatabase(previous, "old");
    createDatabase(live, "new");
    SnapshotInstaller.writeInstalledFilesManifest(live, SnapshotInstaller.expectedInstalledFiles(live));
    if (damage)
      Files.write(firstBucketFile(live), new byte[0]);

    Files.move(previous, live.resolve(SnapshotInstaller.SNAPSHOT_BACKUP_DIR));
    Files.writeString(live.resolve(SnapshotInstaller.SNAPSHOT_PENDING_FILE), "");
    Files.writeString(live.resolve(SnapshotInstaller.SNAPSHOT_SWAP_STATE_FILE), phase);
    if (phase.equals("INSTALLING")) {
      final Path staged = live.resolve(SnapshotInstaller.SNAPSHOT_NEW_DIR);
      Files.createDirectories(staged);
      Files.writeString(staged.resolve(SnapshotInstaller.SNAPSHOT_COMPLETE_FILE), "");
    }
    return live;
  }

  private static Path firstBucketFile(final Path live) throws IOException {
    try (final Stream<Path> files = Files.list(live)) {
      return files.filter(f -> f.getFileName().toString().startsWith("Item_0.") && f.getFileName().toString().endsWith(".bucket"))
          .findFirst().orElseThrow();
    }
  }

  private void assertRecovered(final Path live) {
    assertThat(live.resolve(SnapshotInstaller.SNAPSHOT_PENDING_FILE)).doesNotExist();
    assertThat(live.resolve(SnapshotInstaller.SNAPSHOT_SWAP_STATE_FILE)).doesNotExist();
    assertThat(live.resolve(SnapshotInstaller.SNAPSHOT_BACKUP_DIR)).doesNotExist();
    assertThat(live.resolve(SnapshotInstaller.SNAPSHOT_NEW_DIR)).doesNotExist();
    assertThat(live.resolve(SnapshotInstaller.SNAPSHOT_MANIFEST_FILE)).doesNotExist();
  }

  private static void createDatabase(final Path path, final String value) {
    try (final DatabaseFactory factory = new DatabaseFactory(path.toString()); final var db = factory.create()) {
      db.transaction(() -> {
        db.getSchema().createDocumentType("Item", 1);
        db.newDocument("Item").set("value", value).save();
      });
    }
  }

  private static void assertDatabaseValue(final Path path, final String value) {
    try (final DatabaseFactory factory = new DatabaseFactory(path.toString()); final var db = factory.open()) {
      assertThat(db.iterateType("Item", false).next().asDocument().getString("value")).isEqualTo(value);
    }
  }

  private static void deleteTree(final Path path) throws IOException {
    try (final Stream<Path> files = Files.walk(path)) {
      files.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
    }
  }
}
