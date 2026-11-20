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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.server.ArcadeDBServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Crash-recovery roll-forward of a snapshot swap and the verification of installed snapshot files.
 */
class SnapshotRollForwardTest {

  /**
   * Issue #11200: the validating reopen of an installed snapshot only says "did it throw", which a snapshot of
   * zero-length page files, or one whose {@code schema.json} the engine replaced with {@code schema.prev.json}, satisfies.
   * The install then deleted the retained backup. The installed files are now compared with the account of what the
   * leader sent (name and size of every staged file, taken once the staging was verified against the manifest) before
   * the backup can be deleted, and a mismatch is handled like a failed validating reopen.
   */
  @Nested
  class InstallVerifiesInstalledFiles {

    private static final String DB_NAME = "Universe";

    @TempDir
    Path root;

    private ArcadeDBServer server;

    @AfterEach
    void tearDown() {
      if (server != null && server.isStarted())
        server.stop();
    }

    // Regression for #11200: the install verifies the installed files, not only that the reopen did not throw
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

    // Regression for #11200: the install verifies the installed files, not only that the reopen did not throw
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

    // Regression for #11200: the install verifies the installed files, not only that the reopen did not throw
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

  /**
   * Issue #11223: the crash-recovery roll-forward deleted {@code .snapshot-backup} with nothing to compare the installed
   * files against, because the account of the leader's files was held in memory for the live install only. The account is
   * now persisted as {@code .snapshot-manifest} and verified on every roll-forward arm before the backup is deleted.
   * The open proof is forced to succeed here, so the manifest alone decides.
   */
  @Nested
  class RollForwardComparesTheManifest {

    @TempDir
    Path root;

    @AfterEach
    void resetTheOpenProofOverride() {
      SnapshotInstaller.snapshotOpensForTesting = null;
    }

    // Regression for #11223: roll-forward compares the installed files against the manifest before deleting the backup
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

    // Regression for #11223: roll-forward compares the installed files against the manifest before deleting the backup
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

    // Regression for #11223: roll-forward compares the installed files against the manifest before deleting the backup
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

    // Regression for #11223: roll-forward compares the installed files against the manifest before deleting the backup
    @Test
    void aMissingInstalledFileRestoresTheBackup() throws Exception {
      final Path live = installedLayout("INSTALLED", false);
      final Path victim = firstBucketFile(live);
      Files.delete(victim);

      SnapshotInstaller.recoverPendingSnapshotSwaps(root.resolve("databases"));

      assertDatabaseValue(live, "old");
      assertRecovered(live);
    }

    // Regression for #11223: roll-forward compares the installed files against the manifest before deleting the backup
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

  /**
   * Issue #8950: recovery rolled a swap forward and deleted {@code .snapshot-backup} on the strength of a schema file
   * being present, without proving the snapshot opens. Where no verdict was ever reached - a crash during the validating
   * reopen leaves the phase at INSTALLED, a resumed BACKING_UP or INSTALLING, the legacy layouts - an unopenable snapshot
   * therefore cost the only copy that opens.
   */
  @Nested
  class RollForwardProvesTheSnapshotOpens {

    @TempDir
    Path root;

    @AfterEach
    void resetTheOpenProofOverride() {
      SnapshotInstaller.snapshotOpensForTesting = null;
    }

    // Regression for #8950: roll-forward proves the snapshot opens before deleting the backup
    @Test
    void installedSnapshotThatDoesNotOpenRestoresTheBackup() throws Exception {
      final Path live = installedLayout("INSTALLED", true);

      SnapshotInstaller.recoverPendingSnapshotSwaps(root.resolve("databases"));

      assertDatabaseValue(live, "old");
      assertRecovered(live);
    }

    // Regression for #8950: roll-forward proves the snapshot opens before deleting the backup
    @Test
    void installedSnapshotThatOpensIsRolledForward() throws Exception {
      final Path live = installedLayout("INSTALLED", false);

      SnapshotInstaller.recoverPendingSnapshotSwaps(root.resolve("databases"));

      assertDatabaseValue(live, "new");
      assertRecovered(live);
    }

    // Regression for #8950: roll-forward proves the snapshot opens before deleting the backup
    @Test
    void resumedInstallingSwapWhoseSnapshotDoesNotOpenRestoresTheBackup() throws Exception {
      final Path live = installedLayout("INSTALLING", true);

      SnapshotInstaller.recoverPendingSnapshotSwaps(root.resolve("databases"));

      assertDatabaseValue(live, "old");
      assertRecovered(live);
    }

    // Regression for #8950: roll-forward proves the snapshot opens before deleting the backup
    @Test
    void aSnapshotWithNoBackupToFallBackToIsKept() throws Exception {
      final Path live = installedLayout("INSTALLED", true);
      deleteTree(live.resolve(SnapshotInstaller.SNAPSHOT_BACKUP_DIR));

      SnapshotInstaller.recoverPendingSnapshotSwaps(root.resolve("databases"));

      assertThat(live.resolve("schema.json")).exists();
      assertThat(live.resolve(SnapshotInstaller.SNAPSHOT_PENDING_FILE)).doesNotExist();
    }

    // Regression for #8950: roll-forward proves the snapshot opens before deleting the backup
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
}
