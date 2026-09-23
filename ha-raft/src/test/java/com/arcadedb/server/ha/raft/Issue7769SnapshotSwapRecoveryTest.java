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

import com.arcadedb.database.DatabaseFactory;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Exercises the real swap and recovery paths at file-move and durable phase boundaries. The injected Error
 * skips normal rollback, modelling process interruption rather than an IOException; it does not model power loss.
 */
class Issue7769SnapshotSwapRecoveryTest {
  /**
   * A crash or I/O failure between the durable {@code .snapshot-complete} marker and the first published phase
   * leaves the originals untouched and no backup, whatever the temporary state file holds (a torn first write
   * included). The live directory is the intact original, and after a failure the node kept serving and applying
   * on it, so the staging may be stale: recovery discards it rather than installing it or refusing forever.
   */
  @ParameterizedTest
  @ValueSource(strings = { "BACKING_UP", "", "BACK", "INSTALLING", "ABSENT" })
  void unpublishedInitialPhaseKeepsTheLiveDatabase(final String temporaryPhase, @TempDir final Path root)
      throws Exception {
    final Path db = root.resolve("database");
    final Path staged = db.resolve(".snapshot-new");
    createDatabase(db, "old");
    createDatabase(staged, "new");
    Files.writeString(db.resolve(".snapshot-pending"), "");
    Files.writeString(staged.resolve(".snapshot-complete"), "");
    if (!temporaryPhase.equals("ABSENT"))
      Files.writeString(db.resolve(".snapshot-swap-state.tmp"), temporaryPhase);

    SnapshotInstaller.recoverPendingSnapshotSwaps(root);
    SnapshotInstaller.recoverPendingSnapshotSwaps(root);

    assertDatabaseValue(db, "old");
    assertThat(db.resolve(".snapshot-pending")).doesNotExist();
    assertThat(db.resolve(".snapshot-swap-state")).doesNotExist();
    assertThat(db.resolve(".snapshot-swap-state.tmp")).doesNotExist();
    assertThat(staged).doesNotExist();
    assertThat(db.resolve(".snapshot-backup")).doesNotExist();
  }

  /**
   * The first phase write fails (a full volume, the case a resync install exists to heal): the swap moves nothing,
   * the node keeps serving the old database, and a later recovery must not install the now-stale staging over it.
   */
  @Test
  void failedFirstPhaseWriteNeverInstallsTheStagingLater(@TempDir final Path root) throws Exception {
    final Path db = root.resolve("database");
    final Path staged = db.resolve(".snapshot-new");
    final Path backup = db.resolve(".snapshot-backup");
    final Path temporary = db.resolve(".snapshot-swap-state.tmp");
    createDatabase(db, "old");
    createDatabase(staged, "new");
    Files.writeString(db.resolve(".snapshot-pending"), "");
    Files.writeString(staged.resolve(".snapshot-complete"), "");
    // A non-empty directory in place of the temporary state file makes the first phase write fail.
    Files.createDirectories(temporary.resolve("blocker"));

    assertThatThrownBy(() -> swap(db, staged, backup)).isInstanceOf(IOException.class);
    assertDatabaseValue(db, "old");
    assertThat(backup).doesNotExist();

    // The volume recovers, leaving the empty temporary file a failed write would.
    Files.delete(temporary.resolve("blocker"));
    Files.delete(temporary);
    Files.writeString(temporary, "");
    SnapshotInstaller.recoverPendingSnapshotSwaps(root);

    assertDatabaseValue(db, "old");
    assertThat(db.resolve(".snapshot-pending")).doesNotExist();
    assertThat(temporary).doesNotExist();
    assertThat(staged).doesNotExist();
  }

  /** A backup proves originals may have moved, so an unpublished phase next to one cannot be trusted. */
  @Test
  void unpublishedPhaseBesideABackupPreservesEveryCopy(@TempDir final Path root) throws Exception {
    final Path db = root.resolve("database");
    final Path staged = db.resolve(".snapshot-new");
    final Path backup = db.resolve(".snapshot-backup");
    createDatabase(db, "old");
    createDatabase(staged, "new");
    Files.createDirectories(backup);
    Files.writeString(backup.resolve("A.0.bucket"), "old-A");
    Files.writeString(db.resolve(".snapshot-pending"), "");
    Files.writeString(staged.resolve(".snapshot-complete"), "");
    Files.writeString(db.resolve(".snapshot-swap-state.tmp"), "BACKING_UP");

    SnapshotInstaller.recoverPendingSnapshotSwaps(root);

    assertDatabaseValue(db, "old");
    assertDatabaseValue(staged, "new");
    assertThat(backup.resolve("A.0.bucket")).hasContent("old-A");
    assertThat(db.resolve(".snapshot-pending")).exists();
    assertThat(db.resolve(".snapshot-swap-state.tmp")).hasContent("BACKING_UP");
  }

  /**
   * A legacy swap (no recorded phase) whose staging holds only the completion marker finished phase 2: the old
   * code only started moving staged files after every original was in the backup. Only cleanup remains.
   */
  @Test
  void legacySwapWithEmptyStagingOnlyCleansUp(@TempDir final Path root) throws Exception {
    final Path db = root.resolve("database");
    final Path staged = db.resolve(".snapshot-new");
    final Path backup = db.resolve(".snapshot-backup");
    createDatabase(db, "new");
    Files.createDirectories(staged);
    Files.createDirectories(backup);
    Files.writeString(backup.resolve("A.0.bucket"), "old-A");
    Files.writeString(db.resolve(".snapshot-pending"), "");
    Files.writeString(staged.resolve(".snapshot-complete"), "");

    SnapshotInstaller.recoverPendingSnapshotSwaps(root);

    assertDatabaseValue(db, "new");
    assertThat(db.resolve(".snapshot-pending")).doesNotExist();
    assertThat(staged).doesNotExist();
    assertThat(backup).doesNotExist();
  }

  @Test
  void failedPendingMarkerRemovalKeepsTheBackup(@TempDir final Path root) throws Exception {
    final Path db = root.resolve("database");
    final Path backup = db.resolve(".snapshot-backup");
    createDatabase(db, "new");
    createDatabase(backup, "old");
    Files.createDirectories(db.resolve(".snapshot-pending"));
    Files.writeString(db.resolve(".snapshot-pending/keep"), "prevents marker removal");
    Files.writeString(db.resolve(".snapshot-swap-state"), "INSTALLED");

    SnapshotInstaller.recoverPendingSnapshotSwaps(root);

    assertDatabaseValue(db, "new");
    assertDatabaseValue(backup, "old");
    assertThat(db.resolve(".snapshot-pending")).exists();
    assertThat(db.resolve(".snapshot-swap-state")).hasContent("INSTALLED");
  }

  @Test
  void recoveryAfterAbruptJvmExitKeepsTheWholeSnapshot(@TempDir final Path root) throws Exception {
    final Path db = root.resolve("database");
    final Path staged = db.resolve(".snapshot-new");
    createDatabase(db, "old");
    createDatabase(staged, "new");
    Files.writeString(db.resolve(".snapshot-pending"), "");
    Files.writeString(staged.resolve(".snapshot-complete"), "");
    final Path output = root.resolve("child.log");
    final Process child = new ProcessBuilder(
        ProcessHandle.current().info().command().orElseThrow(), "--add-modules=jdk.incubator.vector", "-cp",
        System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")),
        CrashProcess.class.getName(), db.toString())
        .redirectErrorStream(true).redirectOutput(output.toFile()).start();
    try {
      assertThat(child.waitFor(60, TimeUnit.SECONDS)).as("child reaches the crash boundary").isTrue();
      assertThat(child.exitValue()).as("child output: %s", Files.readString(output)).isEqualTo(73);
    } finally {
      if (child.isAlive()) {
        child.destroyForcibly();
        child.waitFor();
      }
    }

    SnapshotInstaller.recoverPendingSnapshotSwaps(root);
    assertDatabaseValue(db, "new");
    assertThat(db.resolve(".snapshot-pending")).doesNotExist();
    assertThat(db.resolve(".snapshot-backup")).doesNotExist();
    assertThat(db.resolve(".snapshot-new")).doesNotExist();
    assertThat(db.resolve(".snapshot-swap-state")).doesNotExist();
  }

  /** Child JVM deliberately exits without running finally blocks after the first new file has moved. */
  public static class CrashProcess {
    public static void main(final String[] args) throws Throwable {
      final Path db = Path.of(args[0]);
      SnapshotInstaller.swapProgressForTesting = point -> {
        if (point.startsWith("INSTALLING:"))
          Runtime.getRuntime().halt(73);
      };
      swap(db, db.resolve(".snapshot-new"), db.resolve(".snapshot-backup"));
      throw new AssertionError("The swap did not reach the crash boundary");
    }
  }

  @Test
  void recoveryCanItselfBeInterruptedAndResumed(@TempDir final Path root) throws Exception {
    final Path db = root.resolve("database");
    final Path staged = db.resolve(".snapshot-new");
    final Path backup = db.resolve(".snapshot-backup");
    createDatabase(db, "old");
    createDatabase(staged, "new");
    Files.writeString(db.resolve(".snapshot-pending"), "");
    Files.writeString(staged.resolve(".snapshot-complete"), "");
    final AtomicBoolean interrupted = new AtomicBoolean();
    SnapshotInstaller.swapProgressForTesting = point -> {
      if (point.startsWith("INSTALLING:") && interrupted.compareAndSet(false, true))
        throw new SimulatedCrash();
    };
    try {
      assertThatThrownBy(() -> swap(db, staged, backup)).isInstanceOf(SimulatedCrash.class);
      interrupted.set(false);
      assertThatThrownBy(() -> SnapshotInstaller.recoverPendingSnapshotSwaps(root)).isInstanceOf(SimulatedCrash.class);
    } finally {
      SnapshotInstaller.swapProgressForTesting = null;
    }

    SnapshotInstaller.recoverPendingSnapshotSwaps(root);
    assertDatabaseValue(db, "new");
    assertThat(db.resolve(".snapshot-pending")).doesNotExist();
    assertThat(backup).doesNotExist();
  }

  @Test
  void rollbackClearsSnapshotOnlyFilesBeforeRestoring(@TempDir final Path root) throws Exception {
    final Path db = root.resolve("database");
    final Path backup = db.resolve(".snapshot-backup");
    createDatabase(backup, "old");
    Files.writeString(db.resolve(".snapshot-pending"), "");
    Files.writeString(db.resolve(".snapshot-swap-state"), "ROLLING_BACK");
    Files.writeString(db.resolve("new-only.bucket"), "partial snapshot");

    SnapshotInstaller.recoverPendingSnapshotSwaps(root);
    SnapshotInstaller.recoverPendingSnapshotSwaps(root);

    assertDatabaseValue(db, "old");
    assertThat(db.resolve("new-only.bucket")).doesNotExist();
    assertThat(db.resolve(".snapshot-pending")).doesNotExist();
    assertThat(backup).doesNotExist();
  }

  @Test
  void missingSchemaDoesNotDiscardTheBackup(@TempDir final Path root) throws Exception {
    final Path db = root.resolve("database");
    final Path backup = db.resolve(".snapshot-backup");
    createDatabase(backup, "old");
    Files.writeString(db.resolve(".snapshot-pending"), "");
    Files.writeString(db.resolve(".snapshot-swap-state"), "INSTALLED");
    Files.writeString(db.resolve("new-only.bucket"), "incomplete snapshot");

    SnapshotInstaller.recoverPendingSnapshotSwaps(root);

    assertDatabaseValue(backup, "old");
    assertThat(db.resolve("new-only.bucket")).hasContent("incomplete snapshot");
    assertThat(db.resolve(".snapshot-pending")).exists();
    assertThat(db.resolve(".snapshot-swap-state")).hasContent("INSTALLED");
  }

  @ParameterizedTest
  @ValueSource(strings = { "BACKING_UP_UNPUBLISHED", "BACKING_UP", "BACKING_UP:",
      "INSTALLING_UNPUBLISHED", "INSTALLING", "INSTALLING:", "INSTALLED_UNPUBLISHED", "INSTALLED" })
  void realSwapResumesAfterEachDurableBoundary(final String crashPoint, @TempDir final Path root) throws Exception {
    final Path db = root.resolve("database");
    final Path staged = db.resolve(".snapshot-new");
    final Path backup = db.resolve(".snapshot-backup");
    createDatabase(db, "old");
    createDatabase(staged, "new");
    Files.writeString(db.resolve(".snapshot-pending"), "");
    Files.writeString(staged.resolve(".snapshot-complete"), "");
    final AtomicBoolean interrupted = new AtomicBoolean();
    SnapshotInstaller.swapProgressForTesting = point -> {
      if ((crashPoint.endsWith(":") ? point.startsWith(crashPoint) : point.equals(crashPoint))
          && interrupted.compareAndSet(false, true))
        throw new SimulatedCrash();
    };
    try {
      assertThatThrownBy(() -> swap(db, staged, backup)).isInstanceOf(SimulatedCrash.class);
    } finally {
      SnapshotInstaller.swapProgressForTesting = null;
    }
    assertThat(interrupted).isTrue();
    assertThat(db.resolve(crashPoint.equals("BACKING_UP_UNPUBLISHED")
        ? ".snapshot-swap-state.tmp" : ".snapshot-swap-state")).exists();

    SnapshotInstaller.recoverPendingSnapshotSwaps(root);
    SnapshotInstaller.recoverPendingSnapshotSwaps(root);

    // Before the first phase is published nothing has moved: the staging is dropped, never installed.
    assertDatabaseValue(db, crashPoint.equals("BACKING_UP_UNPUBLISHED") ? "old" : "new");
    assertThat(db.resolve(".snapshot-pending")).doesNotExist();
    assertThat(db.resolve(".snapshot-swap-state")).doesNotExist();
    assertThat(staged).doesNotExist();
    assertThat(backup).doesNotExist();
  }

  @ParameterizedTest
  @ValueSource(strings = { "ROLLING_BACK", "RESTORING", "RESTORING:" })
  void failedSwapResumesAnInterruptedRollback(final String crashPoint, @TempDir final Path root) throws Exception {
    final Path db = root.resolve("database");
    final Path staged = db.resolve(".snapshot-new");
    final Path backup = db.resolve(".snapshot-backup");
    createDatabase(db, "old");
    Files.writeString(db.resolve(".snapshot-pending"), "");
    // A missing staging directory causes a real phase-2 IOException after the originals have been backed up.
    final AtomicBoolean interrupted = new AtomicBoolean();
    SnapshotInstaller.swapProgressForTesting = point -> {
      if ((crashPoint.endsWith(":") ? point.startsWith(crashPoint) : point.equals(crashPoint))
          && interrupted.compareAndSet(false, true))
        throw new SimulatedCrash();
    };
    try {
      assertThatThrownBy(() -> swap(db, staged, backup)).isInstanceOf(SimulatedCrash.class);
    } finally {
      SnapshotInstaller.swapProgressForTesting = null;
    }
    assertThat(interrupted).isTrue();

    SnapshotInstaller.recoverPendingSnapshotSwaps(root);
    SnapshotInstaller.recoverPendingSnapshotSwaps(root);

    assertDatabaseValue(db, "old");
    assertThat(db.resolve(".snapshot-pending")).doesNotExist();
    assertThat(backup).doesNotExist();
  }

  @Test
  void unknownPhasePreservesAllFiles(@TempDir final Path root) throws Exception {
    final Path db = root.resolve("database");
    createDatabase(db, "old");
    Files.writeString(db.resolve(".snapshot-pending"), "");
    Files.writeString(db.resolve(".snapshot-swap-state"), "UNKNOWN");
    SnapshotInstaller.recoverPendingSnapshotSwaps(root);
    assertDatabaseValue(db, "old");
    assertThat(db.resolve(".snapshot-pending")).exists();
    assertThat(db.resolve(".snapshot-swap-state")).hasContent("UNKNOWN");
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
      assertThat(db.countType("Item", false)).isEqualTo(1);
      assertThat(db.iterateType("Item", false).next().asDocument().getString("value")).isEqualTo(value);
    }
  }

  private static void swap(final Path db, final Path staged, final Path backup) throws IOException {
    SnapshotInstaller.atomicSwap(db, staged, backup);
  }

  // Bypasses IOException rollback, like process death; this does not simulate loss of the OS page cache.
  private static final class SimulatedCrash extends Error {
  }

  @Test
  void installedSnapshotDoesNotRestoreAPartiallyDeletedBackup(@TempDir final Path root) throws Exception {
    final Path db = root.resolve("database");
    final Path backup = db.resolve(".snapshot-backup");
    Files.createDirectories(backup);
    Files.writeString(db.resolve(".snapshot-pending"), "");
    Files.writeString(db.resolve(".snapshot-swap-state"), "INSTALLED");
    Files.writeString(db.resolve("schema.json"), "new-schema");
    Files.writeString(db.resolve("A.0.bucket"), "new-A");
    Files.writeString(backup.resolve("A.0.bucket"), "old-A");

    SnapshotInstaller.recoverPendingSnapshotSwaps(root);

    assertThat(db.resolve("A.0.bucket")).hasContent("new-A");
    assertThat(db.resolve("schema.json")).hasContent("new-schema");
    assertThat(db.resolve(".snapshot-pending")).doesNotExist();
    assertThat(db.resolve(".snapshot-swap-state")).doesNotExist();
    assertThat(backup).doesNotExist();
  }

  @Test
  void interruptedRollbackNeverClearsAlreadyRestoredOriginals(@TempDir final Path root) throws Exception {
    final Path db = root.resolve("database");
    final Path staged = db.resolve(".snapshot-new");
    final Path backup = db.resolve(".snapshot-backup");
    Files.createDirectories(staged);
    Files.createDirectories(backup);
    Files.writeString(db.resolve(".snapshot-pending"), "");
    Files.writeString(db.resolve(".snapshot-swap-state"), "RESTORING");
    Files.writeString(staged.resolve(".snapshot-complete"), "");
    Files.writeString(staged.resolve("B.0.bucket"), "new-B");
    Files.writeString(db.resolve("A.0.bucket"), "old-A");
    Files.writeString(backup.resolve("B.0.bucket"), "old-B");
    Files.writeString(backup.resolve("schema.json"), "old-schema");

    SnapshotInstaller.recoverPendingSnapshotSwaps(root);
    SnapshotInstaller.recoverPendingSnapshotSwaps(root);

    assertThat(db.resolve("A.0.bucket")).hasContent("old-A");
    assertThat(db.resolve("B.0.bucket")).hasContent("old-B");
    assertThat(db.resolve("schema.json")).hasContent("old-schema");
    assertThat(db.resolve(".snapshot-pending")).doesNotExist();
    assertThat(staged).doesNotExist();
    assertThat(backup).doesNotExist();
  }

  @Test
  void legacyAmbiguousSwapKeepsEveryCopyAndItsPendingMarker(@TempDir final Path root) throws Exception {
    final Path db = root.resolve("database");
    final Path staged = db.resolve(".snapshot-new");
    final Path backup = db.resolve(".snapshot-backup");
    Files.createDirectories(staged);
    Files.createDirectories(backup);
    Files.writeString(db.resolve(".snapshot-pending"), "");
    Files.writeString(staged.resolve(".snapshot-complete"), "");
    Files.writeString(backup.resolve("A.0.bucket"), "old-A");
    Files.writeString(db.resolve("A.0.bucket"), "new-A");
    Files.writeString(staged.resolve("B.0.bucket"), "new-B");

    SnapshotInstaller.recoverPendingSnapshotSwaps(root);
    SnapshotInstaller.recoverPendingSnapshotSwaps(root);

    assertThat(db.resolve("A.0.bucket")).hasContent("new-A");
    assertThat(backup.resolve("A.0.bucket")).hasContent("old-A");
    assertThat(staged.resolve("B.0.bucket")).hasContent("new-B");
    assertThat(db.resolve(".snapshot-pending")).exists();
  }

  @ParameterizedTest
  @ValueSource(booleans = { false, true })
  void recoveryDoesNotDeleteAlreadyInstalledFiles(final boolean allMoved, @TempDir final Path root) throws Exception {
    final Path db = root.resolve("database");
    final Path staged = db.resolve(".snapshot-new");
    final Path backup = db.resolve(".snapshot-backup");
    Files.createDirectories(staged);
    Files.createDirectories(backup);
    Files.writeString(db.resolve(".snapshot-pending"), "");
    Files.writeString(staged.resolve(".snapshot-complete"), "");
    Files.writeString(db.resolve(".snapshot-swap-state"), "INSTALLING");
    Files.writeString(backup.resolve("A.0.bucket"), "old-A");
    Files.writeString(backup.resolve("B.0.bucket"), "old-B");
    Files.writeString(backup.resolve("schema.json"), "old-schema");
    Files.writeString(db.resolve("A.0.bucket"), "new-A");
    Files.writeString((allMoved ? db : staged).resolve("B.0.bucket"), "new-B");
    Files.writeString((allMoved ? db : staged).resolve("schema.json"), "new-schema");

    SnapshotInstaller.recoverPendingSnapshotSwaps(root);
    SnapshotInstaller.recoverPendingSnapshotSwaps(root);

    assertThat(db.resolve("A.0.bucket")).hasContent("new-A");
    assertThat(db.resolve("B.0.bucket")).hasContent("new-B");
    assertThat(db.resolve("schema.json")).hasContent("new-schema");
    assertThat(db.resolve(".snapshot-pending")).doesNotExist();
    assertThat(staged).doesNotExist();
    assertThat(backup).doesNotExist();
  }
}
