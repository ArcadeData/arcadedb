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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Recovery of an interrupted snapshot swap (current and legacy layouts), including prefixed bucket files.
 */
class SnapshotSwapCrashRecoveryTest {

  // ------------------------------------------------------------------------------------------
  // Issue #7769
  // ------------------------------------------------------------------------------------------
  /*
   * Exercises the real swap and recovery paths at file-move and durable phase boundaries. The injected Error
   * skips normal rollback, modelling process interruption rather than an IOException; it does not model power loss.
   */

  /**
   * The fixtures stand in for a snapshot with placeholder files no engine can open, so recovery is told every snapshot
   * opens: what it does with one that does not is the subject of {@link SnapshotRollForwardTest.InstallVerifiesInstalledFiles.RollForwardProvesTheSnapshotOpens}.
   */
  @BeforeEach
  void snapshotsOpenForIssue8950() {
    SnapshotInstaller.snapshotOpensForTesting = path -> true;
  }

  @AfterEach
  void restoreSnapshotOpenProofForIssue8950() {
    SnapshotInstaller.snapshotOpensForTesting = null;
  }

  // Regression for #7769: unpublished initial phase keeps the live database
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

  // Regression for #7769: failed first phase write never installs the staging later
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

  // Regression for #7769: unpublished phase beside abackup preserves every copy
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

  // Regression for #7769: legacy swap with empty staging only cleans up
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

  // Regression for #7769: failed pending marker removal keeps the backup
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

  // Regression for #7769: recovery after abrupt jvm exit keeps the whole snapshot
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

  // Regression for #7769: recovery can itself be interrupted and resumed
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

  // Regression for #7769: rollback clears snapshot only files before restoring
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

  // Regression for #7769: missing schema does not discard the backup
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

  // Regression for #7769: real swap resumes after each durable boundary
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

  // Regression for #7769: failed swap resumes an interrupted rollback
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

  // Regression for #7769: failed validation rollback resumes after each boundary
  /** #8305: a crash after the validating reopen failed but before ROLLING_BACK was published must restore the backup. */
  @ParameterizedTest
  @ValueSource(strings = { "VALIDATION_FAILED", "ROLLING_BACK_UNPUBLISHED", "ROLLING_BACK", "RESTORING:" })
  void failedValidationRollbackResumesAfterEachBoundary(final String crashPoint, @TempDir final Path root) throws Exception {
    final Path db = root.resolve("database");
    final Path staged = db.resolve(".snapshot-new");
    final Path backup = db.resolve(".snapshot-backup");
    createDatabase(db, "old");
    createDatabase(staged, "new");
    Files.writeString(db.resolve(".snapshot-pending"), "");
    Files.writeString(staged.resolve(".snapshot-complete"), "");
    swap(db, staged, backup);
    assertThat(db.resolve(".snapshot-swap-state")).hasContent("INSTALLED");

    final AtomicBoolean interrupted = new AtomicBoolean();
    SnapshotInstaller.swapProgressForTesting = point -> {
      if ((crashPoint.endsWith(":") ? point.startsWith(crashPoint) : point.equals(crashPoint))
          && interrupted.compareAndSet(false, true))
        throw new SimulatedCrash();
    };
    try {
      assertThatThrownBy(() -> SnapshotInstaller.rollbackAfterFailedValidation(db, backup)).isInstanceOf(SimulatedCrash.class);
    } finally {
      SnapshotInstaller.swapProgressForTesting = null;
    }
    assertThat(interrupted).isTrue();

    SnapshotInstaller.recoverPendingSnapshotSwaps(root);
    SnapshotInstaller.recoverPendingSnapshotSwaps(root);

    assertDatabaseValue(db, "old");
    assertThat(db.resolve(".snapshot-pending")).doesNotExist();
    assertThat(db.resolve(".snapshot-swap-state")).doesNotExist();
    assertThat(backup).doesNotExist();
  }

  // Regression for #7769: failed validation without abackup rolls forward
  @Test
  void failedValidationWithoutABackupRollsForward(@TempDir final Path root) throws Exception {
    final Path db = root.resolve("database");
    createDatabase(db, "new");
    Files.writeString(db.resolve(".snapshot-pending"), "");
    Files.writeString(db.resolve(".snapshot-swap-state"), "VALIDATION_FAILED");

    SnapshotInstaller.recoverPendingSnapshotSwaps(root);

    assertDatabaseValue(db, "new");
    assertThat(db.resolve(".snapshot-pending")).doesNotExist();
  }

  // Regression for #7769: unknown phase preserves all files
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

  // Regression for #7769: installed snapshot does not restore apartially deleted backup
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

  // Regression for #7769: interrupted rollback never clears already restored originals
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

  // Regression for #7769: issue repro layouts lose no file
  /**
   * The two layouts the issue reproduces with {@code SwapRepro} and {@code SwapRepro2}, as a node killed mid-swap
   * by a binary that recorded no phase leaves them for this one. Before the fix the first lost {@code A.0.bucket}
   * from both copies and the second emptied the database directory, each with the marker cleared and success
   * logged. Now no file is lost in either, and since issue #8304 the first is rolled forward rather than preserved
   * for an operator: a name in both the live directory and the backup proves phase 2 was under way.
   */
  @ParameterizedTest
  @ValueSource(booleans = { false, true })
  void issueReproLayoutsLoseNoFile(final boolean phase2Finished, @TempDir final Path root) throws Exception {
    final Path db = root.resolve("databases").resolve("mydb");
    final Path staged = db.resolve(".snapshot-new");
    final Path backup = db.resolve(".snapshot-backup");
    Files.createDirectories(staged);
    Files.createDirectories(backup);
    Files.writeString(db.resolve(".snapshot-pending"), "");
    Files.writeString(staged.resolve(".snapshot-complete"), "");
    Files.writeString(backup.resolve("A.0.bucket"), "OLD-A");
    Files.writeString(backup.resolve("B.0.bucket"), "OLD-B");
    Files.writeString(backup.resolve("schema.json"), "OLD-SCHEMA");
    Files.writeString(db.resolve("A.0.bucket"), "NEW-A");
    Files.writeString((phase2Finished ? db : staged).resolve("B.0.bucket"), "NEW-B");
    Files.writeString((phase2Finished ? db : staged).resolve("schema.json"), "NEW-SCHEMA");

    SnapshotInstaller.recoverPendingSnapshotSwaps(root.resolve("databases"));

    assertThat(db.resolve("A.0.bucket")).hasContent("NEW-A");
    assertThat(db.resolve("B.0.bucket")).hasContent("NEW-B");
    assertThat(db.resolve("schema.json")).hasContent("NEW-SCHEMA");
    assertThat(db.resolve(".snapshot-pending")).doesNotExist();
    assertThat(staged).doesNotExist();
    assertThat(backup).doesNotExist();
  }

  // Regression for #7769: legacy swap that recovers no database keeps the backup and its pending marker
  /**
   * A legacy layout that cannot be recovered into a database (no schema anywhere) keeps every copy and its
   * pending marker: the roll-forward moved the staged file, but the backup is only deleted once the result is
   * a database.
   */
  @Test
  void legacySwapThatRecoversNoDatabaseKeepsTheBackupAndItsPendingMarker(@TempDir final Path root) throws Exception {
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
    assertThat(db.resolve("B.0.bucket")).hasContent("new-B");
    assertThat(backup.resolve("A.0.bucket")).hasContent("old-A");
    assertThat(db.resolve(".snapshot-pending")).exists();
  }

  // Regression for #7769: recovery does not delete already installed files
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

  // ------------------------------------------------------------------------------------------
  // Issue #8304
  // ------------------------------------------------------------------------------------------
  /*
   * A snapshot swap interrupted by a binary that recorded no phase (issue #8304) used to be preserved and never
   * recovered: the live directory was a partial mix that would not open, the pending marker kept the database from
   * loading, and the retained backup and marker made every later install refuse to run.
   * <p>
   * The layouts are built from real databases, the way the old binary leaves them: phase 1 moves each original into
   * {@code .snapshot-backup}, phase 2 moves each staged file over the live directory, and a kill between two moves
   * leaves whatever had moved so far.
   *
   */

  // Regression for #8304: legacy kill between two phase2 moves rolls forward to the snapshot
  /** Phase 2 killed part-way: every original is in the backup, some snapshot files are live, the rest still staged. */
  @Test
  void legacyKillBetweenTwoPhase2MovesRollsForwardToTheSnapshot(@TempDir final Path root) throws Exception {
    final Path databases = root.resolve("databases");
    final Path db = databases.resolve("mydb");
    final Path staged = db.resolve(".snapshot-new");
    final Path backup = db.resolve(".snapshot-backup");
    createDatabaseLegacy(backup, "old");
    createDatabaseLegacy(staged, "new");
    Files.writeString(db.resolve(".snapshot-pending"), "");
    Files.writeString(staged.resolve(".snapshot-complete"), "");
    final List<String> names = fileNames(staged);
    assertThat(names).hasSizeGreaterThan(2);
    // The first files move to the live directory, as in the report: the bucket, the configuration, the dictionary.
    for (final String name : names)
      if (!name.startsWith(".snapshot") && !name.startsWith("schema") && !name.startsWith("last-tx-id")
          && !name.startsWith("statistics"))
        Files.move(staged.resolve(name), db.resolve(name));
    assertThat(fileNames(db)).as("the layout under test has live snapshot files").anyMatch(n -> !n.startsWith(".snapshot"));
    assertThat(staged.resolve("schema.json")).exists();

    SnapshotInstaller.recoverPendingSnapshotSwaps(databases);
    SnapshotInstaller.recoverPendingSnapshotSwaps(databases);

    assertDatabaseValueLegacy(db, "new");
    assertThat(db.resolve(".snapshot-pending")).doesNotExist();
    assertThat(staged).doesNotExist();
    assertThat(backup).doesNotExist();
    assertThat(db.resolve(".snapshot-swap-state")).doesNotExist();
  }

  // Regression for #8304: legacy kill between two phase1 moves restores the original database
  /** Phase 1 killed part-way: the backup holds the originals that moved, the live directory those that did not. */
  @Test
  void legacyKillBetweenTwoPhase1MovesRestoresTheOriginalDatabase(@TempDir final Path root) throws Exception {
    final Path databases = root.resolve("databases");
    final Path db = databases.resolve("mydb");
    final Path staged = db.resolve(".snapshot-new");
    final Path backup = db.resolve(".snapshot-backup");
    createDatabaseLegacy(db, "old");
    createDatabaseLegacy(staged, "new");
    Files.createDirectories(backup);
    Files.writeString(db.resolve(".snapshot-pending"), "");
    Files.writeString(staged.resolve(".snapshot-complete"), "");
    final List<String> originals = fileNames(db);
    int moved = 0;
    for (final String name : originals) {
      if (name.startsWith(".snapshot"))
        continue;
      if (moved++ % 2 == 0)
        Files.move(db.resolve(name), backup.resolve(name), StandardCopyOption.REPLACE_EXISTING);
    }
    assertThat(fileNames(backup)).isNotEmpty();
    assertThat(fileNames(db)).as("the layout under test keeps some originals live").anyMatch(n -> !n.startsWith(".snapshot"));

    SnapshotInstaller.recoverPendingSnapshotSwaps(databases);
    SnapshotInstaller.recoverPendingSnapshotSwaps(databases);

    assertDatabaseValueLegacy(db, "old");
    assertThat(db.resolve(".snapshot-pending")).doesNotExist();
    assertThat(staged).doesNotExist();
    assertThat(backup).doesNotExist();
  }

  // Regression for #8304: disjoint layout from aphase2 crash still restores the original database
  /**
   * Disjoint names that are really a phase-2 crash: the only snapshot file that moved is one no original had. The
   * originals are restored and the node opens on them; the snapshot-only bucket, whose file id collides with an
   * original's, is moved aside to {@code .snapshot-orphans} because it would stop the database opening.
   */
  @Test
  void disjointLayoutFromAPhase2CrashStillRestoresTheOriginalDatabase(@TempDir final Path root) throws Exception {
    final Path databases = root.resolve("databases");
    final Path db = databases.resolve("mydb");
    final Path staged = db.resolve(".snapshot-new");
    final Path backup = db.resolve(".snapshot-backup");
    createDatabaseLegacy(backup, "old");
    createDatabaseLegacy(staged, "new");
    Files.writeString(db.resolve(".snapshot-pending"), "");
    Files.writeString(staged.resolve(".snapshot-complete"), "");
    // A real orphan: the bucket of a type only the snapshot has, moved over before the crash.
    createDatabaseLegacy(root.resolve("other"), "other", "SnapshotOnly");
    for (final String name : fileNames(root.resolve("other")))
      if (name.startsWith("SnapshotOnly_"))
        Files.move(root.resolve("other").resolve(name), db.resolve(name));
    assertThat(fileNames(db)).anyMatch(n -> n.startsWith("SnapshotOnly_"));

    SnapshotInstaller.recoverPendingSnapshotSwaps(databases);

    assertDatabaseValueLegacy(db, "old");
    assertThat(fileNames(db)).noneMatch(n -> n.startsWith("SnapshotOnly_"));
    assertThat(fileNames(db.resolve(".snapshot-orphans"))).anyMatch(n -> n.startsWith("SnapshotOnly_"));
    assertThat(db.resolve(".snapshot-pending")).doesNotExist();
    assertThat(staged).doesNotExist();
    assertThat(backup).doesNotExist();
  }

  // Regression for #8304: crash between the marker and the staging delete is resumed
  /** A crash after the quarantine marker but before the staging delete still quarantines on the next pass. */
  @Test
  void crashBetweenTheMarkerAndTheStagingDeleteIsResumed(@TempDir final Path root) throws Exception {
    final Path databases = root.resolve("databases");
    final Path db = databases.resolve("mydb");
    final Path staged = db.resolve(".snapshot-new");
    final Path backup = db.resolve(".snapshot-backup");
    createDatabaseLegacy(backup, "old");
    createDatabaseLegacy(staged, "new");
    Files.writeString(db.resolve(".snapshot-pending"), "");
    Files.writeString(staged.resolve(".snapshot-complete"), "");
    moveSnapshotOnlyBucketToLive(root, db);
    final AtomicBoolean crashed = new AtomicBoolean();
    SnapshotInstaller.swapProgressForTesting = point -> {
      if (point.equals("QUARANTINE_MARKED") && crashed.compareAndSet(false, true))
        throw new SimulatedCrashLegacy();
    };
    try {
      assertThatThrownBy(() -> SnapshotInstaller.recoverPendingSnapshotSwaps(databases))
          .isInstanceOf(SimulatedCrashLegacy.class);
    } finally {
      SnapshotInstaller.swapProgressForTesting = null;
    }
    assertThat(crashed).isTrue();
    // The worst case for the next pass: the staging lost its completion marker before the crash.
    Files.deleteIfExists(staged.resolve(".snapshot-complete"));

    SnapshotInstaller.recoverPendingSnapshotSwaps(databases);

    assertDatabaseValueLegacy(db, "old");
    assertThat(fileNames(db)).noneMatch(n -> n.startsWith("SnapshotOnly_"));
    assertThat(db.resolve(".snapshot-pending")).doesNotExist();
  }

  // Regression for #8304: crash between the restore and the quarantine is resumed
  /** A crash after the backup was consumed but before the orphan quarantine still gets the orphan moved aside. */
  @Test
  void crashBetweenTheRestoreAndTheQuarantineIsResumed(@TempDir final Path root) throws Exception {
    final Path databases = root.resolve("databases");
    final Path db = databases.resolve("mydb");
    final Path staged = db.resolve(".snapshot-new");
    final Path backup = db.resolve(".snapshot-backup");
    createDatabaseLegacy(backup, "old");
    createDatabaseLegacy(staged, "new");
    Files.writeString(db.resolve(".snapshot-pending"), "");
    Files.writeString(staged.resolve(".snapshot-complete"), "");
    moveSnapshotOnlyBucketToLive(root, db);
    final AtomicBoolean crashed = new AtomicBoolean();
    SnapshotInstaller.swapProgressForTesting = point -> {
      if (point.equals("RESTORED") && crashed.compareAndSet(false, true))
        throw new SimulatedCrashLegacy();
    };
    try {
      assertThatThrownBy(() -> SnapshotInstaller.recoverPendingSnapshotSwaps(databases))
          .isInstanceOf(SimulatedCrashLegacy.class);
    } finally {
      SnapshotInstaller.swapProgressForTesting = null;
    }
    assertThat(crashed).isTrue();

    SnapshotInstaller.recoverPendingSnapshotSwaps(databases);

    assertDatabaseValueLegacy(db, "old");
    assertThat(fileNames(db)).noneMatch(n -> n.startsWith("SnapshotOnly_"));
    assertThat(db.resolve(".snapshot-pending")).doesNotExist();
    assertThat(db.resolve(".snapshot-quarantine")).doesNotExist();
  }

  // Regression for #8304: restore and quarantine work when the original has only schema prev
  /** An original database caught mid schema rewrite has only schema.prev.json; recovery still restores it and quarantines. */
  @Test
  void restoreAndQuarantineWorkWhenTheOriginalHasOnlySchemaPrev(@TempDir final Path root) throws Exception {
    final Path databases = root.resolve("databases");
    final Path db = databases.resolve("mydb");
    final Path staged = db.resolve(".snapshot-new");
    final Path backup = db.resolve(".snapshot-backup");
    createDatabaseLegacy(backup, "old");
    Files.move(backup.resolve("schema.json"), backup.resolve("schema.prev.json"), StandardCopyOption.REPLACE_EXISTING);
    createDatabaseLegacy(staged, "new");
    Files.writeString(db.resolve(".snapshot-pending"), "");
    Files.writeString(staged.resolve(".snapshot-complete"), "");
    moveSnapshotOnlyBucketToLive(root, db);

    SnapshotInstaller.recoverPendingSnapshotSwaps(databases);

    assertDatabaseValueLegacy(db, "old");
    assertThat(fileNames(db)).noneMatch(n -> n.startsWith("SnapshotOnly_"));
    assertThat(db.resolve(".snapshot-pending")).doesNotExist();
  }

  // Regression for #8304: buckets not listed under atype stay live
  /**
   * The quarantine judges by file id, not by what the schema lists: a standalone bucket and a bucket with a dotted
   * name are never recorded under a type, and must stay live.
   */
  @Test
  void bucketsNotListedUnderATypeStayLive(@TempDir final Path root) throws Exception {
    final Path databases = root.resolve("databases");
    final Path db = databases.resolve("mydb");
    final Path staged = db.resolve(".snapshot-new");
    final Path backup = db.resolve(".snapshot-backup");
    createDatabaseLegacy(backup, "old");
    try (final DatabaseFactory factory = new DatabaseFactory(backup.toString()); final Database original = factory.open()) {
      original.getSchema().createBucket("Standalone");
      original.getSchema().createBucket("dotted.name");
    }
    createDatabaseLegacy(staged, "new");
    Files.writeString(db.resolve(".snapshot-pending"), "");
    Files.writeString(staged.resolve(".snapshot-complete"), "");
    moveSnapshotOnlyBucketToLive(root, db);

    SnapshotInstaller.recoverPendingSnapshotSwaps(databases);

    assertDatabaseValueLegacy(db, "old");
    assertThat(fileNames(db)).anyMatch(n -> n.startsWith("Standalone."));
    assertThat(fileNames(db)).anyMatch(n -> n.startsWith("dotted.name."));
    assertThat(fileNames(db)).noneMatch(n -> n.startsWith("SnapshotOnly_"));
  }

  private static void moveSnapshotOnlyBucketToLive(final Path root, final Path db) throws IOException {
    createDatabaseLegacy(root.resolve("other"), "other", "SnapshotOnly");
    for (final String name : fileNames(root.resolve("other")))
      if (name.startsWith("SnapshotOnly_"))
        Files.move(root.resolve("other").resolve(name), db.resolve(name));
    assertThat(fileNames(db)).anyMatch(n -> n.startsWith("SnapshotOnly_"));
  }

  // Regression for #8304: rollback interrupted mid restore is resumed by the next recovery
  /** A node that crashes again part-way through the rollback finishes it on the next recovery. */
  @Test
  void rollbackInterruptedMidRestoreIsResumedByTheNextRecovery(@TempDir final Path root) throws Exception {
    final Path databases = root.resolve("databases");
    final Path db = databases.resolve("mydb");
    final Path staged = db.resolve(".snapshot-new");
    final Path backup = db.resolve(".snapshot-backup");
    createDatabaseLegacy(backup, "old");
    createDatabaseLegacy(staged, "new");
    Files.writeString(db.resolve(".snapshot-pending"), "");
    Files.writeString(staged.resolve(".snapshot-complete"), "");
    moveSnapshotOnlyBucketToLive(root, db);
    final AtomicBoolean crashed = new AtomicBoolean();
    SnapshotInstaller.swapProgressForTesting = point -> {
      if (point.startsWith("RESTORING:") && crashed.compareAndSet(false, true))
        throw new SimulatedCrashLegacy();
    };
    try {
      assertThatThrownBy(() -> SnapshotInstaller.recoverPendingSnapshotSwaps(databases))
          .isInstanceOf(SimulatedCrashLegacy.class);
    } finally {
      SnapshotInstaller.swapProgressForTesting = null;
    }
    assertThat(crashed).isTrue();

    SnapshotInstaller.recoverPendingSnapshotSwaps(databases);

    assertDatabaseValueLegacy(db, "old");
    assertThat(fileNames(db)).noneMatch(n -> n.startsWith("SnapshotOnly_"));
    assertThat(db.resolve(".snapshot-pending")).doesNotExist();
    assertThat(backup).doesNotExist();
  }

  // Bypasses IOException handling, like process death.
  private static final class SimulatedCrashLegacy extends Error {
  }

  // Regression for #8304: rollback that recovers no database keeps the pending marker
  /** A restore that does not produce a database keeps the pending marker, so nothing is accepted as healthy. */
  @Test
  void rollbackThatRecoversNoDatabaseKeepsThePendingMarker(@TempDir final Path root) throws Exception {
    final Path databases = root.resolve("databases");
    final Path db = databases.resolve("mydb");
    final Path staged = db.resolve(".snapshot-new");
    final Path backup = db.resolve(".snapshot-backup");
    Files.createDirectories(staged);
    Files.createDirectories(backup);
    Files.writeString(db.resolve(".snapshot-pending"), "");
    Files.writeString(staged.resolve(".snapshot-complete"), "");
    Files.writeString(staged.resolve("C.0.bucket"), "new-C");
    Files.writeString(backup.resolve("A.0.bucket"), "old-A");
    Files.writeString(db.resolve("B.0.bucket"), "old-B");

    SnapshotInstaller.recoverPendingSnapshotSwaps(databases);

    assertThat(db.resolve("A.0.bucket")).hasContent("old-A");
    assertThat(db.resolve("B.0.bucket")).hasContent("old-B");
    assertThat(db.resolve(".snapshot-pending")).exists();
  }

  private static List<String> fileNames(final Path dir) throws IOException {
    final List<String> names = new ArrayList<>();
    try (final DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
      for (final Path entry : stream)
        names.add(entry.getFileName().toString());
    }
    return names;
  }

  private static void createDatabaseLegacy(final Path path, final String value) {
    createDatabaseLegacy(path, value, "Item");
  }

  private static void createDatabaseLegacy(final Path path, final String value, final String typeName) {
    try (final DatabaseFactory factory = new DatabaseFactory(path.toString()); final Database db = factory.create()) {
      db.transaction(() -> {
        db.getSchema().createDocumentType(typeName, 1);
        db.newDocument(typeName).set("value", value).save();
      });
    }
  }

  private static void assertDatabaseValueLegacy(final Path path, final String value) {
    try (final DatabaseFactory factory = new DatabaseFactory(path.toString()); final Database db = factory.open()) {
      assertThat(db.countType("Item", false)).isEqualTo(1);
      assertThat(db.iterateType("Item", false).next().asDocument().getString("value")).isEqualTo(value);
    }
  }

  // ------------------------------------------------------------------------------------------
  // Issue #9446
  // ------------------------------------------------------------------------------------------
  /*
   * Issue #9446: the snapshot swap decided "this entry belongs to the installer, not to the database" with
   * {@code name.startsWith(".snapshot")}, but {@code LocalSchema.checkValidBucketName} accepts a bucket named
   * {@code .snapshotItems}, and that bucket's component files are named after it. Each test drives one of the swap sites
   * that used the prefix with such a file and checks it is handled as database data: moved to the backup by the swap,
   * cleared by a rollback, and seen as a live entry by the legacy recovery classification.
   */

  private static final String PREFIXED_BUCKET_FILE = ".snapshotItems_0.1.65536.v0.bucket";

  @BeforeEach
  void snapshotsOpen() {
    SnapshotInstaller.snapshotOpensForTesting = path -> true;
  }

  @AfterEach
  void restoreSnapshotOpenProof() {
    SnapshotInstaller.snapshotOpensForTesting = null;
  }

  // Regression for #9446: swap moves aprefixed bucket file to the backup
  /** BACKING_UP: the original bucket file goes to the backup, so it is neither left beside the snapshot nor lost on rollback. */
  @Test
  void swapMovesAPrefixedBucketFileToTheBackup(@TempDir final Path tempDir) throws Exception {
    final Path dbDir = tempDir.resolve("mydb");
    final Path newDir = dbDir.resolve(SnapshotInstaller.SNAPSHOT_NEW_DIR);
    final Path backupDir = dbDir.resolve(SnapshotInstaller.SNAPSHOT_BACKUP_DIR);
    Files.createDirectories(newDir);
    Files.writeString(dbDir.resolve("schema.json"), "old");
    Files.writeString(dbDir.resolve(PREFIXED_BUCKET_FILE), "old-bucket");
    Files.writeString(newDir.resolve("schema.json"), "new");
    Files.writeString(newDir.resolve(SnapshotInstaller.SNAPSHOT_COMPLETE_FILE), "");

    SnapshotInstaller.atomicSwap(dbDir, newDir, backupDir);

    assertThat(dbDir.resolve(PREFIXED_BUCKET_FILE)).as("a bucket the snapshot does not have must not survive the swap")
        .doesNotExist();
    assertThat(backupDir.resolve(PREFIXED_BUCKET_FILE)).hasContent("old-bucket");
    assertThat(dbDir.resolve("schema.json")).hasContent("new");
  }

  // Regression for #9446: rollback clears aprefixed bucket file of the failed snapshot
  /** ROLLING_BACK: clearLiveDatabaseFiles removes the failed snapshot's bucket file before the backup is restored. */
  @Test
  void rollbackClearsAPrefixedBucketFileOfTheFailedSnapshot(@TempDir final Path databasesDir) throws Exception {
    final Path dbDir = databasesDir.resolve("mydb");
    final Path backupDir = dbDir.resolve(SnapshotInstaller.SNAPSHOT_BACKUP_DIR);
    Files.createDirectories(backupDir);
    Files.writeString(dbDir.resolve(SnapshotInstaller.SNAPSHOT_PENDING_FILE), "");
    Files.writeString(dbDir.resolve(SnapshotInstaller.SNAPSHOT_SWAP_STATE_FILE), "ROLLING_BACK");
    Files.writeString(dbDir.resolve("schema.json"), "new");
    Files.writeString(dbDir.resolve(PREFIXED_BUCKET_FILE), "snapshot-only-bucket");
    Files.writeString(backupDir.resolve("schema.json"), "old");

    SnapshotInstaller.recoverPendingSnapshotSwaps(databasesDir);

    assertThat(dbDir.resolve(PREFIXED_BUCKET_FILE)).as("a bucket only the failed snapshot had must not survive the rollback")
        .doesNotExist();
    assertThat(dbDir.resolve("schema.json")).hasContent("old");
    assertThat(dbDir.resolve(SnapshotInstaller.SNAPSHOT_PENDING_FILE)).doesNotExist();
    assertThat(backupDir).doesNotExist();
  }

  // Regression for #9446: legacy recovery counts aprefixed bucket file as alive entry
  /**
   * Legacy recovery, phase 1 killed before the bucket file was backed up: the live directory still holds that original,
   * so it is a live entry and the layout is a phase-1 crash, rolled back to the original database. Treated as installer
   * state, the directory looked empty and the swap was completed with the stale bucket file left beside the snapshot.
   */
  @Test
  void legacyRecoveryCountsAPrefixedBucketFileAsALiveEntry(@TempDir final Path databasesDir) throws Exception {
    final Path dbDir = databasesDir.resolve("mydb");
    final Path newDir = dbDir.resolve(SnapshotInstaller.SNAPSHOT_NEW_DIR);
    final Path backupDir = dbDir.resolve(SnapshotInstaller.SNAPSHOT_BACKUP_DIR);
    Files.createDirectories(newDir);
    Files.createDirectories(backupDir);
    Files.writeString(dbDir.resolve(SnapshotInstaller.SNAPSHOT_PENDING_FILE), "");
    Files.writeString(dbDir.resolve(PREFIXED_BUCKET_FILE), "old-bucket");
    Files.writeString(backupDir.resolve("schema.json"), "old");
    Files.writeString(newDir.resolve("schema.json"), "new");
    Files.writeString(newDir.resolve(SnapshotInstaller.SNAPSHOT_COMPLETE_FILE), "");

    SnapshotInstaller.recoverPendingSnapshotSwaps(databasesDir);

    assertThat(dbDir.resolve("schema.json")).hasContent("old");
    assertThat(dbDir.resolve(PREFIXED_BUCKET_FILE)).hasContent("old-bucket");
    assertThat(dbDir.resolve(SnapshotInstaller.SNAPSHOT_PENDING_FILE)).doesNotExist();
    assertThat(newDir).doesNotExist();
    assertThat(backupDir).doesNotExist();
  }

  // Regression for #9446: a prefixed bucket file is not asnapshot machinery entry
  @Test
  void aPrefixedBucketFileIsNotASnapshotMachineryEntry() {
    assertThat(SnapshotInstaller.isSnapshotMachineryEntryName(PREFIXED_BUCKET_FILE)).isFalse();
    assertThat(SnapshotInstaller.isSnapshotMachineryEntryName(".snapshot")).isFalse();
    assertThat(SnapshotInstaller.isSnapshotMachineryEntryName(".snapshot-newer")).isFalse();
    // A bucket named like a machinery directory still cannot collide: its component files carry the file id suffix.
    assertThat(SnapshotInstaller.isSnapshotMachineryEntryName(".snapshot-new_0.1.65536.v0.bucket")).isFalse();
  }

  // Regression for #9446: legacy rollback quarantines acolliding prefixed bucket file
  /**
   * Legacy recovery, disjoint names: the live prefixed bucket file is a snapshot-only file whose file id collides with a
   * restored original, so the quarantine (which lists the live entries) must move it to the orphans rather than leave
   * two files on one id. Treated as installer state, it was neither listed nor moved.
   */
  @Test
  void legacyRollbackQuarantinesACollidingPrefixedBucketFile(@TempDir final Path databasesDir) throws Exception {
    final Path dbDir = databasesDir.resolve("mydb");
    final Path newDir = dbDir.resolve(SnapshotInstaller.SNAPSHOT_NEW_DIR);
    final Path backupDir = dbDir.resolve(SnapshotInstaller.SNAPSHOT_BACKUP_DIR);
    final String collidingSnapshotFile = ".snapshotItems_7.1.65536.v0.bucket";
    Files.createDirectories(newDir);
    Files.createDirectories(backupDir);
    Files.writeString(dbDir.resolve(SnapshotInstaller.SNAPSHOT_PENDING_FILE), "");
    Files.writeString(dbDir.resolve(collidingSnapshotFile), "snapshot-bucket");
    Files.writeString(backupDir.resolve("schema.json"), "old");
    Files.writeString(backupDir.resolve("Item_7.1.65536.v0.bucket"), "original-bucket");
    Files.writeString(newDir.resolve("schema.json"), "new");
    Files.writeString(newDir.resolve(SnapshotInstaller.SNAPSHOT_COMPLETE_FILE), "");

    SnapshotInstaller.recoverPendingSnapshotSwaps(databasesDir);

    assertThat(dbDir.resolve("schema.json")).hasContent("old");
    assertThat(dbDir.resolve("Item_7.1.65536.v0.bucket")).hasContent("original-bucket");
    assertThat(dbDir.resolve(collidingSnapshotFile)).doesNotExist();
    assertThat(dbDir.resolve(SnapshotInstaller.SNAPSHOT_ORPHANS_DIR).resolve(collidingSnapshotFile)).hasContent("snapshot-bucket");
    assertThat(dbDir.resolve(SnapshotInstaller.SNAPSHOT_PENDING_FILE)).doesNotExist();
  }

  // Regression for #9446: every snapshot installer entry is amachinery entry
  /**
   * Guard: every {@code .snapshot...} name the installer declares, file or directory, is one the swap sites know, so a
   * new marker or staging directory added without being listed fails here rather than being moved into the backup.
   */
  @Test
  void everySnapshotInstallerEntryIsAMachineryEntry() throws Exception {
    final List<String> names = new ArrayList<>();
    for (final Field field : SnapshotInstaller.class.getDeclaredFields()) {
      if (!Modifier.isStatic(field.getModifiers()) || field.getType() != String.class)
        continue;
      field.setAccessible(true);
      final String value = (String) field.get(null);
      if ((field.getName().startsWith("SNAPSHOT_") && (field.getName().endsWith("_FILE") || field.getName().endsWith("_DIR")))
          || (value != null && value.startsWith(".snapshot")))
        names.add(value);
    }

    assertThat(names).contains(SnapshotInstaller.SNAPSHOT_NEW_DIR, SnapshotInstaller.SNAPSHOT_BACKUP_DIR,
        SnapshotInstaller.SNAPSHOT_ORPHANS_DIR, SnapshotInstaller.SNAPSHOT_PENDING_FILE);
    assertThat(names).allSatisfy(name -> assertThat(SnapshotInstaller.isSnapshotMachineryEntryName(name)).as(name).isTrue());
  }
}
