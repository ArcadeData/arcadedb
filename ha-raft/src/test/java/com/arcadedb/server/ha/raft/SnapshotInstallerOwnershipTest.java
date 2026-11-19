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

import com.arcadedb.engine.WALFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Ownership of WAL files and of the in-flight install guard held by overlapping snapshot installs.
 */
class SnapshotInstallerOwnershipTest {

  // ------------------------------------------------------------------------------------------
  // Issue #7505
  // ------------------------------------------------------------------------------------------
  /*
   * Regression test for issue #7505.
   * <p>
   * {@code SnapshotInstaller.cleanupWalFiles} listed every {@code *.wal} file in the freshly swapped-in database
   * directory and deleted it unconditionally - a second, unprotected copy of the failure class PR #7504 had just
   * fixed in {@code TransactionManager.close()} for discussion #7479. Under normal HA operation there is no live
   * local instance holding those files at that point, but on a database directory shared with another live
   * process - the case #7479 reported, where a Docker Desktop bind mount does not enforce the per-process advisory
   * lock across the container boundary - this sweep reproduced the exact same corruption.
   * <p>
   * It now goes through {@link WALFile#deleteIfNotHeldByAnotherInstance(File)}, the same lock-aware sweep the
   * engine uses, so an orphan is still removed and a file somebody else has open is left alone.
   *
   */

  @TempDir
  private Path dbDir;

  // Regression for #7505: an orphaned wal file is still removed but one another instance holds is left alone
  @Test
  void anOrphanedWalFileIsStillRemovedButOneAnotherInstanceHoldsIsLeftAlone() throws Exception {
    final File orphan = dbDir.resolve("txlog_0.wal").toFile();
    assertThat(orphan.createNewFile()).isTrue();

    // Stands in for a second, still-running instance holding this WAL file open. Every WALFile has taken an
    // exclusive lock on itself for its whole life since #7479, which is what the sweep probes for.
    final File heldByAnotherInstance = dbDir.resolve("txlog_1.wal").toFile();
    final WALFile anotherInstanceWAL = new WALFile(heldByAnotherInstance.getPath());
    try {
      SnapshotInstaller.cleanupWalFiles(dbDir);

      assertThat(orphan)
          .as("an orphan nobody holds open must still be swept, exactly as before")
          .doesNotExist();
      assertThat(heldByAnotherInstance)
          .as("a WAL file another live instance still has open must not be deleted out from under it")
          .exists();
    } finally {
      anotherInstanceWAL.close();
      heldByAnotherInstance.delete();
    }
  }

  // Regression for #7505: an empty or missing directory is ano op
  /** A directory with no WAL files at all, and one that does not exist, are both no-ops rather than failures. */
  @Test
  void anEmptyOrMissingDirectoryIsANoOp() {
    SnapshotInstaller.cleanupWalFiles(dbDir);
    SnapshotInstaller.cleanupWalFiles(dbDir.resolve("no-such-directory"));

    assertThat(dbDir).isEmptyDirectory();
  }

  // Regression for #7505: non wal files are untouched
  /** Files that are not WAL files are untouched: the sweep is still scoped by the {@code .wal} suffix. */
  @Test
  void nonWalFilesAreUntouched() throws Exception {
    final File notAWal = dbDir.resolve("schema.json").toFile();
    assertThat(notAWal.createNewFile()).isTrue();

    SnapshotInstaller.cleanupWalFiles(dbDir);

    assertThat(notAWal).exists();
  }

  // ------------------------------------------------------------------------------------------
  // Issue #7622
  // ------------------------------------------------------------------------------------------
  /*
   * Regression test for issue #7622: {@code INSTALLS_IN_FLIGHT} became load-bearing when the #7128 recovery-pass
   * skip guards in {@link SnapshotInstaller#recoverPendingSnapshotSwaps(Path)} started consulting it to decide
   * whether it is safe to delete a directory, but the registration itself was still a plain presence set with
   * no owner. Two overlapping installs of the same database shared one entry, and the {@code finally} of
   * whichever finished FIRST removed it unconditionally - clearing the guard while the other install was still
   * running, which is exactly the "delete a live install's directory" failure #7128 was filed about.
   * <p>
   * This test drives the two test-only registration hooks directly (they exercise the same
   * {@code registerInstallInFlight}/{@code releaseInstallInFlight} pair the real {@code install()} uses) to
   * simulate two overlapping installs of the same directory, and proves the guard stays up for as long as
   * either one is still "running".
   *
   */

  // Regression for #7622: guard stays up until every overlapping install releases it
  @Test
  void guardStaysUpUntilEveryOverlappingInstallReleasesIt(@TempDir final Path databasesDir) throws Exception {
    final Path dbDir = databasesDir.resolve("mydb");
    final Path snapshotNew = dbDir.resolve(".snapshot-new");

    Files.createDirectories(snapshotNew);
    Files.writeString(dbDir.resolve(".snapshot-pending"), "");
    Files.writeString(dbDir.resolve("schema.json"), "{}");
    Files.writeString(snapshotNew.resolve("partial.dat"), "still-being-extracted");

    // Two overlapping installs of the same directory register independently, as install() does when a second
    // install joins before the first's bounded maintenance-slot wait has expired (see the class note on
    // SnapshotInstaller.INSTALLS_IN_FLIGHT).
    SnapshotInstaller.markInstallInFlightForTesting(dbDir);
    SnapshotInstaller.markInstallInFlightForTesting(dbDir);
    try {
      // The FIRST of the two finishes and releases its registration. Before the fix this cleared the ONLY
      // entry outright, even though the second install is still running.
      SnapshotInstaller.clearInstallInFlightForTesting(dbDir);

      SnapshotInstaller.recoverPendingSnapshotSwaps(databasesDir);

      // The second (still-running) install's directory must survive: the guard has to stay up as long as ANY
      // overlapping install holds it, not just the first one to register.
      assertThat(snapshotNew).as("a still-running overlapping install's staging directory must not be deleted")
          .exists();
      assertThat(snapshotNew.resolve("partial.dat")).exists();
      assertThat(dbDir.resolve(".snapshot-pending")).exists();
    } finally {
      // The second (and last) install now releases its own registration.
      SnapshotInstaller.clearInstallInFlightForTesting(dbDir);
    }

    // With nothing left in flight, the same on-disk state - now genuinely orphaned - is cleaned up normally.
    SnapshotInstaller.recoverPendingSnapshotSwaps(databasesDir);
    assertThat(snapshotNew).doesNotExist();
    assertThat(dbDir.resolve(".snapshot-pending")).doesNotExist();
  }
}
