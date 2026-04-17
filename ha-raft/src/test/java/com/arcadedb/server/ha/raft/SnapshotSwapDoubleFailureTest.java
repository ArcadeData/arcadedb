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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for the Phase 2 swap logic in
 * {@link SnapshotInstaller#performSnapshotSwap}. The double-failure case (swap fails AND
 * rollback fails) is exercised by injecting a {@link SnapshotInstaller.PathMover} that
 * throws on selected rename calls. In that case the pending marker MUST survive so that
 * {@link SnapshotInstaller#recoverPendingSnapshotSwaps(Path)} can finish the job on the
 * next startup, and callers must NOT treat {@code dbPath} as a healthy database.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SnapshotSwapDoubleFailureTest {

  @TempDir
  Path tempRoot;

  /**
   * Fault-injecting mover: forwards to {@link Files#move} for the first N calls and throws
   * {@link IOException} for every call after that.
   */
  private static final class FailAfterMover implements SnapshotInstaller.PathMover {
    private final int successes;
    private       int calls = 0;

    FailAfterMover(final int successes) {
      this.successes = successes;
    }

    @Override
    public void move(final Path src, final Path dst) throws IOException {
      calls++;
      if (calls > successes)
        throw new IOException("injected failure on move #" + calls + " (" + src + " -> " + dst + ")");
      Files.move(src, dst);
    }
  }

  /**
   * Swap fails on the second move (tempDir -> dbPath), and rollback fails on its move
   * (backupDir -> dbPath). The method must:
   * <ul>
   *   <li>Return {@link SnapshotInstaller.SwapOutcome#UNRECOVERABLE}</li>
   *   <li>NOT delete the pending marker (it is our only signal for startup recovery)</li>
   *   <li>Preserve {@code backupDir} so recovery can still restore the database</li>
   *   <li>Clean up the now-useless tempDir</li>
   * </ul>
   * A follow-up call to {@link SnapshotInstaller#recoverPendingSnapshotSwaps(Path)} must
   * then restore the database from the backup and clear the marker.
   */
  @Test
  void doubleFailureKeepsMarkerAndDoesNotLeakTemp() throws IOException {
    final Path dbDir = tempRoot.resolve("databases");
    Files.createDirectories(dbDir);

    final Path dbPath = dbDir.resolve("testdb");
    final Path tempDir = dbDir.resolve("testdb.snapshot-tmp");
    final Path backupDir = dbDir.resolve("testdb.snapshot-old");
    final Path markerFile = dbDir.resolve("testdb.snapshot-pending");

    Files.createDirectories(dbPath);
    Files.writeString(dbPath.resolve("old.dat"), "old data");

    Files.createDirectories(tempDir);
    Files.writeString(tempDir.resolve("new.dat"), "new data");
    Files.writeString(tempDir.resolve(SnapshotInstaller.SNAPSHOT_COMPLETE_MARKER), "");

    // First mover call (dbPath -> backupDir) succeeds; second (tempDir -> dbPath) and third
    // (rollback backupDir -> dbPath) both throw, reproducing the disk-fault scenario.
    final FailAfterMover mover = new FailAfterMover(1);

    final SnapshotInstaller.SwapOutcome outcome = SnapshotInstaller.performSnapshotSwap(
        dbPath, tempDir, backupDir, markerFile, "testdb", mover);

    assertThat(outcome).isEqualTo(SnapshotInstaller.SwapOutcome.UNRECOVERABLE);

    // Marker must be preserved for startup recovery.
    assertThat(markerFile).exists();
    assertThat(Files.readString(markerFile)).isEqualTo("testdb");

    // Backup must still be on disk so recovery can finish the swap.
    assertThat(backupDir).exists();
    assertThat(backupDir.resolve("old.dat")).exists();

    // dbPath was moved to backupDir by the first (successful) move; rollback could not restore it.
    assertThat(dbPath).doesNotExist();

    // The useless temp directory must not leak.
    assertThat(tempDir).doesNotExist();

    // Verify startup recovery can finish the job. The snapshot data is gone (tempDir was
    // cleaned up) and there is no valid completion marker, so recovery must roll back
    // from backupDir.
    SnapshotInstaller.recoverPendingSnapshotSwaps(dbDir);

    assertThat(dbPath).exists();
    assertThat(dbPath.resolve("old.dat")).exists();
    assertThat(Files.readString(dbPath.resolve("old.dat"))).isEqualTo("old data");
    assertThat(backupDir).doesNotExist();
    assertThat(markerFile).doesNotExist();
  }

  /**
   * Swap fails on the second move (tempDir -> dbPath) but rollback (backupDir -> dbPath)
   * succeeds. The method must return {@link SnapshotInstaller.SwapOutcome#ROLLED_BACK},
   * restore dbPath from the backup, and clean up the marker and temp directory.
   */
  @Test
  void rollbackSuccessRestoresDbAndClearsMarker() throws IOException {
    final Path dbDir = tempRoot.resolve("databases");
    Files.createDirectories(dbDir);

    final Path dbPath = dbDir.resolve("testdb");
    final Path tempDir = dbDir.resolve("testdb.snapshot-tmp");
    final Path backupDir = dbDir.resolve("testdb.snapshot-old");
    final Path markerFile = dbDir.resolve("testdb.snapshot-pending");

    Files.createDirectories(dbPath);
    Files.writeString(dbPath.resolve("old.dat"), "old data");

    Files.createDirectories(tempDir);
    Files.writeString(tempDir.resolve("new.dat"), "new data");
    Files.writeString(tempDir.resolve(SnapshotInstaller.SNAPSHOT_COMPLETE_MARKER), "");

    // First move succeeds (dbPath -> backupDir), second throws (tempDir -> dbPath),
    // third (rollback backupDir -> dbPath) succeeds.
    final SnapshotInstaller.PathMover mover = new SnapshotInstaller.PathMover() {
      private int calls = 0;

      @Override
      public void move(final Path src, final Path dst) throws IOException {
        calls++;
        if (calls == 2)
          throw new IOException("injected failure on forward move");
        Files.move(src, dst);
      }
    };

    final SnapshotInstaller.SwapOutcome outcome = SnapshotInstaller.performSnapshotSwap(
        dbPath, tempDir, backupDir, markerFile, "testdb", mover);

    assertThat(outcome).isEqualTo(SnapshotInstaller.SwapOutcome.ROLLED_BACK);

    // dbPath restored from backup, marker cleaned up, temp and backup gone.
    assertThat(dbPath).exists();
    assertThat(dbPath.resolve("old.dat")).exists();
    assertThat(Files.readString(dbPath.resolve("old.dat"))).isEqualTo("old data");
    assertThat(backupDir).doesNotExist();
    assertThat(tempDir).doesNotExist();
    assertThat(markerFile).doesNotExist();
  }

  /**
   * Happy path through {@link SnapshotInstaller#performSnapshotSwap}: the swap completes,
   * the marker and backup are removed, and the completion marker inside the snapshot is
   * cleaned up.
   */
  @Test
  void happyPathSwapsAndCleansUp() throws IOException {
    final Path dbDir = tempRoot.resolve("databases");
    Files.createDirectories(dbDir);

    final Path dbPath = dbDir.resolve("testdb");
    final Path tempDir = dbDir.resolve("testdb.snapshot-tmp");
    final Path backupDir = dbDir.resolve("testdb.snapshot-old");
    final Path markerFile = dbDir.resolve("testdb.snapshot-pending");

    Files.createDirectories(dbPath);
    Files.writeString(dbPath.resolve("old.dat"), "old data");

    Files.createDirectories(tempDir);
    Files.writeString(tempDir.resolve("new.dat"), "new data");
    Files.writeString(tempDir.resolve(SnapshotInstaller.SNAPSHOT_COMPLETE_MARKER), "");

    final SnapshotInstaller.SwapOutcome outcome = SnapshotInstaller.performSnapshotSwap(
        dbPath, tempDir, backupDir, markerFile, "testdb", Files::move);

    assertThat(outcome).isEqualTo(SnapshotInstaller.SwapOutcome.SUCCESS);
    assertThat(dbPath).exists();
    assertThat(dbPath.resolve("new.dat")).exists();
    assertThat(Files.readString(dbPath.resolve("new.dat"))).isEqualTo("new data");
    assertThat(dbPath.resolve("old.dat")).doesNotExist();
    assertThat(dbPath.resolve(SnapshotInstaller.SNAPSHOT_COMPLETE_MARKER)).doesNotExist();
    assertThat(backupDir).doesNotExist();
    assertThat(tempDir).doesNotExist();
    assertThat(markerFile).doesNotExist();
  }
}
