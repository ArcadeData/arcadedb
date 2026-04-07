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
package com.arcadedb.server.ha.ratis;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests crash recovery for the snapshot swap operation in ArcadeDBStateMachine.
 * Simulates various crash scenarios during the directory swap phase.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SnapshotSwapRecoveryTest {

  @TempDir
  Path tempDir;

  /**
   * Simulates a crash after the live DB was moved to backup but before the new snapshot
   * was moved into place. Recovery should complete the swap by moving the temp dir into
   * the live path.
   */
  @Test
  void testRecoverAfterCrashBetweenMoveAwayAndMoveIn() throws IOException {
    final Path dbDir = tempDir.resolve("databases");
    Files.createDirectories(dbDir);

    final Path livePath = dbDir.resolve("testdb");
    final Path backupPath = dbDir.resolve("testdb.snapshot-old");
    final Path snapshotPath = dbDir.resolve("testdb.snapshot-tmp");

    // Simulate state after crash: live DB moved to backup, temp snapshot exists, live path is gone
    Files.createDirectories(backupPath);
    Files.writeString(backupPath.resolve("old-data.dat"), "old data");

    Files.createDirectories(snapshotPath);
    Files.writeString(snapshotPath.resolve("new-data.dat"), "new snapshot data");

    // Write the pending marker
    final Path markerPath = dbDir.resolve("testdb.snapshot-pending");
    Files.writeString(markerPath, "testdb");

    // Run recovery
    ArcadeDBStateMachine.recoverPendingSnapshotSwaps(dbDir);

    // The snapshot should now be in the live path
    assertThat(livePath).exists();
    assertThat(livePath.resolve("new-data.dat")).exists();
    assertThat(Files.readString(livePath.resolve("new-data.dat"))).isEqualTo("new snapshot data");

    // Backup and temp dirs should be cleaned up
    assertThat(backupPath).doesNotExist();
    assertThat(snapshotPath).doesNotExist();
    assertThat(markerPath).doesNotExist();
  }

  /**
   * Simulates a crash after the marker was written but before the live DB was moved.
   * Both the live DB and temp snapshot exist. Recovery should complete the swap
   * (move live to backup, move temp to live, clean up).
   */
  @Test
  void testRecoverAfterCrashBeforeMoveAway() throws IOException {
    final Path dbDir = tempDir.resolve("databases");
    Files.createDirectories(dbDir);

    final Path livePath = dbDir.resolve("testdb");
    final Path backupPath = dbDir.resolve("testdb.snapshot-old");
    final Path snapshotPath = dbDir.resolve("testdb.snapshot-tmp");

    // Live DB still exists
    Files.createDirectories(livePath);
    Files.writeString(livePath.resolve("old-data.dat"), "old data");

    // Temp snapshot is ready
    Files.createDirectories(snapshotPath);
    Files.writeString(snapshotPath.resolve("new-data.dat"), "new snapshot data");

    // Write the pending marker
    final Path markerPath = dbDir.resolve("testdb.snapshot-pending");
    Files.writeString(markerPath, "testdb");

    // Run recovery
    ArcadeDBStateMachine.recoverPendingSnapshotSwaps(dbDir);

    // The snapshot should now be in the live path
    assertThat(livePath).exists();
    assertThat(livePath.resolve("new-data.dat")).exists();
    assertThat(Files.readString(livePath.resolve("new-data.dat"))).isEqualTo("new snapshot data");

    // Old data should be gone
    assertThat(livePath.resolve("old-data.dat")).doesNotExist();
    assertThat(backupPath).doesNotExist();
    assertThat(snapshotPath).doesNotExist();
    assertThat(markerPath).doesNotExist();
  }

  /**
   * If both the marker and backup exist but the temp snapshot is gone (somehow deleted),
   * recovery should rollback by restoring the backup to the live path.
   */
  @Test
  void testRecoverRollbackWhenSnapshotTempMissing() throws IOException {
    final Path dbDir = tempDir.resolve("databases");
    Files.createDirectories(dbDir);

    final Path livePath = dbDir.resolve("testdb");
    final Path backupPath = dbDir.resolve("testdb.snapshot-old");
    final Path snapshotPath = dbDir.resolve("testdb.snapshot-tmp");

    // Only backup exists - temp snapshot was somehow lost
    Files.createDirectories(backupPath);
    Files.writeString(backupPath.resolve("old-data.dat"), "old data");

    // Write the pending marker
    final Path markerPath = dbDir.resolve("testdb.snapshot-pending");
    Files.writeString(markerPath, "testdb");

    // Run recovery
    ArcadeDBStateMachine.recoverPendingSnapshotSwaps(dbDir);

    // Should rollback: backup restored to live path
    assertThat(livePath).exists();
    assertThat(livePath.resolve("old-data.dat")).exists();
    assertThat(Files.readString(livePath.resolve("old-data.dat"))).isEqualTo("old data");

    // Cleanup
    assertThat(backupPath).doesNotExist();
    assertThat(snapshotPath).doesNotExist();
    assertThat(markerPath).doesNotExist();
  }

  /**
   * No pending markers - recovery should be a no-op.
   */
  @Test
  void testNoRecoveryNeededWhenNoMarkers() throws IOException {
    final Path dbDir = tempDir.resolve("databases");
    Files.createDirectories(dbDir);

    // Create a normal database directory
    final Path livePath = dbDir.resolve("testdb");
    Files.createDirectories(livePath);
    Files.writeString(livePath.resolve("data.dat"), "normal data");

    // Run recovery - should not change anything
    ArcadeDBStateMachine.recoverPendingSnapshotSwaps(dbDir);

    assertThat(livePath).exists();
    assertThat(Files.readString(livePath.resolve("data.dat"))).isEqualTo("normal data");
  }

  /**
   * If the swap already completed but the marker wasn't deleted (crash after move-in
   * but before marker deletion), recovery should just clean up leftover dirs and marker.
   */
  @Test
  void testRecoverAfterSwapCompletedButMarkerNotDeleted() throws IOException {
    final Path dbDir = tempDir.resolve("databases");
    Files.createDirectories(dbDir);

    final Path livePath = dbDir.resolve("testdb");
    final Path backupPath = dbDir.resolve("testdb.snapshot-old");

    // Swap completed: live path has new data, backup still exists
    Files.createDirectories(livePath);
    Files.writeString(livePath.resolve("new-data.dat"), "new snapshot data");

    Files.createDirectories(backupPath);
    Files.writeString(backupPath.resolve("old-data.dat"), "old data");

    // Marker still exists
    final Path markerPath = dbDir.resolve("testdb.snapshot-pending");
    Files.writeString(markerPath, "testdb");

    // Run recovery
    ArcadeDBStateMachine.recoverPendingSnapshotSwaps(dbDir);

    // Live path should be untouched
    assertThat(livePath).exists();
    assertThat(Files.readString(livePath.resolve("new-data.dat"))).isEqualTo("new snapshot data");

    // Backup and marker cleaned up
    assertThat(backupPath).doesNotExist();
    assertThat(markerPath).doesNotExist();
  }

  /**
   * Stale WAL files in the snapshot directory should be cleaned up during recovery.
   */
  @Test
  void testRecoverCleansStaleWalFiles() throws IOException {
    final Path dbDir = tempDir.resolve("databases");
    Files.createDirectories(dbDir);

    final Path livePath = dbDir.resolve("testdb");
    final Path snapshotPath = dbDir.resolve("testdb.snapshot-tmp");

    // Temp snapshot with stale WAL files
    Files.createDirectories(snapshotPath);
    Files.writeString(snapshotPath.resolve("new-data.dat"), "new data");
    Files.writeString(snapshotPath.resolve("txlog_0.wal"), "stale wal");
    Files.writeString(snapshotPath.resolve("txlog_1.wal"), "stale wal 2");

    // Write the pending marker
    final Path markerPath = dbDir.resolve("testdb.snapshot-pending");
    Files.writeString(markerPath, "testdb");

    // Run recovery
    ArcadeDBStateMachine.recoverPendingSnapshotSwaps(dbDir);

    // Live path should have new data but no WAL files
    assertThat(livePath).exists();
    assertThat(livePath.resolve("new-data.dat")).exists();
    assertThat(livePath.resolve("txlog_0.wal")).doesNotExist();
    assertThat(livePath.resolve("txlog_1.wal")).doesNotExist();
    assertThat(markerPath).doesNotExist();
  }
}
