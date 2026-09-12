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
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.backup.BackupCoordinator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #7449, a follow-up to #7444.
 * <p>
 * #7444 gave {@link SnapshotInstaller#install} this node's per-database maintenance slot, so a scheduled backup
 * cannot read a directory the node is replacing from the leader's snapshot. The startup crash-recovery pass was
 * left out: {@code recoverPendingSnapshotSwaps} finishes or rolls back a swap a crash cut short, which moves files
 * in and out of the live database directory exactly as an install does, and it took no slot - its signature had no
 * {@link ArcadeDBServer} to reach a {@link BackupCoordinator} through.
 * <p>
 * The pass is not startup-only in practice: {@code RaftHAServer.restartRatis}, driven by the HealthMonitor, builds a
 * new state machine and starts a new Ratis server while the ArcadeDB server is ONLINE, so
 * {@code ArcadeStateMachine.initialize()} - and this recovery - runs again with the auto-backup scheduler live and
 * the databases registered.
 * <p>
 * These tests drive the real {@code recoverPendingSnapshotSwaps(Path, ArcadeDBServer)} against synthetic pending-swap
 * state and a real server's coordinator, and pin both directions of the exclusion plus the two properties that make
 * it safe: the wait is bounded, and the slot is per database rather than one for the whole pass.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue7449StartupRecoveryTakesMaintenanceSlotTest {

  private static final String DB_NAME      = "recov7449";
  private static final String OTHER_DB     = "other7449";
  private static final String PASSWORD     = "DefaultPasswordForTests";
  private static final String NEW_CONTENT  = "new-snapshot-data";
  private static final String OLD_CONTENT  = "old-data";

  private ArcadeDBServer server;

  @AfterEach
  void stopServer() {
    SnapshotInstaller.recoveryBarrierForTesting = null;
    if (server != null) {
      server.stop();
      server = null;
    }
  }

  /**
   * The bug as reported: while this node repairs a database directory, a backup of that database must not be
   * admitted. The recovery is paused inside {@code recoverSingleDatabase} - the exact moment the files are being
   * moved - and a backup is attempted from the test thread, standing in for the tick {@code BackupTask.run} fires.
   * <p>
   * Without the fix the reservation succeeds and the tick goes on to read a directory being rebuilt.
   */
  @Test
  @Timeout(120)
  void aBackupIsRefusedWhileRecoveryIsRepairingTheDatabaseDirectory(@TempDir final Path root) throws Exception {
    final Path databasesDir = startServer(root, 60_000);
    final Path dbDir = stageInterruptedSwap(databasesDir, DB_NAME);

    final CountDownLatch insideRecovery = new CountDownLatch(1);
    final CountDownLatch releaseRecovery = new CountDownLatch(1);
    SnapshotInstaller.recoveryBarrierForTesting = () -> {
      insideRecovery.countDown();
      try {
        releaseRecovery.await();
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    };

    final Thread recovery = startRecovery(databasesDir, new AtomicBoolean(), new AtomicReference<>());

    final BackupCoordinator coordinator = server.getBackupCoordinator();
    try {
      assertThat(insideRecovery.await(60, TimeUnit.SECONDS)).as("the recovery reached the in-repair barrier").isTrue();

      assertThat(coordinator.isInProgress(DB_NAME, BackupCoordinator.Operation.RESTORE))
          .as("the recovery holds this node's maintenance slot while it repairs the directory").isTrue();
      assertThat(coordinator.begin(DB_NAME, BackupCoordinator.Operation.BACKUP))
          .as("a scheduled backup of a database whose directory is being repaired is refused")
          .isEqualTo(BackupCoordinator.Operation.RESTORE);
    } finally {
      releaseRecovery.countDown();
    }

    recovery.join(60_000);
    assertThat(recovery.isAlive()).as("the recovery finished once released").isFalse();

    // The slot is released whatever the outcome: a leaked reservation would block every later backup of this
    // database until the server restarts.
    assertThat(coordinator.isInProgress(DB_NAME)).as("the recovery released the slot").isFalse();
    assertSwapCompleted(dbDir);
  }

  /**
   * The other direction: a backup already running when the recovery starts. The recovery waits for it rather than
   * moving the files out from under it, and proceeds the moment it is released.
   */
  @Test
  @Timeout(120)
  void recoveryWaitsForABackupThatIsAlreadyRunning(@TempDir final Path root) throws Exception {
    final Path databasesDir = startServer(root, 60_000);
    final Path dbDir = stageInterruptedSwap(databasesDir, DB_NAME);

    final BackupCoordinator coordinator = server.getBackupCoordinator();
    assertThat(coordinator.begin(DB_NAME, BackupCoordinator.Operation.BACKUP)).isNull();

    final AtomicBoolean recoveryDone = new AtomicBoolean(false);
    final Thread recovery = startRecovery(databasesDir, recoveryDone, new AtomicReference<>());

    recovery.join(2_000);
    assertThat(recoveryDone.get()).as("the recovery waited instead of moving files under a running backup").isFalse();
    assertThat(dbDir.resolve(SnapshotInstaller.SNAPSHOT_PENDING_FILE))
        .as("nothing was reconciled while the backup held the slot").exists();

    coordinator.end(DB_NAME, BackupCoordinator.Operation.BACKUP);

    recovery.join(60_000);
    assertThat(recoveryDone.get()).as("the recovery proceeded once the backup released the slot").isTrue();
    assertSwapCompleted(dbDir);
  }

  /**
   * The wait is bounded, and that is not a detail: a directory left half-swapped is worse than a backup that reads
   * a torn one, and the database stays deferred by {@code ArcadeDBServer.loadDatabases} until the marker clears. So
   * a backup that never ends must not leave the directory unrepaired forever; it only makes the repair loud.
   */
  @Test
  @Timeout(120)
  void aBackupThatNeverEndsDoesNotBlockRecoveryForever(@TempDir final Path root) throws Exception {
    final Path databasesDir = startServer(root, 500);
    final Path dbDir = stageInterruptedSwap(databasesDir, DB_NAME);

    final BackupCoordinator coordinator = server.getBackupCoordinator();
    assertThat(coordinator.begin(DB_NAME, BackupCoordinator.Operation.BACKUP)).isNull();
    try {
      SnapshotInstaller.recoverPendingSnapshotSwaps(databasesDir, server);

      assertSwapCompleted(dbDir);
      // The recovery never took the slot, so it must not have released the backup's reservation either.
      assertThat(coordinator.isInProgress(DB_NAME, BackupCoordinator.Operation.BACKUP))
          .as("a timed-out recovery leaves the reservation it never took alone").isTrue();
    } finally {
      coordinator.end(DB_NAME, BackupCoordinator.Operation.BACKUP);
    }
  }

  /**
   * The reservation is per database, not one for the whole pass. A backup of an unrelated database, and an
   * interrupted {@code .acquire-*} acquisition staging directory - which is reserved, never registered, and
   * therefore has no backup to exclude - must neither delay the repair nor be delayed by it.
   * <p>
   * The configured wait is ten minutes while the assertion allows one, so a pass that serialised on the unrelated
   * backup could not slip through this: it would still be parked when the latch expired.
   */
  @Test
  @Timeout(180)
  void theSlotIsPerDatabaseRatherThanOneForTheWholePass(@TempDir final Path root) throws Exception {
    final Path databasesDir = startServer(root, 600_000);
    final Path dbDir = stageInterruptedSwap(databasesDir, DB_NAME);

    final Path staging = databasesDir.resolve(SnapshotInstaller.ACQUIRE_STAGING_PREFIX + "ghost7449");
    Files.createDirectories(staging);
    Files.writeString(staging.resolve("partial.dat"), "interrupted acquisition");

    final BackupCoordinator coordinator = server.getBackupCoordinator();
    assertThat(coordinator.begin(OTHER_DB, BackupCoordinator.Operation.BACKUP)).isNull();
    try {
      final AtomicBoolean recoveryDone = new AtomicBoolean(false);
      final AtomicReference<Throwable> failure = new AtomicReference<>();
      final Thread recovery = startRecovery(databasesDir, recoveryDone, failure);

      recovery.join(60_000);
      assertThat(failure.get()).isNull();
      assertThat(recoveryDone.get())
          .as("a backup of a different database did not hold up the repair of this one").isTrue();

      assertSwapCompleted(dbDir);
      assertThat(staging).as("the reserved acquisition staging dir was cleaned up in the same pass").doesNotExist();
    } finally {
      coordinator.end(OTHER_DB, BackupCoordinator.Operation.BACKUP);
    }
  }

  /**
   * The pass still runs without a coordinator to consult. The one-argument overload is what the existing recovery
   * tests drive and what an embedded caller with no server has, and it must behave exactly as before.
   */
  @Test
  @Timeout(120)
  void recoveryWithoutAServerStillReconcilesTheDirectory(@TempDir final Path databasesDir) throws Exception {
    final Path dbDir = stageInterruptedSwap(databasesDir, DB_NAME);

    SnapshotInstaller.recoverPendingSnapshotSwaps(databasesDir);

    assertSwapCompleted(dbDir);
  }

  private Thread startRecovery(final Path databasesDir, final AtomicBoolean done, final AtomicReference<Throwable> failure) {
    final Thread thread = new Thread(() -> {
      try {
        SnapshotInstaller.recoverPendingSnapshotSwaps(databasesDir, server);
        done.set(true);
      } catch (final Throwable t) {
        failure.set(t);
      }
    }, "snapshot-swap-recovery");
    thread.start();
    return thread;
  }

  /**
   * Stages the state a crash between the download and the cleanup leaves behind: the snapshot is complete and the
   * previous copy is retained as {@code .snapshot-backup}, so recovery completes the swap rather than rolling it
   * back. Observable afterwards as a content change, not only as the absence of an exception.
   */
  private static Path stageInterruptedSwap(final Path databasesDir, final String databaseName) throws IOException {
    final Path dbDir = databasesDir.resolve(databaseName);
    final Path snapshotNew = dbDir.resolve(SnapshotInstaller.SNAPSHOT_NEW_DIR);
    final Path snapshotBackup = dbDir.resolve(SnapshotInstaller.SNAPSHOT_BACKUP_DIR);

    Files.createDirectories(snapshotNew);
    Files.createDirectories(snapshotBackup);
    Files.writeString(dbDir.resolve(SnapshotInstaller.SNAPSHOT_PENDING_FILE), "");
    Files.writeString(snapshotNew.resolve(SnapshotInstaller.SNAPSHOT_COMPLETE_FILE), "");
    Files.writeString(snapshotNew.resolve("data.dat"), NEW_CONTENT);
    Files.writeString(snapshotBackup.resolve("data.dat"), OLD_CONTENT);
    return dbDir;
  }

  private static void assertSwapCompleted(final Path dbDir) throws IOException {
    assertThat(Files.readString(dbDir.resolve("data.dat")))
        .as("the interrupted swap was completed from the downloaded snapshot").isEqualTo(NEW_CONTENT);
    assertThat(dbDir.resolve(SnapshotInstaller.SNAPSHOT_BACKUP_DIR)).doesNotExist();
    assertThat(dbDir.resolve(SnapshotInstaller.SNAPSHOT_PENDING_FILE)).doesNotExist();
  }

  private Path startServer(final Path root, final long backupWaitMs) throws IOException {
    final Path databasesDir = root.resolve("databases");
    Files.createDirectories(databasesDir);

    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_NAME, "ArcadeDB_7449");
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, databasesDir.toString());
    config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, root.toString());
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, PASSWORD);
    config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_HOST, "localhost");
    config.setValue(GlobalConfiguration.HA_ENABLED, false);
    config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_BACKUP_WAIT_MS, backupWaitMs);

    server = new ArcadeDBServer(config);
    server.start();
    return databasesDir;
  }
}
