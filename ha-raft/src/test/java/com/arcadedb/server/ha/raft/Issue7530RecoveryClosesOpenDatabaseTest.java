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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerDatabase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Comparator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #7530, a follow-up to #7449.
 * <p>
 * #7449 gave the snapshot-swap recovery pass this node's per-database maintenance slot, which excludes a
 * <i>backup</i> from a directory the pass is repairing. It excluded nothing else. The pass moves files into and
 * out of the live database directory - {@code atomicSwap}, {@code restoreBackup}, {@code clearLiveDatabaseFiles} -
 * and did so without closing the database, without {@link ArcadeDBServer#getDatabasesLock()} and without the
 * {@code setSnapshotInstallInProgress} 503 window, all three of which {@code reconcileRetainedBackup} already took
 * around the very same {@code recoverSingleDatabase} call.
 * <p>
 * "Open" is a reachable state, not a hypothetical one: {@code swapAndReopen}'s two failure arms deliberately leave
 * the {@code .snapshot-pending} marker in place <i>and</i> reopen the database, and
 * {@code ArcadeDBServer.getDatabase} serves an already-registered open database from its lock-free fast path
 * without ever consulting that marker. The pass then runs again on the next {@code RaftHAServer.restartRatis},
 * with the node ONLINE.
 * <p>
 * These tests drive the real {@code recoverPendingSnapshotSwaps(Path, ArcadeDBServer)} against a real server and a
 * real database, and pin each half of the invariant separately: closed and deregistered while the files move,
 * reopened afterwards, the registry lock held throughout, the 503 window open throughout - and, for the cold-start
 * case, that a database the boot scan deliberately deferred is <b>not</b> registered by the repair.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue7530RecoveryClosesOpenDatabaseTest {

  private static final String DB_NAME       = "recov7530";
  private static final String DEFERRED_DB   = "deferred7530";
  private static final String SYNTHETIC_DB  = "synthetic7530";
  private static final String PASSWORD      = "DefaultPasswordForTests";
  private static final String ORIGINAL_TYPE = "OriginalContent7530";
  private static final String SNAPSHOT_TYPE = "SnapshotContent7530";

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
   * The bug as reported. A registered, open database carrying the pending marker - exactly what a failed install
   * leaves behind - is repaired by the pass. While {@code recoverSingleDatabase} is moving its files it must not
   * still be registered and open, and once the repair is done it must be back, serving the installed snapshot.
   * <p>
   * Without the fix the barrier observes the database still registered: the files are renamed under open handles
   * and under any reader that got there through {@code getDatabase}'s fast path.
   */
  @Test
  @Timeout(180)
  void theRepairClosesAndDeregistersARegisteredOpenDatabaseAndReopensItAfterwards(@TempDir final Path root)
      throws Exception {
    final Path databasesDir = startServer(root);
    final Path dbDir = createRegisteredDatabaseWithPendingSwap(root, databasesDir);

    final AtomicBoolean registeredDuringRepair = new AtomicBoolean(true);
    SnapshotInstaller.recoveryBarrierForTesting = () -> registeredDuringRepair.set(server.existsDatabase(DB_NAME));

    SnapshotInstaller.recoverPendingSnapshotSwaps(databasesDir, server);

    assertThat(registeredDuringRepair.get())
        .as("the database was closed and deregistered before its files were moved").isFalse();

    assertThat(dbDir.resolve(SnapshotInstaller.SNAPSHOT_PENDING_FILE)).doesNotExist();
    assertThat(server.existsDatabase(DB_NAME)).as("the repair reopened the database it closed").isTrue();

    final ServerDatabase reopened = server.getDatabase(DB_NAME);
    assertThat(reopened.isOpen()).isTrue();
    assertThat(reopened.getSchema().existsType(SNAPSHOT_TYPE))
        .as("the reopened database is the installed snapshot").isTrue();
    assertThat(reopened.getSchema().existsType(ORIGINAL_TYPE))
        .as("the previous copy was swapped out").isFalse();
  }

  /**
   * The registry lock is what makes the swap window invisible to the engine-internal open paths, which never
   * consult the 503 flag. Pinned directly: while the repair is paused mid-file-move, no other thread can enter a
   * {@code getDatabasesLock()} section - and the moment the repair is released, it can.
   */
  @Test
  @Timeout(180)
  void theRepairHoldsTheRegistryLockWhileItMovesFiles(@TempDir final Path root) throws Exception {
    final Path databasesDir = startServer(root);
    createRegisteredDatabaseWithPendingSwap(root, databasesDir);

    final CountDownLatch insideRepair = new CountDownLatch(1);
    final CountDownLatch releaseRepair = new CountDownLatch(1);
    SnapshotInstaller.recoveryBarrierForTesting = () -> {
      insideRepair.countDown();
      awaitQuietly(releaseRepair);
    };

    final CountDownLatch probeEnteredLock = new CountDownLatch(1);
    final Thread probe = new Thread(() -> {
      synchronized (server.getDatabasesLock()) {
        probeEnteredLock.countDown();
      }
    }, "registry-lock-probe-7530");

    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final Thread repair = startRecovery(databasesDir, new AtomicBoolean(), failure);
    try {
      assertThat(insideRepair.await(60, TimeUnit.SECONDS)).as("the repair reached the in-repair barrier").isTrue();

      probe.start();
      assertThat(probeEnteredLock.await(2, TimeUnit.SECONDS))
          .as("a concurrent registry operation is blocked while the repair moves files").isFalse();
    } finally {
      releaseRepair.countDown();
    }

    assertThat(probeEnteredLock.await(60, TimeUnit.SECONDS))
        .as("the registry lock is released once the repair finishes").isTrue();
    probe.join(60_000);
    repair.join(60_000);
    assertThat(failure.get()).isNull();
  }

  /**
   * The 503 window. HTTP handlers consult {@code isSnapshotInstallInProgress} at request entry, so it must be open
   * for the whole repair and closed again afterwards - a leaked window would deflect every request on the node
   * until it restarts.
   */
  @Test
  @Timeout(180)
  void theRepairDeflectsHttpClientsWhileItMovesFiles(@TempDir final Path root) throws Exception {
    final Path databasesDir = startServer(root);
    createRegisteredDatabaseWithPendingSwap(root, databasesDir);

    final AtomicBoolean windowOpenDuringRepair = new AtomicBoolean(false);
    SnapshotInstaller.recoveryBarrierForTesting =
        () -> windowOpenDuringRepair.set(server.isSnapshotInstallInProgress());

    SnapshotInstaller.recoverPendingSnapshotSwaps(databasesDir, server);

    assertThat(windowOpenDuringRepair.get())
        .as("HTTP clients are deflected with a 503 while the files are being moved").isTrue();
    assertThat(server.isSnapshotInstallInProgress())
        .as("the window is closed again once the repair finishes").isFalse();
  }

  /**
   * The cold-start half of the invariant, and the one that has to be got right rather than copied from
   * {@code reconcileRetainedBackup}: at startup {@code loadDatabases(false)} <b>defers</b> a directory carrying the
   * marker, and the second {@code loadDatabases(true)} pass is what is supposed to pick it up once the marker is
   * gone. A repair that reopened unconditionally would register it from inside Ratis's state-machine
   * initialization instead.
   */
  @Test
  @Timeout(180)
  void aDatabaseTheBootScanDeferredIsNotRegisteredByTheRepair(@TempDir final Path root) throws Exception {
    final Path databasesDir = startServer(root);

    // A loadable database on disk that this server has never registered: it appears after startup, so the boot
    // scan never saw it. Standing in for the directory loadDatabases(false) deferred because of the marker.
    final Path dbDir = databasesDir.resolve(DEFERRED_DB);
    createStandaloneDatabase(dbDir, ORIGINAL_TYPE);
    assertThat(server.existsDatabase(DEFERRED_DB)).as("precondition: not registered").isFalse();

    // .snapshot-new with its completion marker and no backup: the "swap already completed, cleanup did not"
    // branch, which leaves the live files - a real, openable database - untouched.
    final Path snapshotNew = dbDir.resolve(SnapshotInstaller.SNAPSHOT_NEW_DIR);
    Files.createDirectories(snapshotNew);
    Files.writeString(snapshotNew.resolve(SnapshotInstaller.SNAPSHOT_COMPLETE_FILE), "");
    Files.writeString(dbDir.resolve(SnapshotInstaller.SNAPSHOT_PENDING_FILE), "");

    SnapshotInstaller.recoverPendingSnapshotSwaps(databasesDir, server);

    assertThat(dbDir.resolve(SnapshotInstaller.SNAPSHOT_PENDING_FILE))
        .as("the repair still reconciled the directory").doesNotExist();
    assertThat(snapshotNew).doesNotExist();
    assertThat(server.existsDatabase(DEFERRED_DB))
        .as("the repair did not register a database the boot scan deferred").isFalse();
  }

  /**
   * The 503 flag is node-wide while the repair takes it per database, so the pass opens and closes it once per
   * repaired directory. An {@code install} of a <i>different</i> database running alongside holds the same flag,
   * and the pass must not clear the window out from under it: that would serve HTTP clients from a directory
   * mid-swap, which is precisely what the flag exists to prevent.
   */
  @Test
  @Timeout(180)
  void theInstallWindowSurvivesAPassThatOpensAndClosesItAlongside(@TempDir final Path root) throws Exception {
    final Path databasesDir = startServer(root);
    final Path dbDir = stageSyntheticInterruptedSwap(databasesDir);

    // Stands in for SnapshotInstaller.install() of another database holding the node-wide window open.
    server.setSnapshotInstallInProgress(true);
    try {
      SnapshotInstaller.recoverPendingSnapshotSwaps(databasesDir, server);

      assertThat(dbDir.resolve(SnapshotInstaller.SNAPSHOT_PENDING_FILE)).doesNotExist();
      assertThat(server.isSnapshotInstallInProgress())
          .as("the pass did not clear the window a concurrent install is holding").isTrue();
    } finally {
      server.setSnapshotInstallInProgress(false);
    }

    assertThat(server.isSnapshotInstallInProgress())
        .as("the window closes when its own holder releases it").isFalse();
  }

  /**
   * A database that is registered but already <b>closed</b> while its marker is on disk cannot be resolved at all:
   * {@code getDatabase} sends a closed entry down its locked path, where the {@code .snapshot-pending} refusal
   * lives, and throws. That must not abandon the pass - every marker it had not reached yet would stay on disk,
   * leaving those databases unopenable until the next restart, which is worse than the state being repaired.
   * <p>
   * It is also the one case where the repair proceeds without having closed anything, and it is safe precisely
   * because the entry that failed to resolve is by definition not open, so it holds no files in the directory.
   */
  @Test
  @Timeout(180)
  void aRegisteredButUnresolvableDatabaseDoesNotAbandonTheWholePass(@TempDir final Path root) throws Exception {
    final Path databasesDir = startServer(root);

    // Registered and then closed underneath the registry, without deregistering: exactly the shape getDatabase
    // refuses once the marker is on disk.
    final ServerDatabase live = server.createDatabase(DB_NAME, ComponentFile.MODE.READ_WRITE);
    live.getSchema().createDocumentType(ORIGINAL_TYPE);
    ((DatabaseInternal) server.getDatabase(DB_NAME)).getEmbedded().close();
    assertThat(server.existsDatabase(DB_NAME)).as("precondition: still registered").isTrue();

    final Path dbDir = databasesDir.resolve(DB_NAME);
    final Path snapshotNew = dbDir.resolve(SnapshotInstaller.SNAPSHOT_NEW_DIR);
    Files.createDirectories(snapshotNew);
    Files.writeString(snapshotNew.resolve(SnapshotInstaller.SNAPSHOT_COMPLETE_FILE), "");
    Files.writeString(dbDir.resolve(SnapshotInstaller.SNAPSHOT_PENDING_FILE), "");

    // A second marked directory, reached only if the first one did not abandon the scan.
    final Path secondDir = stageSyntheticInterruptedSwap(databasesDir);

    SnapshotInstaller.recoverPendingSnapshotSwaps(databasesDir, server);

    assertThat(dbDir.resolve(SnapshotInstaller.SNAPSHOT_PENDING_FILE))
        .as("the unresolvable database was still repaired").doesNotExist();
    assertThat(secondDir.resolve(SnapshotInstaller.SNAPSHOT_PENDING_FILE))
        .as("the pass went on to the next marked directory instead of abandoning the scan").doesNotExist();
  }

  /**
   * Closing the database first put an <b>unchecked</b> failure on this path for the first time.
   * {@code recoverSingleDatabase} catches its own {@code IOException}s and every file-moving helper it reaches
   * declares only {@code IOException}, so before this change the scan loop could not be ended by one bad
   * directory. {@code LocalDatabase.close()} declares no checked exception, and a close that throws is most likely
   * in exactly the disk-pressure conditions that leave a marker behind - so an unguarded repair would abandon
   * every other pending marker in the pass and then escape into {@code ArcadeStateMachine.initialize()}, which
   * does not catch it either, failing the Ratis start for the whole node.
   * <p>
   * Driven through the repair barrier rather than through a failing {@code close()}, because that is the seam
   * available without mocking a database: it throws from inside {@code recoverSingleDatabase}, which unwinds
   * through the same registry lock, reopen, 503 window and maintenance slot that a failing close does. What is
   * pinned is the guard, and the guard is the same code for either origin.
   */
  @Test
  @Timeout(180)
  void anUncheckedFailureRepairingOneDatabaseDoesNotEndTheScan(@TempDir final Path root) throws Exception {
    final Path databasesDir = startServer(root);

    // Two marked directories. The one that blows up is named so it sorts first is not something the directory
    // stream guarantees, so the barrier fails only the first database it is called for, whichever that is.
    final Path first = stageSyntheticInterruptedSwap(databasesDir);
    final Path second = databasesDir.resolve(DEFERRED_DB);
    Files.createDirectories(second.resolve(SnapshotInstaller.SNAPSHOT_NEW_DIR));
    Files.writeString(second.resolve(SnapshotInstaller.SNAPSHOT_NEW_DIR).resolve(SnapshotInstaller.SNAPSHOT_COMPLETE_FILE), "");
    Files.writeString(second.resolve(SnapshotInstaller.SNAPSHOT_PENDING_FILE), "");
    Files.writeString(second.resolve("data.dat"), "old-data");

    final AtomicBoolean alreadyFailed = new AtomicBoolean(false);
    SnapshotInstaller.recoveryBarrierForTesting = () -> {
      if (alreadyFailed.compareAndSet(false, true))
        throw new IllegalStateException("simulated unchecked failure while repairing this database");
    };

    // The pass itself must not throw: the exception must not reach ArcadeStateMachine.initialize().
    SnapshotInstaller.recoverPendingSnapshotSwaps(databasesDir, server);

    assertThat(alreadyFailed.get()).as("the barrier fired, so one repair really did blow up").isTrue();

    final boolean firstStillMarked = Files.exists(first.resolve(SnapshotInstaller.SNAPSHOT_PENDING_FILE));
    final boolean secondStillMarked = Files.exists(second.resolve(SnapshotInstaller.SNAPSHOT_PENDING_FILE));
    assertThat(firstStillMarked ^ secondStillMarked)
        .as("exactly one database kept its marker: the one that failed, not the one after it").isTrue();

    // Neither the node-wide window nor the maintenance slot may be leaked by the failing repair.
    assertThat(server.isSnapshotInstallInProgress())
        .as("the 503 window was released even though the repair threw").isFalse();
    assertThat(server.getBackupCoordinator().isInProgress(SYNTHETIC_DB))
        .as("the maintenance slot was released even though the repair threw").isFalse();
    assertThat(server.getBackupCoordinator().isInProgress(DEFERRED_DB))
        .as("the maintenance slot was released even though the repair threw").isFalse();
  }

  // ---------------------------------------------------------------------------------------------------------------

  private Thread startRecovery(final Path databasesDir, final AtomicBoolean done,
      final AtomicReference<Throwable> failure) {
    final Thread thread = new Thread(() -> {
      try {
        SnapshotInstaller.recoverPendingSnapshotSwaps(databasesDir, server);
        done.set(true);
      } catch (final Throwable t) {
        failure.set(t);
      }
    }, "snapshot-swap-recovery-7530");
    thread.start();
    return thread;
  }

  private static void awaitQuietly(final CountDownLatch latch) {
    try {
      latch.await();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Builds the state the issue describes: a database registered and open on the server, its {@code .snapshot-pending}
   * marker still on disk, and a complete staged snapshot plus a retained backup directory - so the repair takes the
   * "complete the interrupted swap" branch and genuinely moves the live files.
   */
  private Path createRegisteredDatabaseWithPendingSwap(final Path root, final Path databasesDir) throws IOException {
    final ServerDatabase live = server.createDatabase(DB_NAME, ComponentFile.MODE.READ_WRITE);
    live.getSchema().createDocumentType(ORIGINAL_TYPE);
    assertThat(server.existsDatabase(DB_NAME)).isTrue();

    final Path dbDir = databasesDir.resolve(DB_NAME);

    // The snapshot to be installed, built as a real database somewhere the server does not scan, closed cleanly,
    // then staged under .snapshot-new. Its schema is what proves the swap actually happened.
    final Path source = root.resolve("leader-snapshot");
    createStandaloneDatabase(source, SNAPSHOT_TYPE);

    final Path snapshotNew = dbDir.resolve(SnapshotInstaller.SNAPSHOT_NEW_DIR);
    copyDirectory(source, snapshotNew);
    Files.writeString(snapshotNew.resolve(SnapshotInstaller.SNAPSHOT_COMPLETE_FILE), "");
    Files.createDirectories(dbDir.resolve(SnapshotInstaller.SNAPSHOT_BACKUP_DIR));
    Files.writeString(dbDir.resolve(SnapshotInstaller.SNAPSHOT_PENDING_FILE), "");
    return dbDir;
  }

  /**
   * A pending swap over synthetic files, for the assertions that are about the node-wide flag rather than about a
   * database: no server registration is involved, so nothing has to be openable.
   */
  private static Path stageSyntheticInterruptedSwap(final Path databasesDir) throws IOException {
    final Path dbDir = databasesDir.resolve(SYNTHETIC_DB);
    final Path snapshotNew = dbDir.resolve(SnapshotInstaller.SNAPSHOT_NEW_DIR);
    final Path snapshotBackup = dbDir.resolve(SnapshotInstaller.SNAPSHOT_BACKUP_DIR);

    Files.createDirectories(snapshotNew);
    Files.createDirectories(snapshotBackup);
    Files.writeString(dbDir.resolve(SnapshotInstaller.SNAPSHOT_PENDING_FILE), "");
    Files.writeString(snapshotNew.resolve(SnapshotInstaller.SNAPSHOT_COMPLETE_FILE), "");
    Files.writeString(snapshotNew.resolve("data.dat"), "new-snapshot-data");
    Files.writeString(snapshotBackup.resolve("data.dat"), "old-data");
    return dbDir;
  }

  private static void createStandaloneDatabase(final Path path, final String typeName) {
    try (final DatabaseFactory factory = new DatabaseFactory(path.toString())) {
      try (final Database db = factory.create()) {
        db.getSchema().createDocumentType(typeName);
      }
    }
  }

  private static void copyDirectory(final Path from, final Path to) throws IOException {
    Files.createDirectories(to);
    try (final Stream<Path> entries = Files.walk(from)) {
      for (final Path entry : entries.sorted(Comparator.naturalOrder()).toList()) {
        final Path target = to.resolve(from.relativize(entry).toString());
        if (Files.isDirectory(entry))
          Files.createDirectories(target);
        else
          Files.copy(entry, target, StandardCopyOption.REPLACE_EXISTING);
      }
    }
  }

  private Path startServer(final Path root) throws IOException {
    final Path databasesDir = root.resolve("databases");
    Files.createDirectories(databasesDir);

    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_NAME, "ArcadeDB_7530");
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, databasesDir.toString());
    config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, root.toString());
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, PASSWORD);
    config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_HOST, "localhost");
    config.setValue(GlobalConfiguration.HA_ENABLED, false);
    config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_BACKUP_WAIT_MS, 60_000);

    server = new ArcadeDBServer(config);
    server.start();
    return databasesDir;
  }
}
