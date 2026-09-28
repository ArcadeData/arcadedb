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
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.engine.MaintenanceCoordinator.Operation;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.backup.BackupCoordinator;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #8035: the replicated drop-database apply must take this node's per-database
 * maintenance slot before it closes and removes the database, the same bounded way the replicated restore
 * ({@code SnapshotInstaller.install}) does, so a backup, export or import running on a peer is waited out instead of
 * being torn down - and must NOT wait on the slot the local {@code drop database} verb already holds for it.
 * <p>
 * Drives a real (unstarted) {@link ArcadeDBServer} and a real {@link LocalDatabase}, like
 * {@code ArcadeStateMachineDeferredDropTest}.
 */
class Issue8035DropApplyTakesMaintenanceSlotTest {

  private static final String DB_NAME = "db8035";
  /** Long enough that an apply which waits for it is unmistakable against the hang detectors below. */
  private static final long   LONG_WAIT_MS = TimeUnit.MINUTES.toMillis(10);
  private static final long   HANG_DETECTOR_SECONDS = 60;

  @TempDir
  private Path serverDir;

  private ArcadeDBServer          server;
  private LocalDatabase           localDatabase;
  private Path                    databaseDirectory;
  private ExecutorService         applyThread;
  private DeferredDatabaseDeleter deleter;

  private ArcadeStateMachine startWithBackupWaitOf(final long waitMs) {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, serverDir.toString());
    config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_BACKUP_WAIT_MS, waitMs);
    server = new ArcadeDBServer(config);

    databaseDirectory = serverDir.resolve(DB_NAME);
    localDatabase = (LocalDatabase) new DatabaseFactory(databaseDirectory.toString()).create();
    localDatabase.transaction(() -> localDatabase.getSchema().createDocumentType("Doc"));
    server.registerDatabase(DB_NAME, localDatabase);

    applyThread = Executors.newSingleThreadExecutor();
    deleter = new DeferredDatabaseDeleter();

    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.setServer(server);
    sm.setDeferredDatabaseDeleter(deleter);
    return sm;
  }

  @AfterEach
  void tearDown() {
    if (applyThread != null)
      applyThread.shutdownNow();
    if (deleter != null)
      deleter.close();
    if (localDatabase != null && localDatabase.isOpen())
      localDatabase.close();
    if (databaseDirectory != null)
      FileUtils.deleteRecursively(new File(databaseDirectory.toString()));
  }

  private static void applyDrop(final ArcadeStateMachine sm) {
    sm.applyDropDatabaseEntry(RaftLogEntryCodec.decode(RaftLogEntryCodec.encodeDropDatabaseEntry(DB_NAME)));
  }

  private Future<?> applyDropOnApplyThread(final ArcadeStateMachine sm) {
    return applyThread.submit(() -> applyDrop(sm));
  }

  private static void assertStillWaiting(final Future<?> apply) {
    // A short wait expected to TIME OUT: the apply must still be parked on the slot.
    assertThatThrownBy(() -> apply.get(500, TimeUnit.MILLISECONDS)).isInstanceOf(TimeoutException.class);
  }

  private BackupCoordinator coordinator() {
    return server.getBackupCoordinator();
  }

  /** The issue's scenario: a backup running on a peer is waited out, and the drop lands the moment it finishes. */
  @Test
  void aPeerApplyWaitsForARunningBackupBeforeDroppingTheDatabase() throws Exception {
    final ArcadeStateMachine sm = startWithBackupWaitOf(LONG_WAIT_MS);
    assertThat(coordinator().begin(DB_NAME)).isTrue();

    final Future<?> apply = applyDropOnApplyThread(sm);
    assertStillWaiting(apply);
    assertThat(server.existsDatabase(DB_NAME)).as("the backup's database must not be dropped under it").isTrue();
    assertThat(localDatabase.isOpen()).isTrue();

    coordinator().end(DB_NAME);
    apply.get(HANG_DETECTOR_SECONDS, TimeUnit.SECONDS);

    assertThat(server.existsDatabase(DB_NAME)).isFalse();
    assertThat(localDatabase.isOpen()).isFalse();
    assertThat(coordinator().isInProgress(DB_NAME)).as("the apply must release the DROP slot it took").isFalse();
  }

  /** An export and an import work through the instance just as a backup does. */
  @Test
  void aPeerApplyWaitsForARunningExportOrImportToo() throws Exception {
    final ArcadeStateMachine sm = startWithBackupWaitOf(LONG_WAIT_MS);
    assertThat(coordinator().begin(DB_NAME, Operation.EXPORT)).isNull();
    assertThat(coordinator().begin(DB_NAME, Operation.IMPORT)).isNull();

    final Future<?> apply = applyDropOnApplyThread(sm);
    assertStillWaiting(apply);

    coordinator().end(DB_NAME, Operation.EXPORT);
    assertStillWaiting(apply);
    assertThat(server.existsDatabase(DB_NAME)).isTrue();

    coordinator().end(DB_NAME, Operation.IMPORT);
    apply.get(HANG_DETECTOR_SECONDS, TimeUnit.SECONDS);
    assertThat(server.existsDatabase(DB_NAME)).isFalse();
  }

  /**
   * The wait is bounded and proceeds on expiry: a committed entry cannot be declined. The expired wait took nothing,
   * so it must not release the reservation the backup is still holding.
   */
  @Test
  void anExpiredWaitStillDropsAndLeavesTheBackupsReservationAlone() {
    final ArcadeStateMachine sm = startWithBackupWaitOf(200L);
    assertThat(coordinator().begin(DB_NAME)).isTrue();

    applyDrop(sm);

    assertThat(server.existsDatabase(DB_NAME)).isFalse();
    assertThat(coordinator().isInProgress(DB_NAME, Operation.BACKUP)).isTrue();
    coordinator().end(DB_NAME);
  }

  /** With nothing running, the apply takes the slot and gives it back. */
  @Test
  void anUncontendedApplyReleasesTheSlotItTook() {
    final ArcadeStateMachine sm = startWithBackupWaitOf(LONG_WAIT_MS);

    applyDrop(sm);

    assertThat(server.existsDatabase(DB_NAME)).isFalse();
    assertThat(coordinator().isInProgress(DB_NAME)).isFalse();
  }

  /**
   * On the node that issued the verb, {@code dropInReplicas} waits for this apply while its request thread holds
   * DROP. The apply must not wait on that slot - it would wait on itself - and must leave it to the verb to release.
   */
  @Test
  void theIssuingNodesApplyDoesNotWaitOnTheSlotItsOwnVerbHolds() throws Exception {
    assertTheLocalVerbsSlotCoversTheApply(Operation.DROP);
  }

  /** The drop a {@code restore database} performs on the database it replaces holds RESTORE, not DROP. */
  @Test
  void theIssuingNodesApplyDoesNotWaitOnTheSlotItsOwnRestoreHolds() throws Exception {
    assertTheLocalVerbsSlotCoversTheApply(Operation.RESTORE);
  }

  private void assertTheLocalVerbsSlotCoversTheApply(final Operation verbsSlot) throws Exception {
    final ArcadeStateMachine sm = startWithBackupWaitOf(LONG_WAIT_MS);
    final LocalDropVerbs verbs = new LocalDropVerbs();
    sm.setLocalDropVerbs(verbs);

    assertThat(coordinator().begin(DB_NAME, verbsSlot)).isNull();
    final LocalDropVerbs.Registration registration = verbs.register(DB_NAME);
    try {
      applyDropOnApplyThread(sm).get(HANG_DETECTOR_SECONDS, TimeUnit.SECONDS);

      assertThat(server.existsDatabase(DB_NAME)).isFalse();
      assertThat(coordinator().isInProgress(DB_NAME, verbsSlot)).as("the verb's slot is the verb's to release").isTrue();
    } finally {
      verbs.release(DB_NAME, registration);
      coordinator().end(DB_NAME, verbsSlot);
    }
  }

  /**
   * A registration is only trusted while an exclusive operation actually holds the slot: without one a backup can be
   * running, and the apply must reserve the slot like any peer's.
   */
  @Test
  void aRegistrationWithoutAnExclusiveSlotStillWaitsForTheBackup() throws Exception {
    final ArcadeStateMachine sm = startWithBackupWaitOf(LONG_WAIT_MS);
    final LocalDropVerbs verbs = new LocalDropVerbs();
    sm.setLocalDropVerbs(verbs);
    final LocalDropVerbs.Registration registration = verbs.register(DB_NAME);
    assertThat(coordinator().begin(DB_NAME)).isTrue();

    try {
      final Future<?> apply = applyDropOnApplyThread(sm);
      assertStillWaiting(apply);
      assertThat(server.existsDatabase(DB_NAME)).isTrue();

      coordinator().end(DB_NAME);
      apply.get(HANG_DETECTOR_SECONDS, TimeUnit.SECONDS);
      assertThat(server.existsDatabase(DB_NAME)).isFalse();
    } finally {
      verbs.release(DB_NAME, registration);
    }
  }

  /**
   * A verb whose wait timed out withdraws its registration and then releases its slot. It must not be able to do so
   * while the apply is still dropping under that registration, or a backup could be admitted mid-drop.
   */
  @Test
  void aVerbCannotWithdrawItsRegistrationWhileTheApplyRunsUnderIt() throws Exception {
    final LocalDropVerbs verbs = new LocalDropVerbs();
    final LocalDropVerbs.Registration registration = verbs.register(DB_NAME);
    final CountDownLatch sectionEntered = new CountDownLatch(1);
    final CountDownLatch finishSection = new CountDownLatch(1);
    final ExecutorService threads = Executors.newFixedThreadPool(2);
    try {
      final Future<Boolean> apply = threads.submit(() -> verbs.runUnderLocalVerb(DB_NAME, () -> {
        sectionEntered.countDown();
        try {
          finishSection.await(HANG_DETECTOR_SECONDS, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }));
      assertThat(sectionEntered.await(HANG_DETECTOR_SECONDS, TimeUnit.SECONDS)).isTrue();

      final Future<?> withdraw = threads.submit(() -> verbs.release(DB_NAME, registration));
      assertThatThrownBy(() -> withdraw.get(500, TimeUnit.MILLISECONDS)).isInstanceOf(TimeoutException.class);

      finishSection.countDown();
      withdraw.get(HANG_DETECTOR_SECONDS, TimeUnit.SECONDS);
      assertThat(apply.get(HANG_DETECTOR_SECONDS, TimeUnit.SECONDS)).isTrue();
      assertThat(verbs.isAwaiting(DB_NAME)).isFalse();
      assertThat(verbs.runUnderLocalVerb(DB_NAME, () -> {
      })).as("a withdrawn registration covers nothing").isFalse();
    } finally {
      threads.shutdownNow();
    }
  }
}
