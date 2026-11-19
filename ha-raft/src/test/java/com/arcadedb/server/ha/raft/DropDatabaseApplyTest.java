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
import com.arcadedb.exception.TransactionException;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.CallLog;
import com.arcadedb.server.ServerControlPlane;
import com.arcadedb.server.TestServerHelper;
import com.arcadedb.server.backup.BackupCoordinator;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.arcadedb.utility.SubclassMocks.mock;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.Mockito.when;

/**
 * The replicated drop-database apply: the maintenance slot it takes, a closed database, the wait for the local apply and the WAL of cancelled files.
 */
class DropDatabaseApplyTest {

  /**
   * Regression tests for issue #8035: the replicated drop-database apply must take this node's per-database
   * maintenance slot before it closes and removes the database, the same bounded way the replicated restore
   * ({@code SnapshotInstaller.install}) does, so a backup, export or import running on a peer is waited out instead of
   * being torn down - and must NOT wait on the slot the local {@code drop database} verb already holds for it.
   * <p>
   * Drives a real (unstarted) {@link ArcadeDBServer} and a real {@link LocalDatabase}, like
   * {@code ArcadeStateMachineDeferredDropTest}.
   */
  @Nested
  class DropApplyTakesMaintenanceSlot {

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
    // Regression for #8035: DropApplyTakesMaintenanceSlot
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
    // Regression for #8035: DropApplyTakesMaintenanceSlot
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
    // Regression for #8035: DropApplyTakesMaintenanceSlot
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
    // Regression for #8035: DropApplyTakesMaintenanceSlot
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
    // Regression for #8035: DropApplyTakesMaintenanceSlot
    @Test
    void theIssuingNodesApplyDoesNotWaitOnTheSlotItsOwnVerbHolds() throws Exception {
      assertTheLocalVerbsSlotCoversTheApply(Operation.DROP);
    }

    /** The drop a {@code restore database} performs on the database it replaces holds RESTORE, not DROP. */
    // Regression for #8035: DropApplyTakesMaintenanceSlot
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
    // Regression for #8035: DropApplyTakesMaintenanceSlot
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
    // Regression for #8035: DropApplyTakesMaintenanceSlot
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

  /**
   * Regression tests for issue #8451: a replicated drop-database apply decided whether there was anything to drop from
   * the in-memory registry alone ({@code server.existsDatabase}). A database closed on a peer with {@code close
   * database} is deregistered but its files stay on disk, so the apply took the "already absent" early return and left
   * the directory behind - and the next request on that peer reopened a database the rest of the cluster had dropped.
   * <p>
   * Drives a real (unstarted) {@link ArcadeDBServer} and a real {@link LocalDatabase}, like
   * {@code DropDatabaseApplyTest}.
   */
  @Nested
  class DropAppliesToClosedDatabase {

    private static final String DB_NAME              = "db8451";
    private static final long   LONG_WAIT_MS         = TimeUnit.MINUTES.toMillis(10);
    private static final long   HANG_DETECTOR_SECONDS = 60;

    @TempDir
    private Path serverDir;

    private Path                    databasesDir;
    private ArcadeDBServer          server;
    private LocalDatabase           localDatabase;
    private Path                    databaseDirectory;
    private ExecutorService         applyThread;
    private DeferredDatabaseDeleter deleter;

    private ArcadeStateMachine start() {
      databasesDir = serverDir.resolve("databases");
      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, databasesDir.toString());
      config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_BACKUP_WAIT_MS, LONG_WAIT_MS);
      server = new ArcadeDBServer(config);

      databaseDirectory = databasesDir.resolve(DB_NAME);
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

    /** What {@code ServerControlPlane.closeDatabase} does: close the instance and deregister it, files left in place. */
    private void closeDatabaseLikeTheVerb() {
      server.getDatabaseForOperator(DB_NAME).getEmbedded().close();
      server.removeDatabase(DB_NAME);
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

    private static void applyDrop(final ArcadeStateMachine sm, final String name) {
      sm.applyDropDatabaseEntry(RaftLogEntryCodec.decode(RaftLogEntryCodec.encodeDropDatabaseEntry(name)));
    }

    private BackupCoordinator coordinator() {
      return server.getBackupCoordinator();
    }

    /** The issue's scenario: the database was closed on this peer before the drop arrived. */
    // Regression for #8451: DropAppliesToClosedDatabase
    @Test
    void aDropOfADatabaseClosedOnThisPeerRemovesItsDirectory() {
      final ArcadeStateMachine sm = start();
      closeDatabaseLikeTheVerb();
      assertThat(server.existsDatabase(DB_NAME)).as("precondition: closed, so not registered").isFalse();
      assertThat(Files.isDirectory(databaseDirectory)).as("precondition: its files are still on disk").isTrue();

      applyDrop(sm, DB_NAME);

      assertThat(Files.exists(databaseDirectory)).as("the dropped database's directory must be gone").isFalse();
      assertThat(server.existsDatabase(DB_NAME)).isFalse();
      assertThatThrownBy(() -> server.getDatabaseForOperator(DB_NAME)).as("nothing is left for a later request to reopen")
          .isNotNull();
      assertThat(server.existsDatabase(DB_NAME)).isFalse();
      assertThat(coordinator().isInProgress(DB_NAME)).as("the apply must release the slot it took").isFalse();
    }

    /**
     * Issue #11739: {@code close database} also marks the database as closed on purpose, which keeps it a database of this
     * server (it exists, and its name is taken) while it is not registered. The drop ends that close, as a drop of an open
     * database does through {@code removeDatabase}: the apply that removed the files of a closed one left the mark, so
     * {@code GET /exists} on this peer answered {@code true} for a database the whole cluster had dropped, and a request for it
     * was refused as closed.
     */
    @Test
    void aDropOfADatabaseTheCloseVerbClosedEndsTheMarkItLeft() {
      final ArcadeStateMachine sm = start();
      new ServerControlPlane(server).closeDatabase(DB_NAME);
      assertThat(server.isDatabaseHeld(DB_NAME)).as("precondition: held by the close verb").isTrue();
      assertThat(server.isDatabaseOfServer(DB_NAME)).as("precondition: still a database of this server").isTrue();

      applyDrop(sm, DB_NAME);

      assertThat(Files.exists(databaseDirectory)).as("the dropped database's directory must be gone").isFalse();
      assertThat(server.isDatabaseHeld(DB_NAME)).as("the close mark goes with the database").isFalse();
      assertThat(server.isDatabaseOfServer(DB_NAME)).as("what GET /exists reads: not a database of this server any more")
          .isFalse();
    }

    /**
     * Review of #11739: with the rename refused, {@code stageForDeletion} deletes inline, and a delete the filesystem refuses is
     * logged and returns with the directory still there. The apply then fails instead of ending the close: the database stays
     * unavailable rather than reopened from the copy the cluster dropped, and the failure takes the path that keeps the entry
     * replayable.
     */
    @Test
    void aDirectoryTheInlineDeleteLeftBehindFailsTheApplyAndKeepsTheCloseMark() throws Exception {
      assumeTrue(FileSystems.getDefault().supportedFileAttributeViews().contains("posix"), "needs POSIX permissions");
      assumeFalse("root".equals(System.getProperty("user.name")), "root ignores directory permissions");

      final ArcadeStateMachine sm = start();
      deleter.close();
      deleter = new DeferredDatabaseDeleter() {
        @Override
        Path rename(final Path directory) {
          return null;
        }
      };
      sm.setDeferredDatabaseDeleter(deleter);
      new ServerControlPlane(server).closeDatabase(DB_NAME);

      final Set<PosixFilePermission> original = Files.getPosixFilePermissions(databaseDirectory);
      // its files cannot be unlinked, so the directory cannot be removed either
      Files.setPosixFilePermissions(databaseDirectory, Set.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_EXECUTE));
      try {
        assertThatThrownBy(() -> applyDrop(sm, DB_NAME)).isInstanceOf(UncheckedIOException.class).hasMessageContaining(DB_NAME);
      } finally {
        Files.setPosixFilePermissions(databaseDirectory, original);
      }

      assertThat(Files.isDirectory(databaseDirectory)).as("precondition: the inline delete left the directory").isTrue();
      assertThat(server.isDatabaseHeld(DB_NAME)).as("the close mark stays while the files do").isTrue();
      assertThat(server.isDatabaseOfServer(DB_NAME)).isTrue();
      assertThat(coordinator().isInProgress(DB_NAME)).as("the failed apply must release the slot it took").isFalse();
    }

    /**
     * Deregistered but still open in this JVM - a close that failed half way: the instance is closed before its
     * directory is renamed, rather than left holding handles on files that are gone.
     */
    // Regression for #8451: DropAppliesToClosedDatabase
    @Test
    void aDropOfADeregisteredButStillOpenInstanceClosesItBeforeRemovingTheDirectory() {
      final ArcadeStateMachine sm = start();
      server.removeDatabase(DB_NAME);
      assertThat(localDatabase.isOpen()).as("precondition: still open, only deregistered").isTrue();

      applyDrop(sm, DB_NAME);

      assertThat(localDatabase.isOpen()).isFalse();
      assertThat(Files.exists(databaseDirectory)).isFalse();
    }

    /**
     * A stat that fails for a reason other than "missing" must fail the apply, not read as "already absent": that would
     * retire the drop while the files stay on disk, to be reopened once the filesystem recovers (review of PR #8466).
     */
    // Regression for #8451: DropAppliesToClosedDatabase
    @Test
    void anUnreadableDatabasesDirectoryFailsTheApplyInsteadOfReadingAsAbsent() throws Exception {
      assumeTrue(FileSystems.getDefault().supportedFileAttributeViews().contains("posix"), "needs POSIX permissions");
      assumeFalse("root".equals(System.getProperty("user.name")), "root ignores directory permissions");

      final ArcadeStateMachine sm = start();
      closeDatabaseLikeTheVerb();
      final Set<PosixFilePermission> original = Files.getPosixFilePermissions(databasesDir);
      Files.setPosixFilePermissions(databasesDir, Set.of());
      try {
        assertThatThrownBy(() -> applyDrop(sm, DB_NAME)).isInstanceOf(UncheckedIOException.class)
            .hasMessageContaining(DB_NAME);
        // Through the retry wrapper the apply thread uses: the failure must quarantine the database, which is what keeps
        // takeSnapshot() from checkpointing past the drop - a ReplicationException would be rethrown past that arm.
        assertThatThrownBy(() -> sm.applyWithRetry(42L, DB_NAME, () -> applyDrop(sm, DB_NAME)))
            .isInstanceOf(ReplicationException.class);
        assertThat(sm.isDatabaseDiverged(DB_NAME)).as("the failed drop must quarantine the database").isTrue();
      } finally {
        Files.setPosixFilePermissions(databasesDir, original);
      }
      assertThat(Files.isDirectory(databaseDirectory)).isTrue();
      assertThat(coordinator().isInProgress(DB_NAME)).isFalse();
    }

    /** Deleting a closed database's files is the same destructive step as dropping an open one: it waits for the slot. */
    // Regression for #8451: DropAppliesToClosedDatabase
    @Test
    void aDropOfAClosedDatabaseWaitsForARunningMaintenanceOperation() throws Exception {
      final ArcadeStateMachine sm = start();
      closeDatabaseLikeTheVerb();
      assertThat(coordinator().begin(DB_NAME)).isTrue();

      final Future<?> apply = applyThread.submit(() -> applyDrop(sm, DB_NAME));
      assertThatThrownBy(() -> apply.get(500, TimeUnit.MILLISECONDS)).isInstanceOf(TimeoutException.class);
      assertThat(Files.isDirectory(databaseDirectory)).as("the files must not be deleted under the backup").isTrue();

      coordinator().end(DB_NAME);
      apply.get(HANG_DETECTOR_SECONDS, TimeUnit.SECONDS);

      assertThat(Files.exists(databaseDirectory)).isFalse();
      assertThat(coordinator().isInProgress(DB_NAME)).isFalse();
    }

    /**
     * A close that completes while the apply waits for the slot: the re-check after the wait used to see an absent
     * name and return, leaving the files behind the same way.
     */
    // Regression for #8451: DropAppliesToClosedDatabase
    @Test
    void aCloseThatLandsWhileTheApplyWaitsForTheSlotStillLosesItsDirectory() throws Exception {
      final ArcadeStateMachine sm = start();
      assertThat(coordinator().begin(DB_NAME)).isTrue();

      final Future<?> apply = applyThread.submit(() -> applyDrop(sm, DB_NAME));
      assertThatThrownBy(() -> apply.get(500, TimeUnit.MILLISECONDS)).isInstanceOf(TimeoutException.class);

      closeDatabaseLikeTheVerb();
      coordinator().end(DB_NAME);
      apply.get(HANG_DETECTOR_SECONDS, TimeUnit.SECONDS);

      assertThat(Files.exists(databaseDirectory)).isFalse();
      assertThat(server.existsDatabase(DB_NAME)).isFalse();
    }

    /** A name with neither a registration nor a directory is still the idempotent replay: it never waits on anything. */
    // Regression for #8451: DropAppliesToClosedDatabase
    @Test
    void aDropOfANameWithNoDirectoryStillReturnsWithoutWaiting() throws Exception {
      final ArcadeStateMachine sm = start();
      final String absent = "never8451";
      assertThat(coordinator().begin(absent)).isTrue();
      try {
        applyThread.submit(() -> applyDrop(sm, absent)).get(HANG_DETECTOR_SECONDS, TimeUnit.SECONDS);
        assertThat(coordinator().isInProgress(absent)).as("the replay must not touch the slot").isTrue();
      } finally {
        coordinator().end(absent);
      }
      assertThat(Files.isDirectory(databaseDirectory)).as("an unrelated database is untouched").isTrue();
    }

    /**
     * The name comes from a log entry, not from a validated request on this node: a name that escapes the databases
     * directory must never lead to deleting anything.
     */
    // Regression for #8451: DropAppliesToClosedDatabase
    @Test
    void aDropOfANameOutsideTheDatabasesDirectoryDeletesNothing() {
      final ArcadeStateMachine sm = start();
      closeDatabaseLikeTheVerb();

      applyDrop(sm, "..");

      assertThat(Files.isDirectory(databasesDir)).isTrue();
      assertThat(Files.isDirectory(databaseDirectory)).isTrue();
      assertThat(Files.isDirectory(serverDir)).isTrue();
    }
  }

  /**
   * Issue #7641 (review of PR #7649): {@code RaftReplicatedDatabase.dropInReplicas()} used to return as soon as the
   * drop-database entry was Raft-COMMITTED, which {@code RaftGroupCommitter.submitAndWait}'s own javadoc is explicit
   * is not the same as APPLIED - the state machine applies entries asynchronously on every node, leader included, so
   * the directory could still be mid-delete on this node when the call returned. {@code ServerControlPlane.dropDatabase}
   * releases the {@link com.arcadedb.engine.MaintenanceCoordinator.Operation#DROP} slot in a {@code finally} right
   * after {@code dropInReplicas()} returns, so a backup, restore, import or export admitted into the now-free slot
   * could start touching a directory the apply thread had not finished deleting - the same async-apply gap issue
   * #5503 closed for transaction commits, on the drop path this time.
   * <p>
   * {@code dropInReplicas()} now waits for its own committed index to be locally applied before returning, with
   * {@code throwOnTimeout = true}: since the caller's {@code finally} releases the slot regardless of the outcome
   * here, a caller that cannot confirm the local delete finished must be told so loudly rather than have the slot
   * released as if the drop had definitely completed.

   */
  @Nested
  class DropInReplicasWaitsForLocalApply {

    private static final String DB_NAME       = "dropinreplicas7641";
    private static final long   COMMITTED_LOG_INDEX = 42L;

    private static RaftReplicatedDatabase databaseWith(final RaftHAServer raft) {
      final LocalDatabase proxied = mock(LocalDatabase.class);
      when(proxied.getName()).thenReturn(DB_NAME);
      final ArcadeDBServer server = TestServerHelper.unstartedServer();
      return new RaftReplicatedDatabase(server, proxied, raft);
    }

    // Regression for #7641: DropInReplicasWaitsForLocalApply
    @Test
    void waitsForTheCommittedIndexToBeLocallyAppliedBeforeReturning() {
      final FakeRaftHAServer raft = FakeRaftHAServer.detached();
      raft.localDropVerbs(new LocalDropVerbs());
      final FakeRaftTransactionBroker broker = new FakeRaftTransactionBroker();
      raft.transactionBroker(broker);
      broker.returns("replicateDropDatabase", COMMITTED_LOG_INDEX);

      databaseWith(raft).dropInReplicas();

      assertThat(broker.calls("replicateDropDatabase")).containsOnlyOnce(List.of(DB_NAME));
      // throwOnTimeout = true: a caller that cannot confirm the local delete finished must be told, not left
      // to assume it did because nothing complained.
      assertThat(raft.calls("waitForAppliedIndex")).containsOnlyOnce(List.of(DB_NAME, COMMITTED_LOG_INDEX, true));
    }

    /**
     * The wait must happen AFTER the entry commits, using THAT entry's own index - waiting on a stale or
     * default index would not prove anything about this drop specifically.
     */
    // Regression for #7641: DropInReplicasWaitsForLocalApply
    @Test
    void theWaitRunsAfterReplicationUsingTheCommittedEntrysOwnIndex() {
      // One log for both fakes, so the order of their calls is observable
      final CallLog log = new CallLog();
      final FakeRaftHAServer raft = FakeRaftHAServer.detached().recordingOn(log);
      raft.localDropVerbs(new LocalDropVerbs());
      final FakeRaftTransactionBroker broker = new FakeRaftTransactionBroker(log);
      raft.transactionBroker(broker);
      broker.returns("replicateDropDatabase", COMMITTED_LOG_INDEX);

      databaseWith(raft).dropInReplicas();

      // Only the two calls that matter here: other recorded calls (the #9510 membership check) are not this test's subject.
      assertThat(log.methods().stream().filter(m -> m.equals("replicateDropDatabase") || m.equals("waitForAppliedIndex")))
          .containsExactly("replicateDropDatabase", "waitForAppliedIndex");
      assertThat(raft.calls("waitForAppliedIndex")).containsExactly(List.of(DB_NAME, COMMITTED_LOG_INDEX, true));
    }

    /**
     * A timeout proving the local apply cannot be confirmed must surface as a failure, not as a silent success -
     * the caller's maintenance slot is released either way, so this is the only signal it gets that the drop's
     * local effect is unconfirmed.
     */
    // Regression for #7641: DropInReplicasWaitsForLocalApply
    @Test
    void aTimeoutConfirmingTheLocalApplyThrowsRatherThanReturningSilently() {
      final FakeRaftHAServer raft = FakeRaftHAServer.detached();
      raft.localDropVerbs(new LocalDropVerbs());
      final FakeRaftTransactionBroker broker = new FakeRaftTransactionBroker();
      raft.transactionBroker(broker);
      broker.returns("replicateDropDatabase", COMMITTED_LOG_INDEX);
      raft.fails("waitForAppliedIndex", new ReplicationException("local apply did not catch up in time"));

      assertThatThrownBy(() -> databaseWith(raft).dropInReplicas())
          .isInstanceOf(TransactionException.class)
          .hasCauseInstanceOf(ReplicationException.class);
    }

    /** A failure to even replicate the entry must not reach the wait at all - there is no index to wait on. */
    // Regression for #7641: DropInReplicasWaitsForLocalApply
    @Test
    void aReplicationFailureNeverReachesTheApplyWait() {
      final FakeRaftHAServer raft = FakeRaftHAServer.detached();
      raft.localDropVerbs(new LocalDropVerbs());
      final FakeRaftTransactionBroker broker = new FakeRaftTransactionBroker();
      raft.transactionBroker(broker);
      broker.fails("replicateDropDatabase", new ReplicationException("quorum not reached"));

      assertThatThrownBy(() -> databaseWith(raft).dropInReplicas())
          .isInstanceOf(TransactionException.class)
          .hasCauseInstanceOf(ReplicationException.class);

      assertThat(raft.calls("waitForAppliedIndex")).isEmpty();
    }

    /**
     * Issue #8035: the local apply of the drop must know this node's verb holds the maintenance slot for it, so the
     * verb is registered for the whole wait - and withdrawn afterwards, so a later peer-style apply reserves the slot.
     */
    // Regression for #7641: DropInReplicasWaitsForLocalApply
    @Test
    void theVerbIsRegisteredAsAwaitingTheDropForTheLengthOfTheWait() {
      final FakeRaftHAServer raft = FakeRaftHAServer.detached();
      final LocalDropVerbs verbs = new LocalDropVerbs();
      raft.localDropVerbs(verbs);
      final FakeRaftTransactionBroker broker = new FakeRaftTransactionBroker();
      raft.transactionBroker(broker);
      broker.on("replicateDropDatabase", args -> {
        assertThat(verbs.isAwaiting(DB_NAME)).as("registered before the entry is submitted").isTrue();
        return COMMITTED_LOG_INDEX;
      });
      final boolean[] registeredDuringWait = new boolean[1];
      raft.on("waitForAppliedIndex", args -> {
        registeredDuringWait[0] = verbs.isAwaiting(DB_NAME);
        return null;
      });

      databaseWith(raft).dropInReplicas();

      assertThat(registeredDuringWait[0]).isTrue();
      assertThat(verbs.isAwaiting(DB_NAME)).isFalse();
    }

    /** A failed drop withdraws the registration too. */
    // Regression for #7641: DropInReplicasWaitsForLocalApply
    @Test
    void aFailedDropWithdrawsTheRegistration() {
      final FakeRaftHAServer raft = FakeRaftHAServer.detached();
      final LocalDropVerbs verbs = new LocalDropVerbs();
      raft.localDropVerbs(verbs);
      final FakeRaftTransactionBroker broker = new FakeRaftTransactionBroker();
      raft.transactionBroker(broker);
      broker.returns("replicateDropDatabase", COMMITTED_LOG_INDEX);
      raft.fails("waitForAppliedIndex", new ReplicationException("local apply did not catch up in time"));

      assertThatThrownBy(() -> databaseWith(raft).dropInReplicas()).isInstanceOf(TransactionException.class);
      assertThat(verbs.isAwaiting(DB_NAME)).isFalse();
    }
  }

  /**
   * Issue #10569: {@link RaftReplicatedDatabase#dropWalOfCancelledFiles} leaves out of a schema session's final entry only the
   * buffered WAL that wrote into files the session created and dropped again, and keeps every bucket delta paired with its WAL.
   */
  @Nested
  class DropWalOfCancelledFiles {
    private static final Set<Integer> CANCELLED = Set.of(7, 8);

    // Regression for #10569: DropWalOfCancelledFiles
    @Test
    void anEntryWritingOnlyIntoCancelledFilesIsDroppedWithItsDelta() {
      final byte[] kept = wal(1, 3);
      final byte[] gone = wal(2, 7, 8);
      final List<byte[]> walEntries = new ArrayList<>(List.of(kept, gone));
      final List<Map<Integer, Integer>> deltas = new ArrayList<>(List.of(Map.of(3, 1), Map.of()));

      RaftReplicatedDatabase.dropWalOfCancelledFiles(CANCELLED, walEntries, deltas);

      assertThat(walEntries).containsExactly(kept);
      assertThat(deltas).containsExactly(Map.of(3, 1));
    }

    // Regression for #10569: DropWalOfCancelledFiles
    @Test
    void anEntryAlsoWritingIntoAFileStillHereIsKeptWhole() {
      final byte[] mixed = wal(1, 7, 3);
      final List<byte[]> walEntries = new ArrayList<>(List.of(mixed));
      final List<Map<Integer, Integer>> deltas = new ArrayList<>(List.of(Map.of(3, 1)));

      RaftReplicatedDatabase.dropWalOfCancelledFiles(CANCELLED, walEntries, deltas);

      assertThat(walEntries).containsExactly(mixed);
      assertThat(deltas).hasSize(1);
    }

    // Regression for #10569: DropWalOfCancelledFiles
    @Test
    void anEntryWithNoPagesOrThatCannotBeReadIsKept() {
      final byte[] empty = wal(1);
      final byte[] unreadable = { 1, 2, 3 };
      final List<byte[]> walEntries = new ArrayList<>(List.of(empty, unreadable));
      final List<Map<Integer, Integer>> deltas = new ArrayList<>(List.of(Map.of(), Map.of()));

      RaftReplicatedDatabase.dropWalOfCancelledFiles(CANCELLED, walEntries, deltas);

      assertThat(walEntries).containsExactly(empty, unreadable);
      assertThat(deltas).hasSize(2);
    }

    // Regression for #10569: DropWalOfCancelledFiles
    @Test
    void nothingIsStrippedWithoutCancelledCreatesOrWhenTheDeltasDoNotPairWithTheWal() {
      final List<byte[]> walEntries = new ArrayList<>(List.of(wal(1, 7)));
      final List<Map<Integer, Integer>> deltas = new ArrayList<>(List.of(Map.of()));

      RaftReplicatedDatabase.dropWalOfCancelledFiles(null, walEntries, deltas);
      RaftReplicatedDatabase.dropWalOfCancelledFiles(Set.of(), walEntries, deltas);
      assertThat(walEntries).hasSize(1);

      final List<Map<Integer, Integer>> unpaired = new ArrayList<>();
      RaftReplicatedDatabase.dropWalOfCancelledFiles(CANCELLED, walEntries, unpaired);
      assertThat(walEntries).as("lists that disagree are shipped whole").hasSize(1);
    }

    /** The wire layout {@code deserializeWalTransaction} reads: one page per file id, each with a 4-byte delta. */
    private static byte[] wal(final long txId, final int... fileIds) {
      final int deltaSize = 4;
      final int pageSize = 6 * Integer.BYTES + deltaSize;
      final ByteBuffer buf = ByteBuffer.allocate(2 * Long.BYTES + 2 * Integer.BYTES + fileIds.length * pageSize);
      buf.putLong(txId);
      buf.putLong(System.currentTimeMillis());
      buf.putInt(fileIds.length);
      buf.putInt(fileIds.length * pageSize);
      for (final int fileId : fileIds) {
        buf.putInt(fileId);
        buf.putInt(0);             // pageNumber
        buf.putInt(0);             // changesFrom
        buf.putInt(deltaSize - 1); // changesTo
        buf.putInt(1);             // targetVersion
        buf.putInt(4096);          // currentPageSize
        buf.put(new byte[deltaSize]);
      }
      return buf.array();
    }
  }
}
