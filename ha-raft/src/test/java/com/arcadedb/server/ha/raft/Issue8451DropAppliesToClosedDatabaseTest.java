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
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.backup.BackupCoordinator;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.UncheckedIOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Regression tests for issue #8451: a replicated drop-database apply decided whether there was anything to drop from
 * the in-memory registry alone ({@code server.existsDatabase}). A database closed on a peer with {@code close
 * database} is deregistered but its files stay on disk, so the apply took the "already absent" early return and left
 * the directory behind - and the next request on that peer reopened a database the rest of the cluster had dropped.
 * <p>
 * Drives a real (unstarted) {@link ArcadeDBServer} and a real {@link LocalDatabase}, like
 * {@code Issue8035DropApplyTakesMaintenanceSlotTest}.
 */
class Issue8451DropAppliesToClosedDatabaseTest {

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
    server.getDatabase(DB_NAME).getEmbedded().close();
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
  @Test
  void aDropOfADatabaseClosedOnThisPeerRemovesItsDirectory() {
    final ArcadeStateMachine sm = start();
    closeDatabaseLikeTheVerb();
    assertThat(server.existsDatabase(DB_NAME)).as("precondition: closed, so not registered").isFalse();
    assertThat(Files.isDirectory(databaseDirectory)).as("precondition: its files are still on disk").isTrue();

    applyDrop(sm, DB_NAME);

    assertThat(Files.exists(databaseDirectory)).as("the dropped database's directory must be gone").isFalse();
    assertThat(server.existsDatabase(DB_NAME)).isFalse();
    assertThatThrownBy(() -> server.getDatabase(DB_NAME)).as("nothing is left for a later request to reopen")
        .isNotNull();
    assertThat(server.existsDatabase(DB_NAME)).isFalse();
    assertThat(coordinator().isInProgress(DB_NAME)).as("the apply must release the slot it took").isFalse();
  }

  /**
   * Deregistered but still open in this JVM - a close that failed half way: the instance is closed before its
   * directory is renamed, rather than left holding handles on files that are gone.
   */
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
