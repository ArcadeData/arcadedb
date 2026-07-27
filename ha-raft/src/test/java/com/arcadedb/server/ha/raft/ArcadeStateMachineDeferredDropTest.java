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
import com.arcadedb.database.BootstrapFingerprint;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #5454.
 * <p>
 * {@code applyDropDatabaseEntry} runs on the single Ratis apply thread shared by every database multiplexed
 * on the state machine, and used to perform the whole physical deletion there. It must now leave the apply
 * thread with nothing but a directory rename, deferring the recursive delete to the deleter's executor.
 * <p>
 * The tests drive a real (unstarted) {@link ArcadeDBServer} with a real {@link LocalDatabase} - no mocking
 * framework - and mirror {@code ArcadeStateMachineBootstrapBaselinePersistenceTest}.
 */
class ArcadeStateMachineDeferredDropTest {

  private static final String DB_NAME          = "db-a";
  private static final long   AWAIT_TIMEOUT_MS = 20_000;

  @TempDir
  private Path                    serverDir;
  private ArcadeDBServer          server;
  private LocalDatabase           localDatabase;
  private Path                    databaseDirectory;
  private ExecutorService         executor;
  private DeferredDatabaseDeleter deleter;

  @BeforeEach
  void setUp() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, serverDir.toString());
    server = new ArcadeDBServer(config);

    databaseDirectory = serverDir.resolve(DB_NAME);
    localDatabase = (LocalDatabase) new DatabaseFactory(databaseDirectory.toString()).create();
    localDatabase.transaction(() -> {
      localDatabase.getSchema().createDocumentType("Doc");
      localDatabase.newDocument("Doc").set("name", "first").save();
    });
    server.registerDatabase(DB_NAME, localDatabase);

    executor = Executors.newSingleThreadExecutor();
    deleter = new DeferredDatabaseDeleter(executor);
  }

  @AfterEach
  void tearDown() {
    deleter.close();
    if (localDatabase != null && localDatabase.isOpen())
      localDatabase.close();
    FileUtils.deleteRecursively(new File(databaseDirectory.toString()));
  }

  private ArcadeStateMachine newStateMachine() {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.setServer(server);
    sm.setDeferredDatabaseDeleter(deleter);
    return sm;
  }

  private void applyDrop(final ArcadeStateMachine sm) {
    sm.applyDropDatabaseEntry(RaftLogEntryCodec.decode(RaftLogEntryCodec.encodeDropDatabaseEntry(DB_NAME)));
  }

  private Path findStagingDirectory() throws Exception {
    try (var entries = Files.list(serverDir)) {
      final List<Path> staged = entries
          .filter(p -> p.getFileName().toString().startsWith(DeferredDatabaseDeleter.STAGING_PREFIX))
          .toList();
      assertThat(staged).as("exactly one staging directory expected").hasSize(1);
      return staged.getFirst();
    }
  }

  private static void awaitGone(final Path path) throws InterruptedException {
    final long deadline = System.currentTimeMillis() + AWAIT_TIMEOUT_MS;
    while (Files.exists(path) && System.currentTimeMillis() < deadline)
      Thread.sleep(20);
    assertThat(path).doesNotExist();
  }

  /**
   * With the deleter's executor blocked, apply still completes and the database is gone from the registry and
   * from its directory - but its files are still on disk under the staging name, proving the recursive delete
   * did not run on the apply thread.
   */
  @Test
  void applyDeregistersTheDatabaseWithoutDeletingItsFilesInline() throws Exception {
    final CountDownLatch gate = new CountDownLatch(1);
    executor.submit(() -> gate.await(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS));

    applyDrop(newStateMachine());

    assertThat(server.existsDatabase(DB_NAME)).isFalse();
    assertThat(localDatabase.isOpen()).isFalse();
    assertThat(databaseDirectory).doesNotExist();

    final Path staged = findStagingDirectory();
    assertThat(staged.resolve("schema.json")).as("the physical delete must still be pending").exists();

    gate.countDown();
    awaitGone(staged);
  }

  /**
   * Once the rename has happened the database name is free again: a create of the same name must not see the
   * old directory, which previously survived for the whole duration of the delete.
   */
  @Test
  void theDatabaseNameIsReusableAsSoonAsApplyReturns() throws Exception {
    final CountDownLatch gate = new CountDownLatch(1);
    executor.submit(() -> gate.await(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS));

    applyDrop(newStateMachine());

    assertThat(new DatabaseFactory(databaseDirectory.toString()).exists()).isFalse();

    gate.countDown();
    awaitGone(findStagingDirectory());
  }

  /**
   * Replaying the entry after the rename is a no-op: the staged directory is reserved, so the server does not
   * see the database any more and the early-return branch is taken.
   */
  @Test
  void replayingTheEntryAfterTheRenameIsANoOp() throws Exception {
    final CountDownLatch gate = new CountDownLatch(1);
    executor.submit(() -> gate.await(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS));

    final ArcadeStateMachine sm = newStateMachine();
    applyDrop(sm);
    final Path staged = findStagingDirectory();

    applyDrop(sm);

    assertThat(findStagingDirectory()).as("the replay must not stage a second directory").isEqualTo(staged);

    gate.countDown();
    awaitGone(staged);
  }

  /**
   * The persisted bootstrap baseline is still evicted by the drop.
   */
  @Test
  void theBootstrapBaselineIsStillEvicted() throws Exception {
    final ArcadeStateMachine sm = newStateMachine();
    final String fingerprint = BootstrapFingerprint.compute(new File(databaseDirectory.toString()));
    sm.applyBootstrapFingerprintEntry(RaftLogEntryCodec.decode(
        RaftLogEntryCodec.encodeBootstrapFingerprintEntry(DB_NAME, fingerprint, localDatabase.getLastTransactionId())), 50L);
    assertThat(sm.getBootstrapBaseline(DB_NAME)).isNotNull();

    applyDrop(sm);

    assertThat(sm.getBootstrapBaseline(DB_NAME)).isNull();
  }
}
