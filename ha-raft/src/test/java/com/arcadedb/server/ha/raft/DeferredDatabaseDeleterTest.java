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

import com.arcadedb.server.ArcadeDBServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Covers {@link DeferredDatabaseDeleter}: the directory rename must complete on the calling thread while the
 * recursive delete happens on the deleter's own executor, so the Raft apply loop never pays for the deletion.
 */
class DeferredDatabaseDeleterTest {

  private static final long AWAIT_TIMEOUT_MS = 20_000;

  @TempDir
  private Path                    databasesDirectory;
  private ExecutorService         executor;
  private DeferredDatabaseDeleter deleter;

  @BeforeEach
  void setUp() {
    executor = Executors.newSingleThreadExecutor();
    deleter = new DeferredDatabaseDeleter(executor);
  }

  @AfterEach
  void tearDown() {
    deleter.close();
  }

  private Path createDatabaseDirectory(final String name) throws IOException {
    final Path directory = Files.createDirectories(databasesDirectory.resolve(name));
    Files.writeString(directory.resolve("schema.json"), "{}");
    Files.writeString(Files.createDirectories(directory.resolve("nested")).resolve("bucket_0.bucket"), "payload");
    return directory;
  }

  /** Mirrors the production call site: stage under the caller's lock, queue the deletion outside it. */
  private Path drop(final Path databaseDirectory) {
    final Path staged = deleter.stageForDeletion(databaseDirectory);
    if (staged != null)
      deleter.deleteInBackground(staged);
    return staged;
  }

  private static void awaitGone(final Path path) throws InterruptedException {
    final long deadline = System.currentTimeMillis() + AWAIT_TIMEOUT_MS;
    while (Files.exists(path) && System.currentTimeMillis() < deadline)
      Thread.sleep(20);
    assertThat(path).doesNotExist();
  }

  /**
   * The regression this class exists for: when the deleter's executor cannot run, the source directory is
   * already gone (renamed) but the files are still on disk under the staged name - proving the recursive
   * delete did not run on the calling thread.
   */
  @Test
  void theRenameIsSynchronousAndTheDeletionIsNot() throws Exception {
    final Path databaseDirectory = createDatabaseDirectory("mydb");

    final CountDownLatch gate = new CountDownLatch(1);
    executor.submit(() -> gate.await(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS));

    final Path staged = drop(databaseDirectory);

    assertThat(databaseDirectory).as("the database directory must be gone when the call returns").doesNotExist();
    assertThat(staged).as("the files must still be on disk: the delete is queued, not inline").exists();
    assertThat(staged.resolve("nested").resolve("bucket_0.bucket")).exists();

    gate.countDown();
    awaitGone(staged);
  }

  @Test
  void theStagedDirectoryIsAReservedSiblingOfTheDatabase() throws Exception {
    final Path databaseDirectory = createDatabaseDirectory("mydb");

    final CountDownLatch gate = new CountDownLatch(1);
    executor.submit(() -> gate.await(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS));

    final Path staged = drop(databaseDirectory);

    assertThat(staged.getParent()).isEqualTo(databasesDirectory);
    assertThat(ArcadeDBServer.isReservedDatabaseName(staged.getFileName().toString())).isTrue();
    assertThat(staged.getFileName().toString()).startsWith(DeferredDatabaseDeleter.STAGING_PREFIX + "mydb-");

    gate.countDown();
    awaitGone(staged);
  }

  @Test
  void concurrentDropsOfTheSameNameGetDistinctStagingDirectories() throws Exception {
    final CountDownLatch gate = new CountDownLatch(1);
    executor.submit(() -> gate.await(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS));

    final Path first = drop(createDatabaseDirectory("mydb"));
    final Path second = drop(createDatabaseDirectory("mydb"));

    assertThat(first).isNotEqualTo(second);
    assertThat(first).exists();
    assertThat(second).exists();

    gate.countDown();
    awaitGone(first);
    awaitGone(second);
  }

  @Test
  void aMissingDatabaseDirectoryIsANoOp() {
    assertThat(drop(databasesDirectory.resolve("absent"))).isNull();
  }

  /**
   * When the filesystem cannot rename the directory aside, the directory must still be gone when the call
   * returns - deleted inline, the behaviour that predates the deferral - so the server never reloads a database
   * the cluster dropped.
   */
  @Test
  void aDirectoryThatCannotBeRenamedIsDeletedInline() throws Exception {
    final DeferredDatabaseDeleter unrenamable = new DeferredDatabaseDeleter(executor) {
      @Override
      Path rename(final Path databaseDirectory) {
        return null;
      }
    };
    final Path databaseDirectory = createDatabaseDirectory("mydb");

    assertThat(unrenamable.stageForDeletion(databaseDirectory)).isNull();
    assertThat(databaseDirectory).as("the inline fallback must still remove the directory").doesNotExist();
  }

  /**
   * A full queue must degrade to deleting on the calling thread rather than dropping the directory on the
   * floor: leaking staged directories would grow the disk footprint silently until the next restart sweep.
   */
  @Test
  void aSaturatedQueueDeletesOnTheCallingThread() throws Exception {
    final ThreadPoolExecutor saturated = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
        new ArrayBlockingQueue<>(1), new ThreadPoolExecutor.AbortPolicy());
    final CountDownLatch gate = new CountDownLatch(1);
    try (final DeferredDatabaseDeleter bounded = new DeferredDatabaseDeleter(saturated)) {
      saturated.execute(() -> {
        try {
          gate.await(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      });
      saturated.execute(() -> {
      });

      final Path staged = bounded.stageForDeletion(createDatabaseDirectory("mydb"));
      bounded.deleteInBackground(staged);

      assertThat(staged).as("the rejected deletion must have run before the call returned").doesNotExist();
      gate.countDown();
    }
  }

  @Test
  void theSweepRemovesOrphanedStagingDirectoriesAndNothingElse() throws Exception {
    final Path orphan = Files.createDirectories(
        databasesDirectory.resolve(DeferredDatabaseDeleter.STAGING_PREFIX + "gone-1234"));
    Files.writeString(orphan.resolve("schema.json"), "{}");
    final Path liveDatabase = createDatabaseDirectory("mydb");
    final Path raftDirectory = Files.createDirectories(databasesDirectory.resolve(".raft"));

    deleter.sweepOrphanedStagingDirectories(databasesDirectory);

    awaitGone(orphan);
    assertThat(liveDatabase).exists();
    assertThat(raftDirectory).exists();
  }

  @Test
  void theSweepToleratesAMissingDatabasesDirectory() {
    deleter.sweepOrphanedStagingDirectories(databasesDirectory.resolve("absent"));
  }
}
