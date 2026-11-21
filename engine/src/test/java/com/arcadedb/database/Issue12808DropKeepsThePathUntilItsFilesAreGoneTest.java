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
package com.arcadedb.database;

import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@code LocalDatabase.drop()} closed the instance, which released its path in the factory's registry, and only then deleted the
 * files. A {@code create()} or {@code open()} of the same path in that window built an instance over the files being deleted, and
 * the delete took them from under it: a torn directory (a schema file and a bucket, no dictionary, no WAL) that could be neither
 * opened ("Dictionary file not found") nor created ("already exists"), or a create failing with FileNotFoundException on its own
 * WAL. The path is now held until the files are gone, and a caller in the window is refused as an active instance.
 */
class Issue12808DropKeepsThePathUntilItsFilesAreGoneTest {
  private static final String PATH = "target/databases/Issue12808DropKeepsThePathUntilItsFilesAreGone";

  private ExecutorService executor;

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(PATH));
    try (final Database db = new DatabaseFactory(PATH).create()) {
      db.command("sql", "CREATE DOCUMENT TYPE T");
      db.transaction(() -> {
        for (int i = 0; i < 10; i++)
          db.newDocument("T").set("i", i).save();
      });
    }
    executor = Executors.newSingleThreadExecutor();
  }

  @AfterEach
  void tearDown() {
    LocalDatabase.TEST_AFTER_DROP_CLOSE_HOOK = null;
    executor.shutdownNow();
    final Database leftOpen = DatabaseFactory.getActiveDatabaseInstance(PATH);
    if (leftOpen != null && leftOpen.isOpen())
      leftOpen.close();
    FileUtils.deleteRecursively(new File(PATH));
  }

  @Test
  @Timeout(60)
  void anOpenOrCreateWhileTheFilesAreBeingDeletedIsRefusedAndTheDirectoryIsLeftToTheDrop() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) new DatabaseFactory(PATH).open();
    final CountDownLatch closed = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    LocalDatabase.TEST_AFTER_DROP_CLOSE_HOOK = dropping -> {
      if (dropping != db.getEmbedded())
        return;
      closed.countDown();
      try {
        release.await();
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    };

    final Future<?> drop = executor.submit(() -> {
      db.drop();
      return null;
    });
    assertThat(closed.await(30, TimeUnit.SECONDS)).isTrue();
    try {
      // THE WINDOW: THE INSTANCE IS CLOSED AND ITS FILES ARE STILL THERE
      assertThat(db.isOpen()).isFalse();
      assertThat(new File(PATH, "schema.json")).exists();

      assertThatThrownBy(() -> new DatabaseFactory(PATH).open()).isInstanceOf(DatabaseOperationException.class)
          .hasMessageContaining("Found active instance");
      assertThatThrownBy(() -> new DatabaseFactory(PATH).create()).isInstanceOf(DatabaseOperationException.class)
          .hasMessageContaining("Found active instance");
    } finally {
      release.countDown();
    }
    drop.get(30, TimeUnit.SECONDS);

    assertThat(new File(PATH)).doesNotExist();
    assertThat(DatabaseFactory.getActiveDatabaseInstance(PATH)).isNull();

    // THE PATH IS FREE AGAIN, AND WHAT IS CREATED ON IT IS A COMPLETE DATABASE
    try (final Database recreated = new DatabaseFactory(PATH).create()) {
      recreated.command("sql", "CREATE DOCUMENT TYPE U");
      recreated.transaction(() -> recreated.newDocument("U").set("i", 1).save());
      assertThat(recreated.countType("U", false)).isEqualTo(1);
    }
    try (final Database reopened = new DatabaseFactory(PATH).open()) {
      assertThat(reopened.getSchema().existsType("T")).isFalse();
      assertThat(reopened.countType("U", false)).isEqualTo(1);
    }
  }

  @Test
  @Timeout(60)
  void aDropRefusedInATransactionLeavesTheDatabaseOpenAndRegistered() {
    final Database db = new DatabaseFactory(PATH).open();
    db.begin();
    try {
      assertThatThrownBy(db::drop).isInstanceOf(DatabaseOperationException.class).hasMessageContaining("in transaction");
      assertThat(db.isOpen()).isTrue();
      assertThat(DatabaseFactory.getActiveDatabaseInstance(PATH)).isSameAs(db);
    } finally {
      db.rollback();
    }

    db.drop();
    assertThat(new File(PATH)).doesNotExist();
    assertThat(DatabaseFactory.getActiveDatabaseInstance(PATH)).isNull();
  }

  @Test
  @Timeout(60)
  void closeForDropStillReleasesThePathAtOnceBecauseItsCallerOwnsTheFiles() {
    final Database db = new DatabaseFactory(PATH).open();
    ((DatabaseInternal) db).closeForDrop();

    assertThat(DatabaseFactory.getActiveDatabaseInstance(PATH)).isNull();
    assertThat(new File(PATH, "schema.json")).exists();
  }
}
