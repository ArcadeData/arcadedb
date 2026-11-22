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

import com.arcadedb.exception.DatabaseNotFoundException;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * {@code open()} and {@code create()} take turns per database PATH, not per factory: every caller builds its own
 * {@link DatabaseFactory}, so a monitor on the factory excluded nothing and a refused concurrent open damaged the instance that
 * won (issue #12791, see {@link ConcurrentOpenOfSamePathTest} for the end to end damage).
 */
class DatabaseFactoryPathLockTest {
  private static final String PATH       = "target/databases/DatabaseFactoryPathLockTest";
  private static final String OTHER_PATH = "target/databases/DatabaseFactoryPathLockTestOther";

  private ExecutorService executor;

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(PATH));
    FileUtils.deleteRecursively(new File(OTHER_PATH));
    try (final Database db = new DatabaseFactory(PATH).create()) {
      db.command("sql", "CREATE DOCUMENT TYPE T");
    }
    try (final Database db = new DatabaseFactory(OTHER_PATH).create()) {
      db.command("sql", "CREATE DOCUMENT TYPE T");
    }
    executor = Executors.newCachedThreadPool();
  }

  @AfterEach
  void tearDown() throws InterruptedException {
    executor.shutdown();
    executor.awaitTermination(30, TimeUnit.SECONDS);
    for (final String path : new String[] { PATH, OTHER_PATH }) {
      final Database leftOpen = DatabaseFactory.getActiveDatabaseInstance(path);
      if (leftOpen != null && leftOpen.isOpen())
        leftOpen.close();
    }
    FileUtils.deleteRecursively(new File(PATH));
    FileUtils.deleteRecursively(new File(OTHER_PATH));
  }

  @Test
  @Timeout(60)
  void aSecondOpenOfTheSamePathWaitsForTheFirstAndIsThenRefused() throws Exception {
    final CountDownLatch inFirstOpen = new CountDownLatch(1);
    final CountDownLatch releaseFirstOpen = new CountDownLatch(1);

    final DatabaseFactory first = new DatabaseFactory(PATH);
    first.registerCallback(DatabaseInternal.CALLBACK_EVENT.DB_AFTER_OPEN, () -> {
      inFirstOpen.countDown();
      releaseFirstOpen.await();
      return null;
    });
    final Future<Database> firstOpen = executor.submit(() -> first.open());
    assertThat(inFirstOpen.await(30, TimeUnit.SECONDS)).isTrue();

    // THE FIRST OPEN HAS BUILT ITS INSTANCE BUT NOT REGISTERED IT. A SECOND FACTORY ON THE SAME PATH MUST NOT BUILD A SECOND
    // INSTANCE OVER THE SAME FILES, SO IT WAITS FOR THE FIRST (WITHOUT THE PATH MONITOR IT RAN, AND FAILED ON THE FILE LOCK)
    final Future<Database> secondOpen = executor.submit(() -> new DatabaseFactory(PATH).open());
    try {
      assertThatThrownBy(() -> secondOpen.get(500, TimeUnit.MILLISECONDS)).isInstanceOf(TimeoutException.class);
    } finally {
      // ALSO ON A FAILURE: THE HELD OPEN WOULD OTHERWISE REGISTER BEHIND THE TEAR DOWN AND FAIL THE NEXT TEST
      releaseFirstOpen.countDown();
    }
    final Database winner = firstOpen.get(30, TimeUnit.SECONDS);
    try {
      assertThatThrownBy(() -> secondOpen.get(30, TimeUnit.SECONDS)).isInstanceOf(ExecutionException.class)
          .hasCauseInstanceOf(DatabaseOperationException.class)
          .hasRootCauseMessage(
              "Found active instance of database '" + new File(PATH).toPath().toRealPath() + "' already in use");
      assertThat(winner.isOpen()).isTrue();
      assertThat(DatabaseFactory.getActiveDatabaseInstance(PATH)).isSameAs(winner);
    } finally {
      winner.close();
    }
  }

  @Test
  @Timeout(60)
  void aSecondCreateOfTheSamePathWaitsForTheFirstOpenAndIsThenRefused() throws Exception {
    final CountDownLatch inFirstOpen = new CountDownLatch(1);
    final CountDownLatch releaseFirstOpen = new CountDownLatch(1);

    final DatabaseFactory first = new DatabaseFactory(PATH);
    first.registerCallback(DatabaseInternal.CALLBACK_EVENT.DB_AFTER_OPEN, () -> {
      inFirstOpen.countDown();
      releaseFirstOpen.await();
      return null;
    });
    final Future<Database> firstOpen = executor.submit(() -> first.open());
    assertThat(inFirstOpen.await(30, TimeUnit.SECONDS)).isTrue();

    final Future<Database> secondCreate = executor.submit(() -> new DatabaseFactory(PATH).create());
    try {
      assertThatThrownBy(() -> secondCreate.get(500, TimeUnit.MILLISECONDS)).isInstanceOf(TimeoutException.class);
    } finally {
      releaseFirstOpen.countDown();
    }
    final Database winner = firstOpen.get(30, TimeUnit.SECONDS);
    try {
      assertThatThrownBy(() -> secondCreate.get(30, TimeUnit.SECONDS)).isInstanceOf(ExecutionException.class)
          .hasCauseInstanceOf(DatabaseOperationException.class);
      assertThat(winner.isOpen()).isTrue();
      assertThat(winner.getSchema().existsType("T")).isTrue();
    } finally {
      winner.close();
    }
  }

  @Test
  @Timeout(60)
  void anOpenOfAnotherPathIsNotHeldUp() throws Exception {
    final CountDownLatch inFirstOpen = new CountDownLatch(1);
    final CountDownLatch releaseFirstOpen = new CountDownLatch(1);

    final DatabaseFactory first = new DatabaseFactory(PATH);
    first.registerCallback(DatabaseInternal.CALLBACK_EVENT.DB_AFTER_OPEN, () -> {
      inFirstOpen.countDown();
      releaseFirstOpen.await();
      return null;
    });
    final Future<Database> firstOpen = executor.submit(() -> first.open());
    assertThat(inFirstOpen.await(30, TimeUnit.SECONDS)).isTrue();

    try {
      // THE MONITOR IS PER PATH: A SLOW OPEN OF ONE DATABASE MUST NOT STALL THE OPEN OF ANOTHER
      final Future<Database> otherOpen = executor.submit(() -> new DatabaseFactory(OTHER_PATH).open());
      try (final Database other = otherOpen.get(30, TimeUnit.SECONDS)) {
        assertThat(other.isOpen()).isTrue();
      }
    } finally {
      releaseFirstOpen.countDown();
      firstOpen.get(30, TimeUnit.SECONDS).close();
    }
  }

  @Test
  @Timeout(60)
  void thePathMonitorLastsOnlyAsLongAsTheCallsOnThePath() {
    assertThat(DatabaseFactory.isPathLockInUse(PATH)).isFalse();

    final Database db = new DatabaseFactory(PATH).open();
    assertThat(DatabaseFactory.isPathLockInUse(PATH)).isFalse();

    // REFUSED AS ACTIVE, REFUSED AS EXISTING, AND FAILING BECAUSE THERE IS NOTHING TO OPEN: EVERY WAY OUT RELEASES IT
    assertThatThrownBy(() -> new DatabaseFactory(PATH).open()).isInstanceOf(DatabaseOperationException.class);
    assertThat(DatabaseFactory.isPathLockInUse(PATH)).isFalse();
    assertThatThrownBy(() -> new DatabaseFactory(PATH).create()).isInstanceOf(DatabaseOperationException.class);
    assertThat(DatabaseFactory.isPathLockInUse(PATH)).isFalse();

    db.close();
    final String missing = "target/databases/DatabaseFactoryPathLockTestMissing";
    FileUtils.deleteRecursively(new File(missing));
    assertThatThrownBy(() -> new DatabaseFactory(missing).open()).isInstanceOf(DatabaseNotFoundException.class);
    assertThat(DatabaseFactory.isPathLockInUse(missing)).isFalse();
    assertThatThrownBy(() -> new DatabaseFactory(PATH).create()).isInstanceOf(DatabaseOperationException.class)
        .hasMessageContaining("already exists");
    assertThat(DatabaseFactory.isPathLockInUse(PATH)).isFalse();
  }

  /**
   * {@code create()} makes the directory before it computes the key of its monitor, so a failure before anything was written
   * must take the directory back, as it did when {@link LocalDatabase#create()} made it.
   */
  @Test
  @Timeout(60)
  void aCreateThatFailsBeforeWritingLeavesNoDirectoryBehind() {
    final File invalid = new File("target/databases/DatabaseFactoryPathLockTest*Invalid");
    FileUtils.deleteRecursively(invalid);
    assertThatThrownBy(() -> new DatabaseFactory(invalid.getPath()).create()).isInstanceOf(DatabaseOperationException.class);
    assertThat(invalid).doesNotExist();
    assertThat(DatabaseFactory.isPathLockInUse(invalid.getPath())).isFalse();
  }

  /**
   * On a case-insensitive volume two spellings of one directory are one database: the second is refused as active before it
   * builds anything, whether it opens or creates. The key of the monitor and of the registry is the real path, which the open
   * and the create both compute only once the directory exists (the create makes it first).
   */
  @Test
  @Timeout(60)
  void anotherCaseOfThePathIsTheSameDatabaseOnACaseInsensitiveVolume() {
    final String spelled = "target/databases/DatabaseFactoryPathLockTestCase";
    final String otherCase = "target/databases/databasefactorypathlocktestcase";
    FileUtils.deleteRecursively(new File(spelled));
    try (final Database db = new DatabaseFactory(spelled).create()) {
      assumeTrue(new File(otherCase).exists(), "the volume is case sensitive");

      assertThat(DatabaseFactory.getActiveDatabaseInstance(otherCase)).isSameAs(db);
      assertThatThrownBy(() -> new DatabaseFactory(otherCase).open()).isInstanceOf(DatabaseOperationException.class)
          .hasMessageContaining("Found active instance");
      assertThatThrownBy(() -> new DatabaseFactory(otherCase.toUpperCase(Locale.ROOT)).create())
          .isInstanceOf(DatabaseOperationException.class).hasMessageContaining("Found active instance");
      assertThat(db.isOpen()).isTrue();
    } finally {
      FileUtils.deleteRecursively(new File(spelled));
    }
  }
}
