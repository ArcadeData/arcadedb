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

import com.arcadedb.engine.ComponentFile;
import com.arcadedb.exception.DatabaseMetadataException;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@code openInternal()} registers a transaction context for the opening thread under the database path before it knows the open
 * will succeed, and its failure clean-up released every resource but that one. The thread was left holding a context whose
 * transaction belongs to an instance that never opened: pinned for as long as the thread lives, and the first use of a database
 * of the same path opened later (by any thread) failed with {@code InvalidDatabaseInstanceException: Invalid transactional
 * context (different db)}.
 */
class FailedOpenLeavesNoThreadContextTest {
  private static final String PATH = "target/databases/FailedOpenLeavesNoThreadContext";

  private ExecutorService executor;

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(PATH));
    final Database db = new DatabaseFactory(PATH).create();
    db.command("sql", "CREATE DOCUMENT TYPE T");
    db.transaction(() -> db.newDocument("T").set("i", 0).save());
    // A CRASH: THE LOCK FILE AND THE WAL STAY, SO THE NEXT OPEN HAS TO RECOVER, WHICH A READ ONLY OPEN CANNOT DO
    ((DatabaseInternal) db).kill();
    db.close();
    executor = Executors.newSingleThreadExecutor();
  }

  @AfterEach
  void tearDown() {
    executor.shutdownNow();
    final Database leftOpen = DatabaseFactory.getActiveDatabaseInstance(PATH);
    if (leftOpen != null && leftOpen.isOpen())
      leftOpen.close();
    FileUtils.deleteRecursively(new File(PATH));
  }

  @Test
  @Timeout(60)
  void aFailedOpenLeavesNoTransactionContextOnTheThread() throws Exception {
    final boolean contextLeft = executor.submit(() -> {
      assertThatThrownBy(() -> new DatabaseFactory(PATH).open(ComponentFile.MODE.READ_ONLY)).isInstanceOf(
          DatabaseMetadataException.class).hasMessageContaining("needs recovery");
      return DatabaseContext.INSTANCE.getContextIfExists(PATH) != null;
    }).get(30, TimeUnit.SECONDS);

    assertThat(contextLeft).as("the failed open's transaction context on the thread that attempted it").isFalse();
  }

  @Test
  @Timeout(60)
  void aThreadWhoseOpenFailedUsesTheDatabaseOpenedLaterAtFirstTry() throws Exception {
    executor.submit(() -> assertThatThrownBy(() -> new DatabaseFactory(PATH).open(ComponentFile.MODE.READ_ONLY)).isInstanceOf(
        DatabaseMetadataException.class)).get(30, TimeUnit.SECONDS);

    // ANOTHER THREAD OPENS IT FOR GOOD (THE RECOVERY RUNS), AND THE THREAD WHOSE OPEN FAILED THEN WORKS ON THAT INSTANCE
    final Database db = new DatabaseFactory(PATH).open();
    final long count = executor.submit(() -> {
      db.transaction(() -> db.newDocument("T").set("i", 1).save());
      return db.countType("T", false);
    }).get(30, TimeUnit.SECONDS);

    assertThat(count).isEqualTo(2L);
  }
}
