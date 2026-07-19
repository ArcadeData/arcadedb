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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * An open that fails must not delete the WAL files of the database it failed to open.
 * <p>
 * {@code releaseResourcesOnOpenFailure} closes the transaction manager, whose clean-close path deletes
 * every {@code *.wal} file in the database directory. That glob is directory-wide and mode-blind, so a
 * rejected open used to discard the WAL of a database that had not been cleanly closed, losing every
 * change made after the last page flush.
 * <p>
 * The test deliberately manages its own factory and directory rather than extending {@code TestHelper}:
 * the database it builds is killed and never reopened, which the shared lifecycle cannot clean up
 * (its teardown queries a dead instance and would leak it into the tests that follow).
 * <p>
 * It stops at the WAL-preservation invariant and does not assert that the following recovery open
 * succeeds. That is a separate, pre-existing defect: {@code openInternal} runs {@code schema.load()}
 * before {@code checkForRecovery()}, so a database killed before its dictionary page reached disk is
 * unopenable whether or not the WAL survives - {@code Dictionary.reload} sees a zero-length file and
 * commits a transaction during the load, where file id 0 is not yet registered.
 */
class FailedOpenPreservesWalTest {
  private static final String DATABASE_NAME = "FailedOpenPreservesWalTest";
  private static final String DATABASE_PATH = "./target/databases/" + DATABASE_NAME;

  @BeforeEach
  @AfterEach
  void cleanDatabaseDirectory() {
    GlobalConfiguration.SERVER_ROOT_PATH.setValue("./target");
    // The database built here is killed, so it never unwinds its thread-local context the way a clean
    // close would. Left behind, it becomes the "active database" of this thread and every later test in
    // the reused fork sees a dead instance instead of no context at all.
    DatabaseContext.INSTANCE.removeCurrentThreadContexts();
    FileUtils.deleteRecursively(new File(DATABASE_PATH));
  }

  @Test
  void rejectedReadOnlyOpenDoesNotDeleteTheWalNeededForRecovery() {
    try (final DatabaseFactory factory = new DatabaseFactory(DATABASE_PATH)) {
      final Database database = factory.create();
      database.getSchema().createDocumentType("WalPreserveType");
      database.transaction(() -> database.newDocument("WalPreserveType").set("k", 1).save());

      // Crash without a clean close: the lock marker stays on disk, so the next open must run recovery.
      ((DatabaseInternal) database).kill();
      database.close();

      assertThat(walFiles()).as("the killed database must leave WAL files to replay").isNotEmpty();

      // A READ_ONLY open of a database that needs recovery is rejected; releasing the resources it
      // acquired must not touch the WAL.
      assertThatThrownBy(() -> factory.open(ComponentFile.MODE.READ_ONLY)).isInstanceOf(RuntimeException.class);

      assertThat(walFiles()).as("a failed open must preserve the WAL files it does not own").isNotEmpty();
    }
  }

  private File[] walFiles() {
    final File[] files = new File(DATABASE_PATH).listFiles((dir, name) -> name.endsWith(".wal"));
    return files == null ? new File[0] : files;
  }
}
