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
package com.arcadedb.engine;

import com.arcadedb.TestHelper;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for the clean-close fsync gap (issue #4332).
 *
 * On a clean close, all committed data pages must be synced to physical storage before WAL
 * files are deleted. Without the fix, pages are in the OS buffer cache when WAL files are
 * removed: a subsequent OS crash loses committed transactions with no recovery path.
 */
class TransactionManagerCloseWALFsyncTest extends TestHelper {

  private static final int RECORD_COUNT = 50;

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType("Item");
  }

  @Test
  void noWalFilesAfterCleanClose() {
    for (int i = 0; i < RECORD_COUNT; i++) {
      final int idx = i;
      database.transaction(() -> database.newDocument("Item").set("n", idx).save());
    }

    final String dbPath = database.getDatabasePath();
    database.close();

    final File[] walFiles = new File(dbPath).listFiles((dir, name) -> name.endsWith(".wal"));
    assertThat(walFiles)
        .as("No WAL files must remain after a clean close")
        .isNotNull()
        .isEmpty();

    // Reopen so TestHelper.afterTest() can drop the database.
    reopenDatabase();
  }

  @Test
  void dataReadableAfterReopenWithoutWALRecovery() {
    for (int i = 0; i < RECORD_COUNT; i++) {
      final int idx = i;
      database.transaction(() -> database.newDocument("Item").set("n", idx).save());
    }

    final String dbPath = database.getDatabasePath();
    database.close();

    // Remove any leftover WAL files so reopen cannot rely on WAL replay.
    // With the fix the data pages are already on physical storage after close().
    final File dir = new File(dbPath);
    final File[] walFiles = dir.listFiles((d, name) -> name.endsWith(".wal"));
    if (walFiles != null)
      Arrays.stream(walFiles).forEach(File::delete);

    reopenDatabase();

    final long count = database.countType("Item", true);
    assertThat(count)
        .as("All committed records must be readable after reopen without WAL recovery")
        .isEqualTo(RECORD_COUNT);
  }
}
