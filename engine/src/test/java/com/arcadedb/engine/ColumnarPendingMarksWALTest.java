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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.TransactionContext;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Task 13, ordering fix (D): the first columnar mark of a commit runs before its pages are published, so an apply pass may drain it
 * from the previous page images. Until the second mark ran, the WAL file of the commit is its only record, and the housekeeping must
 * not drop it even once every page of the file reached the disk.
 */
class ColumnarPendingMarksWALTest extends TestHelper {

  @Override
  protected void endTest() {
    TransactionContext.afterPublishBeforeSecondMarkForTesting = null;
  }

  @Test
  void aCommitBetweenItsMarksKeepsItsWALFileUntilTheSecondMark() throws Exception {
    database.command("sql", "CREATE DOCUMENT TYPE Asset BUCKETS 1");
    database.command("sql", "CREATE PROPERTY Asset.brand STRING");
    database.command("sql", "CREATE INDEX ON Asset (brand) COLUMNAR");
    final TransactionManager transactionManager = ((DatabaseInternal) database).getTransactionManager();
    transactionManager.cancelHousekeepingForTesting();

    final CountDownLatch paused = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    TransactionContext.afterPublishBeforeSecondMarkForTesting = () -> {
      paused.countDown();
      try {
        release.await();
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    };

    final ExecutorService committer = Executors.newSingleThreadExecutor();
    try {
      final Future<?> commit = committer.submit(() -> database.transaction(() -> database.newDocument("Asset").set("brand", "a").save()));
      // a hang detector, not a latency bound
      assertThat(paused.await(30, TimeUnit.SECONDS)).isTrue();
      TransactionContext.afterPublishBeforeSecondMarkForTesting = null;

      // retire every WAL file, as a rotation does, and find the one the paused commit wrote to
      WALFile target = null;
      for (int slot = 0; slot < TransactionManager.walFilePoolSize(database.getConfiguration()); slot++) {
        final WALFile file = transactionManager.replaceActiveWALFileForTesting(slot, null);
        if (file == null)
          continue;
        file.setActive(false);
        transactionManager.addInactiveWALFileForTesting(file);
        if (file.getPendingColumnarMarks() > 0)
          target = file;
      }
      assertThat(target).isNotNull();
      final File targetFile = new File(target.getFilePath());

      // the published pages reach the disk: only the pending second mark protects the file now
      for (int i = 0; i < 30_000 && target.getPendingPagesToFlush() > 0; i++)
        Thread.sleep(1);
      assertThat(target.getPendingPagesToFlush()).isZero();

      assertThat(transactionManager.cleanWALFilesForTesting(true, false, false)).isFalse();
      assertThat(targetFile).exists();

      release.countDown();
      commit.get(30, TimeUnit.SECONDS);
      assertThat(target.getPendingColumnarMarks()).isZero();

      assertThat(transactionManager.cleanWALFilesForTesting(true, false, false)).isTrue();
      assertThat(targetFile).doesNotExist();
    } finally {
      release.countDown();
      committer.shutdownNow();
    }
  }
}
