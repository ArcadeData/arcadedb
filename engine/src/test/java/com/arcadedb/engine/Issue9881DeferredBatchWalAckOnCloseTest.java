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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.utility.CallableNoReturn;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #9881: a database closed or dropped while its flushing was suspended (a backup, a verify, an
 * HA snapshot) has batches sitting in the flush thread's deferred backlog rather than in its queue. The purge used to
 * detach them without ever releasing their WAL acks, so {@code WALFile.pagesToFlush} stayed above zero for each page.
 * <ul>
 * <li>On a {@code drop()} that is wrong: the files are deleted, nothing will ever replay those pages, and the stranded
 * count makes {@code TransactionManager.close()} spin its retry loop on WAL files it cannot drop.</li>
 * <li>On a {@code close()} it is right: the deferred pages never reached the disk, and the un-acked WAL entry is the
 * only durable copy of them - the close is crash-equivalent and the next open replays them.</li>
 * </ul>
 * Both cases are driven end to end through the real flush thread and a real suspension. The {@link TestHelper} database
 * stays open throughout only to hold a page-manager lifecycle reference: the database under test is closed inside the
 * suspension, and were it the last one open, the page manager would shut down under the suspender.
 */
class Issue9881DeferredBatchWalAckOnCloseTest extends TestHelper {
  private static final String DB_PATH               = "target/databases/Issue9881DeferredBatchWalAckOnCloseTest-suspended";
  /** Bounds only waits expected to SUCCEED, so a generous value cannot turn a passing run red (#6260). */
  private static final long   ASSERTION_TIMEOUT_SEC = 60;
  private static final int    RECORDS               = 10;

  @BeforeEach
  void cleanSuspendedDatabase() {
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @AfterEach
  void removeSuspendedDatabase() {
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @Test
  void droppingASuspendedDatabaseReleasesTheWalAcksOfItsDeferredBatches() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) new DatabaseFactory(DB_PATH).create();
    db.getSchema().createDocumentType("Doc");

    final List<WALFile> walFiles = new ArrayList<>();
    final long[] pendingBeforeDrop = { 0 };
    runSuspended(db, () -> {
      commitDeferredRecords(db);
      walFiles.addAll(activeWALFiles(db));
      pendingBeforeDrop[0] = pendingAcks(walFiles);
      db.drop();
    });

    assertThat(pendingBeforeDrop[0]).as("the deferred pages were written to the WAL and not yet acked").isPositive();

    assertThat(db.isOpen()).isFalse();
    assertThat(new File(DB_PATH)).doesNotExist();
    assertThat(pendingAcks(walFiles)).as(
        "a dropped database's deferred pages will never be written nor replayed: their WAL acks must be released").isZero();
  }

  @Test
  void closingASuspendedDatabaseKeepsTheWalOfItsDeferredBatchesForRecovery() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) new DatabaseFactory(DB_PATH).create();
    db.getSchema().createDocumentType("Doc");
    // The deferred pages cannot make progress while the database is suspended: give up the close's flush wait fast.
    db.getConfiguration().setValue(GlobalConfiguration.FLUSH_ALL_PAGES_TIMEOUT, 200L);

    final List<WALFile> walFiles = new ArrayList<>();
    runSuspended(db, () -> {
      commitDeferredRecords(db);
      walFiles.addAll(activeWALFiles(db));
      db.close();
    });

    assertThat(db.isOpen()).isFalse();
    assertThat(pendingAcks(walFiles)).as(
        "a closed database's deferred pages never reached the disk: the WAL entries must stay un-acked for recovery").isPositive();

    try (final Database reopened = new DatabaseFactory(DB_PATH).open()) {
      final long[] count = { 0 };
      reopened.iterateType("Doc", false).forEachRemaining(r -> count[0]++);
      assertThat(count[0]).as("the next open must replay the deferred pages from the preserved WAL").isEqualTo(RECORDS);
    }
  }

  /**
   * Runs {@code body} inside a real flush suspension. {@code suspendFlushAndExecute} logs and swallows whatever its
   * callback throws, so the failure is carried out and rethrown here instead of passing the test silently.
   */
  private static void runSuspended(final DatabaseInternal db, final CallableNoReturn body) throws Exception {
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final AtomicBoolean ran = new AtomicBoolean();
    db.getPageManager().suspendFlushAndExecute(db, () -> {
      try {
        body.call();
        ran.set(true);
      } catch (final Throwable t) {
        failure.set(t);
      }
    });
    if (failure.get() instanceof Error error)
      throw error;
    if (failure.get() instanceof Exception exception)
      throw exception;
    assertThat(ran.get()).as("the suspended body must have run to completion").isTrue();
  }

  /** Commits inside the suspension and waits for the flush thread to defer the resulting batch. */
  private static void commitDeferredRecords(final DatabaseInternal db) {
    db.transaction(() -> {
      for (int i = 0; i < RECORDS; i++)
        db.newDocument("Doc").set("id", i).save();
    });

    final long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(ASSERTION_TIMEOUT_SEC);
    while (db.getPageManager().getDeferredRAMBytesOf(db) == 0 && System.currentTimeMillis() < deadline)
      try {
        Thread.sleep(5);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    assertThat(db.getPageManager().getDeferredRAMBytesOf(db)).as("the commit's pages must be deferred by the suspension")
        .isPositive();
  }

  private static List<WALFile> activeWALFiles(final DatabaseInternal db) {
    try {
      final Field f = TransactionManager.class.getDeclaredField("activeWALFilePool");
      f.setAccessible(true);
      final List<WALFile> files = new ArrayList<>();
      for (final WALFile file : (WALFile[]) f.get(db.getTransactionManager()))
        if (file != null)
          files.add(file);
      return files;
    } catch (final ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }

  private static long pendingAcks(final List<WALFile> walFiles) {
    long total = 0;
    for (final WALFile file : walFiles)
      total += file.getPendingPagesToFlush();
    return total;
  }
}
