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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Group commit ({@code arcadedb.txWalGroupCommit}): with a WAL flush configured, the commit appends its WAL without
 * flushing it under the file locks and waits for a flush shared with the commits around it before returning.
 * <p>
 * The commits run on new threads: a thread's transaction context reads the setting when it is created, and the test
 * thread's was created with the database, before {@link #beginTest()} turned the setting on.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class WALGroupCommitTest extends TestHelper {
  private static final String TYPE = "GroupCommit";

  @Override
  protected void beginTest() {
    database.getConfiguration().setValue(GlobalConfiguration.TX_WAL_FLUSH, 1);
    database.getConfiguration().setValue(GlobalConfiguration.TX_WAL_GROUP_COMMIT, true);
    final DocumentType type = database.getSchema().createDocumentType(TYPE);
    type.createProperty("id", String.class);
    type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");
  }

  @Test
  void everyCommitWaitsForAFlushOfItsWAL() throws Exception {
    final long commitsBefore = stat("groupCommits");
    final long syncsBefore = stat("groupCommitSyncs");

    runInThreads(1, 50);

    // one at a time nothing can share a flush, and each commit returned only once one covered it
    assertThat(stat("groupCommits") - commitsBefore).isEqualTo(50);
    assertThat(stat("groupCommitSyncs") - syncsBefore).isBetween(1L, 50L);
    assertThat(database.countType(TYPE, false)).isEqualTo(50);
  }

  /** The full flush (txWalFlush 2) goes through the same shared flush, with the file's metadata. */
  @Test
  void fullFlushCommitsAreGroupCommittedToo() throws Exception {
    database.getConfiguration().setValue(GlobalConfiguration.TX_WAL_FLUSH, 2);
    final long commitsBefore = stat("groupCommits");

    runInThreads(4, 10);

    assertThat(stat("groupCommits") - commitsBefore).isEqualTo(40);
    reopenDatabase();
    assertThat(database.countType(TYPE, false)).isEqualTo(40);
  }

  @Test
  void concurrentCommitsSurviveACrash() throws Exception {
    final long commitsBefore = stat("groupCommits");

    runInThreads(8, 50);

    assertThat(stat("groupCommits") - commitsBefore).isEqualTo(400);
    assertThat(stat("groupCommitSyncs")).isPositive();

    // every commit returned with its WAL flushed: recovery must find all of them, the pages included
    ((DatabaseInternal) database).kill();
    // the killed instance is still registered: closing it deregisters it without the flush a clean close would do
    database.close();
    database = factory.open();

    assertThat(database.countType(TYPE, false)).isEqualTo(400);
    database.transaction(() -> {
      for (int t = 0; t < 8; t++)
        for (int i = 0; i < 50; i++)
          assertThat(database.lookupByKey(TYPE, "id", t + "-" + i).hasNext()).isTrue();
    });
  }

  /**
   * Group commits mixed with commits that do not defer their flush on the same type, so each reads pages the others published:
   * a synchronous page flush (its WAL flush is its own, after the pending group commits are flushed) and txWalFlush 0 (its
   * pages wait for the group commits they may carry). All of them must survive a crash and recover without a version gap.
   */
  @Test
  void groupCommitsMixedWithCommitsThatDoNotDeferSurviveACrash() throws Exception {
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final List<Thread> workers = new ArrayList<>();
    for (int t = 0; t < 6; t++) {
      final int thread = t;
      final Thread worker = new Thread(() -> {
        try {
          database.begin();
          if (thread == 4)
            ((DatabaseInternal) database).getTransaction().setAsyncFlush(false);
          else if (thread == 5)
            ((DatabaseInternal) database).getTransaction().setWALFlush(WALFile.FlushType.NO);
          database.rollback();
          for (int i = 0; i < 50; i++) {
            final int item = i;
            database.transaction(() -> database.newDocument(TYPE).set("id", "mixed-" + thread + "-" + item).save());
          }
        } catch (final Throwable e) {
          failure.compareAndSet(null, e);
        }
      });
      workers.add(worker);
      worker.start();
    }
    for (final Thread worker : workers)
      worker.join();
    assertThat(failure.get()).isNull();

    ((DatabaseInternal) database).kill();
    database.close();
    database = factory.open();

    assertThat(database.countType(TYPE, false)).isEqualTo(300);
  }

  /**
   * A flush that fails after the pages were published: the commit must not report success, and the database is fenced so that
   * nothing commits on top of a transaction that may not be durable. The close keeps the WAL (the fenced database's pages
   * were discarded, not written) and the reopen's recovery replays it.
   */
  @Test
  void aFailedFlushFailsTheCommitAndFencesTheDatabase() throws Exception {
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final TransactionManager transactionManager = ((DatabaseInternal) database).getTransactionManager();
    transactionManager.walSyncFaultInjector = file -> {
      throw new IOException("injected WAL flush failure");
    };
    try {
      final Thread worker = new Thread(() -> {
        try {
          database.transaction(() -> database.newDocument(TYPE).set("id", "flush-failure").save());
        } catch (final Throwable e) {
          failure.set(e);
        }
      });
      worker.start();
      worker.join();
    } finally {
      transactionManager.walSyncFaultInjector = null;
    }

    assertThat(failure.get()).isInstanceOf(TransactionException.class).hasMessageContaining("could not be flushed");
    assertThat(((DatabaseInternal) database).isFencedForRecovery()).isTrue();

    database.close();
    database = factory.open();
    assertThat(((DatabaseInternal) database).isFencedForRecovery()).isFalse();
    // looked up, not counted: the cached record count is persisted at close and would answer 1 even for a lost record
    database.transaction(() -> assertThat(database.lookupByKey(TYPE, "id", "flush-failure").hasNext()).isTrue());
  }

  /**
   * Many commits waiting on flush passes that all fail: none of them may report success, every one is released (no thread
   * stays parked on a pass), and whatever they wrote is recovered by the reopen.
   */
  @Test
  void failedFlushesReleaseEveryWaiterWithoutASuccess() throws Exception {
    final TransactionManager transactionManager = ((DatabaseInternal) database).getTransactionManager();
    transactionManager.walSyncFaultInjector = file -> {
      // long enough for the other commits to queue on this pass
      try {
        Thread.sleep(20);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      throw new IOException("injected WAL flush failure");
    };
    final AtomicInteger succeeded = new AtomicInteger();
    final AtomicInteger failed = new AtomicInteger();
    final List<Thread> workers = new ArrayList<>();
    try {
      for (int t = 0; t < 8; t++) {
        final int thread = t;
        final Thread worker = new Thread(() -> {
          try {
            database.transaction(() -> database.newDocument(TYPE).set("id", "fail-" + thread).save());
            succeeded.incrementAndGet();
          } catch (final Throwable e) {
            failed.incrementAndGet();
          }
        });
        workers.add(worker);
        worker.start();
      }
      for (final Thread worker : workers) {
        worker.join(30_000);
        assertThat(worker.isAlive()).as("a commit stayed parked on a failed flush pass").isFalse();
      }
    } finally {
      transactionManager.walSyncFaultInjector = null;
    }

    assertThat(succeeded.get()).isZero();
    assertThat(failed.get()).isEqualTo(8);
    assertThat(((DatabaseInternal) database).isFencedForRecovery()).isTrue();

    database.close();
    database = factory.open();
    assertThat(((DatabaseInternal) database).isFencedForRecovery()).isFalse();
  }

  /**
   * The close of a fenced database keeps the WAL also without group commit (#5053 regime): the reopen recovers the committed
   * records whatever the flush pipeline did with their pages while the database was fenced.
   */
  @Test
  void aFencedDatabaseKeepsItsWALOnCloseWithoutGroupCommit() throws Exception {
    database.getConfiguration().setValue(GlobalConfiguration.TX_WAL_GROUP_COMMIT, false);
    runInThreads(1, 10);

    ((LocalDatabase) ((DatabaseInternal) database).getEmbedded()).fenceForRecovery("test: fenced after the commits");
    database.close();
    assertThat(new File(getDatabasePath()).listFiles((dir, name) -> name.endsWith(".wal"))).isNotEmpty();

    database = factory.open();
    assertThat(((DatabaseInternal) database).isFencedForRecovery()).isFalse();
    database.transaction(() -> {
      for (int i = 0; i < 10; i++)
        assertThat(database.lookupByKey(TYPE, "id", "0-" + i).hasNext()).isTrue();
    });
  }

  /**
   * The WAL rule: a page of a group commit reaches its data file only after the shared flush of its WAL completed. The flush is
   * slowed down so the background page flush gets to the page while it is still running; every page write of this database
   * must then come after a completed flush. Removing the wait in {@code PageManager.flushPage} makes it fail.
   */
  @Test
  void aPageReachesItsDataFileOnlyAfterItsWALIsFlushed() throws Exception {
    final TransactionManager transactionManager = ((DatabaseInternal) database).getTransactionManager();
    // the pages of the schema created in beginTest() must not be flushed while the probe below is installed
    assertThat(PageManager.INSTANCE.waitAllPagesOfDatabaseAreFlushed(database)).isTrue();
    final long syncsBefore = stat("groupCommitSyncs");
    final List<Long> syncsAtPageWrite = Collections.synchronizedList(new ArrayList<>());
    transactionManager.walSyncFaultInjector = file -> {
      try {
        Thread.sleep(300);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    };
    PageManager.setPageWriteFaultInjector(pageId -> {
      if (database.getName().equals(pageId.getDatabase().getName()))
        syncsAtPageWrite.add(stat("groupCommitSyncs") - syncsBefore);
    });
    try {
      runInThreads(1, 1);
      PageManager.INSTANCE.waitAllPagesOfDatabaseAreFlushed(database);
    } finally {
      PageManager.setPageWriteFaultInjector(null);
      transactionManager.walSyncFaultInjector = null;
    }

    assertThat(syncsAtPageWrite).isNotEmpty().allMatch(completedFlushes -> completedFlushes >= 1);
  }

  /** A thread with the synchronous page flush never defers: its pages would reach their data files inside the commit. */
  @Test
  void aSynchronousPageFlushNeverDefers() throws Exception {
    final long commitsBefore = stat("groupCommits");
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final Thread worker = new Thread(() -> {
      try {
        database.begin();
        ((DatabaseInternal) database).getTransaction().setAsyncFlush(false);
        database.rollback();
        for (int i = 0; i < 10; i++) {
          final int item = i;
          database.transaction(() -> database.newDocument(TYPE).set("id", "sync-" + item).save());
        }
      } catch (final Throwable e) {
        failure.set(e);
      }
    });
    worker.start();
    worker.join();

    assertThat(failure.get()).isNull();
    assertThat(stat("groupCommits")).isEqualTo(commitsBefore);
    assertThat(database.countType(TYPE, false)).isEqualTo(10);
  }

  @Test
  void withoutTheSettingNothingIsDeferred() throws Exception {
    database.getConfiguration().setValue(GlobalConfiguration.TX_WAL_GROUP_COMMIT, false);
    final long commitsBefore = stat("groupCommits");

    runInThreads(2, 20);

    assertThat(stat("groupCommits")).isEqualTo(commitsBefore);
    assertThat(database.countType(TYPE, false)).isEqualTo(40);
  }

  @Test
  void aClosedDatabaseKeepsEveryGroupCommit() throws Exception {
    runInThreads(4, 25);

    reopenDatabase();

    assertThat(database.countType(TYPE, false)).isEqualTo(100);
  }

  private void runInThreads(final int threads, final int commitsPerThread) throws Exception {
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final List<Thread> workers = new ArrayList<>();
    for (int t = 0; t < threads; t++) {
      final int thread = t;
      final Thread worker = new Thread(() -> {
        try {
          for (int i = 0; i < commitsPerThread; i++) {
            final int item = i;
            database.transaction(() -> database.newDocument(TYPE).set("id", thread + "-" + item).save());
          }
        } catch (final Throwable e) {
          failure.compareAndSet(null, e);
        }
      });
      workers.add(worker);
      worker.start();
    }
    for (final Thread worker : workers)
      worker.join();
    assertThat(failure.get()).isNull();
  }

  private long stat(final String name) {
    final Map<String, Object> stats = ((DatabaseInternal) database).getTransactionManager().getStats();
    return (Long) stats.get(name);
  }
}
