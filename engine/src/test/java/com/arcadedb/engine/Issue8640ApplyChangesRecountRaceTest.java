/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.utility.LockManager;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #8640: after a leader snapshot install a follower's bucket counters are unknown (-1, the
 * snapshot ships no {@code statistics.json}), so the first {@code count(*)} recomputes them by scanning the pages. That
 * recompute is made mutually exclusive with a LOCAL commit by holding the bucket's file lock across scan and publish
 * (#5152), but a replicated entry is applied by {@link TransactionManager#applyChanges}, which took no file lock: the
 * scan could see some pages of an entry and not others, and the fold (skipped while the counter is -1) could land
 * before or after the publish. The stored counter then differed from the real records for good.
 * <p>
 * The apply of an entry that carries a record-count delta for a bucket whose counter is unknown must therefore wait for
 * a recompute holding that bucket's file lock, and fold on top of the value it published.
 */
class Issue8640ApplyChangesRecountRaceTest extends TestHelper {
  /** The test plants counters on purpose, so the blanket end-of-test integrity check would report them. */
  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    return false;
  }

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType("Counted");
  }

  @Test
  void applyWaitsForARecomputeHoldingTheBucketLockAndFoldsOnItsPublishedValue() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;

    db.transaction(() -> {
      for (int i = 0; i < 10; i++)
        db.newDocument("Counted").set("name", "record-" + i).save();
    });

    final LocalBucket bucket = (LocalBucket) db.getSchema().getType("Counted").getBuckets(false).getFirst();
    final int fileId = bucket.getFileId();
    final TransactionManager txManager = db.getTransactionManager();

    // The state a snapshot install leaves behind: counter unknown.
    bucket.setCachedRecordCount(-1);
    // Generous, so a JVM stall while the test parks cannot turn the wait into a timeout (#6260)
    final Object previousTimeout = db.getConfiguration().setValue(GlobalConfiguration.COMMIT_LOCK_TIMEOUT, 60_000L);

    // A count() recompute in flight: it owns the bucket's file lock from before its scan until after its publish.
    final Object recompute = new Object();
    assertThat(txManager.tryLockFile(fileId, 1000, recompute)).isEqualTo(LockManager.LOCK_STATUS.YES);
    // What count() does once it holds the lock: tells the applies a recompute, not a commit, is the holder
    bucket.recountTookLock();

    final PaginatedComponentFile file = (PaginatedComponentFile) db.getFileManager().getFile(fileId);
    final PageId pageId = new PageId(db, fileId, 0);
    final long version = db.getPageManager().getImmutablePage(pageId, file.getPageSize(), false, true).getVersion();

    final ExecutorService applier = Executors.newSingleThreadExecutor();
    boolean released = false;
    try {
      final CountDownLatch started = new CountDownLatch(1);
      final Future<Boolean> apply = applier.submit(() -> {
        started.countDown();
        return txManager.applyChanges(buildWalTransaction(db, fileId, (int) version + 1, 8640), Map.of(fileId, 5), false);
      });
      assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();

      // A short wait that is expected to TIME OUT: the apply is parked on the bucket lock.
      assertThatThrownBy(() -> apply.get(500, TimeUnit.MILLISECONDS)).isInstanceOf(TimeoutException.class);

      // The recompute publishes what it scanned and lets go of the lock.
      bucket.setCachedRecordCount(10);
      bucket.recountReleasedLock();
      txManager.unlockFile(fileId, recompute);
      released = true;

      assertThat(apply.get(30, TimeUnit.SECONDS)).isTrue();
      // The fold ran AFTER the publish, on top of it: not skipped against a -1 counter, not lost under the publish.
      assertThat(bucket.getCachedRecordCount()).isEqualTo(15);
    } finally {
      if (!released) {
        bucket.recountReleasedLock();
        txManager.unlockFile(fileId, recompute);
      }
      applier.shutdownNow();
      db.getConfiguration().setValue(GlobalConfiguration.COMMIT_LOCK_TIMEOUT, previousTimeout);
    }
  }

  @Test
  void applyDoesNotTakeTheBucketLockWhenTheCounterIsKnown() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;

    db.transaction(() -> {
      for (int i = 0; i < 10; i++)
        db.newDocument("Counted").set("name", "record-" + i).save();
    });

    final LocalBucket bucket = (LocalBucket) db.getSchema().getType("Counted").getBuckets(false).getFirst();
    final int fileId = bucket.getFileId();
    final TransactionManager txManager = db.getTransactionManager();

    final long baseline = bucket.count();
    assertThat(bucket.getCachedRecordCount()).isEqualTo(baseline);

    // Some other holder of the file lock (a local commit): the hot path with a known counter must not queue behind it.
    final Object holder = new Object();
    assertThat(txManager.tryLockFile(fileId, 1000, holder)).isEqualTo(LockManager.LOCK_STATUS.YES);
    try {
      final PaginatedComponentFile file = (PaginatedComponentFile) db.getFileManager().getFile(fileId);
      final long version = db.getPageManager().getImmutablePage(new PageId(db, fileId, 0), file.getPageSize(), false, true)
          .getVersion();

      final ExecutorService applier = Executors.newSingleThreadExecutor();
      try {
        final Future<Boolean> apply = applier.submit(
            () -> txManager.applyChanges(buildWalTransaction(db, fileId, (int) version + 1, 8641), Map.of(fileId, 3), false));
        assertThat(apply.get(30, TimeUnit.SECONDS)).isTrue();
      } finally {
        applier.shutdownNow();
      }
      assertThat(bucket.getCachedRecordCount()).isEqualTo(baseline + 3);
    } finally {
      txManager.unlockFile(fileId, holder);
    }
  }

  @Test
  void applyThatCannotTakeTheLockKeepsAnOverlappingRecomputeFromBeingCached() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;

    db.transaction(() -> {
      for (int i = 0; i < 10; i++)
        db.newDocument("Counted").set("name", "record-" + i).save();
    });

    final LocalBucket bucket = (LocalBucket) db.getSchema().getType("Counted").getBuckets(false).getFirst();
    final int fileId = bucket.getFileId();
    final TransactionManager txManager = db.getTransactionManager();

    bucket.setCachedRecordCount(-1);
    final Object previousTimeout = db.getConfiguration().setValue(GlobalConfiguration.COMMIT_LOCK_TIMEOUT, 200L);

    // A recompute that outlasts the commit timeout: it holds the lock and has read the stamp before its scan.
    final Object recompute = new Object();
    assertThat(txManager.tryLockFile(fileId, 1000, recompute)).isEqualTo(LockManager.LOCK_STATUS.YES);
    // What count() does once it holds the lock: tells the applies a recompute, not a commit, is the holder
    bucket.recountTookLock();
    final long stampAtScanStart = bucket.getUnlockedApplyStamp();
    try {
      // The apply gives up on the lock and goes ahead: a committed entry is never dropped.
      applyInBackground(db, fileId, 5, 8642);
      assertThat(bucket.getCachedRecordCount()).isEqualTo(-1);
      assertThat(bucket.isApplyLockContended()).isTrue();

      // The recompute's scan may hold part of that apply, so it must not be published.
      assertThat(bucket.publishRecomputedCount(12, stampAtScanStart)).isFalse();
      assertThat(bucket.getCachedRecordCount()).isEqualTo(-1);

      // The next entry behind the same recompute does not wait for it again, and is still counted as unlocked.
      final long stampBefore = bucket.getUnlockedApplyStamp();
      applyInBackground(db, fileId, 5, 8643);
      assertThat(bucket.getUnlockedApplyStamp()).isGreaterThan(stampBefore);
    } finally {
      bucket.recountReleasedLock();
      txManager.unlockFile(fileId, recompute);
      db.getConfiguration().setValue(GlobalConfiguration.COMMIT_LOCK_TIMEOUT, previousTimeout);
    }

    // Once the recompute is gone the next apply takes the lock and clears the mark.
    applyInBackground(db, fileId, 5, 8644);
    assertThat(bucket.isApplyLockContended()).isFalse();

    // The next count() recomputes from the pages and caches the real value.
    assertThat(bucket.count()).isEqualTo(10);
    assertThat(bucket.getCachedRecordCount()).isEqualTo(10);
  }

  @Test
  void aRealRecomputeOverlappingAnUnlockedApplyIsNotCached() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;

    db.transaction(() -> {
      for (int i = 0; i < 10; i++)
        db.newDocument("Counted").set("name", "record-" + i).save();
    });

    final LocalBucket bucket = (LocalBucket) db.getSchema().getType("Counted").getBuckets(false).getFirst();
    final int fileId = bucket.getFileId();
    bucket.setCachedRecordCount(-1);
    final Object previousTimeout = db.getConfiguration().setValue(GlobalConfiguration.COMMIT_LOCK_TIMEOUT, 200L);

    final CountDownLatch scanning = new CountDownLatch(1);
    final CountDownLatch resume = new CountDownLatch(1);
    LocalBucket.recountScanHookForTesting = () -> {
      scanning.countDown();
      try {
        resume.await(30, TimeUnit.SECONDS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    };

    final ExecutorService counter = Executors.newSingleThreadExecutor();
    try {
      // A real count() recompute: it takes the bucket lock, reads its stamp and parks at the start of its scan.
      final Future<Long> count = counter.submit(bucket::count);
      assertThat(scanning.await(30, TimeUnit.SECONDS)).isTrue();

      // A replicated entry lands meanwhile: the lock is held by the scan, so it applies without it.
      applyInBackground(db, fileId, 5, 8645);

      resume.countDown();
      assertThat(count.get(30, TimeUnit.SECONDS)).isEqualTo(10);
      // The scan overlapped the unlocked apply, so its result is returned but NOT cached.
      assertThat(bucket.getCachedRecordCount()).isEqualTo(-1);
    } finally {
      LocalBucket.recountScanHookForTesting = null;
      resume.countDown();
      counter.shutdownNow();
      db.getConfiguration().setValue(GlobalConfiguration.COMMIT_LOCK_TIMEOUT, previousTimeout);
    }

    // With nothing overlapping, the next recompute caches.
    assertThat(bucket.count()).isEqualTo(10);
    assertThat(bucket.getCachedRecordCount()).isEqualTo(10);
  }

  @Test
  void aFailingLockPhaseReleasesTheApplyLock() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final LocalBucket bucket = (LocalBucket) db.getSchema().getType("Counted").getBuckets(false).getFirst();
    final int fileId = bucket.getFileId();
    bucket.setCachedRecordCount(-1);

    // A delta map that throws while the lock phase reads it.
    final Map<Integer, Integer> failing = new HashMap<>(Map.of(fileId, 1)) {
      @Override
      public Set<Integer> keySet() {
        throw new IllegalStateException("lock phase failure");
      }
    };

    final ExecutorService applier = Executors.newSingleThreadExecutor();
    try {
      final Future<Boolean> apply = applier.submit(
          () -> db.getTransactionManager().applyChanges(buildWalTransaction(db, fileId, 1, 8646), failing, false));
      assertThatThrownBy(() -> apply.get(30, TimeUnit.SECONDS)).hasRootCauseInstanceOf(IllegalStateException.class);
    } finally {
      applier.shutdownNow();
    }

    // The snapshot t0 barrier (write side) can still be taken, and no file lock was left behind.
    final ReentrantReadWriteLock applyLock = db.getTransactionManager().getApplyLock();
    assertThat(applyLock.writeLock().tryLock(10, TimeUnit.SECONDS)).isTrue();
    applyLock.writeLock().unlock();
    final Object other = new Object();
    assertThat(db.getTransactionManager().tryLockFile(fileId, 1000, other)).isEqualTo(LockManager.LOCK_STATUS.YES);
    db.getTransactionManager().unlockFile(fileId, other);
  }

  @Test
  void aSuccessfulRecomputeClearsTheContendedMark() {
    final LocalBucket bucket = (LocalBucket) database.getSchema().getType("Counted").getBuckets(false).getFirst();
    bucket.setCachedRecordCount(-1);
    bucket.setApplyLockContended(true);

    assertThat(bucket.publishRecomputedCount(0, bucket.getUnlockedApplyStamp())).isTrue();
    assertThat(bucket.isApplyLockContended()).isFalse();
  }

  static void applyInBackground(final DatabaseInternal db, final int fileId, final int delta, final long txId)
      throws Exception {
    final PaginatedComponentFile file = (PaginatedComponentFile) db.getFileManager().getFile(fileId);
    final PageId pageId = new PageId(db, fileId, 0);
    db.getPageManager().removePageFromCache(pageId);
    final long version = db.getPageManager().getImmutablePage(pageId, file.getPageSize(), false, true).getVersion();
    final ExecutorService applier = Executors.newSingleThreadExecutor();
    try {
      final Future<Boolean> apply = applier.submit(() -> db.getTransactionManager()
          .applyChanges(buildWalTransaction(db, fileId, (int) version + 1, txId), Map.of(fileId, delta), false));
      assertThat(apply.get(30, TimeUnit.SECONDS)).isTrue();
    } finally {
      applier.shutdownNow();
    }
  }

  @Test
  void anApplyWithoutTheLockDiscardsARecomputePublishedWhileItRan() {
    final LocalBucket bucket = (LocalBucket) database.getSchema().getType("Counted").getBuckets(false).getFirst();
    bucket.setCachedRecordCount(-1);

    // Scan started, then the apply (unable to lock) invalidates before writing its pages ...
    final long stamp = bucket.getUnlockedApplyStamp();
    bucket.invalidateCachedRecordCountForUnlockedApply();
    assertThat(bucket.publishRecomputedCount(7, stamp)).isFalse();

    // ... and a scan that started after that first call and published is thrown away by the call after the fold.
    final long later = bucket.getUnlockedApplyStamp();
    assertThat(bucket.publishRecomputedCount(7, later)).isTrue();
    bucket.invalidateCachedRecordCountForUnlockedApply();
    assertThat(bucket.getCachedRecordCount()).isEqualTo(-1);
  }

  static WALFile.WALTransaction buildWalTransaction(final DatabaseInternal db, final int fileId,
      final int targetVersion, final long txId) throws Exception {
    final PaginatedComponentFile file = (PaginatedComponentFile) db.getFileManager().getFile(fileId);
    final PageId pageId = new PageId(db, fileId, 0);
    final ImmutablePage page = db.getPageManager().getImmutablePage(pageId, file.getPageSize(), false, true);

    final WALFile.WALPage walPage = new WALFile.WALPage();
    walPage.fileId = fileId;
    walPage.pageNumber = 0;
    walPage.currentPageVersion = targetVersion;
    walPage.changesFrom = BasePage.PAGE_HEADER_SIZE;
    walPage.changesTo = BasePage.PAGE_HEADER_SIZE + 10;
    walPage.currentPageSize = page.getContentSize();

    final byte[] content = new byte[walPage.changesTo - walPage.changesFrom + 1];
    System.arraycopy(page.getContent().array(), walPage.changesFrom, content, 0, content.length);
    walPage.currentContent = new Binary(content);

    final WALFile.WALTransaction walTx = new WALFile.WALTransaction();
    walTx.txId = txId;
    walTx.timestamp = System.currentTimeMillis();
    walTx.pages = new WALFile.WALPage[] { walPage };
    return walTx;
  }
}
