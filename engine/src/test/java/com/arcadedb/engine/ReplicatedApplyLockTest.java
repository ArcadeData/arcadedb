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
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.arcadedb.engine.ReplicatedApplyLockTest.RecountRace.applyInBackground;
import static com.arcadedb.engine.ReplicatedApplyLockTest.RecountRace.buildWalTransaction;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Locking of {@code TransactionManager.applyChanges} on a replica: per-page IO lock (issue #4712), the bucket-lock recount
 * race (issue #8640) and the patient apply lock (issue #8649). Each section keeps its own fixture in a nested class.
 */
class ReplicatedApplyLockTest {
  // Regression test for issue #4712: {@link TransactionManager#applyChanges} wrote a replicated/recovery page directly via
  // {@code file.write(modifiedPage)}, bypassing the per-page I/O interlock ({@code concurrentPageAccess}) that
  // {@link PageManager#flushPage} and the read path ({@code loadPage}) use to avoid partial reads/writes. A reader loading the same
  // page concurrently with a {@code forceApply} replay could therefore observe partially-written bytes.
  // <p>
  // The fix routes the write through {@link PageManager#writePageWithLock}, which acquires the same interlock. This test verifies the
  // write now blocks while the interlock is held by a (simulated) in-flight reader and completes only once it is released. With the
  // old direct {@code file.write} the write would complete immediately regardless of the interlock.
  @Nested
  class PerPageIoLock extends TestHelper {
    @Override
    protected void beginTest() {
      database.getSchema().createDocumentType("TestType");
    }

    @Test
    @SuppressWarnings("unchecked")
    void applyChangesRespectsPerPageIoLock() throws Exception {
      final DatabaseInternal db = (DatabaseInternal) database;

      db.transaction(() -> {
        for (int i = 0; i < 10; i++)
          db.newDocument("TestType").set("name", "record-" + i).save();
      });

      final int fileId = db.getSchema().getType("TestType").getBuckets(false).get(0).getFileId();
      final PaginatedComponentFile file = (PaginatedComponentFile) db.getFileManager().getFile(fileId);
      final int pageSize = file.getPageSize();
      final PageId pageId = new PageId(db, fileId, 0);
      final PageManager pageManager = db.getPageManager();

      // Pre-load the page into the read cache so applyChanges' internal version read is served from cache and does
      // not itself touch the interlock; the only contention point is then the replicated page write.
      final ImmutablePage page = pageManager.getImmutablePage(pageId, pageSize, false, true);
      final int baseVersion = (int) page.getVersion();

      // Full-content WAL page bumped by one version: this is the replicated/recovery write that applyChanges performs.
      final WALFile.WALPage walPage = new WALFile.WALPage();
      walPage.fileId = fileId;
      walPage.pageNumber = 0;
      walPage.currentPageVersion = baseVersion + 1;
      walPage.changesFrom = BasePage.PAGE_HEADER_SIZE;
      walPage.changesTo = BasePage.PAGE_HEADER_SIZE + 10;
      walPage.currentPageSize = page.getContentSize();
      final byte[] content = new byte[11];
      System.arraycopy(page.getContent().array(), walPage.changesFrom, content, 0, content.length);
      walPage.currentContent = new Binary(content);

      final WALFile.WALTransaction walTx = new WALFile.WALTransaction();
      walTx.txId = 47120;
      walTx.timestamp = System.currentTimeMillis();
      walTx.pages = new WALFile.WALPage[] { walPage };

      // Simulate a reader/flusher holding the per-page interlock: concurrentPageAccess registers the pageId in
      // pendingFlushPages for the whole I/O operation. While it is present, the replicated write inside applyChanges
      // must wait. With the old direct file.write it would proceed immediately (the bug), exposing partial bytes.
      final Field field = PageManager.class.getDeclaredField("pendingFlushPages");
      field.setAccessible(true);
      final ConcurrentMap<PageId, Boolean> pendingFlushPages = (ConcurrentMap<PageId, Boolean>) field.get(pageManager);
      pendingFlushPages.put(pageId, Boolean.FALSE); // FALSE = read access in flight

      final CountDownLatch applyDone = new CountDownLatch(1);
      final AtomicReference<Throwable> error = new AtomicReference<>();
      final Thread applier = new Thread(() -> {
        try {
          db.getTransactionManager().applyChanges(walTx, Collections.emptyMap(), false);
        } catch (final Throwable t) {
          error.set(t);
        } finally {
          applyDone.countDown();
        }
      }, "issue4712-applier");
      applier.start();

      try {
        // While the interlock is occupied applyChanges must NOT complete (the page write spins waiting for the lock).
        assertThat(applyDone.await(500, TimeUnit.MILLISECONDS)).isFalse();
      } finally {
        // Release the interlock: the write can now proceed.
        pendingFlushPages.remove(pageId);
      }

      // A generous bound, not a tight one: this only needs to be "clearly longer than the applier could ever take
      // once unblocked" (a single yield-loop retry + a tiny page write). On a constrained/loaded CI runner sharing
      // one JVM fork with the rest of the unit-test suite, 5s occasionally was not enough headroom for the applier
      // thread to get scheduled promptly, causing intermittent failures unrelated to the interlock logic itself.
      assertThat(applyDone.await(20, TimeUnit.SECONDS)).isTrue();
      applier.join(20_000);
      assertThat(error.get()).isNull();

      // The new version was durably written and reloads correctly from disk.
      pageManager.removePageFromCache(pageId);
      final ImmutablePage reloaded = pageManager.getImmutablePage(pageId, pageSize, false, true);
      assertThat(reloaded.getVersion()).isEqualTo(baseVersion + 1);
    }

    @Test
    void applyChangesStillReplaysPageThroughLockedWrite() throws Exception {
      final DatabaseInternal db = (DatabaseInternal) database;

      db.transaction(() -> {
        for (int i = 0; i < 10; i++)
          db.newDocument("TestType").set("name", "record-" + i).save();
      });

      final int fileId = db.getSchema().getType("TestType").getBuckets(false).get(0).getFileId();
      final PaginatedComponentFile file = (PaginatedComponentFile) db.getFileManager().getFile(fileId);
      final int pageSize = file.getPageSize();
      final PageId pageId = new PageId(db, fileId, 0);

      final ImmutablePage page = db.getPageManager().getImmutablePage(pageId, pageSize, false, true);
      final int baseVersion = (int) page.getVersion();

      // Full-content WAL page so the apply is accepted, carrying a recognizable marker byte at the start of the content area.
      final WALFile.WALPage walPage = new WALFile.WALPage();
      walPage.fileId = fileId;
      walPage.pageNumber = 0;
      walPage.currentPageVersion = baseVersion + 1;
      walPage.changesFrom = BasePage.PAGE_HEADER_SIZE;
      walPage.changesTo = BasePage.PAGE_HEADER_SIZE + 10;
      walPage.currentPageSize = page.getContentSize();
      final byte[] content = new byte[11];
      System.arraycopy(page.getContent().array(), walPage.changesFrom, content, 0, content.length);
      walPage.currentContent = new Binary(content);

      final WALFile.WALTransaction walTx = new WALFile.WALTransaction();
      walTx.txId = 4712;
      walTx.timestamp = System.currentTimeMillis();
      walTx.pages = new WALFile.WALPage[] { walPage };

      final boolean changed = db.getTransactionManager().applyChanges(walTx, Collections.emptyMap(), false);
      assertThat(changed).isTrue();

      db.getPageManager().removePageFromCache(pageId);
      final ImmutablePage reloaded = db.getPageManager().getImmutablePage(pageId, pageSize, false, true);
      assertThat(reloaded.getVersion()).isEqualTo(baseVersion + 1);
    }
  }

  // Regression test for issue #8640: after a leader snapshot install a follower's bucket counters are unknown (-1, the
  // snapshot ships no {@code statistics.json}), so the first {@code count(*)} recomputes them by scanning the pages. That
  // recompute is made mutually exclusive with a LOCAL commit by holding the bucket's file lock across scan and publish
  // (#5152), but a replicated entry is applied by {@link TransactionManager#applyChanges}, which took no file lock: the
  // scan could see some pages of an entry and not others, and the fold (skipped while the counter is -1) could land
  // before or after the publish. The stored counter then differed from the real records for good.
  // <p>
  // The apply of an entry that carries a record-count delta for a bucket whose counter is unknown must therefore wait for
  // a recompute holding that bucket's file lock, and fold on top of the value it published.
  @Nested
  class RecountRace extends TestHelper {
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

  // Regression test for issue #8649, the follow-up of #8640: once a replicated apply timed out behind a long
  // {@code count()} recompute, the bucket is marked contended and the following applies only try its lock for 1ms. Each
  // of them writes without the lock and refuses the recompute it overlaps, so under sustained replication every recompute
  // was refused, the counter stayed unknown and every {@code count(*)} on the follower was a full scan, with nothing but
  // a FINE log line to show for it.
  // <p>
  // Two refused recomputes in a row now turn the applies patient: they wait for a running recompute again, bounded by
  // the length of the last scan, so the next recompute gets a quiet window and publishes. Refused publishes are counted
  // per bucket and per database.
  @Nested
  class PatientApplyLock extends TestHelper {
    @Override
    protected boolean isCheckingDatabaseIntegrity() {
      return false;
    }

    @Override
    protected void beginTest() {
      database.getSchema().createDocumentType("Counted");
      database.transaction(() -> {
        for (int i = 0; i < 10; i++)
          database.newDocument("Counted").set("name", "record-" + i).save();
      });
    }

    @Test
    void realTrafficTurnsTheAppliesPatientAndTheRecomputeIsCached() throws Exception {
      final DatabaseInternal db = (DatabaseInternal) database;
      final LocalBucket bucket = bucket();
      final int fileId = bucket.getFileId();

      bucket.setCachedRecordCount(-1);
      final Object previousTimeout = db.getConfiguration().setValue(GlobalConfiguration.COMMIT_LOCK_TIMEOUT, 200L);
      final ExecutorService counter = Executors.newSingleThreadExecutor();
      try {
        // Two rounds of what #8649 describes, with no planted state: a recompute outlasts the commit timeout, the entries
        // that land meanwhile apply without the lock, and the recompute is refused. Between the two rounds an entry gets
        // the lock and clears the contended mark, which is why the trigger cannot be the age of that mark.
        long txId = 8649;
        for (int round = 1; round <= 2; round++) {
          final ParkedRecompute recompute = parkRecompute(counter, bucket);
          try {
            applyInBackground(db, fileId, 5, txId++); // waits out the commit timeout, then applies unlocked
            applyInBackground(db, fileId, 5, txId++); // 1ms try, unlocked
            assertThat(bucket.isApplyLockPatient()).isFalse();
          } finally {
            recompute.resume.countDown();
          }
          assertThat(recompute.count.get(30, TimeUnit.SECONDS)).isEqualTo(10);
          assertThat(bucket.getCachedRecordCount()).isEqualTo(-1);
          assertThat(bucket.getConsecutiveRecountPublishesRefused()).isEqualTo(round);

          // The scan is gone: the next entry takes the lock. After one refusal that clears the contended mark; after the
          // second one the entry turns patient instead
          applyInBackground(db, fileId, 5, txId++);
          if (round == 1)
            assertThat(bucket.isApplyLockContended()).isFalse();
        }
        assertThat(bucket.isApplyLockPatient()).isTrue();

        // Third round. A commit timeout the parked scan cannot outlast, so the wait below can only end by the scan ending
        db.getConfiguration().setValue(GlobalConfiguration.COMMIT_LOCK_TIMEOUT, 60_000L);
        final ParkedRecompute recompute = parkRecompute(counter, bucket);
        final ExecutorService applier = Executors.newSingleThreadExecutor();
        try {
          final Future<Boolean> apply = applier.submit(() -> db.getTransactionManager()
              .applyChanges(buildWalTransaction(db, fileId, currentVersion(db, fileId) + 1, 8660), Map.of(fileId, 5), false));

          // A short wait expected to TIME OUT: the patient apply is parked on the lock instead of writing under the scan
          assertThatThrownBy(() -> apply.get(500, TimeUnit.MILLISECONDS)).isInstanceOf(TimeoutException.class);
          assertThat(bucket.isApplyLockPatient()).isTrue();

          recompute.resume.countDown();
          assertThat(recompute.count.get(30, TimeUnit.SECONDS)).isEqualTo(10);
          assertThat(apply.get(30, TimeUnit.SECONDS)).isTrue();
        } finally {
          recompute.resume.countDown();
          applier.shutdownNow();
        }

        // Nothing wrote under the scan, so it was cached, and the apply folded on top of it
        assertThat(bucket.getCachedRecordCount()).isEqualTo(15);
        assertThat(bucket.getRecountPublishesRefused()).isEqualTo(2);
        assertThat(bucket.getConsecutiveRecountPublishesRefused()).isZero();
        // The publish ends the patient phase: with a known counter the applies skip the lock again
        assertThat(bucket.isApplyLockPatient()).isFalse();
        assertThat(bucket.isApplyLockContended()).isFalse();
      } finally {
        LocalBucket.recountScanHookForTesting = null;
        counter.shutdownNow();
        db.getConfiguration().setValue(GlobalConfiguration.COMMIT_LOCK_TIMEOUT, previousTimeout);
      }
    }

    @Test
    void aSingleRefusalDoesNotMakeTheAppliesPatient() throws Exception {
      final DatabaseInternal db = (DatabaseInternal) database;
      final LocalBucket bucket = bucket();
      bucket.setCachedRecordCount(-1);

      // One refused recompute is the normal cost of a catch-up
      bucket.invalidateCachedRecordCountForUnlockedApply();
      assertThat(bucket.publishRecomputedCount(10, bucket.getUnlockedApplyStamp() - 1)).isFalse();

      applyInBackground(db, bucket.getFileId(), 5, 8661);
      assertThat(bucket.isApplyLockPatient()).isFalse();
    }

    @Test
    void aFreshContendedMarkStillAppliesWithoutTheLockAndTheRefusalIsCounted() throws Exception {
      final DatabaseInternal db = (DatabaseInternal) database;
      final LocalBucket bucket = bucket();
      final int fileId = bucket.getFileId();

      bucket.setCachedRecordCount(-1);
      // A commit timeout long enough that the mark set below cannot age while the test runs
      final Object previousTimeout = db.getConfiguration().setValue(GlobalConfiguration.COMMIT_LOCK_TIMEOUT, 600_000L);
      bucket.setApplyLockContended(true);
      final long refusedInDatabase = (Long) db.getStats().get("recountPublishesRefused");

      final CountDownLatch scanning = new CountDownLatch(1);
      final CountDownLatch resume = new CountDownLatch(1);
      LocalBucket.recountScanHookForTesting = parkingHook(scanning, resume);

      final ExecutorService counter = Executors.newSingleThreadExecutor();
      try {
        final Future<Long> count = counter.submit(bucket::count);
        assertThat(scanning.await(30, TimeUnit.SECONDS)).isTrue();

        // The #8640 catch-up behaviour is unchanged: the entry does not queue behind the scan ...
        applyInBackground(db, fileId, 5, 8650);
        assertThat(bucket.isApplyLockPatient()).isFalse();

        resume.countDown();
        assertThat(count.get(30, TimeUnit.SECONDS)).isEqualTo(10);
        // ... so the scan is refused, and that is now counted where an operator can see it
        assertThat(bucket.getCachedRecordCount()).isEqualTo(-1);
        assertThat(bucket.getRecountPublishesRefused()).isEqualTo(1);
        assertThat(bucket.getConsecutiveRecountPublishesRefused()).isEqualTo(1);
        assertThat((Long) db.getStats().get("recountPublishesRefused")).isEqualTo(refusedInDatabase + 1);
      } finally {
        LocalBucket.recountScanHookForTesting = null;
        resume.countDown();
        counter.shutdownNow();
        db.getConfiguration().setValue(GlobalConfiguration.COMMIT_LOCK_TIMEOUT, previousTimeout);
      }
    }

    @Test
    void aPatientWaitThatTimesOutMarksTheBucketContendedAgain() throws Exception {
      final DatabaseInternal db = (DatabaseInternal) database;
      final LocalBucket bucket = bucket();
      final int fileId = bucket.getFileId();
      final TransactionManager txManager = db.getTransactionManager();

      bucket.setCachedRecordCount(-1);
      final Object previousTimeout = db.getConfiguration().setValue(GlobalConfiguration.COMMIT_LOCK_TIMEOUT, 200L);
      bucket.setApplyLockContended(true);
      bucket.setApplyLockPatient(true);
      bucket.setLastRecountScanMs(0);

      // A recompute that outlasts even the patient wait
      final Object recompute = new Object();
      assertThat(txManager.tryLockFile(fileId, 1000, recompute)).isEqualTo(LockManager.LOCK_STATUS.YES);
      // What count() does once it holds the lock: tells the applies a recompute, not a commit, is the holder
      bucket.recountTookLock();
      try {
        final long stampBefore = bucket.getUnlockedApplyStamp();
        final long before = System.nanoTime();
        applyInBackground(db, fileId, 5, 8651);

        // Applied anyway and counted as unlocked, and back to 1ms tries
        assertThat(bucket.getUnlockedApplyStamp()).isGreaterThan(stampBefore);
        assertThat(bucket.isApplyLockPatient()).isFalse();
        assertThat(bucket.isApplyLockContended()).isTrue();
        // The timeout started a back-off, which runs past the moment the apply began whatever the scheduler did since
        assertThat(bucket.isApplyLockPatientBackingOff(before)).isTrue();

        // While backing off (planted long, so it cannot run out under the test) refusals keep piling up, but the next
        // entry does not stall behind the same scan again
        bucket.startApplyLockPatientBackoff(600_000);
        bucket.invalidateCachedRecordCountForUnlockedApply();
        final long stale = bucket.getUnlockedApplyStamp() - 1;
        bucket.publishRecomputedCount(10, stale);
        bucket.publishRecomputedCount(10, stale);
        applyInBackground(db, fileId, 5, 8653);
        assertThat(bucket.isApplyLockPatient()).isFalse();
      } finally {
        bucket.recountReleasedLock();
        txManager.unlockFile(fileId, recompute);
        db.getConfiguration().setValue(GlobalConfiguration.COMMIT_LOCK_TIMEOUT, previousTimeout);
      }

      // The back-off ends: past it, the refusals make the applies patient again
      assertThat(bucket.isApplyLockPatientBackingOff(System.nanoTime() + TimeUnit.HOURS.toNanos(1))).isFalse();
    }

    @Test
    void aPatientWaitOutlastsTheCommitTimeoutByTwoScans() throws Exception {
      final DatabaseInternal db = (DatabaseInternal) database;
      final LocalBucket bucket = bucket();
      final int fileId = bucket.getFileId();
      final TransactionManager txManager = db.getTransactionManager();

      bucket.setCachedRecordCount(-1);
      final Object previousTimeout = db.getConfiguration().setValue(GlobalConfiguration.COMMIT_LOCK_TIMEOUT, 100L);
      bucket.setApplyLockPatient(true);
      bucket.setLastRecountScanMs(150);

      final Object recompute = new Object();
      assertThat(txManager.tryLockFile(fileId, 1000, recompute)).isEqualTo(LockManager.LOCK_STATUS.YES);
      // What count() does once it holds the lock: tells the applies a recompute, not a commit, is the holder
      bucket.recountTookLock();
      try {
        final long start = System.nanoTime();
        applyInBackground(db, fileId, 5, 8654);
        // A lower bound only, which a JVM stall can only make more true: the wait was 100 + 2 x 150 ms, not the 100ms
        // commit timeout alone
        assertThat(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start)).isGreaterThanOrEqualTo(400L);
        assertThat(bucket.isApplyLockPatient()).isFalse();
      } finally {
        bucket.recountReleasedLock();
        txManager.unlockFile(fileId, recompute);
        db.getConfiguration().setValue(GlobalConfiguration.COMMIT_LOCK_TIMEOUT, previousTimeout);
      }
    }

    @Test
    void aCounterMadeKnownAnotherWayEndsTheEpisode() {
      final LocalBucket bucket = bucket();
      bucket.setCachedRecordCount(-1);
      bucket.setApplyLockContended(true);
      bucket.setApplyLockPatient(true);
      bucket.startApplyLockPatientBackoff(600_000);
      bucket.invalidateCachedRecordCountForUnlockedApply();
      bucket.publishRecomputedCount(10, bucket.getUnlockedApplyStamp() - 1);

      // A statistics load or a CHECK DATABASE reconcile, not a recompute
      bucket.setCachedRecordCount(10);

      // So the next unknown counter does not start out patient, contended or backing off
      assertThat(bucket.isApplyLockPatient()).isFalse();
      assertThat(bucket.isApplyLockContended()).isFalse();
      assertThat(bucket.isApplyLockPatientBackingOff(System.nanoTime())).isFalse();
      assertThat(bucket.getConsecutiveRecountPublishesRefused()).isZero();
      assertThat(bucket.getRecountPublishesRefused()).isEqualTo(1);
    }

    @Test
    void theRefusalRunIsReportedByTheStepThatCountedIt() {
      final LocalBucket bucket = bucket();
      bucket.setCachedRecordCount(-1);
      bucket.invalidateCachedRecordCountForUnlockedApply();
      final long stale = bucket.getUnlockedApplyStamp() - 1;

      // The log decides on these values, not on a later read a concurrent recompute could have moved
      assertThat(bucket.publishRecomputedCountOrRefusalRun(10, stale)).isEqualTo(1);
      assertThat(bucket.publishRecomputedCountOrRefusalRun(10, stale)).isEqualTo(2);
      assertThat(bucket.publishRecomputedCountOrRefusalRun(10, bucket.getUnlockedApplyStamp())).isZero();
      assertThat(bucket.getCachedRecordCount()).isEqualTo(10);
    }

    @Test
    void aBucketTurnsPatientOnlyWhileTheRefusalRunStillHolds() {
      final LocalBucket bucket = bucket();
      bucket.setCachedRecordCount(-1);
      bucket.invalidateCachedRecordCountForUnlockedApply();
      final long stale = bucket.getUnlockedApplyStamp() - 1;
      bucket.publishRecomputedCount(10, stale);
      bucket.publishRecomputedCount(10, stale);

      // A recompute publishes after an apply saw the run but before it turned the bucket patient: it must not turn
      bucket.publishRecomputedCount(10, bucket.getUnlockedApplyStamp());
      assertThat(bucket.turnApplyLockPatientIfRefused(TransactionManager.PATIENT_AFTER_REFUSED_RECOMPUTES)).isZero();
      assertThat(bucket.isApplyLockPatient()).isFalse();

      bucket.invalidateCachedRecordCountForUnlockedApply();
      final long stale2 = bucket.getUnlockedApplyStamp() - 1;
      bucket.publishRecomputedCount(10, stale2);
      bucket.publishRecomputedCount(10, stale2);
      assertThat(bucket.turnApplyLockPatientIfRefused(TransactionManager.PATIENT_AFTER_REFUSED_RECOMPUTES)).isEqualTo(2);
      assertThat(bucket.isApplyLockPatient()).isTrue();
    }

    @Test
    void aRunOfRefusalsIsLoggedAtWarningOnlyOnPowersOfTwoFromFour() {
      assertThat(LocalBucket.isRefusalRunWorthAWarning(1)).isFalse();
      assertThat(LocalBucket.isRefusalRunWorthAWarning(2)).isFalse();
      assertThat(LocalBucket.isRefusalRunWorthAWarning(3)).isFalse();
      assertThat(LocalBucket.isRefusalRunWorthAWarning(4)).isTrue();
      assertThat(LocalBucket.isRefusalRunWorthAWarning(5)).isFalse();
      assertThat(LocalBucket.isRefusalRunWorthAWarning(7)).isFalse();
      assertThat(LocalBucket.isRefusalRunWorthAWarning(8)).isTrue();
      assertThat(LocalBucket.isRefusalRunWorthAWarning(12)).isFalse();
      assertThat(LocalBucket.isRefusalRunWorthAWarning(1024)).isTrue();
    }

    @Test
    void aPatientApplyThatGetsTheLockStaysPatientUntilAPublish() throws Exception {
      final DatabaseInternal db = (DatabaseInternal) database;
      final LocalBucket bucket = bucket();
      final int fileId = bucket.getFileId();

      bucket.setCachedRecordCount(-1);
      bucket.setApplyLockContended(true);
      bucket.setApplyLockPatient(true);

      applyInBackground(db, fileId, 5, 8652);
      // Dropping back to the plain timeout would let the next long scan time an apply out and refuse itself again
      assertThat(bucket.isApplyLockPatient()).isTrue();

      // Two refused recomputes, then one that publishes: the run resets, the total stays
      bucket.invalidateCachedRecordCountForUnlockedApply();
      final long stale = bucket.getUnlockedApplyStamp() - 1;
      assertThat(bucket.publishRecomputedCount(10, stale)).isFalse();
      assertThat(bucket.publishRecomputedCount(10, stale)).isFalse();
      assertThat(bucket.getConsecutiveRecountPublishesRefused()).isEqualTo(2);

      assertThat(bucket.count()).isEqualTo(10);
      assertThat(bucket.getCachedRecordCount()).isEqualTo(10);
      assertThat(bucket.isApplyLockPatient()).isFalse();
      assertThat(bucket.isApplyLockContended()).isFalse();
      assertThat(bucket.getConsecutiveRecountPublishesRefused()).isZero();
      assertThat(bucket.getRecountPublishesRefused()).isEqualTo(2);
    }

    @Test
    void theWaitsAreSizedOnTheLastScan() {
      final LocalBucket bucket = bucket();

      // No scan measured yet: the commit timeout bounds both
      bucket.setLastRecountScanMs(0);
      assertThat(TransactionManager.patientBackoffMs(bucket, 5_000)).isEqualTo(5_000);
      assertThat(TransactionManager.patientWaitMs(bucket, 5_000)).isEqualTo(5_000);

      // A 10s scan: the back-off lasts four scans, and a patient apply outwaits two of them on top of the timeout
      bucket.setLastRecountScanMs(10_000);
      assertThat(TransactionManager.patientBackoffMs(bucket, 5_000)).isEqualTo(40_000);
      assertThat(TransactionManager.patientWaitMs(bucket, 5_000)).isEqualTo(25_000);
    }

    @Test
    void aRecomputeUnderTheLockRecordsHowLongItsScanTook() {
      final LocalBucket bucket = bucket();
      bucket.setCachedRecordCount(-1);
      bucket.setLastRecountScanMs(-1);

      assertThat(bucket.count()).isEqualTo(10);
      assertThat(bucket.getLastRecountScanMs()).isGreaterThanOrEqualTo(0L);
    }

    private LocalBucket bucket() {
      return (LocalBucket) database.getSchema().getType("Counted").getBuckets(false).getFirst();
    }

    private static int currentVersion(final DatabaseInternal db, final int fileId) throws Exception {
      final PaginatedComponentFile file = (PaginatedComponentFile) db.getFileManager().getFile(fileId);
      final PageId pageId = new PageId(db, fileId, 0);
      db.getPageManager().removePageFromCache(pageId);
      return (int) db.getPageManager().getImmutablePage(pageId, file.getPageSize(), false, true).getVersion();
    }

    private record ParkedRecompute(Future<Long> count, CountDownLatch resume) {
    }

    /** Starts a real count() recompute and returns once it holds the bucket lock and has read its stamp. */
    private static ParkedRecompute parkRecompute(final ExecutorService counter, final LocalBucket bucket) throws Exception {
      final CountDownLatch scanning = new CountDownLatch(1);
      final CountDownLatch resume = new CountDownLatch(1);
      LocalBucket.recountScanHookForTesting = parkingHook(scanning, resume);
      final Future<Long> count = counter.submit(bucket::count);
      assertThat(scanning.await(30, TimeUnit.SECONDS)).isTrue();
      return new ParkedRecompute(count, resume);
    }

    private static Runnable parkingHook(final CountDownLatch scanning, final CountDownLatch resume) {
      return () -> {
        scanning.countDown();
        try {
          resume.await(30, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      };
    }
  }
}
