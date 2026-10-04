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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.utility.LockManager;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.arcadedb.engine.Issue8640ApplyChangesRecountRaceTest.applyInBackground;
import static com.arcadedb.engine.Issue8640ApplyChangesRecountRaceTest.buildWalTransaction;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #8649, the follow-up of #8640: once a replicated apply timed out behind a long
 * {@code count()} recompute, the bucket is marked contended and the following applies only try its lock for 1ms. Each
 * of them writes without the lock and refuses the recompute it overlaps, so under sustained replication every recompute
 * was refused, the counter stayed unknown and every {@code count(*)} on the follower was a full scan, with nothing but
 * a FINE log line to show for it.
 * <p>
 * Two refused recomputes in a row now turn the applies patient: they wait for a running recompute again, bounded by
 * the length of the last scan, so the next recompute gets a quiet window and publishes. Refused publishes are counted
 * per bucket and per database.
 */
class Issue8649PatientApplyLockTest extends TestHelper {
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
    try {
      final long start = System.nanoTime();
      applyInBackground(db, fileId, 5, 8654);
      // A lower bound only, which a JVM stall can only make more true: the wait was 100 + 2 x 150 ms, not the 100ms
      // commit timeout alone
      assertThat(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start)).isGreaterThanOrEqualTo(400L);
      assertThat(bucket.isApplyLockPatient()).isFalse();
    } finally {
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
