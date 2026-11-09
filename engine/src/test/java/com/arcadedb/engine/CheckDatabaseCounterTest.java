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
import com.arcadedb.Profiler;
import com.arcadedb.TestHelper;
import com.arcadedb.database.Binary;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.LockManager;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.util.Collection;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * CHECK DATABASE and the cached record counter of a bucket.
 * <ul>
 * <li>#8040: {@code CHECK DATABASE} walks every slot of every bucket, so it holds the exact number {@code count(*)} should be
 * serving - and never compared the two. A counter drifted by the #7126 Raft-replay double-fold therefore produced a perfectly
 * clean check while {@code count(*)} disagreed with a full scan and, in a cluster, with the other replicas. That is
 * indistinguishable from HA state divergence until somebody scans by hand, which is how #8040 was diagnosed in production.</li>
 * <li>#9235 (follow-up of #8649): the {@code count()} recomputes a bucket could not cache because a replicated apply wrote it
 * without its lock reach {@link Profiler}, in the JSON output and in the Prometheus export. Assertions are {@code >=} deltas
 * because {@code Profiler.INSTANCE} is a JVM singleton.</li>
 * <li>#10712: a REPEATABLE_READ transaction pins the COMMITTED part of a recompute scan, so its own writes are counted once.</li>
 * <li>#7126: the bucket record-count delta is folded only when a replayed WAL page is strictly newer than the one on disk, so a Raft
 * entry replayed after a clean restart no longer double-folds the delta and makes {@code count(*)} over-report.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CheckDatabaseCounterTest extends TestHelper {

  private static final String TYPE    = "Counted";
  private static final int    RECORDS = 30;

  /** These tests leave a deliberately wrong counter behind, which the blanket end-of-test check would report. */
  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    return false;
  }

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType(TYPE, 1);
    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++)
        database.newDocument(TYPE).set("id", i).save();
    });
    assertThat(bucket().count()).isEqualTo(RECORDS);
  }

  // #8040
  @Test
  void aReadOnlyCheckReportsAnOverReportingCounterAndLeavesItAlone() {
    // What a double-folded INSERT delta leaves behind on a cleanly-restarted HA node.
    bucket().setCachedRecordCount(RECORDS + 4);

    final Result row = check(false);

    assertThat((Long) row.getProperty("staleRecordCounters")).as("%s", row.toJSON()).isEqualTo(1L);
    assertThat((Long) row.getProperty("staleRecordCountersFixed")).isEqualTo(0L);
    assertThat((Long) row.getProperty("totalErrors")).isGreaterThanOrEqualTo(1L);
    assertThat((Collection<String>) row.getProperty("warnings")).as("%s", row.toJSON())
        .anyMatch(w -> w.contains(bucket().getName()) && w.contains("over-reports by 4")
            && w.contains("CHECK DATABASE FIX"));

    // Read-only means read-only: the counter, and the wrong count(*) it serves, are still there.
    assertThat(bucket().getCachedRecordCount()).isEqualTo(RECORDS + 4L);
  }

  /**
   * The second defect this finding exposed: bucket-level warnings reached the result through {@code addAll}, which
   * put them in the list and told the tally nothing, so a run whose only findings were bucket-level reported zero
   * warnings while listing one - and the {@code maxWarnings} cap that keeps a badly damaged database from OOMing the
   * run did not apply to them either.
   * <p>
   * Pinned on {@code totalWarnings} rather than on the list, because the list is what both versions fill: only the
   * tally tells the fixed path from the broken one.
   */
  // #8040
  @Test
  void aBucketWarningIsCountedAndCapped() {
    bucket().setCachedRecordCount(RECORDS + 4);

    try (final ResultSet rs = database.command("sql", "CHECK DATABASE")) {
      final Result row = rs.next();
      assertThat((Collection<String>) row.getProperty("warnings")).as("%s", row.toJSON()).hasSize(1);
      assertThat((Long) row.getProperty("totalWarnings")).as("%s", row.toJSON()).isEqualTo(1L);
    }

    // And it goes through the cap: a run allowed to retain none retains none, and still counts it.
    final Map<String, Object> capped = new DatabaseChecker(database).setVerboseLevel(0).setMaxWarnings(0).check();

    assertThat((Collection<String>) capped.get("warnings")).as("%s", capped).isEmpty();
    assertThat((Long) capped.get("totalWarnings")).as("%s", capped).isEqualTo(1L);
  }

  /**
   * The direction #7126 predicted is not the one production saw: a delete delta is negative, and double-folding a
   * negative delta subtracts twice. The symptom to look for is a disagreement, not an excess.
   */
  // #8040
  @Test
  void aReadOnlyCheckReportsAnUnderReportingCounterToo() {
    bucket().setCachedRecordCount(RECORDS - 3);

    final Result row = check(false);

    assertThat((Long) row.getProperty("staleRecordCounters")).as("%s", row.toJSON()).isEqualTo(1L);
    assertThat((Collection<String>) row.getProperty("warnings")).as("%s", row.toJSON())
        .anyMatch(w -> w.contains(bucket().getName()) && w.contains("under-reports by 3"));
  }

  // #8040
  @Test
  void checkDatabaseFixRecomputesTheCounter() {
    bucket().setCachedRecordCount(RECORDS + 4);

    final Result row = check(true);

    assertThat((Long) row.getProperty("staleRecordCounters")).as("%s", row.toJSON()).isEqualTo(1L);
    assertThat((Long) row.getProperty("staleRecordCountersFixed")).isEqualTo(1L);

    assertThat(bucket().count()).isEqualTo(RECORDS);
    assertThat(bucket().getCachedRecordCount()).isEqualTo((long) RECORDS);

    // And the repaired database is clean under a second pass.
    final Result second = check(false);
    assertThat((Long) second.getProperty("staleRecordCounters")).as("%s", second.toJSON()).isEqualTo(0L);
  }

  /** A healthy database must not be told its counters are wrong: the comparison has to be exact in both directions. */
  // #8040
  @Test
  void anAccurateCounterIsNotReported() {
    final Result row = check(false);

    assertThat((Long) row.getProperty("staleRecordCounters")).as("%s", row.toJSON()).isEqualTo(0L);
    assertThat((Long) row.getProperty("totalErrors")).isZero();
  }

  /** A counter that was never computed is not wrong, it is unknown: nothing to report and nothing to repair. */
  // #8040
  @Test
  void anUncomputedCounterIsNotReported() {
    bucket().setCachedRecordCount(-1);

    final Result row = check(false);

    assertThat((Long) row.getProperty("staleRecordCounters")).as("%s", row.toJSON()).isEqualTo(0L);
  }

  /**
   * The counter carries the COMMITTED count while the walk sees the transaction's view, so a bucket with a pending
   * delta would differ by that delta alone. Reporting it would make every CHECK DATABASE FIX that deleted a corrupted
   * record accuse the buckets it had just repaired.
   */
  // #8040
  @Test
  void aBucketWithPendingChangesIsNotReported() {
    database.begin();
    try {
      for (int i = 0; i < 5; i++)
        database.newDocument(TYPE).set("id", 1000 + i).save();

      final Result row = check(false);

      assertThat((Long) row.getProperty("staleRecordCounters")).as("%s", row.toJSON()).isEqualTo(0L);
    } finally {
      database.rollback();
    }
  }

  /**
   * The suppression that matters most, and the one nothing pinned directly: a bucket whose counter really IS wrong,
   * on a run whose own walk cannot answer for the tally. A slot the pass cannot even read leaves the tally short by
   * an unknown amount, so the comparison would report a number it did not measure - and on a FIX run it would be
   * accusing the very bucket it is repairing.
   * <p>
   * Both directions of the run are checked: read-only, where the walk simply cannot read the slot, and FIX, where it
   * also repairs it away. The counter is genuinely adrift throughout - the assertion is that the check knows it is
   * not entitled to say so.
   */
  // #8040
  @Test
  void aRunThatCannotAnswerForItsOwnTallyReportsNoDrift() {
    bucket().setCachedRecordCount(RECORDS + 4);
    makeOneSlotUnreadable();

    final Result readOnly = check(false);
    assertThat((Long) readOnly.getProperty("staleRecordCounters")).as("%s", readOnly.toJSON()).isEqualTo(0L);
    // The run did find the slot, so it is not silent about the database - only about the counter.
    assertThat((Long) readOnly.getProperty("totalErrors")).isGreaterThanOrEqualTo(1L);
    assertThat(bucket().getCachedRecordCount()).as("a read-only run repairs nothing").isEqualTo(RECORDS + 4L);

    final Result fixed = check(true);
    assertThat((Long) fixed.getProperty("staleRecordCounters")).as("%s", fixed.toJSON()).isEqualTo(0L);
    assertThat((Long) fixed.getProperty("staleRecordCountersFixed")).isEqualTo(0L);

    // And the counter is right afterwards anyway, through the invalidation every FIX run makes: suppressing the
    // FINDING never means leaving the drift behind.
    assertThat(bucket().count()).isEqualTo(RECORDS - 1L);
  }

  /**
   * Points one slot's offset past the end of its page, which is the one corruption the bucket walk answers with
   * "this slot was never read" rather than with a classification - exactly what makes its tally unusable.
   */
  private void makeOneSlotUnreadable() {
    final DatabaseInternal db = (DatabaseInternal) database;
    final LocalBucket bucket = bucket();
    final int fileId = bucket.getFileId();
    final int pageSize = ((PaginatedComponentFile) db.getFileManager().getFile(fileId)).getPageSize();

    db.transaction(() -> {
      try {
        final MutablePage page = db.getTransaction().getPageToModify(new PageId(db, fileId, 0), pageSize, false);
        // Slot 0 of page 0: the record table starts right after the short record count. The offset written is the
        // whole page size, which no page's content can reach - getContentSize() subtracts the header - so the walk
        // meets it as an offset it cannot read whatever the page happens to hold.
        page.writeUnsignedInt(Binary.SHORT_SERIALIZED_SIZE, pageSize);
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    });
  }

  private Result check(final boolean fix) {
    try (final ResultSet rs = database.command("sql", "CHECK DATABASE" + (fix ? " FIX" : ""))) {
      assertThat(rs.hasNext()).isTrue();
      return rs.next();
    }
  }

  private LocalBucket bucket() {
    return (LocalBucket) database.getSchema().getType(TYPE).getBuckets(false).getFirst();
  }

  private static final String RECOUNT_TYPE = "Issue10712Recount";

  // #10712
  @Test
  void aRecountPinsTheCommittedPartOfTheTransactionView() {
    database.getSchema().createDocumentType(RECOUNT_TYPE);
    database.transaction(() -> {
      for (int i = 0; i < 100; i++)
        database.newDocument(RECOUNT_TYPE).set("id", i).save();
    });
    final LocalBucket bucket = (LocalBucket) database.getSchema().getType(RECOUNT_TYPE).getBuckets(false).getFirst();
    final TransactionManager txManager = ((LocalDatabase) ((DatabaseInternal) database).getEmbedded()).getTransactionManager();

    database.getConfiguration().setValue(GlobalConfiguration.COMMIT_LOCK_TIMEOUT, 100L);
    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    try {
      // THE TRANSACTION'S OWN WRITE: A DELTA OF +1 THE SCAN OF ITS VIEW ALREADY SEES
      database.newDocument(RECOUNT_TYPE).set("id", 1000).save();

      // THE LOCK-FREE FALLBACK: THE COUNTER IS UNKNOWN AND ANOTHER REQUESTER HOLDS THE BUCKET LOCK
      final Object holder = new Object();
      bucket.invalidateCachedRecordCountForUnlockedApply();
      assertThat(txManager.tryLockFile(bucket.getFileId(), 1000, holder)).isEqualTo(LockManager.LOCK_STATUS.YES);
      try {
        assertThat(database.countType(RECOUNT_TYPE, true)).as("the committed 100 plus the transaction's own 1").isEqualTo(101);
      } finally {
        txManager.unlockFile(bucket.getFileId(), holder);
      }

      // THE PATH THAT HOLDS THE LOCK: THE COUNTER WAS NOT PUBLISHED, SO THIS RECOUNTS TOO, AND THE PIN IS WHAT ANSWERS
      assertThat(database.countType(RECOUNT_TYPE, true)).isEqualTo(101);

      // AND THE O(1) PATH ONCE THE COUNTER IS KNOWN AGAIN
      assertThat(database.countType(RECOUNT_TYPE, true)).isEqualTo(101);
    } finally {
      database.rollback();
      database.getConfiguration().setValue(GlobalConfiguration.COMMIT_LOCK_TIMEOUT, GlobalConfiguration.COMMIT_LOCK_TIMEOUT.getDefValue());
    }

    assertThat(database.countType(RECOUNT_TYPE, true)).isEqualTo(100);

    // The class turns the blanket end-of-test integrity check off, so run it here
    final Map<String, Object> check = new DatabaseChecker(database).setVerboseLevel(0).check();
    assertThat((Collection<String>) check.get("warnings")).isEmpty();
  }


  private static final String PROFILER_DB_PATH = "target/databases/issue9235-recount-refusal-profiler";

  // #9235
  @Test
  void refusedRecountsReachTheProfilerAndSurviveTheDatabaseClose() {
    FileUtils.deleteRecursively(new File(PROFILER_DB_PATH));
    final long before = profilerCount();

    try (final DatabaseFactory factory = new DatabaseFactory(PROFILER_DB_PATH)) {
      final Database db = factory.create();
      try {
        db.getSchema().createDocumentType("Counted");
        db.transaction(() -> {
          for (int i = 0; i < 10; i++)
            db.newDocument("Counted").set("name", "record-" + i).save();
        });
        final LocalBucket bucket = (LocalBucket) db.getSchema().getType("Counted").getBuckets(false).getFirst();

        // What an unlocked replicated apply does to a recompute that overlapped it (#8640/#8649): the stamp the scan
        // read at its start is no longer current, so the publish is refused
        for (int i = 0; i < 2; i++) {
          bucket.invalidateCachedRecordCountForUnlockedApply();
          assertThat(bucket.publishRecomputedCount(10, bucket.getUnlockedApplyStamp() - 1)).isFalse();
        }
        assertThat(bucket.getRecountPublishesRefused()).isEqualTo(2);

        final long open = profilerCount();
        assertThat(open).as("the refused recounts of an open database must reach the profiler")
            .isGreaterThanOrEqualTo(before + 2);

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        Profiler.INSTANCE.dumpMetrics(new PrintStream(out));
        assertThat(out.toString()).contains("recountPublishesRefused=");

        db.close();

        // #5636: a monotonic total that went back down on a close would read as a counter reset in Prometheus
        assertThat(profilerCount()).as("closing the database must not rewind the refused-recount total")
            .isGreaterThanOrEqualTo(open);
      } finally {
        // A failed assertion above must not leave the database open for the @AfterEach delete
        if (db.isOpen())
          db.close();
      }
    } finally {
      FileUtils.deleteRecursively(new File(PROFILER_DB_PATH));
    }
  }

  private static long profilerCount() {
    return Profiler.INSTANCE.toJSON().getJSONObject("recountPublishesRefused").getLong("count", -1L);
  }

  // #7126
  @Test
  void replayAtEqualVersionDoesNotDoubleFoldTheRecordCountDelta() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;

    db.transaction(() -> {
      for (int i = 0; i < 10; i++)
        db.newDocument("Counted").set("name", "record-" + i).save();
    });

    final LocalBucket bucket = (LocalBucket) db.getSchema().getType("Counted").getBuckets(false).getFirst();
    // Materialize the cached counter: the fold is a no-op while it is still -1, which is what makes an
    // UNCLEAN restart safe and a CLEAN one not (issue #7126).
    final long baseline = bucket.count();
    assertThat(bucket.getCachedRecordCount()).isEqualTo(baseline);

    final int fileId = bucket.getFileId();
    final PaginatedComponentFile file = (PaginatedComponentFile) db.getFileManager().getFile(fileId);
    final PageId pageId = new PageId(db, fileId, 0);

    // A Raft replay of an entry whose pages are already at their final version: same bytes, same version.
    final long versionBeforeReplay = db.getPageManager().getImmutablePage(pageId, file.getPageSize(), false, true)
        .getVersion();
    final boolean replayChanged = db.getTransactionManager()
        .applyChanges(buildWalTransaction(db, fileId, (int) versionBeforeReplay, 7001), Map.of(fileId, 5), false);

    // The page IS re-written (torn-write repair, #4926) ...
    assertThat(replayChanged).isTrue();
    // ... but the delta must NOT be folded again: this entry brought no content the node did not have.
    assertThat(bucket.getCachedRecordCount()).isEqualTo(baseline);

    // A genuinely new entry (version advances) still folds its delta exactly once.
    db.getPageManager().removePageFromCache(pageId);
    final long versionAfterReplay = db.getPageManager().getImmutablePage(pageId, file.getPageSize(), false, true)
        .getVersion();
    final boolean freshChanged = db.getTransactionManager()
        .applyChanges(buildWalTransaction(db, fileId, (int) versionAfterReplay + 1, 7002), Map.of(fileId, 5), false);

    assertThat(freshChanged).isTrue();
    assertThat(bucket.getCachedRecordCount()).isEqualTo(baseline + 5);

    // And replaying THAT entry once more leaves the counter where it is.
    db.getPageManager().removePageFromCache(pageId);
    final long versionAfterFresh = db.getPageManager().getImmutablePage(pageId, file.getPageSize(), false, true)
        .getVersion();
    db.getTransactionManager()
        .applyChanges(buildWalTransaction(db, fileId, (int) versionAfterFresh, 7003), Map.of(fileId, 5), false);
    assertThat(bucket.getCachedRecordCount()).isEqualTo(baseline + 5);
  }

  /**
   * Builds a single-page WAL transaction that rewrites the first bytes of page 0 with the content they
   * already hold, at {@code targetVersion}. The bytes are irrelevant here: what the test drives is the
   * version comparison that decides whether the record-count delta is folded.
   */
  private static WALFile.WALTransaction buildWalTransaction(final DatabaseInternal db, final int fileId,
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
