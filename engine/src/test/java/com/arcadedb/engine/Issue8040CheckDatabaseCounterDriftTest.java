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

import com.arcadedb.TestHelper;
import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #8040: {@code CHECK DATABASE} walks every slot of every bucket, so it holds the exact
 * number {@code count(*)} should be serving - and never compared the two. A counter drifted by the #7126 Raft-replay
 * double-fold therefore produced a perfectly clean check while {@code count(*)} disagreed with a full scan and, in a
 * cluster, with the other replicas. That is indistinguishable from HA state divergence until somebody scans by hand,
 * which is how #8040 was diagnosed in production.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8040CheckDatabaseCounterDriftTest extends TestHelper {

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
  @Test
  void aReadOnlyCheckReportsAnUnderReportingCounterToo() {
    bucket().setCachedRecordCount(RECORDS - 3);

    final Result row = check(false);

    assertThat((Long) row.getProperty("staleRecordCounters")).as("%s", row.toJSON()).isEqualTo(1L);
    assertThat((Collection<String>) row.getProperty("warnings")).as("%s", row.toJSON())
        .anyMatch(w -> w.contains(bucket().getName()) && w.contains("under-reports by 3"));
  }

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
  @Test
  void anAccurateCounterIsNotReported() {
    final Result row = check(false);

    assertThat((Long) row.getProperty("staleRecordCounters")).as("%s", row.toJSON()).isEqualTo(0L);
    assertThat((Long) row.getProperty("totalErrors")).isZero();
  }

  /** A counter that was never computed is not wrong, it is unknown: nothing to report and nothing to repair. */
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
}
