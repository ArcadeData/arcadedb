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

import com.arcadedb.database.BucketPageLayoutTestSupport;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6358: what a write TELLS the free-space statistics and what the commit MEASURES of the same page have to be
 * the same number. Since #6349 the commit packs every page a transaction touched and records the free tail it
 * measured, so the two descriptions of one page sit side by side - and they disagreed by a known constant.
 * <p>
 * Every assertion here is that one invariant, taken over the same page twice: the hint at the end of the transaction
 * body (which only the write-path deltas have produced) against the hint after the commit (which the measurement has
 * produced). The workloads are pure inserts and grows, so the compression moves no bytes and the two must agree
 * exactly; a difference is a writer subtracting something other than what its chunk occupies.
 * <p>
 * The three writers this covers each subtracted something of their own - a continuation chunk of a fresh spill left
 * the 12 header bytes out, the extension of an existing chain left the marker byte out and counted the whole
 * remaining buffer, the one landing on a brand-new page counted neither and measured against a base that still
 * included the page header - and one shared {@code chunkFootprint} now answers for all of them.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6358ChunkFootprintAccountingTest extends BucketPageLayoutTestSupport {
  private static final String TYPE = "Chunked";

  /**
   * The continuation chunks of a fresh spill ({@code writeMultiPageRecord}). Every page the chain fills whole has no
   * free tail to report, so the page that shows the defect is the LAST one - the only one a chunk leaves room on -
   * and the payload is sized against the page geometry so that room really is there.
   */
  @Test
  void theTailAFreshSpillLeavesIsReportedAsThePageHasIt() {
    database.transaction(() -> database.getSchema().createDocumentType(TYPE, 1).createProperty("v", Type.STRING));

    final LocalBucket bucket = bucketOf(TYPE);
    // Two full pages of chunks and a third one half taken, so the last chunk leaves a tail far above the 10% a hint
    // is kept for.
    final String payload = "s".repeat(wholeUsablePage(bucket) * 5 / 2);

    final Map<Integer, Integer> beforeCommit = new LinkedHashMap<>();
    database.transaction(() -> {
      database.newDocument(TYPE).set("v", payload).save();
      collectHints(bucket, beforeCommit);
    });

    assertThat(bucket.getTotalPages()).as("the fixture must really span a chain of continuation chunks").isGreaterThan(2);
    assertHintsSurviveTheCommit(bucket, beforeCommit);
    checkDatabase();
  }

  /**
   * The chunks an UPDATE adds to a chain it already has ({@code updateMultiPageRecord}), both ways it can place them:
   * on a page the allocator reused - here the one the previous chunk left room on - and on a page created for them.
   * Growing the record in steps drives it through both, since each step first fills what the chain has and then asks
   * for more.
   */
  @Test
  void theTailAChainExtensionLeavesIsReportedAsThePageHasIt() {
    database.transaction(() -> database.getSchema().createDocumentType(TYPE, 1).createProperty("v", Type.STRING));

    final LocalBucket bucket = bucketOf(TYPE);
    final int usable = wholeUsablePage(bucket);
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument(TYPE).set("v", "g".repeat(usable)).save().getIdentity());

    // Strictly growing, on purpose: a shrink FREES chunks, and the bytes a free gives back are deliberately reported
    // by the commit's compression rather than by the write (#6339, and the reason it is measured there), so a
    // shrinking step would break this invariant by design instead of by defect.
    for (int step = 3; step <= 8; step++) {
      final String payload = "g".repeat(usable * step / 2 + usable / 3);
      final Map<Integer, Integer> beforeCommit = new LinkedHashMap<>();
      database.transaction(() -> {
        writeNow(rid[0].asDocument(true).modify().set("v", payload));
        collectHints(bucket, beforeCommit);
      });
      assertHintsSurviveTheCommit(bucket, beforeCommit);
      database.transaction(() -> assertThat(rid[0].asDocument(true).getString("v")).isEqualTo(payload));
    }
    checkDatabase();
  }

  /**
   * Item 1 of the issue, the one the three writers above kept feeding: {@code updatePageStatistics} read an
   * {@code availableSpace} of 0 as "I have no measurement, apply my delta to the number you already hold" whenever it
   * held one for that page - so a caller reporting a page with genuinely no free tail left had its delta added to a
   * stale number.
   * <p>
   * The caller built here is real and is the one that reaches it with the largest delta: a record that spilled as the
   * LAST record of its page owns a head chunk running to the page's maximum content size, so when it shrinks back
   * into its own slot (#6178) it reports a free tail of 0 and a delta of everything the chunk gave back. The stale
   * entry is planted rather than waited for, because what stands between today's callers and this branch is a
   * coincidence of thresholds and not a rule.
   */
  @Test
  void aZeroFreeTailIsNotReadAsAnUnknownFreeTail() {
    database.transaction(() -> database.getSchema().createDocumentType(TYPE, 1).createProperty("v", Type.STRING));

    final LocalBucket bucket = bucketOf(TYPE);
    final RID sealer = sealFirstPage(TYPE);
    assertThat(bucket.getFreeSpaceHintForPage(0)).as("a page a spill has sealed has no free tail to offer")
        .isEqualTo(-1);

    // Tell the allocator page 0 has a kilobyte to spare. It has not: the spill took its last byte.
    final int planted = 1_024;
    final JSONArray statistics = new JSONArray();
    statistics.put(new JSONObject().put("id", 0).put("free", planted));
    bucket.setPageStatistics(statistics);

    final int[] duringTheTransaction = new int[1];
    database.transaction(() -> {
      writeNow(sealer.asDocument(true).modify().set("v", "t"));
      duringTheTransaction[0] = bucket.getFreeSpaceHintForPage(0);
    });

    final int afterTheCommit = bucket.getFreeSpaceHintForPage(0);
    assertThat(afterTheCommit).as("the collapse must really have given the page its tail back").isGreaterThan(planted);
    assertThat(afterTheCommit).as("and the tail is the head chunk's, not the whole page's")
        .isLessThan(wholeUsablePage(bucket));
    assertThat(duringTheTransaction[0])
        .as("the collapse measured the page it wrote; nothing may be added to the stale number the map held")
        .isEqualTo(afterTheCommit);

    database.transaction(() -> assertThat(sealer.asDocument(true).getString("v")).isEqualTo("t"));
    checkDatabase();
  }

  /**
   * The same double count, on the one path #6339 did not reach: the delete a commit-time page conflict makes it
   * REPLAY ({@code rebaseRecordDeleteOnPage}). It measured the free tail of the page with the slot already zeroed -
   * so whatever the free released is in the number - and then subtracted the record's footprint from it as well. On a
   * record freed in the MIDDLE of its page the tail does not move at all, so the subtraction was pure loss, and on a
   * nearly full page it takes the number BELOW ZERO - a page reported as having less than no free space.
   * <p>
   * Found by the tripwire this issue added rather than by reading: the assertion in {@code updatePageStatistics}
   * fires under {@code -ea} (surefire's default) inside the commit, which is what this drives and what makes the
   * test fail loudly instead of quietly recording nonsense the next compression would paper over.
   */
  @Test
  void aRebasedDeleteReportsTheTailItMeasuredAndNothingLess() throws Exception {
    database.transaction(() -> database.getSchema().createDocumentType(TYPE, 1).createProperty("v", Type.STRING));

    final LocalBucket bucket = bucketOf(TYPE);
    final RID lastOnPageZero = fillFirstPage(TYPE);
    assertThat(lastOnPageZero.getPosition()).as("page 0 must hold several records for one of them to be a middle one")
        .isGreaterThan(1L);

    // A record in the MIDDLE of page 0 - freeing it moves no free tail - and another one to bump the page version
    // with an in-place, same-size write the slot merge can replay.
    final RID victim = new RID(lastOnPageZero.getBucketId(), 1);
    final RID bumped = new RID(lastOnPageZero.getBucketId(), 0);

    final long recordsBefore = countRecords(TYPE);
    final CountDownLatch deleteDone = new CountDownLatch(1);
    final CountDownLatch bumpCommitted = new CountDownLatch(1);
    final List<Throwable> errors = new CopyOnWriteArrayList<>();

    final Thread bumper = new Thread(() -> {
      try {
        deleteDone.await();
        database.transaction(() -> {
          final String current = bumped.asDocument(true).getString("v");
          bumped.asDocument(true).modify().set("v", "B" + current.substring(1)).save();
        }, true, 50);
      } catch (final Throwable e) {
        errors.add(e);
      } finally {
        bumpCommitted.countDown();
      }
    }, "issue-6358-bumper");
    bumper.start();

    boolean committed = false;
    for (int attempt = 0; attempt < 20 && !committed; attempt++) {
      database.begin();
      try {
        victim.asDocument(true).delete();
        if (attempt == 0) {
          deleteDone.countDown();
          bumpCommitted.await();
        }
        database.commit();
        committed = true;
      } catch (final ConcurrentModificationException retryable) {
        if (database.isTransactionActive())
          database.rollback();
      }
    }
    bumper.join();

    if (!errors.isEmpty())
      throw new AssertionError("the competing write failed: " + errors.getFirst(), errors.getFirst());
    assertThat(committed).as("the delete must eventually commit").isTrue();

    assertThat(bucket.getFreeSpaceHintForPage(0)).as("a page can never be offered less than no free space")
        .isGreaterThanOrEqualTo(-1);
    assertThat(countRecords(TYPE)).as("the replayed delete must have removed exactly the one record")
        .isEqualTo(recordsBefore - 1);
    database.transaction(() -> assertThat(bumped.asDocument(true).getString("v")).startsWith("B"));
    checkDatabase();
  }

  /**
   * Runs the update the COMMIT would run, one line earlier: {@code save()} only queues a record, and the bucket write
   * happens inside the commit - a line before the compression whose measurement is the very thing these tests need to
   * compare against. This is the same call the commit makes on every queued record
   * ({@code TransactionContext.commit1stPhase} -> {@code updateRecordNoLock}), not a path fabricated for the test:
   * pulling it forward changes when the write happens, never what it writes.
   */
  private void writeNow(final MutableDocument record) {
    ((DatabaseInternal) database).updateRecordNoLock(record, false);
  }

  /**
   * The hint every page of the bucket carries. Taken at the END of a transaction body - every write has landed and
   * the only thing left is the commit - so what it holds is the sum of the write-path deltas and nothing else.
   */
  private static void collectHints(final LocalBucket bucket, final Map<Integer, Integer> into) {
    into.clear();
    // The pages this transaction reserved are not in getTotalPages() until it commits, so the walk goes well past
    // what the bucket admits to having; a page that does not exist simply has no entry.
    for (int pageId = 0; pageId < bucket.getTotalPages() + 32; pageId++)
      into.put(pageId, bucket.getFreeSpaceHintForPage(pageId));
  }

  /**
   * The invariant. Not vacuous by construction: at least one page must carry a real hint, otherwise the comparison
   * would hold on a map of nothing but -1 and would go on holding it however wrong the writers were.
   */
  private static void assertHintsSurviveTheCommit(final LocalBucket bucket, final Map<Integer, Integer> beforeCommit) {
    assertThat(beforeCommit.values().stream().anyMatch(hint -> hint > 0))
        .as("the fixture must leave at least one page with a free tail worth a hint: " + beforeCommit)
        .isTrue();

    for (final Map.Entry<Integer, Integer> hint : beforeCommit.entrySet())
      assertThat(bucket.getFreeSpaceHintForPage(hint.getKey()))
          .as("page " + hint.getKey() + " was reported as having " + hint.getValue()
              + " free bytes by the write; the commit measured what it really has")
          .isEqualTo(hint.getValue());
  }

  /** What an EMPTY page of this bucket has to offer: everything past the page header and the slot table. */
  private static int wholeUsablePage(final LocalBucket bucket) {
    return bucket.getPageSize() - BasePage.PAGE_HEADER_SIZE - bucket.contentHeaderSize;
  }
}
