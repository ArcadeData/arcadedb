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
import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #5608 (follow-up 2 of #5596): {@link LocalBucket#compressPage(MutablePage, boolean)} declares every byte it
 * writes as covered by ALL the commit-time page merges ({@code MutablePage.COVERAGE_ALL_MERGES}). That declaration is
 * true only because the merge RE-RUNS the compression on the page it re-derived - the defrag is then reproduced rather
 * than lost. Nothing used to enforce it: the re-compress lived in the commit loop, one statement away from the rebase,
 * and dropping or reordering it would have left the declaration vouching for a defrag that no longer happened. The
 * consequence is a lost defrag, not a lost write, so no correctness assertion would have noticed.
 * <p>
 * The re-compress now lives INSIDE {@code TransactionContext.rebaseEdgeAppends}/{@code rebaseSlots}, and these tests
 * pin the invariant from the outside, where the implementation cannot drift away from them: force a merge on a page
 * whose replayed write leaves a hole (a shrinking update, and a delete), then assert the COMMITTED page carries no
 * hole at all. Both shapes are checked because the two hole sources are replayed by different primitives
 * ({@code rebaseRecordOnPage} and {@code rebaseRecordDeleteOnPage}).
 * <p>
 * Lives in {@code com.arcadedb.engine} to read the page layout constants directly: measuring the holes against the
 * record table is the only assertion that says "compressed" without re-deriving it from the engine's own defrag code.
 * <p>
 * DELIBERATELY NOT {@code @Tag("slow")}, for the reason {@code Issue5596MergeCoverageTest} spells out next door: the
 * tag keys on elapsed time, not on whether a test spawns a thread. Both methods here drive the SMALLEST contention
 * that reproduces - one competitor, one commit each - and the whole class measures ~0.02s. It also belongs on every
 * build: a merge that stops re-compressing costs a defrag, which no correctness assertion anywhere would catch.
 * Re-measure before tagging it, rather than tagging it by family resemblance to the contention suites.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5608RebasedPageCompressionTest extends TestHelper {
  private static final String PAD   = "0".repeat(64);
  private static final String BULKY = "x".repeat(2000);
  /** Generous enough to never fire on a loaded CI box, short enough that a real deadlock fails fast. */
  private static final long   HANDOFF_TIMEOUT_SECONDS = 60;

  private boolean savedSlotMerge;

  @BeforeEach
  void enableSlotMerge() {
    savedSlotMerge = GlobalConfiguration.TX_PAGE_SLOT_MERGE.getValueAsBoolean();
    GlobalConfiguration.TX_PAGE_SLOT_MERGE.setValue(true);
  }

  @AfterEach
  void restoreSlotMerge() {
    GlobalConfiguration.TX_PAGE_SLOT_MERGE.setValue(savedSlotMerge);
  }

  /**
   * A shrinking in-place update is a tracked slot write that leaves a hole behind ("CREATE A HOLE (REMOVED LATER BY
   * COMPRESS-PAGE)" in {@code updateRecordInternal}). Replayed on the newer committed page by the slot merge it leaves
   * exactly the same hole, so the committed page is compressed only if the merge re-ran the compression.
   */
  @Test
  void aSlotMergedShrinkLeavesNoHoleBehind() throws Exception {
    final RID[] rids = createOnePageOfRecords();
    final RID shrinking = rids[0];
    final RID bumped = rids[3];

    final long merges = mergeUnderContention(() -> shrinking.asDocument(true).modify().set("tag", "y").save(), bumped);

    assertThat(merges).as("the disjoint-slot merge must have absorbed the conflict").isGreaterThan(0);
    assertThat(holeBytesInPage(pageOf(rids[0]))).as("the merged page must be committed compressed").isZero();

    // ...and the replayed write plus the competitor's are both intact.
    database.transaction(() -> {
      assertThat(shrinking.asDocument(true).getString("tag")).isEqualTo("y");
      assertThat(bumped.asDocument(true).getString("tag")).isEqualTo("9".repeat(PAD.length()));
    });
  }

  /**
   * The same invariant for the other hole source: a plain record DELETE (#5569), replayed by
   * {@code rebaseRecordDeleteOnPage}, which frees the slot and turns the record's bytes into a hole.
   */
  @Test
  void aSlotMergedDeleteLeavesNoHoleBehind() throws Exception {
    final RID[] rids = createOnePageOfRecords();
    final RID victim = rids[1];
    final RID bumped = rids[3];

    final long merges = mergeUnderContention(() -> victim.asDocument(true).delete(), bumped);

    assertThat(merges).as("the disjoint-slot merge must have absorbed the conflict").isGreaterThan(0);
    assertThat(holeBytesInPage(pageOf(rids[0]))).as("the merged page must be committed compressed").isZero();

    database.transaction(() -> {
      assertThat(database.countType("Compact", false)).isEqualTo(rids.length - 1);
      assertThat(bumped.asDocument(true).getString("tag")).isEqualTo("9".repeat(PAD.length()));
      // The records that shared the page with the deleted one must still be readable AFTER the defrag moved them.
      assertThat(rids[0].asDocument(true).getString("tag")).isEqualTo(BULKY);
      assertThat(rids[2].asDocument(true).getString("tag")).isEqualTo(BULKY);
    });
  }

  /**
   * Four records of one type on one page. The first three are bulky so that shrinking or deleting one of them leaves a
   * hole no rounding can hide; the fourth is the competitor's, updated in place at constant size.
   */
  private RID[] createOnePageOfRecords() {
    final RID[] rids = new RID[4];
    database.transaction(() -> {
      database.getSchema().createDocumentType("Compact", 1);
      for (int i = 0; i < rids.length - 1; i++)
        rids[i] = database.newDocument("Compact").set("tag", BULKY).save().getIdentity();
      rids[rids.length - 1] = database.newDocument("Compact").set("tag", PAD).save().getIdentity();
    });

    final LocalBucket bucket = bucketOf(rids[0]);
    for (final RID rid : rids)
      assertThat(rid.getPosition() / bucket.getMaxRecordsInPage()).as("every record must share one page")
          .isEqualTo(rids[0].getPosition() / bucket.getMaxRecordsInPage());
    return rids;
  }

  /**
   * Runs {@code write} in a transaction that loses the page version race against a same-size update of {@code bumped}
   * (a false conflict on ANOTHER slot of the same page), so the commit resolves it through the disjoint-slot merge.
   * <p>
   * Every wait is bounded and every hand-off latch is released from a {@code finally}: a two-thread commit race that
   * deadlocks must fail this test in seconds, not hang the whole suite. In particular the main thread releases
   * {@code mainTxWroteAndReadThePage} even when {@code write} throws, which would otherwise park the competitor
   * forever on a latch nobody counts down.
   *
   * @return how many slot merges fired, so the caller can tell a merged commit from a plain uncontended one.
   */
  private long mergeUnderContention(final Runnable write, final RID bumped) throws InterruptedException {
    final long mergesBefore = slotMerges();
    final CountDownLatch mainTxWroteAndReadThePage = new CountDownLatch(1);
    final CountDownLatch bumpCommitted = new CountDownLatch(1);
    final List<Throwable> errors = new CopyOnWriteArrayList<>();

    final Thread bumper = new Thread(() -> {
      try {
        if (!mainTxWroteAndReadThePage.await(HANDOFF_TIMEOUT_SECONDS, TimeUnit.SECONDS))
          throw new AssertionError("the main transaction never signalled that it had written and read the page");
        database.transaction(() -> bumped.asDocument(true).modify().set("tag", "9".repeat(PAD.length())).save(), true, 50);
      } catch (final Throwable e) {
        errors.add(e);
      } finally {
        bumpCommitted.countDown();
      }
    }, "bumper");
    bumper.start();

    database.begin();
    try {
      try {
        write.run();                        // loads the page at its current version and registers the tracked write
      } finally {
        mainTxWroteAndReadThePage.countDown();
      }
      if (!bumpCommitted.await(HANDOFF_TIMEOUT_SECONDS, TimeUnit.SECONDS))
        throw new AssertionError("the competitor never finished its update of the shared page");
      database.commit();                    // ...whose commit has now made this transaction's page version stale
    } finally {
      if (database.isTransactionActive())
        database.rollback();
      bumper.join(TimeUnit.SECONDS.toMillis(HANDOFF_TIMEOUT_SECONDS));
    }

    assertThat(bumper.isAlive()).as("the competitor thread must not still be running").isFalse();
    if (!errors.isEmpty())
      throw new AssertionError("competitor failed: " + errors.getFirst(), errors.getFirst());

    return slotMerges() - mergesBefore;
  }

  private long slotMerges() {
    return ((DatabaseInternal) database).getPageManager().getStats().txPageSlotMerges;
  }

  private LocalBucket bucketOf(final RID rid) {
    return (LocalBucket) database.getSchema().getBucketById(rid.getBucketId());
  }

  /** Reads the page back from the COMMITTED image, so a compression that never reached disk cannot hide in a cache. */
  private BasePage pageOf(final RID rid) {
    final LocalBucket bucket = bucketOf(rid);
    final PageId pageId = new PageId(database, bucket.getFileId(), (int) (rid.getPosition() / bucket.getMaxRecordsInPage()));
    final BasePage[] page = new BasePage[1];
    database.transaction(() -> {
      try {
        page[0] = ((DatabaseInternal) database).getTransaction().getPage(pageId, bucket.getPageSize());
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
    });
    return page[0];
  }

  /**
   * Bytes of the page's record region that belong to no record: the gap before the first record plus every gap between
   * two consecutive ones. Zero exactly when the page carries no hole, i.e. when {@code compressPage} ran on the image
   * that got committed. Deliberately measured from the record table rather than by calling the engine's own hole
   * computation, so a defrag that stops running cannot take the assertion down with it.
   */
  private int holeBytesInPage(final BasePage page) throws IOException {
    final LocalBucket bucket = (LocalBucket) database.getSchema().getBucketById(page.getPageId().getFileId());
    final int contentHeaderSize = LocalBucket.PAGE_RECORD_TABLE_OFFSET + bucket.getMaxRecordsInPage() * Binary.INT_SERIALIZED_SIZE;
    final short recordCountInPage = page.readShort(LocalBucket.PAGE_RECORD_COUNT_IN_PAGE_OFFSET);

    final List<int[]> records = new ArrayList<>(recordCountInPage);
    for (int slot = 0; slot < recordCountInPage; slot++) {
      final int position = (int) page.readUnsignedInt(
          LocalBucket.PAGE_RECORD_TABLE_OFFSET + slot * Binary.INT_SERIALIZED_SIZE);
      if (position < 1 || position >= page.getContentSize())
        // FREED SLOT
        continue;
      final long[] recordSize = page.readNumberAndSize(position);
      assertThat(recordSize[0]).as("this test only lays out plain in-place records, no placeholder or chunk")
          .isGreaterThan(0L);
      records.add(new int[] { position, (int) (recordSize[0] + recordSize[1]) });
    }
    records.sort((a, b) -> Integer.compare(a[0], b[0]));

    int holeBytes = 0;
    int expectedNextPosition = contentHeaderSize;
    for (final int[] record : records) {
      holeBytes += record[0] - expectedNextPosition;
      expectedNextPosition = record[0] + record[1];
    }
    return holeBytes;
  }
}
