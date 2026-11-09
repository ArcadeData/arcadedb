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
import com.arcadedb.database.BucketPageLayoutTestSupport;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.RecordInternal;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.Isolated;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Consolidated tests for BucketFreeSpaceReuse behaviours, one nested class per originally reported issue.
 */
class BucketFreeSpaceReuseTest {

  /**
   * Issue #6396: nothing kept the free-space deltas honest.
   * <p>
   * Every write path in {@code LocalBucket} tells the free-space statistics how much free tail the page it just wrote
   * has left. Since #6339/#6349 the commit compresses every page a transaction touched, MEASURES that same quantity
   * there and overwrites whatever the deltas had accumulated - so nothing observable after a commit depends on a delta
   * being right, and a writer could be (and, in #6154, #6339 and #6358, repeatedly was) thousands of bytes wrong
   * without a single red test. #6358's regression test had to reach for {@code updateRecordNoLock} to pull a bucket
   * write out of the commit just to read the hint before the measurement replaced it.
   * <p>
   * The tripwire this pins closes that gap: the write's claim is kept on the page image itself and confronted, by the
   * compression, with the tail the page actually has. It is an assertion, so it costs nothing in production and fires
   * on every commit of every test under {@code -ea}.
   * <p>
   * What the tests below have to establish, in this order, is that the check is ENGAGED (a real insert leaves a claim,
   * and it is the tail the page really has - not an unknown that would make every other assertion here vacuous), that
   * it catches BOTH directions of a wrong writer, that a write which legitimately gives bytes back demotes the claim to
   * a lower bound instead of breaking it - and that the lower bound still catches an over-claim - and that a page no
   * write spoke about is skipped rather than failed.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  // #6396
  @Nested
  class FreeSpaceClaim extends BucketPageLayoutTestSupport {
    private static final String TYPE = "Claimed";

    private LocalBucket bucket;

    @BeforeEach
    void createType() {
      assumeThat(LocalBucket.class.desiredAssertionStatus())
          .as("the free-space claim check IS an assertion: it can only be tested with -ea")
          .isTrue();
      database.transaction(() -> database.getSchema().createDocumentType(TYPE, 1).createProperty("v", Type.STRING));
      bucket = bucketOf(TYPE);
    }

    /**
     * The check is engaged. An ordinary insert leaves a claim on the page it wrote, and that claim is the free tail the
     * page has - derived here from the record table, independently of the walk {@code compressPageInternal} makes.
     * <p>
     * Without this, every other test in this class could pass on a page carrying no claim at all.
     */
    @Test
    void anInsertClaimsTheTailThePageReallyHas() {
      final List<RID> rids = new ArrayList<>();
      database.transaction(() -> {
        for (int i = 0; i < 8; i++)
          rids.add(database.newDocument(TYPE).set("v", "v".repeat(512)).save().getIdentity());

        final MutablePage page = pageOf(rids.getFirst());
        assertThat(page.getFreeSpaceClaim())
            .as("an insert must state what free tail it leaves; without a claim the commit has nothing to check")
            .isNotEqualTo(MutablePage.FREE_SPACE_CLAIM_UNKNOWN);
        assertThat(page.getFreeSpaceClaim()).isEqualTo(freeTailOf(page, rids.getLast()));
      });

      // And it survives the commit that measures the same page: the two descriptions agree end to end.
      assertThat(bucket.getFreeSpaceHintForPage(0)).isEqualTo(freeTailAfterCommit(rids.getLast()));
      checkDatabase();
    }

    /**
     * Over-reporting: the direction that hands the allocator a page with nothing left on it, and the shape of the
     * {@code getAvailableContentSize()} base #6358 removed - a writer measuring against a base that still counted the
     * page header, and so claiming bytes the page does not have.
     */
    @Test
    void aWriterClaimingMoreTailThanThePageHasIsCaughtByTheCommit() {
      final RID rid = insertOne();

      assertThatThrownBy(() -> database.transaction(() -> {
        database.newDocument(TYPE).set("v", "v".repeat(512)).save();
        pageOf(rid).claimFreeSpace(bucket.getPageSize());
      }))
          .as("a page cannot have a whole page's worth of free tail once records are on it")
          .rootCause().isInstanceOf(AssertionError.class)
          .hasMessageContaining("was reported to the free-space statistics with");
    }

    /**
     * Under-reporting: harmless to the allocator (it only loses a placement it could have made), but it is the shape of
     * #6358's {@code rebaseRecordDeleteOnPage} - a measured tail with the freed record's footprint subtracted from it
     * a second time - and the ONE of the four that no tripwire caught until it was found by reading. A check in the
     * over-reporting direction alone would let it through, which is why the comparison is an equality against the tail
     * BEFORE the defrag rather than an upper bound against the packed one.
     */
    @Test
    void aWriterClaimingLessTailThanThePageHasIsCaughtToo() {
      final RID rid = insertOne();

      assertThatThrownBy(() -> database.transaction(() -> {
        database.newDocument(TYPE).set("v", "v".repeat(512)).save();
        pageOf(rid).claimFreeSpace(pageOf(rid).getFreeSpaceClaim() - 1);
      }))
          .as("one byte short is still a writer describing bytes it did not write")
          .rootCause().isInstanceOf(AssertionError.class)
          .hasMessageContaining("was reported to the free-space statistics with");
    }

    /**
     * A delete frees its bytes and deliberately tells the statistics nothing - the compression measures the packed page
     * anyway, and accounting at the free itself costs a slot-table scan per freed slot for four pages out of 239
     * (#6339). So does an in-place update that shrinks the record. Both demote the claim to a LOWER BOUND rather than
     * dropping it: they can only move the free tail outwards, so what the insert said stays a floor under it and the
     * over-reporting direction goes on being checked on the page.
     * <p>
     * Both delete shapes matter: a record deleted in the MIDDLE of the page leaves a hole the defrag closes, and the
     * LAST record of the page hands its tail straight back. The second is the one that breaks the equality, and the
     * reason the demotion has to exist at all.
     */
    @Test
    void aSilentFreeDemotesTheClaimToALowerBound() {
      final List<RID> rids = new ArrayList<>();
      database.transaction(() -> {
        for (int i = 0; i < 8; i++)
          rids.add(database.newDocument(TYPE).set("v", "v".repeat(512)).save().getIdentity());
      });

      database.transaction(() -> {
        database.newDocument(TYPE).set("v", "v".repeat(512)).save();
        rids.get(3).asDocument(true).delete();
        assertLowerBoundOn(pageOf(rids.getFirst()));
      });

      database.transaction(() -> {
        database.newDocument(TYPE).set("v", "v".repeat(512)).save();
        rids.getLast().asDocument(true).delete();
        assertLowerBoundOn(pageOf(rids.getFirst()));
      });

      // The last record of the page, shrunk in place: the tail moves outwards and nothing reports it.
      database.transaction(() -> {
        final RID last = database.newDocument(TYPE).set("v", "v".repeat(512)).save().getIdentity();
        assertThat(pageOf(last).isFreeSpaceClaimExact()).as("the insert that created it stated an exact tail").isTrue();
        writeNow(last.asDocument(true).modify().set("v", "v"));
        assertLowerBoundOn(pageOf(last));
      });

      assertThat(countRecords(TYPE)).isEqualTo(9);
      checkDatabase();
    }

    /**
     * The demotion is not an exemption. A page that saw a silent free is checked with {@code <=} instead of {@code ==},
     * and the floor is exactly what a writer claiming bytes the page does not have still trips over - which is the
     * whole reason the claim is demoted rather than dropped.
     */
    @Test
    void theLowerBoundStillCatchesAnOverClaim() {
      final RID rid = insertOne();

      assertThatThrownBy(() -> database.transaction(() -> {
        database.newDocument(TYPE).set("v", "v".repeat(512)).save();
        final MutablePage page = pageOf(rid);
        page.claimFreeSpace(bucket.getPageSize());
        page.relaxFreeSpaceClaim();
      }))
          .as("a floor above the page's real tail is still a writer describing bytes it did not write")
          .rootCause().isInstanceOf(AssertionError.class)
          .hasMessageContaining("free bytes or more");
    }

    /**
     * The disjoint-slot merge replays SEVERAL slot writes onto ONE page image and compresses it once, at the end
     * (#5381, {@code TransactionContext.rebaseSlots}) - so a replay branch that says nothing about the free tail does
     * not merely fail to update a hint, it leaves the claim ANOTHER slot's replay made standing over a page that has
     * moved on. Found in review of this change.
     * <p>
     * The shape: one record GROWS (its replay states an exact tail) and the page's LAST record SHRINKS in the same
     * transaction, while a concurrent commit to the same page forces the replay. The shrink's replay branch is the
     * mirror of {@code updateRecordInternal}'s same-or-shorter overwrite and gives the tail back exactly as that one
     * does, so it demotes the claim exactly as that one does.
     * <p>
     * Deterministic rather than racing: the concurrent commit happens on another thread which is joined before this
     * transaction commits, so the page version has certainly moved by the time the merge is asked for.
     */
    @Test
    void aSlotMergeReplayGivingBytesBackDoesNotLeaveAnotherSlotsClaimStanding() {
      final boolean slotMerge = GlobalConfiguration.TX_PAGE_SLOT_MERGE.getValueAsBoolean();
      GlobalConfiguration.TX_PAGE_SLOT_MERGE.setValue(true);
      try {
        final List<RID> rids = new ArrayList<>();
        database.transaction(() -> {
          for (int i = 0; i < 8; i++)
            rids.add(database.newDocument(TYPE).set("v", "v".repeat(400)).save().getIdentity());
        });

        final RID first = rids.getFirst();
        final RID last = rids.getLast();
        final RID bystander = rids.get(4);

        database.transaction(() -> {
          // A growth, whose replay states an exact free tail for the whole page...
          first.asDocument(true).modify().set("v", "g".repeat(700)).save();
          // ...and a shrink of the record that ENDS the page, whose replay hands bytes back to that same tail.
          last.asDocument(true).modify().set("v", "s").save();

          // A committed change to the same page, from another transaction: this is what makes our commit reload the
          // page and replay our two slots onto it instead of writing our own image.
          inAnotherThread(() -> database.transaction(
              () -> bystander.asDocument(true).modify().set("v", "b".repeat(400)).save()));
        });

        database.transaction(() -> {
          assertThat(first.asDocument(true).getString("v")).isEqualTo("g".repeat(700));
          assertThat(last.asDocument(true).getString("v")).isEqualTo("s");
          assertThat(bystander.asDocument(true).getString("v")).isEqualTo("b".repeat(400));
        });
        assertThat(countRecords(TYPE)).isEqualTo(8);
        checkDatabase();
      } finally {
        GlobalConfiguration.TX_PAGE_SLOT_MERGE.setValue(slotMerge);
      }
    }

    /**
     * A page a transaction modified without any write speaking about its free space carries no claim, and the commit's
     * measurement has nothing to be held against. Skipped, never failed: a tripwire that fires on pages nobody made a
     * statement about is a tripwire that gets widened until it says nothing.
     * <p>
     * It also pins that the claim is per TRANSACTION: the page below is the same page the insert of the previous
     * transaction claimed an exact tail on, and this one starts over with nothing stated.
     */
    @Test
    void aPageNoWriteSpokeAboutIsNotChecked() {
      final RID rid = insertOne();

      database.transaction(() -> {
        final MutablePage page = pageOf(rid);
        assertThat(page.getFreeSpaceClaim()).isEqualTo(MutablePage.FREE_SPACE_CLAIM_UNKNOWN);
        // Dirty the page without changing it, so the commit really does compress it.
        page.writeShort(LocalBucket.PAGE_RECORD_COUNT_IN_PAGE_OFFSET, page.readShort(LocalBucket.PAGE_RECORD_COUNT_IN_PAGE_OFFSET));
      });

      assertThat(countRecords(TYPE)).isEqualTo(1);
      checkDatabase();
    }

    /**
     * The whole point of keeping the claim on the page image rather than in the bucket's {@code freeSpaceInPages} map:
     * that map is bucket-wide, so a concurrent transaction's delta for the same page would be what our commit measures
     * itself against - and the two transactions describe two different futures of that page, only one of which is going
     * to commit. Threads inserting into the same bucket at once must not make each other fail.
     */
    @Test
    void concurrentWritersToTheSameBucketDoNotCheckEachOther() throws Exception {
      final int writers = 4;
      final int perWriter = 60;
      final CountDownLatch start = new CountDownLatch(1);
      final List<Throwable> errors = new CopyOnWriteArrayList<>();
      final List<Thread> threads = new ArrayList<>();

      for (int w = 0; w < writers; w++) {
        final int id = w;
        final Thread thread = new Thread(() -> {
          try {
            start.await();
            for (int i = 0; i < perWriter; i++) {
              final int seq = i;
              database.transaction(() -> database.newDocument(TYPE).set("v", "w" + id + "-" + seq).save(), true, 50);
            }
          } catch (final Throwable e) {
            errors.add(e);
          }
        }, "issue6396-writer-" + w);
        threads.add(thread);
        thread.start();
      }

      start.countDown();
      for (final Thread thread : threads)
        thread.join();

      assertThat(errors).as("a claim is one transaction's own; no writer may be checked against another's")
          .isEmpty();
      assertThat(countRecords(TYPE)).isEqualTo((long) writers * perWriter);
      checkDatabase();
    }

    /** The claim survives a silent free as a floor: still stated, no longer exact. */
    private void assertLowerBoundOn(final MutablePage page) {
      assertThat(page.getFreeSpaceClaim())
          .as("a silent free demotes the claim, it does not drop it - the floor is what keeps over-reporting checked")
          .isNotEqualTo(MutablePage.FREE_SPACE_CLAIM_UNKNOWN);
      assertThat(page.isFreeSpaceClaimExact()).isFalse();
    }

    /**
     * Runs the update the COMMIT would run, one line earlier: {@code save()} only queues a record and the bucket write
     * happens inside the commit, a line before the compression whose measurement is what these tests compare against.
     * The same call the commit makes on every queued record ({@code TransactionContext.commit1stPhase} ->
     * {@code updateRecordNoLock}); pulling it forward changes when the write happens, never what it writes.
     */
    private void writeNow(final MutableDocument record) {
      ((DatabaseInternal) database).updateRecordNoLock(record, false);
    }

    private RID insertOne() {
      final RID[] rid = new RID[1];
      database.transaction(() -> rid[0] = database.newDocument(TYPE).set("v", "v".repeat(512)).save().getIdentity());
      return rid[0];
    }

    /** The page image the running transaction holds for {@code rid}'s page - the very object its writes claimed on. */
    private MutablePage pageOf(final RID rid) {
      final DatabaseInternal db = (DatabaseInternal) database;
      try {
        return db.getTransaction().getPageToModify(
            new PageId(db, rid.getBucketId(), (int) (rid.getPosition() / bucket.getMaxRecordsInPage())),
            bucket.getPageSize(), false);
      } catch (final Exception e) {
        throw new AssertionError(e);
      }
    }

    /**
     * Free tail of {@code page} read from the record table alone, given {@code last} is the record physically last on
     * it (true of the pages built here: plain records appended one after the other into a fresh page).
     */
    private int freeTailOf(final MutablePage page, final RID last) {
      final int offset = recordOffsetOf(page, last);
      final long[] size = page.readNumberAndSize(offset);
      final int tail = page.getMaxContentSize() - (int) (offset + size[1] + size[0]);
      assertThat(tail).as("the fixture must leave a tail to talk about").isPositive();
      return tail;
    }

    /** The same derivation, run in a transaction of its own once the commit has settled the page. */
    private int freeTailAfterCommit(final RID last) {
      final int[] tail = new int[1];
      database.transaction(() -> tail[0] = freeTailOf(pageOf(last), last));
      return tail[0];
    }
  }

  /**
   * Issue #6339: freeing a slot gave its bytes back to the PAGE but not to the allocator's free-space statistics, so
   * the allocator went on passing the page over. The three places that free one - the tail an update drops
   * ({@code freeChunkChain}), the single slot the {@code check(fix)} repairs give back ({@code freeSlotOnly}), and the
   * ordinary {@code deleteRecordInternal} - are asserted here together, because they share one answer and it is not at
   * any of them: a freed slot is a HOLE, which the allocator cannot hand out, and what the page really has to offer is
   * settled a moment later by the {@code compressPage} the commit runs on every page the transaction modified. That is
   * the only place that knows a page's packed occupancy exactly, and it now reports it.
   * <p>
   * Asserted on the HINT itself and not only through the allocator, for the reason #6154's test already gives: the
   * allocator re-reads every candidate page through {@code getAvailableSpaceInPage} before using it, so a test driven
   * through it alone passes whether or not the statistics were ever told. The end-to-end assertions are here too, two
   * of them, because the hint is a means and page reuse is the end.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  // #6339
  @Nested
  class FreedSpaceStatistics extends BucketPageLayoutTestSupport {
    private static final String TYPE        = "Freed";
    /** Big enough to spill into a chain that spans several pages of its own. */
    private static final String HUGE        = "h".repeat(400 * 1024);
    /** Small enough to collapse the record back into its own slot, freeing the whole chain (#6178). */
    private static final String TINY        = "t";
    /** A record that comfortably fits a page but cannot share one with many of its kind. */
    private static final String MEDIUM      = "m".repeat(8 * 1024);
    /** The filler {@code fillFirstPage} writes, and therefore what deleting one of its records gives the page back. */
    private static final int    FILLER_SIZE = 8 * 1024;

    /**
     * The chain an update drops. Every page the continuation chunks had to themselves is empty once the record shrinks,
     * and the statistics must offer the whole of it again - not the nothing they offered while the chunk was there.
     */
    @Test
    void theTailAFreedChainReleasesIsOfferedAgain() {
      final RID big = createSpilledRecord();
      final LocalBucket bucket = bucketOf(TYPE);
      final int totalPages = bucket.getTotalPages();
      assertThat(totalPages).as("the fixture must really span several pages of continuation chunks").isGreaterThan(3);

      database.transaction(() -> big.asDocument(true).modify().set("v", TINY).save());

      assertThat((Long) bucketStats(TYPE).get("totalChunks")).as("the shrink must free the whole chain").isZero();

      for (int pageId = 1; pageId < totalPages; pageId++)
        assertThat(bucket.getFreeSpaceHintForPage(pageId))
            .as("page " + pageId + " holds nothing at all once the chain is freed, so the whole of it must be offered")
            .isEqualTo(wholeUsablePage(bucket));

      database.transaction(() -> assertThat(big.asDocument(true).getString("v")).isEqualTo(TINY));
      checkDatabase();
    }

    /**
     * What the hint is FOR. The pages a freed chain released must take new records instead of the bucket growing a page
     * per record - which is what happened while the statistics still described those pages as full, because
     * {@code findAvailableSpaceFromStatistics} never even offered them as candidates.
     * <p>
     * The last page is deliberately kept occupied by a second record's tail: it is the one page the allocator reaches
     * without the statistics at all ({@code findAvailableSpace} falls back to it), so a fixture whose last page were
     * empty would pass with the statistics still wrong.
     */
    @Test
    void thePagesAFreedChainReleasesAreHandedToNewRecords() {
      final RID big = createSpilledRecord();
      final RID[] second = new RID[1];
      database.transaction(() -> second[0] = database.newDocument(TYPE).set("v", HUGE).save().getIdentity());

      final LocalBucket bucket = bucketOf(TYPE);
      database.transaction(() -> big.asDocument(true).modify().set("v", TINY).save());

      final int pagesAfterTheShrink = bucket.getTotalPages();

      // Sixteen 8 KB records: far more than the free tail of the pages the second record left unfilled, and far less
      // than the space the first record's chain gave back.
      database.transaction(() -> {
        for (int i = 0; i < 16; i++)
          database.newDocument(TYPE).set("v", MEDIUM).save();
      });

      assertThat(bucket.getTotalPages())
          .as("the released pages must host the new records rather than the bucket growing fresh ones")
          .isEqualTo(pagesAfterTheShrink);

      database.transaction(() -> {
        assertThat(big.asDocument(true).getString("v")).isEqualTo(TINY);
        assertThat(second[0].asDocument(true).getString("v")).isEqualTo(HUGE);
      });
      checkDatabase();
    }

    /**
     * The repair path, {@code freeSlotOnly}: the orphaned-chunk sweep of {@code check(fix)} (#6294) gives a slot back
     * and nothing else. It is an admin operation, so the cost of deriving what it released is irrelevant - and the
     * bucket it runs on is precisely the one whose space accounting nobody has been keeping.
     */
    @Test
    void theSlotsTheOrphanedChunkSweepReclaimsAreOfferedAgain() {
      final RID big = createSpilledRecord();
      final LocalBucket bucket = bucketOf(TYPE);
      final int totalPages = bucket.getTotalPages();

      // Cut the chain off at the head: every chunk behind it becomes an orphan, exactly as a corrupted continuation
      // pointer leaves them.
      database.transaction(() -> onSlot(big, page -> {
        // [marker:1][chunkSize:int][nextChunkPointer:long][content...]
        page.writeLong(recordOffsetOf(page, big) + 1 + Binary.INT_SERIALIZED_SIZE, Integer.MAX_VALUE);
        return 0L;
      }));

      final Result fixed = checkDatabaseRow(true);
      assertThat(numberProperty(fixed, "orphanedChunksReclaimed")).as("the sweep must have run: " + fixed.toJSON())
          .isPositive();

      for (int pageId = 1; pageId < totalPages; pageId++)
        assertThat(bucket.getFreeSpaceHintForPage(pageId))
            .as("page " + pageId + " was reclaimed whole, so the whole of it must be offered")
            .isEqualTo(wholeUsablePage(bucket));
    }

    /**
     * The ordinary delete, which had the same defect with the sign the other way round: it measured the page AFTER
     * zeroing the slot - so the tail the delete had just released was already in the number - and then subtracted the
     * record's footprint from it, landing back on exactly the free space the page had BEFORE the delete. A page emptied
     * by deleting its records was therefore never offered any of them back.
     */
    @Test
    void theTailAPlainDeleteReleasesIsOfferedAgain() {
      database.transaction(() -> database.getSchema().createDocumentType(TYPE, 1).createProperty("v", Type.STRING));

      final LocalBucket bucket = bucketOf(TYPE);
      final RID last = fillFirstPage(TYPE);
      final int hintWhileFull = bucket.getFreeSpaceHintForPage(0);

      database.transaction(() -> last.asDocument(true).delete());

      assertThat(bucket.getFreeSpaceHintForPage(0))
          .as("deleting the page's last record hands its bytes back, and the allocator has to hear all of them")
          .isGreaterThanOrEqualTo(Math.max(hintWhileFull, 0) + FILLER_SIZE);
      checkDatabase();
    }

    /**
     * The case that decides where the accounting belongs: a slot freed in the MIDDLE of its page is not free tail at
     * the moment it is freed - the allocator hands out the tail and nothing else - so a number produced at the free
     * would be a guess, and a hint bigger than what the page can hand out costs the allocator a wasted candidate on
     * every probe. One line later the commit's compression closes the hole and the bytes are genuinely on offer, which
     * is exactly where they are now reported, and the assertion is on the whole record's worth of them.
     */
    @Test
    void aSlotFreedInTheMiddleOfItsPageIsAccountedByTheCompressionThatClosesTheHole() {
      database.transaction(() -> database.getSchema().createDocumentType(TYPE, 1).createProperty("v", Type.STRING));

      final RID[] first = new RID[1];
      database.transaction(() -> first[0] = database.newDocument(TYPE).set("v", MEDIUM).save().getIdentity());
      fillFirstPage(TYPE);

      final LocalBucket bucket = bucketOf(TYPE);
      final int hintBefore = bucket.getFreeSpaceHintForPage(0);

      database.transaction(() -> first[0].asDocument(true).delete());

      assertThat(bucket.getFreeSpaceHintForPage(0))
          .as("the hole the commit packed away is free tail now, and all of it must be on offer")
          .isGreaterThanOrEqualTo(Math.max(hintBefore, 0) + MEDIUM.length());
      checkDatabase();
    }

    /**
     * The delete workload end to end, which is what the commit-time half of the accounting is worth: churn that removes
     * two records out of three and puts back exactly as many must leave the bucket roughly the size it was, instead of
     * growing it by every page the allocator could not see the free space in.
     * <p>
     * The bound is deliberately loose - the allocator packs by best fit and a re-inserted record need not land where
     * the deleted one was - but it is far below what the bucket did before: measured on this fixture, 143 pages became
     * 238 with the statistics unfixed and 171 with them fixed.
     */
    @Test
    void deleteChurnDoesNotGrowTheBucketByThePagesItFreed() {
      final int records = 4_000;
      final String payload = "c".repeat(2_000);

      final RID[] rids = new RID[records];
      database.transaction(() -> {
        database.getSchema().createDocumentType(TYPE, 1).createProperty("v", Type.STRING);
        for (int i = 0; i < records; i++)
          rids[i] = database.newDocument(TYPE).set("v", payload).save().getIdentity();
      });

      final LocalBucket bucket = bucketOf(TYPE);
      final int pagesAfterTheLoad = bucket.getTotalPages();
      assertThat(pagesAfterTheLoad).as("the fixture must span enough pages for the churn to show").isGreaterThan(64);

      database.transaction(() -> {
        for (int i = 0; i < records; i++)
          if (i % 3 != 0)
            rids[i].asDocument(true).delete();
      });

      final int reinserted = records - (records + 2) / 3;
      database.transaction(() -> {
        for (int i = 0; i < reinserted; i++)
          database.newDocument(TYPE).set("v", payload).save();
      });

      assertThat(bucket.getTotalPages())
          .as("the pages the deletes emptied must take the new records back")
          .isLessThan(pagesAfterTheLoad * 3 / 2);
      assertThat(countRecords(TYPE)).as("and nothing may be lost on the way").isEqualTo(records);
      checkDatabase();
    }

    /**
     * The other way into the same accounting: {@code CHECK DATABASE ... COMPRESS} walks every page of every bucket and
     * compresses it with {@code forceWipeOut}, which the commit path never does. Because that walk measures each page it
     * touches, it also REPAIRS a free-space entry that has drifted - which is what this asserts, by planting a wrong one
     * first. Without the plant the assertion would hold whether or not the compression reported anything, since the
     * pages a commit leaves behind are already packed and already accounted.
     */
    @Test
    void checkDatabaseCompressRemeasuresThePagesItWalks() {
      database.transaction(() -> database.getSchema().createDocumentType(TYPE, 1).createProperty("v", Type.STRING));

      final LocalBucket bucket = bucketOf(TYPE);
      sealFirstPage(TYPE);
      assertThat(bucket.getFreeSpaceHintForPage(0)).as("a page a spill has sealed has nothing left to offer")
          .isEqualTo(-1);

      // Tell the allocator page 0 is half empty. It is not: the spill took its last byte.
      final JSONArray planted = new JSONArray();
      planted.put(new JSONObject().put("id", 0).put("free", wholeUsablePage(bucket) / 2));
      bucket.setPageStatistics(planted);
      assertThat(bucket.getFreeSpaceHintForPage(0)).as("the fixture must really have planted a wrong entry").isPositive();

      database.command("sql", "check database compress").close();

      assertThat(bucket.getFreeSpaceHintForPage(0))
          .as("the compress walked page 0 and must have replaced the drifted entry with what it measured")
          .isEqualTo(-1);
      checkDatabase();
    }

    /** A record big enough to spill into a chain of its own, on a bucket that holds nothing else. */
    private RID createSpilledRecord() {
      final RID[] big = new RID[1];
      database.transaction(() -> {
        database.getSchema().createDocumentType(TYPE, 1).createProperty("v", Type.STRING);
        big[0] = database.newDocument(TYPE).set("v", HUGE).save().getIdentity();
      });

      final Map<String, Object> layout = bucketStats(TYPE);
      assertThat((Long) layout.get("totalChunks")).as("the fixture must really be a chain: " + layout).isPositive();
      return big[0];
    }

    /** What an EMPTY page of this bucket has to offer: everything past the page header and the slot table. */
    private static int wholeUsablePage(final LocalBucket bucket) {
      return bucket.getPageSize() - BasePage.PAGE_HEADER_SIZE - bucket.contentHeaderSize;
    }
  }

  /**
   * Issue #10709: a bulk delete gives room back on every page of the bucket, but the free-space map lists at most
   * {@code MAX_PAGES_GATHER_STATS} (100) pages: the room of the others is turned away when the map is full. Only a gather scan
   * finds it again, and the gather was held back by its 5 s throttle. So an insert right after the delete drained the 100 listed
   * pages and then extended the file next to pages with room, while the same insert 6 s later did not: where the rows landed
   * depended on the wall clock, not on the data.
   * <p>
   * Room the full map turned away is now a reason for the gather like a scan cut short by the full map is (#8660): the gather
   * looks there before the file grows, whatever the clock says. The throttle is restarted through a hook right before the
   * insert, so the test does not depend on how long its own setup took.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  // #10709
  @Nested
  class ReinsertAfterBulkDelete extends TestHelper {
    private static final int    RECORDS = 40_000;
    private static final String PAYLOAD = "x".repeat(300);

    @Test
    void aReinsertRightAfterABulkDeleteReusesTheRoomTheFullMapTurnedAway() {
      final LocalBucket bucket = fillThenDeleteThreeRowsInTen();
      final int pagesAfterFill = bucket.getTotalPages();
      // the premise: more pages got room back than the map can list
      assertThat(pagesAfterFill).isGreaterThan(150);

      bucket.restartGatherThrottleForTesting();
      // half of the room the delete gave back, so every row fits on the old pages
      insert(RECORDS, RECORDS * 3 / 20);

      assertThat(bucket.getTotalPages()).as("pages after the reinsert, %d before it", pagesAfterFill).isEqualTo(pagesAfterFill);
      assertThat(database.countType("Doc", false)).isEqualTo(RECORDS - RECORDS * 3 / 10 + RECORDS * 3 / 20);
    }

    /**
     * The reason is spent by the gather it asks for: the scan reads the file once, and the inserts that follow on a bucket with no
     * room left do not pay one scan each.
     */
    @Test
    void theRoomTurnedAwayAsksForOneGatherNotOnePerAllocation() {
      final LocalBucket bucket = fillThenDeleteThreeRowsInTen();
      final int pagesAfterFill = bucket.getTotalPages();
      bucket.restartGatherThrottleForTesting();
      final long scansBefore = bucket.getGatherScanCountForTesting();
      final long pagesReadBefore = bucket.getGatherPagesReadForTesting();

      // twice the room the delete gave back: the old pages fill up and the rest of the rows go to new pages
      insert(RECORDS, RECORDS * 6 / 10);

      assertThat(bucket.getTotalPages()).isGreaterThan(pagesAfterFill);
      final long scans = bucket.getGatherScanCountForTesting() - scansBefore;
      final long pagesRead = bucket.getGatherPagesReadForTesting() - pagesReadBefore;
      assertThat(pagesRead).as("pages the gather read in %d scans over a bucket of %d pages", scans, pagesAfterFill)
          .isLessThanOrEqualTo(2L * pagesAfterFill);
    }

    /**
     * A gather that fails backs off like any other, so it must not lose where the room is: the scan that runs once the timeout is
     * over starts at the page the full map turned away, not wherever the cursor was.
     */
    @Test
    void aGatherThatFailsStillStartsTheNextOneAtTheRoomTurnedAway() {
      // the delete leaves the first quarter of the file alone, so the room turned away lies past the page the cursor is on
      final LocalBucket bucket = fillThenDelete("a % 10 < 3 AND a >= " + RECORDS / 4);
      final int roomFrom = bucket.getUnlistedRoomFromForTesting();
      // the premise: the full map turned room away, past the page the cursor is on
      assertThat(roomFrom).isGreaterThan(bucket.getGatherResumePageForTesting());

      final int fileId = bucket.getFileId();
      PageManager.setPageReadFaultInjector(pageId -> {
        if (pageId.getFileId() == fileId)
          throw new IOException("injected by ReinsertAfterBulkDelete");
      });
      try {
        bucket.gatherPageStatistics();
      } finally {
        PageManager.setPageReadFaultInjector(null);
      }

      assertThat(bucket.getGatherResumePageForTesting()).isEqualTo(roomFrom);
    }

    private LocalBucket fillThenDeleteThreeRowsInTen() {
      return fillThenDelete("a % 10 < 3");
    }

    private LocalBucket fillThenDelete(final String where) {
      database.getSchema().buildDocumentType().withName("Doc").withTotalBuckets(1).create();
      insert(0, RECORDS);
      database.transaction(() -> database.command("sql", "DELETE FROM Doc WHERE " + where).close());
      return (LocalBucket) database.getSchema().getType("Doc").getBuckets(false).get(0);
    }

    private void insert(final int from, final int count) {
      for (int c = 0; c < count; c += 5_000) {
        final int start = c;
        database.transaction(() -> {
          for (int i = start; i < Math.min(count, start + 5_000); i++)
            database.newDocument("Doc").set("a", from + i, "payload", PAYLOAD).save();
        });
      }
    }
  }

  /**
   * Issue #8660: space freed by a bulk delete was reused for one batch of pages (the ~100 the free-space map held) and never
   * again, because near-full pages stayed in the map and the map was only re-gathered when empty. Deleting 90% of a bucket and
   * loading the same number of records back nearly doubled the files.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  // #8660
  @Nested
  class BucketSpaceReuse extends TestHelper {
    private static final int TOTAL = 300_000;
    private static final int CHUNK = 54_000;

    @Tag("slow")
    @Test
    void bulkRefillAfterBulkDeleteReusesTheFreedPages() {
      final String pad = "x".repeat(100);
      database.command("sql", "CREATE DOCUMENT TYPE D");
      database.command("sql", "CREATE PROPERTY D.id LONG");
      database.command("sql", "CREATE INDEX ON D (id) NOTUNIQUE");
      insert(0, TOTAL, pad);

      for (long lo = 0; lo < TOTAL; lo += 10_000) {
        final long from = lo, to = lo + 10_000;
        database.transaction(() -> database.command("sql", "DELETE FROM D WHERE id >= ? AND id < ? AND id % 10 <> 0", from, to));
      }

      reopenDatabase();
      LocalBucket bucket = (LocalBucket) database.getSchema().getType("D").getBuckets(false).get(0);
      final int pagesBefore = bucket.getTotalPages();

      long next = TOTAL;
      for (int chunk = 0; chunk < 5; chunk++) {
        insert(next, next + CHUNK, pad);
        next += CHUNK;
      }

      bucket = (LocalBucket) database.getSchema().getType("D").getBuckets(false).get(0);
      // 270,000 records back into a bucket that lost 270,000: the files should stay close to their size, not double
      assertThat(bucket.getTotalPages()).as("pages before the refill: %d", pagesBefore).isLessThan((int) (pagesBefore * 1.25));
      assertThat(database.countType("D", false)).isEqualTo(TOTAL / 10 + 5L * CHUNK);
    }

    private void insert(final long from, final long to, final String pad) {
      database.begin();
      for (long i = from; i < to; i++) {
        final MutableDocument d = database.newDocument("D").set("id", i).set("pad", pad);
        d.save();
        if ((i + 1) % 10_000 == 0) {
          database.commit();
          database.begin();
        }
      }
      database.commit();
    }
  }

  /**
   * Issue #8660 follow-up: a page whose record table is full cannot take another record, whatever its free tail is, and the
   * allocator drops it from the free-space map on first sight. The gather used to list it anyway (the free-space test looked at
   * bytes only), so a bucket of small records filled the map with entries that could never be used. The map drained one entry
   * per allocation and every refill was an unthrottled resume gather: a bulk UPDATE over 633K small vertices ran 27K gather
   * scans (5.7M page reads, 80s) where 8 are enough (3s).
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  // #8660
  @Nested
  class GatherSkipsFullRecordTable extends TestHelper {
    private static final int RECORDS = 300_000;

    @Test
    void gatherDoesNotListPagesWithAFullRecordTable() throws Exception {
      createTinyRecords();
      reopenDatabase();
      final LocalBucket bucket = tinyBucket();

      // the premise: tiny records exhaust the record table long before the page's bytes
      assertThat(bucket.getTotalPages()).isGreaterThan(5);

      bucket.gatherPageStatistics();

      final DatabaseInternal databaseInternal = (DatabaseInternal) database;
      for (final int pageId : bucket.getListedFreeSpacePagesForTesting()) {
        final BasePage page = databaseInternal.getTransaction()
            .getPage(new PageId(databaseInternal, bucket.getFileId(), pageId), bucket.getPageSize());
        assertThat((int) page.readShort(LocalBucket.PAGE_RECORD_COUNT_IN_PAGE_OFFSET))
            .as("page %d is listed in the free-space map but its record table (%d slots) is full", pageId,
                bucket.getMaxRecordsInPage()).isLessThan(bucket.getMaxRecordsInPage());
      }
    }

    /**
     * Statistics persisted before a full record table was kept out of the map are restored by setPageStatistics as they
     * were written. A gather visiting such a page has to take the entry out, not only decline to add one.
     */
    @Test
    void aGatherDropsAStaleEntryForAPageThatCannotTakeARecord() {
      createTinyRecords();
      reopenDatabase();
      final LocalBucket bucket = tinyBucket();

      // page 0 is full by record count; a stale hint claims a few bytes of free tail
      bucket.setPageStatistics(new JSONArray().put(new JSONObject().put("id", 0).put("free", 1)));
      assertThat(bucket.getListedFreeSpacePagesForTesting()).containsExactly(0);

      bucket.gatherPageStatistics();

      assertThat(bucket.getListedFreeSpacePagesForTesting()).doesNotContain(0);
    }

    /**
     * The bulk update grows every record, so each one needs a new home and asks the allocator for space. With a map that
     * holds only usable pages that is a handful of gather scans for the whole statement; with the map full of pages that
     * cannot take a record it was one scan every ~20 allocations (hundreds here, tens of thousands on a real graph).
     * Counting scans needs no clock, so a stalled JVM cannot flip it.
     */
    @Test
    void aBulkGrowingUpdateOverSmallRecordsDoesNotRescanTheBucketPerAllocation() {
      createTinyRecords();
      reopenDatabase();
      final LocalBucket bucket = tinyBucket();
      final long scansBefore = bucket.getGatherScanCountForTesting();

      database.transaction(() -> database.command("sql", "UPDATE Tiny SET grown = 'a value that widens every record'"));

      final long scans = bucket.getGatherScanCountForTesting() - scansBefore;
      assertThat(database.countType("Tiny", false)).isEqualTo(RECORDS);
      assertThat(scans).as("gather scans during a bulk update of %d records over %d pages", RECORDS, bucket.getTotalPages())
          .isLessThanOrEqualTo(MAX_GATHER_SCANS);
    }

    private static final long MAX_GATHER_SCANS = 50;

    private void createTinyRecords() {
      database.getSchema().createDocumentType("Tiny", 1);
      database.begin();
      for (int i = 0; i < RECORDS; i++) {
        final MutableDocument d = database.newDocument("Tiny").set("i", i);
        d.save();
        if ((i + 1) % 5_000 == 0) {
          database.commit();
          database.begin();
        }
      }
      database.commit();
    }

    private LocalBucket tinyBucket() {
      return (LocalBucket) database.getSchema().getType("Tiny").getBuckets(false).get(0);
    }
  }

  /**
   * Issue #7121: {@code arcadedb.bucketReuseSpaceMode} is declared {@code SCOPE.DATABASE} and {@code ALTER DATABASE}
   * persists it per database, but both {@link LocalBucket} constructors read the JVM-global
   * {@link GlobalConfiguration} value. An operator who tuned one database to {@code low} for write throughput saw no
   * change, and the persisted schema recorded a value the engine ignored.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  // #7121
  @Nested
  @Isolated
  class BucketReuseSpaceMode {
    private static final String DB_PATH = "./target/databases/Issue7121ReuseMode";

    @AfterEach
    void cleanUp() {
      GlobalConfiguration.BUCKET_REUSE_SPACE_MODE.reset();
      FileUtils.deleteRecursively(new File(DB_PATH));
    }

    @Test
    void theDatabasesOwnValueWinsOverTheProcessWideOne() {
      // The process-wide default says HIGH; the database asks for LOW through the documented DATABASE scope
      GlobalConfiguration.BUCKET_REUSE_SPACE_MODE.setValue("high");

      final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
      factory.getContextConfiguration().setValue(GlobalConfiguration.BUCKET_REUSE_SPACE_MODE, "low");

      try (final Database db = factory.create()) {
        db.transaction(() -> db.getSchema().createDocumentType("Reuse"));

        assertThat(bucketOf(db).reuseSpaceModeName())
            .as("the bucket must honour the database's own setting, not the JVM-wide one")
            .isEqualTo("LOW");
      }

      // And again on the reopen path, which is the other constructor
      final DatabaseFactory reopen = new DatabaseFactory(DB_PATH);
      reopen.getContextConfiguration().setValue(GlobalConfiguration.BUCKET_REUSE_SPACE_MODE, "low");
      try (final Database db = reopen.open()) {
        assertThat(bucketOf(db).reuseSpaceModeName())
            .as("the load constructor read the global value too")
            .isEqualTo("LOW");
      }

      assertThat(GlobalConfiguration.BUCKET_REUSE_SPACE_MODE.getValueAsString())
          .as("and the per-database value must not leak into the process-wide default")
          .isEqualTo("high");
    }

    /**
     * The scenario #7121 actually describes: an operator tunes ONE database with {@code ALTER DATABASE} and expects it
     * to survive a restart. That exercises the {@code configuration.json} round trip - {@code saveConfiguration()} on
     * the way out, {@code fromJSON()} on the way back in - rather than only the in-memory config object, and it is the
     * path on which the persisted schema used to record a value the engine then ignored.
     */
    @Test
    void aValuePersistedByAlterDatabaseSurvivesAReopen() {
      GlobalConfiguration.BUCKET_REUSE_SPACE_MODE.setValue("high");

      try (final Database db = new DatabaseFactory(DB_PATH).create()) {
        db.transaction(() -> db.getSchema().createDocumentType("Reuse"));
        db.command("sql", "ALTER DATABASE `arcadedb.bucketReuseSpaceMode` 'low'");
      }

      // A plain factory: nothing but the persisted configuration can carry the value now
      try (final Database db = new DatabaseFactory(DB_PATH).open()) {
        assertThat(db.getConfiguration().getValueAsString(GlobalConfiguration.BUCKET_REUSE_SPACE_MODE))
            .as("ALTER DATABASE must persist the setting into the database's own configuration")
            .isEqualTo("low");
        assertThat(bucketOf(db).reuseSpaceModeName())
            .as("and the reopened bucket must be built from it, not from the JVM-wide default")
            .isEqualTo("LOW");
      }

      assertThat(GlobalConfiguration.BUCKET_REUSE_SPACE_MODE.getValueAsString())
          .as("ALTER DATABASE tunes ONE database: it must not write through to the process-wide default")
          .isEqualTo("high");
    }

    private static LocalBucket bucketOf(final Database db) {
      return (LocalBucket) db.getSchema().getType("Reuse").getBuckets(false).getFirst();
    }
  }

  /**
   * Issue #8401: the disjoint-slot merge of a record DELETE ({@code rebaseRecordDeleteOnPage}) measured the page's free
   * tail through the allocator's {@code getFreeSpaceInPage}, which gives up without measuring anything on a page whose
   * slot table is full - it only has to tell the allocator "look elsewhere" - and leaves the free space at its -1
   * default. That -1 went to the free-space statistics as if it were a measurement, tripping the {@code
   * updatePageStatistics} tripwire under {@code -ea} (and failing the commit with it). Small records reach that shape
   * routinely: 2048 edges of ~14 bytes fill the slot table of a 64 KB page with half of its bytes still free.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  // #8401
  @Nested
  class RebasedDeleteOnFullSlotTable extends BucketPageLayoutTestSupport {
    private static final String TYPE = "Tiny";

    @Test
    void aRebasedDeleteOnAPageWithAFullSlotTableReportsTheRealFreeTail() {
      database.transaction(() -> database.getSchema().createDocumentType(TYPE, 1).createProperty("v", Type.STRING));
      final LocalBucket bucket = bucketOf(TYPE);
      final int slots = bucket.getMaxRecordsInPage();

      final RID[] rids = new RID[slots];
      database.transaction(() -> {
        for (int i = 0; i < slots; i++)
          rids[i] = database.newDocument(TYPE).set("v", "a").save().getIdentity();
      });

      // The premise, proved rather than assumed: page 0 has run out of SLOTS while it still has plenty of BYTES.
      assertThat(rids[slots - 1].getPosition()).as("every record must be on page 0").isEqualTo(slots - 1L);
      final int freeTailBefore = freeTailOfFirstPage(bucket);
      assertThat(freeTailBefore).as("page 0 must still have free bytes, it is the slot table that is full").isGreaterThan(1024);

      final RID deleted = rids[10];
      final RID neighbour = rids[20];

      database.begin();
      deleted.asDocument(true).delete();

      // A concurrent commit to ANOTHER record of page 0 makes this transaction's page image stale, so its commit has to
      // replay the delete onto the newer page: rebaseRecordDeleteOnPage.
      inAnotherThread(() -> database.transaction(() -> neighbour.asDocument(true).modify().set("v", "b").save()));

      database.commit();

      database.transaction(() -> {
        assertThat(database.existsRecord(deleted)).as("the delete must have been merged, not lost").isFalse();
        assertThat(neighbour.asDocument(true).getString("v")).as("the concurrent update must survive").isEqualTo("b");
      });

      final int freeTailAfter = freeTailOfFirstPage(bucket);
      assertThat(freeTailAfter).as("deleting a record never takes bytes away from the free tail").isGreaterThanOrEqualTo(freeTailBefore);
      final int hint = bucket.getFreeSpaceHintForPage(0);
      assertThat(hint).as("the statistics must hold the free tail the page really has, or nothing").isIn(-1, freeTailAfter);

      checkDatabase();
    }

    /**
     * The same short-circuit, in the other place that borrowed the allocator's analysis as a measurement: with
     * {@code bucketWipeOutOnDelete} (the default) the compaction a delete triggers zeroes the free tail it leaves behind,
     * so the bytes the defragmentation moved down are not left in the file a second time. On a page with a full slot
     * table the wipe measured nothing and wiped nothing, leaving a stale copy of the records past the new end.
     */
    @Test
    void theCompactionAfterADeleteWipesTheFreeTailOfAPageWithAFullSlotTable() {
      database.transaction(() -> database.getSchema().createDocumentType(TYPE, 1).createProperty("v", Type.STRING));
      final LocalBucket bucket = bucketOf(TYPE);
      final int slots = bucket.getMaxRecordsInPage();

      final RID[] rids = new RID[slots];
      database.transaction(() -> {
        for (int i = 0; i < slots; i++)
          rids[i] = database.newDocument(TYPE).set("v", "wipe-me-" + i).save().getIdentity();
      });
      assertThat(rids[slots - 1].getPosition()).as("every record must be on page 0").isEqualTo(slots - 1L);

      // A record in the MIDDLE: the compaction closes its hole by moving everything after it down, which is what leaves
      // the old copy of the page's last bytes past the new content end.
      database.transaction(() -> rids[slots / 2].asDocument(true).delete());

      database.transaction(() -> {
        try {
          final BasePage page = ((DatabaseInternal) database).getTransaction()
              .getPage(new PageId(database, bucket.getFileId(), 0), bucket.getPageSize());
          final int contentEnd = page.getMaxContentSize() - freeTailOf(page);
          for (int offset = contentEnd; offset < page.getMaxContentSize(); offset++)
            assertThat(page.readByte(offset)).as("free tail byte at offset %d of %d must be wiped", offset, page.getMaxContentSize())
                .isZero();
        } catch (final IOException e) {
          throw new AssertionError(e);
        }
      });

      checkDatabase();
    }

    /** The free tail of page 0 derived from the record table, independently of any statistics. */
    private int freeTailOfFirstPage(final LocalBucket bucket) {
      final int[] tail = new int[1];
      database.transaction(() -> {
        try {
          tail[0] = freeTailOf(((DatabaseInternal) database).getTransaction()
              .getPage(new PageId(database, bucket.getFileId(), 0), bucket.getPageSize()));
        } catch (final IOException e) {
          throw new AssertionError(e);
        }
      });
      return tail[0];
    }

    /** Free tail of a page derived from its record table; every record of these fixtures is a plain one. */
    private static int freeTailOf(final BasePage page) {
      final int records = page.readShort(LocalBucket.PAGE_RECORD_COUNT_IN_PAGE_OFFSET);
      int contentEnd = 0;
      for (int i = 0; i < records; i++) {
        final int offset = (int) page.readUnsignedInt(LocalBucket.PAGE_RECORD_TABLE_OFFSET + i * Binary.INT_SERIALIZED_SIZE);
        if (offset == 0)
          continue;
        // A plain record's footprint is its size varint plus the size it declares
        final long[] size = page.readNumberAndSize(offset);
        contentEnd = Math.max(contentEnd, (int) (offset + size[1] + size[0]));
      }
      return page.getMaxContentSize() - contentEnd;
    }
  }

  /**
   * Issue #8982: under READ_COMMITTED, a read-modify-write of a document stored as a multi-page record silently overwrote
   * a change committed between the read and the update. The #6950 guard compared the image the update was computed from
   * with the slot, but a head chunk holds only the start of the record, so it answered "unchanged" without looking.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  // hang detector only, not a latency bound
  // #8982
  @Nested
  @Timeout(120)
  class LostUpdateLargeDocument extends TestHelper {

    private RID createDocument(final int size) {
      database.getSchema().getOrCreateDocumentType("Doc");
      final RID[] rid = new RID[1];
      database.transaction(() -> rid[0] = database.newDocument("Doc").set("n", 0).set("s", "x".repeat(size)).save().getIdentity());
      return rid[0];
    }

    @Test
    void staleReadModifyWriteIsRefusedForPlainAndMultiPageDocuments() throws Exception {
      final ExecutorService threadA = Executors.newSingleThreadExecutor();
      final ExecutorService threadB = Executors.newSingleThreadExecutor();
      try {
        for (final int size : new int[] { 30_000, 70_000, 300_000 }) {
          final RID rid = createDocument(size);
          final Document[] read = new Document[1];
          threadA.submit(() -> {
            database.begin();
            read[0] = database.lookupByRID(rid, true).asDocument();
          }).get();

          threadB.submit(() -> database.transaction(() -> {
            final MutableDocument d = database.lookupByRID(rid, true).asDocument().modify();
            d.set("n", d.getInteger("n") + 1);
            d.save();
          })).get();

          assertThatThrownBy(() -> threadA.submit(() -> {
            try {
              final MutableDocument d = read[0].modify();
              d.set("n", read[0].getInteger("n") + 1);
              d.save();
              database.commit();
            } finally {
              if (database.isTransactionActive())
                database.rollback();
            }
            return null;
          }).get()).as("size " + size).hasRootCauseInstanceOf(ConcurrentModificationException.class);

          assertThat(database.lookupByRID(rid, true).asDocument().getInteger("n")).as("size " + size).isEqualTo(1);
        }
      } finally {
        threadA.shutdown();
        threadB.shutdown();
      }
    }

    @Test
    void concurrentCountersOnMultiPageDocumentLoseNoIncrement() throws Exception {
      final RID rid = createDocument(70_000);
      final int threads = 4;
      final int perThread = 100;
      final AtomicInteger committed = new AtomicInteger();
      final Thread[] workers = new Thread[threads];
      final Throwable[] failure = new Throwable[1];
      for (int t = 0; t < threads; t++) {
        workers[t] = new Thread(() -> {
          try {
            for (int i = 0; i < perThread; i++) {
              database.transaction(() -> {
                final MutableDocument d = database.lookupByRID(rid, true).asDocument().modify();
                d.set("n", d.getInteger("n") + 1);
                d.save();
              }, false, 1000);
              committed.incrementAndGet();
            }
          } catch (final Throwable e) {
            failure[0] = e;
          }
        });
        workers[t].start();
      }
      for (final Thread w : workers)
        w.join();

      assertThat(failure[0]).isNull();
      assertThat(database.lookupByRID(rid, true).asDocument().getInteger("n")).isEqualTo(committed.get()).isEqualTo(threads * perThread);
    }
  }

  /**
   * Pins {@link LocalBucket#hasRecordChangedSinceRead(RID, BasePage, Binary)} branch by branch (#6950).
   * <p>
   * The method is the guard that turns a read-modify-write computed from a replaced record image into a retryable
   * conflict, and it deliberately FAILS OPEN: every shape it cannot compare - and any exception while reading the page -
   * answers "unchanged", so that a write which would otherwise succeed is never refused by the guard itself. That is
   * also what makes it worth pinning directly: a refactor that widened one of those "unchanged" answers would silently
   * reopen the lost-update this test's sibling
   * ({@code com.arcadedb.query.sql.executor.ConcurrentUpdateStatementLostUpdateTest}) exists to close, without failing
   * anything.
   */
  // #6950
  @Nested
  class RecordImageGuard extends TestHelper {
    private static final String TYPE = "Guarded";

    @Test
    void anUnchangedPlainRecordIsNotReportedAsChanged() {
      final RID rid = createRecord("aaa");
      final Binary image = imageOf(rid);

      database.transaction(() -> assertThat(guard(rid, image))
          .as("the very image the record still holds")
          .isFalse());
    }

    @Test
    void aPlainRecordRewrittenWithTheSameLengthIsReportedAsChanged() {
      final RID rid = createRecord("aaa");
      final Binary image = imageOf(rid);

      replaceValueWith(rid, "bbb");

      database.transaction(() -> assertThat(guard(rid, image))
          .as("same length, different bytes: a length check alone would miss this one")
          .isTrue());
    }

    @Test
    void aPlainRecordRewrittenWithADifferentLengthIsReportedAsChanged() {
      final RID rid = createRecord("aaa");
      final Binary image = imageOf(rid);

      replaceValueWith(rid, "a much longer value than the one the image was taken from");

      database.transaction(() -> assertThat(guard(rid, image)).isTrue());
    }

    @Test
    void aDeletedRecordIsNotReportedAsChanged() {
      final RID rid = createRecord("aaa");
      final Binary image = imageOf(rid);

      database.transaction(() -> database.lookupByRID(rid, true).delete());

      database.transaction(() -> assertThat(guard(rid, image))
          .as("a vanished record is reported by the commit's own check (#4959), not by this guard")
          .isFalse());
    }

    @Test
    void aNeverUsedSlotIsNotReportedAsChanged() {
      final RID rid = createRecord("aaa");
      final Binary image = imageOf(rid);
      final RID unusedSlot = new RID(rid.getBucketId(), rid.getPosition() + 1);

      database.transaction(() -> assertThat(guard(unusedSlot, image))
          .as("a slot past the page's record count carries no record to compare")
          .isFalse());
    }

    @Test
    void aMultiPageRecordIsReportedAsChangedWhenItWas() {
      final RID rid = createRecord("x".repeat(200_000));
      final Binary image = imageOf(rid);

      database.transaction(() -> assertThat(guard(rid, image)).as("an untouched multi-page record is the one that was read").isFalse());

      replaceValueWith(rid, "y".repeat(200_000));

      database.transaction(() -> assertThat(guard(rid, image))
          .as("a head chunk keeps its body off this page, so the record is assembled and compared whole (#8985)")
          .isTrue());
    }

    @Test
    void aMultiPageRecordIsNotReportedAsChangedWhenItWasNot() {
      final RID rid = createRecord("x".repeat(200_000));
      final Binary image = imageOf(rid);

      database.transaction(() -> assertThat(guard(rid, image)).isFalse());
    }

    private boolean guard(final RID rid, final Binary image) {
      final LocalBucket bucket = (LocalBucket) database.getSchema().getBucketById(rid.getBucketId());
      try {
        return bucket.hasRecordChangedSinceRead(rid, bucket.fetchPageInTransaction(rid), image);
      } catch (final Exception e) {
        throw new IllegalStateException("Unable to load the page of " + rid, e);
      }
    }

    private RID createRecord(final String value) {
      final RID[] rid = new RID[1];
      database.transaction(() -> {
        if (!database.getSchema().existsType(TYPE))
          database.getSchema().createDocumentType(TYPE).createProperty("v", Type.STRING);
        rid[0] = database.newDocument(TYPE).set("v", value).save().getIdentity();
      });
      return rid[0];
    }

    private void replaceValueWith(final RID rid, final String value) {
      database.transaction(() -> {
        final MutableDocument doc = database.lookupByRID(rid, true).asDocument().modify();
        doc.set("v", value).save();
      });
    }

    /** The committed image of {@code rid}, detached from the page it was read from. */
    private Binary imageOf(final RID rid) {
      final Binary[] image = new Binary[1];
      database.transaction(
          () -> image[0] = ((RecordInternal) database.lookupByRID(rid, true).asDocument().modify()).getBuffer().copyOfContent());
      return image[0];
    }
  }

  /**
   * Issue #6399 (item 1): {@link PaginatedComponent}'s constructor derives its page count as {@code fileSize / pageSize},
   * which FLOORS. Nothing anywhere asked whether there was a remainder, so a file killed part way through writing a page
   * kept a partial tail that no component counted, no check reported and nothing repaired - simply rounded away at every
   * open, forever.
   * <p>
   * This is the natural third sibling of the file-id (#6283) and page-size (#6314) guards that live ten lines above it
   * in the same constructor, but unlike them it is NOT a tripwire: those two fire on states that are programming errors
   * and cannot exist in a file a correct build wrote, while a torn tail is exactly what a power cut produces, and a
   * database a previous build opened happily must keep opening. The fix is a WARNING logged at open instead, and for a
   * bucket or index (as opposed to the Dictionary, which #6355 fixed separately because it reads file content, not just
   * a count) the torn bytes cost nothing beyond disk space: the next page appended to the file lands at
   * {@code pageCount * pageSize} and overwrites them wholesale.
   * <p>
   * In package {@code com.arcadedb.engine} on purpose: reaching a component's {@link File}/{@link PaginatedComponentFile}
   * needs package access.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  // #6399
  @Nested
  class PartialTailPage extends TestHelper {

    @Test
    void aBucketFileWithATornTailPageWarnsButStillOpensAndStaysFullyUsable() throws Exception {
      final DatabaseInternal db = (DatabaseInternal) database;

      db.getSchema().createDocumentType("Issue6399Type", 1);
      final RID[] savedRid = new RID[1];
      db.transaction(() -> savedRid[0] = db.newDocument("Issue6399Type").set("id", 1).save().getIdentity());

      final LocalBucket bucket = (LocalBucket) db.getSchema().getType("Issue6399Type").getBuckets(false).getFirst();
      final int pageSize = bucket.getPageSize();
      final int wholePages = bucket.getTotalPages();
      assertThat(wholePages).as("premise: at least the header page is committed").isPositive();

      final File bucketFile = bucket.getOSFile();
      final String path = database.getDatabasePath();

      assertThat(db.getPageManager().waitAllPagesOfDatabaseAreFlushed(db)).isTrue();
      db.close();

      // TORN TAIL: a whole page plus a stub that is not one, exactly what a kill mid-write leaves behind.
      final long tornLength = (long) wholePages * pageSize + 37;
      try (final RandomAccessFile raw = new RandomAccessFile(bucketFile, "rw")) {
        raw.setLength(tornLength);
      }

      final LogCapture capture = attach(LocalBucket.class);
      Database reopened = null;
      try {
        reopened = new DatabaseFactory(path).open();

        // THE WARNING FIRED AT OPEN, NOT A THROWN EXCEPTION: this is a report, not a tripwire.
        assertThat(capture.handler.snapshot().stream()
            .anyMatch(m -> m.contains("not a multiple of its page size") && m.contains("37")))
            .as("expected a WARNING about the torn tail; got: %s", capture.handler.snapshot())
            .isTrue();

        final DatabaseInternal reopenedInternal = (DatabaseInternal) reopened;
        final LocalBucket reopenedBucket =
            (LocalBucket) reopenedInternal.getSchema().getType("Issue6399Type").getBuckets(false).getFirst();

        // THE TORN BYTES WERE ROUNDED AWAY, NOT COUNTED AS AN EXTRA PAGE
        assertThat(reopenedBucket.getTotalPages()).isEqualTo(wholePages);

        // THE DATABASE IS FULLY USABLE: THE PRE-EXISTING RECORD IS STILL DIRECTLY LOADABLE BY ITS OWN RID (GROUND
        // TRUTH - NOT A CACHED/RECOMPUTED AGGREGATE COUNT) AND THE TYPE'S COUNT AGREES WITH IT
        assertThat(reopenedInternal.lookupByRID(savedRid[0], true)).isNotNull();
        assertThat(reopenedInternal.countType("Issue6399Type", false)).isEqualTo(1);

        // ENOUGH RECORDS TO FORCE A REAL PAGE 1 TO BE ALLOCATED AND WRITTEN - THE SAME PAGE NUMBER THE TORN TAIL
        // STUBBED. ITS WRITE LANDS AT `pageNumber * pageSize` REGARDLESS OF WHAT WAS THERE (PaginatedComponentFile#write),
        // SO IT OVERWRITES THE TORN BYTES WHOLESALE RATHER THAN READING THEM: THE FIX'S WARNING PROMISES EXACTLY THIS.
        reopenedInternal.transaction(() -> {
          for (int i = 0; i < 5_000; i++)
            reopenedInternal.newDocument("Issue6399Type").set("id", i, "payload", "x".repeat(100)).save();
        });
        assertThat(reopenedInternal.getPageManager().waitAllPagesOfDatabaseAreFlushed(reopenedInternal)).isTrue();

        assertThat(reopenedBucket.getTotalPages()).as("a real page 1 must have been allocated").isGreaterThan(wholePages);
        assertThat(reopenedBucket.getComponentFile().getSize() % pageSize)
            .as("the torn stub must be gone, overwritten by the real page 1").isZero();
      } finally {
        detach(capture);
        if (reopened != null)
          reopened.drop();
      }
    }

    private static LogCapture attach(final Class<?> loggerOwner) {
      final CapturingHandler handler = new CapturingHandler();
      handler.setLevel(Level.ALL);
      final Logger logger = Logger.getLogger(loggerOwner.getName());
      final Level previousLevel = logger.getLevel();
      logger.addHandler(handler);
      logger.setLevel(Level.ALL);
      return new LogCapture(logger, handler, previousLevel);
    }

    private static void detach(final LogCapture capture) {
      capture.logger.removeHandler(capture.handler);
      capture.logger.setLevel(capture.previousLevel);
    }

    /**
     * Bundles the {@link Logger} together with its {@link CapturingHandler} so the caller keeps a strong reference to
     * the logger for the whole capture window: {@code java.util.logging.LogManager} holds registered loggers via
     * {@code WeakReference}, so a logger obtained and discarded inside a helper method could be collected - taking its
     * handler with it - before the code under test ever logs anything. {@code previousLevel} lets {@link #detach} put
     * the logger's threshold back the way it found it, rather than leaving it pinned at {@code ALL} for the rest of
     * the JVM.
     */
    private record LogCapture(Logger logger, CapturingHandler handler, Level previousLevel) {
    }

    private static final class CapturingHandler extends Handler {
      private final List<String> records = new CopyOnWriteArrayList<>();

      @Override
      public void publish(final LogRecord record) {
        if (record == null || record.getLevel().intValue() < Level.WARNING.intValue())
          return;
        String msg = record.getMessage();
        if (msg != null && record.getParameters() != null && record.getParameters().length > 0) {
          try {
            msg = msg.formatted(record.getParameters());
          } catch (final Exception ignored) {
          }
        }
        if (msg != null)
          records.add(msg);
      }

      @Override
      public void flush() {
      }

      @Override
      public void close() {
      }

      List<String> snapshot() {
        return records;
      }
    }
  }
}
