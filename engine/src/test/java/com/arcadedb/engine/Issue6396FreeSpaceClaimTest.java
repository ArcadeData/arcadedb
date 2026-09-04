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
import com.arcadedb.database.BucketPageLayoutTestSupport;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

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
class Issue6396FreeSpaceClaimTest extends BucketPageLayoutTestSupport {
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

      final MutablePage page = pageOf(rids.get(0));
      assertThat(page.getFreeSpaceClaim())
          .as("an insert must state what free tail it leaves; without a claim the commit has nothing to check")
          .isNotEqualTo(MutablePage.FREE_SPACE_CLAIM_UNKNOWN);
      assertThat(page.getFreeSpaceClaim()).isEqualTo(freeTailOf(page, rids.get(rids.size() - 1)));
    });

    // And it survives the commit that measures the same page: the two descriptions agree end to end.
    assertThat(bucket.getFreeSpaceHintForPage(0)).isEqualTo(freeTailAfterCommit(rids.get(rids.size() - 1)));
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
      assertLowerBoundOn(pageOf(rids.get(0)));
    });

    database.transaction(() -> {
      database.newDocument(TYPE).set("v", "v".repeat(512)).save();
      rids.get(rids.size() - 1).asDocument(true).delete();
      assertLowerBoundOn(pageOf(rids.get(0)));
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

      final RID first = rids.get(0);
      final RID last = rids.get(rids.size() - 1);
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
