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
import com.arcadedb.database.Binary;
import com.arcadedb.database.BucketPageLayoutTestSupport;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #6163: the HEAD chunk of a multi-page record used to size itself from what the page already said, and could
 * only cap that downwards - so a record that shrank pinned its head chunk to the smallest size it had ever had, and
 * every later update pushed the difference into continuation chunks on other pages, for good. Issue #6154: the head
 * chunk was also the one chunk never fed into the page free-space statistics, although it is the one that can take a
 * whole page's free tail in a single write.
 * <p>
 * Both are the same missing idea - the room the head chunk may use is a property of the PAGE, not of what the chunk
 * declared last time - so they are fixed and tested together.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6163HeadChunkRoomTest extends BucketPageLayoutTestSupport {
  /** Comfortably inside the room a sealed page's head chunk owns (the 8 KB filler record's own footprint). */
  private static final String MEDIUM = "m".repeat(4 * 1024);
  private static final String SMALL  = "s";

  /**
   * The ratchet itself. A record that spilled, then shrank to nothing, then grew back to a size its own room can hold
   * must be stored in that room alone. Before the fix the head chunk stayed at the byte count of the SMALL value and
   * the 4 KB went to a continuation chunk on another page - permanently, since nothing ever raised the declared size
   * again.
   * <p>
   * <b>Since #6178 the intermediate shape is not a chunk head at all.</b> A record that shrinks back inside the region
   * its slot owns is collapsed into a plain record, and the two conditions coincide exactly: content that fits the
   * head chunk (13 bytes of header) fits the region as a plain record (at most 5 bytes of size marker) by
   * construction. So the ratchet is no longer merely undone by the grow-back - the state it needed cannot be reached
   * in the first place, and the grow-back below happens on a plain record inside its page. What the test asserts is
   * unchanged and is the property that always mattered: after the round trip the 4 KB is on this page, in one piece.
   */
  @Test
  void aHeadChunkThatShrankIsSizedAgainFromTheRoomItOwns() {
    database.transaction(() -> database.getSchema().createDocumentType("Ratchet", 1).createProperty("v", Type.STRING));
    final RID chunked = sealFirstPage("Ratchet");

    database.transaction(() -> chunked.asDocument(true).modify().set("v", SMALL).save());
    Map<String, Object> layout = bucketStats("Ratchet");
    assertThat((Long) layout.get("totalChunks")).as("the shrink must free the continuation chunks").isZero();
    assertThat((Long) layout.get("totalMultiPageRecords"))
        .as("#6178: a record back inside its own region is a plain record again, not a chain of one chunk: " + layout)
        .isZero();

    database.transaction(() -> chunked.asDocument(true).modify().set("v", MEDIUM).save());

    layout = bucketStats("Ratchet");
    assertThat((Long) layout.get("totalChunks"))
        .as("the record owns the room this value fits in, so nothing may spill out of the page: " + layout).isZero();
    assertThat((Long) layout.get("totalMultiPageRecords")).as("and it did not have to spill again: " + layout).isZero();

    database.transaction(() -> assertThat(chunked.asDocument(true).getString("v")).isEqualTo(MEDIUM));
    checkDatabase();
  }

  /**
   * The oscillation the issue describes: a record that repeatedly goes small and large again must settle on one
   * physical shape and keep it, instead of paying one more continuation chunk per cycle.
   * <p>
   * The baseline is the first cycle's own result, not the shape the fixture's spill produced: the spill placed its
   * continuation chunks with whatever room the pages around it had at that moment, so a later rewrite of the chain
   * can legitimately need FEWER of them. What must not happen is the count drifting from one cycle to the next.
   */
  @Test
  void repeatedShrinkAndGrowCyclesDoNotDegradeTheRecord() {
    database.transaction(() -> database.getSchema().createDocumentType("Oscillating", 1).createProperty("v", Type.STRING));
    final RID chunked = sealFirstPage("Oscillating");

    final String large = "l".repeat(70 * 1024);
    assertThat((Long) bucketStats("Oscillating").get("totalChunks")).as("the fixture must really have spilled into a chain")
        .isPositive();

    long chunksWhenLarge = -1;
    for (int cycle = 0; cycle < 4; cycle++) {
      database.transaction(() -> chunked.asDocument(true).modify().set("v", SMALL).save());
      database.transaction(() -> chunked.asDocument(true).modify().set("v", large).save());

      final Map<String, Object> layout = bucketStats("Oscillating");
      final long chunks = (Long) layout.get("totalChunks");
      if (chunksWhenLarge < 0)
        chunksWhenLarge = chunks;
      assertThat(chunks).as("cycle " + cycle + " must not cost an extra chunk: " + layout).isEqualTo(chunksWhenLarge);
      database.transaction(() -> assertThat(chunked.asDocument(true).getString("v")).isEqualTo(large));

      // And back down to a value the head chunk's own room holds: every cycle must be able to reach the shape a
      // record that had never been shrunk has, not merely stay as bad as the cycle before it.
      database.transaction(() -> chunked.asDocument(true).modify().set("v", SMALL).save());
      database.transaction(() -> chunked.asDocument(true).modify().set("v", MEDIUM).save());
      final Map<String, Object> mediumLayout = bucketStats("Oscillating");
      assertThat((Long) mediumLayout.get("totalChunks")).as("cycle " + cycle + " must fit the head chunk again: " + mediumLayout)
          .isZero();
      database.transaction(() -> assertThat(chunked.asDocument(true).getString("v")).isEqualTo(MEDIUM));
    }
    checkDatabase();
  }

  /**
   * The bound on the grow-back, and the reason it is read from the page rather than remembered: the room the shrink
   * released is the page's again, and once another record has taken it the head chunk must stop at that record's
   * offset - not overwrite it.
   */
  @Test
  void theHeadChunkNeverGrowsPastARecordThatTookTheRoomItReleased() {
    database.transaction(() -> database.getSchema().createDocumentType("Bounded", 1).createProperty("v", Type.STRING));
    final RID chunked = sealFirstPage("Bounded");

    database.transaction(() -> chunked.asDocument(true).modify().set("v", SMALL).save());

    final String tenant = "t".repeat(2 * 1024);
    final RID[] newcomer = new RID[1];
    database.transaction(() -> newcomer[0] = database.newDocument("Bounded").set("v", tenant).save().getIdentity());
    assertThat(newcomer[0].getPosition()).as("the released tail must be handed to the next insert on the same page")
        .isEqualTo(chunked.getPosition() + 1);

    database.transaction(() -> chunked.asDocument(true).modify().set("v", MEDIUM).save());

    database.transaction(() -> {
      assertThat(chunked.asDocument(true).getString("v")).isEqualTo(MEDIUM);
      assertThat(newcomer[0].asDocument(true).getString("v")).as("the record that took the room must be untouched")
          .isEqualTo(tenant);
    });
    checkDatabase();
  }

  /**
   * #6154: the head chunk of a record that spills as the LAST record of its page takes the whole free tail, which is
   * the single largest bite anything takes out of a bucket page - and it was the one bite the free-space statistics
   * never heard about. Asserted on the hint itself: the allocator re-reads the page before using a candidate, so a
   * test driven through it would pass either way.
   */
  @Test
  void theHeadChunkIsAccountedInThePageFreeSpaceStatistics() {
    database.transaction(() -> database.getSchema().createDocumentType("Accounted", 1).createProperty("v", Type.STRING));

    final LocalBucket bucket = (LocalBucket) database.getSchema().getType("Accounted").getBuckets(false).getFirst();
    final RID chunked = sealFirstPage("Accounted");

    assertThat(bucket.getFreeSpaceHintForPage(0)).as("a page a spill has sealed has no free space left to offer")
        .isEqualTo(-1);

    // The other direction: shrinking the head chunk hands the tail back, and the statistics must hear that too.
    database.transaction(() -> chunked.asDocument(true).modify().set("v", SMALL).save());
    assertThat(bucket.getFreeSpaceHintForPage(0)).as("the tail the shrink released must be offered again").isPositive();

    checkDatabase();
  }

  /**
   * The grow-back is still a single-slot write, so the disjoint-slot merge (#5279, #6129) has to replay it on a newer
   * committed version of the page. That replay used to refuse any head chunk longer than the committed one outright;
   * it now re-derives the room from the committed page, which is what lets this commit go through.
   */
  @Test
  void aHeadChunkGrowingBackIsReplayedOnACommittedPage() throws Exception {
    final RID[] neighbourHolder = new RID[1];
    database.transaction(() -> {
      database.getSchema().createDocumentType("Merged", 1).createProperty("v", Type.STRING);
      // Written FIRST, so it is on page 0 and is never the record the fill ends up sealing the page with.
      neighbourHolder[0] = database.newDocument("Merged").set("v", "n".repeat(8 * 1024)).save().getIdentity();
    });
    final RID neighbour = neighbourHolder[0];
    final RID chunked = sealFirstPage("Merged");
    database.transaction(() -> chunked.asDocument(true).modify().set("v", SMALL).save());

    // The premise, proved rather than assumed: with the merge switched OFF the same interleaving must FAIL, which is
    // what shows the two records really do share a page.
    database.getConfiguration().setValue(GlobalConfiguration.TX_PAGE_SLOT_MERGE, false);
    try {
      assertThat(growBackSurvives(chunked, neighbour, MEDIUM, "n".repeat(8 * 1024)))//
          .as("the growing record and the neighbour must share a page").isFalse();
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.TX_PAGE_SLOT_MERGE, true);
    }

    final String rewritten = "r".repeat(8 * 1024);
    assertThat(growBackSurvives(chunked, neighbour, MEDIUM, rewritten)).isTrue();

    final Map<String, Object> layout = bucketStats("Merged");
    assertThat((Long) layout.get("totalChunks")).as("the replayed head chunk must still hold the whole value: " + layout)
        .isZero();

    database.transaction(() -> {
      assertThat(chunked.asDocument(true).getString("v")).isEqualTo(MEDIUM);
      assertThat(neighbour.asDocument(true).getString("v")).isEqualTo(rewritten);
    });
    checkDatabase();
  }

  /**
   * The contended form of the same round trip: a concurrent insert takes the tail the shrink released, and our
   * grow-back needs it back.
   * <p>
   * <b>Since #6178 this exercises the plain-record growth replay, not the head-chunk one</b>: the shrink collapses the
   * record out of its chain, so what the merge replays on the committed page is {@code growRecordInPage}, which makes
   * the room by shifting the newcomer right and therefore succeeds where the head-chunk replay - which may not move
   * anything, its footprint being fixed for the life of the record - had to refuse. The refusal arm of
   * {@code rebaseRecordOnPage}'s head-chunk branch stays where it is: it guards a region that a concurrent commit
   * shrank, and it is now unreachable through the public API for the same reason the ratchet is (a head chunk that
   * declares less than its region no longer survives an update), which makes it a defence and no longer a path.
   * <p>
   * What must hold either way, and is what this test is really about, is the last assertion: the record that took the
   * room is never overwritten.
   */
  @Test
  void aHeadChunkGrowingBackIsReplayedOverAConcurrentInsertWithoutTouchingIt() throws Exception {
    database.transaction(() -> database.getSchema().createDocumentType("Contended", 1).createProperty("v", Type.STRING));
    final RID chunked = sealFirstPage("Contended");
    database.transaction(() -> chunked.asDocument(true).modify().set("v", SMALL).save());

    final String tenant = "t".repeat(2 * 1024);
    final RID[] newcomer = new RID[1];

    database.begin();
    chunked.asDocument(true).modify().set("v", MEDIUM).save();

    final Thread concurrent = new Thread(() -> database.transaction(
        () -> newcomer[0] = database.newDocument("Contended").set("v", tenant).save().getIdentity()));
    concurrent.start();
    concurrent.join();

    assertThat(newcomer[0].getPosition()).as("the concurrent insert must take the room the shrink released")
        .isEqualTo(chunked.getPosition() + 1);

    boolean committed = false;
    try {
      database.commit();
      committed = true;
    } catch (final ConcurrentModificationException e) {
      // Also acceptable: a retry never loses data. What may not happen is a commit that overwrites the newcomer.
    } finally {
      if (database.isTransactionActive())
        database.rollback();
    }

    final String expected = committed ? MEDIUM : SMALL;
    database.transaction(() -> {
      assertThat(newcomer[0].asDocument(true).getString("v"))
          .as("the record that took the room must never be overwritten by a replayed write").isEqualTo(tenant);
      assertThat(chunked.asDocument(true).getString("v"))
          .as("and our own record must hold exactly what the outcome of the commit says").isEqualTo(expected);
    });
    checkDatabase();
  }

  /**
   * The one shape of the region that is NOT safe to write through: a region that cannot host a single byte of chunk
   * content. It says the page is corrupted - a neighbour's slot points inside this chunk's own header - and a chunk
   * size of zero or less is not a shorter chunk, it is a header no reader can walk. It must fail the transaction
   * rather than be written.
   * <p>
   * No sequence of public API calls produces this: the region always contains at least the head chunk's own
   * footprint. So the page is corrupted by hand, in the SAME transaction as the update, which is what keeps the
   * commit-time {@code compressPage} from re-flowing the overlap before the update runs.
   */
  @Test
  void aRegionTooSmallForAnyContentIsRefusedRatherThanWritten() throws Exception {
    final RID[] chunked = new RID[1];
    database.transaction(() -> {
      database.getSchema().createDocumentType("Corrupted", 1).createProperty("v", Type.STRING);
      // Written FIRST so the record that spills has neighbours AFTER it: its region is bounded by one of them.
      chunked[0] = database.newDocument("Corrupted").set("v", "p").save().getIdentity();
    });
    fillFirstPage("Corrupted");
    database.transaction(() -> chunked[0].asDocument(true).modify().set("v", "b".repeat(200 * 1024)).save());
    assertThat((Long) bucketStats("Corrupted").get("totalMultiPageRecords")).as("the fixture must have spilled")
        .isEqualTo(1L);

    final LocalBucket bucket = (LocalBucket) database.getSchema().getType("Corrupted").getBuckets(false).getFirst();

    database.begin();
    try {
      final DatabaseInternal db = (DatabaseInternal) database;
      final MutablePage page = db.getTransaction()
          .getPageToModify(new PageId(db, bucket.getFileId(), 0), bucket.getPageSize(), false);
      final int headChunkPosition = (int) page.readUnsignedInt(
          LocalBucket.PAGE_RECORD_TABLE_OFFSET + (int) chunked[0].getPosition() * Binary.INT_SERIALIZED_SIZE);

      // Point the NEXT slot one byte past the head chunk's marker: its region can then hold no content at all.
      final int recordCountInPage = page.readShort(LocalBucket.PAGE_RECORD_COUNT_IN_PAGE_OFFSET);
      assertThat(recordCountInPage).as("the fixture needs a neighbour after the chunk head").isGreaterThan(1);
      page.writeUnsignedInt(LocalBucket.PAGE_RECORD_TABLE_OFFSET + Binary.INT_SERIALIZED_SIZE, headChunkPosition + 1);

      chunked[0].asDocument(true).modify().set("v", MEDIUM).save();

      // The commit wraps the refusal (a TransactionException over the DatabaseOperationException), so it is asserted
      // on the whole chain rather than on the wrapper of the day.
      assertThatThrownBy(database::commit).hasStackTraceContaining(DatabaseOperationException.class.getName())
          .hasStackTraceContaining("no room left in page");
    } finally {
      if (database.isTransactionActive())
        database.rollback();
    }
  }

  /**
   * Grows a chunked record back and, before committing, has another thread commit a change to a record sharing its
   * page. Returns true when our commit went through, i.e. the merge absorbed the version bump.
   */
  private boolean growBackSurvives(final RID growing, final RID neighbour, final String value, final String neighbourValue)
      throws InterruptedException {
    database.begin();
    growing.asDocument(true).modify().set("v", value).save();

    final Thread concurrent = new Thread(
        () -> database.transaction(() -> neighbour.asDocument(true).modify().set("v", neighbourValue).save()));
    concurrent.start();
    concurrent.join();

    try {
      database.commit();
      return true;
    } catch (final ConcurrentModificationException e) {
      return false;
    } finally {
      if (database.isTransactionActive())
        database.rollback();
    }
  }
}
