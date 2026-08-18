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

import com.arcadedb.database.Binary;
import com.arcadedb.database.BucketPageLayoutTestSupport;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

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
class Issue6339FreedSpaceStatisticsTest extends BucketPageLayoutTestSupport {
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
