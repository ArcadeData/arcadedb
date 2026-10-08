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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Issue #9483: the commit walked every slot of each bucket page it modified to prove the page had no hole, even when
 * the transaction had only appended records, overwritten them in place by the same footprint or grown them inside
 * the page. A page now remembers whether every write of the transaction was of that hole-free kind, and only a page
 * that was written otherwise (a delete, a shrinking update, a record that spilled out of the page) pays the proof.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9483PackedWriteCommitTest extends BucketPageLayoutTestSupport {
  private static final String TYPE    = "Packed9483";
  private static final int    RECORDS = 20;

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType(TYPE, 1);
  }

  @Test
  void aRecordAppendedToAPageLeavesItSkippable() {
    final RID[] rids = insertRecords();

    database.transaction(() -> {
      database.newDocument(TYPE).set("v", value(RECORDS)).save();
      assertThat(onlyPackedWrites(rids[0])).isTrue();
    });
    database.transaction(() -> assertPacked(rids[0]));
    checkDatabase();
  }

  @Test
  void aBrandNewPageFilledByAppendsIsSkippable() {
    final RID[] firstOfNewPage = new RID[1];
    database.transaction(() -> {
      final RID first = database.newDocument(TYPE).set("v", value(0)).save().getIdentity();
      for (int i = 1; i < RECORDS; i++)
        database.newDocument(TYPE).set("v", value(i)).save();
      assertThat(onlyPackedWrites(first)).isTrue();
      firstOfNewPage[0] = first;
    });
    database.transaction(() -> assertPacked(firstOfNewPage[0]));
    checkDatabase();
  }

  @Test
  void anOverwriteOfTheSameFootprintLeavesThePageSkippable() {
    final RID[] rids = insertRecords();

    database.transaction(() -> {
      updateNow(rids[7], value(7).replace('x', 'y'));
      assertThat(onlyPackedWrites(rids[7])).isTrue();
    });

    database.transaction(() -> {
      assertThat(rids[7].asDocument(true).getString("v")).isEqualTo(value(7).replace('x', 'y'));
      assertPacked(rids[7]);
    });
    checkDatabase();
  }

  /** An overwrite of the same footprint and an append in one transaction are both hole-free, so the page stays skippable. */
  @Test
  void anOverwriteAndAnAppendInOneTransactionKeepThePageSkippable() {
    final RID[] rids = insertRecords();

    database.transaction(() -> {
      updateNow(rids[2], value(2).replace('x', 'z'));
      database.newDocument(TYPE).set("v", value(RECORDS)).save();
      assertThat(onlyPackedWrites(rids[2])).isTrue();
    });

    database.transaction(() -> assertPacked(rids[0]));
    checkDatabase();
  }

  /**
   * A record that grows inside its page shifts what follows it by exactly the bytes it gains, the middle one and the
   * last one alike: no hole, so the page stays skippable, and under assertions the commit checks both the packing and
   * the free tail the growth reported.
   */
  @Test
  void aGrowthInsideThePageLeavesItSkippable() {
    final RID[] rids = insertRecords();

    database.transaction(() -> {
      updateNow(rids[3], value(3) + "-grown");
      updateNow(rids[RECORDS - 1], value(RECORDS - 1) + "-grown");
      assertThat(onlyPackedWrites(rids[0])).isTrue();
    });

    database.transaction(() -> {
      assertPacked(rids[0]);
      for (int i = 0; i < RECORDS; i++)
        assertThat(rids[i].asDocument(true).getString("v")).isEqualTo(i == 3 || i == RECORDS - 1 ? value(i) + "-grown" : value(i));
    });
    checkDatabase();
  }

  /** A growth the page cannot host spills the record into a chunk chain, which makes no hole-free promise. */
  @Test
  void aGrowthThatSpillsOutOfThePageWithdrawsIt() {
    final RID[] rids = insertRecords();
    final String huge = "g".repeat(bucketOf(TYPE).getPageSize() * 2);

    database.transaction(() -> {
      updateNow(rids[3], huge);
      assertThat(onlyPackedWrites(rids[0])).isFalse();
    });

    database.transaction(() -> {
      assertPacked(rids[0]);
      assertThat(rids[3].asDocument(true).getString("v")).isEqualTo(huge);
    });
    checkDatabase();
  }

  @Test
  void aShrinkingUpdateWithdrawsThePageFromTheShortcutAndTheCommitClosesTheHole() {
    final RID[] rids = insertRecords();

    database.transaction(() -> {
      updateNow(rids[5], "short");
      assertThat(onlyPackedWrites(rids[5])).isFalse();
    });

    database.transaction(() -> {
      assertPacked(rids[0]);
      for (int i = 0; i < RECORDS; i++)
        assertThat(rids[i].asDocument(true).getString("v")).isEqualTo(i == 5 ? "short" : value(i));
    });
    checkDatabase();
  }

  @Test
  void aDeleteWithdrawsThePageFromTheShortcutAndTheCommitClosesTheHole() {
    final RID[] rids = insertRecords();

    database.transaction(() -> {
      rids[3].asDocument(true).delete();
      assertThat(onlyPackedWrites(rids[3])).isFalse();
    });

    database.transaction(() -> assertPacked(rids[0]));
    checkDatabase();
  }

  /** Appends first, the hole-leaving write after: the order must not matter, the page is withdrawn for good. */
  @Test
  void aHoleAfterAppendsInTheSameTransactionStillGetsPacked() {
    final RID[] rids = insertRecords();

    database.transaction(() -> {
      database.newDocument(TYPE).set("v", value(RECORDS)).save();
      rids[1].asDocument(true).delete();
      assertThat(onlyPackedWrites(rids[0])).isFalse();
    });

    database.transaction(() -> assertPacked(rids[0]));
    checkDatabase();
  }

  /** A record too big for the page becomes a chunk chain through paths that make no hole-free promise: never skipped. */
  @Test
  void aMultiPageRecordWithdrawsThePageFromTheShortcut() {
    final RID[] rids = insertRecords();
    final String huge = "h".repeat(bucketOf(TYPE).getPageSize() * 2);

    database.transaction(() -> {
      database.newDocument(TYPE).set("v", huge).save();
      assertThat(onlyPackedWrites(rids[0])).isFalse();
    });

    database.transaction(() -> assertPacked(rids[0]));
    checkDatabase();
  }

  /** What the commit asks of every page: a page of hole-free writes is skipped, one with a hole goes through the full compression. */
  @Test
  void theCommitSkipsAPackedPageAndCompressesAPageWithAHole() {
    final RID[] rids = insertRecords();
    final LocalBucket bucket = bucketOf(TYPE);

    database.begin();
    database.newDocument(TYPE).set("v", value(RECORDS)).save();
    assertThat(commitCompressionSkipped(bucket, rids[0])).as("an appended page is skipped").isTrue();
    database.rollback();

    database.begin();
    rids[4].asDocument(true).delete();
    assertThat(commitCompressionSkipped(bucket, rids[0])).as("a page with a hole is compressed").isFalse();
    assertPacked(rids[0]);
    database.rollback();
  }

  /**
   * The flag belongs to the transaction's own image of the page: a commit that loses the page version race is merged
   * onto the newer committed page, which the merge compresses in full, and both transactions' records survive on a
   * packed page. The merge counter proves the race really happened.
   */
  @Test
  void aTransactionRebasedOntoAConcurrentCommitLeavesThePagePacked() {
    final RID[] rids = insertRecords();
    final PageManager pageManager = ((DatabaseInternal) database).getPageManager();
    final long mergesBefore = pageManager.getStats().txPageSlotMerges;

    database.begin();
    database.newDocument(TYPE).set("v", value(RECORDS)).save();
    inAnotherThread(() -> database.transaction(() -> database.newDocument(TYPE).set("v", value(RECORDS + 1)).save()));
    database.commit();

    assertThat(pageManager.getStats().txPageSlotMerges).as("the commit was merged onto the concurrent one").isGreaterThan(mergesBefore);
    database.transaction(() -> {
      assertPacked(rids[0]);
      assertThat(database.countType(TYPE, true)).isEqualTo(RECORDS + 2);
    });
    checkDatabase();
  }

  /**
   * The shortcut stands on every mutation of a page passing through its modified-range tracking: a freshly formatted
   * page starts out hole-free, and each public writer, used outside a hole-free declaration, withdraws it - while
   * the same writer inside one does not.
   */
  @Test
  void everyPublicWriterOfAPageWithdrawsItUnlessDeclaredHoleFree() {
    final int fileId = bucketOf(TYPE).getFileId();
    final List<Consumer<MutablePage>> writers = List.of(//
        page -> page.writeNumber(100, 7L), page -> page.writeLong(100, 7L), page -> page.writeInt(100, 7),
        page -> page.writeUnsignedInt(100, 7L), page -> page.writeShort(100, (short) 7), page -> page.writeUnsignedShort(100, 7),
        page -> page.writeFloat(100, 1f), page -> page.writeDouble(100, 1d), page -> page.writeByte(100, (byte) 7),
        page -> page.writeBytes(100, new byte[] { 1, 2 }), page -> page.writeByteArray(100, new byte[] { 1, 2 }),
        page -> page.writeByteArray(100, new byte[] { 1, 2 }, 0, 2), page -> page.writeZeros(100, 4),
        page -> page.writeString(100, "x"), page -> page.move(100, 200, 4));

    for (int i = 0; i < writers.size(); i++) {
      final MutablePage page = new MutablePage(new PageId(database, fileId, 1_000 + i), bucketOf(TYPE).getPageSize());
      assertThat(page.hasOnlyPackedWrites()).as("a new page holds no record, so no hole").isTrue();

      final boolean previous = page.beginPackedWrite();
      writers.get(i).accept(page);
      page.endPackedWrite(previous);
      assertThat(page.hasOnlyPackedWrites()).as("writer #" + i + " inside a declaration").isTrue();

      writers.get(i).accept(page);
      assertThat(page.hasOnlyPackedWrites()).as("writer #" + i + " outside a declaration").isFalse();
    }
  }

  /**
   * The guard that keeps the declarations honest: under assertions, a page written only by declared writes that has a
   * hole at commit, on a page that was packed when the transaction took it, is a declared write that broke its
   * promise - and the commit says so instead of quietly repairing the page the way production would not.
   */
  @Test
  void aDeclaredWriteThatLeavesAHoleTripsTheGuard() {
    assumeTrue(LocalBucket.class.desiredAssertionStatus(), "the guard is an assertion");
    final RID[] rids = insertRecords();
    final LocalBucket bucket = bucketOf(TYPE);

    database.begin();
    try {
      assertThatThrownBy(() -> onSlot(rids[0], page -> {
        final boolean previous = page.beginPackedWrite();
        try {
          // A "declared" write that frees a slot in the middle of the page: exactly what no declaration may do
          page.writeUnsignedInt(slotOffset(rids[5]), 0L);
        } finally {
          page.endPackedWrite(previous);
        }
        bucket.compressPageAtCommit(page);
        return 0L;
      })).isInstanceOf(AssertionError.class).hasMessageContaining("broke its promise");
    } finally {
      database.rollback();
    }
  }

  /**
   * The one hole the guard must let through: a page an old engine left with a hole, which a transaction only appends
   * to. It never came from a declared write, so the commit takes the full compression (under assertions) and the
   * page comes out packed, with every record intact.
   */
  @Test
  void aHoleAnOldEngineLeftDoesNotTripTheGuard() throws Exception {
    final RID[] rids = insertRecords();
    final LocalBucket bucket = bucketOf(TYPE);
    final PageManager pageManager = ((DatabaseInternal) database).getPageManager();
    final PageId pageId = new PageId(database, bucket.getFileId(), (int) (rids[0].getPosition() / bucket.getMaxRecordsInPage()));

    // WHAT AN OLD ENGINE COULD LEAVE ON DISK, WRITTEN AROUND ANY TRANSACTION: THE LAST RECORD SITTING 16 BYTES PAST THE
    // END OF THE ONE BEFORE IT, EVERY RECORD STILL THERE
    final MutablePage legacy = pageManager.getImmutablePage(pageId, bucket.getPageSize(), false, false).modify();
    final int contentEnd = bucket.packedContentEnd(legacy, legacy.readShort(LocalBucket.PAGE_RECORD_COUNT_IN_PAGE_OFFSET));
    final int lastRecordOffset = (int) legacy.readUnsignedInt(slotOffset(rids[RECORDS - 1]));
    legacy.move(lastRecordOffset, lastRecordOffset + 16, contentEnd - lastRecordOffset);
    legacy.writeUnsignedInt(slotOffset(rids[RECORDS - 1]), lastRecordOffset + 16);
    pageManager.writePages(List.of(legacy), false);

    database.transaction(() -> {
      onSlot(rids[0], page -> {
        assertThat(bucket.packedContentEnd(page, page.readShort(LocalBucket.PAGE_RECORD_COUNT_IN_PAGE_OFFSET)))
            .as("the committed page carries the hole").isEqualTo(-1);
        return 0L;
      });
      database.newDocument(TYPE).set("v", value(RECORDS)).save();
      assertThat(onlyPackedWrites(rids[0])).isTrue();
    });

    database.transaction(() -> {
      assertPacked(rids[0]);
      for (int i = 0; i < RECORDS; i++)
        assertThat(rids[i].asDocument(true).getString("v")).isEqualTo(value(i));
    });
    checkDatabase();
  }

  /**
   * A commit that only appended to a page no longer re-measures it, so a free-space entry rewritten while the appends
   * were still private can outlive that commit over-stated. The allocator corrects such an entry the first time it
   * finds the page cannot take a record, rather than probing the page again on every allocation that trusts it. The
   * record here fits neither the page nor, halved, its real free tail, so nothing lands on the page and only the
   * correction can change its entry.
   */
  @Test
  void anOverStatedFreeSpaceEntryIsCorrectedByTheFirstProbeItFails() {
    final LocalBucket bucket = bucketOf(TYPE);
    final RID onFirstPage = fillFirstPage(bucket);
    final int[] realFree = new int[1];
    database.transaction(() -> onSlot(onFirstPage, page -> realFree[0] =
        page.getMaxContentSize() - bucket.packedContentEnd(page, page.readShort(LocalBucket.PAGE_RECORD_COUNT_IN_PAGE_OFFSET))));
    final int planted = realFree[0] + 20_000;
    bucket.setPageStatistics(new JSONArray().put(new JSONObject().put("id", 0).put("free", planted)));

    final RID[] placed = new RID[1];
    database.transaction(() -> placed[0] = database.newDocument(TYPE).set("v", "b".repeat(12_000)).save().getIdentity());

    assertThat(placed[0].getPosition() / bucket.getMaxRecordsInPage()).as("the record cannot land on the full page").isNotZero();
    assertThat(bucket.getFreeSpaceHintForPage(0)).as("the entry the failed probe found over-stated").isNotEqualTo(planted)
        .isLessThanOrEqualTo(realFree[0]);
    checkDatabase();
  }

  /** Appends 1KB records until one lands past page 0, leaving page 0 with less free tail than such a record needs. */
  private RID fillFirstPage(final LocalBucket bucket) {
    final RID[] first = new RID[1];
    database.transaction(() -> {
      first[0] = database.newDocument(TYPE).set("v", "f".repeat(1_000)).save().getIdentity();
      RID last = first[0];
      while (last.getPosition() / bucket.getMaxRecordsInPage() == 0)
        last = database.newDocument(TYPE).set("v", "f".repeat(1_000)).save().getIdentity();
    });
    return first[0];
  }

  /** Writes the record now, where {@code save()} would defer it to the commit, so the page can be looked at after it. */
  private void updateNow(final RID rid, final String value) {
    final MutableDocument document = rid.asDocument(true).modify();
    document.set("v", value);
    bucketOf(TYPE).updateRecord(document, false);
  }

  private boolean onlyPackedWrites(final RID rid) {
    final boolean[] result = new boolean[1];
    onSlot(rid, page -> {
      result[0] = page.hasOnlyPackedWrites();
      return 0L;
    });
    return result[0];
  }

  private boolean commitCompressionSkipped(final LocalBucket bucket, final RID rid) {
    final boolean[] result = new boolean[1];
    onSlot(rid, page -> {
      result[0] = bucket.compressPageAtCommit(page);
      return 0L;
    });
    return result[0];
  }

  private void assertPacked(final RID rid) {
    final LocalBucket bucket = bucketOf(TYPE);
    onSlot(rid, page -> {
      assertThat(bucket.packedContentEnd(page, page.readShort(LocalBucket.PAGE_RECORD_COUNT_IN_PAGE_OFFSET)))
          .as("the commit left the page packed").isPositive();
      return 0L;
    });
  }

  /** Offset of {@code rid}'s entry in its page's record table. */
  private int slotOffset(final RID rid) {
    final int slot = (int) (rid.getPosition() % bucketOf(TYPE).getMaxRecordsInPage());
    return Binary.SHORT_SERIALIZED_SIZE + slot * Binary.INT_SERIALIZED_SIZE;
  }

  private RID[] insertRecords() {
    final RID[] rids = new RID[RECORDS];
    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++)
        rids[i] = database.newDocument(TYPE).set("v", value(i)).save().getIdentity();
    });
    return rids;
  }

  private static String value(final int i) {
    return "record-" + i + "-" + "x".repeat(200);
  }
}
