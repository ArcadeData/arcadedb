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
package com.arcadedb.database;

import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10669: since #10637 a record whose slot is smaller than a placeholder pointer (an empty document, 6 bytes)
 * borrows the bytes the pointer misses from another PLAIN record of its page. A page whose free tail was taken by the
 * head chunk of a record that spilled holds no such record, and the update was refused - for good, since a retry met
 * the same page. The bytes now come from a chunk of the page: its trailing content moves into a new continuation chunk
 * inserted right after it in the chain, so the chunked record keeps its RID and its content, one hop longer.
 */
class Issue10669SmallSlotShrinksAChunkTest extends BucketPageLayoutTestSupport {
  private static final String BIG    = "b".repeat(20 * 1024);
  private static final String SEALER = "s".repeat(70 * 1024);

  /**
   * The issue's reproduction: one empty document, then a record grown to 70 KB so that its head chunk takes the whole
   * free tail of the page. The empty document must grow, and the spilled record must still read back whole.
   */
  @Test
  void aSmallSlotNextToASpilledRecordGrows() {
    final RID[] rids = sealedPage("Shrink10669");
    final RID empty = rids[0];
    final RID sealer = rids[1];
    final long countBefore = countRecordsFromCounter("Shrink10669");

    database.transaction(() -> empty.asDocument(true).modify().set("v", BIG).save());

    assertIntact("Shrink10669", countBefore);
    database.transaction(() -> {
      assertThat(empty.asDocument(true).getString("v")).isEqualTo(BIG);
      assertThat(sealer.asDocument(true).getString("v")).isEqualTo(SEALER);
    });
  }

  /**
   * The chunked record that gave the bytes reads back whole INSIDE the transaction that shrank its chunk, from the pages
   * that transaction modified, and not only once the commit published them.
   */
  @Test
  void theShrunkRecordReadsBackInTheSameTransaction() {
    final RID[] rids = sealedPage("SameTx10669");
    database.transaction(() -> {
      rids[0].asDocument(true).modify().set("v", BIG).save();
      assertThat(rids[1].asDocument(true).getString("v")).isEqualTo(SEALER);
      assertThat(rids[0].asDocument(true).getString("v")).isEqualTo(BIG);
    });
  }

  /**
   * Both records stay ordinary from then on: they survive a reopen, the chunked record can be rewritten (shorter and
   * longer) and deleted, and none of that disturbs the record it gave the bytes to.
   */
  @Test
  void theShrunkRecordStaysAnOrdinaryRecord() {
    final RID[] rids = sealedPage("Ordinary10669");
    final RID empty = rids[0];
    final RID sealer = rids[1];
    final long countBefore = countRecordsFromCounter("Ordinary10669");

    database.transaction(() -> empty.asDocument(true).modify().set("v", BIG).save());

    reopenDatabase();
    database.transaction(() -> {
      assertThat(database.lookupByRID(sealer, true).asDocument().getString("v")).isEqualTo(SEALER);
      assertThat(database.lookupByRID(empty, true).asDocument().getString("v")).isEqualTo(BIG);
    });

    final String longer = "l".repeat(90 * 1024);
    database.transaction(() -> database.lookupByRID(sealer, true).asDocument().modify().set("v", longer).save());
    database.transaction(() -> assertThat(database.lookupByRID(sealer, true).asDocument().getString("v")).isEqualTo(longer));
    database.transaction(() -> database.lookupByRID(sealer, true).asDocument().modify().set("v", "short").save());
    database.transaction(() -> assertThat(database.lookupByRID(sealer, true).asDocument().getString("v")).isEqualTo("short"));
    checkDatabase();

    database.transaction(() -> database.deleteRecord(database.lookupByRID(sealer, true)));
    assertThat(countRecordsFromCounter("Ordinary10669")).isEqualTo(countBefore - 1);
    assertThat(countRecords("Ordinary10669")).isEqualTo(countBefore - 1);
    checkDatabase();
    database.transaction(() -> assertThat(database.lookupByRID(empty, true).asDocument().getString("v")).isEqualTo(BIG));
  }

  /**
   * The chunked record itself is rewritten by the same transaction, saved BEFORE the small slot grows: its chain was
   * shrunk at commit under an update that had already been queued, and both must land.
   */
  @Test
  void theChunkedRecordUpdatedFirstInTheSameTransaction() {
    final RID[] rids = sealedPage("First10669");
    final String rewritten = "r".repeat(80 * 1024);
    database.transaction(() -> {
      rids[1].asDocument(true).modify().set("v", rewritten).save();
      rids[0].asDocument(true).modify().set("v", BIG).save();
    });
    assertIntact("First10669", 2);
    database.transaction(() -> {
      assertThat(rids[0].asDocument(true).getString("v")).isEqualTo(BIG);
      assertThat(rids[1].asDocument(true).getString("v")).isEqualTo(rewritten);
    });
  }

  /** The same, the small slot saved first. */
  @Test
  void theChunkedRecordUpdatedLastInTheSameTransaction() {
    final RID[] rids = sealedPage("Last10669");
    final String rewritten = "r".repeat(80 * 1024);
    database.transaction(() -> {
      rids[0].asDocument(true).modify().set("v", BIG).save();
      rids[1].asDocument(true).modify().set("v", rewritten).save();
    });
    assertIntact("Last10669", 2);
    database.transaction(() -> {
      assertThat(rids[0].asDocument(true).getString("v")).isEqualTo(BIG);
      assertThat(rids[1].asDocument(true).getString("v")).isEqualTo(rewritten);
    });
  }

  /**
   * Several small slots of the same sealed page each take the bytes they miss from the chunk, inside ONE transaction:
   * the offsets every shrink moves have to be the ones the next one reads.
   */
  @Test
  void severalSmallSlotsOfOnePageGrowInOneTransaction() {
    final List<RID> empties = new ArrayList<>();
    final RID[] sealer = new RID[1];
    database.transaction(() -> {
      database.getSchema().createDocumentType("Several10669", 1).createProperty("v", Type.STRING);
      for (int i = 0; i < 3; i++)
        empties.add(database.newDocument("Several10669").save().getIdentity());
      sealer[0] = database.newDocument("Several10669").set("v", "s").save().getIdentity();
    });
    empties.forEach(this::shrinkToUnpaddedSize);
    seal(sealer[0], "Several10669");
    final long countBefore = countRecordsFromCounter("Several10669");

    database.transaction(() -> {
      for (final RID empty : empties)
        empty.asDocument(true).modify().set("v", BIG + empty.getPosition()).save();
    });

    assertIntact("Several10669", countBefore);
    database.transaction(() -> {
      for (final RID empty : empties)
        assertThat(empty.asDocument(true).getString("v")).isEqualTo(BIG + empty.getPosition());
      assertThat(sealer[0].asDocument(true).getString("v")).isEqualTo(SEALER);
    });
  }

  /**
   * Every shrink of the same chain adds one continuation chunk (code review on #10860): three small slots of one page,
   * three chunks more. None of them may outlive the record: once it shrinks back into a plain record, the bucket holds
   * no chunk at all, orphaned or not.
   */
  @Test
  void repeatedShrinksLeaveNoChunkBehindOnceTheRecordShrinks() {
    final List<RID> empties = new ArrayList<>();
    final RID[] sealer = new RID[1];
    database.transaction(() -> {
      database.getSchema().createDocumentType("Repeated10669", 1).createProperty("v", Type.STRING);
      for (int i = 0; i < 3; i++)
        empties.add(database.newDocument("Repeated10669").save().getIdentity());
      sealer[0] = database.newDocument("Repeated10669").set("v", "s").save().getIdentity();
    });
    empties.forEach(this::shrinkToUnpaddedSize);
    seal(sealer[0], "Repeated10669");
    final long chunksBefore = (Long) bucketStats("Repeated10669").get("totalChunks");

    for (final RID empty : empties)
      database.transaction(() -> empty.asDocument(true).modify().set("v", BIG + empty.getPosition()).save());

    final Map<String, Object> grown = bucketStats("Repeated10669");
    assertThat((Long) grown.get("totalChunks")).as("one continuation chunk per shrink " + grown)
        .isEqualTo(chunksBefore + empties.size());
    database.transaction(() -> assertThat(sealer[0].asDocument(true).getString("v")).isEqualTo(SEALER));
    checkDatabase();

    database.transaction(() -> sealer[0].asDocument(true).modify().set("v", "short").save());

    final Map<String, Object> shrunk = bucketStats("Repeated10669");
    assertThat((Long) shrunk.get("totalMultiPageRecords")).as(shrunk.toString()).isZero();
    assertThat((Long) shrunk.get("totalChunks")).as("no chunk outlives the chain " + shrunk).isZero();
    assertThat((Long) shrunk.get("orphanedChunks")).as(shrunk.toString()).isZero();
    assertIntact("Repeated10669", empties.size() + 1);
    database.transaction(() -> {
      assertThat(sealer[0].asDocument(true).getString("v")).isEqualTo("short");
      for (final RID empty : empties)
        assertThat(empty.asDocument(true).getString("v")).isEqualTo(BIG + empty.getPosition());
    });
  }

  /**
   * An update of another record applied in the same transaction commits with the grown one: the shape that used to be
   * refused, and roll the whole transaction back, now commits whole.
   */
  @Test
  void theOtherWritesOfTheTransactionCommitWithIt() {
    final RID[] other = new RID[1];
    database.transaction(() -> {
      database.getSchema().createDocumentType("Other10669", 1).createProperty("v", Type.STRING);
      other[0] = database.newDocument("Other10669").set("v", "before").save().getIdentity();
    });
    final RID[] rids = sealedPage("Whole10669");
    final long countBefore = countRecordsFromCounter("Whole10669");

    database.transaction(() -> {
      other[0].asDocument(true).modify().set("v", "after").save();
      database.newDocument("Whole10669").set("v", "inserted").save();
      rids[0].asDocument(true).modify().set("v", BIG).save();
    });

    assertIntact("Whole10669", countBefore + 1);
    assertThat(countRecordsHolding("Whole10669", "inserted")).isEqualTo(1L);
    database.transaction(() -> {
      assertThat(other[0].asDocument(true).getString("v")).isEqualTo("after");
      assertThat(rids[0].asDocument(true).getString("v")).isEqualTo(BIG);
      assertThat(rids[1].asDocument(true).getString("v")).isEqualTo(SEALER);
    });
  }

  /**
   * Page 0 of a new single-bucket type holding an empty document and, after it, a record grown until it spilled: its head
   * chunk takes the whole free tail, and the page holds no plain record that could move behind a pointer.
   *
   * @return the empty document, then the spilled record.
   */
  private RID[] sealedPage(final String typeName) {
    final RID[] rids = new RID[2];
    database.transaction(() -> {
      database.getSchema().createDocumentType(typeName, 1).createProperty("v", Type.STRING);
      rids[0] = database.newDocument(typeName).save().getIdentity();
      rids[1] = database.newDocument(typeName).set("v", "s").save().getIdentity();
    });
    shrinkToUnpaddedSize(rids[0]);
    seal(rids[1], typeName);
    return rids;
  }

  private void seal(final RID sealer, final String typeName) {
    database.transaction(() -> sealer.asDocument(true).modify().set("v", SEALER).save());
    final Map<String, Object> layout = bucketStats(typeName);
    assertThat((Long) layout.get("totalMultiPageRecords")).as("the premise: the sealing record spilled " + layout).isEqualTo(1L);
    assertThat((Long) layout.get("totalPlaceholderRecords")).as("the premise: no pointer yet " + layout).isZero();
  }

  /** Every record is still there and readable: a scan agrees with the counter, and the database checks out clean. */
  private void assertIntact(final String typeName, final long expectedCount) {
    assertThat(countRecordsFromCounter(typeName)).isEqualTo(expectedCount);
    assertThat(countRecords(typeName)).as("a scan must return every record the counter counts").isEqualTo(expectedCount);
    checkDatabase();
  }
}
