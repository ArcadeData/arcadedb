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

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The insert path of a bucket answers "where is the next free slot and where does the content end" from a summary of
 * the record table that the transaction's own page keeps while it appends (see
 * {@link LocalBucket#getFreeSpaceInPage}), instead of walking every slot per insert. The summary must never outlive a
 * write that could change the answer: deletes, record growth that moves content, chunked records, and the reuse of
 * holes in the same or a later transaction. This test drives all of those against a model, reopens the database and
 * checks the model again.
 */
class BucketPackedScanTest extends TestHelper {
  private final Map<Integer, String> model = new HashMap<>();
  private final Map<Integer, RID>    rids  = new HashMap<>();
  private       int                  nextId = 0;

  @Test
  void appendsDeletesAndGrowthMatchAModelAcrossTransactionsAndReopen() {
    database.getSchema().createDocumentType("T", 1);
    final Random rnd = new Random(42);

    // 1) one long transaction of small appends (the summary path), several pages worth
    database.transaction(() -> {
      for (int i = 0; i < 6000; i++)
        insert(rnd, 0);
    });
    verifyModel();

    // 2) same-transaction mix: appends, deletes of records of this very transaction, appends again, a growing update
    database.transaction(() -> {
      for (int round = 0; round < 40; round++) {
        for (int i = 0; i < 50; i++)
          insert(rnd, 1);
        for (int i = 0; i < 20; i++)
          deleteRandom(rnd);
        for (int i = 0; i < 10; i++)
          growRandom(rnd);
      }
    });
    verifyModel();

    // 3) holes from earlier transactions get reused, interleaved with chunked records that span pages
    for (int tx = 0; tx < 6; tx++)
      database.transaction(() -> {
        for (int i = 0; i < 400; i++)
          insert(rnd, i % 97 == 0 ? 2 : 1);
        for (int i = 0; i < 300; i++)
          deleteRandom(rnd);
        for (int i = 0; i < 200; i++)
          insert(rnd, 0);
      });
    verifyModel();

    reopenDatabase();
    verifyModel();
    checkDatabaseIntegrity();
  }

  /**
   * A transaction appends (which builds the summary of the page's record table), deletes a slot in the middle of the
   * page, appends again and then rolls back. The model alone cannot see a stale summary here: the walk skips a slot
   * deleted in the current transaction, so the inserts after the delete append at new slots whether or not the summary
   * was dropped. The test therefore asserts the summary of the transaction's page directly: present after the appends,
   * dropped by the delete, still dropped after the next append (the page now has a hole), gone with the rolled back
   * transaction and rebuilt by the first append of the next one.
   */
  @Test
  void appendAfterInTransactionDeleteOfAMiddleSlotThenRollback() {
    database.getSchema().createDocumentType("T", 1);
    final Random rnd = new Random(7);
    database.transaction(() -> {
      for (int i = 0; i < 200; i++)
        insert(rnd, 0);
    });
    verifyModel();

    final Map<Integer, String> modelBefore = new HashMap<>(model);
    final Map<Integer, RID> ridsBefore = new HashMap<>(rids);
    final int nextIdBefore = nextId;
    final RID middle = rids.get(100);

    database.begin();
    for (int i = 0; i < 20; i++)
      insert(rnd, 0);
    // all 220 records are on the first page of the bucket, and the transaction's own copy of it holds the summary
    assertThat(transactionPage(middle).hasPackedScan(220)).isTrue();
    database.deleteRecord(database.lookupByRID(middle, true));
    model.remove(100);
    rids.remove(100);
    // the delete wrote a slot of the record table below the summarized end: the summary must be gone
    assertThat(transactionPage(middle).hasPackedScan(220)).isFalse();
    for (int i = 0; i < 20; i++)
      insert(rnd, 0);
    // the appends went to slots 220..239, and the page still has the hole of the deleted slot: no summary describes it
    assertThat(transactionPage(middle).hasPackedScan(240)).isFalse();
    assertThat(database.countType("T", true)).isEqualTo(model.size());
    verifyModel();
    database.rollback();

    model.clear();
    model.putAll(modelBefore);
    rids.clear();
    rids.putAll(ridsBefore);
    nextId = nextIdBefore;
    verifyModel();

    // a new transaction starts from the committed page: no summary until it appends, then one that matches the page
    database.begin();
    assertThat(transactionPage(middle).hasPackedScan(200)).isFalse();
    insert(rnd, 0);
    assertThat(transactionPage(middle).hasPackedScan(201)).isTrue();
    database.rollback();
    model.clear();
    model.putAll(modelBefore);
    rids.clear();
    rids.putAll(ridsBefore);
    nextId = nextIdBefore;

    // the next transaction appends to the same page again, and all of it survives a reopen
    database.transaction(() -> {
      for (int i = 0; i < 50; i++)
        insert(rnd, 0);
    });
    verifyModel();
    reopenDatabase();
    verifyModel();
    checkDatabaseIntegrity();
  }

  /** The transaction's own copy of the page that holds {@code rid}. */
  private MutablePage transactionPage(final RID rid) {
    final LocalBucket bucket = (LocalBucket) database.getSchema().getBucketById(rid.getBucketId());
    final PageId pageId = new PageId((DatabaseInternal) database, bucket.getFileId(),
        (int) (rid.getPosition() / bucket.getMaxRecordsInPage()));
    try {
      return ((DatabaseInternal) database).getTransaction().getPageToModify(pageId, bucket.getPageSize(), false);
    } catch (final java.io.IOException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * The summary stands on one invariant: every mutation of a page passes updateModifiedRange. Drive every public
   * write path of MutablePage over a page that holds a summary and check that each one drops it, so a new writer
   * that bypasses the invariant (a direct content.put*) fails here instead of leaving a stale summary behind.
   */
  @Test
  void everyMutablePageWritePathDropsTheSummary() {
    final int pageSize = 4_096;
    final List<Consumer<MutablePage>> writers = List.of(
        p -> p.writeNumber(8, 1L), p -> p.writeLong(8, 1L), p -> p.writeInt(8, 1), p -> p.writeUnsignedInt(8, 1L),
        p -> p.writeShort(8, (short) 1), p -> p.writeUnsignedShort(8, 1), p -> p.writeFloat(8, 1f), p -> p.writeDouble(8, 1d),
        p -> p.writeByte(8, (byte) 1), p -> p.writeBytes(8, new byte[] { 1, 2 }), p -> p.writeByteArray(8, new byte[] { 1, 2 }),
        p -> p.writeByteArray(8, new byte[] { 1, 2, 3 }, 1, 2), p -> p.writeZeros(8, 4), p -> p.writeString(8, "x"),
        p -> p.move(16, 8, 4));
    for (int i = 0; i < writers.size(); i++) {
      final MutablePage page = new MutablePage(new PageId((DatabaseInternal) database, 1, 0), pageSize, new byte[pageSize], 0,
          pageSize - BasePage.PAGE_HEADER_SIZE);
      page.rememberSlotScan(3, 100, 3, 32);
      assertThat(page.hasSlotScan(3)).isTrue();
      // a write past the summarized part of the record table leaves it alone
      page.writeInt(2_000, 1);
      assertThat(page.hasSlotScan(3)).as("write beyond the table, writer %d", i).isTrue();
      writers.get(i).accept(page);
      assertThat(page.hasSlotScan(3)).as("writer %d drops the summary", i).isFalse();
    }
    final MutablePage cleared = new MutablePage(new PageId((DatabaseInternal) database, 1, 0), pageSize, new byte[pageSize], 0,
        pageSize - BasePage.PAGE_HEADER_SIZE);
    cleared.rememberSlotScan(3, 100, 3, 32);
    cleared.clearContent();
    assertThat(cleared.hasSlotScan(3)).isFalse();
  }

  /** kind 0 = tiny, 1 = a few hundred bytes, 2 = larger than a page, so it is stored as a chunk chain */
  private void insert(final Random rnd, final int kind) {
    final int id = nextId++;
    final int len = switch (kind) {
      case 0 -> 1 + rnd.nextInt(12);
      case 1 -> 100 + rnd.nextInt(500);
      default -> 70_000 + rnd.nextInt(30_000);
    };
    final String payload = payload(rnd, len);
    final MutableDocument doc = database.newDocument("T").set("id", id, "pad", payload);
    doc.save();
    model.put(id, payload);
    rids.put(id, doc.getIdentity());
  }

  private void deleteRandom(final Random rnd) {
    if (model.isEmpty())
      return;
    final ArrayList<Integer> keys = new ArrayList<>(model.keySet());
    final int id = keys.get(rnd.nextInt(keys.size()));
    database.deleteRecord(database.lookupByRID(rids.get(id), true));
    model.remove(id);
    rids.remove(id);
  }

  private void growRandom(final Random rnd) {
    if (model.isEmpty())
      return;
    final ArrayList<Integer> keys = new ArrayList<>(model.keySet());
    final int id = keys.get(rnd.nextInt(keys.size()));
    final String payload = payload(rnd, 300 + rnd.nextInt(900));
    final MutableDocument doc = database.lookupByRID(rids.get(id), true).asDocument().modify();
    doc.set("pad", payload).save();
    model.put(id, payload);
    rids.put(id, doc.getIdentity());
  }

  private static String payload(final Random rnd, final int len) {
    final StringBuilder sb = new StringBuilder(len);
    for (int i = 0; i < len; i++)
      sb.append((char) ('a' + rnd.nextInt(26)));
    return sb.toString();
  }

  private void verifyModel() {
    assertThat(database.countType("T", true)).isEqualTo(model.size());
    final Map<Integer, String> seen = new HashMap<>();
    database.iterateType("T", true).forEachRemaining(r -> {
      final Document d = r.asDocument();
      assertThat(seen.put(d.getInteger("id"), d.getString("pad"))).isNull();
    });
    assertThat(seen).isEqualTo(model);
    // every RID the model tracks still reads back its own record
    for (final Map.Entry<Integer, RID> e : rids.entrySet())
      assertThat(database.lookupByRID(e.getValue(), true).asDocument().getString("pad")).isEqualTo(model.get(e.getKey()));
  }
}
