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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.engine.PageId;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10661: a record inserted into a free slot of an existing page no longer keeps a second copy of its bytes
 * alive until commit for the disjoint-slot merge. The page the transaction wrote already holds them, so the merge
 * reads the inserted image back from that page when (and only when) it has to replay the insert on a newer committed
 * version of the page.
 */
class Issue10661PendingInsertRetentionTest extends TestHelper {
  private boolean savedSlotMerge;
  private long    savedMaxBytes;

  @BeforeEach
  void saveConfig() {
    savedSlotMerge = GlobalConfiguration.TX_PAGE_SLOT_MERGE.getValueAsBoolean();
    savedMaxBytes = GlobalConfiguration.TX_PAGE_SLOT_MERGE_MAX_BYTES.getValueAsLong();
    GlobalConfiguration.TX_PAGE_SLOT_MERGE.setValue(true);
  }

  @AfterEach
  void restoreConfig() {
    GlobalConfiguration.TX_PAGE_SLOT_MERGE.setValue(savedSlotMerge);
    GlobalConfiguration.TX_PAGE_SLOT_MERGE_MAX_BYTES.setValue(savedMaxBytes);
  }

  /**
   * The retention cap bounds the bytes the merge holds on to. An insert holds none any more, so a transaction made of
   * inserts whose images add up to far more than the cap must still merge with a concurrent insert on the same page.
   * Before the fix every insert was counted, the cap switched the merge off and the commit failed with a page conflict.
   */
  @Test
  void insertsDoNotCountAgainstTheRetentionCap() throws Exception {
    GlobalConfiguration.TX_PAGE_SLOT_MERGE_MAX_BYTES.setValue(4_096L);
    createSeededType();

    final int inserts = 60;
    final RID[] rids = new RID[inserts];
    final long mergesBefore = ((DatabaseInternal) database).getPageManager().getStats().txPageSlotMerges;

    database.begin();
    for (int i = 0; i < inserts; i++) {
      final MutableDocument doc = database.newDocument("Retained").set("payload", payload(i, 200));
      doc.save();
      rids[i] = doc.getIdentity();
    }
    assertThat(rids[0].getPosition()).as("the inserts must land on the reused page 0").isLessThan(1_000L);

    final RID concurrent = commitConcurrentInsert("concurrent");

    // NO RETRY: the two transactions inserted different records, so the page conflict is a false one
    database.commit();

    assertThat(((DatabaseInternal) database).getPageManager().getStats().txPageSlotMerges).isGreaterThan(mergesBefore);
    database.transaction(() -> {
      for (int i = 0; i < inserts; i++)
        assertThat(rids[i].asDocument(true).getString("payload")).isEqualTo(payload(i, 200));
      assertThat(concurrent.asDocument(true).getString("payload")).isEqualTo("concurrent");
      assertThat(database.countType("Retained", false)).isEqualTo(inserts + 2L);
    });
  }

  /**
   * The image replayed for an inserted slot is the one the slot holds at commit, whatever happened to it after the
   * insert: overwritten with a shorter value, grown in place, shrunk then grown back, deleted. Enough slots are written to outgrow the initial
   * capacity of the per-page slot arrays, and a committed record is updated next to them so an UPDATE (which keeps
   * its own images) is replayed in the same rebase.
   */
  @Test
  void rebaseReplaysTheFinalImageOfEveryInsertedSlot() throws Exception {
    createSeededType();
    final RID seed = database.query("sql", "select from Retained").next().getIdentity().orElseThrow();

    final int inserts = 40;
    final RID[] rids = new RID[inserts];
    final String[] expected = new String[inserts];

    database.begin();
    final MutableDocument[] docs = new MutableDocument[inserts];
    for (int i = 0; i < inserts; i++) {
      docs[i] = database.newDocument("Retained").set("payload", payload(i, 40));
      docs[i].save();
      rids[i] = docs[i].getIdentity();
      expected[i] = payload(i, 40);
    }
    // shrink in place
    docs[3].set("payload", "short").save();
    expected[3] = "short";
    // grow in place (the page has plenty of room, so the record stays in its slot)
    docs[20].set("payload", payload(20, 300)).save();
    expected[20] = payload(20, 300);
    // shrink in place, then grow back past the original size: the image is the last one, wherever it now sits
    docs[9].set("payload", "s").save();
    docs[9].set("payload", payload(9, 120)).save();
    expected[9] = payload(9, 120);
    // insert-and-delete: replayed as nothing
    docs[31].delete();
    expected[31] = null;
    // an UPDATE of a committed record on the same page
    seed.asDocument(true).modify().set("payload", "seed-updated").save();

    final long mergesBefore = ((DatabaseInternal) database).getPageManager().getStats().txPageSlotMerges;
    final RID concurrent = commitConcurrentInsert("concurrent");
    database.commit();

    assertThat(((DatabaseInternal) database).getPageManager().getStats().txPageSlotMerges).isGreaterThan(mergesBefore);
    database.transaction(() -> {
      for (int i = 0; i < inserts; i++)
        if (expected[i] == null)
          assertThat(database.existsRecord(rids[i])).isFalse();
        else
          assertThat(rids[i].asDocument(true).getString("payload")).isEqualTo(expected[i]);
      assertThat(seed.asDocument(true).getString("payload")).isEqualTo("seed-updated");
      assertThat(concurrent.asDocument(true).getString("payload")).isEqualTo("concurrent");
      assertThat(database.countType("Retained", false)).isEqualTo(inserts - 1 + 2L);
    });
  }

  /**
   * The read-back the merge relies on answers the body of a plain record, and {@code null} - which the merge turns into
   * a retryable refusal - for a slot that holds none: a deleted record, a slot past the page's record count.
   */
  @Test
  void readPlainRecordBodyAnswersOnlyAPlainRecord() {
    createSeededType();
    final RID[] rids = new RID[2];
    database.transaction(() -> {
      for (int i = 0; i < rids.length; i++) {
        final MutableDocument doc = database.newDocument("Retained").set("payload", payload(i, 40));
        doc.save();
        rids[i] = doc.getIdentity();
      }
    });
    database.transaction(() -> rids[1].asDocument(true).delete());

    final LocalBucket bucket = (LocalBucket) database.getSchema().getBucketById(rids[0].getBucketId());
    database.transaction(() -> {
      try {
        final BasePage page = ((DatabaseInternal) database).getTransaction()
            .getPage(new PageId(database, bucket.getFileId(), 0), bucket.getPageSize());
        final int liveSlot = (int) rids[0].getPosition();
        final int deletedSlot = (int) rids[1].getPosition();

        final byte[] body = bucket.readPlainRecordBody(page, liveSlot);
        assertThat(body).isNotNull();
        assertThat(new String(body)).contains(payload(0, 40));

        assertThat(bucket.readPlainRecordBody(page, deletedSlot)).as("a deleted record").isNull();
        assertThat(bucket.readPlainRecordBody(page, 1_000)).as("a slot past the record count").isNull();
      } catch (final Exception e) {
        throw new AssertionError(e);
      }
    });
  }

  private void createSeededType() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Retained", 1).createProperty("payload", Type.STRING);
      // page 0 exists before the test transactions, so their inserts go to free slots of a REUSED page
      database.newDocument("Retained").set("payload", "seed").save();
    });
  }

  /** Inserts and commits one record on another thread while the caller's transaction is still open. */
  private RID commitConcurrentInsert(final String payload) throws Exception {
    final AtomicReference<RID> rid = new AtomicReference<>();
    final AtomicReference<Throwable> error = new AtomicReference<>();
    final Thread thread = new Thread(() -> {
      try {
        database.transaction(() -> {
          final MutableDocument doc = database.newDocument("Retained").set("payload", payload);
          doc.save();
          rid.set(doc.getIdentity());
        });
      } catch (final Throwable e) {
        error.set(e);
      }
    });
    thread.start();
    thread.join();
    if (error.get() != null)
      throw new AssertionError("the concurrent insert failed", error.get());
    assertThat(rid.get().getPosition()).as("the concurrent insert must land on the same page").isLessThan(1_000L);
    return rid.get();
  }

  private static String payload(final int i, final int length) {
    final String prefix = "r" + i + "-";
    return prefix + "x".repeat(length - prefix.length());
  }
}
