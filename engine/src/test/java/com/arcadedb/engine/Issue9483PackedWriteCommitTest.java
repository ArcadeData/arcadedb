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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9483: the commit walked every slot of each bucket page it modified to prove the page had no hole, even when
 * the transaction had only appended records or overwritten them in place by the same footprint. A page now remembers
 * whether every write of the transaction was of that hole-free kind, and only a page that was written otherwise (a
 * delete, a shrinking or growing update) pays the proof.
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
  void aRecordAppendedToAPageLeavesItProvablySkippable() {
    final RID[] rids = insertRecords();

    database.transaction(() -> {
      database.newDocument(TYPE).set("v", value(RECORDS)).save();
      assertThat(onlyPackedWrites(rids[0])).isTrue();
    });
    checkDatabase();
  }

  @Test
  void aBrandNewPageFilledByAppendsIsSkippable() {
    database.transaction(() -> {
      final RID first = database.newDocument(TYPE).set("v", value(0)).save().getIdentity();
      for (int i = 1; i < RECORDS; i++)
        database.newDocument(TYPE).set("v", value(i)).save();
      assertThat(onlyPackedWrites(first)).isTrue();
    });
    checkDatabase();
  }

  @Test
  void anOverwriteOfTheSameFootprintLeavesThePageSkippable() {
    final RID[] rids = insertRecords();

    database.transaction(() -> {
      updateNow(rids[7], value(7).replace('x', 'y'));
      assertThat(onlyPackedWrites(rids[7])).isTrue();
    });

    database.transaction(() -> assertThat(rids[7].asDocument(true).getString("v")).isEqualTo(value(7).replace('x', 'y')));
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

  @Test
  void aGrowingUpdateWithdrawsThePageFromTheShortcut() {
    final RID[] rids = insertRecords();

    database.transaction(() -> {
      updateNow(rids[RECORDS - 1], value(RECORDS - 1) + "-grown");
      assertThat(onlyPackedWrites(rids[0])).isFalse();
    });
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

  private void assertPacked(final RID rid) {
    final LocalBucket bucket = bucketOf(TYPE);
    onSlot(rid, page -> {
      assertThat(bucket.packedContentEnd(page, page.readShort(LocalBucket.PAGE_RECORD_COUNT_IN_PAGE_OFFSET)))
          .as("the commit left the page packed").isPositive();
      return 0L;
    });
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
