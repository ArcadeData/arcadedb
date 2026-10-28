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
import com.arcadedb.database.RID;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10701: position 0 of a bucket (slot 0 of page 0) is also the value a chunk's next-chunk pointer uses for "no
 * next chunk". A continuation chunk allocated there is linked with a pointer of 0, so every reader stops one chunk
 * early: the record is silently truncated and can no longer be decoded. The rule "never give a continuation chunk
 * position 0" was applied by one of the three allocator paths only, and on a page holding records it was not applied
 * at all by the slot search underneath them.
 */
class Issue10701ContinuationChunkNeverAtPositionZeroTest extends BucketPageLayoutTestSupport {
  private static final String FILLER = "f".repeat(1024);

  /**
   * The reporter's deterministic repro: a chain that GROWS, whose last reused chunk sits on page 0, while slot 0 of
   * page 0 is a hole. The extension is allocated through the "prioritize space in the same page" path, which never
   * screened position 0.
   */
  @Test
  void aGrowingChainIsNeverLinkedToPositionZero() {
    database.transaction(() -> database.getSchema().createDocumentType("Grow", 1).createProperty("v", Type.STRING));

    final RID[] slotZero = new RID[1];
    final RID[] target = new RID[1];
    final List<RID> page0Fillers = new ArrayList<>();
    final int maxRecordsInPage = bucketOf("Grow").getMaxRecordsInPage();

    // 1. A small record at position 0, then 1 KB fillers until page 0 is full.
    database.transaction(() -> {
      slotZero[0] = database.newDocument("Grow").set("v", "zero").save().getIdentity();
      assertThat(slotZero[0].getPosition()).as("the fixture needs the first record at position 0").isZero();
      for (int i = 0; i < 200; i++) {
        final RID rid = database.newDocument("Grow").set("v", FILLER).save().getIdentity();
        if (rid.getPosition() >= maxRecordsInPage)
          return;
        page0Fillers.add(rid);
      }
      throw new AssertionError("page 0 did not fill up");
    });

    // 2. On page 1: the target and a neighbour right after it, then fill page 1 to the brim.
    database.transaction(() -> {
      target[0] = database.newDocument("Grow").set("v", "t").save().getIdentity();
      database.newDocument("Grow").set("v", "neighbour").save();
      assertThat(target[0].getPosition() / maxRecordsInPage).as("the target must live on page 1").isEqualTo(1);
      for (int i = 0; i < 200; i++) {
        final RID rid = database.newDocument("Grow").set("v", FILLER).save().getIdentity();
        if (rid.getPosition() >= 2L * maxRecordsInPage)
          return;
      }
      throw new AssertionError("page 1 did not fill up");
    });

    // 3. Make room on page 0, but keep position 0 alive: page 0's first reusable slot is NOT 0.
    database.transaction(() -> {
      for (int i = 0; i < 40; i++)
        page0Fillers.get(i).asDocument(true).delete();
    });

    // 4./5. Spill the target (head chunk on page 1, continuation on page 0), then shrink the chain.
    database.transaction(() -> target[0].asDocument(true).modify().set("v", "x".repeat(30_000)).save());
    database.transaction(() -> target[0].asDocument(true).modify().set("v", "y".repeat(300)).save());

    // 6. Now position 0 is page 0's first reusable slot.
    database.transaction(() -> slotZero[0].asDocument(true).delete());

    // 7. Grow the chain past what its last chunk declares.
    final String regrown = "z".repeat(500);
    database.transaction(() -> target[0].asDocument(true).modify().set("v", regrown).save());

    database.transaction(() -> assertThat(target[0].asDocument(true).getString("v")).isEqualTo(regrown));
    checkDatabase();
  }

  /**
   * The allocator's per-page answer, on a page 0 whose slot 0 is a hole, through the full slot walk: a continuation
   * chunk must be offered another slot, an ordinary record may still reuse slot 0.
   */
  @Test
  void theSlotWalkNeverOffersPositionZeroToAContinuationChunk() {
    final RID zero = pageZeroWithAHoleAtSlotZero("Walk");

    database.transaction(() -> {
      final LocalBucket bucket = bucketOf("Walk");
      assertThat(bucket.getSlotOfferedInPageForTesting(0, true)).as("a continuation chunk may never take position 0").isPositive();
      assertThat(bucket.getSlotOfferedInPageForTesting(0, false)).as("an ordinary record still reuses the hole").isZero();
    });
    assertThat(zero.getPosition()).isZero();
  }

  /**
   * Same question, answered from the per-transaction record-table summary (#10612) rather than the full walk: a page
   * this transaction already took for modification and whose table it already summarized.
   */
  @Test
  void theSlotSummaryShortcutNeverOffersPositionZeroToAContinuationChunk() {
    final RID zero = pageZeroWithAHoleAtSlotZero("Summary");

    database.transaction(() -> {
      final LocalBucket bucket = bucketOf("Summary");
      // Take page 0 for modification so the allocator reads the transaction's own page, then let the full walk
      // summarize it: the next question is answered by the shortcut.
      onSlot(zero, page -> 0L);
      assertThat(bucket.getSlotOfferedInPageForTesting(0, false)).as("the walk that writes the summary").isZero();
      final long slotsReadBefore = bucket.getInsertSlotsReadForTesting();
      assertThat(bucket.getSlotOfferedInPageForTesting(0, true)).as("a continuation chunk may never take position 0").isPositive();
      assertThat(bucket.getInsertSlotsReadForTesting() - slotsReadBefore).as("the shortcut, not a second full walk, answered")
          .isLessThan(10);
      assertThat(bucket.getSlotOfferedInPageForTesting(0, false)).as("the summary must still offer the hole to a plain record")
          .isZero();
    });
  }

  /** Page 0 of a single-bucket type holding a few records, with slot 0 freed by a committed delete. */
  private RID pageZeroWithAHoleAtSlotZero(final String typeName) {
    database.transaction(() -> database.getSchema().createDocumentType(typeName, 1).createProperty("v", Type.STRING));
    final RID[] zero = new RID[1];
    database.transaction(() -> {
      zero[0] = database.newDocument(typeName).set("v", "zero").save().getIdentity();
      for (int i = 0; i < 5; i++)
        database.newDocument(typeName).set("v", "r" + i).save();
    });
    assertThat(zero[0].getPosition()).isZero();
    database.transaction(() -> zero[0].asDocument(true).delete());
    return zero[0];
  }
}
