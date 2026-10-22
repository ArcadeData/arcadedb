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

import com.arcadedb.engine.MutablePage;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.schema.Type;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Issue #10617: a record that outgrows its page and whose slot is too small for a chunk header spills into a
 * placeholder POINTER - a 1-byte marker plus an 8-byte position, 9 bytes in all. The smallest slot is 6 bytes (an
 * empty document), and nothing made sure the slot could hold the 9 bytes before writing them: the pointer overwrote
 * the start of the next record, or ran past the page's content area when the slot was the last one.
 * <p>
 * The pointer must now either fit the slot, fit it once the slot is enlarged by the in-page shift a growth uses, or
 * not be written at all.
 */
class Issue10617PlaceholderPointerSlotOverflowTest extends BucketPageLayoutTestSupport {
  private static final String BIG               = "b".repeat(20 * 1024);
  /** A placeholder pointer: the 1-byte RECORD_PLACEHOLDER_POINTER marker plus the 8-byte position. */
  private static final int    POINTER_FOOTPRINT = 1 + Binary.LONG_SERIALIZED_SIZE;

  /**
   * The issue's first shape: a slot smaller than a pointer that is NOT the last record, on a page with no free tail
   * at all. The missing bytes cannot come from anywhere on the page, so the update must be refused - and the record after it must
   * survive untouched.
   */
  @Test
  void aSmallSlotOnASealedPageIsNotOverwrittenPastItsEnd() {
    final RID[] empty = new RID[1];
    final RID[] neighbour = new RID[1];
    database.transaction(() -> {
      database.getSchema().createDocumentType("Sealed10617", 1).createProperty("v", Type.STRING);
      empty[0] = database.newDocument("Sealed10617").save().getIdentity();
      neighbour[0] = database.newDocument("Sealed10617").set("v", "neighbour").save().getIdentity();
    });
    sealFirstPage("Sealed10617");

    assertThat(footprintOf(empty[0])).as("the premise: an empty document takes a slot too small for a pointer")
        .isLessThan(POINTER_FOOTPRINT);
    assertThat(freeTailOfPageOf(empty[0])).as("the premise: the page has no free tail").isZero();

    final long countBefore = countRecordsFromCounter("Sealed10617");

    assertRefused(() -> database.transaction(() -> grow(empty[0])));

    assertIntact("Sealed10617", countBefore);
    database.transaction(() -> {
      assertThat(neighbour[0].asDocument(true).getString("v")).isEqualTo("neighbour");
      assertThat(empty[0].asDocument(true).getPropertyNames()).isEmpty();
    });
  }

  /**
   * The same small slot, not the last record, but with a free tail too small for a chunk header and big enough for
   * the bytes a pointer is missing. Before the fix the pointer was written anyway and spilled into the next record; now the slot is enlarged first, and the update succeeds without touching anything else.
   */
  @Test
  void aSmallSlotIsEnlargedToHostThePointerWhenThePageHasTheBytes() {
    final RID[] empty = new RID[1];
    final RID[] neighbour = new RID[1];
    database.transaction(() -> {
      database.getSchema().createDocumentType("Enlarged10617", 1).createProperty("v", Type.STRING);
      empty[0] = database.newDocument("Enlarged10617").save().getIdentity();
      neighbour[0] = database.newDocument("Enlarged10617").set("v", "neighbour").save().getIdentity();
    });
    final RID filler = fillFirstPage("Enlarged10617");
    leaveFreeTail(filler, 5);

    assertThat(footprintOf(empty[0])).isLessThan(POINTER_FOOTPRINT);

    final long countBefore = countRecordsFromCounter("Enlarged10617");
    database.transaction(() -> grow(empty[0]));

    final Map<String, Object> layout = bucketStats("Enlarged10617");
    assertThat((Long) layout.get("totalPlaceholderRecords")).as("the slot must have become a pointer: " + layout)
        .isEqualTo(1L);
    assertIntact("Enlarged10617", countBefore);
    database.transaction(() -> {
      assertThat(empty[0].asDocument(true).getString("v")).isEqualTo(BIG);
      assertThat(empty[0].asDocument(true).getString("brand")).isEqualTo("x");
      assertThat(neighbour[0].asDocument(true).getString("v")).isEqualTo("neighbour");
    });
  }

  /**
   * The issue's second shape: the small slot is the LAST record of its page and the free tail is one byte short of
   * what the pointer is missing. The pointer used to run past the page's content area; now the update is refused.
   */
  @Test
  void aSmallLastSlotIsNotWrittenPastTheContentArea() {
    final RID empty = lastEmptyDocumentWithFreeTail("Last10617", -1);
    final long countBefore = countRecordsFromCounter("Last10617");

    assertRefused(() -> database.transaction(() -> grow(empty)));

    assertIntact("Last10617", countBefore);
    assertThat(freeTailOfPageOf(empty)).as("the refused update must leave the page as it was")
        .isEqualTo(POINTER_FOOTPRINT - footprintOf(empty) - 1);
  }

  /**
   * The boundary of the shape above: a last small slot followed by exactly the bytes of tail the pointer is missing
   * hosts it, as it always did.
   */
  @Test
  void aSmallLastSlotWithExactlyTheMissingBytesOfTailHostsThePointer() {
    final RID empty = lastEmptyDocumentWithFreeTail("Exact10617", 0);
    final long countBefore = countRecordsFromCounter("Exact10617");

    database.transaction(() -> grow(empty));

    assertThat((Long) bucketStats("Exact10617").get("totalPlaceholderRecords")).isEqualTo(1L);
    assertIntact("Exact10617", countBefore);
    database.transaction(() -> assertThat(empty.asDocument(true).getString("v")).isEqualTo(BIG));
  }

  /**
   * The update reaches the page at commit, so the refusal arrives wrapped in the commit's own exception: what matters
   * is that it IS the refusal, and not whatever a write past the slot happened to break.
   */
  private static void assertRefused(final ThrowingCallable update) {
    final Throwable thrown = catchThrowable(update);
    assertThat(thrown).as("the update must be refused").isNotNull();
    Throwable cause = thrown;
    while (cause != null && !(cause instanceof DatabaseOperationException
        && String.valueOf(cause.getMessage()).contains("cannot host a placeholder pointer")))
      cause = cause.getCause();
    assertThat(cause).as("refused because the slot cannot host the pointer, but got: " + thrown).isNotNull();
  }

  private static void grow(final RID rid) {
    rid.asDocument(true).modify().set("brand", "x").set("v", BIG).save();
  }

  /** Every record is still there and readable: a scan agrees with the counter, and the database checks out clean. */
  private void assertIntact(final String typeName, final long expectedCount) {
    assertThat(countRecordsFromCounter(typeName)).isEqualTo(expectedCount);
    assertThat(countRecords(typeName)).as("a scan must return every record the counter counts").isEqualTo(expectedCount);
    checkDatabase();
  }

  /**
   * Builds page 0 of a single-bucket type with an empty document as its LAST record, followed by the bytes a pointer
   * would be missing in its slot plus {@code tailBeyondMissing}: the page is filled with 8 KB records, the empty
   * document lands behind them, and the first record is then grown by exactly the bytes that leave that tail free.
   */
  private RID lastEmptyDocumentWithFreeTail(final String typeName, final int tailBeyondMissing) {
    database.transaction(() -> database.getSchema().createDocumentType(typeName, 1).createProperty("v", Type.STRING));
    final RID lastFiller = fillFirstPage(typeName);
    final RID first = new RID(lastFiller.getBucketId(), 0);

    final RID[] empty = new RID[1];
    database.transaction(() -> empty[0] = database.newDocument(typeName).save().getIdentity());
    assertThat(empty[0].getPosition()).as("the premise: the empty document lands right after the fillers on page 0")
        .isEqualTo(lastFiller.getPosition() + 1);
    final int footprint = footprintOf(empty[0]);
    assertThat(footprint).isLessThan(POINTER_FOOTPRINT);

    leaveFreeTail(first, POINTER_FOOTPRINT - footprint + tailBeyondMissing);
    return empty[0];
  }

  /** Grows {@code rid}'s string value by exactly the bytes that leave its page with {@code tail} free bytes. */
  private void leaveFreeTail(final RID rid, final int tail) {
    for (int attempt = 0; attempt < 4; attempt++) {
      final int current = freeTailOfPageOf(rid);
      if (current == tail)
        return;
      final int delta = current - tail;
      database.transaction(() -> {
        final String value = rid.asDocument(true).getString("v");
        rid.asDocument(true).modify().set("v", value + "g".repeat(delta)).save();
      });
    }
    assertThat(freeTailOfPageOf(rid)).as("could not leave exactly " + tail + " free bytes on the page").isEqualTo(tail);
  }

  /** The bytes {@code rid}'s slot takes: size marker plus content, for a plain record. */
  private int footprintOf(final RID rid) {
    final int[] footprint = new int[1];
    database.transaction(() -> onSlot(rid, page -> {
      final long[] size = page.readNumberAndSize(recordOffsetOf(page, rid));
      assertThat(size[0]).as("a plain record").isPositive();
      footprint[0] = (int) (size[0] + size[1]);
      return 0L;
    }));
    return footprint[0];
  }

  /** Bytes between the end of the last record of {@code rid}'s page and the page's maximum content size. */
  private int freeTailOfPageOf(final RID rid) {
    final int[] tail = new int[1];
    database.transaction(() -> onSlot(rid, page -> {
      tail[0] = page.getMaxContentSize() - contentEnd(page);
      return 0L;
    }));
    return tail[0];
  }

  private static int contentEnd(final MutablePage page) {
    final int slots = page.readShort(0);
    int end = 0;
    for (int slot = 0; slot < slots; slot++) {
      final int offset = (int) page.readUnsignedInt(Binary.SHORT_SERIALIZED_SIZE + slot * Binary.INT_SERIALIZED_SIZE);
      if (offset == 0)
        continue;
      final long[] size = page.readNumberAndSize(offset);
      final int footprint;
      if (size[0] > 0)
        footprint = (int) (size[0] + size[1]);
      else if (size[0] == -1L)
        footprint = (int) size[1] + Binary.LONG_SERIALIZED_SIZE;
      else if (size[0] == -2L || size[0] == -3L || size[0] == -4L)
        footprint = (int) size[1] + Binary.INT_SERIALIZED_SIZE + Binary.LONG_SERIALIZED_SIZE + page.readInt(
            (int) (offset + size[1]));
      else
        footprint = (int) (-size[0] + size[1]);
      end = Math.max(end, offset + footprint);
    }
    return end;
  }
}
