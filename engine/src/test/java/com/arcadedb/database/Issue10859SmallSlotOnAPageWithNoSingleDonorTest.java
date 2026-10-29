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

import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.schema.Type;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Issue #10859, the residual of #10669: a record whose slot is smaller than a placeholder pointer (an empty document
 * written before records were padded, 7 bytes) grows past a sealed page by having another record of the page release
 * the bytes the pointer misses. Three shapes were left over, each covered here on a page built byte by byte, since no
 * sequence of inserts and updates produces them on a page of the default size:
 * <ul>
 * <li>no single record can release every missing byte, but several can between them: each gives a part;</li>
 * <li>the only record with bytes to spare is a placeholder's CONTENT record, which cannot move behind a pointer of its
 * own: it becomes the head of a chunk chain, as the content of a placeholder no page could host already is;</li>
 * <li>the transaction that takes bytes from a placeholder's content also updates that placeholder: the off-page
 * fingerprint it took at save() is carried across its own write instead of reading as a concurrent modification.</li>
 * </ul>
 * A page whose records cannot release the bytes even together still refuses the update, and the refusal rolls the
 * whole transaction back. And a record written now is padded to a pointer's footprint, so new data never takes the
 * small slot in the first place.
 */
class Issue10859SmallSlotOnAPageWithNoSingleDonorTest extends BucketPageLayoutTestSupport {
  private static final String BIG               = "b".repeat(20 * 1024);
  /** A placeholder pointer: the 1-byte RECORD_PLACEHOLDER_POINTER marker plus the 8-byte position. */
  private static final int    POINTER_FOOTPRINT = 1 + Binary.LONG_SERIALIZED_SIZE;
  /** Slots of a page of a bucket created now: the content area below is sized against it. */
  private static final int    SLOTS_IN_PAGE     = 2048;
  /**
   * A page of about 1 KB of content: small enough for records no bigger than a pointer to fill it before its slot table
   * runs out, which a page of the default size, with the same slots, never lets them do.
   */
  private static final int    PAGE_SIZE         =
      BasePage.PAGE_HEADER_SIZE + Binary.SHORT_SERIALIZED_SIZE + SLOTS_IN_PAGE * Binary.INT_SERIALIZED_SIZE + 1024;
  private static final String QUOTED            = "q".repeat(40);

  /**
   * The shape the issue says no test reaches any more: a sealed page whose other records are all no bigger than a
   * pointer. Nothing can release the bytes, so the update is refused - and with it every other write of its
   * transaction.
   */
  @Test
  void aPageNoRecordOfWhichCanReleaseBytesRefusesTheUpdateAndRollsItsTransactionBack() {
    final RID[] other = new RID[1];
    database.transaction(() -> {
      database.getSchema().createDocumentType("RollbackOther10859", 1).createProperty("v", Type.STRING);
      other[0] = database.newDocument("RollbackOther10859").set("v", "before").save().getIdentity();
    });
    final FabricatedPage page = fabricate("Rollback10859", 1, new int[0], false);
    final long countBefore = countRecordsFromCounter("Rollback10859");

    assertRefused(() -> database.transaction(() -> {
      other[0].asDocument(true).modify().set("v", "after").save();
      database.newDocument("Rollback10859").set("v", "inserted").save();
      grow(page.small.getFirst());
    }));

    assertIntact("Rollback10859", countBefore);
    assertThat(countRecordsHolding("Rollback10859", "inserted")).as("the insert must have been rolled back").isZero();
    database.transaction(() -> {
      assertThat(other[0].asDocument(true).getString("v")).as("the other update must have been rolled back")
          .isEqualTo("before");
      assertThat(page.small.getFirst().asDocument(true).getPropertyNames()).isEmpty();
    });
  }

  /**
   * Two records of 11 bytes, each able to release 2 of the 4 bytes the small slot needs: neither can on its own, both
   * can together, and both move behind a pointer of their own.
   */
  @Test
  void severalRecordsReleaseAPartEach() {
    final FabricatedPage page = fabricate("Several10859", 1, new int[] { 11, 11 }, false);
    final long countBefore = countRecordsFromCounter("Several10859");
    assertThat((Long) bucketStats("Several10859").get("totalPlaceholderRecords")).as("the premise: no pointer yet").isZero();

    database.transaction(() -> grow(page.small.getFirst()));

    assertThat((Long) bucketStats("Several10859").get("totalPlaceholderRecords"))
        .as("the grown record and both donors live behind a pointer now").isEqualTo(3L);
    assertIntact("Several10859", countBefore);
    database.transaction(() -> {
      assertThat(page.small.getFirst().asDocument(true).getString("v")).isEqualTo(BIG);
      for (final RID donor : page.donors)
        assertThat(donor.asDocument(true).getPropertyNames()).isEmpty();
    });

    reopenDatabase();
    database.transaction(() -> assertThat(database.lookupByRID(page.small.getFirst(), true).asDocument().getString("v"))
        .isEqualTo(BIG));
    checkDatabase();
  }

  /**
   * The only record with bytes to spare is the CONTENT record of a placeholder whose pointer is on the same page: it
   * becomes the head of a chunk chain and keeps its slot, so the pointer still finds it, and the placeholder reads
   * back, updates and deletes as before.
   */
  @Test
  void aPlaceholderContentRecordBecomesAChunkChainToReleaseTheBytes() {
    final FabricatedPage page = fabricate("Content10859", 1, new int[0], true);
    final long countBefore = countRecordsFromCounter("Content10859");

    database.transaction(() -> grow(page.small.getFirst()));

    assertThat(markerOf(page.content)).as("the content record is now the head of a chunk chain")
        .isEqualTo(-4L);
    assertIntact("Content10859", countBefore);
    database.transaction(() -> {
      assertThat(page.small.getFirst().asDocument(true).getString("v")).isEqualTo(BIG);
      assertThat(page.placeholder.asDocument(true).getString("v")).isEqualTo(QUOTED);
    });

    reopenDatabase();
    database.transaction(() -> assertThat(database.lookupByRID(page.placeholder, true).asDocument().getString("v"))
        .isEqualTo(QUOTED));
    database.transaction(() -> database.lookupByRID(page.placeholder, true).asDocument().modify().set("v", "rewritten").save());
    database.transaction(() -> assertThat(database.lookupByRID(page.placeholder, true).asDocument().getString("v"))
        .isEqualTo("rewritten"));
    checkDatabase();

    database.transaction(() -> database.deleteRecord(database.lookupByRID(page.placeholder, true)));
    assertIntact("Content10859", countBefore - 1);
    database.transaction(() -> assertThat(database.lookupByRID(page.small.getFirst(), true).asDocument().getString("v"))
        .isEqualTo(BIG));
  }

  /**
   * The placeholder whose content gives the bytes is updated by the same transaction: the off-page fingerprint taken
   * when it was saved no longer describes its content once that content is a chunk chain, but the change is this
   * transaction's own, and both writes commit.
   */
  @Test
  void aPlaceholderWhoseContentGivesTheBytesIsUpdatedInTheSameTransaction() {
    final FabricatedPage page = fabricate("SameTx10859", 1, new int[0], true);
    final String rewritten = "r".repeat(60);

    database.transaction(() -> {
      page.placeholder.asDocument(true).modify().set("v", rewritten).save();
      grow(page.small.getFirst());
    });

    assertIntact("SameTx10859", 2 + page.fillers);
    database.transaction(() -> {
      assertThat(page.small.getFirst().asDocument(true).getString("v")).isEqualTo(BIG);
      assertThat(page.placeholder.asDocument(true).getString("v")).isEqualTo(rewritten);
    });
  }

  /**
   * Item 3 of the issue as it stood: a placeholder's content is ALREADY the head of a chunk chain (the first small slot
   * made it one), and the head is the only chunk of the page able to give a second small slot its bytes. The same
   * transaction updates the placeholder: before #10859 the fingerprint mismatch was a ConcurrentModificationException,
   * which every retry met again.
   */
  @Test
  void aPlaceholderWhoseContentHeadChunkGivesTheBytesIsUpdatedInTheSameTransaction() {
    final FabricatedPage page = fabricate("Head10859", 2, new int[0], true);
    database.transaction(() -> grow(page.small.getFirst()));
    assertThat(markerOf(page.content)).as("the premise: the placeholder's content is a chunk chain").isEqualTo(-4L);

    final String rewritten = "r".repeat(70);
    database.transaction(() -> {
      page.placeholder.asDocument(true).modify().set("v", rewritten).save();
      grow(page.small.get(1));
    });

    assertIntact("Head10859", 3 + page.fillers);
    database.transaction(() -> {
      assertThat(page.small.get(0).asDocument(true).getString("v")).isEqualTo(BIG);
      assertThat(page.small.get(1).asDocument(true).getString("v")).isEqualTo(BIG);
      assertThat(page.placeholder.asDocument(true).getString("v")).isEqualTo(rewritten);
    });
  }

  /**
   * Pad at create: an empty document takes a pointer's footprint, so it turns into a pointer in place the day it
   * outgrows its page; an overwrite with shorter content keeps the slot that size; a slot written before the padding
   * keeps its own size when it is overwritten, instead of having to grow for padding's sake.
   */
  @Test
  void aRecordIsPaddedToAPointersFootprint() {
    final RID[] rids = new RID[2];
    database.transaction(() -> {
      database.getSchema().createDocumentType("Padded10859", 1).createProperty("v", Type.STRING);
      rids[0] = database.newDocument("Padded10859").save().getIdentity();
      rids[1] = database.newDocument("Padded10859").set("v", "a value").save().getIdentity();
    });
    assertThat(footprintOf(rids[0])).as("an empty document, created").isEqualTo(POINTER_FOOTPRINT);

    database.transaction(() -> removeValue(rids[1]));
    assertThat(footprintOf(rids[1])).as("a record overwritten with nothing").isGreaterThanOrEqualTo(POINTER_FOOTPRINT);

    final int legacy = shrinkToUnpaddedSize(rids[0]);
    assertThat(legacy).as("the premise: a slot written before #10859 is smaller than a pointer").isLessThan(POINTER_FOOTPRINT);
    database.transaction(() -> {
      // Dirty, and serialized exactly as it was: an overwrite of the same empty content
      final MutableDocument document = rids[0].asDocument(true).modify();
      document.set("v", "transient");
      document.remove("v");
      document.save();
    });
    assertThat(footprintOf(rids[0])).as("an overwrite never grows a slot to pad it").isEqualTo(legacy);
    database.transaction(() -> assertThat(rids[0].asDocument(true).getPropertyNames()).isEmpty());
    checkDatabase();
  }

  /** A record that spilled into chunks and shrinks back into its slot is padded as a record created now is. */
  @Test
  void aChunkChainCollapsingIntoAnEmptyRecordIsPadded() {
    final RID[] rid = new RID[1];
    database.transaction(() -> {
      database.getSchema().createDocumentType("Collapsed10859", 1).createProperty("v", Type.STRING);
      rid[0] = database.newDocument("Collapsed10859").set("v", "s").save().getIdentity();
    });
    sealFirstPageWith(rid[0]);
    assertThat(markerOf(rid[0])).as("the premise: the record spilled into a chunk chain").isEqualTo(-2L);

    database.transaction(() -> removeValue(rid[0]));

    assertThat(footprintOf(rid[0])).as("collapsed back into a plain record").isGreaterThanOrEqualTo(POINTER_FOOTPRINT);
    database.transaction(() -> assertThat(rid[0].asDocument(true).getPropertyNames()).isEmpty());
    checkDatabase();
  }

  private static void removeValue(final RID rid) {
    final MutableDocument document = rid.asDocument(true).modify();
    document.remove("v");
    document.save();
  }

  private void sealFirstPageWith(final RID rid) {
    database.transaction(() -> rid.asDocument(true).modify().set("v", "s".repeat(70 * 1024)).save());
  }

  /** What page 0 of a fabricated type holds, by RID. */
  private record FabricatedPage(List<RID> small, List<RID> donors, RID placeholder, RID content, int fillers) {
  }

  /**
   * Builds page 0 of a single-bucket type whose pages hold about 1 KB of content, in this order: {@code smallSlots}
   * empty documents given back the smaller slot of a record written before #10859; an empty document every filler is
   * copied from; with {@code withPlaceholder}, a record moved behind a placeholder pointer whose content record lands
   * right after the fillers' template; one record per entry of {@code donorFootprints}, of that footprint; and fillers no
   * bigger than a pointer up to the very end of the content area, so the page has no free tail at all. Every record is
   * an empty document but the placeholder, and the bucket's counter counts them all.
   */
  private FabricatedPage fabricate(final String typeName, final int smallSlots, final int[] donorFootprints,
      final boolean withPlaceholder) {
    final List<RID> small = new ArrayList<>();
    final RID[] template = new RID[1];
    final RID[] placeholder = new RID[1];
    database.transaction(() -> {
      database.getSchema().buildDocumentType().withName(typeName).withTotalBuckets(1).withPageSize(PAGE_SIZE).create()
          .createProperty("v", Type.STRING);
      for (int i = 0; i < smallSlots; i++)
        small.add(database.newDocument(typeName).save().getIdentity());
      template[0] = database.newDocument(typeName).save().getIdentity();
      if (withPlaceholder)
        // Empty, so its slot is exactly a pointer's footprint once it is one: no hole a pack could give the page back
        placeholder[0] = database.newDocument(typeName).save().getIdentity();
    });
    final LocalBucket bucket = bucketOf(typeName);
    assertThat(bucket.getMaxRecordsInPage()).as("the premise: the page geometry the content area is sized against")
        .isEqualTo(SLOTS_IN_PAGE);
    small.forEach(this::shrinkToUnpaddedSize);

    final List<RID> donors = new ArrayList<>();
    final RID[] content = new RID[1];
    final int[] fillers = new int[1];
    final DatabaseInternal db = (DatabaseInternal) database;
    final int emptyContent = db.getSerializer().serialize(db, database.newDocument(typeName)).size();
    final byte[] quoted = db.getSerializer().serialize(db, database.newDocument(typeName).set("v", QUOTED)).toByteArray();
    database.transaction(() -> onSlot(template[0], page -> {
      final int templateOffset = recordOffsetOf(page, template[0]);
      final long[] templateSize = page.readNumberAndSize(templateOffset);
      final byte[] templateContent = new byte[(int) templateSize[0]];
      page.readByteArray(templateOffset + (int) templateSize[1], templateContent, 0, templateContent.length);

      int slots = page.readShort(0);
      int end = contentEnd(page);

      if (withPlaceholder) {
        final int offset = recordOffsetOf(page, placeholder[0]);
        assertThat(page.readNumberAndSize(offset)).as("the premise: a slot a pointer fills exactly")
            .containsExactly(POINTER_FOOTPRINT - 1, 1L);

        content[0] = new RID(template[0].getBucketId(), slots);
        end = appendSlot(page, slots++, end, -quoted.length, quoted, quoted.length, quoted.length);
        assertThat(page.writeNumber(offset, -1L)).isEqualTo(1);
        page.writeLong(offset + 1, content[0].getPosition());
      }

      int counted = 0;
      for (final int footprint : donorFootprints) {
        donors.add(new RID(template[0].getBucketId(), slots));
        end = appendSlot(page, slots++, end, footprint - 1, templateContent, templateContent.length, emptyContent);
        counted++;
      }

      // Empty documents of an unpadded record's footprint up to a pointer's, never more: none of them has a byte to
      // spare, and together they end exactly at the end of the content area.
      final int maxShave = POINTER_FOOTPRINT - 1 - emptyContent;
      final int room = page.getMaxContentSize() - end;
      final int records = (room + POINTER_FOOTPRINT - 1) / POINTER_FOOTPRINT;
      int excess = records * POINTER_FOOTPRINT - room;
      for (int i = 0; i < records; i++) {
        final int shave = Math.min(maxShave, excess);
        excess -= shave;
        end = appendSlot(page, slots++, end, POINTER_FOOTPRINT - 1 - shave, templateContent, templateContent.length,
            emptyContent);
        counted++;
      }
      assertThat(excess).isZero();
      assertThat(slots).as("the premise: the slot table holds every record").isLessThan(SLOTS_IN_PAGE);
      assertThat(end).as("the premise: the page has no free tail").isEqualTo(page.getMaxContentSize());

      page.writeShort(0, (short) slots);
      ((DatabaseInternal) database).getTransaction().updateBucketRecordDelta(template[0].getBucketId(), counted);
      fillers[0] = counted - donors.size();
      return 0L;
    }));

    return new FabricatedPage(small, donors, placeholder[0], content[0], fillers[0] + 1);
  }

  /**
   * Writes a record of {@code size} bytes (negated for a placeholder's content) at {@code end} of the page, its content
   * the first bytes of {@code body} and blanks past them, and points slot {@code slot} at it. {@code serializedLength}
   * is how many bytes of {@code body} the record's deserializer reads.
   *
   * @return where the page's content now ends.
   */
  private static int appendSlot(final MutablePage page, final int slot, final int end, final int size, final byte[] body,
      final int bodyLength, final int serializedLength) {
    final int length = Math.abs(size);
    assertThat(length).as("a record never keeps fewer bytes than its serialized content").isGreaterThanOrEqualTo(serializedLength);
    page.writeUnsignedInt(Binary.SHORT_SERIALIZED_SIZE + slot * Binary.INT_SERIALIZED_SIZE, end);
    final int marker = page.writeNumber(end, size);
    final byte[] content = new byte[length];
    System.arraycopy(body, 0, content, 0, Math.min(length, bodyLength));
    page.writeByteArray(end + marker, content, 0, length);
    return end + marker + length;
  }

  private static void grow(final RID rid) {
    rid.asDocument(true).modify().set("brand", "x").set("v", BIG).save();
  }

  private static void assertRefused(final ThrowingCallable update) {
    final Throwable thrown = catchThrowable(update);
    assertThat(thrown).as("the update must be refused").isNotNull();
    Throwable cause = thrown;
    while (cause != null && !(cause instanceof DatabaseOperationException
        && String.valueOf(cause.getMessage()).contains("cannot host a placeholder pointer")))
      cause = cause.getCause();
    assertThat(cause).as("refused because the slot cannot host the pointer, but got: " + thrown).isNotNull();
  }

  /** Every record is still there and readable: a scan agrees with the counter, and the database checks out clean. */
  private void assertIntact(final String typeName, final long expectedCount) {
    assertThat(countRecordsFromCounter(typeName)).isEqualTo(expectedCount);
    assertThat(countRecords(typeName)).as("a scan must return every record the counter counts").isEqualTo(expectedCount);
    checkDatabase();
  }

  /** The size marker {@code rid}'s slot starts with. */
  private long markerOf(final RID rid) {
    final long[] marker = new long[1];
    database.transaction(() -> onSlot(rid, page -> marker[0] = page.readNumberAndSize(recordOffsetOf(page, rid))[0]));
    return marker[0];
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

  /** Where the content of {@code page} ends: the end of the record that ends last. */
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
        footprint = (int) size[1] + Binary.INT_SERIALIZED_SIZE + Binary.LONG_SERIALIZED_SIZE + page.readInt((int) (offset + size[1]));
      else
        footprint = (int) (-size[0] + size[1]);
      end = Math.max(end, offset + footprint);
    }
    return end;
  }
}
