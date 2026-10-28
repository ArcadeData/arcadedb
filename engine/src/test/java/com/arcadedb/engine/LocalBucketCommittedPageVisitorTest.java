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
import com.arcadedb.database.RID;
import com.arcadedb.engine.LocalBucket.CommittedSlotVisitor;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalDocumentType;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link LocalBucket#visitCommittedPage}: the transaction-free, committed-version read of a bucket page the columnar apply
 * pass is built on. Placeholders are built with {@link BucketPageLayoutTestSupport#sealFirstPage} (since #6149 a record that
 * outgrows its page becomes a chunk chain unless the page has no free tail).
 */
class LocalBucketCommittedPageVisitorTest extends BucketPageLayoutTestSupport {
  /** One reported slot, with its arguments copied because the page array and the content view are only valid in the callback. */
  private record Slot(long position, int kind, byte[] content, int[] pages) {
  }

  private static final class Collector implements CommittedSlotVisitor {
    final List<Slot> slots = new ArrayList<>();

    @Override
    public void onSlot(final long position, final int kind, final Binary content, final int[] offPagePages, final int offPageCount) {
      slots.add(new Slot(position, kind, content != null ? content.toByteArray() : null, Arrays.copyOf(offPagePages, offPageCount)));
    }

    Slot at(final long position) {
      return slots.stream().filter(s -> s.position == position).findFirst().orElseThrow();
    }
  }

  private Collector visit(final LocalBucket bucket, final int page) {
    final Collector collector = new Collector();
    assertThat(bucket.visitCommittedPage(page, collector)).isGreaterThanOrEqualTo(0L);
    return collector;
  }

  private byte[] loaded(final LocalBucket bucket, final RID rid) {
    final byte[][] result = new byte[1][];
    database.transaction(() -> result[0] = bucket.getRecord(rid).toByteArray());
    return result[0];
  }

  /** Walks the chain from the head slot: [marker:1][chunkSize:int][nextChunkPointer:long] per chunk. */
  private Integer[] chunkPagesAfterTheHead(final RID head) {
    final int maxRecords = ((LocalBucket) database.getSchema().getBucketById(head.getBucketId())).getMaxRecordsInPage();
    final Set<Integer> pages = new LinkedHashSet<>();
    RID current = head;
    while (true) {
      final RID chunk = current;
      final long[] next = new long[1];
      database.transaction(() -> onSlot(chunk, page -> next[0] = page.readLong(recordOffsetOf(page, chunk) + 1 + Binary.INT_SERIALIZED_SIZE)));
      if (next[0] == 0)
        return pages.toArray(new Integer[0]);
      pages.add((int) (next[0] / maxRecords));
      current = new RID(head.getBucketId(), next[0]);
    }
  }

  private RID create(final String typeName, final String value) {
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument(typeName).set("v", value).save().getIdentity());
    return rid[0];
  }

  private void createType(final String typeName) {
    database.transaction(() -> database.getSchema().createDocumentType(typeName, 1).createProperty("v", Type.STRING));
  }

  @Test
  void plainRecordsAreVisitedWithContent() {
    createType("Plain");
    final List<RID> rids = new ArrayList<>();
    for (int i = 0; i < 5; i++)
      rids.add(create("Plain", "value-" + i));

    final LocalBucket bucket = bucketOf("Plain");
    final Collector collector = visit(bucket, 0);

    assertThat(collector.slots).hasSize(5);
    for (final RID rid : rids) {
      final Slot slot = collector.at(rid.getPosition());
      assertThat(slot.kind).isEqualTo(CommittedSlotVisitor.RECORD);
      assertThat(slot.content).isEqualTo(loaded(bucket, rid));
      assertThat(slot.pages).isEmpty();
    }
  }

  @Test
  void deletedSlotsAreEmpty() {
    createType("Holes");
    final List<RID> rids = new ArrayList<>();
    for (int i = 0; i < 4; i++)
      rids.add(create("Holes", "value-" + i));
    database.transaction(() -> rids.get(1).asDocument(true).delete());

    final Collector collector = visit(bucketOf("Holes"), 0);

    assertThat(collector.at(rids.get(1).getPosition()).kind).isEqualTo(CommittedSlotVisitor.EMPTY);
    assertThat(collector.at(rids.get(1).getPosition()).content).isNull();
    for (final int i : new int[] { 0, 2, 3 })
      assertThat(collector.at(rids.get(i).getPosition()).kind).isEqualTo(CommittedSlotVisitor.RECORD);
  }

  @Test
  void placeholderPointerOwnerGetsTargetPage() {
    createType("Holder");
    final RID tiny = create("Holder", "p");
    sealFirstPage(database, "Holder");
    final String big = "b".repeat(20 * 1024);
    database.transaction(() -> tiny.asDocument(true).modify().set("v", big).save());
    assertThat((Long) bucketStats("Holder").get("totalPlaceholderRecords")).as("the fixture must be a placeholder").isEqualTo(1L);

    final LocalBucket bucket = bucketOf("Holder");
    final Slot owner = visit(bucket, 0).at(tiny.getPosition());

    assertThat(owner.kind).isEqualTo(CommittedSlotVisitor.RECORD);
    assertThat(owner.content).isNotNull().isEqualTo(loaded(bucket, tiny));
    assertThat(owner.pages).hasSize(1);
    assertThat(owner.pages[0]).isNotZero();

    // [marker:1][target position:long]: the page also holds the continuation chunk of the sealing record, so name the target
    final long[] target = new long[1];
    database.transaction(() -> onSlot(tiny, page -> target[0] = page.readLong(recordOffsetOf(page, tiny) + 1)));
    assertThat(target[0] / bucket.getMaxRecordsInPage()).isEqualTo(owner.pages[0]);
    final Slot content = visit(bucket, owner.pages[0]).at(target[0]);
    assertThat(content.kind).as("the target is a CONTENT slot of the page the owner named").isEqualTo(CommittedSlotVisitor.CONTENT);
    assertThat(content.content).isNull();
  }

  @Test
  void staticSealFirstPageMatchesTheInstanceForm() {
    createType("StaticSeal");
    createType("InstanceSeal");
    create("StaticSeal", "p");
    create("InstanceSeal", "p");

    final RID statik = sealFirstPage(database, "StaticSeal");
    final RID instance = sealFirstPage("InstanceSeal");

    assertThat((Long) bucketStats(database, "StaticSeal").get("totalMultiPageRecords")).isPositive();
    assertThat((Long) bucketStats("InstanceSeal").get("totalMultiPageRecords")).isPositive();
    assertThat(statik.getPosition()).isEqualTo(instance.getPosition());
    assertThat(bucketStats(database, "StaticSeal").get("totalChunks")).isEqualTo(bucketStats("InstanceSeal").get("totalChunks"));
  }

  @Test
  void chunkedRecordReportsEveryChunkPage() {
    createType("Chunked");
    final String huge = "h".repeat(200 * 1024);
    final RID rid = create("Chunked", huge);
    final LocalBucket bucket = bucketOf("Chunked");

    final Slot owner = visit(bucket, 0).at(rid.getPosition());

    assertThat(owner.kind).isEqualTo(CommittedSlotVisitor.RECORD);
    assertThat(owner.content).isNotNull().isEqualTo(loaded(bucket, rid));
    assertThat(owner.pages.length).as("200 KB over 64 KB pages").isGreaterThanOrEqualTo(2);
    assertThat(owner.pages).doesNotHaveDuplicates();
    assertThat(owner.pages).doesNotContain(0);
    assertThat(owner.pages).containsExactlyInAnyOrder(chunkPagesAfterTheHead(rid));
    long continuationSlots = 0;
    for (final int page : owner.pages)
      continuationSlots += visit(bucket, page).slots.stream().filter(s -> s.kind == CommittedSlotVisitor.CONTENT).count();
    assertThat(continuationSlots).as("every reported page holds at least one continuation chunk").isGreaterThanOrEqualTo(owner.pages.length);
  }

  @Test
  void aChunkChainThatCannotBeWalkedIsAnUnreadableRecord() {
    createType("Broken");
    final RID rid = create("Broken", "h".repeat(200 * 1024));
    final LocalBucket bucket = bucketOf("Broken");
    // [marker:1][chunkSize:int][nextChunkPointer:long]: points the head chunk past the end of the bucket
    final long[] original = new long[1];
    database.transaction(() -> original[0] = onSlot(rid, page -> {
      final int pointerOffset = recordOffsetOf(page, rid) + 1 + Binary.INT_SERIALIZED_SIZE;
      final long previous = page.readLong(pointerOffset);
      page.writeLong(pointerOffset, Integer.MAX_VALUE);
      return previous;
    }));
    try {
      final Slot owner = visit(bucket, 0).at(rid.getPosition());

      assertThat(owner.kind).isEqualTo(CommittedSlotVisitor.RECORD);
      assertThat(owner.content).isNull();
    } finally {
      database.transaction(() -> onSlot(rid, page -> {
        page.writeLong(recordOffsetOf(page, rid) + 1 + Binary.INT_SERIALIZED_SIZE, original[0]);
        return 0L;
      }));
    }
  }

  @Test
  void aSlotWhoseOffsetPointsAtTheEndOfThePageDoesNotFailThePage() {
    createType("Corrupt");
    final RID first = create("Corrupt", "a");
    final RID second = create("Corrupt", "b");
    // a page with no free tail: its used content reaches the physical end, which is the only place a decode can run off the buffer
    sealFirstPage(database, "Corrupt");
    final LocalBucket bucket = bucketOf("Corrupt");
    // point the first slot at the last byte of the page content, holding a varint continuation byte: decoding the size runs off the page
    final long[] saved = new long[2];
    database.transaction(() -> onSlot(first, page -> {
      final int lastByte = page.getMaxContentSize() - 1;
      saved[0] = recordOffsetOf(page, first);
      saved[1] = page.readByte(lastByte);
      page.writeByte(lastByte, (byte) 0x80);
      page.writeUnsignedInt(Binary.SHORT_SERIALIZED_SIZE + (int) (first.getPosition() % bucket.getMaxRecordsInPage()) * Binary.INT_SERIALIZED_SIZE,
          lastByte);
      return 0L;
    }));
    try {
      final Collector collector = visit(bucket, 0);

      assertThat(collector.at(first.getPosition()).kind).isEqualTo(CommittedSlotVisitor.RECORD);
      assertThat(collector.at(first.getPosition()).content).isNull();
      assertThat(collector.at(second.getPosition()).content).isEqualTo(loaded(bucket, second));
    } finally {
      database.transaction(() -> onSlot(first, page -> {
        final int lastByte = page.getContentSize() - 1;
        page.writeByte(lastByte, (byte) saved[1]);
        page.writeUnsignedInt(Binary.SHORT_SERIALIZED_SIZE + (int) (first.getPosition() % bucket.getMaxRecordsInPage()) * Binary.INT_SERIALIZED_SIZE,
            saved[0]);
        return 0L;
      }));
    }
  }

  @Test
  void legacyPlaceholderContentHeadIsContent() {
    createType("Surrogate");
    final RID tiny = create("Surrogate", "p");
    sealFirstPage(database, "Surrogate");
    final String huge = "h".repeat(200 * 1024);
    database.transaction(() -> tiny.asDocument(true).modify().set("v", huge).save());
    assertThat((Long) bucketStats("Surrogate").get("totalPlaceholderRecords")).as("the fixture must be a placeholder").isEqualTo(1L);

    final LocalBucket bucket = bucketOf("Surrogate");
    final Slot owner = visit(bucket, 0).at(tiny.getPosition());

    assertThat(owner.kind).isEqualTo(CommittedSlotVisitor.RECORD);
    assertThat(owner.content).isNotNull().isEqualTo(loaded(bucket, tiny));
    assertThat(owner.pages.length).as("the content head page and its continuation chunks").isGreaterThanOrEqualTo(2);

    // the head of the content chain (FIRST_CHUNK_PLACEHOLDER_CONTENT, -4) must never be reported as a record
    long contentHeads = 0;
    for (final int page : owner.pages)
      for (final Slot slot : visit(bucket, page).slots)
        if (slot.kind == CommittedSlotVisitor.CONTENT)
          ++contentHeads;
        else
          assertThat(slot.kind).isEqualTo(CommittedSlotVisitor.EMPTY);
    assertThat(contentHeads).isGreaterThanOrEqualTo(owner.pages.length);
  }

  @Test
  void externalBucketPagesAreVisitedToo() {
    final DocumentType type = database.getSchema().createDocumentType("Ext", 1);
    type.createProperty("payload", Type.STRING).setExternal(true);
    database.transaction(() -> {
      for (int i = 0; i < 300; i++)
        database.newDocument("Ext").set("payload", "x".repeat(50)).save();
    });
    final int primaryId = type.getBuckets(false).getFirst().getFileId();
    final int externalId = ((LocalDocumentType) type).getExternalBucketIdFor(primaryId);
    final LocalBucket external = (LocalBucket) ((LocalSchema) database.getSchema().getEmbedded()).getBucketById(externalId);

    assertThat(external.getMaxRecordsInPage()).isEqualTo(256);
    final Collector first = visit(external, 0);
    final Collector second = visit(external, 1);

    assertThat(first.slots).hasSize(256);
    assertThat(first.slots).allMatch(s -> s.position >= 0 && s.position < 256 && s.kind == CommittedSlotVisitor.RECORD);
    assertThat(second.slots).isNotEmpty();
    assertThat(second.slots).allMatch(s -> s.position >= 256 && s.position < 512);
    assertThat(second.slots.getFirst().position).isEqualTo(256L);
  }

  @Test
  void versionIsTheCommittedOne() throws Exception {
    createType("Versioned");
    final RID rid = create("Versioned", "before");
    final LocalBucket bucket = bucketOf("Versioned");
    final DatabaseInternal db = (DatabaseInternal) database;
    final int pageSize = ((PaginatedComponentFile) db.getFileManager().getFile(bucket.getFileId())).getPageSize();

    final Collector first = new Collector();
    final long v1 = bucket.visitCommittedPage(0, first);
    assertThat(v1).isEqualTo(db.getPageManager().getImmutablePage(new PageId(db, bucket.getFileId(), 0), pageSize, false, false).getVersion());

    database.transaction(() -> rid.asDocument(true).modify().set("v", "after!").save());
    final Collector second = new Collector();
    final long v2 = bucket.visitCommittedPage(0, second);
    assertThat(v2).isGreaterThan(v1);
    assertThat(second.at(rid.getPosition()).content).isNotEqualTo(first.at(rid.getPosition()).content);

    // an open transaction's own uncommitted image is not what the visitor sees: a create writes its page at once
    database.begin();
    try {
      final RID inserted = database.newDocument("Versioned").set("v", "dirty!").save().getIdentity();
      assertThat(inserted.getPosition() / bucket.getMaxRecordsInPage()).as("the insert must land on page 0").isZero();
      final Collector during = new Collector();
      assertThat(bucket.visitCommittedPage(0, during)).isEqualTo(v2);
      assertThat(during.slots).hasSameSizeAs(second.slots);
    } finally {
      database.rollback();
    }

    assertThat(bucket.visitCommittedPage(bucket.getCommittedPageCount(), new Collector())).as("not committed yet").isEqualTo(-1L);
    assertThat(bucket.visitCommittedPage(-1, new Collector())).isEqualTo(-1L);
  }
}
