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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.Binary;
import com.arcadedb.database.BucketPageLayoutTestSupport;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.exception.BrokenChunkChainException;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntFunction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Consolidated tests for BucketChunkChain behaviours, one nested class per originally reported issue.
 */
class BucketChunkChainTest {

  /**
   * Issue #10701: position 0 of a bucket (slot 0 of page 0) is also the value a chunk's next-chunk pointer uses for "no
   * next chunk". A continuation chunk allocated there is linked with a pointer of 0, so every reader stops one chunk
   * early: the record is silently truncated and can no longer be decoded. The rule "never give a continuation chunk
   * position 0" was applied by one of the three allocator paths only, and on a page holding records it was not applied
   * at all by the slot search underneath them.
   */
  // #10701
  @Nested
  class ContinuationChunkNeverAtPositionZero extends BucketPageLayoutTestSupport {
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

    /**
     * The backstop at the two link sites: whatever an allocator hands out, a link whose value would be 0 is refused instead
     * of written, since writing it truncates the record silently. Every other position links to itself.
     */
    @Test
    void aLinkToPositionZeroIsRefusedNotWritten() {
      assertThatThrownBy(() -> LocalBucket.nextChunkLink(null, "b", 0, 0, 2048))
          .isInstanceOf(DatabaseOperationException.class).hasMessageContaining("no next chunk");
      assertThat(LocalBucket.nextChunkLink(null, "b", 0, 1, 2048)).isEqualTo(1L);
      assertThat(LocalBucket.nextChunkLink(null, "b", 1, 0, 2048)).isEqualTo(2048L);
      assertThat(LocalBucket.nextChunkLink(null, "b", 3, 7, 2048)).isEqualTo(3L * 2048 + 7);
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

  /**
   * Issue #10853: the next-chunk link of a chunk header gains a second, self-describing encoding, {@code -(position + 1)},
   * which can name position 0 of the bucket - the one position the legacy encoding cannot, because its 0 means "no next
   * chunk" (#10701). This release adds the READ side only: every reader decodes both forms, every writer still stores
   * the legacy one, so a database written by it stays readable by the previous release.
   * <p>
   * No writer produces the new form yet, so each test fabricates it by rewriting the links of real chains in place, then
   * drives one reader of the link through its own entry point. Every one of them misreads a negative link without the
   * decoder: the walks stopped at it ({@code > 0}), or followed it to a negative page.
   */
  // #10853
  @Nested
  class NegativeNextChunkLink extends BucketPageLayoutTestSupport {

    @Test
    void bothEncodingsDecodeToTheSamePosition() {
      assertThat(LocalBucket.decodeNextChunkLink(0L)).as("0 still ends a chain").isEqualTo(LocalBucket.NO_NEXT_CHUNK);
      assertThat(LocalBucket.decodeNextChunkLink(1L)).as("a legacy link is the position itself").isEqualTo(1L);
      assertThat(LocalBucket.decodeNextChunkLink(3L * 2048 + 7)).isEqualTo(3L * 2048 + 7);

      assertThat(LocalBucket.nextChunkLinkOf(0L)).as("position 0 is representable in the new form").isEqualTo(-1L);
      for (final long position : new long[] { 0L, 1L, 2047L, 2048L, 123_456_789L, Long.MAX_VALUE - 1 })
        assertThat(LocalBucket.decodeNextChunkLink(LocalBucket.nextChunkLinkOf(position))).isEqualTo(position);

      assertThat(LocalBucket.decodeNextChunkLink(Long.MIN_VALUE)).as("no stored value decodes to the end-of-chain answer")
          .isEqualTo(Long.MAX_VALUE);
      assertThatThrownBy(() -> LocalBucket.nextChunkLinkOf(-1L)).isInstanceOf(IllegalArgumentException.class);
    }

    /** loadMultiPageRecord (point read and scan), assembleFromNewestCommitted and the check walk. */
    @Test
    void aChainLinkedInTheNewFormReadsBackUnchanged() {
      final RID[] rids = createChunkedRecords("Read");
      final LocalBucket bucket = bucketOf("Read");
      final byte[][] before = rawContents(bucket, rids);
      final long[] fingerprintsBefore = fingerprints(bucket, rids);

      for (final RID rid : rids)
        assertThat(relinkInNewForm(rid)).as("the fixture needs chains with continuation chunks").isNotEmpty();

      final byte[][] after = rawContents(bucket, rids);
      for (int i = 0; i < rids.length; i++) {
        assertThat(after[i]).as("point read of " + rids[i]).isEqualTo(before[i]);
        final int index = i;
        database.transaction(() -> assertThat(rids[index].asDocument(true).getString("payload")).isEqualTo(payload(index, 'x')));
        assertThat(bucket.getRecordFromCommittedPage(rids[i]).toByteArray()).as("committed-page read of " + rids[i])
            .isEqualTo(before[i]);
      }

      assertThat(countRecords("Read")).as("a scan reads every chain").isEqualTo(CHUNKED_RECORDS);
      database.transaction(() -> database.scanType("Read", false, record -> {
        final String payload = record.getString("payload");
        final int index = Integer.parseInt(payload.substring(1, payload.indexOf('-')));
        assertThat(payload).isEqualTo(payload(index, 'x'));
        return true;
      }));

      assertThat(fingerprints(bucket, rids)).as("the off-page fingerprint walks the same chain in either form")
          .isEqualTo(fingerprintsBefore);

      assertCleanCheck("Read");
    }

    /** updateMultiPageRecord (reuse, extend, shrink), freeChunkChain and collapseChunkChainToRecord. */
    @Test
    void aChainLinkedInTheNewFormIsUpdatedWithoutLeavingChunksBehind() {
      final RID[] rids = createChunkedRecords("Update");
      for (final RID rid : rids)
        relinkInNewForm(rid);

      // SAME SIZE: every chunk of the chain is reused through its link
      database.transaction(() -> {
        for (int i = 0; i < rids.length; i++)
          rids[i].asDocument(true).modify().set("payload", payload(i, 'y')).save();
      });
      assertPayloads(rids, i -> payload(i, 'y'));
      assertCleanCheck("Update");

      for (final RID rid : rids)
        relinkInNewForm(rid);

      // GROW, SHRINK (still a chain), COLLAPSE back into a plain record
      final String grown = "g".repeat(60_000);
      final String shrunk = "s".repeat(12_000);
      database.transaction(() -> rids[0].asDocument(true).modify().set("payload", grown).save());
      database.transaction(() -> rids[1].asDocument(true).modify().set("payload", shrunk).save());
      database.transaction(() -> rids[2].asDocument(true).modify().set("payload", "tiny").save());

      database.transaction(() -> {
        assertThat(rids[0].asDocument(true).getString("payload")).isEqualTo(grown);
        assertThat(rids[1].asDocument(true).getString("payload")).isEqualTo(shrunk);
        assertThat(rids[2].asDocument(true).getString("payload")).isEqualTo("tiny");
      });
      assertCleanCheck("Update");
    }

    /** deleteRecordInternal's chain walk. */
    @Test
    void aChainLinkedInTheNewFormIsDeletedWhole() {
      final RID[] rids = createChunkedRecords("Erase");
      for (final RID rid : rids)
        relinkInNewForm(rid);

      database.transaction(() -> {
        for (final RID rid : rids)
          rid.asDocument(true).delete();
      });

      assertThat(countRecords("Erase")).isZero();
      final Map<String, Object> stats = bucketStats("Erase");
      assertThat((Long) stats.get("totalChunks")).as("every continuation chunk is freed: " + stats).isZero();
      assertCleanCheck("Erase");
    }

    /** repairCrossLinkedChain, and the reachability marks of the check walk it starts from. */
    @Test
    void aCrossLinkInTheNewFormIsRepaired() {
      final RID[] rids = new RID[2];
      database.transaction(() -> {
        database.getSchema().createDocumentType("Cross", 1).createProperty("payload", Type.STRING);
        rids[0] = database.newDocument("Cross").set("payload", "a".repeat(150_000)).save().getIdentity();
        rids[1] = database.newDocument("Cross").set("payload", "b".repeat(150_000)).save().getIdentity();
      });
      final List<Long> ownerChunks = relinkInNewForm(rids[0]);
      relinkInNewForm(rids[1]);

      // The second head now links, in the new form, to the first record's first continuation chunk
      database.transaction(() -> onSlot(rids[1], page -> {
        page.writeLong(linkOffset(page, rids[1]), LocalBucket.nextChunkLinkOf(ownerChunks.getFirst()));
        return 0L;
      }));
      final String first = readPayload(rids[0]);
      final String second = readPayload(rids[1]);

      final Result fixed = checkDatabaseRow(true);
      assertThat(numberProperty(fixed, "crossLinkedChunks")).as(fixed.toJSON().toString()).isEqualTo(1L);
      assertThat(numberProperty(fixed, "crossLinkedChunksRepaired")).as(fixed.toJSON().toString()).isEqualTo(1L);

      assertThat(readPayload(rids[0])).isEqualTo(first);
      assertThat(readPayload(rids[1])).isEqualTo(second);
      final Result after = checkDatabaseRow(false);
      assertThat(numberProperty(after, "crossLinkedChunks")).as(after.toJSON().toString()).isZero();
      assertThat(numberProperty(after, "totalErrors")).as(after.toJSON().toString()).isZero();
      assertThat(numberProperty(after, "orphanedChunks")).as(after.toJSON().toString()).isZero();
    }

    /**
     * validateChainRead / isChunkStillTheOneRead: the link a moved page carries is compared with the next chunk's position
     * from the trace, so the comparison has to decode it. A REPEATABLE_READ transaction holding page 0 (pinned by the read
     * of a neighbour) reads a record whose head lives there after another record on the same page was rewritten: the
     * head's page moved, its chunk did not, and the read must be a revalidation rather than a change.
     */
    @Test
    void aRevalidatedChunkComparesTheDecodedLink() {
      final RID[] rids = createChunkedRecords("Revalidate");
      for (final RID rid : rids)
        relinkInNewForm(rid);
      final LocalBucket bucket = bucketOf("Revalidate");
      final PageManager pageManager = ((DatabaseInternal) database).getPageManager();
      final byte[] expected = rawContents(bucket, new RID[] { rids[0] })[0];

      database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
      try {
        bucket.getRecordInternal(rids[1], false);
        inAnotherThread(() -> database.transaction(
            () -> rids[2].asDocument(true).modify().set("payload", payload(2, 'y')).save()));

        final long revalidationsBefore = pageManager.getStats().chunkChainReadRevalidations;
        assertThat(bucket.getRecordInternal(rids[0], false).toByteArray()).isEqualTo(expected);
        assertThat(pageManager.getStats().chunkChainReadRevalidations - revalidationsBefore)
            .as("the moved head page is compared chunk by chunk, links included, and found unchanged").isPositive();
      } finally {
        database.rollback();
      }
      assertCleanCheck("Revalidate");
    }

    /**
     * What the encoding is for: a continuation chunk AT position 0. The last link of a chain is pointed, in the new form,
     * at a continuation chunk fabricated in slot 0 of page 0. The legacy form cannot express that link at all - its 0
     * ends the chain - while the new one reads it, frees it on delete, and counts it as reached.
     */
    @Test
    void aContinuationChunkAtPositionZeroIsReachable() {
      final RID[] zero = new RID[1];
      final RID[] record = new RID[1];
      database.transaction(() -> {
        database.getSchema().createDocumentType("Zero", 1).createProperty("v", Type.STRING);
        zero[0] = database.newDocument("Zero").set("v", "z".repeat(200)).save().getIdentity();
        // Larger than a 64 KB page, so it is written as a chunk chain from the start
        record[0] = database.newDocument("Zero").set("v", "x".repeat(150_000)).save().getIdentity();
      });
      assertThat(zero[0].getPosition()).as("the fixture needs a record at position 0").isZero();

      final LocalBucket bucket = bucketOf("Zero");
      final byte[] original = rawContents(bucket, record)[0];
      final List<Long> chunks = relinkInNewForm(record[0]);
      assertThat(chunks).isNotEmpty().doesNotContain(0L);

      // Slot 0 becomes the chain's new last chunk: [NEXT_CHUNK][int size][long link = end][content]
      final byte[] tail = new byte[64];
      Arrays.fill(tail, (byte) 't');
      database.transaction(() -> {
        onSlot(zero[0], page -> {
          int offset = recordOffsetOf(page, zero[0]);
          offset += page.writeNumber(offset, LocalBucket.NEXT_CHUNK);
          page.writeInt(offset, tail.length);
          page.writeLong(offset + Binary.INT_SERIALIZED_SIZE, 0L);
          page.writeByteArray(offset + Binary.INT_SERIALIZED_SIZE + Binary.LONG_SERIALIZED_SIZE, tail);
          return 0L;
        });
        final RID last = new RID(record[0].getBucketId(), chunks.getLast());
        onSlot(last, page -> {
          page.writeLong(linkOffset(page, last), LocalBucket.nextChunkLinkOf(0L));
          return 0L;
        });
      });

      final byte[] expected = Arrays.copyOf(original, original.length + tail.length);
      System.arraycopy(tail, 0, expected, original.length, tail.length);
      assertThat(rawContents(bucket, record)[0]).as("the chain runs through position 0").isEqualTo(expected);

      final Map<String, Object> stats = bucketStats("Zero");
      assertThat((Long) stats.get("orphanedChunks")).as("the chunk at position 0 is reached: " + stats).isZero();

      database.transaction(() -> bucket.deleteRecord(record[0]));
      database.transaction(() -> assertThatThrownBy(() -> bucket.getRecordInternal(zero[0], false))
          .as("the delete freed the chunk at position 0 with the rest of the chain").isInstanceOf(RecordNotFoundException.class));
      final Map<String, Object> after = bucketStats("Zero");
      assertThat((Long) after.get("totalChunks")).as(after.toString()).isZero();
      assertThat((Long) after.get("orphanedChunks")).as(after.toString()).isZero();

      // The fabricated chunk took the slot of a document, and the delete went through the bucket, both behind the record
      // counter's back: the stale counter is the one thing left to repair, and nothing else is found.
      final Result fixed = checkDatabaseRow(true);
      assertThat(numberProperty(fixed, "staleRecordCountersFixed")).as(fixed.toJSON().toString()).isEqualTo(1L);
      assertThat(numberProperty(fixed, "totalErrors")).as(fixed.toJSON().toString()).isEqualTo(1L);
      final Result clean = checkDatabaseRow(false);
      assertThat(numberProperty(clean, "totalErrors")).as(clean.toJSON().toString()).isZero();
    }

    /**
     * Rewrites every link of the chain headed at {@code head} into the new form, in one committed transaction.
     *
     * @return the positions of the continuation chunks, in chain order.
     */
    private List<Long> relinkInNewForm(final RID head) {
      final List<Long> chunks = new ArrayList<>();
      database.transaction(() -> {
        RID current = head;
        while (true) {
          final RID at = current;
          final boolean isHead = at.equals(head);
          final long position = onSlot(at, page -> {
            final long marker = page.readNumberAndSize(recordOffsetOf(page, at))[0];
            assertThat(marker).as(at + " must be a chunk of the chain, not a plain record")
                .isEqualTo(isHead ? LocalBucket.FIRST_CHUNK : LocalBucket.NEXT_CHUNK);
            final int offset = linkOffset(page, at);
            final long link = page.readLong(offset);
            if (link > 0L)
              page.writeLong(offset, LocalBucket.nextChunkLinkOf(link));
            return LocalBucket.decodeNextChunkLink(link);
          });
          if (position == LocalBucket.NO_NEXT_CHUNK)
            return;
          chunks.add(position);
          current = new RID(head.getBucketId(), position);
        }
      });
      return chunks;
    }

    /** Offset of the next-chunk link of the chunk in {@code rid}'s slot: [marker][int chunkSize][long link]. */
    private int linkOffset(final MutablePage page, final RID rid) {
      final int offset = recordOffsetOf(page, rid);
      return (int) (offset + page.readNumberAndSize(offset)[1]) + Binary.INT_SERIALIZED_SIZE;
    }

    private byte[][] rawContents(final LocalBucket bucket, final RID[] rids) {
      final byte[][] contents = new byte[rids.length][];
      database.transaction(() -> {
        for (int i = 0; i < rids.length; i++)
          contents[i] = bucket.getRecordInternal(rids[i], false).toByteArray();
      });
      return contents;
    }

    private long[] fingerprints(final LocalBucket bucket, final RID[] rids) {
      final long[] fingerprints = new long[rids.length];
      database.transaction(() -> {
        for (int i = 0; i < rids.length; i++) {
          final RID rid = rids[i];
          fingerprints[i] = onSlot(rid, page -> bucket.offPageContentFingerprint(rid, page, true));
        }
      });
      return fingerprints;
    }

    private void assertPayloads(final RID[] rids, final IntFunction<String> expected) {
      database.transaction(() -> {
        for (int i = 0; i < rids.length; i++)
          assertThat(rids[i].asDocument(true).getString("payload")).isEqualTo(expected.apply(i));
      });
    }

    private String readPayload(final RID rid) {
      final String[] payload = new String[1];
      database.transaction(() -> payload[0] = rid.asDocument(true).getString("payload"));
      return payload[0];
    }

    /** No error, no repair and no orphaned chunk: every chain was walked to its end and every chunk was reached. */
    private void assertCleanCheck(final String typeName) {
      final Map<String, Object> stats = bucketStats(typeName);
      assertThat((Long) stats.get("totalErrors")).as(stats.toString()).isZero();
      assertThat((Long) stats.get("orphanedChunks")).as(stats.toString()).isZero();
      checkDatabase();
    }
  }

  /**
   * Regression test for #10875: {@code LocalBucket.freeChunkChain} zeroed the slot-table entry of the slot a chain
   * pointer reached BEFORE it checked that the slot held a continuation chunk of the chain. When the check failed the
   * slot was already freed, so a slot a concurrent insert had reused inside the window between the read of the chain
   * and the free of its tail lost its record - gone from {@code scan()}, {@code count()} and {@code lookupByRID}, in a
   * transaction that committed normally.
   * <p>
   * The concurrent window is not reproducible deterministically, so the walk is handed the state it would find there
   * directly: a chain pointer that reaches a slot holding an unrelated live record, and one that reaches a free slot.
   */
  // #10875
  @Nested
  class FreeChunkChainValidatesBeforeFreeing extends TestHelper {

    private static final String TYPE = "Issue10875";

    @Test
    void aPointerReachingALiveRecordLeavesThatRecordInPlace() throws Exception {
      database.getSchema().createDocumentType(TYPE);

      final RID[] rids = new RID[2];
      database.transaction(() -> {
        rids[0] = database.newDocument(TYPE).set("tag", "SHRINKING").save().getIdentity();
        rids[1] = database.newDocument(TYPE).set("tag", "VICTIM").save().getIdentity();
      });
      final RID shrinking = rids[0];
      final RID victim = rids[1];
      final LocalBucket bucket = (LocalBucket) database.getSchema().getBucketById(victim.getBucketId());

      // A RID position IS the encoding a next-chunk pointer uses: pageNumber * maxRecordsInPage + slot.
      database.transaction(() -> freeChunkChain(bucket, shrinking, victim.getPosition()));

      assertThat(database.lookupByRID(victim, true).asDocument().getString("tag")).isEqualTo("VICTIM");
      assertThat(database.countBucket(bucket.getName())).isEqualTo(2L);
      assertThat(database.query("sql", "select count() as c from " + TYPE).next().<Long>getProperty("c")).isEqualTo(2L);

      // The victim is still a plain record that can be updated and read back.
      database.transaction(() -> {
        final MutableDocument doc = database.lookupByRID(victim, true).asDocument().modify();
        doc.set("tag", "VICTIM-UPDATED").save();
      });
      assertThat(database.lookupByRID(victim, true).asDocument().getString("tag")).isEqualTo("VICTIM-UPDATED");
    }

    @Test
    void aPointerReachingAFreeSlotDoesNotFailTheTransaction() throws Exception {
      database.getSchema().createDocumentType(TYPE);

      final RID[] rids = new RID[3];
      database.transaction(() -> {
        rids[0] = database.newDocument(TYPE).set("tag", "SHRINKING").save().getIdentity();
        rids[1] = database.newDocument(TYPE).set("tag", "DELETED").save().getIdentity();
        rids[2] = database.newDocument(TYPE).set("tag", "NEIGHBOUR").save().getIdentity();
      });
      database.transaction(() -> database.lookupByRID(rids[1], true).delete());
      final LocalBucket bucket = (LocalBucket) database.getSchema().getBucketById(rids[1].getBucketId());

      database.transaction(() -> freeChunkChain(bucket, rids[0], rids[1].getPosition()));

      assertThat(database.countBucket(bucket.getName())).isEqualTo(2L);
      assertThat(database.lookupByRID(rids[0], true).asDocument().getString("tag")).isEqualTo("SHRINKING");
      assertThat(database.lookupByRID(rids[2], true).asDocument().getString("tag")).isEqualTo("NEIGHBOUR");
    }

    private static void freeChunkChain(final LocalBucket bucket, final RID originalRID, final long chunkPointer) {
      try {
        final Method method = LocalBucket.class.getDeclaredMethod("freeChunkChain", RID.class, long.class);
        method.setAccessible(true);
        method.invoke(bucket, originalRID, chunkPointer);
      } catch (final ReflectiveOperationException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Issue #10949: step 2 of the rollout of the self-describing next-chunk link {@code -(position + 1)} (#10948). A
   * database-scoped setting, OFF by default, makes the link writers store the new form instead of the absolute position.
   * Every reader decodes both forms since #10853, so a chain written with the setting ON must read, update, delete and
   * check clean, and a chain holding both forms must behave the same.
   */
  // #10949
  @Nested
  class SelfDescribingNextChunkLinkWriter extends BucketPageLayoutTestSupport {

    @Test
    void theSettingIsOffByDefault() {
      assertThat(GlobalConfiguration.BUCKET_SELF_DESCRIBING_CHUNK_LINKS.getDefValue()).isEqualTo(false);
      assertThat(GlobalConfiguration.BUCKET_SELF_DESCRIBING_CHUNK_LINKS.getScope()).isEqualTo(GlobalConfiguration.SCOPE.DATABASE);
    }

    @Test
    void withTheSettingOffEveryLinkIsTheLegacyAbsolutePosition() {
      final RID[] rids = createChunkedRecords("Off");
      for (final RID rid : rids) {
        final List<Long> links = storedLinksOf(rid);
        assertThat(links).as("the fixture needs continuation chunks").isNotEmpty();
        assertThat(links).as("legacy links of " + rid).allMatch(link -> link > 0L);
      }
      assertCleanCheck("Off");
    }

    @Test
    void withTheSettingOnEveryNewLinkIsSelfDescribingAndTheChainRoundTrips() {
      enableSelfDescribingLinks(true);
      final RID[] rids = createChunkedRecords("On");

      for (final RID rid : rids) {
        final List<Long> links = storedLinksOf(rid);
        assertThat(links).as("the fixture needs continuation chunks").isNotEmpty();
        assertThat(links).as("self-describing links of " + rid).allMatch(link -> link < 0L);
      }
      assertPayloads(rids, 'x');
      assertCleanCheck("On");

      // GROW (extends the chain), SHRINK (still a chain), COLLAPSE (back into a plain record), then a same-size rewrite
      final String grown = "g".repeat(200_000);
      final String shrunk = "s".repeat(12_000);
      database.transaction(() -> rids[0].asDocument(true).modify().set("payload", grown).save());
      database.transaction(() -> rids[1].asDocument(true).modify().set("payload", shrunk).save());
      database.transaction(() -> rids[2].asDocument(true).modify().set("payload", "tiny").save());
      database.transaction(() -> {
        assertThat(rids[0].asDocument(true).getString("payload")).isEqualTo(grown);
        assertThat(rids[1].asDocument(true).getString("payload")).isEqualTo(shrunk);
        assertThat(rids[2].asDocument(true).getString("payload")).isEqualTo("tiny");
      });
      assertThat(storedLinksOf(rids[0])).as("the extension is linked in the new form").isNotEmpty().allMatch(link -> link < 0L);
      assertCleanCheck("On");

      // DELETE: every chunk, reached through the new links, is freed
      database.transaction(() -> {
        for (int i = 3; i < rids.length; i++)
          rids[i].asDocument(true).delete();
      });
      database.transaction(() -> rids[0].asDocument(true).delete());
      assertCleanCheck("On");
    }

    @Test
    void aRecordCreatedAsAChainWithTheSettingOnIsLinkedInTheNewForm() {
      enableSelfDescribingLinks(true);
      final RID[] record = new RID[1];
      database.transaction(() -> {
        database.getSchema().createDocumentType("Created", 1).createProperty("v", Type.STRING);
        record[0] = database.newDocument("Created").set("v", "x".repeat(150_000)).save().getIdentity();
      });

      assertThat(storedLinksOf(record[0])).isNotEmpty().allMatch(link -> link < 0L);
      database.transaction(() -> assertThat(record[0].asDocument(true).getString("v")).isEqualTo("x".repeat(150_000)));
      assertCleanCheck("Created");
    }

    @Test
    void aChainHoldingBothFormsBehavesLikeAnyOther() {
      final RID[] rids = createChunkedRecords("Mixed");
      final int legacyLinks = storedLinksOf(rids[0]).size();

      // Flip the setting on a database that already holds legacy chains, then extend one: new tail, old head
      enableSelfDescribingLinks(true);
      final String grown = "g".repeat(200_000);
      database.transaction(() -> rids[0].asDocument(true).modify().set("payload", grown).save());

      final List<Long> links = storedLinksOf(rids[0]);
      assertThat(links.size()).isGreaterThan(legacyLinks);
      assertThat(links).as("the links the extension wrote are new, the ones it kept are legacy").anyMatch(link -> link > 0L)
          .anyMatch(link -> link < 0L);

      database.transaction(() -> assertThat(rids[0].asDocument(true).getString("payload")).isEqualTo(grown));
      assertCleanCheck("Mixed");

      // Back to OFF: a rewrite of the mixed chain still reads and checks clean, then deletes whole
      enableSelfDescribingLinks(false);
      final String regrown = "h".repeat(250_000);
      database.transaction(() -> rids[0].asDocument(true).modify().set("payload", regrown).save());
      database.transaction(() -> assertThat(rids[0].asDocument(true).getString("payload")).isEqualTo(regrown));
      assertCleanCheck("Mixed");
      database.transaction(() -> rids[0].asDocument(true).delete());
      assertCleanCheck("Mixed");
    }

    @Test
    void position0IsLinkableOnlyInTheNewForm() {
      // OFF: the #10701 refusal stays
      assertThatThrownBy(() -> LocalBucket.nextChunkLink(null, "b", 0, 0, 2048, false)).isInstanceOf(DatabaseOperationException.class);
      assertThat(LocalBucket.nextChunkLink(null, "b", 3, 7, 2048, false)).isEqualTo(3L * 2048 + 7);

      // ON: position 0 can be named, every other position decodes back to itself
      assertThat(LocalBucket.nextChunkLink(null, "b", 0, 0, 2048, true)).isEqualTo(-1L);
      assertThat(LocalBucket.decodeNextChunkLink(LocalBucket.nextChunkLink(null, "b", 0, 0, 2048, true))).isZero();
      assertThat(LocalBucket.decodeNextChunkLink(LocalBucket.nextChunkLink(null, "b", 3, 7, 2048, true))).isEqualTo(3L * 2048 + 7);
    }

    private void enableSelfDescribingLinks(final boolean enabled) {
      ((DatabaseInternal) database).getConfiguration().setValue(GlobalConfiguration.BUCKET_SELF_DESCRIBING_CHUNK_LINKS, enabled);
    }

    /** The next-chunk link fields of the chain headed at {@code head}, as stored, head chunk first, end of chain excluded. */
    private List<Long> storedLinksOf(final RID head) {
      final List<Long> links = new ArrayList<>();
      database.transaction(() -> {
        RID current = head;
        while (true) {
          final RID at = current;
          final long link = onSlot(at, page -> {
            final int offset = recordOffsetOf(page, at);
            return page.readLong((int) (offset + page.readNumberAndSize(offset)[1]) + Binary.INT_SERIALIZED_SIZE);
          });
          if (link == 0L)
            return;
          links.add(link);
          current = new RID(head.getBucketId(), LocalBucket.decodeNextChunkLink(link));
        }
      });
      return links;
    }

    private void assertPayloads(final RID[] rids, final char filler) {
      database.transaction(() -> {
        for (int i = 0; i < rids.length; i++)
          assertThat(rids[i].asDocument(true).getString("payload")).isEqualTo(payload(i, filler));
      });
    }

    private void assertCleanCheck(final String typeName) {
      assertThat((Long) bucketStats(typeName).get("totalErrors")).isZero();
      assertThat((Long) bucketStats(typeName).get("orphanedChunks")).isZero();
      checkDatabase();
    }
  }

  /**
   * Issue #6282: the three places the corruption-is-not-contention distinction #6258 drew on the read path was missing
   * from, plus the hook that makes the highest-consequence of them testable at all.
   * <ol>
   * <li><b>The delete path</b> positively identified a structurally broken chunk chain and then reported it as a
   * {@link ConcurrentModificationException} - the #4932 retry signal - costing the caller a full round of transactions
   * before it gave up, and forcing five call sites to walk the chain a SECOND time just to find out which of the two
   * they were holding.</li>
   * <li><b>{@code isChunkChainBroken}</b>, the probe those call sites used, walked the chain through the CALLER's
   * transaction. Under {@code REPEATABLE_READ} that is a pinned - possibly stale - snapshot, and the answer gates
   * FORCE-DELETE, so a false positive removes a healthy record.</li>
   * <li><b>{@code IOException} meant two opposite things</b> within a few lines of the same walk: "the disk failed,
   * prove nothing" at the page load, and "this record is corrupt, report a break" from the slot-offset check. The only
   * thing keeping them apart was where the {@code try} sat.</li>
   * <li><b>No fault-injection hook</b> for page reads, so the path whose wrong answer is a permanent corruption verdict
   * on a healthy record had no coverage.</li>
   * </ol>
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  // #6282
  @Nested
  class BrokenChainDeleteAndProbe extends TestHelper {

    private static final String TYPE = "Chunked";

    /** The healthy continuation pointer {@link #createBrokenMultiPageVertex()} overwrote. */
    private long originalChunkPointer;

    @Override
    protected boolean isCheckingDatabaseIntegrity() {
      // Every fixture here injects on-disk corruption on purpose, which the post-test integrity check would flag.
      return false;
    }

    @AfterEach
    void clearFaultInjection() {
      PageManager.setPageReadFaultInjector(null);
    }

    /**
     * Item 1. The delete path has already established, from the bytes, that the continuation pointer leads nowhere.
     * Saying "modified concurrently. Please retry" about it is wrong twice over: the retry cannot succeed, and the
     * exception is a {@link NeedRetryException}, so the machinery above re-runs the whole transaction for it.
     */
    @Test
    void aBrokenChunkChainIsNamedByTheDeletePathInsteadOfBeingCalledContention() {
      final RID broken = createBrokenMultiPageVertex();
      final LocalBucket bucket = (LocalBucket) database.getSchema().getBucketById(broken.getBucketId());

      database.begin();
      try {
        assertThatThrownBy(() -> bucket.deleteRecord(broken))
            .as("a chain the delete path itself proved broken is corruption, not contention")
            .isInstanceOf(BrokenChunkChainException.class)
            .hasMessageContaining("broken chunk chain")
            .hasMessageContaining("CHECK DATABASE FIX")
            // THE POINT OF THE TYPE, NOT MERELY OF THE MESSAGE: NOTHING ABOVE MAY SPEND A RETRY BUDGET ON IT
            .isNotInstanceOf(NeedRetryException.class);
      } finally {
        database.rollback();
      }

      assertThat(bucket.existsRecord(broken)).as("a refused delete must leave the record where it was").isTrue();

      // THE PAIRED HALF: THE RECORD IS STILL REMOVABLE, SO THE NEW VERDICT DID NOT MAKE IT UNDELETABLE
      database.transaction(() -> {
        TestHelper.deleteRecordAtLowLevel(database, broken, true);
      });
      assertThat(bucket.existsRecord(broken)).isFalse();
    }

    /**
     * Item 1, through the public API: the arm #6258 deliberately left out of {@code LocalDatabase}'s plain-record
     * branch, with a comment saying it would be dead code because {@code deleteRecordInternal} never named a broken
     * chain as one, is live now. The record is deletable through it WITHOUT the second structural walk that branch
     * used to need.
     */
    @Test
    void theTolerantDeletePathTakesTheLoaderVerdictOnTheDeleteSideToo() {
      final RID broken = createBrokenMultiPageVertex();
      database.getConfiguration().setValue(GlobalConfiguration.DELETE_TOLERATE_BROKEN_CHAIN, true);

      database.transaction(() -> database.command("sql", "DELETE FROM " + broken));

      assertThat(database.getSchema().getBucketById(broken.getBucketId()).existsRecord(broken)).isFalse();
    }

    /**
     * Item 1, the other direction and the one that keeps the change honest: a chain the committed image does NOT agree
     * is broken must keep the retryable exception. Here the record is healthy and the delete is made to fail on a page
     * it cannot resolve - it must not be promoted to a permanent corruption verdict.
     */
    @Test
    void aBreakTheCommittedImageDoesNotConfirmKeepsTheRetrySignal() {
      final RID healthy = createMultiPageVertex();
      final LocalBucket bucket = (LocalBucket) database.getSchema().getBucketById(healthy.getBucketId());

      // The delete's own walk reads its chunk pages with getPageToModify; the CONFIRMATION reads the head page from
      // the page manager. Failing every read after the delete's own ones leaves the confirmation unable to prove
      // anything, which must fall back to the retry rather than condemn a healthy record.
      database.begin();
      try {
        final AtomicInteger reads = new AtomicInteger();
        PageManager.setPageReadFaultInjector(pageId -> {
          if (pageId.getFileId() == healthy.getBucketId() && reads.incrementAndGet() > 1)
            throw new IOException("injected fault on " + pageId);
        });

        assertThatThrownBy(() -> bucket.deleteRecord(healthy))
            .as("a delete that could not read its chain must never end in a permanent corruption verdict")
            .isNotInstanceOf(BrokenChunkChainException.class);
      } finally {
        PageManager.setPageReadFaultInjector(null);
        database.rollback();
      }

      assertThat(bucket.existsRecord(healthy)).as("and the healthy record is still there").isTrue();
    }

    /**
     * Item 2. The probe used to walk the chain through the caller's transaction, so under {@code REPEATABLE_READ} it
     * answered from whatever that transaction had PINNED. Here the transaction pins a page whose chain is broken, a
     * concurrent commit repairs it, and the probe has to report the committed truth - because every caller escalates a
     * {@code true} to a force-delete, and {@code check(fix)} deletes the record it flags.
     */
    @Test
    void theStructuralProbeJudgesFromTheCommittedImageAndNotFromThePinnedOne() throws Exception {
      final RID broken = createBrokenMultiPageVertex();
      final long originalPointer = originalChunkPointer;
      final DatabaseInternal db = (DatabaseInternal) database;
      final LocalBucket bucket = (LocalBucket) db.getSchema().getBucketById(broken.getBucketId());
      final int pageSize = ((PaginatedComponentFile) db.getFileManager().getFile(broken.getBucketId())).getPageSize();

      db.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
      try {
        // PIN THE BROKEN IMAGE OF THE HEAD PAGE IN THIS TRANSACTION'S SNAPSHOT
        db.getTransaction().getPage(new PageId(db, broken.getBucketId(), 0), pageSize);

        assertThat(bucket.isChunkChainBroken(broken))
            .as("precondition: the chain really is broken in the committed image right now").isTrue();

        // ANOTHER THREAD REPAIRS THE POINTER AND COMMITS. THIS TRANSACTION KEEPS ITS PINNED, STILL-BROKEN PAGE
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final Thread repairer = new Thread(() -> {
          try {
            db.transaction(() -> writeFirstChunkPointer(broken.getBucketId(), originalPointer));
          } catch (final Throwable e) {
            failure.compareAndSet(null, e);
          }
        }, "issue6282-repairer");
        repairer.start();
        repairer.join(30_000);
        assertThat(failure.get()).isNull();

        assertThat(bucket.isChunkChainBroken(broken))
            .as("the probe must answer from the newest committed image: this transaction's pinned page still says "
                + "broken, and a true here would force-delete a record that is healthy on disk")
            .isFalse();
      } finally {
        db.rollback();
      }
    }

    /**
     * Item 3. A slot-table entry pointing inside the page header is CORRUPTION - the page read fine, its bytes are
     * nonsense - and it used to leave the delete path as a bare {@link IOException}, indistinguishable to the caller
     * from a disk that had just failed. It is now the same structural verdict as a pointer that leads off the end of
     * the file, which is what it has always been.
     */
    @Test
    void aCorruptChunkSlotIsCorruptionAndNotAnIoError() {
      final RID rid = createMultiPageVertex();
      corruptFirstContinuationSlot(rid.getBucketId());
      reopenDatabase();

      final LocalBucket bucket = (LocalBucket) database.getSchema().getBucketById(rid.getBucketId());

      assertThat(bucket.isChunkChainBroken(rid))
          .as("the version-blind walk has to read a corrupt slot offset as a break").isTrue();

      database.begin();
      try {
        assertThatThrownBy(() -> bucket.deleteRecord(rid))
            .as("and the delete path must name it rather than let a raw IOException out")
            .isInstanceOf(BrokenChunkChainException.class)
            .isNotInstanceOf(NeedRetryException.class);
      } finally {
        database.rollback();
      }

      // THE PAIRED HALF: THE RECORD IS STILL REMOVABLE WITH FORCE
      database.transaction(() -> {
        TestHelper.deleteRecordAtLowLevel(database, rid, true);
      });
      assertThat(bucket.existsRecord(rid)).isFalse();
    }

    /**
     * Item 4, and the reason it was filed first: the confirmation walk answering "the chain is broken" for a page it
     * merely could not READ condemns a healthy record permanently and points the operator at
     * {@code CHECK DATABASE FIX}, which deletes it. There was no way to make a page read fail on demand, so this path
     * had no coverage at all.
     */
    @Test
    void aPageReadFaultIsNeverALicenceToForceDeleteARecord() {
      final RID broken = createBrokenMultiPageVertex();
      final LocalBucket bucket = (LocalBucket) database.getSchema().getBucketById(broken.getBucketId());

      database.transaction(() -> {
        assertThat(bucket.isChunkChainBroken(broken))
            .as("precondition: with a healthy disk the probe does prove the break, so the assertion below is not "
                + "passing for the wrong reason").isTrue();

        PageManager.setPageReadFaultInjector(pageId -> {
          if (pageId.getFileId() == broken.getBucketId())
            throw new IOException("injected fault on " + pageId);
        });

        assertThat(bucket.isChunkChainBroken(broken))
            .as("a probe that could not read the page has proved nothing, and every caller reads a true here as "
                + "permission to force-delete").isFalse();
      });
    }

    /**
     * A chunk that does not describe itself legally is corruption too, and the walk used to follow its continuation
     * pointer straight past it: a size running off the end of the page reached the end of a chain of nonsense and was
     * reported CLEAN. {@code loadMultiPageRecord} could then never confirm the break the read had just hit, so the
     * record spent its whole retry budget and came back as a concurrency problem that did not exist (PR review).
     * <p>
     * Three sizes, because the bound has two ways to be wrong: a large one that stays in range of an int, the largest
     * representable one - which overflows {@code offset + header + size} to a NEGATIVE number and so passes any
     * {@code >} test written in int arithmetic - and a negative one.
     */
    @ParameterizedTest(name = "declared chunk size {0}")
    @ValueSource(ints = { 1_073_741_823, Integer.MAX_VALUE, -1 })
    void aChunkSizeThatRunsOffThePageIsABreakRatherThanACleanWalk(final int corruptSize) {
      final RID rid = createMultiPageVertex();
      corruptFirstChunkSize(rid.getBucketId(), corruptSize);

      final LocalBucket bucket = (LocalBucket) database.getSchema().getBucketById(rid.getBucketId());

      assertThat(bucket.isChunkChainBroken(rid))
          .as("a chunk whose declared size leaves the page cannot be walked past as if the chain were sound").isTrue();

      database.transaction(() -> assertThatThrownBy(() -> database.lookupByRID(rid, true).asVertex().toJSON())
          .as("and the read must confirm it rather than spend TX_RETRIES calling it contention")
          .isInstanceOf(BrokenChunkChainException.class)
          .isNotInstanceOf(NeedRetryException.class));
    }

    /**
     * The delete-side counterpart of the test above, and the reason the size check lives only on the walk that needs
     * it (PR review round 4 asked for the scope to be explicit rather than implicit).
     * <p>
     * {@code deleteRecordInternal} walks the chain by POINTERS and MARKERS and never reads a chunk's declared size -
     * and it does not need to, because the size field sits beside the continuation pointer rather than in front of it.
     * A chain whose only corruption is a bad size therefore still walks perfectly: every chunk is found, every slot is
     * freed, and the delete simply succeeds. There is nothing here to name and nothing to retry.
     * <p>
     * The combined shape - a bad size AND a bad pointer - is the one where the two walks disagree about which hop
     * failed, and there the confirmation deliberately declines to match and the delete keeps its retryable exception.
     * That fails SAFE: {@code CHECK DATABASE FIX} force-deletes on its own detection (which does read sizes), so no
     * record is ever left permanently stuck.
     */
    @Test
    void aBadChunkSizeAloneStillDeletesBecauseTheChainItselfIsIntact() {
      final RID rid = createMultiPageVertex();
      corruptFirstChunkSize(rid.getBucketId(), Integer.MAX_VALUE);

      final LocalBucket bucket = (LocalBucket) database.getSchema().getBucketById(rid.getBucketId());
      assertThat(bucket.isChunkChainBroken(rid))
          .as("precondition: the READ path does see this as corruption").isTrue();

      // NO force, NO opt-in: the pointers are all sound, so the physical free walks the whole chain and completes
      database.transaction(() -> TestHelper.deleteRecordAtLowLevel(database, rid));

      assertThat(bucket.existsRecord(rid))
          .as("a chain whose only fault is a declared size is still fully walkable, so it deletes like any other")
          .isFalse();
    }

    /** The hook is absent unless a test installs it, which is what makes it free in production. */
    @Test
    void theFaultInjectorIsAbsentByDefault() {
      assertThat(PageManager.getPageReadFaultInjector()).isNull();
    }

    // ------------------------------------------------------------------------------------------------------ HELPERS

    /** A vertex whose payload spans several pages, so it is stored as a FIRST_CHUNK with a real continuation chain. */
    private RID createMultiPageVertex() {
      final DatabaseInternal db = (DatabaseInternal) database;

      final int[] bucketIdHolder = new int[1];
      db.transaction(() -> {
        final VertexType type = db.getSchema().createVertexType(TYPE, 1);
        type.createProperty("id", Type.INTEGER);
        type.createProperty("data", Type.STRING);
        bucketIdHolder[0] = type.getBuckets(false).getFirst().getFileId();
      });

      final int pageSize = ((PaginatedComponentFile) db.getFileManager().getFile(bucketIdHolder[0])).getPageSize();
      final String bigData = "x".repeat(pageSize * 4);

      final RID[] rids = new RID[1];
      db.transaction(() -> {
        // FIRST, SO IT LANDS AT PAGE 0 / SLOT 0
        rids[0] = db.newVertex(TYPE).set("id", 0).set("data", bigData).save().getIdentity();
        db.newVertex(TYPE).set("id", 1).set("data", "small").save();
      });
      assertThat(rids[0].getPosition()).isZero();

      db.getPageManager().waitAllPagesOfDatabaseAreFlushed(db);
      return rids[0];
    }

    /**
     * The same record, with the FIRST_CHUNK's continuation pointer aimed at a page well beyond the file, and the
     * database reopened so the chain is walked from the corrupted page rather than from a cached one.
     */
    private RID createBrokenMultiPageVertex() {
      final RID rid = createMultiPageVertex();
      final int maxRecordsInPage = ((LocalBucket) database.getSchema().getBucketById(rid.getBucketId())).getMaxRecordsInPage();
      // KEPT SO A TEST CAN PUT THE CHAIN BACK TOGETHER AGAIN, WHICH IS HOW THE PINNED-VIEW GAP IS PROVOKED
      originalChunkPointer = readFirstChunkPointer(rid.getBucketId());
      assertThat(originalChunkPointer).as("the fixture record must really have a continuation chunk").isPositive();
      writeFirstChunkPointer(rid.getBucketId(), 1_000_000L * maxRecordsInPage);
      reopenDatabase();
      return rid;
    }

    private long readFirstChunkPointer(final int bucketId) {
      final DatabaseInternal db = (DatabaseInternal) database;
      final int pageSize = ((PaginatedComponentFile) db.getFileManager().getFile(bucketId)).getPageSize();
      final long[] holder = new long[1];
      db.transaction(() -> {
        try {
          final BasePage page = db.getTransaction().getPage(new PageId(db, bucketId, 0), pageSize);
          final int recordOffset = firstChunkOffset(page);
          holder[0] = page.readLong(recordOffset + 1 + Binary.INT_SERIALIZED_SIZE);
        } catch (final IOException e) {
          throw new RuntimeException(e);
        }
      });
      return holder[0];
    }

    /** Overwrites the continuation pointer of the FIRST_CHUNK at page 0 / slot 0. */
    private void writeFirstChunkPointer(final int bucketId, final long pointer) {
      final DatabaseInternal db = (DatabaseInternal) database;
      final int pageSize = ((PaginatedComponentFile) db.getFileManager().getFile(bucketId)).getPageSize();
      db.transaction(() -> {
        try {
          final MutablePage page = db.getTransaction().getPageToModify(new PageId(db, bucketId, 0), pageSize, false);
          page.writeLong(firstChunkOffset(page) + 1 + Binary.INT_SERIALIZED_SIZE, pointer);
        } catch (final IOException e) {
          throw new RuntimeException(e);
        }
      });
    }

    /**
     * Points the slot-table entry of the record's FIRST continuation chunk inside the page header, which no writer can
     * produce: the page reads fine and its bytes are nonsense. That is what {@code PageCorruptionException} names.
     */
    private void corruptFirstContinuationSlot(final int bucketId) {
      final DatabaseInternal db = (DatabaseInternal) database;
      final int pageSize = ((PaginatedComponentFile) db.getFileManager().getFile(bucketId)).getPageSize();
      final int maxRecordsInPage = ((LocalBucket) db.getSchema().getBucketById(bucketId)).getMaxRecordsInPage();
      final long pointer = readFirstChunkPointer(bucketId);
      assertThat(pointer).as("the fixture record must really have a continuation chunk").isPositive();

      final int chunkPageNumber = (int) (pointer / maxRecordsInPage);
      final int chunkSlot = (int) (pointer % maxRecordsInPage);

      db.transaction(() -> {
        try {
          final MutablePage page = db.getTransaction()
              .getPageToModify(new PageId(db, bucketId, chunkPageNumber), pageSize, false);
          // ANY NON-ZERO VALUE BELOW contentHeaderSize IS ILLEGAL; 1 IS BELOW EVERY POSSIBLE ONE
          page.writeInt(LocalBucket.PAGE_RECORD_TABLE_OFFSET + chunkSlot * Binary.INT_SERIALIZED_SIZE, 1);
        } catch (final IOException e) {
          throw new RuntimeException(e);
        }
      });
      db.getPageManager().waitAllPagesOfDatabaseAreFlushed(db);
    }

    /**
     * Declares a head-chunk size far larger than the page can hold - the shape a torn or mis-written header leaves.
     * <p>
     * Written straight into the CLOSED file rather than through a transaction: a commit compresses the pages it
     * touched, and that pass has a size guard of its own which deletes the record before the walk under test ever
     * sees it. Corrupting the bytes on disk is also the more honest fixture for the fault this is about. The database
     * is left closed; the caller reopens it.
     *
     * @param corruptSize the size to declare. {@code Integer.MAX_VALUE} is the case that matters most: it makes
     *                    {@code offset + header + size} overflow an int to a NEGATIVE number, so a bounds check
     *                    written in int arithmetic lets the largest corrupt size representable straight through the
     *                    guard meant to stop it (PR review).
     */
    private void corruptFirstChunkSize(final int bucketId, final int corruptSize) {
      final DatabaseInternal db = (DatabaseInternal) database;
      final PaginatedComponentFile file = (PaginatedComponentFile) db.getFileManager().getFile(bucketId);
      final int pageSize = file.getPageSize();
      final String path = file.getFilePath();

      final int[] chunkOffset = new int[1];
      db.transaction(() -> {
        try {
          chunkOffset[0] = firstChunkOffset(db.getTransaction().getPage(new PageId(db, bucketId, 0), pageSize));
        } catch (final IOException e) {
          throw new RuntimeException(e);
        }
      });

      database.close();
      try (final RandomAccessFile raw = new RandomAccessFile(path, "rw")) {
        // Page 0, content-relative offset: the marker is one byte, then the declared chunk size.
        raw.seek(BasePage.PAGE_HEADER_SIZE + chunkOffset[0] + 1L);
        raw.writeInt(corruptSize);
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
      database = factory.open();
    }

    private int firstChunkOffset(final BasePage page) throws IOException {
      final int recordOffset = (int) page.readUnsignedInt(LocalBucket.PAGE_RECORD_TABLE_OFFSET);
      // FIRST_CHUNK (-2) IS ZIGZAG-ENCODED AS THE SINGLE BYTE 0x03
      assertThat(page.readByte(recordOffset)).as("record at page 0 / slot 0 must be a multi-page head")
          .isEqualTo((byte) 3);
      return recordOffset;
    }
  }

  /**
   * Issue #6163: the HEAD chunk of a multi-page record used to size itself from what the page already said, and could
   * only cap that downwards - so a record that shrank pinned its head chunk to the smallest size it had ever had, and
   * every later update pushed the difference into continuation chunks on other pages, for good. Issue #6154: the head
   * chunk was also the one chunk never fed into the page free-space statistics, although it is the one that can take a
   * whole page's free tail in a single write.
   * <p>
   * Both are the same missing idea - the room the head chunk may use is a property of the PAGE, not of what the chunk
   * declared last time - so they are fixed and tested together.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  // #6163
  @Nested
  class HeadChunkRoom extends BucketPageLayoutTestSupport {
    /** Comfortably inside the room a sealed page's head chunk owns (the 8 KB filler record's own footprint). */
    private static final String MEDIUM = "m".repeat(4 * 1024);
    private static final String SMALL  = "s";

    /**
     * The ratchet itself. A record that spilled, then shrank to nothing, then grew back to a size its own room can hold
     * must be stored in that room alone. Before the fix the head chunk stayed at the byte count of the SMALL value and
     * the 4 KB went to a continuation chunk on another page - permanently, since nothing ever raised the declared size
     * again.
     * <p>
     * <b>Since #6178 the intermediate shape is not a chunk head at all.</b> A record that shrinks back inside the region
     * its slot owns is collapsed into a plain record, and the two conditions coincide exactly: content that fits the
     * head chunk (13 bytes of header) fits the region as a plain record (at most 5 bytes of size marker) by
     * construction. So the ratchet is no longer merely undone by the grow-back - the state it needed cannot be reached
     * in the first place, and the grow-back below happens on a plain record inside its page. What the test asserts is
     * unchanged and is the property that always mattered: after the round trip the 4 KB is on this page, in one piece.
     */
    @Test
    void aHeadChunkThatShrankIsSizedAgainFromTheRoomItOwns() {
      database.transaction(() -> database.getSchema().createDocumentType("Ratchet", 1).createProperty("v", Type.STRING));
      final RID chunked = sealFirstPage("Ratchet");

      database.transaction(() -> chunked.asDocument(true).modify().set("v", SMALL).save());
      Map<String, Object> layout = bucketStats("Ratchet");
      assertThat((Long) layout.get("totalChunks")).as("the shrink must free the continuation chunks").isZero();
      assertThat((Long) layout.get("totalMultiPageRecords"))
          .as("#6178: a record back inside its own region is a plain record again, not a chain of one chunk: " + layout)
          .isZero();

      database.transaction(() -> chunked.asDocument(true).modify().set("v", MEDIUM).save());

      layout = bucketStats("Ratchet");
      assertThat((Long) layout.get("totalChunks"))
          .as("the record owns the room this value fits in, so nothing may spill out of the page: " + layout).isZero();
      assertThat((Long) layout.get("totalMultiPageRecords")).as("and it did not have to spill again: " + layout).isZero();

      database.transaction(() -> assertThat(chunked.asDocument(true).getString("v")).isEqualTo(MEDIUM));
      checkDatabase();
    }

    /**
     * The oscillation the issue describes: a record that repeatedly goes small and large again must settle on one
     * physical shape and keep it, instead of paying one more continuation chunk per cycle.
     * <p>
     * The baseline is the first cycle's own result, not the shape the fixture's spill produced: the spill placed its
     * continuation chunks with whatever room the pages around it had at that moment, so a later rewrite of the chain
     * can legitimately need FEWER of them. What must not happen is the count drifting from one cycle to the next.
     */
    @Test
    void repeatedShrinkAndGrowCyclesDoNotDegradeTheRecord() {
      database.transaction(() -> database.getSchema().createDocumentType("Oscillating", 1).createProperty("v", Type.STRING));
      final RID chunked = sealFirstPage("Oscillating");

      final String large = "l".repeat(70 * 1024);
      assertThat((Long) bucketStats("Oscillating").get("totalChunks")).as("the fixture must really have spilled into a chain")
          .isPositive();

      long chunksWhenLarge = -1;
      for (int cycle = 0; cycle < 4; cycle++) {
        database.transaction(() -> chunked.asDocument(true).modify().set("v", SMALL).save());
        database.transaction(() -> chunked.asDocument(true).modify().set("v", large).save());

        final Map<String, Object> layout = bucketStats("Oscillating");
        final long chunks = (Long) layout.get("totalChunks");
        if (chunksWhenLarge < 0)
          chunksWhenLarge = chunks;
        assertThat(chunks).as("cycle " + cycle + " must not cost an extra chunk: " + layout).isEqualTo(chunksWhenLarge);
        database.transaction(() -> assertThat(chunked.asDocument(true).getString("v")).isEqualTo(large));

        // And back down to a value the head chunk's own room holds: every cycle must be able to reach the shape a
        // record that had never been shrunk has, not merely stay as bad as the cycle before it.
        database.transaction(() -> chunked.asDocument(true).modify().set("v", SMALL).save());
        database.transaction(() -> chunked.asDocument(true).modify().set("v", MEDIUM).save());
        final Map<String, Object> mediumLayout = bucketStats("Oscillating");
        assertThat((Long) mediumLayout.get("totalChunks")).as("cycle " + cycle + " must fit the head chunk again: " + mediumLayout)
            .isZero();
        database.transaction(() -> assertThat(chunked.asDocument(true).getString("v")).isEqualTo(MEDIUM));
      }
      checkDatabase();
    }

    /**
     * The bound on the grow-back, and the reason it is read from the page rather than remembered: the room the shrink
     * released is the page's again, and once another record has taken it the head chunk must stop at that record's
     * offset - not overwrite it.
     */
    @Test
    void theHeadChunkNeverGrowsPastARecordThatTookTheRoomItReleased() {
      database.transaction(() -> database.getSchema().createDocumentType("Bounded", 1).createProperty("v", Type.STRING));
      final RID chunked = sealFirstPage("Bounded");

      database.transaction(() -> chunked.asDocument(true).modify().set("v", SMALL).save());

      final String tenant = "t".repeat(2 * 1024);
      final RID[] newcomer = new RID[1];
      database.transaction(() -> newcomer[0] = database.newDocument("Bounded").set("v", tenant).save().getIdentity());
      assertThat(newcomer[0].getPosition()).as("the released tail must be handed to the next insert on the same page")
          .isEqualTo(chunked.getPosition() + 1);

      database.transaction(() -> chunked.asDocument(true).modify().set("v", MEDIUM).save());

      database.transaction(() -> {
        assertThat(chunked.asDocument(true).getString("v")).isEqualTo(MEDIUM);
        assertThat(newcomer[0].asDocument(true).getString("v")).as("the record that took the room must be untouched")
            .isEqualTo(tenant);
      });
      checkDatabase();
    }

    /**
     * #6154: the head chunk of a record that spills as the LAST record of its page takes the whole free tail, which is
     * the single largest bite anything takes out of a bucket page - and it was the one bite the free-space statistics
     * never heard about. Asserted on the hint itself: the allocator re-reads the page before using a candidate, so a
     * test driven through it would pass either way.
     */
    @Test
    void theHeadChunkIsAccountedInThePageFreeSpaceStatistics() {
      database.transaction(() -> database.getSchema().createDocumentType("Accounted", 1).createProperty("v", Type.STRING));

      final LocalBucket bucket = (LocalBucket) database.getSchema().getType("Accounted").getBuckets(false).getFirst();
      final RID chunked = sealFirstPage("Accounted");

      assertThat(bucket.getFreeSpaceHintForPage(0)).as("a page a spill has sealed has no free space left to offer")
          .isEqualTo(-1);

      // The other direction: shrinking the head chunk hands the tail back, and the statistics must hear that too.
      database.transaction(() -> chunked.asDocument(true).modify().set("v", SMALL).save());
      assertThat(bucket.getFreeSpaceHintForPage(0)).as("the tail the shrink released must be offered again").isPositive();

      checkDatabase();
    }

    /**
     * The grow-back is still a single-slot write, so the disjoint-slot merge (#5279, #6129) has to replay it on a newer
     * committed version of the page. That replay used to refuse any head chunk longer than the committed one outright;
     * it now re-derives the room from the committed page, which is what lets this commit go through.
     */
    @Test
    void aHeadChunkGrowingBackIsReplayedOnACommittedPage() throws Exception {
      final RID[] neighbourHolder = new RID[1];
      database.transaction(() -> {
        database.getSchema().createDocumentType("Merged", 1).createProperty("v", Type.STRING);
        // Written FIRST, so it is on page 0 and is never the record the fill ends up sealing the page with.
        neighbourHolder[0] = database.newDocument("Merged").set("v", "n".repeat(8 * 1024)).save().getIdentity();
      });
      final RID neighbour = neighbourHolder[0];
      final RID chunked = sealFirstPage("Merged");
      database.transaction(() -> chunked.asDocument(true).modify().set("v", SMALL).save());

      // The premise, proved rather than assumed: with the merge switched OFF the same interleaving must FAIL, which is
      // what shows the two records really do share a page.
      database.getConfiguration().setValue(GlobalConfiguration.TX_PAGE_SLOT_MERGE, false);
      try {
        assertThat(growBackSurvives(chunked, neighbour, MEDIUM, "n".repeat(8 * 1024)))//
            .as("the growing record and the neighbour must share a page").isFalse();
      } finally {
        database.getConfiguration().setValue(GlobalConfiguration.TX_PAGE_SLOT_MERGE, true);
      }

      final String rewritten = "r".repeat(8 * 1024);
      assertThat(growBackSurvives(chunked, neighbour, MEDIUM, rewritten)).isTrue();

      final Map<String, Object> layout = bucketStats("Merged");
      assertThat((Long) layout.get("totalChunks")).as("the replayed head chunk must still hold the whole value: " + layout)
          .isZero();

      database.transaction(() -> {
        assertThat(chunked.asDocument(true).getString("v")).isEqualTo(MEDIUM);
        assertThat(neighbour.asDocument(true).getString("v")).isEqualTo(rewritten);
      });
      checkDatabase();
    }

    /**
     * The contended form of the same round trip: a concurrent insert takes the tail the shrink released, and our
     * grow-back needs it back.
     * <p>
     * <b>Since #6178 this exercises the plain-record growth replay, not the head-chunk one</b>: the shrink collapses the
     * record out of its chain, so what the merge replays on the committed page is {@code growRecordInPage}, which makes
     * the room by shifting the newcomer right and therefore succeeds where the head-chunk replay - which may not move
     * anything, its footprint being fixed for the life of the record - had to refuse. The refusal arm of
     * {@code rebaseRecordOnPage}'s head-chunk branch stays where it is: it guards a region that a concurrent commit
     * shrank, and it is now unreachable through the public API for the same reason the ratchet is (a head chunk that
     * declares less than its region no longer survives an update), which makes it a defence and no longer a path.
     * <p>
     * What must hold either way, and is what this test is really about, is the last assertion: the record that took the
     * room is never overwritten.
     */
    @Test
    void aHeadChunkGrowingBackIsReplayedOverAConcurrentInsertWithoutTouchingIt() throws Exception {
      database.transaction(() -> database.getSchema().createDocumentType("Contended", 1).createProperty("v", Type.STRING));
      final RID chunked = sealFirstPage("Contended");
      database.transaction(() -> chunked.asDocument(true).modify().set("v", SMALL).save());

      final String tenant = "t".repeat(2 * 1024);
      final RID[] newcomer = new RID[1];

      database.begin();
      chunked.asDocument(true).modify().set("v", MEDIUM).save();

      final Thread concurrent = new Thread(() -> database.transaction(
          () -> newcomer[0] = database.newDocument("Contended").set("v", tenant).save().getIdentity()));
      concurrent.start();
      concurrent.join();

      assertThat(newcomer[0].getPosition()).as("the concurrent insert must take the room the shrink released")
          .isEqualTo(chunked.getPosition() + 1);

      boolean committed = false;
      try {
        database.commit();
        committed = true;
      } catch (final ConcurrentModificationException e) {
        // Also acceptable: a retry never loses data. What may not happen is a commit that overwrites the newcomer.
      } finally {
        if (database.isTransactionActive())
          database.rollback();
      }

      final String expected = committed ? MEDIUM : SMALL;
      database.transaction(() -> {
        assertThat(newcomer[0].asDocument(true).getString("v"))
            .as("the record that took the room must never be overwritten by a replayed write").isEqualTo(tenant);
        assertThat(chunked.asDocument(true).getString("v"))
            .as("and our own record must hold exactly what the outcome of the commit says").isEqualTo(expected);
      });
      checkDatabase();
    }

    /**
     * The one shape of the region that is NOT safe to write through: a region that cannot host a single byte of chunk
     * content. It says the page is corrupted - a neighbour's slot points inside this chunk's own header - and a chunk
     * size of zero or less is not a shorter chunk, it is a header no reader can walk. It must fail the transaction
     * rather than be written.
     * <p>
     * No sequence of public API calls produces this: the region always contains at least the head chunk's own
     * footprint. So the page is corrupted by hand, in the SAME transaction as the update, which is what keeps the
     * commit-time {@code compressPage} from re-flowing the overlap before the update runs.
     */
    @Test
    void aRegionTooSmallForAnyContentIsRefusedRatherThanWritten() throws Exception {
      final RID[] chunked = new RID[1];
      database.transaction(() -> {
        database.getSchema().createDocumentType("Corrupted", 1).createProperty("v", Type.STRING);
        // Written FIRST so the record that spills has neighbours AFTER it: its region is bounded by one of them.
        chunked[0] = database.newDocument("Corrupted").set("v", "p").save().getIdentity();
      });
      fillFirstPage("Corrupted");
      database.transaction(() -> chunked[0].asDocument(true).modify().set("v", "b".repeat(200 * 1024)).save());
      assertThat((Long) bucketStats("Corrupted").get("totalMultiPageRecords")).as("the fixture must have spilled")
          .isEqualTo(1L);

      final LocalBucket bucket = (LocalBucket) database.getSchema().getType("Corrupted").getBuckets(false).getFirst();

      database.begin();
      try {
        final DatabaseInternal db = (DatabaseInternal) database;
        final MutablePage page = db.getTransaction()
            .getPageToModify(new PageId(db, bucket.getFileId(), 0), bucket.getPageSize(), false);
        final int headChunkPosition = (int) page.readUnsignedInt(
            LocalBucket.PAGE_RECORD_TABLE_OFFSET + (int) chunked[0].getPosition() * Binary.INT_SERIALIZED_SIZE);

        // Point the NEXT slot one byte past the head chunk's marker: its region can then hold no content at all.
        final int recordCountInPage = page.readShort(LocalBucket.PAGE_RECORD_COUNT_IN_PAGE_OFFSET);
        assertThat(recordCountInPage).as("the fixture needs a neighbour after the chunk head").isGreaterThan(1);
        page.writeUnsignedInt(LocalBucket.PAGE_RECORD_TABLE_OFFSET + Binary.INT_SERIALIZED_SIZE, headChunkPosition + 1);

        chunked[0].asDocument(true).modify().set("v", MEDIUM).save();

        // The commit wraps the refusal (a TransactionException over the DatabaseOperationException), so it is asserted
        // on the whole chain rather than on the wrapper of the day.
        assertThatThrownBy(database::commit).hasStackTraceContaining(DatabaseOperationException.class.getName())
            .hasStackTraceContaining("no room left in page");
      } finally {
        if (database.isTransactionActive())
          database.rollback();
      }
    }

    /**
     * Grows a chunked record back and, before committing, has another thread commit a change to a record sharing its
     * page. Returns true when our commit went through, i.e. the merge absorbed the version bump.
     */
    private boolean growBackSurvives(final RID growing, final RID neighbour, final String value, final String neighbourValue)
        throws InterruptedException {
      database.begin();
      growing.asDocument(true).modify().set("v", value).save();

      final Thread concurrent = new Thread(
          () -> database.transaction(() -> neighbour.asDocument(true).modify().set("v", neighbourValue).save()));
      concurrent.start();
      concurrent.join();

      try {
        database.commit();
        return true;
      } catch (final ConcurrentModificationException e) {
        return false;
      } finally {
        if (database.isTransactionActive())
          database.rollback();
      }
    }
  }

  /**
   * Issue #6358: what a write TELLS the free-space statistics and what the commit MEASURES of the same page have to be
   * the same number. Since #6349 the commit packs every page a transaction touched and records the free tail it
   * measured, so the two descriptions of one page sit side by side - and they disagreed by a known constant.
   * <p>
   * Every assertion here is that one invariant, taken over the same page twice: the hint at the end of the transaction
   * body (which only the write-path deltas have produced) against the hint after the commit (which the measurement has
   * produced). The workloads are pure inserts and grows, so the compression moves no bytes and the two must agree
   * exactly; a difference is a writer subtracting something other than what its chunk occupies.
   * <p>
   * The three writers this covers each subtracted something of their own - a continuation chunk of a fresh spill left
   * the 12 header bytes out, the extension of an existing chain left the marker byte out and counted the whole
   * remaining buffer, the one landing on a brand-new page counted neither and measured against a base that still
   * included the page header - and one shared {@code chunkFootprint} now answers for all of them.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  // #6358
  @Nested
  class ChunkFootprintAccounting extends BucketPageLayoutTestSupport {
    private static final String TYPE = "Chunked";

    /**
     * The continuation chunks of a fresh spill ({@code writeMultiPageRecord}). Every page the chain fills whole has no
     * free tail to report, so the page that shows the defect is the LAST one - the only one a chunk leaves room on -
     * and the payload is sized against the page geometry so that room really is there.
     */
    @Test
    void theTailAFreshSpillLeavesIsReportedAsThePageHasIt() {
      database.transaction(() -> database.getSchema().createDocumentType(TYPE, 1).createProperty("v", Type.STRING));

      final LocalBucket bucket = bucketOf(TYPE);
      // Two full pages of chunks and a third one half taken, so the last chunk leaves a tail far above the 10% a hint
      // is kept for.
      final String payload = "s".repeat(wholeUsablePage(bucket) * 5 / 2);

      final Map<Integer, Integer> beforeCommit = new LinkedHashMap<>();
      database.transaction(() -> {
        database.newDocument(TYPE).set("v", payload).save();
        collectHints(bucket, beforeCommit);
      });

      assertThat(bucket.getTotalPages()).as("the fixture must really span a chain of continuation chunks").isGreaterThan(2);
      assertHintsSurviveTheCommit(bucket, beforeCommit);
      checkDatabase();
    }

    /**
     * The chunks an UPDATE adds to a chain it already has ({@code updateMultiPageRecord}), both ways it can place them:
     * on a page the allocator reused - here the one the previous chunk left room on - and on a page created for them.
     * Growing the record in steps drives it through both, since each step first fills what the chain has and then asks
     * for more.
     */
    @Test
    void theTailAChainExtensionLeavesIsReportedAsThePageHasIt() {
      database.transaction(() -> database.getSchema().createDocumentType(TYPE, 1).createProperty("v", Type.STRING));

      final LocalBucket bucket = bucketOf(TYPE);
      final int usable = wholeUsablePage(bucket);
      final RID[] rid = new RID[1];
      database.transaction(() -> rid[0] = database.newDocument(TYPE).set("v", "g".repeat(usable)).save().getIdentity());

      // Strictly growing, on purpose: a shrink FREES chunks, and the bytes a free gives back are deliberately reported
      // by the commit's compression rather than by the write (#6339, and the reason it is measured there), so a
      // shrinking step would break this invariant by design instead of by defect.
      for (int step = 3; step <= 8; step++) {
        final String payload = "g".repeat(usable * step / 2 + usable / 3);
        final Map<Integer, Integer> beforeCommit = new LinkedHashMap<>();
        database.transaction(() -> {
          writeNow(rid[0].asDocument(true).modify().set("v", payload));
          collectHints(bucket, beforeCommit);
        });
        assertHintsSurviveTheCommit(bucket, beforeCommit);
        database.transaction(() -> assertThat(rid[0].asDocument(true).getString("v")).isEqualTo(payload));
      }
      checkDatabase();
    }

    /**
     * Item 1 of the issue, the one the three writers above kept feeding: {@code updatePageStatistics} read an
     * {@code availableSpace} of 0 as "I have no measurement, apply my delta to the number you already hold" whenever it
     * held one for that page - so a caller reporting a page with genuinely no free tail left had its delta added to a
     * stale number.
     * <p>
     * The caller built here is real and is the one that reaches it with the largest delta: a record that spilled as the
     * LAST record of its page owns a head chunk running to the page's maximum content size, so when it shrinks back
     * into its own slot (#6178) it reports a free tail of 0 and a delta of everything the chunk gave back. The stale
     * entry is planted rather than waited for, because what stands between today's callers and this branch is a
     * coincidence of thresholds and not a rule.
     */
    @Test
    void aZeroFreeTailIsNotReadAsAnUnknownFreeTail() {
      database.transaction(() -> database.getSchema().createDocumentType(TYPE, 1).createProperty("v", Type.STRING));

      final LocalBucket bucket = bucketOf(TYPE);
      final RID sealer = sealFirstPage(TYPE);
      assertThat(bucket.getFreeSpaceHintForPage(0)).as("a page a spill has sealed has no free tail to offer")
          .isEqualTo(-1);

      // Tell the allocator page 0 has a kilobyte to spare. It has not: the spill took its last byte.
      final int planted = 1_024;
      final JSONArray statistics = new JSONArray();
      statistics.put(new JSONObject().put("id", 0).put("free", planted));
      bucket.setPageStatistics(statistics);

      final int[] duringTheTransaction = new int[1];
      database.transaction(() -> {
        writeNow(sealer.asDocument(true).modify().set("v", "t"));
        duringTheTransaction[0] = bucket.getFreeSpaceHintForPage(0);
      });

      final int afterTheCommit = bucket.getFreeSpaceHintForPage(0);
      assertThat(afterTheCommit).as("the collapse must really have given the page its tail back").isGreaterThan(planted);
      assertThat(afterTheCommit).as("and the tail is the head chunk's, not the whole page's")
          .isLessThan(wholeUsablePage(bucket));
      assertThat(duringTheTransaction[0])
          .as("the collapse measured the page it wrote; nothing may be added to the stale number the map held")
          .isEqualTo(afterTheCommit);

      database.transaction(() -> assertThat(sealer.asDocument(true).getString("v")).isEqualTo("t"));
      checkDatabase();
    }

    /**
     * The same double count, on the one path #6339 did not reach: the delete a commit-time page conflict makes it
     * REPLAY ({@code rebaseRecordDeleteOnPage}). It measured the free tail of the page with the slot already zeroed -
     * so whatever the free released is in the number - and then subtracted the record's footprint from it as well. On a
     * record freed in the MIDDLE of its page the tail does not move at all, so the subtraction was pure loss, and on a
     * nearly full page it takes the number BELOW ZERO - a page reported as having less than no free space.
     * <p>
     * Found by the tripwire this issue added rather than by reading: the assertion in {@code updatePageStatistics}
     * fires under {@code -ea} (surefire's default) inside the commit, which is what this drives and what makes the
     * test fail loudly instead of quietly recording nonsense the next compression would paper over.
     */
    @Test
    void aRebasedDeleteReportsTheTailItMeasuredAndNothingLess() throws Exception {
      database.transaction(() -> database.getSchema().createDocumentType(TYPE, 1).createProperty("v", Type.STRING));

      final LocalBucket bucket = bucketOf(TYPE);
      final RID lastOnPageZero = fillFirstPage(TYPE);
      assertThat(lastOnPageZero.getPosition()).as("page 0 must hold several records for one of them to be a middle one")
          .isGreaterThan(1L);

      // A record in the MIDDLE of page 0 - freeing it moves no free tail - and another one to bump the page version
      // with an in-place, same-size write the slot merge can replay.
      final RID victim = new RID(lastOnPageZero.getBucketId(), 1);
      final RID bumped = new RID(lastOnPageZero.getBucketId(), 0);

      final long recordsBefore = countRecords(TYPE);
      final CountDownLatch deleteDone = new CountDownLatch(1);
      final CountDownLatch bumpCommitted = new CountDownLatch(1);
      final List<Throwable> errors = new CopyOnWriteArrayList<>();

      final Thread bumper = new Thread(() -> {
        try {
          deleteDone.await();
          database.transaction(() -> {
            final String current = bumped.asDocument(true).getString("v");
            bumped.asDocument(true).modify().set("v", "B" + current.substring(1)).save();
          }, true, 50);
        } catch (final Throwable e) {
          errors.add(e);
        } finally {
          bumpCommitted.countDown();
        }
      }, "issue-6358-bumper");
      bumper.start();

      boolean committed = false;
      for (int attempt = 0; attempt < 20 && !committed; attempt++) {
        database.begin();
        try {
          victim.asDocument(true).delete();
          if (attempt == 0) {
            deleteDone.countDown();
            bumpCommitted.await();
          }
          database.commit();
          committed = true;
        } catch (final ConcurrentModificationException retryable) {
          if (database.isTransactionActive())
            database.rollback();
        }
      }
      bumper.join();

      if (!errors.isEmpty())
        throw new AssertionError("the competing write failed: " + errors.getFirst(), errors.getFirst());
      assertThat(committed).as("the delete must eventually commit").isTrue();

      assertThat(bucket.getFreeSpaceHintForPage(0)).as("a page can never be offered less than no free space")
          .isGreaterThanOrEqualTo(-1);
      assertThat(countRecords(TYPE)).as("the replayed delete must have removed exactly the one record")
          .isEqualTo(recordsBefore - 1);
      database.transaction(() -> assertThat(bumped.asDocument(true).getString("v")).startsWith("B"));
      checkDatabase();
    }

    /**
     * Runs the update the COMMIT would run, one line earlier: {@code save()} only queues a record, and the bucket write
     * happens inside the commit - a line before the compression whose measurement is the very thing these tests need to
     * compare against. This is the same call the commit makes on every queued record
     * ({@code TransactionContext.commit1stPhase} -> {@code updateRecordNoLock}), not a path fabricated for the test:
     * pulling it forward changes when the write happens, never what it writes.
     */
    private void writeNow(final MutableDocument record) {
      ((DatabaseInternal) database).updateRecordNoLock(record, false);
    }

    /**
     * The hint every page of the bucket carries. Taken at the END of a transaction body - every write has landed and
     * the only thing left is the commit - so what it holds is the sum of the write-path deltas and nothing else.
     */
    private static void collectHints(final LocalBucket bucket, final Map<Integer, Integer> into) {
      into.clear();
      // The pages this transaction reserved are not in getTotalPages() until it commits, so the walk goes well past
      // what the bucket admits to having; a page that does not exist simply has no entry.
      for (int pageId = 0; pageId < bucket.getTotalPages() + 32; pageId++)
        into.put(pageId, bucket.getFreeSpaceHintForPage(pageId));
    }

    /**
     * The invariant. Not vacuous by construction: at least one page must carry a real hint, otherwise the comparison
     * would hold on a map of nothing but -1 and would go on holding it however wrong the writers were.
     */
    private static void assertHintsSurviveTheCommit(final LocalBucket bucket, final Map<Integer, Integer> beforeCommit) {
      assertThat(beforeCommit.values().stream().anyMatch(hint -> hint > 0))
          .as("the fixture must leave at least one page with a free tail worth a hint: " + beforeCommit)
          .isTrue();

      for (final Map.Entry<Integer, Integer> hint : beforeCommit.entrySet())
        assertThat(bucket.getFreeSpaceHintForPage(hint.getKey()))
            .as("page " + hint.getKey() + " was reported as having " + hint.getValue()
                + " free bytes by the write; the commit measured what it really has")
            .isEqualTo(hint.getValue());
    }

    /** What an EMPTY page of this bucket has to offer: everything past the page header and the slot table. */
    private static int wholeUsablePage(final LocalBucket bucket) {
      return bucket.getPageSize() - BasePage.PAGE_HEADER_SIZE - bucket.contentHeaderSize;
    }
  }
}
