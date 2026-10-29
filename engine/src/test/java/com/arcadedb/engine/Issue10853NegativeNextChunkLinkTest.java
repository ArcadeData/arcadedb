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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
class Issue10853NegativeNextChunkLinkTest extends BucketPageLayoutTestSupport {

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
