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

import com.arcadedb.engine.LocalBucket;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #6196: a placeholder whose CONTENT record had to spill into chunks was returned TWICE by a scan.
 * <p>
 * A content record is normally recognised by the NEGATIVE size marker its slot carries, and every reader that walks a
 * page skips it on that basis. But a content record that no page can host whole is written by
 * {@code writeMultiPageRecord}, which used to stamp the plain {@code FIRST_CHUNK} marker of an ordinary multi-page
 * record on its head chunk - and with it went the only record of "this slot is somebody's content, not a record".
 * The bytes were then handed out twice: once through the placeholder POINTER that references them, and once as a
 * document of their own under the head chunk's RID. {@code check()} had the mirror of the same confusion, counting
 * such a record under {@code totalMultiPageRecords} and never under {@code totalSurrogateRecords}, and {@code count()}
 * counted it as a record.
 * <p>
 * The fix gives the head chunk of a content record a marker of its own, {@code FIRST_CHUNK_PLACEHOLDER_CONTENT} (-4),
 * in the free slot the marker namespace already had between {@code NEXT_CHUNK} (-3) and the negated sizes (&lt; -5).
 * Databases written before it still hold the ambiguous shape, so {@code CHECK DATABASE} reports one and
 * {@code CHECK DATABASE FIX} converts it - the ambiguity is repaired, not tolerated for ever.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6196PlaceholderContentChainTest extends BucketPageLayoutTestSupport {
  private static final String TYPE                       = "Surrogate";
  /** {@code LocalBucket.FIRST_CHUNK} (-2) and {@code FIRST_CHUNK_PLACEHOLDER_CONTENT} (-4), zigzag-encoded. */
  private static final byte   FIRST_CHUNK_MARKER         = 3;
  private static final byte   CONTENT_CHUNK_MARKER       = 7;
  /** {@code LocalBucket.RECORD_PLACEHOLDER_POINTER} (-1), zigzag-encoded. */
  private static final byte   PLACEHOLDER_POINTER_MARKER = 1;
  /** 200 KB fits no page whole, so the content record created behind the pointer spills into a chain of its own. */
  private static final String HUGE                       = "h".repeat(200 * 1024);

  /** Continuation chunks the record that SEALS page 0 owns - every chunk in the bucket that is not the content's. */
  private long chunksOfTheSealingRecord;
  /** The record that seals page 0 by spilling: an ordinary multi-page record, and the only other chunk head around. */
  private RID  sealingRecord;

  /**
   * The issue as reported: the placeholder's content must be a document exactly once, under the RID the user knows,
   * and the head chunk that holds it must be invisible to a scan, to {@code count()} and to a direct load.
   */
  @Test
  void aPlaceholderWhoseContentSpilledIsScannedOnce() {
    final RID placeholder = placeholderWithChainedContent();

    final Map<String, Object> layout = bucketStats(TYPE);
    assertThat((Long) layout.get("totalPlaceholderRecords")).as("the fixture must really be a placeholder: " + layout)
        .isEqualTo(1L);
    assertThat((Long) layout.get("totalSurrogateRecords"))
        .as("the content record is a surrogate even though it is stored as a chain: " + layout).isEqualTo(1L);
    // Only the record that sealed page 0 when it spilled: the content record's chain is no longer counted as one.
    assertThat((Long) layout.get("totalMultiPageRecords"))
        .as("a placeholder's content is not a multi-page record of its own: " + layout).isEqualTo(1L);

    assertThat(countRecordsHolding(HUGE)).as("the content must be handed out exactly once, through the pointer")
        .isEqualTo(1L);

    final RID content = contentRidOf(placeholder);
    assertThat(markerByteAt(content)).as("the head chunk of a content record carries its own marker")
        .isEqualTo(CONTENT_CHUNK_MARKER);
    database.transaction(() -> {
      assertThat(bucketOf(TYPE).existsRecord(content)).as("a content record does not exist on its own").isFalse();
      assertThat(bucketOf(TYPE).getRecordInternal(content, false))
          .as("and cannot be loaded as a record on its own").isNull();
      assertThat(placeholder.asDocument(true).getString("v")).isEqualTo(HUGE);
    });

    checkDatabase();
  }

  /**
   * The whole life of such a record through the pointer: rewritten large (the chain is reused), shrunk back (since
   * #6286 the chain COLLAPSES into the negated-size shape a content record has when it fits its page), grown again,
   * and finally deleted with whatever it had become behind it.
   */
  @Test
  void theContentChainKeepsItsMarkerThroughEveryRewrite() {
    final RID placeholder = placeholderWithChainedContent();
    final RID content = contentRidOf(placeholder);

    final String otherHuge = "o".repeat(200 * 1024);
    database.transaction(() -> placeholder.asDocument(true).modify().set("v", otherHuge).save());
    assertThat(markerByteAt(content)).as("a rewrite in place must not turn the content into a record")
        .isEqualTo(CONTENT_CHUNK_MARKER);
    assertThat(countRecordsHolding(otherHuge)).isEqualTo(1L);

    database.transaction(() -> placeholder.asDocument(true).modify().set("v", "p").save());
    Map<String, Object> layout = bucketStats(TYPE);
    assertThat((Long) layout.get("totalPlaceholderRecords")).as("the pointer must still be a pointer: " + layout)
        .isEqualTo(1L);
    assertThat((Long) layout.get("totalChunks")).as("and the content's own chain is collapsed away (#6286): " + layout)
        .isEqualTo(chunksOfTheSealingRecord);
    assertThat((Long) layout.get("totalSurrogateRecords"))
        .as("into the negated-size shape a content record has: " + layout).isEqualTo(1L);
    assertThat(countRecordsHolding("p")).as("nor must a shrink multiply the content").isEqualTo(1L);

    database.transaction(() -> placeholder.asDocument(true).modify().set("v", HUGE).save());
    assertThat(countRecordsHolding(HUGE)).isEqualTo(1L);

    final long recordsBefore = countRecords();
    database.transaction(() -> placeholder.asDocument(true).delete());
    assertThat(countRecords()).as("deleting the pointer frees the content chain with it").isEqualTo(recordsBefore - 1);
    layout = bucketStats(TYPE);
    assertThat((Long) layout.get("totalPlaceholderRecords")).as("the pointer is gone: " + layout).isZero();
    assertThat((Long) layout.get("totalSurrogateRecords")).as("no content record is left behind: " + layout).isZero();
    // Back to the chunks the record that SEALED page 0 owns, and not one more: the content record's own chain, which
    // was every chunk on top of those, was freed with the pointer that led to it.
    assertThat((Long) layout.get("totalChunks")).as("nor any of its chunks: " + layout).isEqualTo(chunksOfTheSealingRecord);

    checkDatabase();
  }

  /**
   * A database written BEFORE this fix holds the ambiguous shape, and no reader can tell it from an ordinary
   * multi-page record - so the duplicate is still there. {@code CHECK DATABASE} names it and {@code FIX} converts the
   * marker in place, which is what keeps the tolerance out of the readers.
   */
  @Test
  void checkDatabaseFixRepairsALegacyAmbiguousContentRecord() {
    final RID placeholder = placeholderWithChainedContent();
    final RID content = contentRidOf(placeholder);

    // Downgrade the marker to what every release before this fix wrote there.
    writeMarkerByteAt(content, FIRST_CHUNK_MARKER);
    assertThat(countRecordsHolding(HUGE)).as("the shape #6196 reported: the content is scanned twice").isEqualTo(2L);

    final Result unfixed = checkDatabaseRow(false);
    assertThat(numberProperty(unfixed, "totalErrors")).as("check must report it: " + unfixed.toJSON()).isEqualTo(1L);
    assertThat(warningsOf(unfixed).toString()).contains(content.toString()).contains("placeholder");

    final Result fixed = checkDatabaseRow(true);
    assertThat(numberProperty(fixed, "totalErrors")).as("fix must repair it: " + fixed.toJSON()).isEqualTo(1L);
    assertThat((Collection<?>) fixed.getProperty("deletedRecordsAfterFix"))
        .as("and repair it by rewriting the marker, never by deleting the record: " + fixed.toJSON()).isEmpty();

    assertThat(markerByteAt(content)).isEqualTo(CONTENT_CHUNK_MARKER);
    assertThat(countRecordsHolding(HUGE)).as("the duplicate is gone").isEqualTo(1L);
    database.transaction(() -> assertThat(placeholder.asDocument(true).getString("v")).isEqualTo(HUGE));

    checkDatabase();
  }

  /**
   * The other way a legacy ambiguous head stops being ambiguous, and it needs no {@code FIX}: an ordinary UPDATE
   * through the pointer that shrinks the record back inside the region its slot owns collapses it (#6286), and the
   * collapse writes the NEGATED size marker - which is the very thing the ambiguous shape was missing. The duplicate
   * a scan used to hand out goes with it.
   * <p>
   * Which sign that collapse writes is decided by the FLAG the slot was reached through and never by the marker
   * found, because the marker is exactly what cannot tell a legacy content head from a record's own head chunk. Get
   * that wrong and the collapse writes a POSITIVE size, which is #6196 all over again on a record that was already
   * suffering from it - so the copy count is what this asserts, not the byte.
   * <p>
   * The write is not offered to the disjoint-slot merge (neither collapse kind names FIRST_CHUNK as the marker it
   * replays over) and the page is poisoned instead; a legacy head that does NOT collapse keeps its ordinary
   * head-chunk tracking, which is what the second half of this test pins.
   */
  @Test
  void anUpdateCollapsesALegacyAmbiguousContentHeadAndEndsItsAmbiguity() {
    final RID placeholder = placeholderWithChainedContent();
    final RID content = contentRidOf(placeholder);

    writeMarkerByteAt(content, FIRST_CHUNK_MARKER);
    assertThat(countRecordsHolding(HUGE)).as("the shape #6196 reported: the content is scanned twice").isEqualTo(2L);

    // Still far too big for the region the content record's slot owns: it stays a chain, and stays ambiguous.
    //
    // That this rewrite also keeps its head-chunk tracking - the poison belongs to the collapse, and firing it up
    // front cost an ordinary rewrite the replay it has always had (PR review) - is deliberately NOT asserted here.
    // An update is deferred to commit, so the flag does not exist while the transaction is open, and by the time it
    // does the transaction is gone; an assertion placed either side passes whether or not the poison fires, which is
    // worth less than no assertion at all. What it costs is a merge opportunity and never correctness, and only on
    // data written before #6196.
    final String stillHuge = "s".repeat(200 * 1024);
    database.transaction(() -> placeholder.asDocument(true).modify().set("v", stillHuge).save());
    assertThat(markerByteAt(content)).as("a rewrite that stays a chain leaves the legacy marker alone")
        .isEqualTo(FIRST_CHUNK_MARKER);
    assertThat(countRecordsHolding(stillHuge)).as("so the duplicate is still there").isEqualTo(2L);

    // Back to a handful of bytes: now it fits, and the collapse writes the marker the shape was missing.
    database.transaction(() -> placeholder.asDocument(true).modify().set("v", "p").save());

    final Map<String, Object> layout = bucketStats(TYPE);
    assertThat((Long) layout.get("totalPlaceholderRecords")).as("the pointer must still be a pointer: " + layout)
        .isEqualTo(1L);
    assertThat((Long) layout.get("totalSurrogateRecords"))
        .as("and its content a surrogate, not a record of its own: " + layout).isEqualTo(1L);
    assertThat((Long) layout.get("totalChunks")).as("with the chain freed: " + layout)
        .isEqualTo(chunksOfTheSealingRecord);
    assertThat(countRecordsHolding("p")).as("one copy, through the pointer, and no second one").isEqualTo(1L);

    database.transaction(() -> assertThat(placeholder.asDocument(true).getString("v")).isEqualTo("p"));
    // Nothing left for CHECK DATABASE to repair: the update did what the FIX would have done, and more.
    checkDatabase();
  }

  /**
   * A content record is not a record, on the WRITE paths as much as on the read ones: reached at its own RID, an
   * update and a delete both refuse it, exactly as they already do for a content record small enough to have kept the
   * negated size marker. Anything else would let a caller rewrite or free content a placeholder pointer still
   * references, leaving that pointer aimed at somebody else's bytes or at nothing.
   * <p>
   * Driven through {@code LocalBucket} rather than through a document, deliberately: {@code content.asDocument()} is
   * refused one layer earlier, by the read that will not hand out a content record, so it would never reach the two
   * guards this is about.
   */
  @Test
  void aChunkedContentRecordIsNotUpdatableOrDeletableAtItsOwnRid() {
    final RID content = contentRidOf(placeholderWithChainedContent());

    database.transaction(() -> {
      final LocalBucket bucket = bucketOf(TYPE);

      final MutableDocument surrogate = database.newDocument(TYPE).set("v", "whatever the caller meant");
      ((RecordInternal) surrogate).setIdentity(content);
      assertThatThrownBy(() -> bucket.updateRecord(surrogate, false))
          .as("a content record cannot be rewritten behind its placeholder's back")
          .isInstanceOf(RecordNotFoundException.class);

      assertThatThrownBy(() -> bucket.deleteRecord(content))
          .as("nor freed, which would leave the pointer aimed at nothing")
          .isInstanceOf(RecordNotFoundException.class);
    });

    // Both refusals left the record exactly as it was: still one copy, still reachable through the pointer.
    assertThat(countRecordsHolding(HUGE)).isEqualTo(1L);
    assertThat(markerByteAt(content)).isEqualTo(CONTENT_CHUNK_MARKER);
    checkDatabase();
  }

  /**
   * The compound legacy shape: a content record still wearing the ambiguous marker whose chain is ALSO broken. The
   * ambiguity itself must be booked ONCE - the broken chain is what FIX can act on, so the record is force-deleted,
   * and the ambiguity reconciliation must then leave alone a slot that is no longer there rather than report a second
   * error and a repair that could not have happened.
   * <p>
   * What the run also reports is everything the corruption really cost, which is more than the one head slot: the
   * placeholder POINTER that led to the record is removed with it (#6292 - left behind, it is a slot every scan skips
   * and {@code count(*)} counts, for good), and the continuation chunks the overwritten pointer had already cut the
   * chain off from are reclaimed (#6294 - nothing collected them before, whatever the comments promised). Those three
   * chunks are dead space rather than a corruption, so they are counted and reclaimed without being booked as errors:
   * the record had ONE defect, and one error is what the run reports.
   */
  @Test
  void aLegacyContentHeadWithABrokenChainIsReportedOnce() {
    final RID placeholder = placeholderWithChainedContent();
    final RID content = contentRidOf(placeholder);

    writeMarkerByteAt(content, FIRST_CHUNK_MARKER);
    final long chunksBefore = (Long) bucketStats(TYPE).get("totalChunks");
    breakChainAt(content);

    final Result fixed = checkDatabaseRow(true);
    assertThat(warningsOf(fixed).toString()).as("the broken chain is what it names: " + fixed.toJSON())
        .contains("broken multi-page chunk chain").doesNotContain("could not be repaired")
        .doesNotContain("ambiguous chunk chain");
    assertThat((Collection<Object>) fixed.<Collection<Object>>getProperty("deletedRecordsAfterFix"))
        .as("the head slot and the pointer that was the record's identity: " + fixed.toJSON())
        .containsExactlyInAnyOrder(content, placeholder);
    assertThat(numberProperty(fixed, "danglingPlaceholderPointersFixed"))
        .as("the pointer is removed with the content it can no longer reach: " + fixed.toJSON()).isEqualTo(1L);

    // The chunks the corrupted pointer had cut off: leaked before this run, and reclaimed by it.
    final long orphanedChunks = numberProperty(fixed, "orphanedChunks");
    assertThat(orphanedChunks).as("the cut-off chunks must be found: " + fixed.toJSON()).isPositive();
    assertThat(numberProperty(fixed, "orphanedChunksReclaimed")).as("and reclaimed: " + fixed.toJSON())
        .isEqualTo(orphanedChunks);
    // ONE error: the broken chain. The chunks it leaked are dead space, not a corruption - no record is wrong and no
    // query is affected - so they are counted and reclaimed, never booked as errors of their own.
    assertThat(numberProperty(fixed, "totalErrors"))
        .as("one record, one defect it can act on, one error: " + fixed.toJSON()).isEqualTo(1L);

    // Nothing is left for a second run to find: not the slot the pointer led to, not the pointer, not the chunks.
    final Result again = checkDatabaseRow(false);
    assertThat(numberProperty(again, "totalErrors")).as("and the database checks out afterwards: " + again.toJSON())
        .isZero();
    assertThat(numberProperty(again, "totalChunks")).as("with the leaked chunks really gone: " + again.toJSON())
        .isLessThan(chunksBefore);
    assertThat(numberProperty(again, "totalPlaceholderRecords"))
        .as("and no pointer aimed at nothing: " + again.toJSON()).isZero();
  }

  /**
   * The repair rests on the engine's own invariant - a placeholder pointer leads to the content record written for it
   * and nothing else - and a corrupted pointer breaks it. The one form of that which can be RECOGNISED is two pointers
   * leading to the same slot, which a healthy bucket cannot produce: FIX must then report and leave the marker alone,
   * because the marker is unrecoverable information (it says whose the record is) and nothing here knows which of the
   * two pointers is the lie.
   */
  @Test
  void aContentHeadTwoPointersLeadToIsReportedAndLeftAlone() {
    final RID placeholder = placeholderWithChainedContent();
    final RID content = contentRidOf(placeholder);

    writeMarkerByteAt(content, FIRST_CHUNK_MARKER);
    // A second pointer to the same content, over a record that has room for one: a corrupted pointer, in the one shape
    // that can be told apart from a healthy one.
    final RID hijacked = new RID(placeholder.getBucketId(), placeholder.getPosition() + 1);
    database.transaction(() -> onSlot(hijacked, page -> {
      final int recordOffset = recordOffsetOf(page, hijacked);
      page.writeByte(recordOffset, PLACEHOLDER_POINTER_MARKER);
      page.writeLong(recordOffset + 1, content.getPosition());
      return 0L;
    }));

    final Result fixed = checkDatabaseRow(true);
    assertThat(warningsOf(fixed).toString()).as("the refusal must name the record and the reason: " + fixed.toJSON())
        .contains(content.toString()).contains("more than one placeholder pointer");
    assertThat(markerByteAt(content)).as("and the marker must be exactly as it was found").isEqualTo(FIRST_CHUNK_MARKER);
    assertThat((Collection<?>) fixed.getProperty("deletedRecordsAfterFix"))
        .as("nothing is deleted either: " + fixed.toJSON()).isEmpty();

    // The corruption this test fabricated is deliberately NOT repairable - that is the whole point - so the type goes
    // with the test rather than being left for the integrity check every test ends with.
    database.transaction(() -> database.getSchema().dropType(TYPE));
  }

  /** Points the head chunk of {@code rid} at a page far past the end of the bucket, which breaks its chain. */
  private void breakChainAt(final RID rid) {
    database.transaction(() -> onSlot(rid, page -> {
      // [marker:1][chunkSize:int][nextChunkPointer:long][content...]
      page.writeLong(recordOffsetOf(page, rid) + 1 + Binary.INT_SERIALIZED_SIZE, Integer.MAX_VALUE);
      return 0L;
    }));
  }

  /**
   * The commit-time half of the marker: the disjoint-slot merge replays a head chunk only onto a committed slot that
   * still carries the marker the write started from, and the two kinds are what tell it which one that is.
   * <p>
   * Driven through {@code rebaseRecordOnPage} directly, in the manner {@code Issue6129ChunkedSlotMergeTest} already
   * uses for the tracking guards and for the same reason: the shape transition this refuses - a slot that turns from
   * a record's head chunk into a placeholder's content, or back, under a concurrent commit - is one no write path
   * produces today, so a test that went through the engine would prove nothing about the guard. What must not happen
   * is that a future one reaches it and is silently mis-merged, which is what the two refusals below assert.
   * <p>
   * The two positive controls are not decoration: without them a guard that refused EVERYTHING would pass this test.
   */
  @Test
  void aHeadChunkIsOnlyReplayedOntoTheMarkerItsWriteStartedFrom() {
    final RID content = contentRidOf(placeholderWithChainedContent());

    database.begin();
    try {
      final LocalBucket bucket = bucketOf(TYPE);

      // The CONTENT record's head chunk: replayable as placeholder content, refused as a record.
      final byte[] contentImage = chunkImageOf(content);
      assertThat(rebase(bucket, content, contentImage, TransactionContext.SLOT_KIND_FIRST_CHUNK))
          .as("a content head must not be replayed under the kind of a record's own head").isFalse();
      assertThat(rebase(bucket, content, contentImage, TransactionContext.SLOT_KIND_FIRST_CHUNK_PLACEHOLDER_CONTENT))
          .as("and must be replayable under its own").isTrue();

      // The record that SEALED page 0: an ordinary multi-page record, and the mirror of the above.
      final byte[] recordImage = chunkImageOf(sealingRecord);
      assertThat(rebase(bucket, sealingRecord, recordImage,
          TransactionContext.SLOT_KIND_FIRST_CHUNK_PLACEHOLDER_CONTENT))
          .as("a record's head must not be replayed under the kind of a content head").isFalse();
      assertThat(rebase(bucket, sealingRecord, recordImage, TransactionContext.SLOT_KIND_FIRST_CHUNK))
          .as("and must be replayable under its own").isTrue();
    } finally {
      database.rollback();
    }

    database.transaction(() -> assertThat(placeholderRecordCount()).isEqualTo(1L));
    checkDatabase();
  }

  /**
   * The same guard for the COLLAPSE (#6286), which is where the two kinds earn their keep: both start from a head
   * chunk and both end with the slot holding plain content, and the ONLY thing that separates them is the marker the
   * replay must still find and the SIGN of the one it writes. A replay that took the wrong one would give a
   * placeholder's content a positive size - a document of its own, which is #6196 - or hide a record behind a
   * negative one.
   * <p>
   * Driven through {@code rebaseRecordOnPage} for the reason the sibling test above states: the transition it refuses
   * is one no write path produces today, so only a direct drive proves the guard rather than the absence of a caller.
   * Both directions, and both positive controls, for the same reason as there.
   */
  @Test
  void aCollapseIsOnlyReplayedOntoTheMarkerItsWriteStartedFrom() {
    final RID content = contentRidOf(placeholderWithChainedContent());

    // Small enough for the region either slot owns, so a refusal can only come from the marker check.
    final byte[] collapsed = "collapsed".getBytes(StandardCharsets.UTF_8);

    database.begin();
    try {
      final LocalBucket bucket = bucketOf(TYPE);

      final byte[] contentImage = chunkImageOf(content);
      assertThat(rebase(bucket, content, collapsed, contentImage, TransactionContext.SLOT_KIND_CHUNK_COLLAPSED_TO_RECORD))
          .as("a content head must not collapse under the kind that writes a record's positive size").isFalse();
      assertThat(rebase(bucket, content, collapsed, contentImage,
          TransactionContext.SLOT_KIND_CHUNK_COLLAPSED_TO_PLACEHOLDER_CONTENT))
          .as("and must collapse under the one that writes the negated size its shape is known by").isTrue();

      final byte[] recordImage = chunkImageOf(sealingRecord);
      assertThat(rebase(bucket, sealingRecord, collapsed, recordImage,
          TransactionContext.SLOT_KIND_CHUNK_COLLAPSED_TO_PLACEHOLDER_CONTENT))
          .as("a record's own head must not collapse under the kind of a content head").isFalse();
      assertThat(rebase(bucket, sealingRecord, collapsed, recordImage,
          TransactionContext.SLOT_KIND_CHUNK_COLLAPSED_TO_RECORD))
          .as("and must collapse under its own").isTrue();
    } finally {
      database.rollback();
    }

    database.transaction(() -> assertThat(placeholderRecordCount()).isEqualTo(1L));
    checkDatabase();
  }

  /**
   * The image the disjoint-slot merge keeps for a head chunk: everything after the marker, i.e.
   * {@code [int chunkSize][long nextChunk][chunkSize bytes of content]}. Replaying it unchanged is a no-op write, so
   * the positive controls above assert the guard and not the content.
   */
  private byte[] chunkImageOf(final RID rid) {
    final byte[][] image = new byte[1][];
    onSlot(rid, page -> {
      // Both markers this test meets are one zigzag byte, so the chunk header starts right after it.
      final int chunkHeaderPos = recordOffsetOf(page, rid) + 1;
      final int size = Binary.INT_SERIALIZED_SIZE + Binary.LONG_SERIALIZED_SIZE + page.readInt(chunkHeaderPos);
      image[0] = new byte[size];
      page.readByteArray(chunkHeaderPos, image[0], 0, size);
      return 0L;
    });
    return image[0];
  }

  /** Replays {@code image} onto the slot of {@code rid} under {@code kind}, as a commit-time slot rebase would. */
  private boolean rebase(final LocalBucket bucket, final RID rid, final byte[] image, final byte kind) {
    return rebase(bucket, rid, image, image, kind);
  }

  /**
   * The general form, for the two kinds whose images DIFFER: a collapse starts from the head chunk ({@code baseBody})
   * and ends with the slot holding plain content ({@code body}).
   */
  private boolean rebase(final LocalBucket bucket, final RID rid, final byte[] body, final byte[] baseBody,
      final byte kind) {
    final boolean[] rebased = new boolean[1];
    onSlot(rid, page -> {
      rebased[0] = bucket.rebaseRecordOnPage(page, (int) (rid.getPosition() % bucket.getMaxRecordsInPage()), body,
          baseBody, kind);
      return 0L;
    });
    return rebased[0];
  }

  /** Placeholder pointers the bucket holds, so the rolled-back replays above are shown to have changed nothing. */
  private long placeholderRecordCount() {
    return (Long) bucketStats(TYPE).get("totalPlaceholderRecords");
  }

  /**
   * The fixture #6196 needs: a placeholder POINTER whose CONTENT record is itself a chunk chain. Both halves are
   * required - a page with a free tail of exactly zero (since #6149 the only shape that still produces a pointer) and
   * a value no page can host whole.
   */
  private RID placeholderWithChainedContent() {
    final RID[] placeholder = new RID[1];
    database.transaction(() -> {
      database.getSchema().createDocumentType(TYPE, 1).createProperty("v", Type.STRING);
      placeholder[0] = database.newDocument(TYPE).set("v", "p").save().getIdentity();
    });
    sealingRecord = sealFirstPage(TYPE);
    chunksOfTheSealingRecord = (Long) bucketStats(TYPE).get("totalChunks");

    database.transaction(() -> placeholder[0].asDocument(true).modify().set("v", HUGE).save());

    assertThat((Long) bucketStats(TYPE).get("totalPlaceholderRecords"))
        .as("the fixture must produce a placeholder pointer").isEqualTo(1L);
    return placeholder[0];
  }

  /** The RID of the CONTENT record the placeholder POINTER at {@code rid} references. */
  private RID contentRidOf(final RID rid) {
    final long[] pointer = new long[1];
    database.transaction(() -> pointer[0] = onSlot(rid, page -> {
      final int recordOffset = recordOffsetOf(page, rid);
      assertThat(page.readByte(recordOffset)).as("%s must be a placeholder pointer", rid)
          .isEqualTo(PLACEHOLDER_POINTER_MARKER);
      return page.readLong(recordOffset + 1);
    }));
    return new RID(rid.getBucketId(), pointer[0]);
  }

  /** The single byte the record's size marker is stored as - the whole of it for every marker used here. */
  private byte markerByteAt(final RID rid) {
    final long[] marker = new long[1];
    database.transaction(() -> marker[0] = onSlot(rid, page -> (long) page.readByte(recordOffsetOf(page, rid))));
    return (byte) marker[0];
  }

  /** Rewrites that byte, which is how the pre-fix shape is fabricated on a database this build wrote. */
  private void writeMarkerByteAt(final RID rid, final byte marker) {
    database.transaction(() -> onSlot(rid, page -> {
      page.writeByte(recordOffsetOf(page, rid), marker);
      return 0L;
    }));
  }

  /** The two shared counts, bound to this class's single type. */
  private long countRecords() {
    return countRecords(TYPE);
  }

  private long countRecordsHolding(final String value) {
    return countRecordsHolding(TYPE, value);
  }
}
