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

import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The three things {@code CHECK DATABASE} could not say about a bucket, all found while building the #6196 fixtures.
 * <ul>
 * <li><b>#6292</b> - a placeholder POINTER whose CONTENT is gone was never followed, so {@code count(@rid)} (a scan,
 * which resolves the pointer to nothing and skips the slot) and {@code count(*)} (the cached counter, for which a
 * pointer IS a record) disagreed for ever while the checker reported the database clean.</li>
 * <li><b>#6293</b> - a record the FIX deleted during the run was counted BOTH in {@code totalDeletedRecords} and in
 * the category tally it had held before, because the slot was classified first and repaired afterwards. One run
 * described the same record twice, and only a second run told the truth.</li>
 * <li><b>#6294</b> - the continuation chunks a force-delete leaves behind were reclaimed by nothing, though three
 * comments promised compaction or a database check would. {@code compressPage} re-flows LIVE slots and an orphaned
 * chunk still has one, and {@code check()} counted it and moved on.</li>
 * </ul>
 * The fixtures below fabricate the physical shapes on a database this build wrote, because the writers that used to
 * produce them are gone: a placeholder needs the zero-free-tail page #6149 left as the last fallback, and an orphan
 * needs a chain broken behind the record's back.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6292CheckDatabaseReconciliationTest extends BucketPageLayoutTestSupport {
  private static final String TYPE = "Reconciled";
  /** 200 KB fits no page whole, so the content record created behind a pointer spills into a chain of its own. */
  private static final String HUGE = "h".repeat(200 * 1024);

  /**
   * The disagreement as reported: a pointer left aimed at a slot that is not there any more. The record is invisible
   * to every query and still counted by {@code count(*)}, and before #6292 nothing ever noticed - CHECK DATABASE
   * counted the pointer as a placeholder and moved on without following it.
   */
  @Test
  void aDanglingPlaceholderPointerIsReportedAndRemoved() {
    final RID placeholder = placeholderWithPlainContent();
    final RID content = contentRidOf(placeholder);

    final long recordsBefore = countRecords(TYPE);
    assertThat(countRecordsFromCounter(TYPE)).as("the two counts must agree before anything is broken")
        .isEqualTo(recordsBefore);

    // An interrupted repair, or the content record lost to physical corruption: the slot goes, the pointer stays.
    freeSlotBehindItsBack(content);

    assertThat(countRecords(TYPE)).as("a scan can no longer resolve the pointer").isEqualTo(recordsBefore - 1);
    assertThat(countRecordsFromCounter(TYPE)).as("but the counter still counts the pointer slot")
        .isEqualTo(recordsBefore);

    final Result found = checkDatabaseRow(false);
    assertThat(numberProperty(found, "danglingPlaceholderPointers"))
        .as("the checker must follow the pointer: " + found.toJSON()).isEqualTo(1L);
    assertThat(numberProperty(found, "totalErrors")).as("and call it an error: " + found.toJSON()).isEqualTo(1L);
    assertThat(warningsOf(found).toString()).as("naming both ends of it: " + found.toJSON())
        .contains(placeholder.toString()).contains(content.toString());
    assertThat(numberProperty(found, "danglingPlaceholderPointersFixed"))
        .as("nothing is repaired without FIX: " + found.toJSON()).isZero();

    final Result fixed = checkDatabaseRow(true);
    assertThat(numberProperty(fixed, "danglingPlaceholderPointersFixed")).as("FIX removes it: " + fixed.toJSON())
        .isEqualTo(1L);
    assertThat((Collection<Object>) fixed.<Collection<Object>>getProperty("deletedRecordsAfterFix"))
        .as("the pointer is the record's identity, so it is what the report names: " + fixed.toJSON())
        .containsExactly(placeholder);

    assertThat(countRecords(TYPE)).isEqualTo(recordsBefore - 1);
    assertThat(countRecordsFromCounter(TYPE)).as("and the two counts agree again").isEqualTo(recordsBefore - 1);

    final Result again = checkDatabaseRow(false);
    assertThat(numberProperty(again, "totalErrors")).as("with nothing left to find: " + again.toJSON()).isZero();
  }

  /**
   * The corrupted-pointer form of the same defect, and the reason the repair frees the pointer SLOT rather than going
   * through the ordinary delete: a pointer that names an unrelated LIVE record would have that record deleted with it.
   */
  @Test
  void repairingACorruptedPointerDoesNotDeleteWhateverItNowNames() {
    final RID placeholder = placeholderWithPlainContent();
    final RID content = contentRidOf(placeholder);

    // An ordinary record of the same bucket, which the corrupted pointer is then aimed at.
    final RID[] innocent = new RID[1];
    database.transaction(() -> innocent[0] = database.newDocument(TYPE).set("v", "innocent").save().getIdentity());

    freeSlotBehindItsBack(content);
    database.transaction(() -> onSlot(placeholder, page -> {
      // [marker:1][contentPosition:long]
      page.writeLong(recordOffsetOf(page, placeholder) + 1, innocent[0].getPosition());
      return 0L;
    }));

    final Result fixed = checkDatabaseRow(true);
    assertThat(numberProperty(fixed, "danglingPlaceholderPointersFixed")).as("the pointer goes: " + fixed.toJSON())
        .isEqualTo(1L);
    database.transaction(
        () -> assertThat(innocent[0].asDocument(true).getString("v")).as("and the record it named stays")
            .isEqualTo("innocent"));

    checkDatabase();
  }

  /**
   * A pointer with a healthy content record behind it - of either shape, a plain one or a chain - is not touched. The
   * negative control without which the two tests above would pass on a checker that reported every pointer.
   */
  @Test
  void aHealthyPlaceholderIsNotReported() {
    final RID placeholder = placeholderWithPlainContent();

    Result row = checkDatabaseRow(false);
    assertThat(numberProperty(row, "danglingPlaceholderPointers"))
        .as("a plain content record behind a pointer is not dangling: " + row.toJSON()).isZero();
    assertThat(numberProperty(row, "totalErrors")).as(row.toJSON().toString()).isZero();

    // And again once the content has outgrown its own page and become a chain of its own.
    database.transaction(() -> placeholder.asDocument(true).modify().set("v", HUGE).save());
    row = checkDatabaseRow(false);
    assertThat(numberProperty(row, "danglingPlaceholderPointers"))
        .as("nor is a content record stored as a chain: " + row.toJSON()).isZero();
    assertThat(numberProperty(row, "orphanedChunks")).as("and its chunks are reachable: " + row.toJSON()).isZero();
    assertThat(numberProperty(row, "totalErrors")).as(row.toJSON().toString()).isZero();

    checkDatabase();
  }

  /**
   * The boundary of the marker namespace, on the one path where getting it wrong deletes a live record.
   * <p>
   * A content record stores its size NEGATED, and {@code RECORD_PLACEHOLDER_CONTENT} (-5) is where the sizes stop and
   * the markers begin. Every classification site excludes it with a strict {@code <}, so a content record of exactly
   * {@code MINIMUM_RECORD_SIZE} bytes would be read as something that is not content at all - and since #6292 that
   * answer is acted on: {@code FIX} would free the pointer to a record that is still there.
   * <p>
   * Measured, not assumed: the smallest document this serializer produces is 6 bytes, so the boundary value is not
   * reachable today, and the padding now pushes a content body past it in any case. This shrinks a placeholder's
   * content as far as a document can go - every property removed - and asserts the pointer survives it. Should a
   * future serializer shave those bytes, this fails here rather than in somebody's database (PR review on #6299).
   */
  @Test
  void theSmallestPossibleContentRecordIsNotCalledDangling() {
    final RID placeholder = placeholderWithPlainContent();
    final long recordsBefore = countRecords(TYPE);

    database.transaction(() -> {
      final MutableDocument doc = placeholder.asDocument(true).modify();
      doc.remove("v");
      doc.save();
    });

    final Result row = checkDatabaseRow(true);
    assertThat(numberProperty(row, "danglingPlaceholderPointers"))
        .as("a content record at the smallest size a document has is still content: " + row.toJSON()).isZero();
    assertThat(numberProperty(row, "totalErrors")).as(row.toJSON().toString()).isZero();
    assertThat(numberProperty(row, "totalSurrogateRecords"))
        .as("and it is still counted as the surrogate it is: " + row.toJSON()).isEqualTo(1L);

    assertThat(countRecords(TYPE)).as("the record is still there, and exactly once").isEqualTo(recordsBefore);
    assertThat(countRecordsFromCounter(TYPE)).as("with both counts still agreeing").isEqualTo(recordsBefore);
    database.transaction(
        () -> assertThat(placeholder.asDocument(true).getString("v")).as("and still readable through its pointer")
            .isNull());

    checkDatabase();
  }

  /**
   * #6294: the chunks a broken chain cuts off are reclaimed, and until #6294 nothing ever collected them - not
   * compaction, which re-flows a page's LIVE slots and an orphaned chunk still has one, and not this checker, which
   * counted a NEXT_CHUNK slot and moved on.
   */
  @Test
  void orphanedChunksAreFoundAndReclaimed() {
    final RID[] big = new RID[1];
    database.transaction(() -> {
      database.getSchema().createDocumentType(TYPE, 1).createProperty("v", Type.STRING);
      big[0] = database.newDocument(TYPE).set("v", HUGE).save().getIdentity();
    });

    final long chunksBefore = (Long) bucketStats(TYPE).get("totalChunks");
    assertThat(chunksBefore).as("the fixture must really be a chain").isPositive();

    // Cut the chain off at the head, which is what a corrupted or half-written continuation pointer does.
    database.transaction(() -> onSlot(big[0], page -> {
      // [marker:1][chunkSize:int][nextChunkPointer:long][content...]
      page.writeLong(recordOffsetOf(page, big[0]) + 1 + Binary.INT_SERIALIZED_SIZE, Integer.MAX_VALUE);
      return 0L;
    }));

    final Result found = checkDatabaseRow(false);
    assertThat(numberProperty(found, "orphanedChunks")).as("every cut-off chunk must be found: " + found.toJSON())
        .isEqualTo(chunksBefore);
    assertThat(numberProperty(found, "orphanedChunksReclaimed")).as("but not reclaimed without FIX: " + found.toJSON())
        .isZero();
    assertThat((Long) bucketStats(TYPE).get("totalChunks")).as("nor freed").isEqualTo(chunksBefore);

    final Result fixed = checkDatabaseRow(true);
    assertThat(numberProperty(fixed, "orphanedChunksReclaimed")).as("FIX reclaims them: " + fixed.toJSON())
        .isEqualTo(chunksBefore);

    final Map<String, Object> layout = bucketStats(TYPE);
    assertThat((Long) layout.get("totalChunks")).as("and the space really goes back: " + layout).isZero();
    assertThat((Long) layout.get("totalMultiPageRecords")).as("with the broken record gone: " + layout).isZero();

    final Result again = checkDatabaseRow(false);
    assertThat(numberProperty(again, "totalErrors")).as("nothing is left for a second run: " + again.toJSON()).isZero();
    assertThat(numberProperty(again, "orphanedChunks")).as(again.toJSON().toString()).isZero();
  }

  /**
   * The negative control the sweep cannot do without: a bucket full of healthy chains, where every chunk is reachable
   * from the head that owns it. A mark phase with a hole in it would delete live data here.
   */
  @Test
  void noChunkOfAHealthyBucketIsCalledAnOrphan() {
    final RID[] chained = createChunkedRecords(TYPE);

    final Map<String, Object> layout = bucketStats(TYPE);
    assertThat((Long) layout.get("totalChunks")).as("the fixture must really have chains: " + layout).isPositive();

    final Result row = checkDatabaseRow(true);
    assertThat(numberProperty(row, "orphanedChunks")).as("no live chunk may be called an orphan: " + row.toJSON())
        .isZero();
    assertThat(numberProperty(row, "orphanedChunksReclaimed")).as(row.toJSON().toString()).isZero();
    assertThat(numberProperty(row, "totalErrors")).as(row.toJSON().toString()).isZero();

    database.transaction(() -> {
      for (int i = 0; i < chained.length; i++)
        assertThat(chained[i].asDocument(true).getString("payload")).isEqualTo(payload(i, 'x'));
    });
    checkDatabase();
  }

  /**
   * #6293: the FIX run must describe the database it LEAVES, not the one it found with two fields patched. A record it
   * deleted used to be counted in {@code totalDeletedRecords} AND under {@code totalMultiPageRecords}, so the run
   * disagreed with a re-run on the very numbers an operator diffs across the two.
   */
  @Test
  void aRecordTheFixDeletedIsNotAlsoCountedInItsCategory() {
    final RID[] big = new RID[1];
    database.transaction(() -> {
      database.getSchema().createDocumentType(TYPE, 1).createProperty("v", Type.STRING);
      database.newDocument(TYPE).set("v", "plain").save();
      big[0] = database.newDocument(TYPE).set("v", HUGE).save().getIdentity();
    });

    database.transaction(() -> onSlot(big[0], page -> {
      page.writeLong(recordOffsetOf(page, big[0]) + 1 + Binary.INT_SERIALIZED_SIZE, Integer.MAX_VALUE);
      return 0L;
    }));

    final Result fixed = checkDatabaseRow(true);
    assertThat((Collection<?>) fixed.getProperty("deletedRecordsAfterFix")).as(fixed.toJSON().toString()).hasSize(1);
    assertThat(numberProperty(fixed, "totalMultiPageRecords"))
        .as("the record it deleted is not also a multi-page record it found: " + fixed.toJSON()).isZero();

    // The proof that the run described the state it left: a re-run of the same query has nothing to correct.
    final Result again = checkDatabaseRow(false);
    for (final String tally : new String[] { "totalAllocatedRecords", "totalActiveRecords", "totalMultiPageRecords",
        "totalPlaceholderRecords", "totalSurrogateRecords", "totalChunks" })
      assertThat(numberProperty(fixed, tally)).as("%s: the FIX run and a re-run must agree - %s vs %s", tally,
          fixed.toJSON(), again.toJSON()).isEqualTo(numberProperty(again, tally));

    assertThat(numberProperty(again, "totalErrors")).as(again.toJSON().toString()).isZero();
  }

  /**
   * The fixture every placeholder test needs: a page with a free tail of exactly ZERO, which since #6149 is the only
   * shape that still makes a record spill into a POINTER instead of a chunk chain.
   */
  private RID placeholderWithPlainContent() {
    final RID[] placeholder = new RID[1];
    database.transaction(() -> {
      database.getSchema().createDocumentType(TYPE, 1).createProperty("v", Type.STRING);
      placeholder[0] = database.newDocument(TYPE).set("v", "p").save().getIdentity();
    });
    sealFirstPage(TYPE);

    // Big enough that page 0 - which has no free tail at all - cannot host it, small enough for a page of its own.
    database.transaction(() -> placeholder[0].asDocument(true).modify().set("v", "c".repeat(16 * 1024)).save());

    assertThat((Long) bucketStats(TYPE).get("totalPlaceholderRecords"))
        .as("the fixture must produce a placeholder pointer").isEqualTo(1L);
    return placeholder[0];
  }

  /** The RID of the CONTENT record the placeholder POINTER at {@code rid} references. */
  private RID contentRidOf(final RID rid) {
    final long[] pointer = new long[1];
    database.transaction(() -> pointer[0] = onSlot(rid, page -> page.readLong(recordOffsetOf(page, rid) + 1)));
    return new RID(rid.getBucketId(), pointer[0]);
  }
}
