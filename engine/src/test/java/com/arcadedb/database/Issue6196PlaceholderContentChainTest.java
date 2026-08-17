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
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PaginatedComponentFile;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

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
   * The whole life of such a record through the pointer: rewritten large (the chain is reused), shrunk back (the
   * chain shrinks and stays a chain, #6178), grown again, and finally deleted with the chain behind it.
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
    sealFirstPage(TYPE);
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

  private int recordOffsetOf(final MutablePage page, final RID rid) {
    final int slot = (int) (rid.getPosition() % bucketOf(TYPE).getMaxRecordsInPage());
    // PAGE_RECORD_TABLE_OFFSET == PAGE_RECORD_COUNT_IN_PAGE_OFFSET(0) + SHORT_SERIALIZED_SIZE; one uint per slot.
    return (int) page.readUnsignedInt(Binary.SHORT_SERIALIZED_SIZE + slot * Binary.INT_SERIALIZED_SIZE);
  }

  /** Runs {@code body} on the page holding {@code rid}, taken for modification so a write lands in the transaction. */
  private long onSlot(final RID rid, final PageAccess body) {
    final DatabaseInternal db = (DatabaseInternal) database;
    final int pageSize = ((PaginatedComponentFile) db.getFileManager().getFile(rid.getBucketId())).getPageSize();
    final int pageNumber = (int) (rid.getPosition() / bucketOf(TYPE).getMaxRecordsInPage());
    try {
      return body.apply(db.getTransaction().getPageToModify(new PageId(db, rid.getBucketId(), pageNumber), pageSize, false));
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  @FunctionalInterface
  private interface PageAccess {
    long apply(MutablePage page) throws Exception;
  }

  private Result checkDatabaseRow(final boolean fix) {
    try (final ResultSet rs = database.command("sql", fix ? "check database fix" : "check database")) {
      return rs.next();
    }
  }

  /** {@code CHECK DATABASE} folds the per-bucket warning lists into a set, so this reads it as the collection it is. */
  private static Collection<?> warningsOf(final Result row) {
    final Object warnings = row.getProperty("warnings");
    return warnings == null ? List.of() : (Collection<?>) warnings;
  }

  /**
   * Records a full SCAN returns. {@code count(@rid)} and not {@code count(*)}: the latter answers from the bucket's
   * cached counter without scanning a page, so it could not see the difference this test is about.
   */
  private long countRecords() {
    final long[] total = new long[1];
    database.transaction(() -> {
      try (final ResultSet rs = database.query("SQL", "select count(@rid) as c from " + TYPE)) {
        total[0] = ((Number) rs.next().getProperty("c")).longValue();
      }
    });
    return total[0];
  }

  /** Records a scan returns holding exactly {@code value} - the count that notices a content record handed out twice. */
  private long countRecordsHolding(final String value) {
    final long[] total = new long[1];
    database.transaction(() -> {
      try (final ResultSet rs = database.query("SQL", "select count(@rid) as c from " + TYPE + " where v = :v",
          Map.of("v", value))) {
        total[0] = ((Number) rs.next().getProperty("c")).longValue();
      }
    });
    return total[0];
  }
}
