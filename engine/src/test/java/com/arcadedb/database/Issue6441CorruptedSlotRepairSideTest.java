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
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PaginatedComponentFile;
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #6441: which half of {@code LocalBucket.getOrderedRecordsInPage} is allowed to free a slot it cannot make sense
 * of.
 * <p>
 * The walk has two callers and they are the two halves of one contract: {@code compressPageInternal} owns the page
 * for writing and is where a repair belongs, and {@code gatherPageStatistics} is a pure scan reached from an
 * ordinary allocation over pages it has not written. A {@code readOnly} flag guarded the two slot-freeing branches
 * in OPPOSITE directions, so the invalid-size slot was freed by the scan - which reached
 * {@code getPageToModify} and enlisted an unrelated page into whatever transaction happened to be inserting a
 * record, a write that vanished on a rollback and deleted somebody else's record on a commit - and walked past by
 * the compression, the one place that could have repaired it for good.
 * <p>
 * The two tests below are each other's proof: the fixture is only known to build a genuinely invalid-size slot
 * because the compression frees it, and freeing it is only known to be the compression's doing because the scan
 * leaves it alone.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6441CorruptedSlotRepairSideTest extends BucketPageLayoutTestSupport {
  private static final String TYPE = "Slotted";

  /** How many 8 KB records to insert: enough for the statistics scan, which skips the last two pages, to reach page 0. */
  private static final int RECORDS = 48;

  /** A declared record size no page can hold - the shape the walk rejects. Encodes in 3 varint bytes. */
  private static final long IMPOSSIBLE_RECORD_SIZE = 1_000_000L;

  /** Every test here leaves one deliberately unreadable record behind, so the blanket end-of-test check would fire. */
  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    return false;
  }

  /**
   * The read-only half. A statistics scan meeting a corrupted slot must report it and move on: no page enlisted in
   * the transaction that happens to be running the scan, and the slot still where it was afterwards.
   */
  @Test
  void theStatisticsScanReportsTheCorruptedSlotAndTouchesNothing() throws IOException {
    final RID corrupted = corruptOnPageZero()[1];

    final long entryBefore = slotEntryOf(corrupted);
    assertThat(entryBefore).as("the fixture must leave the slot still pointing at the record").isPositive();

    final List<String> logged = new CopyOnWriteArrayList<>();
    final int[] modifiedPages = new int[1];
    final Logger original = LogManager.instance().getLogger();
    LogManager.instance().setLogger(new CapturingLogger(logged, original));
    try {
      database.transaction(() -> {
        bucketOf(TYPE).gatherPageStatistics();
        modifiedPages[0] = ((DatabaseInternal) database).getTransaction().getModifiedPages();
      });
    } finally {
      LogManager.instance().setLogger(original);
    }

    // Load-bearing: without it every assertion below would also hold for a scan that never reached page 0.
    final List<String> invalid = logged.stream().filter(l -> l.contains("Invalid record size")).toList();
    assertThat(invalid).as("the scan must have walked the corrupted slot (captured=%s)", logged).isNotEmpty();

    assertThat(modifiedPages[0])
        .as("a statistics scan must enlist no page in the transaction that runs it")
        .isZero();
    assertThat(slotEntryOf(corrupted)).as("the scan must leave the slot exactly as it found it").isEqualTo(entryBefore);

    assertThat(invalid).allSatisfy(l -> assertThat(l).as("a scan deletes nothing, so it must not claim it did")
        .contains("skipping record"));
  }

  /**
   * The write half. {@code CHECK DATABASE COMPRESS} exists to clean pages up, and this was the one slot shape it
   * walked past - the record was excluded from the packing, so the defrag closed the gap around it and left the
   * slot-table entry pointing into whatever moved on top of it.
   */
  @Test
  void checkDatabaseCompressFreesTheCorruptedSlot() throws IOException {
    final RID corrupted = corruptOnPageZero()[1];
    assertThat(slotEntryOf(corrupted)).isPositive();

    database.command("sql", "check database compress").close();

    assertThat(slotEntryOf(corrupted))
        .as("the compression is the one caller that owns the page for writing: it must free the slot")
        .isZero();

    // The defrag packs the page around the hole the corrupted record leaves: every other record has to survive it.
    assertThat(countRecords(TYPE)).isEqualTo(RECORDS - 1L);

    // #6441 follow-up: the repair writes the record-table entry directly, bypassing the transaction's insert/delete
    // bookkeeping - exactly like every corrupt-record deletion in deleteRecordInternal. A record that was live and
    // COUNTED (all of these were, this session) must not leave the O(1) cached counter one too high forever.
    assertThat(countRecordsFromCounter(TYPE))
        .as("the cached record counter must not go stale when the compression frees a live record's slot")
        .isEqualTo(RECORDS - 1L);
  }

  /**
   * The same repair on the ordinary path: any commit that touches the page runs the compression on it, so a
   * corrupted slot does not wait for an operator to notice it.
   */
  @Test
  void anOrdinaryCommitOnThePageFreesTheCorruptedSlot() throws IOException {
    final RID[] rids = corruptOnPageZero();
    final RID corrupted = rids[1];
    final RID neighbour = rids[2];

    assertThat(slotEntryOf(corrupted)).isPositive();

    // Re-resolved against the reopened database: the RID collected before the corruption still points at the
    // closed instance.
    database.transaction(() -> database.lookupByRID(new RID(neighbour.getBucketId(), neighbour.getPosition()), true)
        .asDocument().modify().set("v", "short").save());

    assertThat(slotEntryOf(corrupted))
        .as("the commit-time compression of the page must free the slot")
        .isZero();
    assertThat(countRecords(TYPE)).isEqualTo(RECORDS - 1L);
    assertThat(countRecordsFromCounter(TYPE))
        .as("the cached record counter must not go stale when an ordinary commit's compression frees the slot")
        .isEqualTo(RECORDS - 1L);
  }

  /**
   * Fills several pages, then turns the second record of page 0 into a slot whose declared size no page can hold.
   *
   * @return every RID inserted; the one at index 1 is the record that is now unreadable.
   */
  private RID[] corruptOnPageZero() throws IOException {
    final RID[] rids = new RID[RECORDS];
    database.transaction(() -> {
      database.getSchema().createDocumentType(TYPE, 1).createProperty("v", Type.STRING);
      final String filler = "f".repeat(8 * 1024);
      for (int i = 0; i < RECORDS; i++)
        rids[i] = database.newDocument(TYPE).set("v", filler).save().getIdentity();
    });

    final RID corrupted = rids[1];
    assertThat(pageNumberOf(corrupted)).as("the corrupted slot must sit on page 0, the one the scan reaches").isZero();
    assertThat(totalPagesOf(corrupted)).as("the statistics scan skips the last two pages of a bucket").isGreaterThan(3);

    corruptRecordSizeOnDisk(corrupted);
    return rids;
  }

  /**
   * Overwrites the size marker of {@code rid} with one no page can hold, in the CLOSED file.
   * <p>
   * Not through a transaction on purpose: committing one compresses every page it modified, which is exactly the
   * code path under test - the fixture would repair itself before a single assertion ran.
   */
  private void corruptRecordSizeOnDisk(final RID rid) throws IOException {
    final DatabaseInternal db = (DatabaseInternal) database;
    final PaginatedComponentFile file = (PaginatedComponentFile) db.getFileManager().getFile(rid.getBucketId());
    final int pageSize = file.getPageSize();
    final String filePath = file.getFilePath();
    final int pageNumber = pageNumberOf(rid);

    final int recordOffset = (int) readOnPage(rid, page -> recordOffsetOf(page, rid));
    final int markerLength = (int) readOnPage(rid, page -> page.readNumberAndSize(recordOffset)[1]);

    final Binary marker = new Binary(16);
    marker.putNumber(IMPOSSIBLE_RECORD_SIZE);
    marker.flip();
    final byte[] bytes = marker.toByteArray();
    // A longer marker would spill into the neighbouring record and corrupt a second slot as a side effect.
    assertThat(bytes.length).as("the fabricated marker must fit the one it replaces").isLessThanOrEqualTo(markerLength);

    database.close();
    try (final RandomAccessFile raf = new RandomAccessFile(filePath, "rw")) {
      raf.seek((long) pageNumber * pageSize + BasePage.PAGE_HEADER_SIZE + recordOffset);
      raf.write(bytes);
    }
    database = factory.open();
  }

  /** The record-table entry of {@code rid}: its content offset, or 0 once the slot has been freed. */
  private long slotEntryOf(final RID rid) {
    return readOnPage(rid, page -> page.readUnsignedInt(
        Binary.SHORT_SERIALIZED_SIZE + slotOf(rid) * Binary.INT_SERIALIZED_SIZE));
  }

  /**
   * Runs {@code body} on the page holding {@code rid} WITHOUT taking it for modification: a
   * {@code getPageToModify} here would enlist the page and have the commit compress it, i.e. run the very repair
   * these tests are measuring.
   */
  private long readOnPage(final RID rid, final PageRead body) {
    final DatabaseInternal db = (DatabaseInternal) database;
    final int pageSize = ((PaginatedComponentFile) db.getFileManager().getFile(rid.getBucketId())).getPageSize();
    final int pageNumber = pageNumberOf(rid);
    final long[] result = new long[1];
    database.transaction(() -> {
      try {
        result[0] = body.apply(db.getTransaction().getPage(new PageId(db, rid.getBucketId(), pageNumber), pageSize));
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    });
    return result[0];
  }

  /** Content offset of {@code rid}'s slot on a read-only page - the {@link BasePage} twin of the support class one. */
  private int recordOffsetOf(final BasePage page, final RID rid) {
    return (int) page.readUnsignedInt(Binary.SHORT_SERIALIZED_SIZE + slotOf(rid) * Binary.INT_SERIALIZED_SIZE);
  }

  private int pageNumberOf(final RID rid) {
    return (int) (rid.getPosition() / bucketOf(rid).getMaxRecordsInPage());
  }

  private int slotOf(final RID rid) {
    return (int) (rid.getPosition() % bucketOf(rid).getMaxRecordsInPage());
  }

  private int totalPagesOf(final RID rid) {
    return bucketOf(rid).getTotalPages();
  }

  private LocalBucket bucketOf(final RID rid) {
    return (LocalBucket) database.getSchema().getBucketById(rid.getBucketId());
  }

  @FunctionalInterface
  private interface PageRead {
    long apply(BasePage page) throws Exception;
  }

  /** Records every WARNING-or-worse line, so a log the engine stopped emitting fails a test instead of nothing. */
  private static final class CapturingLogger implements Logger {
    private final List<String> messages;
    private final Logger       delegate;

    CapturingLogger(final List<String> messages, final Logger delegate) {
      this.messages = messages;
      this.delegate = delegate;
    }

    private void capture(final Level level, final String message, final Object... args) {
      if (message == null || level.intValue() < Level.WARNING.intValue())
        return;
      String formatted = message;
      if (args != null && args.length > 0) {
        try {
          formatted = message.formatted(args);
        } catch (final Exception ignored) {
          // Fall back to the raw template - good enough for the substring matching the tests do.
        }
      }
      messages.add(formatted);
    }

    @Override
    public void log(final Object requester, final Level level, final String message, final Throwable exception,
        final String context, final Object arg1, final Object arg2, final Object arg3, final Object arg4,
        final Object arg5, final Object arg6, final Object arg7, final Object arg8, final Object arg9,
        final Object arg10, final Object arg11, final Object arg12, final Object arg13, final Object arg14,
        final Object arg15, final Object arg16, final Object arg17) {
      capture(level, message, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14,
          arg15, arg16, arg17);
      delegate.log(requester, level, message, exception, context, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9,
          arg10, arg11, arg12, arg13, arg14, arg15, arg16, arg17);
    }

    @Override
    public void log(final Object requester, final Level level, final String message, final Throwable exception,
        final String context, final Object... args) {
      capture(level, message, args);
      delegate.log(requester, level, message, exception, context, args);
    }

    @Override
    public void flush() {
      delegate.flush();
    }
  }
}
