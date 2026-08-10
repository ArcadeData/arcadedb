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

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.schema.DocumentType;

import org.junit.jupiter.api.Test;

import static com.arcadedb.database.Binary.INT_SERIALIZED_SIZE;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for the placeholder-pointer counterpart of {@link BucketIteratorCorruptedSlotTest}, raised in
 * code review on #6015's fix: {@code currentPage.readLong(...)} resolving a {@code RECORD_PLACEHOLDER_POINTER}
 * slot's target position is the same page-bytes-only shape as the slot-table reads at the top of the loop (no
 * application/listener code runs there either), so a corrupted pointer field must be treated the same way -
 * skipped and counted via {@link BucketIterator#getSkippedRecordCount()} - rather than propagate and abort the
 * whole scan.
 * <p>
 * Two earlier attempts at this test (see #6016's review history) relied on a normal transactional commit to
 * write the corruption, and both failed: relocating a slot to an artificial far-away position gets "healed" by
 * {@code LocalBucket.compressPage()}'s hole-defragmentation, which runs unconditionally in
 * {@code TransactionContext.commit1stPhase} for every modified bucket page; and an in-place overwrite needed
 * more overflow margin than a real {@code LocalBucket} insert ever naturally leaves before the next record stops
 * fitting (a fixed reservation floor of several dozen bytes that a same-sized replacement record can never
 * close). This version sidesteps {@code compressPage} entirely: it writes the corrupted page straight to the
 * underlying file with {@link PaginatedComponentFile#write}, bypassing {@code TransactionContext.commit()} (and
 * therefore its unconditional compression pass) altogether, then reopens the database so the next read comes
 * from the corrupted file instead of a cached page - the same reopen {@code BrokenMultiPageRecordDeleteTest}
 * uses for its own on-disk corruption.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class BucketIteratorCorruptedPlaceholderPointerTest extends TestHelper {

  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    // This test deliberately injects on-disk corruption, which the post-test integrity check would (correctly) flag.
    return false;
  }

  @Test
  void corruptedPlaceholderPointerIsSkippedNotPropagated() throws Exception {
    final DocumentType type = database.getSchema().createDocumentType("CorruptedPlaceholderTest", 1);

    database.begin();
    database.newDocument("CorruptedPlaceholderTest").set("id", 1).save();
    database.newDocument("CorruptedPlaceholderTest").set("id", 2).save();
    database.commit();

    final LocalBucket bucket = (LocalBucket) type.getBuckets(false).getFirst();
    assertThat(bucket.getTotalPages()).as("both records must fit on a single page").isEqualTo(1);

    // Read the current page, relocate the SECOND record's slot to a placeholder marker positioned 4 bytes from
    // the page's content boundary, and write the result straight to the file - never going through
    // TransactionContext.commit(), so LocalBucket.compressPage()'s defragmentation never runs on this write and
    // cannot "heal" the relocation.
    database.begin();
    final MutablePage page = ((DatabaseInternal) database).getTransaction()
        .getPageToModify(new PageId((DatabaseInternal) database, bucket.getFileId(), 0), bucket.getPageSize(), false);

    // MutablePage.writeByte() itself refuses to write past the page's physical size, so the 9-byte marker must
    // fit EXACTLY within bounds (ending precisely at maxContentSize) for the write to succeed at all; the
    // subsequent 8-byte pointer read then starts exactly at maxContentSize, with zero bytes of slack remaining.
    final int corruptPosition = page.getMaxContentSize() - 9;
    final int bytesWritten = writeOverlongPlaceholderMarker(page, corruptPosition);
    page.writeUnsignedInt(LocalBucket.PAGE_RECORD_TABLE_OFFSET + INT_SERIALIZED_SIZE, corruptPosition);

    assertThat(corruptPosition + bytesWritten)
        .as("sanity check: the pointer read must actually overflow the page for this test to be meaningful")
        .isGreaterThan(page.getMaxContentSize() - 8);

    final PaginatedComponentFile file = (PaginatedComponentFile) ((DatabaseInternal) database).getFileManager()
        .getFile(bucket.getFileId());
    file.write(page);
    database.rollback(); // the raw write above already persisted the corruption; don't also run a normal commit over it

    reopenDatabase();
    final LocalBucket reopenedBucket = (LocalBucket) database.getSchema().getBucketByName(bucket.getName());

    final BucketIterator iterator = (BucketIterator) reopenedBucket.iterator();
    int count = 0;
    while (iterator.hasNext()) {
      iterator.next();
      count++;
    }

    assertThat(count).as("the uncorrupted record must still be returned, the scan must not abort").isEqualTo(1);
    assertThat(iterator.getSkippedRecordCount())
        .as("the corrupted placeholder pointer must be counted as skipped instead of aborting the scan")
        .isEqualTo(1);
  }

  /**
   * Writes a 9-byte denormalized LEB128 encoding of {@link LocalBucket#RECORD_PLACEHOLDER_POINTER} (-1) at
   * {@code position}: a minimal encoding would take 1 byte, but the decoder ({@code Binary.getUnsignedNumber()})
   * accepts any number of zero-contribution continuation bytes before the terminator, so this decodes to the
   * same value while consuming 9 bytes instead of 1. Returns the number of bytes written (9).
   */
  private static int writeOverlongPlaceholderMarker(final MutablePage page, final int position) {
    // zigzag(-1) == 1: low 7 bits = 1, continuation bit set.
    page.writeByte(position, (byte) 0x81);
    // Seven zero-contribution continuation bytes.
    for (int i = 1; i < 8; i++)
      page.writeByte(position + i, (byte) 0x80);
    // Terminator: no continuation bit, contributes nothing.
    page.writeByte(position + 8, (byte) 0x00);
    return 9;
  }
}
