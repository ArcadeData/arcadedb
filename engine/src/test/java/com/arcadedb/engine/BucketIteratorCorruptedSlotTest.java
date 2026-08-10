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
 * Regression test for the page-layer corruption case raised in code review on #6015's fix: the raw
 * {@code BasePage.readUnsignedInt()}/{@code readNumberAndSize()} calls that resolve a slot's record position and
 * size only ever touch page bytes (no application/listener code runs there), so a corrupted slot-table entry
 * landing outside the page's valid bounds must be treated like any other known-corrupt record - logged, counted
 * via {@link BucketIterator#getSkippedRecordCount()}, and skipped - rather than propagate and abort the whole
 * scan the way a bug in {@code AfterRecordReadListener}/application code correctly does after #6015's fix.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class BucketIteratorCorruptedSlotTest extends TestHelper {

  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    // This test deliberately injects on-disk corruption, which the post-test integrity check would (correctly) flag.
    return false;
  }

  @Test
  void corruptedSlotTableEntryIsSkippedNotPropagated() throws Exception {
    final DocumentType type = database.getSchema().createDocumentType("CorruptedSlotTest", 1);

    database.begin();
    database.newDocument("CorruptedSlotTest").set("id", 1).save();
    database.newDocument("CorruptedSlotTest").set("id", 2).save();
    database.commit();

    final LocalBucket bucket = (LocalBucket) type.getBuckets(false).getFirst();
    assertThat(bucket.pageCount.get()).as("both records must fit on a single page").isEqualTo(1);

    database.begin();
    final MutablePage page = ((DatabaseInternal) database).getTransaction()
        .getPageToModify(new PageId((DatabaseInternal) database, bucket.getFileId(), 0), bucket.getPageSize(), false);
    // POINT THE SECOND RECORD'S SLOT-TABLE ENTRY WELL PAST THE END OF THE PAGE: readNumberAndSize() will hit
    // Binary.position()'s bounds check and throw, simulating a torn write that corrupted the slot-table entry
    // itself rather than the record body (which is what SerializationException guards against).
    page.writeUnsignedInt(LocalBucket.PAGE_RECORD_TABLE_OFFSET + INT_SERIALIZED_SIZE, (long) bucket.getPageSize() * 1000);
    database.commit();

    final BucketIterator iterator = (BucketIterator) bucket.iterator();
    int count = 0;
    while (iterator.hasNext()) {
      iterator.next();
      count++;
    }

    assertThat(count).as("the uncorrupted record must still be returned, the scan must not abort").isEqualTo(1);
    assertThat(iterator.getSkippedRecordCount())
        .as("the corrupted slot must be counted as skipped instead of silently disappearing or aborting the scan")
        .isEqualTo(1);
  }
}
