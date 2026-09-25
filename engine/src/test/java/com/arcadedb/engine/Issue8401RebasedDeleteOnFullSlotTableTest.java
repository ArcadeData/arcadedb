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

import com.arcadedb.database.BucketPageLayoutTestSupport;
import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8401: the disjoint-slot merge of a record DELETE ({@code rebaseRecordDeleteOnPage}) measured the page's free
 * tail through the allocator's {@code getFreeSpaceInPage}, which gives up without measuring anything on a page whose
 * slot table is full - it only has to tell the allocator "look elsewhere" - and leaves the free space at its -1
 * default. That -1 went to the free-space statistics as if it were a measurement, tripping the {@code
 * updatePageStatistics} tripwire under {@code -ea} (and failing the commit with it). Small records reach that shape
 * routinely: 2048 edges of ~14 bytes fill the slot table of a 64 KB page with half of its bytes still free.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8401RebasedDeleteOnFullSlotTableTest extends BucketPageLayoutTestSupport {
  private static final String TYPE = "Tiny";

  @Test
  void aRebasedDeleteOnAPageWithAFullSlotTableReportsTheRealFreeTail() {
    database.transaction(() -> database.getSchema().createDocumentType(TYPE, 1).createProperty("v", Type.STRING));
    final LocalBucket bucket = bucketOf(TYPE);
    final int slots = bucket.getMaxRecordsInPage();

    final RID[] rids = new RID[slots];
    database.transaction(() -> {
      for (int i = 0; i < slots; i++)
        rids[i] = database.newDocument(TYPE).set("v", "a").save().getIdentity();
    });

    // The premise, proved rather than assumed: page 0 has run out of SLOTS while it still has plenty of BYTES.
    assertThat(rids[slots - 1].getPosition()).as("every record must be on page 0").isEqualTo(slots - 1L);
    final int freeTailBefore = freeTailOfFirstPage(bucket);
    assertThat(freeTailBefore).as("page 0 must still have free bytes, it is the slot table that is full").isGreaterThan(1024);

    final RID deleted = rids[10];
    final RID neighbour = rids[20];

    database.begin();
    deleted.asDocument(true).delete();

    // A concurrent commit to ANOTHER record of page 0 makes this transaction's page image stale, so its commit has to
    // replay the delete onto the newer page: rebaseRecordDeleteOnPage.
    inAnotherThread(() -> database.transaction(() -> neighbour.asDocument(true).modify().set("v", "b").save()));

    database.commit();

    database.transaction(() -> {
      assertThat(database.existsRecord(deleted)).as("the delete must have been merged, not lost").isFalse();
      assertThat(neighbour.asDocument(true).getString("v")).as("the concurrent update must survive").isEqualTo("b");
    });

    final int freeTailAfter = freeTailOfFirstPage(bucket);
    assertThat(freeTailAfter).as("deleting a record never takes bytes away from the free tail").isGreaterThanOrEqualTo(freeTailBefore);
    final int hint = bucket.getFreeSpaceHintForPage(0);
    assertThat(hint).as("the statistics must hold the free tail the page really has, or nothing").isIn(-1, freeTailAfter);

    checkDatabase();
  }

  /**
   * The same short-circuit, in the other place that borrowed the allocator's analysis as a measurement: with
   * {@code bucketWipeOutOnDelete} (the default) the compaction a delete triggers zeroes the free tail it leaves behind,
   * so the bytes the defragmentation moved down are not left in the file a second time. On a page with a full slot
   * table the wipe measured nothing and wiped nothing, leaving a stale copy of the records past the new end.
   */
  @Test
  void theCompactionAfterADeleteWipesTheFreeTailOfAPageWithAFullSlotTable() {
    database.transaction(() -> database.getSchema().createDocumentType(TYPE, 1).createProperty("v", Type.STRING));
    final LocalBucket bucket = bucketOf(TYPE);
    final int slots = bucket.getMaxRecordsInPage();

    final RID[] rids = new RID[slots];
    database.transaction(() -> {
      for (int i = 0; i < slots; i++)
        rids[i] = database.newDocument(TYPE).set("v", "wipe-me-" + i).save().getIdentity();
    });
    assertThat(rids[slots - 1].getPosition()).as("every record must be on page 0").isEqualTo(slots - 1L);

    // A record in the MIDDLE: the compaction closes its hole by moving everything after it down, which is what leaves
    // the old copy of the page's last bytes past the new content end.
    database.transaction(() -> rids[slots / 2].asDocument(true).delete());

    database.transaction(() -> {
      try {
        final BasePage page = ((DatabaseInternal) database).getTransaction()
            .getPage(new PageId(database, bucket.getFileId(), 0), bucket.getPageSize());
        final int contentEnd = page.getMaxContentSize() - freeTailOf(page);
        for (int offset = contentEnd; offset < page.getMaxContentSize(); offset++)
          assertThat(page.readByte(offset)).as("free tail byte at offset %d of %d must be wiped", offset, page.getMaxContentSize())
              .isZero();
      } catch (final IOException e) {
        throw new AssertionError(e);
      }
    });

    checkDatabase();
  }

  /** The free tail of page 0 derived from the record table, independently of any statistics. */
  private int freeTailOfFirstPage(final LocalBucket bucket) {
    final int[] tail = new int[1];
    database.transaction(() -> {
      try {
        tail[0] = freeTailOf(((DatabaseInternal) database).getTransaction()
            .getPage(new PageId(database, bucket.getFileId(), 0), bucket.getPageSize()));
      } catch (final IOException e) {
        throw new AssertionError(e);
      }
    });
    return tail[0];
  }

  /** Free tail of a page derived from its record table; every record of these fixtures is a plain one. */
  private static int freeTailOf(final BasePage page) {
    final int records = page.readShort(LocalBucket.PAGE_RECORD_COUNT_IN_PAGE_OFFSET);
    int contentEnd = 0;
    for (int i = 0; i < records; i++) {
      final int offset = (int) page.readUnsignedInt(LocalBucket.PAGE_RECORD_TABLE_OFFSET + i * Binary.INT_SERIALIZED_SIZE);
      if (offset == 0)
        continue;
      // A plain record's footprint is its size varint plus the size it declares
      final long[] size = page.readNumberAndSize(offset);
      contentEnd = Math.max(contentEnd, (int) (offset + size[1] + size[0]));
    }
    return page.getMaxContentSize() - contentEnd;
  }
}
