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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8660 follow-up: a page whose record table is full cannot take another record, whatever its free tail is, and the
 * allocator drops it from the free-space map on first sight. The gather used to list it anyway (the free-space test looked at
 * bytes only), so a bucket of small records filled the map with entries that could never be used. The map drained one entry
 * per allocation and every refill was an unthrottled resume gather: a bulk UPDATE over 633K small vertices ran 27K gather
 * scans (5.7M page reads, 80s) where 8 are enough (3s).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8660GatherSkipsFullRecordTableTest extends TestHelper {
  private static final int RECORDS = 300_000;

  @Test
  void gatherDoesNotListPagesWithAFullRecordTable() throws Exception {
    createTinyRecords();
    reopenDatabase();
    final LocalBucket bucket = tinyBucket();

    // the premise: tiny records exhaust the record table long before the page's bytes
    assertThat(bucket.getTotalPages()).isGreaterThan(5);

    bucket.gatherPageStatistics();

    final DatabaseInternal databaseInternal = (DatabaseInternal) database;
    for (final int pageId : bucket.getListedFreeSpacePagesForTesting()) {
      final BasePage page = databaseInternal.getTransaction()
          .getPage(new PageId(databaseInternal, bucket.getFileId(), pageId), bucket.getPageSize());
      assertThat((int) page.readShort(LocalBucket.PAGE_RECORD_COUNT_IN_PAGE_OFFSET))
          .as("page %d is listed in the free-space map but its record table (%d slots) is full", pageId,
              bucket.getMaxRecordsInPage()).isLessThan(bucket.getMaxRecordsInPage());
    }
  }

  /**
   * Statistics persisted before a full record table was kept out of the map are restored by setPageStatistics as they
   * were written. A gather visiting such a page has to take the entry out, not only decline to add one.
   */
  @Test
  void aGatherDropsAStaleEntryForAPageThatCannotTakeARecord() {
    createTinyRecords();
    reopenDatabase();
    final LocalBucket bucket = tinyBucket();

    // page 0 is full by record count; a stale hint claims a few bytes of free tail
    bucket.setPageStatistics(new JSONArray().put(new JSONObject().put("id", 0).put("free", 1)));
    assertThat(bucket.getListedFreeSpacePagesForTesting()).containsExactly(0);

    bucket.gatherPageStatistics();

    assertThat(bucket.getListedFreeSpacePagesForTesting()).doesNotContain(0);
  }

  /**
   * The bulk update grows every record, so each one needs a new home and asks the allocator for space. With a map that
   * holds only usable pages that is a handful of gather scans for the whole statement; with the map full of pages that
   * cannot take a record it was one scan every ~20 allocations (hundreds here, tens of thousands on a real graph).
   * Counting scans needs no clock, so a stalled JVM cannot flip it.
   */
  @Test
  void aBulkGrowingUpdateOverSmallRecordsDoesNotRescanTheBucketPerAllocation() {
    createTinyRecords();
    reopenDatabase();
    final LocalBucket bucket = tinyBucket();
    final long scansBefore = bucket.getGatherScanCountForTesting();

    database.transaction(() -> database.command("sql", "UPDATE Tiny SET grown = 'a value that widens every record'"));

    final long scans = bucket.getGatherScanCountForTesting() - scansBefore;
    assertThat(database.countType("Tiny", false)).isEqualTo(RECORDS);
    assertThat(scans).as("gather scans during a bulk update of %d records over %d pages", RECORDS, bucket.getTotalPages())
        .isLessThanOrEqualTo(MAX_GATHER_SCANS);
  }

  private static final long MAX_GATHER_SCANS = 50;

  private void createTinyRecords() {
    database.getSchema().createDocumentType("Tiny", 1);
    database.begin();
    for (int i = 0; i < RECORDS; i++) {
      final MutableDocument d = database.newDocument("Tiny").set("i", i);
      d.save();
      if ((i + 1) % 5_000 == 0) {
        database.commit();
        database.begin();
      }
    }
    database.commit();
  }

  private LocalBucket tinyBucket() {
    return (LocalBucket) database.getSchema().getType("Tiny").getBuckets(false).get(0);
  }
}
