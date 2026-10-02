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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.utility.IntIntHashMap;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8660 follow-up: a page whose record table is full cannot take another record, whatever its free tail is, and the
 * allocator drops it from the free-space map on first sight. The gather used to list it anyway (the free-space test looked at
 * bytes only), so a bucket of small records filled the map with entries that could never be used. The map drained one entry
 * per allocation and every refill was an unthrottled resume gather: a bulk UPDATE over 633K small vertices spent 80s there
 * (3s once the gather agrees with the allocator).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8660GatherSkipsFullRecordTableTest extends TestHelper {
  private static final int RECORDS = 40_000;

  @Test
  void gatherDoesNotListPagesWithAFullRecordTable() throws Exception {
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

    reopenDatabase();
    final LocalBucket bucket = (LocalBucket) database.getSchema().getType("Tiny").getBuckets(false).get(0);

    // the premise: tiny records exhaust the record table long before the page's bytes
    assertThat(bucket.getTotalPages()).isGreaterThan(5);

    bucket.gatherPageStatistics();

    final Field field = LocalBucket.class.getDeclaredField("freeSpaceInPages");
    field.setAccessible(true);
    final IntIntHashMap freeSpaceInPages = (IntIntHashMap) field.get(bucket);
    final java.util.List<Integer> listed = new java.util.ArrayList<>();
    synchronized (freeSpaceInPages) {
      freeSpaceInPages.forEach((pageId, freeSpace) -> listed.add(pageId));
    }
    final java.util.List<Integer> full = new java.util.ArrayList<>();
    for (final int pageId : listed) {
      final BasePage page = ((com.arcadedb.database.DatabaseInternal) database).getTransaction()
          .getPage(new PageId((com.arcadedb.database.DatabaseInternal) database, bucket.getFileId(), pageId), bucket.getPageSize());
      if (page.readShort(LocalBucket.PAGE_RECORD_COUNT_IN_PAGE_OFFSET) >= bucket.getMaxRecordsInPage())
        full.add(pageId);
    }
    assertThat(full).as("pages at maxRecordsInPage (%d) must not enter the free-space map", bucket.getMaxRecordsInPage()).isEmpty();
  }

  @Test
  void bulkGrowingUpdateOverSmallRecordsStaysCheap() {
    database.getSchema().createDocumentType("Tiny", 1);

    database.begin();
    for (int i = 0; i < RECORDS; i++) {
      database.newDocument("Tiny").set("i", i).save();
      if ((i + 1) % 5_000 == 0) {
        database.commit();
        database.begin();
      }
    }
    database.commit();
    reopenDatabase();

    final long start = System.nanoTime();
    database.transaction(() -> database.command("sql", "UPDATE Tiny SET grown = 'a value that widens every record'"));
    final long ms = (System.nanoTime() - start) / 1_000_000;

    assertThat(database.countType("Tiny", false)).isEqualTo(RECORDS);
    assertThat(ms).as("bulk update of %d small records", RECORDS).isLessThan(30_000);
  }
}
