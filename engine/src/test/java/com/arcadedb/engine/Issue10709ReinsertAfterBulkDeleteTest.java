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
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10709: a bulk delete gives room back on every page of the bucket, but the free-space map lists at most
 * {@code MAX_PAGES_GATHER_STATS} (100) pages: the room of the others is turned away when the map is full. Only a gather scan
 * finds it again, and the gather was held back by its 5 s throttle. So an insert right after the delete drained the 100 listed
 * pages and then extended the file next to pages with room, while the same insert 6 s later did not: where the rows landed
 * depended on the wall clock, not on the data.
 * <p>
 * Room the full map turned away is now a reason for the gather like a scan cut short by the full map is (#8660): the gather
 * looks there before the file grows, whatever the clock says. The throttle is restarted through a hook right before the
 * insert, so the test does not depend on how long its own setup took.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10709ReinsertAfterBulkDeleteTest extends TestHelper {
  private static final int    RECORDS = 40_000;
  private static final String PAYLOAD = "x".repeat(300);

  @Test
  void aReinsertRightAfterABulkDeleteReusesTheRoomTheFullMapTurnedAway() {
    final LocalBucket bucket = fillThenDeleteThreeRowsInTen();
    final int pagesAfterFill = bucket.getTotalPages();
    // the premise: more pages got room back than the map can list
    assertThat(pagesAfterFill).isGreaterThan(150);

    bucket.restartGatherThrottleForTesting();
    // half of the room the delete gave back, so every row fits on the old pages
    insert(RECORDS, RECORDS * 3 / 20);

    assertThat(bucket.getTotalPages()).as("pages after the reinsert, %d before it", pagesAfterFill).isEqualTo(pagesAfterFill);
    assertThat(database.countType("Doc", false)).isEqualTo(RECORDS - RECORDS * 3 / 10 + RECORDS * 3 / 20);
  }

  /**
   * The reason is spent by the gather it asks for: the scan reads the file once, and the inserts that follow on a bucket with no
   * room left do not pay one scan each.
   */
  @Test
  void theRoomTurnedAwayAsksForOneGatherNotOnePerAllocation() {
    final LocalBucket bucket = fillThenDeleteThreeRowsInTen();
    final int pagesAfterFill = bucket.getTotalPages();
    bucket.restartGatherThrottleForTesting();
    final long scansBefore = bucket.getGatherScanCountForTesting();
    final long pagesReadBefore = bucket.getGatherPagesReadForTesting();

    // twice the room the delete gave back: the old pages fill up and the rest of the rows go to new pages
    insert(RECORDS, RECORDS * 6 / 10);

    assertThat(bucket.getTotalPages()).isGreaterThan(pagesAfterFill);
    final long scans = bucket.getGatherScanCountForTesting() - scansBefore;
    final long pagesRead = bucket.getGatherPagesReadForTesting() - pagesReadBefore;
    assertThat(pagesRead).as("pages the gather read in %d scans over a bucket of %d pages", scans, pagesAfterFill)
        .isLessThanOrEqualTo(2L * pagesAfterFill);
  }

  /**
   * A gather that fails backs off like any other, so it must not lose where the room is: the scan that runs once the timeout is
   * over starts at the page the full map turned away, not wherever the cursor was.
   */
  @Test
  void aGatherThatFailsStillStartsTheNextOneAtTheRoomTurnedAway() {
    // the delete leaves the first quarter of the file alone, so the room turned away lies past the page the cursor is on
    final LocalBucket bucket = fillThenDelete("a % 10 < 3 AND a >= " + RECORDS / 4);
    final int roomFrom = bucket.getUnlistedRoomFromForTesting();
    // the premise: the full map turned room away, past the page the cursor is on
    assertThat(roomFrom).isGreaterThan(bucket.getGatherResumePageForTesting());

    final int fileId = bucket.getFileId();
    PageManager.setPageReadFaultInjector(pageId -> {
      if (pageId.getFileId() == fileId)
        throw new IOException("injected by Issue10709ReinsertAfterBulkDeleteTest");
    });
    try {
      bucket.gatherPageStatistics();
    } finally {
      PageManager.setPageReadFaultInjector(null);
    }

    assertThat(bucket.getGatherResumePageForTesting()).isEqualTo(roomFrom);
  }

  private LocalBucket fillThenDeleteThreeRowsInTen() {
    return fillThenDelete("a % 10 < 3");
  }

  private LocalBucket fillThenDelete(final String where) {
    database.getSchema().buildDocumentType().withName("Doc").withTotalBuckets(1).create();
    insert(0, RECORDS);
    database.transaction(() -> database.command("sql", "DELETE FROM Doc WHERE " + where).close());
    return (LocalBucket) database.getSchema().getType("Doc").getBuckets(false).get(0);
  }

  private void insert(final int from, final int count) {
    for (int c = 0; c < count; c += 5_000) {
      final int start = c;
      database.transaction(() -> {
        for (int i = start; i < Math.min(count, start + 5_000); i++)
          database.newDocument("Doc").set("a", from + i, "payload", PAYLOAD).save();
      });
    }
  }
}
