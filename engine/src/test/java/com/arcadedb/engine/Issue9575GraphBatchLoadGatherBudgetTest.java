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
import com.arcadedb.database.RID;
import com.arcadedb.graph.GraphBatch;
import com.arcadedb.serializer.json.JSONArray;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9575, second report on discussion #9559: with the append fix of #9901 in, a GraphImporter load of 209M vertices still
 * stopped for minutes at a time, every stall longer than the one before (3 minutes at 35M vertices, 20 at 209M), with the GC
 * idle. That is the cost of reading every page of the vertex bucket once: something other than a plain append still arms the
 * free-space gather, and a gather, once armed, read the whole file in one go on the writer's thread.
 * <p>
 * The arming was a record larger than a page: the chunks of a multi-page record are not written by the hole-free append, so
 * the commit measures their pages, and every measurement counted as a change. A load of real-world rows meets one every few
 * seconds. A measurement states the page's room in the free-space map itself, so it now arms a scan only when the map is full
 * and cannot keep it. And because the next arming path would otherwise be found the same way, the scan itself is bounded too:
 * a gather reads at most a fixed number of pages and picks up where it stopped the next time it runs.
 * <p>
 * Counting pages needs no clock, so a stalled JVM cannot flip these tests; the 5 s throttle is expired through a hook instead
 * of slept through.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9575GraphBatchLoadGatherBudgetTest extends TestHelper {
  private static final int    BUDGET          = 64;
  private static final String PAYLOAD         = "x".repeat(1_000);
  private static final int    RECORDS         = 30_000;
  private static final int    VERTICES        = 6_000;
  private static final int    MAX_VERTEX_SIZE = 100_000;

  /**
   * The reporter's shape: a GraphBatch vertex pass whose rows vary in size, some of them larger than a page. The commits
   * measure the pages the chunks land on, and none of those measurements arms a scan.
   */
  @Test
  void aGraphBatchLoadWithRecordsLargerThanAPageArmsNoScan() {
    database.getSchema().createVertexType("Work", 1);
    final LocalBucket bucket = (LocalBucket) database.getSchema().getType("Work").getBuckets(false).get(0);
    bucket.setGatherPagesPerScanForTesting(BUDGET);

    final Random sizes = new Random(7);
    try (final GraphBatch batch = database.batch().withBidirectional(false).withWAL(false).withPreAllocateEdgeChunks(false)
        .withCommitEvery(0).build()) {
      database.begin();
      for (int i = 0; i < VERTICES; i++) {
        batch.createVertex("Work", "id", i, "payload", "x".repeat(50 + sizes.nextInt(MAX_VERTEX_SIZE)));
        if ((i + 1) % 200 == 0) {
          database.commit();
          bucket.expireGatherThrottleForTesting();
          database.begin();
        }
      }
      database.commit();
    }

    // the premise: a file many budgets long, so one armed scan would show
    assertThat(bucket.getTotalPages()).isGreaterThan(BUDGET * 20);

    assertThat(bucket.getGatherScanCountForTesting()).as("gather scans run by the load, the first one included")
        .isLessThanOrEqualTo(1L);
    assertThat(bucket.getGatherPagesReadForTesting()).isLessThanOrEqualTo(BUDGET);
  }

  /**
   * A delete lists the room it frees itself, so it arms no scan, until the free-space map is full and drops what it is told:
   * that room is known to nothing else, and only a scan finds it again.
   */
  @Test
  void aDeleteArmsAScanOnlyWhenTheMapCannotListItsRoom() {
    database.getSchema().createDocumentType("Wide", 1);
    final LocalBucket bucket = wideBucket();
    bucket.setGatherPagesPerScanForTesting(BUDGET);

    final List<RID> rids = append(RECORDS);
    final int pages = bucket.getTotalPages();
    assertThat(pages).isGreaterThan(220);
    finishLap(bucket, pages);

    // 20 pages: the map has room for them all
    freeMostOfPages(bucket, rids, 0, 20);
    assertThat(bucket.getListedFreeSpacePagesForTesting()).contains(0, 10, 19);
    assertThat(pagesReadByOneGather(bucket)).as("pages read after deletes the map listed").isZero();

    // 200 more: the map holds 100, the rest is dropped
    freeMostOfPages(bucket, rids, 20, 220);
    assertThat(pagesReadByOneGather(bucket)).as("pages read after deletes the map could not list")
        .isPositive().isLessThanOrEqualTo(BUDGET);
  }

  /**
   * An insert that lands on a page the full map does not list evicts the listed page with the least room. That page still has
   * room the map no longer knows of, so the eviction arms a scan, whatever the write was: once the map has room again, the
   * scan finds the page.
   */
  @Test
  void aPageAnInsertEvictsFromAFullMapIsFoundAgain() {
    database.getSchema().createDocumentType("Wide", 1);
    final LocalBucket bucket = wideBucket();
    bucket.setGatherPagesPerScanForTesting(BUDGET);

    final List<RID> rids = append(RECORDS);
    final int pages = bucket.getTotalPages();
    finishLap(bucket, pages);

    // a little room on just enough pages to fill the map: nothing is dropped
    final int pagesToFree = 100 - bucket.getListedFreeSpacePagesForTesting().length;
    database.transaction(() -> {
      for (final RID rid : rids)
        if (rid.getPosition() / bucket.getMaxRecordsInPage() < pagesToFree && rid.getPosition() % bucket.getMaxRecordsInPage() < 9)
          database.deleteRecord(rid.asDocument());
    });
    final int[] listedBefore = bucket.getListedFreeSpacePagesForTesting();
    assertThat(listedBefore).hasSize(100);
    assertThat(pagesReadByOneGather(bucket)).as("pages read with every freed page listed").isZero();

    // a record no listed page can take goes to a new page, which has more room than the page with the least: that one leaves
    // the map
    database.transaction(() -> database.newDocument("Wide").set("payload", "x".repeat(20_000)).save());
    final int evicted = notIn(listedBefore, bucket.getListedFreeSpacePagesForTesting());
    assertThat(evicted).as("the page the insert evicted").isNotNegative();

    // records that fill listed pages give the map room again
    database.transaction(() -> {
      for (int i = 0; i < 5; i++)
        database.newDocument("Wide").set("payload", "x".repeat(7_500)).save();
    });
    assertThat(bucket.getListedFreeSpacePagesForTesting()).hasSizeLessThan(100);

    for (int i = 0; i < pages / BUDGET + 2 && !contains(bucket.getListedFreeSpacePagesForTesting(), evicted); i++) {
      bucket.expireGatherThrottleForTesting();
      bucket.gatherPageStatistics();
    }
    assertThat(bucket.getListedFreeSpacePagesForTesting()).as("pages listed once the map had room again").contains(evicted);
  }

  /**
   * A map that stays full keeps dropping the pages that writes tell it of, and each drop arms a gather. That must not turn
   * into a gather per allocation: the allocator does not ask for one while the map has a page that fits, and the throttle
   * holds the rest.
   */
  @Test
  void aMapKeptFullRunsNoGatherPerAllocation() {
    database.getSchema().createDocumentType("Wide", 1);
    final LocalBucket bucket = wideBucket();
    bucket.setGatherPagesPerScanForTesting(BUDGET);

    final List<RID> rids = append(RECORDS);
    finishLap(bucket, bucket.getTotalPages());

    // the map is full, and 200 more pages with room are dropped
    freeMostOfPages(bucket, rids, 0, 300);
    assertThat(bucket.getListedFreeSpacePagesForTesting()).hasSize(100);

    final long scansBefore = bucket.getGatherScanCountForTesting();
    final long readBefore = bucket.getGatherPagesReadForTesting();
    int allocations = 0;
    for (int round = 0; round < 20; round++) {
      // every round frees room on pages the full map cannot list (each drop arms a gather), then allocates
      freeMostOfPages(bucket, rids, 300 + round * 5, 305 + round * 5);
      database.begin();
      for (int i = 0; i < 100; i++) {
        database.newDocument("Wide").set("payload", PAYLOAD).save();
        ++allocations;
      }
      database.commit();
    }

    assertThat(bucket.getGatherScanCountForTesting() - scansBefore).as("gathers run by %d allocations", allocations)
        .isLessThanOrEqualTo(1L);
    assertThat(bucket.getGatherPagesReadForTesting() - readBefore).isLessThanOrEqualTo(BUDGET);
  }

  /**
   * A bounded scan must still get round the whole file: space freed at its far end is found, one throttled slice at a time.
   */
  @Test
  void freedSpaceAtTheEndOfALargeBucketIsStillFound() {
    database.getSchema().createDocumentType("Wide", 1);
    final LocalBucket bucket = wideBucket();
    bucket.setGatherPagesPerScanForTesting(BUDGET);

    final List<RID> rids = append(RECORDS);
    final int pages = bucket.getTotalPages();
    assertThat(pages).isGreaterThan(BUDGET * 5);

    // free most of one page near the end of the file, the last place a scan starting at the head reaches
    final int targetPage = pages - 10;
    database.transaction(() -> {
      for (final RID rid : rids)
        if (rid.getPosition() / bucket.getMaxRecordsInPage() == targetPage && rid.getPosition() % 4 != 0)
          database.deleteRecord(rid.asDocument());
    });

    // the commit lists the page it has just measured: forget it, so only a scan can find it again
    bucket.setPageStatistics(new JSONArray());
    assertThat(bucket.getListedFreeSpacePagesForTesting()).isEmpty();

    final long readBefore = bucket.getGatherPagesReadForTesting();
    int slices = 0;
    while (!contains(bucket.getListedFreeSpacePagesForTesting(), targetPage) && slices < pages) {
      final long readBeforeSlice = bucket.getGatherPagesReadForTesting();
      bucket.expireGatherThrottleForTesting();
      bucket.gatherPageStatistics();
      assertThat(bucket.getGatherPagesReadForTesting() - readBeforeSlice).as("pages read by one gather").isLessThanOrEqualTo(BUDGET);

      // between two slices the throttle holds: a gather asked again at once reads nothing
      final long readAfterSlice = bucket.getGatherPagesReadForTesting();
      bucket.gatherPageStatistics();
      assertThat(bucket.getGatherPagesReadForTesting()).isEqualTo(readAfterSlice);
      ++slices;
    }

    assertThat(bucket.getListedFreeSpacePagesForTesting()).as("pages listed after %d slices", slices).contains(targetPage);
    assertThat(bucket.getGatherPagesReadForTesting() - readBefore).isLessThanOrEqualTo(pages + BUDGET);
  }

  /**
   * Once a lap has visited every page, nothing is left to scan until a write arms the next one.
   */
  @Test
  void aFinishedLapStopsScanning() {
    database.getSchema().createDocumentType("Wide", 1);
    final LocalBucket bucket = wideBucket();
    bucket.setGatherPagesPerScanForTesting(BUDGET);

    append(RECORDS);
    final int pages = bucket.getTotalPages();
    // a map replaced wholesale, as an open loads it: what it no longer lists, only a scan can find
    bucket.setPageStatistics(new JSONArray());

    final long readBefore = bucket.getGatherPagesReadForTesting();
    for (int i = 0; i < pages / BUDGET + 2; i++) {
      bucket.expireGatherThrottleForTesting();
      bucket.gatherPageStatistics();
    }
    final long lap = bucket.getGatherPagesReadForTesting() - readBefore;
    assertThat(lap).isGreaterThan(0L).isLessThanOrEqualTo(pages);

    for (int i = 0; i < 5; i++) {
      bucket.expireGatherThrottleForTesting();
      bucket.gatherPageStatistics();
    }
    assertThat(bucket.getGatherPagesReadForTesting() - readBefore).as("pages read after the lap was over").isEqualTo(lap);
  }

  private List<RID> append(final int count) {
    final List<RID> rids = new ArrayList<>(count);
    database.begin();
    for (int i = 0; i < count; i++) {
      final MutableDocument d = database.newDocument("Wide").set("i", i).set("payload", PAYLOAD);
      d.save();
      rids.add(d.getIdentity());
      if ((i + 1) % 2_000 == 0) {
        database.commit();
        database.begin();
      }
    }
    database.commit();
    return rids;
  }

  /** Deletes three records out of four on pages {@code [from, to)}, in one transaction. */
  private void freeMostOfPages(final LocalBucket bucket, final List<RID> rids, final int from, final int to) {
    database.transaction(() -> {
      for (final RID rid : rids) {
        final long page = rid.getPosition() / bucket.getMaxRecordsInPage();
        if (page >= from && page < to && rid.getPosition() % 4 != 0)
          database.deleteRecord(rid.asDocument());
      }
    });
  }

  /** Runs throttled gathers until the open lap, if any, has visited every page. */
  private static void finishLap(final LocalBucket bucket, final int pages) {
    for (int i = 0; i < pages / BUDGET + 2; i++) {
      bucket.expireGatherThrottleForTesting();
      bucket.gatherPageStatistics();
    }
  }

  private static long pagesReadByOneGather(final LocalBucket bucket) {
    final long before = bucket.getGatherPagesReadForTesting();
    bucket.expireGatherThrottleForTesting();
    bucket.gatherPageStatistics();
    return bucket.getGatherPagesReadForTesting() - before;
  }

  private LocalBucket wideBucket() {
    return (LocalBucket) database.getSchema().getType("Wide").getBuckets(false).get(0);
  }

  /** The first value of {@code before} that {@code after} lacks, or -1. */
  private static int notIn(final int[] before, final int[] after) {
    for (final int v : before)
      if (!contains(after, v))
        return v;
    return -1;
  }

  private static boolean contains(final int[] values, final int value) {
    for (final int v : values)
      if (v == value)
        return true;
    return false;
  }
}
