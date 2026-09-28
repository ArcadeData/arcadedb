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
package com.arcadedb.query.sql.executor;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.query.ParallelScanProducerPool;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8594: a parallel scan whose query no longer needs rows - a satisfied LIMIT, or a result set read in part and
 * left open - kept its producers parked on full channels, each holding a thread of the JVM-wide producer pool, until
 * the result set was closed or the abandonment timeout (10 minutes) passed. A few unclosed pages of a RID-paginated
 * read took the whole pool, and the next query waited for rows no producer was left to produce.
 * <p>
 * Every wait here is bounded by a hang detector far below the 10 minute abandonment timeout, which is left at its
 * default: a regression hangs until the detector fires.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8594ParallelScanLimitReleaseTest extends TestHelper {
  private static final String TYPE_NAME = "Rating";
  // ONE BUCKET (THE DEFAULT), SPLIT IN UNITS OF MORE ROWS THAN A UNIT'S CHANNEL HOLDS (4096), SO A PRODUCER NOBODY
  // DRAINS PARKS ON ITS FULL CHANNEL: THE SHAPE OF THE REPORTER'S REPRO
  private static final int    RECORDS   = 100_000;
  private static final int    PAGE_SIZE = 5_000;

  private void createAndPopulate() {
    // UNITS OF 8 PAGES, ABOUT 13,000 ROWS EACH: THE ~60 PAGES OF THIS TYPE WOULD STAY ONE UNIT AT THE DEFAULT 32
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN_PAGES_PER_UNIT, 8);
    database.getSchema().createDocumentType(TYPE_NAME);
    database.begin();
    for (int i = 0; i < RECORDS; i++) {
      database.newDocument(TYPE_NAME).set("id", i, "userId", (long) (i % 610), "movieId", (long) (i % 9742), "rating", (i % 10) / 2.0)
          .save();
      if (i % 10_000 == 9_999) {
        database.commit();
        database.begin();
      }
    }
    database.commit();
  }

  /**
   * The reporter's pattern: every page read to its end and never closed. Before the fix the fifth page (on 8 cores)
   * waited for the abandonment timeout.
   */
  @Test
  @Timeout(value = 120, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void unclosedRidPagesReadToTheirEndDoNotStallTheNextPage() {
    createAndPopulate();
    assertThat(wouldRunInParallel("SELECT @rid AS rid, userId FROM " + TYPE_NAME + " WHERE @rid > #-1:-1 LIMIT " + PAGE_SIZE))
        .as("the page query must take the parallel path, or this test proves nothing").isTrue();

    String last = "#-1:-1";
    long total = 0;
    int pages = 0;
    while (true) {
      // NEVER CLOSED, ON PURPOSE
      final ResultSet rs = database.query("sql",
          "SELECT @rid AS rid, userId FROM " + TYPE_NAME + " WHERE @rid > " + last + " LIMIT " + PAGE_SIZE);
      int n = 0;
      String lastRid = null;
      while (rs.hasNext()) {
        lastRid = rs.next().getProperty("rid").toString();
        n++;
      }
      ++pages;
      total += n;
      if (n < PAGE_SIZE)
        break;
      last = lastRid;
    }

    assertThat(total).isEqualTo(RECORDS);
    assertThat(pages).isEqualTo(RECORDS / PAGE_SIZE + 1);
  }

  /**
   * A LIMIT that has delivered its last row releases the scan's producers at once: the pool threads they held are free
   * again although the result set is never closed.
   */
  @Test
  @Timeout(value = 120, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void satisfiedLimitReleasesTheProducersWithoutClose() throws Exception {
    createAndPopulate();
    final int maxThreads = ParallelScanProducerPool.getInstance().getMaxParallelism();

    for (int i = 0; i < maxThreads * 2; i++) {
      // NEVER CLOSED, ON PURPOSE
      // EXACTLY TEN next(), NO FINAL hasNext(): THE RELEASE MUST COME WITH THE LAST ROW, NOT WITH A LATER CALL
      final ResultSet rs = database.query("sql", "SELECT FROM " + TYPE_NAME + " LIMIT 10");
      for (int row = 0; row < 10; row++) {
        assertThat(rs.hasNext()).isTrue();
        rs.next();
      }
    }

    final long deadline = System.currentTimeMillis() + 20_000;
    while (ParallelScanProducerPool.getInstance().getPoolStats().activeThreads() > 0 && System.currentTimeMillis() < deadline)
      Thread.sleep(10);
    assertThat(ParallelScanProducerPool.getInstance().getPoolStats().activeThreads())
        .as("the producers of a satisfied LIMIT must not keep their pool threads until the result set is closed").isZero();
  }

  /**
   * Result sets read in part and left open, no LIMIT: nothing tells the engine they are done with, so their producers
   * stay parked until they are closed or abandoned. They take every thread of the producer pool here, and a scan, a
   * parallel aggregation and a parallel GROUP BY (with its parallel merge) still complete: a query never waits on a
   * unit no producer has taken, nor on a task the saturated pool has not started.
   */
  @Test
  @Timeout(value = 120, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void queriesProgressWhileAbandonedScansHoldEveryProducerThread() throws Exception {
    createAndPopulate();
    final int maxThreads = ParallelScanProducerPool.getInstance().getMaxParallelism();
    final String scan = "SELECT FROM " + TYPE_NAME;
    final String count = "SELECT count(*) AS c FROM " + TYPE_NAME + " WHERE rating >= 0";
    // ONE GROUP PER RECORD: 100,000 PARTIAL GROUPS, FAR ABOVE THE 16,384 FROM WHICH THE PARTIALS ARE MERGED IN PARALLEL TOO
    final String groupBy = "SELECT id, count(*) AS c FROM " + TYPE_NAME + " GROUP BY id";
    assertThat(explain(scan)).as("the scan must take the parallel path, or this test proves nothing").contains("(parallel)");
    assertThat(explain(count)).as("the count must aggregate in parallel, or this test proves nothing")
        .contains("CALCULATE AGGREGATE PROJECTIONS (parallel");
    assertThat(explain(groupBy)).as("the GROUP BY must aggregate in parallel, or this test proves nothing")
        .contains("CALCULATE AGGREGATE PROJECTIONS (parallel");

    final List<ResultSet> abandoned = new ArrayList<>();
    try {
      for (int i = 0; i < maxThreads; i++) {
        final ResultSet rs = database.query("sql", scan);
        assertThat(rs.hasNext()).isTrue();
        rs.next();
        abandoned.add(rs);
      }

      final long deadline = System.currentTimeMillis() + 20_000;
      while (ParallelScanProducerPool.getInstance().getPoolStats().activeThreads() < maxThreads
          && System.currentTimeMillis() < deadline)
        Thread.sleep(10);
      assertThat(ParallelScanProducerPool.getInstance().getPoolStats().activeThreads())
          .as("the abandoned scans must hold every producer thread, or this test proves nothing").isEqualTo(maxThreads);

      try (final ResultSet rs = database.query("sql", scan)) {
        long n = 0;
        while (rs.hasNext()) {
          rs.next();
          n++;
        }
        assertThat(n).isEqualTo(RECORDS);
      }

      try (final ResultSet rs = database.query("sql", count)) {
        assertThat(rs.next().<Long>getProperty("c")).isEqualTo(RECORDS);
      }

      try (final ResultSet rs = database.query("sql", groupBy)) {
        long groups = 0;
        while (rs.hasNext()) {
          assertThat(rs.next().<Long>getProperty("c")).isEqualTo(1L);
          groups++;
        }
        assertThat(groups).isEqualTo(RECORDS);
      }
    } finally {
      for (final ResultSet rs : abandoned)
        rs.close();
    }
  }

  private boolean wouldRunInParallel(final String sql) {
    return explain(sql).contains("(parallel)");
  }

  private String explain(final String sql) {
    try (final ResultSet rs = database.query("sql", "EXPLAIN " + sql)) {
      return rs.next().getProperty("executionPlanAsString");
    }
  }
}
