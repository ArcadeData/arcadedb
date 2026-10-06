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
package com.arcadedb.engine.timeseries;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.ImmutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.WALFile;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.LocalTimeSeriesType;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8574: a newest-first scan of the mutable bucket for one tag read and tag-checked every row of every page
 * that did not hold the tag, because the only thing that stopped the walk was a cut-off built from matching rows.
 * A shard holding no row of the tag, or a series that stopped reporting long ago, meant reading the whole bucket.
 * Each data page now carries a summary of the dictionary ids it holds, trusted only for the page version it was built
 * from, and {@code appendBatch} hands each shard a contiguous run of the batch instead of striping it row by row
 * (which put every row of a series on one shard for a time-major load). {@code COMPACT TIMESERIES TYPE} seals the
 * tail on demand.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8574MutableTagSummaryTest extends TestHelper {

  private static final int  HOSTS   = 4;
  private static final long BASE_TS = 1_700_000_000_000L;
  private static final long STEP_MS = 1_000L;

  private TimeSeriesEngine create(final int shards) {
    database.command("sql",
        "CREATE TIMESERIES TYPE Point TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE) SHARDS " + shards);
    return ((LocalTimeSeriesType) database.getSchema().getType("Point")).getEngine();
  }

  /**
   * Time-major, as a line-protocol load arrives: every host at t, then every host at t + 1.
   */
  private static void appendTimeMajor(final TimeSeriesEngine engine, final int fromTick, final int toTick) throws IOException {
    final int rows = (toTick - fromTick) * HOSTS;
    final long[] timestamps = new long[rows];
    final Object[] hosts = new Object[rows];
    final Object[] values = new Object[rows];
    int i = 0;
    for (int t = fromTick; t < toTick; t++)
      for (int h = 0; h < HOSTS; h++) {
        timestamps[i] = BASE_TS + (long) t * STEP_MS;
        hosts[i] = "host_" + h;
        values[i] = (double) (t * HOSTS + h);
        i++;
      }
    engine.appendBatch(timestamps, new Object[][] { hosts, values });
  }

  private static void appendOne(final TimeSeriesEngine engine, final String host, final int tick) throws IOException {
    engine.appendBatch(new long[] { BASE_TS + (long) tick * STEP_MS }, new Object[][] { { host }, { -1.0 * tick } });
  }

  private static List<List<Object>> deep(final List<Object[]> rows) {
    final List<List<Object>> out = new ArrayList<>(rows.size());
    for (final Object[] row : rows)
      out.add(Arrays.asList(row));
    return out;
  }

  /**
   * Reference: every row, filtered and sorted here, so it does not go through the tag filter under test.
   */
  private static List<Object[]> expected(final TimeSeriesEngine engine, final String host, final boolean descending,
      final int limit) throws IOException {
    final List<Object[]> all = new ArrayList<>();
    for (final Object[] row : engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null))
      if (host.equals(row[1]))
        all.add(row);
    all.sort((a, b) -> descending ? Long.compare((long) b[0], (long) a[0]) : Long.compare((long) a[0], (long) b[0]));
    return limit > 0 && all.size() > limit ? new ArrayList<>(all.subList(0, limit)) : all;
  }

  private int dataPages(final TimeSeriesEngine engine, final int shard) throws IOException {
    return engine.getShard(shard).getMutableBucket().getDataPageCount();
  }

  /**
   * A series that stopped reporting: its only rows are on the first page, and the newest-first walk used to read
   * every row of every newer page before reaching them.
   */
  @Test
  void pagesWithoutTheTagAreSkippedOnTheirSummary() throws Exception {
    final TimeSeriesEngine engine = create(1);
    appendOne(engine, "stale", 0);
    appendOne(engine, "stale", 1);
    appendTimeMajor(engine, 2, 20_000);

    final int pages = dataPages(engine, 0);
    assertThat(pages).isGreaterThan(10);

    final AggregationMetrics metrics = new AggregationMetrics();
    final List<Object[]> newest = engine.queryDescending(Long.MIN_VALUE, Long.MAX_VALUE, null, TagFilter.eq(0, "stale"), 1,
        metrics);

    assertThat(deep(newest)).isEqualTo(deep(expected(engine, "stale", true, 1)));
    assertThat((long) newest.get(0)[0]).isEqualTo(BASE_TS + STEP_MS);
    // Only the first page holds the tag: every other one is dropped on its summary.
    assertThat(metrics.getScannedPages()).isEqualTo(1);
    assertThat(metrics.getScannedPages() + metrics.getSkippedPages()).isEqualTo(pages);

    // A tag that is nowhere at all reads no page.
    final AggregationMetrics none = new AggregationMetrics();
    appendOne(engine, "elsewhere", 0);
    assertThat(engine.queryDescending(Long.MIN_VALUE, Long.MAX_VALUE, null, TagFilter.eq(0, "missing"), 1, none)).isEmpty();
    assertThat(none.getScannedPages()).isZero();
  }

  @Test
  void ascendingAndUnlimitedScansAgreeWithAFullScan() throws Exception {
    final TimeSeriesEngine engine = create(2);
    appendTimeMajor(engine, 0, 5_000);
    appendOne(engine, "rare", 5_000);
    appendTimeMajor(engine, 5_001, 10_000);
    appendOne(engine, "rare", 10_000);

    for (final String host : new String[] { "rare", "host_0", "host_3" }) {
      for (final int limit : new int[] { 1, 2, 5, 0 }) {
        assertThat(deep(engine.queryDescending(Long.MIN_VALUE, Long.MAX_VALUE, null, TagFilter.eq(0, host), limit, null)))
            .as("descending %s limit %d", host, limit).isEqualTo(deep(expected(engine, host, true, limit)));
        assertThat(deep(engine.queryAscending(Long.MIN_VALUE, Long.MAX_VALUE, null, TagFilter.eq(0, host), limit, null)))
            .as("ascending %s limit %d", host, limit).isEqualTo(deep(expected(engine, host, false, limit)));
      }
      assertThat(deep(engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, TagFilter.eq(0, host))))
          .as("unlimited %s", host).isEqualTo(deep(expected(engine, host, false, 0)));
    }

    // An IN-style condition: the page is kept when it holds any of the candidates.
    final TagFilter either = TagFilter.in(0, Set.<Object>of("rare", "missing"));
    assertThat(engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, either)).hasSize(2);
  }

  /**
   * The summary describes one image of the page. A row appended to a page already summarised, and a page cleared by
   * compaction and then reused, both change the page version and must not be answered from the old summary.
   */
  @Test
  void aSummaryIsNeverTrustedForAnotherImageOfThePage() throws Exception {
    final TimeSeriesEngine engine = create(1);
    appendTimeMajor(engine, 0, 100);

    // Summarise every page, the last one included, with no row of "late" anywhere.
    assertThat(engine.queryDescending(Long.MIN_VALUE, Long.MAX_VALUE, null, TagFilter.eq(0, "host_1"), 0, null)).hasSize(100);
    assertThat(engine.queryDescending(Long.MIN_VALUE, Long.MAX_VALUE, null, TagFilter.eq(0, "late"), 1, null)).isEmpty();

    // Into the same page the summary was built from.
    appendOne(engine, "late", 50);
    assertThat(deep(engine.queryDescending(Long.MIN_VALUE, Long.MAX_VALUE, null, TagFilter.eq(0, "late"), 1, null)))
        .isEqualTo(deep(expected(engine, "late", true, 1)))
        .hasSize(1);

    // Clear the pages, then write them again from page 1: same page numbers, different rows.
    engine.compactAll();
    assertThat(engine.getShard(0).getMutableBucket().getSampleCount()).isZero();
    appendOne(engine, "reused", 200);
    appendOne(engine, "late", 201);
    assertThat(deep(engine.queryDescending(Long.MIN_VALUE, Long.MAX_VALUE, null, TagFilter.eq(0, "reused"), 1, null)))
        .isEqualTo(deep(expected(engine, "reused", true, 1)))
        .hasSize(1);
    assertThat(engine.queryDescending(Long.MIN_VALUE, Long.MAX_VALUE, null, TagFilter.eq(0, "late"), 0, null)).hasSize(2);
  }

  /**
   * A time-major batch used to be striped one row at a time, so with a host count that is a multiple of the shard
   * count every row of a host landed on one shard. Each shard now receives a contiguous run: a time slice of every
   * host, in the order the caller wrote it.
   */
  @Test
  void aTimeMajorBatchGivesEveryShardEverySeries() throws Exception {
    final int shards = 4;
    final TimeSeriesEngine engine = create(shards);
    appendTimeMajor(engine, 0, 1_000);

    long total = 0;
    for (int s = 0; s < shards; s++) {
      final TimeSeriesShard shard = engine.getShard(s);
      total += shard.getMutableBucket().getSampleCount();
      for (int h = 0; h < HOSTS; h++)
        assertThat(shard.scanRangeDescending(Long.MIN_VALUE, Long.MAX_VALUE, null, TagFilter.eq(0, "host_" + h), 1, null))
            .as("shard %d host_%d", s, h).hasSize(1);
      // A contiguous run of a time-ordered batch stays time-ordered.
      final List<Object[]> rows = shard.scanRange(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
      for (int i = 1; i < rows.size(); i++)
        assertThat((long) rows.get(i)[0]).isGreaterThanOrEqualTo((long) rows.get(i - 1)[0]);
    }
    assertThat(total).isEqualTo(1_000L * HOSTS);

    // The newest reading of a host is answered from one page: every other shard is pruned by the running bound.
    final AggregationMetrics metrics = new AggregationMetrics();
    final List<Object[]> newest = engine.queryDescending(Long.MIN_VALUE, Long.MAX_VALUE, null, TagFilter.eq(0, "host_2"), 1,
        metrics);
    assertThat(deep(newest)).isEqualTo(deep(expected(engine, "host_2", true, 1)));
    assertThat(metrics.getMaterializedRows()).isLessThanOrEqualTo(shards);
  }

  /**
   * Batches smaller than the shard count must still rotate across every shard: advancing the routing counter by the
   * shard count instead of by the runs handed out sent every two-row batch to the same two shards.
   */
  @Test
  void smallBatchesRotateAcrossEveryShard() throws Exception {
    final int shards = 4;
    final TimeSeriesEngine engine = create(shards);
    for (int b = 0; b < 8; b++)
      engine.appendBatch(new long[] { BASE_TS + b * 2L, BASE_TS + b * 2L + 1 }, new Object[][] { { "h", "h" }, { 1.0, 2.0 } });

    for (int s = 0; s < shards; s++)
      assertThat(engine.getShard(s).getMutableBucket().getSampleCount()).as("shard %d", s).isEqualTo(4L);
  }

  /**
   * A page rewritten by replay at its own version drops its cached summary, so the next scan rebuilds it.
   */
  @Test
  void invalidatingASummaryKeepsTheAnswerExact() throws Exception {
    final TimeSeriesEngine engine = create(1);
    appendTimeMajor(engine, 0, 3_000);
    final TimeSeriesBucket bucket = engine.getShard(0).getMutableBucket();
    final List<Object[]> before = engine.queryDescending(Long.MIN_VALUE, Long.MAX_VALUE, null, TagFilter.eq(0, "host_1"), 3, null);

    for (int p = 0; p <= bucket.getDataPageCount() + 5; p++)
      bucket.invalidatePageTagSummary(p);

    assertThat(deep(engine.queryDescending(Long.MIN_VALUE, Long.MAX_VALUE, null, TagFilter.eq(0, "host_1"), 3, null)))
        .isEqualTo(deep(before))
        .isEqualTo(deep(expected(engine, "host_1", true, 3)));
  }

  /**
   * Drives the real replay path: {@code TransactionManager.applyChanges} re-applies a data page at the version it
   * already has, as the torn-write repair does, with different tag ids in it. The page's summary was cached for that
   * version beforehand, and must not be reused to skip the page afterwards.
   */
  @Test
  void anEqualVersionReplayOfAPageInvalidatesItsSummary() throws Exception {
    final TimeSeriesEngine engine = create(1);
    // Put "replayed" into the dictionary, then seal it away so no mutable page holds it
    appendOne(engine, "replayed", 0);
    engine.compactAll();
    appendTimeMajor(engine, 1, 50);

    final TimeSeriesBucket bucket = engine.getShard(0).getMutableBucket();
    assertThat(bucket.getDataPageCount()).isEqualTo(1);
    // Caches page 1's summary, which holds no "replayed"
    assertThat(bucket.scanRangeDescending(Long.MIN_VALUE, Long.MAX_VALUE, null, TagFilter.eq(0, "replayed"), 1, null)).isEmpty();

    // Page 1 at its current version, with the first row's host id replaced by the id of "replayed"
    final DatabaseInternal db = (DatabaseInternal) database;
    final PageId pageId = new PageId(db, bucket.getFileId(), 1);
    final ImmutablePage page = db.getPageManager().getImmutablePage(pageId, bucket.getPageSize(), false, true);
    final byte[] content = new byte[page.getContentSize()];
    page.readByteArray(0, content);
    final int replayedId = bucket.getTagDictionary().getId("replayed");
    // Data page content: sample count (2) + min ts (8) + max ts (8), then rows of [ts(8) | host id(4) | value(8)]
    final int hostIdOffset = 18 + 8;
    content[hostIdOffset] = (byte) (replayedId >>> 24);
    content[hostIdOffset + 1] = (byte) (replayedId >>> 16);
    content[hostIdOffset + 2] = (byte) (replayedId >>> 8);
    content[hostIdOffset + 3] = (byte) replayedId;

    final WALFile.WALPage walPage = new WALFile.WALPage();
    walPage.fileId = bucket.getFileId();
    walPage.pageNumber = 1;
    walPage.changesFrom = BasePage.PAGE_HEADER_SIZE;
    walPage.changesTo = BasePage.PAGE_HEADER_SIZE + content.length - 1;
    walPage.currentContent = new Binary(content);
    walPage.currentPageVersion = (int) page.getVersion();
    walPage.currentPageSize = page.getContentSize() + BasePage.PAGE_HEADER_SIZE;
    final WALFile.WALTransaction tx = new WALFile.WALTransaction();
    tx.txId = -1;
    tx.pages = new WALFile.WALPage[] { walPage };
    db.getTransactionManager().applyChanges(tx, new HashMap<>(), false);

    assertThat(db.getPageManager().getImmutablePage(pageId, bucket.getPageSize(), false, true).getVersion())
        .isEqualTo(page.getVersion());
    assertThat(bucket.scanRangeDescending(Long.MIN_VALUE, Long.MAX_VALUE, null, TagFilter.eq(0, "replayed"), 1, null))
        .as("the page now holds the tag: its pre-replay summary must not be reused").hasSize(1);
  }

  @Test
  void compactTimeSeriesTypeSealsTheTail() throws Exception {
    final TimeSeriesEngine engine = create(2);
    appendTimeMajor(engine, 0, 2_000);

    try (final ResultSet rs = database.command("sql", "COMPACT TIMESERIES TYPE Point")) {
      final Result r = rs.next();
      assertThat(r.<String>getProperty("operation")).isEqualTo("compact timeseries type");
      assertThat(r.<String>getProperty("typeName")).isEqualTo("Point");
      assertThat(r.<Long>getProperty("mutableSamplesBefore")).isEqualTo(2_000L * HOSTS);
      assertThat(r.<Long>getProperty("mutableSamples")).isZero();
    }
    for (int s = 0; s < 2; s++) {
      assertThat(engine.getShard(s).getMutableBucket().getSampleCount()).isZero();
      assertThat(engine.getShard(s).getSealedStore().getBlockCount()).isPositive();
    }

    try (final ResultSet rs = database.query("sql", "SELECT ts, value FROM Point WHERE host = 'host_3' ORDER BY ts DESC LIMIT 1")) {
      assertThat(rs.next().<Double>getProperty("value")).isEqualTo((double) (1_999 * HOSTS + 3));
    }
  }

  @Test
  void compactTimeSeriesTypeRejectsAnotherKindOfType() {
    database.command("sql", "CREATE DOCUMENT TYPE Doc");
    assertThatThrownBy(() -> database.command("sql", "COMPACT TIMESERIES TYPE Doc"))
        .hasMessageContaining("is not a TimeSeries type");
  }
}
