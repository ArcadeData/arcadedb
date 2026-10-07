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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.Profiler;
import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.database.Record;
import com.arcadedb.query.sql.executor.QueryHeapBudget;
import com.arcadedb.query.sql.executor.QueryHeapTracker;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.utility.MultiIterator;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Iterator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Issue #9404: a scan prefetched up to 1,024 records per bucket, by count only, and every record that spans several pages is
 * assembled into a buffer of its own. A type whose records carry a big nested document therefore had a bucket iterator
 * holding up to 1,024 of them - about 100MB per bucket - the moment the scan opened, long before the query consumed a row.
 * Nothing accounted for them, so a few dozen concurrent queries exhausted the heap while the query heap budget stayed far
 * from its limit. The batch now also ends once the bytes it copied out of the pages reach
 * {@link GlobalConfiguration#QUERY_BATCH_MAX_BYTES}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class BucketIteratorPrefetchBytesIssue9404Test extends TestHelper {
  private static final int LARGE_RECORDS = 40;
  private static final int LARGE_PAYLOAD = 200 * 1024;
  private static final int SMALL_RECORDS = 3_000;

  private long lastBatch;

  @Override
  protected void beginTest() {
    database.getSchema().createVertexType("Large", 1);
    database.getSchema().createDocumentType("Small", 1);
    database.transaction(() -> {
      for (int i = 0; i < LARGE_RECORDS; i++)
        database.newVertex("Large").set("id", i, "payload", "x".repeat(LARGE_PAYLOAD)).save();
      for (int i = 0; i < SMALL_RECORDS; i++)
        database.newDocument("Small").set("id", i).save();
    });
  }

  @Test
  void largeRecordsAreNotPrefetchedBeyondTheByteBound() {
    final long bound = 1024L * 1024;
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_BATCH_MAX_BYTES, bound);

    final BucketIterator iterator = openIterator("Large");

    // THE BATCH ENDS AS SOON AS IT REACHES THE BOUND: AT MOST ONE RECORD PAST IT
    final long prefetched = lastBatch;
    assertThat(prefetched).isGreaterThan(0).isLessThanOrEqualTo(bound / LARGE_PAYLOAD + 1);

    // EVERY RECORD IS STILL RETURNED, IN ORDER, WITH ITS CONTENT
    int count = 0;
    while (iterator.hasNext()) {
      final Document record = (Document) iterator.next();
      assertThat(record.getInteger("id")).isEqualTo(count);
      assertThat(record.getString("payload")).hasSize(LARGE_PAYLOAD);
      ++count;
    }
    assertThat(count).isEqualTo(LARGE_RECORDS);
  }

  @Test
  void aBudgetCloseToFullShrinksTheBatchToOneRecord() {
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_BATCH_MAX_BYTES, 4L * 1024 * 1024);
    final long previousBudget = GlobalConfiguration.QUERY_MAX_HEAP_RAM.getValueAsLong();
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(1024L);
    final QueryHeapTracker otherQuery = new QueryHeapTracker();
    try {
      // WITH THE BUDGET FREE THE BATCH IS THE ONE THE SETTING ALLOWS
      assertThat(batchOf("Large")).isGreaterThan(10);

      // THE RUNNING QUERIES TAKE ALL BUT A FEW MEGABYTES: AN ITERATOR MAY READ AHEAD A SMALL SHARE OF WHAT IS LEFT, WHICH IS LESS
      // THAN ONE RECORD, AND A BATCH ALWAYS HOLDS ONE
      otherQuery.charge(QueryHeapBudget.getAvailableBytes() - 4L * 1024 * 1024, "test");
      final BucketIterator iterator = openIterator("Large");
      assertThat(lastBatch).isEqualTo(1);

      // STILL EVERY RECORD, IN ORDER
      int count = 0;
      while (iterator.hasNext()) {
        assertThat(((Document) iterator.next()).getInteger("id")).isEqualTo(count);
        ++count;
      }
      assertThat(count).isEqualTo(LARGE_RECORDS);
    } finally {
      otherQuery.close();
      GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(previousBudget);
    }
  }

  @Test
  void aProfiledScanReportsTheReadAheadTheBudgetReduced() {
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_BATCH_MAX_BYTES, 4L * 1024 * 1024);
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, false);
    final String sql = "PROFILE SELECT id FROM Large";
    final String cypher = "PROFILE MATCH (n:Large) RETURN n.id AS id";
    // A CLAUSE ORDER THE OPTIMIZER DECLINES, WHICH TAKES THE STEP-BY-STEP PATH
    final String legacyCypher = "PROFILE UNWIND [1] AS x MATCH (n:Large) RETURN n.id AS id";

    // WITH THE BUDGET FREE A PROFILE SAYS NOTHING OF IT
    assertThat(profile("sql", sql)).contains("FETCH FROM BUCKET").doesNotContain("read-ahead reduced");
    assertThat(profile("opencypher", cypher)).contains("NodeByLabelScan").doesNotContain("read-ahead reduced");
    assertThat(profile("opencypher", legacyCypher)).contains("MATCH NODE").doesNotContain("read-ahead reduced");

    final long previousBudget = GlobalConfiguration.QUERY_MAX_HEAP_RAM.getValueAsLong();
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(1024L);
    final QueryHeapTracker otherQuery = new QueryHeapTracker();
    try {
      otherQuery.charge(QueryHeapBudget.getAvailableBytes() - 4L * 1024 * 1024, "test");
      final long shrunkBefore = QueryHeapBudget.getScanBatchesShrunk();

      // WITH THE BUDGET NEARLY FULL THE SLOW SCAN NAMES ITS CAUSE, IN SQL AND IN OPENCYPHER
      assertThat(profile("sql", sql)).contains("read-ahead reduced in").contains("by memory pressure");
      assertThat(profile("opencypher", cypher)).contains("read-ahead reduced in").contains("by memory pressure");
      assertThat(profile("opencypher", legacyCypher)).contains("MATCH NODE").contains("read-ahead reduced in");

      // AND THE SERVER PROFILER COUNTS THEM
      assertThat(QueryHeapBudget.getScanBatchesShrunk()).isGreaterThan(shrunkBefore);
      assertThat(Profiler.INSTANCE.toJSON().toString()).contains("queryHeapScanShrinks");
    } finally {
      otherQuery.close();
      GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(previousBudget);
    }
  }

  @Test
  void theIteratorsCountTheBatchesTheBudgetReduced() {
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_BATCH_MAX_BYTES, 4L * 1024 * 1024);
    final long previousBudget = GlobalConfiguration.QUERY_MAX_HEAP_RAM.getValueAsLong();
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(1024L);
    final QueryHeapTracker otherQuery = new QueryHeapTracker();
    try {
      final BucketIterator free = openIterator("Large");
      drain(free);
      assertThat(free.getBudgetShrunkBatches()).isZero();

      otherQuery.charge(QueryHeapBudget.getAvailableBytes() - 4L * 1024 * 1024, "test");
      final BucketIterator pressed = openIterator("Large");
      drain(pressed);
      // ONE BATCH PER RECORD, UNDER PRESSURE
      assertThat(pressed.getBudgetShrunkBatches()).isGreaterThanOrEqualTo(LARGE_RECORDS);

      // A SCAN OF A TYPE SUMS THE ITERATORS OF ITS BUCKETS
      final MultiIterator<Record> scan = (MultiIterator<Record>) database.iterateType("Large", true);
      drain(scan);
      assertThat(scan.getBudgetShrunkBatches()).isGreaterThanOrEqualTo(LARGE_RECORDS);
    } finally {
      otherQuery.close();
      GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(previousBudget);
    }
  }

  @Test
  void aFullBudgetDoesNotShortenABatchOfRecordsThatCopyNothing() {
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_BATCH_MAX_BYTES, 1024L * 1024);
    final long previousBudget = GlobalConfiguration.QUERY_MAX_HEAP_RAM.getValueAsLong();
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(1024L);
    final QueryHeapTracker otherQuery = new QueryHeapTracker();
    try {
      // THE BUDGET IS TAKEN TO ITS LAST KILOBYTES: THE SHARE OF AN ITERATOR IS FAR BELOW ONE LARGE RECORD
      otherQuery.charge(QueryHeapBudget.getAvailableBytes() - 1024L, "test");
      assertThat(QueryHeapBudget.getAvailableBytes() / 64).isLessThan(64L * 1024);

      // RECORDS ON THEIR OWN PAGE COPY NOTHING: THE BATCH IS STILL THE COUNT'S, AND THE BUDGET REDUCED NOTHING
      final long shrunkBefore = QueryHeapBudget.getScanBatchesShrunk();
      final BucketIterator small = openIterator("Small");
      assertThat(lastBatch).isEqualTo(1_024);
      drain(small);
      assertThat(small.getBudgetShrunkBatches()).isZero();
      assertThat(QueryHeapBudget.getScanBatchesShrunk()).isEqualTo(shrunkBefore);

      // LARGE RECORDS ARE COPIED: ONE PER BATCH, AND EACH BATCH IS A REDUCED ONE
      final BucketIterator large = openIterator("Large");
      assertThat(lastBatch).isEqualTo(1);
      drain(large);
      assertThat(large.getBudgetShrunkBatches()).isGreaterThanOrEqualTo(LARGE_RECORDS);
    } finally {
      otherQuery.close();
      GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(previousBudget);
    }
  }

  @Test
  void aDisabledBudgetLeavesTheConfiguredBound() {
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_BATCH_MAX_BYTES, 1024L * 1024);
    final long previousBudget = GlobalConfiguration.QUERY_MAX_HEAP_RAM.getValueAsLong();
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(0L);
    try {
      assertThat(QueryHeapBudget.getAvailableBytes()).isEqualTo(Long.MAX_VALUE);
      // THE BOUND IS THE SETTING, NOT A SHARE OF A BUDGET THAT IS NOT THERE
      final long prefetched = batchOf("Large");
      assertThat(prefetched).isGreaterThan(1).isLessThanOrEqualTo(1024L * 1024 / LARGE_PAYLOAD + 1);
    } finally {
      GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(previousBudget);
    }
  }

  @Test
  void aBackwardScanAndAScanOfPositionsAreBoundedToo() {
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_BATCH_MAX_BYTES, 1024L * 1024);
    final LocalBucket bucket = (LocalBucket) database.getSchema().getType("Large").getBuckets(false).getFirst();
    final long bound = 1024L * 1024 / LARGE_PAYLOAD + 1;

    // BACKWARD: THE LAST RECORD FIRST, EVERY RECORD ONCE
    long before = recordsRead();
    final Iterator<Record> backward = bucket.inverseIterator();
    backward.hasNext();
    assertThat(recordsRead() - before).isGreaterThan(0).isLessThanOrEqualTo(bound);
    int expected = LARGE_RECORDS - 1;
    while (backward.hasNext()) {
      assertThat(((Document) backward.next()).getInteger("id")).isEqualTo(expected);
      --expected;
    }
    assertThat(expected).isEqualTo(-1);

    // POSITIONS: THE RECORDS AT THE GIVEN POSITIONS, IN ORDER
    final long[] positions = new long[LARGE_RECORDS];
    final Iterator<Record> all = bucket.iterator();
    for (int i = 0; i < LARGE_RECORDS; i++)
      positions[i] = all.next().getIdentity().getPosition();
    before = recordsRead();
    final BucketIterator byPosition = bucket.iterator(positions, 0, positions.length);
    byPosition.hasNext();
    assertThat(recordsRead() - before).isGreaterThan(0).isLessThanOrEqualTo(bound);
    int count = 0;
    while (byPosition.hasNext()) {
      assertThat(((Document) byPosition.next()).getInteger("id")).isEqualTo(count);
      ++count;
    }
    assertThat(count).isEqualTo(LARGE_RECORDS);
  }

  @Test
  void aScanOfATypeReadsOneBucketBatchAtATime() {
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_BATCH_MAX_BYTES, 1024L * 1024);
    database.getSchema().createDocumentType("Spread", 4);
    database.transaction(() -> {
      for (int i = 0; i < 40; i++)
        database.newDocument("Spread").set("id", i, "payload", "x".repeat(LARGE_PAYLOAD)).save();
    });

    // AN ITERATOR PER BUCKET IS CREATED UP FRONT: NONE OF THEM READS A RECORD UNTIL THE SCAN GETS TO IT
    final long before = recordsRead();
    final Iterator<Record> scan = database.iterateType("Spread", true);
    assertThat(recordsRead() - before).isZero();

    // THE FIRST READ FILLS THE BATCH OF THE FIRST BUCKET ONLY
    assertThat(scan.hasNext()).isTrue();
    assertThat(recordsRead() - before).isGreaterThan(0).isLessThanOrEqualTo(1024L * 1024 / LARGE_PAYLOAD + 1);

    int count = 1;
    scan.next();
    while (scan.hasNext()) {
      scan.next();
      ++count;
    }
    assertThat(count).isEqualTo(40);
  }

  @Test
  void aTypeScanReadsTheRecordsAsTheyAreWhenItIsFirstRead() {
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_BATCH_MAX_BYTES, 1024L * 1024);

    // THE PAGES A SCAN COVERS ARE FIXED WHEN IT IS CREATED, BUT ITS FIRST BATCH IS READ WHEN IT IS FIRST READ: A RECORD CHANGED IN
    // BETWEEN IS RETURNED AS IT IS THEN, EXACTLY ONCE, AND EVERY RECORD THAT EXISTED WHEN THE SCAN WAS CREATED IS RETURNED
    final Iterator<Record> scan = database.iterateType("Large", true);
    database.transaction(() -> database.command("sql", "UPDATE Large SET marker = 'changed' WHERE id = 0").close());

    final java.util.Set<Integer> ids = new java.util.HashSet<>();
    boolean changedSeen = false;
    while (scan.hasNext()) {
      final Document record = (Document) scan.next();
      assertThat(ids.add(record.getInteger("id"))).as("record %d returned once", record.getInteger("id")).isTrue();
      if (record.getInteger("id") == 0)
        changedSeen = "changed".equals(record.getString("marker"));
    }
    assertThat(ids).hasSize(LARGE_RECORDS);
    assertThat(changedSeen).isTrue();
  }

  @Test
  void aPositionedIteratorReturnsTheRecordAtThePositionAndGoesOn() throws Exception {
    final LocalBucket bucket = (LocalBucket) database.getSchema().getType("Large").getBuckets(false).getFirst();
    final Iterator<Record> all = bucket.iterator();
    all.next();
    all.next();
    final Record third = all.next();

    final BucketIterator positioned = (BucketIterator) bucket.iterator();
    positioned.setPosition(third.getIdentity());
    // THE RECORD AT THE POSITION FIRST, THEN THE ONES AFTER IT
    int expected = ((Document) third).getInteger("id");
    int count = 0;
    while (positioned.hasNext()) {
      assertThat(((Document) positioned.next()).getInteger("id")).isEqualTo(expected);
      ++expected;
      ++count;
    }
    assertThat(count).isEqualTo(LARGE_RECORDS - 2);
  }

  @Test
  void aPositionedIteratorDropsTheBatchItAlreadyRead() throws Exception {
    final LocalBucket bucket = (LocalBucket) database.getSchema().getType("Large").getBuckets(false).getFirst();
    final Iterator<Record> all = bucket.iterator();
    final Record first = all.next();
    all.next();
    final Record third = all.next();

    // THE ITERATOR HAS READ ITS FIRST BATCH WHEN IT IS POSITIONED: THAT BATCH IS GONE, THE POSITIONED RECORD COMES NEXT
    final BucketIterator positioned = (BucketIterator) bucket.iterator();
    assertThat(positioned.hasNext()).isTrue();
    positioned.setPosition(third.getIdentity());
    assertThat(((Document) positioned.next()).getInteger("id")).isEqualTo(((Document) third).getInteger("id"));
    assertThat(((Document) positioned.next()).getInteger("id")).isEqualTo(((Document) third).getInteger("id") + 1);
    assertThat(((Document) first).getInteger("id")).isZero();
  }

  @Test
  void aDisabledBoundAlsoDisablesTheBudgetShrinking() {
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_BATCH_MAX_BYTES, 0L);
    final long previousBudget = GlobalConfiguration.QUERY_MAX_HEAP_RAM.getValueAsLong();
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(1024L);
    final QueryHeapTracker otherQuery = new QueryHeapTracker();
    try {
      otherQuery.charge(QueryHeapBudget.getAvailableBytes() - 4L * 1024 * 1024, "test");
      assertThat(batchOf("Large")).isEqualTo(LARGE_RECORDS);
      assertThat(lastBatch).isEqualTo(LARGE_RECORDS);
    } finally {
      otherQuery.close();
      GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(previousBudget);
    }
  }

  @Test
  void theScansShareAJvmWidePoolAndGiveItBack() {
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_BATCH_MAX_BYTES, 1024L * 1024);
    final long baseline = settledPool();

    // A BATCH READ AHEAD IS HELD IN THE POOL UNTIL IT IS HANDED OVER; TWO SCANS HOLD MORE THAN ONE
    final BucketIterator first = openIterator("Large");
    final long heldByOne = ScanReadAheadBudget.getReservedBytes() - baseline;
    assertThat(heldByOne).isGreaterThan(0).isLessThanOrEqualTo(2L * 1024 * 1024);
    final BucketIterator second = openIterator("Large");
    assertThat(ScanReadAheadBudget.getReservedBytes() - baseline).isGreaterThan(heldByOne);

    // ONCE BOTH ARE READ TO THE END THEY HOLD NOTHING
    drain(first);
    drain(second);
    assertThat(ScanReadAheadBudget.getReservedBytes()).isLessThanOrEqualTo(baseline);
  }

  @Test
  void aScanStoppedBeforeItsEndGivesItsBytesBackWhenTheStepCloses() {
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_BATCH_MAX_BYTES, 1024L * 1024);
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, false);
    final long baseline = settledPool();

    // A LIMIT STOPS THE SCAN AFTER ITS FIRST BATCH; CLOSING THE RESULT SET GIVES THE BYTES BACK AT ONCE, WITHOUT WAITING FOR THE GC
    final long[] heldWhileOpen = new long[1];
    try (final ResultSet rs = database.query("sql", "SELECT id FROM Large LIMIT 1")) {
      assertThat(rs.hasNext()).isTrue();
      heldWhileOpen[0] = ScanReadAheadBudget.getReservedBytes() - baseline;
      rs.next();
    }
    assertThat(heldWhileOpen[0]).isGreaterThan(0);
    assertThat(ScanReadAheadBudget.getReservedBytes()).isLessThanOrEqualTo(baseline);

    try (final ResultSet rs = database.query("opencypher", "UNWIND [1] AS x MATCH (n:Large) RETURN n.id AS id LIMIT 1")) {
      assertThat(rs.hasNext()).isTrue();
      rs.next();
    }
    assertThat(ScanReadAheadBudget.getReservedBytes()).isLessThanOrEqualTo(baseline);
  }

  @Test
  void aScanTheCallerAbandonedGivesItsBytesBackToThePool() {
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_BATCH_MAX_BYTES, 1024L * 1024);
    final long baseline = settledPool();

    // READ ONE BATCH AND DROP THE ITERATOR WITHOUT READING THE REST
    openIterator("Large");
    assertThat(ScanReadAheadBudget.getReservedBytes()).isGreaterThan(baseline);

    await().atMost(Duration.ofSeconds(20)).until(() -> {
      System.gc();
      return ScanReadAheadBudget.getReservedBytes() <= baseline;
    });
  }

  @Test
  void aPoolTakenByOthersStillReadsOneRecordAtATime() {
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_BATCH_MAX_BYTES, 1024L * 1024);
    final long previousPool = GlobalConfiguration.QUERY_SCAN_READ_AHEAD_MAX_RAM.getValueAsLong();
    GlobalConfiguration.QUERY_SCAN_READ_AHEAD_MAX_RAM.setValue(1L);
    final Object owner = new Object();
    final ScanReadAheadBudget.Reservation others = ScanReadAheadBudget.newReservation(owner);
    try {
      // THE POOL IS TAKEN, AND MORE: NOTHING IS LEFT
      others.reserve(512L * 1024 * 1024);
      assertThat(ScanReadAheadBudget.getAvailableBytes()).isZero();

      // WORST CASE: A BATCH OF ONE RECORD, AND EVERY RECORD STILL COMES BACK, IN ORDER
      final BucketIterator scan = openIterator("Large");
      assertThat(lastBatch).isEqualTo(1);
      int expected = 0;
      while (scan.hasNext()) {
        assertThat(((Document) scan.next()).getInteger("id")).isEqualTo(expected);
        ++expected;
      }
      assertThat(expected).isEqualTo(LARGE_RECORDS);
      assertThat(scan.getBudgetShrunkBatches()).isGreaterThanOrEqualTo(LARGE_RECORDS);
    } finally {
      others.release();
      GlobalConfiguration.QUERY_SCAN_READ_AHEAD_MAX_RAM.setValue(previousPool);
    }
  }

  @Test
  void aTinyOrNonPositiveLimitNeverStopsAScan() {
    final long previousPool = GlobalConfiguration.QUERY_SCAN_READ_AHEAD_MAX_RAM.getValueAsLong();
    try {
      // ONE BYTE: EVERY BATCH IS ONE RECORD, AND NOTHING IS LOST
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_BATCH_MAX_BYTES, 1L);
      assertThat(batchOf("Large")).isEqualTo(1);
      assertThat(countAll("Large")).isEqualTo(LARGE_RECORDS);

      // ZERO AND NEGATIVE VALUES ARE VALUES, NOT ERRORS: THEY SWITCH THE BOUND OFF, AND THE SCAN READS BY COUNT
      for (final long bound : new long[] { 0L, -1L, Long.MIN_VALUE }) {
        database.getConfiguration().setValue(GlobalConfiguration.QUERY_BATCH_MAX_BYTES, bound);
        assertThat(batchOf("Large")).isEqualTo(LARGE_RECORDS);
        assertThat(countAll("Large")).isEqualTo(LARGE_RECORDS);
      }

      // THE SAME FOR THE POOL: OFF, AND THE BOUND OF EACH SCAN STILL HOLDS
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_BATCH_MAX_BYTES, 1024L * 1024);
      for (final long pool : new long[] { 0L, -1L, Long.MIN_VALUE }) {
        GlobalConfiguration.QUERY_SCAN_READ_AHEAD_MAX_RAM.setValue(pool);
        assertThat(ScanReadAheadBudget.isEnabled()).isFalse();
        assertThat(ScanReadAheadBudget.getAvailableBytes()).isEqualTo(Long.MAX_VALUE);
        assertThat(batchOf("Large")).isGreaterThan(1).isLessThanOrEqualTo(1024L * 1024 / LARGE_PAYLOAD + 1);
        assertThat(countAll("Large")).isEqualTo(LARGE_RECORDS);
      }

      // A HUGE POOL IS NOT AN OVERFLOW
      GlobalConfiguration.QUERY_SCAN_READ_AHEAD_MAX_RAM.setValue(Long.MAX_VALUE);
      assertThat(ScanReadAheadBudget.getLimitBytes()).isEqualTo(Long.MAX_VALUE);
      assertThat(countAll("Large")).isEqualTo(LARGE_RECORDS);
    } finally {
      GlobalConfiguration.QUERY_SCAN_READ_AHEAD_MAX_RAM.setValue(previousPool);
    }
  }

  @Test
  void aNonPositiveBoundKeepsTheCountOnlyBatch() {
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_BATCH_MAX_BYTES, 0L);

    assertThat(batchOf("Large")).isEqualTo(LARGE_RECORDS);
  }

  @Test
  void smallRecordsStillBatchByCount() {
    // RECORDS ON THEIR OWN PAGE ARE VIEWS OF THE CACHED PAGE: THEY COPY NOTHING, SO THE BYTE BOUND NEVER SHORTENS THEIR BATCH
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_BATCH_MAX_BYTES, 1024L);

    final BucketIterator iterator = openIterator("Small");

    assertThat(lastBatch).isEqualTo(1_024);
    int count = 0;
    while (iterator.hasNext()) {
      iterator.next();
      ++count;
    }
    assertThat(count).isEqualTo(SMALL_RECORDS);
  }

  @Test
  void queriesReturnTheSameRowsWhateverTheBound() {
    final long expectedSum = (long) LARGE_RECORDS * (LARGE_RECORDS - 1) / 2;
    for (final long bound : new long[] { 1024L, 1024L * 1024, 0L }) {
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_BATCH_MAX_BYTES, bound);
      try (final ResultSet sql = database.query("sql", "SELECT count(*) AS c, sum(id) AS s FROM Large WHERE payload IS NOT NULL")) {
        final Result row = sql.next();
        assertThat(row.<Number>getProperty("c").longValue()).isEqualTo(LARGE_RECORDS);
        assertThat(row.<Number>getProperty("s").longValue()).isEqualTo(expectedSum);
      }
      try (final ResultSet cypher = database.query("opencypher", "MATCH (n:Large) RETURN count(n) AS c")) {
        assertThat(cypher.next().<Number>getProperty("c").longValue()).isEqualTo(LARGE_RECORDS);
      }
    }
  }

  /** Opens an iterator on the first bucket of the type and remembers how many records the scan read to fill its first batch. */
  private String profile(final String language, final String query) {
    try (final ResultSet rs = database.query(language, query)) {
      while (rs.hasNext())
        rs.next();
      return rs.getExecutionPlan().get().prettyPrint(0, 2);
    }
  }

  /**
   * What the pool holds once the scans earlier tests left behind are collected: the cleaner gives their bytes back when the garbage
   * collector gets to them, which would otherwise move the pool under a test that measures a difference.
   */
  private static long settledPool() {
    try {
      await().atMost(Duration.ofSeconds(10)).until(() -> {
        System.gc();
        return ScanReadAheadBudget.getReservedBytes() == 0L;
      });
    } catch (final ConditionTimeoutException e) {
      // SOMETHING ELSE IN THE JVM HOLDS READ-AHEAD: MEASURE FROM WHAT IT HOLDS
    }
    return ScanReadAheadBudget.getReservedBytes();
  }

  private int countAll(final String typeName) {
    int count = 0;
    final Iterator<Record> scan = database.iterateType(typeName, true);
    while (scan.hasNext()) {
      scan.next();
      ++count;
    }
    return count;
  }

  private static void drain(final Iterator<?> iterator) {
    while (iterator.hasNext())
      iterator.next();
  }

  private BucketIterator openIterator(final String typeName) {
    final DocumentType type = database.getSchema().getType(typeName);
    final long before = recordsRead();
    final BucketIterator iterator = (BucketIterator) ((LocalBucket) type.getBuckets(false).getFirst()).iterator();
    // THE FIRST BATCH IS READ WHEN THE ITERATOR IS FIRST READ
    iterator.hasNext();
    lastBatch = recordsRead() - before;
    return iterator;
  }

  private long batchOf(final String typeName) {
    openIterator(typeName);
    return lastBatch;
  }

  // THE RECORDS THE SCAN READ, AS THE DATABASE COUNTS THEM: WHAT IT LOADED, NOT HOW THE ITERATOR HOLDS THEM
  private long recordsRead() {
    return ((Number) database.getStats().get("readRecord")).longValue();
  }
}
