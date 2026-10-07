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
import org.junit.jupiter.api.Test;

import java.util.Iterator;

import static org.assertj.core.api.Assertions.assertThat;

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
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_BATCH_MAX_BYTES, 16L * 1024 * 1024);
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
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_BATCH_MAX_BYTES, 16L * 1024 * 1024);
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
      assertThat(profile("sql", sql)).contains("read-ahead reduced in").contains("query heap budget nearly full");
      assertThat(profile("opencypher", cypher)).contains("read-ahead reduced in").contains("query heap budget nearly full");
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
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_BATCH_MAX_BYTES, 16L * 1024 * 1024);
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
    assertThat(recordsRead() - before).isGreaterThan(0).isLessThanOrEqualTo(bound);
    int count = 0;
    while (byPosition.hasNext()) {
      assertThat(((Document) byPosition.next()).getInteger("id")).isEqualTo(count);
      ++count;
    }
    assertThat(count).isEqualTo(LARGE_RECORDS);
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

  private static void drain(final Iterator<?> iterator) {
    while (iterator.hasNext())
      iterator.next();
  }

  private BucketIterator openIterator(final String typeName) {
    final DocumentType type = database.getSchema().getType(typeName);
    final long before = recordsRead();
    final BucketIterator iterator = (BucketIterator) ((LocalBucket) type.getBuckets(false).getFirst()).iterator();
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
