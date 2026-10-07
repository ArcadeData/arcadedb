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
import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.database.Record;
import com.arcadedb.query.sql.executor.QueryHeapBudget;
import com.arcadedb.query.sql.executor.QueryHeapTracker;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import org.junit.jupiter.api.Test;

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
    final int prefetched = prefetched(iterator);
    assertThat(prefetched).isGreaterThan(0).isLessThanOrEqualTo((int) (bound / LARGE_PAYLOAD) + 1);

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
      assertThat(prefetched(openIterator("Large"))).isGreaterThan(10);

      // THE RUNNING QUERIES TAKE ALL BUT A FEW MEGABYTES: AN ITERATOR MAY READ AHEAD A SMALL SHARE OF WHAT IS LEFT, WHICH IS LESS
      // THAN ONE RECORD, AND A BATCH ALWAYS HOLDS ONE
      otherQuery.charge(QueryHeapBudget.getAvailableBytes() - 4L * 1024 * 1024, "test");
      final BucketIterator iterator = openIterator("Large");
      assertThat(prefetched(iterator)).isEqualTo(1);

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
  void aNonPositiveBoundKeepsTheCountOnlyBatch() {
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_BATCH_MAX_BYTES, 0L);

    assertThat(prefetched(openIterator("Large"))).isEqualTo(LARGE_RECORDS);
  }

  @Test
  void smallRecordsStillBatchByCount() {
    // RECORDS ON THEIR OWN PAGE ARE VIEWS OF THE CACHED PAGE: THEY COPY NOTHING, SO THE BYTE BOUND NEVER SHORTENS THEIR BATCH
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_BATCH_MAX_BYTES, 1024L);

    final BucketIterator iterator = openIterator("Small");

    assertThat(prefetched(iterator)).isEqualTo(1_024);
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

  private BucketIterator openIterator(final String typeName) {
    final DocumentType type = database.getSchema().getType(typeName);
    return (BucketIterator) ((LocalBucket) type.getBuckets(false).getFirst()).iterator();
  }

  private static int prefetched(final BucketIterator iterator) {
    int count = 0;
    for (final Record record : iterator.nextBatch)
      if (record != null)
        ++count;
    return count;
  }
}
