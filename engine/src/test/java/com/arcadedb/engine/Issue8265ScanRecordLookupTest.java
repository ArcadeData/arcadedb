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
import com.arcadedb.database.BaseRecord;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.event.AfterRecordReadListener;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8265: a full scan resolved every in-page record through page lookups it did not need, and the SQL parallel
 * scan left the content load of every record to the single consumer thread.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8265ScanRecordLookupTest extends TestHelper {
  private static final int RECORDS = 10_000;

  /**
   * {@code BucketIterator.fetchNext()} already holds the page and the slot's size marker when it builds the record: the
   * {@code bucket.existsRecord(rid)} it used to call on top re-read the same marker through a fresh page-cache lookup,
   * once per record. Iterating lazy shells must now touch the page cache about once per PAGE, not once per record.
   */
  @Test
  void iteratingLazyShellsDoesNotLookUpEveryRecordAgain() {
    database.getSchema().createDocumentType("Scan8265");
    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++)
        database.newDocument("Scan8265").set("id", i).save();
    });

    final PageManager pageManager = ((DatabaseInternal) database).getPageManager();
    // WARM-UP PASS: LOADS EVERY PAGE INTO THE CACHE, SO THE MEASURED PASS BELOW COUNTS ONLY CACHE HITS
    assertThat(countShells()).isEqualTo(RECORDS);

    final long hitsBefore = pageManager.getStats().cacheHits;
    assertThat(countShells()).isEqualTo(RECORDS);
    final long hits = pageManager.getStats().cacheHits - hitsBefore;

    // A FEW DOZEN PAGES HOLD 10,000 SMALL RECORDS: BEFORE THE FIX THIS WAS >= RECORDS (ONE existsRecord() EACH)
    assertThat(hits).as("page-cache lookups to iterate %d lazy records", RECORDS).isLessThan(RECORDS / 10);
  }

  private int countShells() {
    int count = 0;
    final Iterator<Record> it = database.iterateType("Scan8265", false);
    while (it.hasNext()) {
      assertThat(it.next()).isNotNull();
      ++count;
    }
    return count;
  }

  /**
   * Dropping the existence re-check must not let a deleted record, an updated-and-moved record (placeholder) or a
   * multi-page record slip through or get lost, in either direction.
   */
  @Test
  void scanStillAnswersEveryLiveRecordExactlyOnce() {
    database.getSchema().createDocumentType("Mixed8265");
    final List<RID> rids = new ArrayList<>();
    database.transaction(() -> {
      for (int i = 0; i < 2_000; i++)
        rids.add(database.newDocument("Mixed8265").set("id", i).save().getIdentity());
    });

    final Set<Integer> expected = new HashSet<>();
    database.transaction(() -> {
      for (int i = 0; i < rids.size(); i++) {
        if (i % 7 == 0)
          rids.get(i).asDocument().delete();
        else {
          if (i % 11 == 0) {
            // GROWS PAST ITS SLOT: MOVES TO A PLACEHOLDER, AND THE 1 IN 3 THAT GETS 200KB SPANS MULTIPLE PAGES
            final MutableDocument doc = rids.get(i).asDocument().modify();
            doc.set("payload", "x".repeat(i % 3 == 0 ? 200_000 : 2_000));
            doc.save();
          }
          expected.add(i);
        }
      }
    });

    final LocalBucket bucket = (LocalBucket) database.getSchema().getType("Mixed8265").getBuckets(false).getFirst();
    for (final boolean forward : new boolean[] { true, false }) {
      final Set<Integer> seen = new HashSet<>();
      final Iterator<Record> it = forward ? bucket.iterator() : bucket.inverseIterator();
      while (it.hasNext())
        assertThat(seen.add(it.next().asDocument().getInteger("id"))).isTrue();
      assertThat(seen).as(forward ? "forward" : "backward").isEqualTo(expected);
    }
  }

  /**
   * The SQL parallel scan hands each record to one consumer thread. The producers used to hand over lazy shells, so
   * the content load the parallel scan exists to spread ran on the consumer; they now hand over loaded records.
   */
  @Test
  void parallelScanHandsOverLoadedRecords() {
    database.getSchema().buildDocumentType().withName("Par8265").withTotalBuckets(4).create();
    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++)
        database.newDocument("Par8265").set("id", i).save();
    });

    int count = 0;
    int unloaded = 0;
    final Set<Integer> ids = new HashSet<>();
    try (final ResultSet rs = database.query("sql", "select from Par8265")) {
      assertThat(rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2)).contains("(parallel)");
      while (rs.hasNext()) {
        final Result r = rs.next();
        final Document doc = r.toElement();
        // CHECKED BEFORE ANY PROPERTY IS READ ON THIS (THE CONSUMER) THREAD
        if (((BaseRecord) doc).getBuffer() == null)
          ++unloaded;
        ids.add(doc.getInteger("id"));
        ++count;
      }
    }
    assertThat(count).isEqualTo(RECORDS);
    assertThat(ids).hasSize(RECORDS);
    assertThat(unloaded).as("records the parallel scan handed over still unloaded").isZero();
  }

  /**
   * The producers now hand records over in batches. A consumer that pulls in pages (as a LIMIT, or any step pulling
   * less than a batch at a time, does) must neither lose nor repeat the rest of a batch it has already taken.
   */
  @Test
  void parallelScanBatchesSurviveSmallPulls() {
    database.getSchema().buildDocumentType().withName("Page8265").withTotalBuckets(4).create();
    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++)
        database.newDocument("Page8265").set("id", i).save();
    });

    for (final int limit : new int[] { 1, 7, 300, 4_097, RECORDS - 1, RECORDS, RECORDS + 5 }) {
      final Set<Integer> ids = new HashSet<>();
      int count = 0;
      try (final ResultSet rs = database.query("sql", "select id from Page8265 limit " + limit)) {
        while (rs.hasNext()) {
          ids.add(rs.next().<Integer>getProperty("id"));
          ++count;
        }
      }
      assertThat(count).as("limit %d", limit).isEqualTo(Math.min(limit, RECORDS));
      assertThat(ids).as("limit %d", limit).hasSize(count);
    }

    // A WHERE THAT DISCARDS MOST ROWS MAKES THE FILTER PULL FROM THE FETCH STEP REPEATEDLY, IN SMALL PAGES
    try (final ResultSet rs = database.query("sql", "select count(*) as n from Page8265 where id % 3 = 0")) {
      assertThat(rs.next().<Long>getProperty("n")).isEqualTo((RECORDS + 2) / 3);
    }
  }

  /**
   * Loading on the producer fires the after-read listeners there, and it must be the only time: the consumer reading the
   * properties afterwards must not load (and notify) a second time.
   */
  @Test
  void parallelScanNotifiesAfterReadOncePerRecord() {
    database.getSchema().buildDocumentType().withName("Filt8265").withTotalBuckets(4).create();
    database.transaction(() -> {
      for (int i = 0; i < 1_000; i++)
        database.newDocument("Filt8265").set("id", i).set("secret", i % 2 == 0).save();
    });

    final AtomicInteger calls = new AtomicInteger();
    final AfterRecordReadListener listener = record -> {
      calls.incrementAndGet();
      return record;
    };
    database.getEvents().registerListener(listener);
    try {
      try (final ResultSet rs = database.query("sql", "select count(*) as n from Filt8265 where secret = true")) {
        assertThat(rs.next().<Long>getProperty("n")).isEqualTo(500L);
      }
      assertThat(calls.get()).isEqualTo(1_000);
    } finally {
      database.getEvents().unregisterListener(listener);
    }
  }
}
