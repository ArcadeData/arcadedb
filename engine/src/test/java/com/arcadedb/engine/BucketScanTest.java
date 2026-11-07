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
import com.arcadedb.database.BaseRecord;
import com.arcadedb.database.BucketPageLayoutTestSupport;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.DatabaseRID;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.event.AfterRecordReadListener;
import com.arcadedb.event.BeforeRecordReadListener;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.graph.GraphBatch;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONArray;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static com.arcadedb.database.Binary.INT_SERIALIZED_SIZE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Consolidated tests for BucketScan behaviours, one nested class per originally reported issue.
 */
class BucketScanTest {

  /**
   * Issue #8265: a full scan resolved every in-page record through page lookups it did not need, and the SQL parallel
   * scan left the content load of every record to the single consumer thread.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  // #8265
  @Nested
  class ScanRecordLookup extends TestHelper {
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

    /**
     * A record an {@link AfterRecordReadListener} filters away must not survive into the result set of a parallel scan,
     * even when nothing downstream ever asks for one of its properties. The row count below deliberately never calls
     * {@code getProperty()} on any returned row for exactly that reason: the earlier
     * {@link #parallelScanNotifiesAfterReadOncePerRecord()} test reads a column in its {@code WHERE}, which forces a
     * check on the consumer side too and would hide {@code loadContent()}'s return value being discarded - the row
     * would come back deleted-looking (no buffer) rather than simply not come back at all. Counting rows with nothing
     * ever read from them isolates the producer-side filtering this method exists for.
     */
    @Test
    void parallelScanDropsAfterReadFilteredRecords() {
      database.getSchema().buildDocumentType().withName("Leak8265").withTotalBuckets(4).create();
      database.transaction(() -> {
        for (int i = 0; i < 1_000; i++)
          database.newDocument("Leak8265").set("id", i).save();
      });

      // FILTERS AWAY EVERY 5TH RECORD, BY RETURNING null FROM THE LISTENER
      final AfterRecordReadListener listener = record -> record.asDocument().get("id") instanceof Integer id && id % 5 == 0
          ? null
          : record;
      database.getEvents().registerListener(listener);
      try (final ResultSet rs = database.query("sql", "select from Leak8265")) {
        assertThat(rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2)).contains("(parallel)");
        int count = 0;
        while (rs.hasNext()) {
          rs.next();
          ++count;
        }
        // BEFORE THE FIX THIS WAS 1000: loadContent()'S RETURN VALUE WAS DISCARDED, SO A FILTERED RECORD STAYED IN THE
        // BATCH AND REACHED THE CONSUMER, WHICH NEVER TOUCHED ANY PROPERTY OF IT HERE TO NOTICE
        assertThat(count).as("rows returned by a parallel scan with 1 in 5 records filtered away").isEqualTo(800);
      } finally {
        database.getEvents().unregisterListener(listener);
      }
    }

    /**
     * Eager producer-side loading means a batch retains fully-materialized record content instead of the lazy shells
     * the parallel-scan queue used to carry, so {@code SCAN_BATCH_SIZE} (a row count) no longer bounds a batch's
     * retained bytes for wide records. A cap far smaller than a single record forces every batch down to one row -
     * proof the byte bound is doing something (a plain row-count cap would need 256 such records to see any effect at
     * all) - and the scan must still make forward progress and return every record intact, not hang or truncate.
     */
    @Test
    void parallelScanCapsBatchBytesForWideRecords() {
      database.getSchema().buildDocumentType().withName("Wide8265").withTotalBuckets(4).create();
      final int recordCount = 300;
      final int payloadSize = 50_000; // WELL OVER database.getConfiguration()'s DEFAULT queryParallelScanMaxBatchBytes / SCAN_BATCH_SIZE
      database.transaction(() -> {
        for (int i = 0; i < recordCount; i++)
          database.newDocument("Wide8265").set("id", i).set("payload", "x".repeat(payloadSize)).save();
      });

      // FAR SMALLER THAN ONE payloadSize-SIZED RECORD: EVERY BATCH THE PRODUCER BUILDS MUST STOP AT ONE ROW
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN_MAX_BATCH_BYTES, 1_000L);
      try {
        final Set<Integer> ids = new HashSet<>();
        try (final ResultSet rs = database.query("sql", "select from Wide8265")) {
          assertThat(rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2)).contains("(parallel)");
          while (rs.hasNext()) {
            final Result r = rs.next();
            assertThat(r.<String>getProperty("payload")).hasSize(payloadSize);
            ids.add(r.<Integer>getProperty("id"));
          }
        }
        assertThat(ids).hasSize(recordCount);
      } finally {
        database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN_MAX_BATCH_BYTES,
            GlobalConfiguration.QUERY_PARALLEL_SCAN_MAX_BATCH_BYTES.getDefValue());
      }
    }

    /**
     * A non-positive byte bound disables it rather than admitting no row at all: taken literally as a limit it stopped every
     * batch before its first row, and the producers spun forever handing over nothing.
     */
    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void parallelScanTreatsNonPositiveBatchBytesAsUnbounded() {
      database.getSchema().buildDocumentType().withName("NoCap8265").withTotalBuckets(4).create();
      database.transaction(() -> {
        for (int i = 0; i < 2_000; i++)
          database.newDocument("NoCap8265").set("id", i).save();
      });

      try {
        for (final long cap : new long[] { 0L, -1L }) {
          database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN_MAX_BATCH_BYTES, cap);
          try (final ResultSet rs = database.query("sql", "select count(*) as n from NoCap8265 where id >= 0")) {
            assertThat(rs.next().<Long>getProperty("n")).as("cap %d", cap).isEqualTo(2_000L);
          }
        }
      } finally {
        database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN_MAX_BATCH_BYTES,
            GlobalConfiguration.QUERY_PARALLEL_SCAN_MAX_BATCH_BYTES.getDefValue());
      }
    }

    /**
     * With an after-read listener hiding almost every row, a producer fills its batches from far more rows than it keeps.
     * The rows read under one read-lock acquisition are bounded by rows examined, not rows kept, so the scan comes back for
     * more in short (often empty) batches, and must still return exactly the rows the listener lets through.
     */
    @Test
    void parallelScanWithMostRowsFilteredReturnsTheSurvivors() {
      database.getSchema().buildDocumentType().withName("Sparse8265").withTotalBuckets(4).create();
      database.transaction(() -> {
        for (int i = 0; i < 5_000; i++)
          database.newDocument("Sparse8265").set("id", i).save();
      });

      final AfterRecordReadListener listener = record -> record.asDocument().getInteger("id") % 100 == 0 ? record : null;
      database.getEvents().registerListener(listener);
      try {
        final Set<Integer> ids = new HashSet<>();
        try (final ResultSet rs = database.query("sql", "select from Sparse8265")) {
          while (rs.hasNext())
            ids.add(rs.next().<Integer>getProperty("id"));
        }
        assertThat(ids).hasSize(50).allMatch(id -> id % 100 == 0);
      } finally {
        database.getEvents().unregisterListener(listener);
      }
    }
  }

  /**
   * Issue #8312: a full scan builds each record from the page it already holds, instead of handing out a lazy shell that
   * re-reads the record through a second page lookup on its first property access. {@code modify()} no longer reloads
   * a record whose content is still the one on the current page version, so "scan, then modify" reads the record once.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  // #8312
  @Nested
  class ScanEagerRecord extends BucketPageLayoutTestSupport {
    private static final int RECORDS = 10_000;

    @Override
    protected void beginTest() {
      database.getSchema().createDocumentType("Doc8312");
      database.getSchema().createVertexType("V8312");
      database.getSchema().createEdgeType("E8312");
    }

    /**
     * Reading a property of every scanned record used to cost one more page-cache lookup per record (the lazy load).
     */
    @Test
    void readingPropertiesOfScannedRecordsDoesNotLookThemUpAgain() {
      database.transaction(() -> {
        for (int i = 0; i < RECORDS; i++)
          database.newDocument("Doc8312").set("id", i).save();
      });

      final PageManager pageManager = ((DatabaseInternal) database).getPageManager();
      assertThat(sumIds()).isEqualTo((long) RECORDS * (RECORDS - 1) / 2); // WARM-UP: EVERY PAGE CACHED

      final long hitsBefore = pageManager.getStats().cacheHits;
      assertThat(sumIds()).isEqualTo((long) RECORDS * (RECORDS - 1) / 2);
      final long hits = pageManager.getStats().cacheHits - hitsBefore;

      assertThat(hits).as("page-cache lookups to scan %d records and read a property of each", RECORDS)
          .isLessThan(RECORDS / 10);
    }

    private RID saved(final Supplier<Record> creation) {
      final RID[] rid = new RID[1];
      database.transaction(() -> rid[0] = creation.get().getIdentity());
      return rid[0];
    }

    private long sumIds() {
      long sum = 0;
      final Iterator<Record> it = database.iterateType("Doc8312", false);
      while (it.hasNext())
        sum += it.next().asDocument().getInteger("id");
      return sum;
    }

    /**
     * Scan, then modify: the record's read events fire once. The content the scan built is still the one on the page, so
     * modify() must not reload it (which would notify the listeners a second time).
     */
    @Test
    void scanThenModifyNotifiesReadListenersOnce() {
      database.transaction(() -> {
        database.newVertex("V8312").set("id", 1).save();
        database.newDocument("Doc8312").set("id", 1).save();
      });

      final AtomicInteger before = new AtomicInteger();
      final AtomicInteger after = new AtomicInteger();
      final BeforeRecordReadListener beforeListener = rid -> {
        before.incrementAndGet();
        return true;
      };
      final AfterRecordReadListener afterListener = record -> {
        after.incrementAndGet();
        return record;
      };
      database.getEvents().registerListener(beforeListener);
      database.getEvents().registerListener(afterListener);
      try {
        for (final String type : new String[] { "V8312", "Doc8312" }) {
          before.set(0);
          after.set(0);
          database.transaction(() -> {
            final MutableDocument modified = database.iterateType(type, false).next().asDocument().modify();
            modified.set("modified", true).save();
          });
          assertThat(before.get()).as("before-read on %s", type).isEqualTo(1);
          assertThat(after.get()).as("after-read on %s", type).isEqualTo(1);
        }
      } finally {
        database.getEvents().unregisterListener(beforeListener);
        database.getEvents().unregisterListener(afterListener);
      }

      database.transaction(() -> {
        assertThat(database.iterateType("V8312", false).next().asDocument().getBoolean("modified")).isTrue();
        assertThat(database.iterateType("Doc8312", false).next().asDocument().getBoolean("modified")).isTrue();
      });
    }

    /**
     * The reload modify() skips is only skippable while the page is unchanged: a commit landing between the scan and the
     * modify() must be seen, or the modify would write back the stale content and silently undo it. Since #8610 a
     * property write on that record is refused as a retryable conflict, since it may be computed from the stale scan;
     * the concurrent commit survives either way, and the retry, reading again, applies the write.
     */
    @Test
    void modifyAfterAConcurrentCommitStillSeesIt() throws Exception {
      final RID rid = saved(() -> database.newVertex("V8312").set("id", 1).save());

      database.begin();
      final Vertex scanned = database.iterateType("V8312", false).next().asVertex();
      assertThat(scanned.getIdentity()).isEqualTo(rid);

      // ANOTHER THREAD COMMITS A CHANGE TO THE SAME RECORD AFTER THE SCAN READ IT
      final Thread other = new Thread(() -> database.transaction(() -> rid.asVertex().modify().set("other", "yes").save()));
      other.start();
      other.join();

      final MutableVertex modified = scanned.modify();
      assertThat(modified.getString("other")).isEqualTo("yes");
      assertThatThrownBy(() -> modified.set("mine", "yes").save()).isInstanceOf(ConcurrentModificationException.class);
      database.rollback();

      database.transaction(() -> database.iterateType("V8312", false).next().asVertex().modify().set("mine", "yes").save());

      final Vertex reloaded = rid.asVertex();
      assertThat(reloaded.getString("other")).isEqualTo("yes");
      assertThat(reloaded.getString("mine")).isEqualTo("yes");
    }

    /**
     * The same guarantee for a document and an edge: their content now comes from the scan, not from a lazy load at
     * modify() time, so modify() has to notice the page moved on and reload, as it does for a vertex. The property write
     * that follows is refused as a retryable conflict (#8610), and the retry applies it.
     */
    @Test
    void modifyOfDocumentAndEdgeAfterAConcurrentCommitStillSeesIt() throws Exception {
      final RID docRid = saved(() -> database.newDocument("Doc8312").set("id", 1).save());
      final RID[] edgeRid = new RID[1];
      database.transaction(() -> {
        final MutableVertex a = database.newVertex("V8312").set("id", 10).save();
        final MutableVertex b = database.newVertex("V8312").set("id", 11).save();
        edgeRid[0] = a.newEdge("E8312", b).getIdentity();
      });

      for (final RID rid : new RID[] { docRid, edgeRid[0] }) {
        final String type = rid.equals(docRid) ? "Doc8312" : "E8312";
        database.begin();
        final Document scanned = database.iterateType(type, false).next().asDocument();
        assertThat(scanned.getIdentity()).isEqualTo(rid);

        final Thread other = new Thread(
            () -> database.transaction(() -> rid.asDocument().modify().set("other", "yes").save()));
        other.start();
        other.join();

        final MutableDocument modified = scanned.modify();
        assertThat(modified.getString("other")).as("%s sees the concurrent commit", type).isEqualTo("yes");
        assertThatThrownBy(() -> modified.set("mine", "yes").save()).as(type)
            .isInstanceOf(ConcurrentModificationException.class);
        database.rollback();

        database.transaction(() -> database.iterateType(type, false).next().asDocument().modify().set("mine", "yes").save());

        final Document reloaded = rid.asDocument();
        assertThat(reloaded.getString("other")).as(type).isEqualTo("yes");
        assertThat(reloaded.getString("mine")).as(type).isEqualTo("yes");
      }
    }

    /**
     * A record modified earlier in the same transaction is answered from the transaction, not from the page.
     */
    @Test
    void scanInsideATransactionReturnsItsOwnChanges() {
      final RID rid = saved(() -> database.newDocument("Doc8312").set("id", 1).save());

      database.transaction(() -> {
        rid.asDocument().modify().set("id", 2).save();
        final Document scanned = database.iterateType("Doc8312", false).next().asDocument();
        assertThat(scanned.getInteger("id")).isEqualTo(2);
      });
    }

    /**
     * An after-read listener that transforms the record (the shape of the encryption hook) applies to every record a scan
     * returns: one stored in its own slot, one that moved to a placeholder after an update made it grow, and a multi-page
     * one. A record the listener filters away is not returned.
     */
    @Test
    void afterReadAppliesToEveryScannedRecordShape() {
      // THE LAYOUT THAT FORCES A PLACEHOLDER (#6149): A TINY RECORD ON PAGE 0, PAGE 0 SEALED WITH NO FREE TAIL (ITS LAST
      // RECORD SPILLS INTO CHUNKS: THE MULTI-PAGE ONE), THEN THE TINY RECORD GROWN PAST WHAT THE PAGE CAN HOST
      final RID[] tiny = new RID[1];
      database.transaction(() -> {
        database.getSchema().createDocumentType("Shape8312", 1).createProperty("v", Type.STRING);
        tiny[0] = database.newDocument("Shape8312").set("v", "p").save().getIdentity();
      });
      final RID sealing = sealFirstPage("Shape8312");
      database.transaction(() -> tiny[0].asDocument(true).modify().set("v", "b".repeat(20 * 1024)).save());
      database.transaction(() -> {
        for (int i = 0; i < 10; i++)
          database.newDocument("Shape8312").set("v", "small").set("hidden", i % 2 == 0).save();
      });

      // THE FIXTURE MUST REALLY HOLD EVERY SHAPE, OR THIS TEST PROVES NOTHING ABOUT THE ONES IT LACKS
      final Map<String, Object> layout = bucketStats("Shape8312");
      assertThat((Long) layout.get("totalPlaceholderRecords")).as("placeholder records: %s", layout).isPositive();
      assertThat((Long) layout.get("totalMultiPageRecords")).as("multi-page records: %s", layout).isPositive();
      // EVERY RECORD OF THE TYPE, COUNTED BY A SCAN WITH NO LISTENER REGISTERED
      int total = 0;
      for (final Iterator<Record> all = database.iterateType("Shape8312", false); all.hasNext(); all.next())
        ++total;

      final AfterRecordReadListener listener = record -> {
        final Document doc = record.asDocument();
        if (Boolean.TRUE.equals(doc.getBoolean("hidden")))
          return null;
        return doc.modify().set("secret", "decrypted");
      };
      database.getEvents().registerListener(listener);
      try {
        final Set<RID> seen = new HashSet<>();
        final Iterator<Record> it = database.iterateType("Shape8312", false);
        while (it.hasNext()) {
          final Document doc = it.next().asDocument();
          assertThat(doc.getString("secret")).as("record %s", doc.getIdentity()).isEqualTo("decrypted");
          assertThat(doc.getBoolean("hidden")).as("record %s", doc.getIdentity()).isNotEqualTo(Boolean.TRUE);
          assertThat(seen.add(doc.getIdentity())).isTrue();
        }
        assertThat(seen).hasSize(total - 5).contains(tiny[0], sealing);
      } finally {
        database.getEvents().unregisterListener(listener);
      }
    }

    /**
     * A scanned record answers the same database instance as one looked up by RID, so that anything the record does
     * through it (modify, save, graph operations) goes through the same wrapper the lookup would give it.
     */
    @Test
    void scannedRecordsBelongToTheSameDatabaseInstanceAsLookedUpOnes() {
      final RID rid = saved(() -> database.newDocument("Doc8312").set("id", 1).save());
      final Document scanned = database.iterateType("Doc8312", false).next().asDocument();
      assertThat(scanned.getDatabase()).isSameAs(database.lookupByRID(rid, true).getDatabase());
    }

    /**
     * The scan no longer goes through lookupByRID(), which is what counted records into the readRecord statistic: it has
     * to report them itself, one per scanned record.
     */
    @Test
    void scannedRecordsAreCountedInTheReadRecordStatistic() {
      database.transaction(() -> {
        for (int i = 0; i < 3_000; i++)
          database.newDocument("Doc8312").set("id", i).save();
      });

      final long before = ((Number) database.getStats().get("readRecord")).longValue();
      int scanned = 0;
      final Iterator<Record> it = database.iterateType("Doc8312", false);
      while (it.hasNext()) {
        it.next();
        ++scanned;
      }
      assertThat(scanned).isEqualTo(3_000);
      assertThat(((Number) database.getStats().get("readRecord")).longValue() - before).isEqualTo(3_000L);

      // A RECORD A BEFORE-READ EVENT HIDES WAS STILL READ: lookupByRID() COUNTS IT, AND SO DOES THE SCAN
      final BeforeRecordReadListener hideAll = rid -> false;
      database.getEvents().registerListener(hideAll);
      try {
        final long beforeHidden = ((Number) database.getStats().get("readRecord")).longValue();
        assertThat(database.iterateType("Doc8312", false).hasNext()).isFalse();
        assertThat(((Number) database.getStats().get("readRecord")).longValue() - beforeHidden).isEqualTo(3_000L);
      } finally {
        database.getEvents().unregisterListener(hideAll);
      }
    }

    /**
     * A record that moved to a placeholder is notified to a before-read listener once, under its own RID, whether it is
     * scanned or looked up. The loader used to notify it a second time under the internal position of its content.
     */
    @Test
    void placeholderRecordIsNotifiedOnceUnderItsOwnRid() {
      final RID[] tiny = new RID[1];
      database.transaction(() -> {
        database.getSchema().createDocumentType("Ph8312", 1).createProperty("v", Type.STRING);
        tiny[0] = database.newDocument("Ph8312").set("v", "p").save().getIdentity();
      });
      sealFirstPage("Ph8312");
      database.transaction(() -> tiny[0].asDocument(true).modify().set("v", "b".repeat(20 * 1024)).save());
      assertThat((Long) bucketStats("Ph8312").get("totalPlaceholderRecords")).isPositive();

      final List<RID> notified = new ArrayList<>();
      final BeforeRecordReadListener listener = rid -> {
        notified.add(rid);
        return true;
      };
      database.getEvents().registerListener(listener);
      try {
        final List<RID> scanned = new ArrayList<>();
        final Iterator<Record> it = database.iterateType("Ph8312", false);
        while (it.hasNext())
          scanned.add(it.next().getIdentity());
        // EXACTLY ONE NOTIFICATION PER RECORD RETURNED, UNDER THAT RECORD'S RID: NONE UNDER THE CONTENT'S INTERNAL ONE
        assertThat(scanned).contains(tiny[0]);
        assertThat(notified).as("scan").containsExactlyInAnyOrderElementsOf(scanned);

        notified.clear();
        database.lookupByRID(tiny[0], true);
        assertThat(notified).as("lookup").containsExactly(tiny[0]);
      } finally {
        database.getEvents().unregisterListener(listener);
      }
    }
  }

  /**
   * #10655 item 3: a scan built a bare {@link RID} per slot and the record then copied it into a {@link DatabaseRID}. The scan now
   * builds the {@link DatabaseRID} once. The identity a scanned record hands out must stay what it was: a {@link DatabaseRID} bound
   * to the record's database, equal to and hashing as the bare RID, and usable after the scan.
   */
  // #10655
  @Nested
  class ScanRid extends TestHelper {
    private static final int ROWS = 300;

    @Test
    void scannedDocumentsKeepTheirIdentity() {
      final DocumentType type = database.getSchema().createDocumentType("Doc10655", 1);
      final Set<RID> saved = new HashSet<>();
      database.transaction(() -> {
        for (int i = 0; i < ROWS; i++)
          saved.add(database.newDocument("Doc10655").set("id", i).save().getIdentity());
      });
      assertThat(saved).hasSize(ROWS);

      final DatabaseInternal bound = ((DatabaseInternal) database).getWrappedDatabaseInstance();
      final Set<RID> scanned = new HashSet<>();
      final LocalBucket bucket = (LocalBucket) type.getBuckets(false).getFirst();
      final Iterator<Record> iterator = bucket.iterator();
      while (iterator.hasNext()) {
        final Record record = iterator.next();
        final RID rid = record.getIdentity();
        assertThat(rid).isInstanceOf(DatabaseRID.class);
        assertThat(((DatabaseRID) rid).getBoundDatabase()).isSameAs(bound);
        assertThat(record.getIdentity()).as("the identity is one object, not rebuilt on each call").isSameAs(rid);
        assertThat(rid).isEqualTo(new RID(rid.getBucketId(), rid.getPosition()));
        assertThat(new RID(rid.getBucketId(), rid.getPosition())).isEqualTo(rid);
        assertThat(rid.hashCode()).isEqualTo(new RID(rid.getBucketId(), rid.getPosition()).hashCode());
        scanned.add(rid);
      }
      assertThat(scanned).isEqualTo(saved);

      // THE RIDS WORK AFTER THE SCAN: THEY RESOLVE AGAINST THEIR DATABASE
      for (final RID rid : scanned)
        assertThat(rid.asDocument().getInteger("id")).isNotNull();
    }

    @Test
    void rowsOfAQueryAndATransactionCachedRecordKeepTheirIdentity() {
      database.getSchema().createVertexType("V10655", 1);
      database.transaction(() -> {
        for (int i = 0; i < 50; i++)
          database.newVertex("V10655").set("id", i).save();
      });

      // THE SQL SCAN GOES THROUGH BucketIterator TOO; A RECORD MODIFIED IN THE OPEN TRANSACTION IS ANSWERED FROM IT, NOT FROM THE PAGE
      database.begin();
      try {
        final RID first = database.query("sql", "SELECT FROM V10655 WHERE id = 0").next().getIdentity().orElseThrow();
        final MutableDocument modified = first.asVertex().modify().set("id", 1000);
        modified.save();

        int count = 0;
        boolean sawModified = false;
        final var rs = database.query("sql", "SELECT FROM V10655");
        while (rs.hasNext()) {
          final Vertex vertex = rs.next().getVertex().orElseThrow();
          assertThat(vertex.getIdentity()).isInstanceOf(DatabaseRID.class);
          assertThat(vertex.getIdentity().asVertex().getIdentity()).isEqualTo(vertex.getIdentity());
          if (vertex.getIdentity().equals(first)) {
            sawModified = true;
            assertThat(vertex.getInteger("id")).isEqualTo(1000);
          }
          count++;
        }
        assertThat(count).isEqualTo(50);
        assertThat(sawModified).isTrue();
      } finally {
        database.rollback();
      }
    }

    @Test
    void scannedRecordsAreBoundToTheirOwnDatabaseWhenTwoAreOpen() {
      database.getSchema().createDocumentType("Doc10655", 1);
      database.transaction(() -> database.newDocument("Doc10655").set("id", 1).save());

      final DatabaseFactory otherFactory = new DatabaseFactory("target/databases/Issue10655Other");
      if (otherFactory.exists())
        otherFactory.open().drop();
      final Database other = otherFactory.create();
      try {
        other.getSchema().createDocumentType("Doc10655", 1);
        other.transaction(() -> other.newDocument("Doc10655").set("id", 2).save());

        for (final Database db : new Database[] { database, other }) {
          final Iterator<Record> iterator = db.iterateType("Doc10655", true);
          assertThat(iterator.hasNext()).isTrue();
          final Document document = (Document) iterator.next();
          assertThat(((DatabaseRID) document.getIdentity()).getBoundDatabase())
              .isSameAs(((DatabaseInternal) db).getWrappedDatabaseInstance());
          assertThat(document.getIdentity().asDocument().getInteger("id")).isEqualTo(document.getInteger("id"));
        }
      } finally {
        other.drop();
      }
    }
  }

  /**
   * Issue #11012: {@link LocalBucket#forEachRecordPosition} answers the positions a scan of the bucket hands out, so a slot
   * whose record-table entry is corrupted is skipped, as {@link BucketIterator} skips it, instead of aborting the walk.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  // #11012
  @Nested
  class RecordPositionsCorruptedSlot extends TestHelper {

    @Override
    protected boolean isCheckingDatabaseIntegrity() {
      // The test injects the corruption on purpose
      return false;
    }

    @Test
    void aCorruptedSlotIsSkippedAndTheOtherPositionsAreAnswered() {
      final DocumentType type = database.getSchema().createDocumentType("Positions", 1);
      final List<RID> rids = new ArrayList<>();
      database.transaction(() -> {
        for (int i = 0; i < 3; i++)
          rids.add(database.newDocument("Positions").set("id", i).save().getIdentity());
      });

      final LocalBucket bucket = (LocalBucket) type.getBuckets(false).getFirst();
      database.transaction(() -> {
        try {
          final MutablePage page = ((DatabaseInternal) database).getTransaction()
              .getPageToModify(new PageId((DatabaseInternal) database, bucket.getFileId(), 0), bucket.getPageSize(), false);
          // THE SECOND SLOT POINTS FAR PAST THE END OF THE PAGE
          page.writeUnsignedInt(LocalBucket.PAGE_RECORD_TABLE_OFFSET + INT_SERIALIZED_SIZE, (long) bucket.getPageSize() * 1000);
        } catch (final Exception e) {
          throw new RuntimeException(e);
        }
      });

      final List<Long> positions = new ArrayList<>();
      database.transaction(() -> bucket.forEachRecordPosition(positions::add));
      assertThat(positions).containsExactly(rids.get(0).getPosition(), rids.get(2).getPosition());
    }
  }

  /**
   * Issue #9575 (discussion #9559): a GraphImporter load of 209M vertices slowed from 60,000 to 600 vertices a second while
   * the heap sat at 1% GC. The vertex pass only appends, and every append reported itself as "a change" to the bucket's free
   * space statistics. The map of reusable pages is empty each time an appended page fills up, so the next allocation asked for
   * a gather, and the gather - once its 5 s throttle was over - read EVERY page of the bucket looking for space that no append
   * can have freed. The scan grows with the file (6 minutes of disk reads at 1.3M pages, against 5 s of useful work between
   * two scans), so the load got slower the more it had written.
   * <p>
   * Counting scans needs no clock, so a stalled JVM cannot flip these tests; the 5 s throttle is expired through a hook
   * instead of slept through.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  // #9575
  @Nested
  class AppendOnlyLoadDoesNotRescanBucket extends TestHelper {
    private static final int    ROUNDS          = 5;
    private static final int    RECORDS_PER_RUN = 20_000;
    private static final String PAYLOAD         = "x".repeat(1_000);

    @Test
    void appendingNeverTriggersAFullBucketScan() {
      database.getSchema().createDocumentType("Wide", 1);
      final LocalBucket bucket = wideBucket();

      append(RECORDS_PER_RUN);
      final long scansAfterFirstRun = bucket.getGatherScanCountForTesting();
      final int pagesAfterFirstRun = bucket.getTotalPages();
      // the premise: records of ~1 KB pack a page to within 2% of its end, so no page is left worth listing, and every
      // page that fills up empties the map
      assertThat(pagesAfterFirstRun).isGreaterThan(100);

      for (int round = 0; round < ROUNDS; round++) {
        bucket.expireGatherThrottleForTesting();
        append(RECORDS_PER_RUN);
      }

      assertThat(bucket.getTotalPages()).isGreaterThan(pagesAfterFirstRun * 3);
      assertThat(bucket.getGatherScanCountForTesting())
          .as("gather scans over %d pages after %d append-only rounds with the throttle expired before each", bucket.getTotalPages(),
              ROUNDS).isEqualTo(scansAfterFirstRun);
    }

    /**
     * The other side of the same rule: space that a delete frees IS something a scan can find, so a delete must still arm the
     * next gather and the pages it emptied must be reused rather than the file grown.
     */
    @Test
    void deletedSpaceIsStillFoundByTheNextGather() {
      database.getSchema().createDocumentType("Wide", 1);
      final LocalBucket bucket = wideBucket();

      final List<RID> rids = append(RECORDS_PER_RUN);
      final int pagesBeforeDelete = bucket.getTotalPages();
      final long scansBeforeDelete = bucket.getGatherScanCountForTesting();

      database.transaction(() -> {
        for (int i = 0; i < rids.size(); i++)
          if (i % 2 == 0)
            database.deleteRecord(rids.get(i).asDocument());
      });

      bucket.expireGatherThrottleForTesting();
      append(RECORDS_PER_RUN / 4);

      assertThat(bucket.getGatherScanCountForTesting()).as("the delete armed a gather").isGreaterThan(scansBeforeDelete);
      assertThat(bucket.getTotalPages()).as("pages after re-inserting a quarter of what the delete freed, from %d", pagesBeforeDelete)
          .isLessThanOrEqualTo(pagesBeforeDelete + 5);
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

    private LocalBucket wideBucket() {
      return (LocalBucket) database.getSchema().getType("Wide").getBuckets(false).get(0);
    }
  }

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
  // #9575
  @Nested
  class GraphBatchLoadGatherBudget extends TestHelper {
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

  /**
   * Issue #10547: {@code TransactionContext.setAppendOnlyInserts(true)} makes the records a transaction creates skip the
   * bucket free-space search (and its bucket-wide monitor) and land on the last page or a new one, for that transaction only.
   * A transaction without it still reuses the room a delete freed on an earlier page.
   */
  // #10547
  @Nested
  class AppendOnlyInserts extends TestHelper {
    private static final int RECORDS = 5_000;

    @Test
    void anAppendOnlyTransactionSkipsTheFreedRoomThatASearchingOneReuses() {
      final LocalBucket bucket = fillThenFreeTheFirstPage();
      final int pagesBefore = bucket.getTotalPages();
      final long recordsPerPage = bucket.getMaxRecordsInPage();

      final RID[] appended = new RID[1];
      database.transaction(() -> {
        tx().setAppendOnlyInserts(true);
        appended[0] = database.newDocument("D").set("pad", "y").save().getIdentity();
      });
      assertThat(appended[0].getPosition() / recordsPerPage).as("page of the record the append-only transaction created")
          .isGreaterThanOrEqualTo(pagesBefore - 1L);

      final RID[] searched = new RID[1];
      database.transaction(() -> searched[0] = database.newDocument("D").set("pad", "y").save().getIdentity());
      assertThat(searched[0].getPosition() / recordsPerPage).as("page of the record a searching transaction created").isZero();
    }

    @Test
    void theFlagEndsWithItsTransaction() {
      database.command("sql", "CREATE DOCUMENT TYPE D");

      database.begin();
      tx().setAppendOnlyInserts(true);
      assertThat(tx().isAppendOnlyInserts()).isTrue();
      database.newDocument("D").set("pad", "y").save();
      database.commit();

      database.begin();
      assertThat(tx().isAppendOnlyInserts()).as("after a commit").isFalse();
      tx().setAppendOnlyInserts(true);
      database.rollback();

      database.begin();
      assertThat(tx().isAppendOnlyInserts()).as("after a rollback").isFalse();
      database.rollback();
    }

    private TransactionContext tx() {
      return ((DatabaseInternal) database).getTransaction();
    }

    /** Fills a one-bucket type over several pages, then deletes every record of the first page. */
    private LocalBucket fillThenFreeTheFirstPage() {
      database.command("sql", "CREATE DOCUMENT TYPE D BUCKETS 1");
      final String pad = "x".repeat(200);
      final List<RID> rids = new ArrayList<>(RECORDS);
      database.transaction(() -> {
        for (int i = 0; i < RECORDS; i++)
          rids.add(database.newDocument("D").set("i", i).set("pad", pad).save().getIdentity());
      });

      final LocalBucket bucket = (LocalBucket) database.getSchema().getType("D").getBuckets(false).getFirst();
      assertThat(bucket.getTotalPages()).as("the fixture must span several pages").isGreaterThan(2);

      final long recordsPerPage = bucket.getMaxRecordsInPage();
      database.transaction(() -> {
        for (final RID rid : rids)
          if (rid.getPosition() / recordsPerPage == 0)
            database.deleteRecord(rid.asDocument());
      });
      return bucket;
    }
  }

  /**
   * Issue #7227, item 1: the third site that turned an {@link IOException} into a
   * {@link DatabaseOperationException} without chaining it.
   * <p>
   * {@code LocalBucket.existsRecord} runs on the ordinary query and traversal path - a bucket scan skipping
   * deleted slots, a graph traversal validating an edge endpoint, {@code FetchFromRidsStep} resolving a RID -
   * so an I/O error there is precisely when the caller needs to tell a permission problem from a full volume
   * from a short file. The message alone tells it none of them. {@code isRecordStoredInSinglePage}, which reads
   * the same size marker through the same helper, has chained since it was written; #7141 chained the two sites
   * it named, and this one was missed.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  // #7227
  @Nested
  class ExistsRecordCause extends TestHelper {

    private static final String TYPE           = "Doc7227";
    private static final String INJECTED_CAUSE = "injected: read-only volume";

    private RID rid;

    @BeforeEach
    void createOneRecord() {
      database.getSchema().createDocumentType(TYPE).createProperty("id", Type.INTEGER);
      database.transaction(() -> {
        final MutableDocument doc = database.newDocument(TYPE);
        doc.set("id", 1);
        doc.save();
        rid = doc.getIdentity();
      });
      assertThat(rid).isNotNull();
    }

    @AfterEach
    void clearFaultInjection() {
      PageManager.setPageReadFaultInjector(null);
    }

    /**
     * The direct {@code Bucket} entry point: what {@code GraphEngine}, {@code BucketIterator} and
     * {@code RestoreEdgeStatement} call.
     */
    @Test
    void theCauseOfAFailedExistenceCheckSurvivesOnTheBucketApi() {
      final Bucket bucket = database.getSchema().getBucketById(rid.getBucketId());

      database.begin();
      try {
        failEveryReadOfTheRecordsBucket();

        assertThatThrownBy(() -> bucket.existsRecord(rid))
            .as("an existence check that failed on I/O must say WHY, not only that it failed")
            .isInstanceOf(DatabaseOperationException.class)
            .hasMessageContaining(rid.toString())
            .hasCauseInstanceOf(IOException.class)
            .hasRootCauseMessage(INJECTED_CAUSE);
      } finally {
        PageManager.setPageReadFaultInjector(null);
        database.rollback();
      }
    }

    /**
     * The database-level entry point: {@code LocalDatabase.existsRecord} resolves the bucket and delegates, so
     * everything reaching the check through {@code Database} - SQL's {@code FetchFromRidsStep}, the Gremlin
     * deleted-element filters, the server and HA database wrappers - lands on the same catch.
     */
    @Test
    void theCauseSurvivesThroughTheDatabaseLevelEntryPoint() {
      database.begin();
      try {
        failEveryReadOfTheRecordsBucket();

        assertThatThrownBy(() -> database.existsRecord(rid))
            .isInstanceOf(DatabaseOperationException.class)
            .hasMessageContaining(rid.toString())
            .hasCauseInstanceOf(IOException.class)
            .hasRootCauseMessage(INJECTED_CAUSE);
      } finally {
        PageManager.setPageReadFaultInjector(null);
        database.rollback();
      }
    }

    /**
     * The fixture has to be able to fail for the right reason: with no fault injected the very same call answers
     * {@code true}, so a green run above is the chained cause and not a record that was never readable.
     */
    @Test
    void theSameCheckSucceedsWhenTheDiskIsHealthy() {
      assertThat(database.getSchema().getBucketById(rid.getBucketId()).existsRecord(rid)).isTrue();
      assertThat(database.existsRecord(rid)).isTrue();
    }

    private void failEveryReadOfTheRecordsBucket() {
      final int bucketId = rid.getBucketId();
      PageManager.setPageReadFaultInjector(pageId -> {
        if (pageId.getFileId() == bucketId)
          throw new IOException(INJECTED_CAUSE);
      });
    }
  }
}
