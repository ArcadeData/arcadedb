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
package com.arcadedb.index;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.async.ErrorCallback;
import com.arcadedb.engine.WALFile;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * This test stresses the index compaction by forcing using only 1MB of RAM for compaction causing multiple page compacted index.
 *
 * @author Luca
 */
class LSMTreeIndexCompactionTest extends TestHelper {
  private static final int    TOT               = 50_000;
  private static final int    INDEX_PAGE_SIZE   = 64 * 1024; // 64K
  private static final int    COMPACTION_RAM_MB = 1; // 1MB
  private static final int    PARALLEL          = 4;
  private static final String TYPE_NAME         = "Device";

  @Test
  void testCompaction() throws Exception {
    try {
      GlobalConfiguration.INDEX_COMPACTION_RAM_MB.setValue(COMPACTION_RAM_MB);
      GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.setValue(0);

      // INSERT DATA AND CHECK WITH LOOKUP (EVERY 100)
      LogManager.instance().log(this, Level.FINE, "TEST: INSERT DATA AND CHECK WITH LOKUPS (EVERY 100)");
      insertData();
      checkLookups(100, 1);
      checkRanges(100, 1);
      checkNotUniqueEntries(1);

      LogManager.instance().log(this, Level.FINE, "Iteration 1 completed");

      // THIS TIME LOOK UP FOR KEYS WHILE COMPACTION
      LogManager.instance().log(this, Level.FINE, "TEST: THIS TIME LOOK UP FOR KEYS WHILE COMPACTION");
      final CountDownLatch semaphore1 = new CountDownLatch(1);
      new Timer().schedule(new TimerTask() {
        @Override
        public void run() {
          try {
            compaction();
          } finally {
            semaphore1.countDown();
          }
        }
      }, 0);

      checkLookups(1, 1);
      checkRanges(1, 1);
      checkNotUniqueEntries(1);

      semaphore1.await();

      // INSERT DATA ON TOP OF THE MIXED MUTABLE-COMPACTED INDEX AND CHECK WITH LOOKUPS
      LogManager.instance()
          .log(this, Level.FINE, "TEST: INSERT DATA ON TOP OF THE MIXED MUTABLE-COMPACTED INDEX AND CHECK WITH LOOKUPS");
      insertData();

      LogManager.instance().log(this, Level.FINE, "Iteration 2 completed");

      checkLookups(1, 2);
      checkRanges(1, 2);
      checkNotUniqueEntries(2);

      compaction();

      checkLookups(1, 2);
      checkRanges(1, 2);
      checkNotUniqueEntries(2);

      // INSERT DATA WHILE COMPACTING AND CHECK AGAIN
      LogManager.instance().log(this, Level.FINE, "TEST: INSERT DATA WHILE COMPACTING AND CHECK AGAIN");
      final CountDownLatch semaphore2 = new CountDownLatch(1);
      new Timer().schedule(new TimerTask() {
        @Override
        public void run() {
          compaction();
          semaphore2.countDown();
        }
      }, 0);

      insertData();
      LogManager.instance().log(this, Level.FINE, "Iteration 3 completed");

      semaphore2.await();

      checkLookups(1, 3);
      checkRanges(1, 3);
      checkNotUniqueEntries(3);

      compaction();

      checkLookups(1, 3);
      checkRanges(1, 3);
      checkNotUniqueEntries(3);

    } finally {
      GlobalConfiguration.INDEX_COMPACTION_RAM_MB.setValue(300);
      GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.setValue(10);
    }
  }

  private void checkNotUniqueEntries(final int iterations) {
     final IndexCursor records = database.lookupByKey(TYPE_NAME, new String[] { "sameValue" },
        new Object[] { "same value - test multi pages" });
    assertThat(Optional.ofNullable(records)).isNotNull();

    int count = 0;
    final Set<Identifiable> values = new HashSet<>();
    for (final Iterator<Identifiable> it = records.iterator(); it.hasNext(); ) {
      final Identifiable rid = it.next();
      final Document record = (Document) rid.getRecord();
      assertThat(record.get("sameValue")).isEqualTo("same value - test multi pages");
      ++count;
      values.add(rid);
    }
    assertThat(count).isEqualTo(TOT * iterations);
    assertThat(values.size()).isEqualTo(TOT * iterations);
  }

  private void compaction() {
    if (database.isOpen())
      for (final Index index : database.getSchema().getIndexes()) {
        if (database.isOpen() && index instanceof TypeIndex)
          try {
            ((IndexInternal) index).scheduleCompaction();
            ((IndexInternal) index).compact();
          } catch (final Exception e) {
            e.printStackTrace();
            fail("", e);
          }
      }
  }

  private void insertData() {
    database.transaction(() -> {
      Schema schema = database.getSchema();
      if (!schema.existsType(TYPE_NAME)) {
        final DocumentType v = schema.buildDocumentType().withName(TYPE_NAME).withTotalBuckets(PARALLEL).create();

        v.createProperty("id", String.class);
        v.createProperty("number", Integer.class);
        v.createProperty("relativeName", String.class);
        v.createProperty("sameValue", String.class); // TEST SPLITTING KEY ENTRIES IN MULTIPLE PAGES
        v.createProperty("Name", String.class);

        schema.buildTypeIndex(TYPE_NAME, new String[] { "id" }).withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false)
            .withPageSize(INDEX_PAGE_SIZE).create();
        schema.buildTypeIndex(TYPE_NAME, new String[] { "number" }).withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false)
            .withPageSize(INDEX_PAGE_SIZE).create();
        schema.buildTypeIndex(TYPE_NAME, new String[] { "relativeName" }).withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false)
            .withPageSize(INDEX_PAGE_SIZE).create();
        schema.buildTypeIndex(TYPE_NAME, new String[] { "sameValue" }).withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false)
            .withPageSize(INDEX_PAGE_SIZE).create();
      }
    });

    final long begin = System.currentTimeMillis();
    try {

      database.setReadYourWrites(false);
      database.async().setCommitEvery(50000);
      database.async().setParallelLevel(PARALLEL);
      database.async().setTransactionUseWAL(true);
      database.async().setTransactionSync(WALFile.FlushType.YES_NOMETADATA);

      database.async().onError(new ErrorCallback() {
        @Override
        public void call(final Throwable exception) {
          LogManager.instance().log(this, Level.SEVERE, "TEST: ERROR: ", exception);
          exception.printStackTrace();
          fail(exception);
        }
      });

      final int totalToInsert = TOT;
      final long startTimer = System.currentTimeMillis();

      database.async().transaction(new Database.TransactionScope() {
        @Override
        public void execute() {
          long lastLap = startTimer;
          long lastLapCounter = 0;

          long counter = 0;
          for (; counter < totalToInsert; ++counter) {
            final MutableDocument v = database.newDocument("Device");

            final String randomString = "" + counter;

            v.set("id", randomString); // INDEXED
            v.set("number", counter); // INDEXED
            v.set("relativeName", "/shelf=" + counter + "/slot=1"); // INDEXED
            v.set("sameValue", "same value - test multi pages"); // INDEXED

            v.set("Name", "1" + counter);

            v.save();

            if (counter % 1000 == 0) {
              if (System.currentTimeMillis() - lastLap > 1000) {
                LogManager.instance()
                    .log(this, Level.FINE, "TEST: - Progress %d/%d (%d records/sec)", null, counter, totalToInsert,
                        counter - lastLapCounter);
                lastLap = System.currentTimeMillis();
                lastLapCounter = counter;
              }
            }
          }
        }
      });

      LogManager.instance()
          .log(this, Level.FINE, "TEST: Inserted " + totalToInsert + " elements in " + (System.currentTimeMillis() - begin) + "ms");

    } finally {
      LogManager.instance().log(this, Level.FINE, "TEST: Insertion finished in " + (System.currentTimeMillis() - begin) + "ms");
    }

    database.async().waitCompletion();
  }

  private void checkLookups(final int step, final int expectedItemsPerSameKey) {
    database.transaction(() -> assertThat(database.countType(TYPE_NAME, false)).isEqualTo(TOT * expectedItemsPerSameKey));

    LogManager.instance().log(this, Level.FINE, "TEST: Lookup all the keys...");

    long begin = System.currentTimeMillis();

    int checked = 0;

    for (long id = 0; id < TOT; id += step) {
      final IndexCursor records = database.lookupByKey(TYPE_NAME, new String[] { "id" }, new Object[] { id });
      assertThat(Optional.ofNullable(records)).isNotNull();

      int count = 0;
      for (final Iterator<Identifiable> it = records.iterator(); it.hasNext(); ) {
        final Identifiable rid = it.next();
        final Document record = (Document) rid.getRecord();
        assertThat(record.get("id")).isEqualTo("" + id);
        ++count;
      }

      if (count != expectedItemsPerSameKey)
        LogManager.instance().log(this, Level.FINE, "Cannot find key '%s'", null, id);

      assertThat(count).as("Wrong result for lookup of key " + id).isEqualTo(expectedItemsPerSameKey);

      checked++;

      if (checked % 10000 == 0) {
        long delta = System.currentTimeMillis() - begin;
        if (delta < 1)
          delta = 1;
        LogManager.instance()
            .log(this, Level.FINE, "Checked " + checked + " lookups in " + delta + "ms = " + (10000 / delta) + " lookups/msec");
        begin = System.currentTimeMillis();
      }
    }
    LogManager.instance().log(this, Level.FINE, "TEST: Lookup finished in " + (System.currentTimeMillis() - begin) + "ms");
  }

  private void checkRanges(final int step, final int expectedItemsPerSameKey) {
    database.transaction(() -> assertThat(database.countType(TYPE_NAME, false)).isEqualTo(TOT * expectedItemsPerSameKey));

    LogManager.instance().log(this, Level.FINE, "TEST: Range pair of keys...");

    long begin = System.currentTimeMillis();

    int checked = 0;

    final Index index = database.getSchema().getIndexByName(TYPE_NAME + "[number]");

    for (long number = 0; number < TOT - 1; number += step) {
      final IndexCursor records = ((RangeIndex) index).range(true, new Object[] { number }, true, new Object[] { number + 1 },
          true);
      assertThat(Optional.ofNullable(records)).isNotNull();

      int count = 0;
      for (final Iterator<Identifiable> it = records.iterator(); it.hasNext(); ) {
        for (int i = 0; i < expectedItemsPerSameKey; i++) {
          final Identifiable rid = it.next();
          final Document record = (Document) rid.getRecord();
          assertThat(record.getLong("number")).isEqualTo(number + count);
        }
        ++count;
      }

      if (count != 2)
        LogManager.instance().log(this, Level.FINE, "Cannot find key '%s'", null, number);

      assertThat(count).as("Wrong result for lookup of key " + number).isEqualTo(2);

      checked++;

      if (checked % 10000 == 0) {
        long delta = System.currentTimeMillis() - begin;
        if (delta < 1)
          delta = 1;
        LogManager.instance()
            .log(this, Level.FINE, "Checked " + checked + " lookups in " + delta + "ms = " + (10000 / delta) + " lookups/msec");
        begin = System.currentTimeMillis();
      }
    }
    LogManager.instance().log(this, Level.FINE, "TEST: Lookup finished in " + (System.currentTimeMillis() - begin) + "ms");
  }

  private static String issue3714FullErrorMessage(final Throwable e) {
    final StringBuilder sb = new StringBuilder();
    Throwable current = e;
    while (current != null) {
      if (current.getMessage() != null)
        sb.append(current.getMessage()).append(" ");
      current = current.getCause();
    }
    return sb.toString();
  }

  /**
   * Regression tests for #3714: concurrent batch UPSERTs forcing early index compaction must not corrupt
   * index pages ("arraycopy: length is negative", page-move stress).
   */
  @Nested
  class Issue3714BatchUpsertArrayCopy {
    private static final String DB_PATH = "target/databases/Issue3714BatchUpsertArrayCopyTest";

    private Database issue3714Db;
    private int originalCompactionMinPages;

    @BeforeEach
    void setUp() {
      FileUtils.deleteRecursively(new File(DB_PATH));

      originalCompactionMinPages = GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.getValueAsInteger();
      GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.setValue(3);

      issue3714Db = new DatabaseFactory(DB_PATH).create();
      issue3714Db.transaction(() -> {
        final var type = issue3714Db.getSchema().buildDocumentType().withName("Metadata").withTotalBuckets(4).create();
        type.createProperty("recordId", String.class);
        type.createProperty("data", String.class);
        issue3714Db.getSchema().buildTypeIndex("Metadata", new String[] { "recordId" })
            .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true).withPageSize(16 * 1024).create();
      });
    }

    @AfterEach
    void tearDown() {
      GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.setValue(originalCompactionMinPages);
      if (issue3714Db != null && issue3714Db.isOpen())
        issue3714Db.drop();
    }

    // Issue #3714: 120K concurrent batch UPSERTs across 4 threads with small page sizes must not corrupt index pages
    @Test
    void concurrentBatchUpsertsWithCompactionDoNotCorruptPages() throws Exception {
      final int threads = 4;
      final int batchesPerThread = 1500;
      final int recordsPerBatch = 20;
      final AtomicReference<Throwable> error = new AtomicReference<>();
      final AtomicInteger successfulBatches = new AtomicInteger();
      final CountDownLatch startLatch = new CountDownLatch(1);
      final CountDownLatch doneLatch = new CountDownLatch(threads);

      for (int t = 0; t < threads; t++) {
        final int threadId = t;
        new Thread(() -> {
          try {
            startLatch.await();
            for (int batch = 0; batch < batchesPerThread; batch++) {
              final StringBuilder sql = new StringBuilder("BEGIN;");
              for (int r = 0; r < recordsPerBatch; r++) {
                final String id = "t" + threadId + "_b" + batch + "_r" + r;
                final String data = "{\"recordId\":\"" + id + "\",\"value\":\"data_" + id + "\",\"batch\":" + batch + "}";
                sql.append("UPDATE Metadata CONTENT ").append(data)
                    .append(" UPSERT WHERE recordId = '").append(id).append("';");
              }
              sql.append("COMMIT;");

              boolean success = false;
              for (int retry = 0; retry < 200 && !success; retry++) {
                try {
                  issue3714Db.command("sqlscript", sql.toString());
                  success = true;
                  successfulBatches.incrementAndGet();
                } catch (final Exception e) {
                  final String msg = issue3714FullErrorMessage(e);
                  if (msg.contains("ConcurrentModification") || msg.contains("Please retry"))
                    continue;
                  error.compareAndSet(null, e);
                  return;
                }
              }
            }
          } catch (final Throwable e) {
            error.compareAndSet(null, e);
          } finally {
            doneLatch.countDown();
          }
        }, "UpsertThread-" + t).start();
      }

      startLatch.countDown();
      assertThat(doneLatch.await(300, TimeUnit.SECONDS)).as("Threads should finish within timeout").isTrue();

      if (error.get() != null)
        error.get().printStackTrace();

      assertThat(error.get()).as("Batch UPSERTs should not throw: %s",
          error.get() != null ? error.get().getMessage() : "").isNull();

      issue3714Db.transaction(() -> {
        final long count = issue3714Db.countType("Metadata", false);
        assertThat(count).isEqualTo((long) successfulBatches.get() * recordsPerBatch);
      });
    }

    // Issue #3714: single-thread repeated UPSERTs on the same keys with growing payloads must not corrupt pages on move()
    @Test
    void highVolumeRepeatedUpsertsWithGrowingRecords() {
      final int totalKeys = 2000;
      final int iterations = 200;

      issue3714Db.transaction(() -> {
        for (int i = 0; i < totalKeys; i++)
          issue3714Db.command("sql", "INSERT INTO Metadata SET recordId = 'key_" + i + "', data = 'x'");
      });

      for (int iter = 0; iter < iterations; iter++) {
        final StringBuilder sql = new StringBuilder("BEGIN;");
        for (int i = 0; i < 20; i++) {
          final int keyIdx = (iter * 20 + i) % totalKeys;
          final String padding = "x".repeat(10 + iter % 50);
          sql.append("UPDATE Metadata CONTENT {\"recordId\":\"key_").append(keyIdx)
              .append("\",\"data\":\"").append(padding).append("_iter").append(iter)
              .append("\"} UPSERT WHERE recordId = 'key_").append(keyIdx).append("';");
        }
        sql.append("COMMIT;");

        boolean success = false;
        for (int retry = 0; retry < 50 && !success; retry++) {
          try {
            issue3714Db.command("sqlscript", sql.toString());
            success = true;
          } catch (final Exception e) {
            final String msg = issue3714FullErrorMessage(e);
            if (msg.contains("ConcurrentModification") || msg.contains("Please retry"))
              continue;
            throw e;
          }
        }
      }

      issue3714Db.transaction(() -> assertThat(issue3714Db.countType("Metadata", false)).isEqualTo(totalKeys));
    }
  }

  /**
   * Regression tests for #3714: concurrent batch UPSERTs must not produce "Variable length quantity is too long"
   * or "arraycopy: length is negative" during transaction commit.
   */
  @Nested
  class Issue3714BatchUpsertVLQ {
    private static final String DB_PATH = "target/databases/Issue3714BatchUpsertVLQTest";

    private Database issue3714Db;

    @BeforeEach
    void setUp() {
      FileUtils.deleteRecursively(new File(DB_PATH));
      issue3714Db = new DatabaseFactory(DB_PATH).create();
      issue3714Db.transaction(() -> {
        final var type = issue3714Db.getSchema().buildDocumentType().withName("Metadata").withTotalBuckets(4).create();
        type.createProperty("recordId", String.class);
        type.createProperty("data", String.class);
        issue3714Db.getSchema().buildTypeIndex("Metadata", new String[] { "recordId" })
            .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true).withPageSize(64 * 1024).create();
      });
    }

    @AfterEach
    void tearDown() {
      if (issue3714Db != null && issue3714Db.isOpen())
        issue3714Db.drop();
    }

    // Issue #3714: concurrent batch UPSERTs with per-thread key namespaces must not corrupt the index VLQ-encoded entries
    @Test
    void concurrentBatchUpsertsDoNotCorruptIndex() throws Exception {
      final int threads = 2;
      final int batchesPerThread = 500;
      final int recordsPerBatch = 20;
      final AtomicReference<Throwable> error = new AtomicReference<>();
      final AtomicInteger successfulBatches = new AtomicInteger();
      final CountDownLatch startLatch = new CountDownLatch(1);
      final CountDownLatch doneLatch = new CountDownLatch(threads);

      for (int t = 0; t < threads; t++) {
        final int threadId = t;
        new Thread(() -> {
          try {
            startLatch.await();
            for (int batch = 0; batch < batchesPerThread; batch++) {
              final StringBuilder sql = new StringBuilder("BEGIN;");
              for (int r = 0; r < recordsPerBatch; r++) {
                final String id = "t" + threadId + "_b" + batch + "_r" + r;
                final String data = "{\"recordId\":\"" + id + "\",\"value\":\"data_" + id + "\",\"batch\":" + batch + "}";
                sql.append("UPDATE Metadata CONTENT ").append(data)
                    .append(" UPSERT WHERE recordId = '").append(id).append("';");
              }
              sql.append("COMMIT;");

              boolean success = false;
              for (int retry = 0; retry < 200 && !success; retry++) {
                try {
                  issue3714Db.command("sqlscript", sql.toString());
                  success = true;
                  successfulBatches.incrementAndGet();
                } catch (final Exception e) {
                  final String msg = issue3714FullErrorMessage(e);
                  if (msg.contains("ConcurrentModification") || msg.contains("Please retry"))
                    continue;
                  error.compareAndSet(null, e);
                  return;
                }
              }
            }
          } catch (final Throwable e) {
            error.compareAndSet(null, e);
          } finally {
            doneLatch.countDown();
          }
        }, "UpsertThread-" + t).start();
      }

      startLatch.countDown();
      assertThat(doneLatch.await(180, TimeUnit.SECONDS)).as("Threads should finish within timeout").isTrue();

      if (error.get() != null)
        error.get().printStackTrace();

      assertThat(error.get()).as("Batch UPSERTs should not throw: %s",
          error.get() != null ? error.get().getMessage() : "").isNull();

      issue3714Db.transaction(() -> {
        final long count = issue3714Db.countType("Metadata", false);
        assertThat(count).isEqualTo((long) successfulBatches.get() * recordsPerBatch);
      });
    }

    // Issue #3714: single-thread repeated batch UPSERTs on the same keys must not corrupt the index
    @Test
    void repeatedBatchUpsertsOnSameKeys() {
      final int totalKeys = 1000;
      final int iterations = 100;

      issue3714Db.transaction(() -> {
        for (int i = 0; i < totalKeys; i++)
          issue3714Db.command("sql", "INSERT INTO Metadata SET recordId = 'key_" + i + "', data = 'initial'");
      });

      for (int iter = 0; iter < iterations; iter++) {
        final StringBuilder sql = new StringBuilder("BEGIN;");
        for (int i = 0; i < 20; i++) {
          final int keyIdx = (iter * 20 + i) % totalKeys;
          sql.append("UPDATE Metadata CONTENT {\"recordId\":\"key_").append(keyIdx)
              .append("\",\"data\":\"updated_iter").append(iter).append("_").append(i)
              .append("\"} UPSERT WHERE recordId = 'key_").append(keyIdx).append("';");
        }
        sql.append("COMMIT;");
        issue3714Db.command("sqlscript", sql.toString());
      }

      issue3714Db.transaction(() -> assertThat(issue3714Db.countType("Metadata", false)).isEqualTo(totalKeys));
    }
  }
}
