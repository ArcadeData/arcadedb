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
package performance;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.NullLogger;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.engine.WALFile;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.utility.FileUtils;

import java.io.File;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

/**
 * Benchmark comparing LSM Tree index vs Hash index for CRUD operations.
 * <p>
 * Creates two separate databases with 10M records each using async multi-threaded insertion
 * (4 buckets, 4 parallel threads). Then runs multiple passes of point lookups, updates
 * (key changes), inserts and deletes, so the JVM can optimize hotspots across iterations.
 * <p>
 * Run from command line:
 * <pre>
 *   mvn test-compile -pl engine -q
 *   java -Xmx4g -cp engine/target/test-classes:engine/target/classes:$(mvn -pl engine dependency:build-classpath -q -DincludeScope=test -Dmdep.outputFile=/dev/stderr 2>&1 1>/dev/null) performance.IndexBenchmark
 * </pre>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class IndexBenchmark {
  private static final String BASE_PATH      = "target/databases/indexbenchmark";
  private static final String LSM_DB_PATH    = BASE_PATH + "/lsm";
  private static final String HASH_DB_PATH   = BASE_PATH + "/hash";
  private static final String TYPE_NAME      = "Record";
  private static final int    TOTAL_RECORDS  = 10_000_000;
  private static final int    PARALLEL       = 4;
  private static final int    BATCH_SIZE     = 10_000;
  private static final int    PASSES         = 5;
  private static final int    OPS_PER_PASS   = 10_000;
  private static final int    UPDATE_OPS     = 10_000;
  private static final int    DELETE_OPS     = 10_000;
  private static final int    INSERT_OPS     = 10_000;

  private Database lsmDb;
  private Database hashDb;

  public static void main(final String[] args) {
    GlobalConfiguration.PROFILE.setValue("high-performance");
    LogManager.instance().setLogger(NullLogger.INSTANCE);

    final IndexBenchmark bench = new IndexBenchmark();
    try {
      bench.setup();
      bench.runBenchmarks();
    } finally {
      bench.tearDown();
    }
  }

  private void setup() {
    // Clean previous runs
    FileUtils.deleteRecursively(new File(BASE_PATH));
    new File(LSM_DB_PATH).mkdirs();
    new File(HASH_DB_PATH).mkdirs();

    System.out.println("=== INDEX BENCHMARK: LSM Tree vs Hash Index ===");
    System.out.println("Records: " + TOTAL_RECORDS + ", Parallel: " + PARALLEL + ", Passes: " + PASSES);
    System.out.println();

    // Create LSM database
    System.out.print("Creating LSM Tree database with " + TOTAL_RECORDS + " records... ");
    final long lsmSetup = createDatabase(LSM_DB_PATH, Schema.INDEX_TYPE.LSM_TREE);
    System.out.println("done in " + lsmSetup + "ms (" + (TOTAL_RECORDS / (lsmSetup / 1000.0)) + " rec/s)");

    // Create Hash database
    System.out.print("Creating Hash index database with " + TOTAL_RECORDS + " records... ");
    final long hashSetup = createDatabase(HASH_DB_PATH, Schema.INDEX_TYPE.HASH);
    System.out.println("done in " + hashSetup + "ms (" + (TOTAL_RECORDS / (hashSetup / 1000.0)) + " rec/s)");

    System.out.println();

    // Open both for benchmarks
    lsmDb = new DatabaseFactory(LSM_DB_PATH).open();
    hashDb = new DatabaseFactory(HASH_DB_PATH).open();
  }

  private long createDatabase(final String path, final Schema.INDEX_TYPE indexType) {
    final long begin = System.currentTimeMillis();

    try (final Database db = new DatabaseFactory(path).create()) {
      db.begin();
      final DocumentType type = db.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(PARALLEL).create();
      type.createProperty("id", Integer.class);
      type.createProperty("name", String.class);
      type.createProperty("value", Long.class);
      db.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "id" })
          .withType(indexType).withUnique(true).create();
      db.commit();

      // Async multi-threaded inserts
      db.setReadYourWrites(false);
      db.async().setParallelLevel(PARALLEL);
      db.async().setTransactionUseWAL(false);
      db.async().setTransactionSync(WALFile.FlushType.NO);
      db.async().setCommitEvery(10_000);
      db.async().onError(exception -> {
        System.err.println("ASYNC ERROR: " + exception.getMessage());
        exception.printStackTrace();
        System.exit(1);
      });

      final AtomicLong counter = new AtomicLong();
      for (int i = 0; i < TOTAL_RECORDS; ++i) {
        final MutableDocument doc = db.newDocument(TYPE_NAME);
        doc.set("id", i);
        doc.set("name", "user_" + i);
        doc.set("value", (long) i * 17);

        db.async().createRecord(doc, null);

        final long c = counter.incrementAndGet();
        if (c % 1_000_000 == 0) {
          final long elapsed = System.currentTimeMillis() - begin;
          final double rate = c / (elapsed / 1000.0);
          System.out.printf("%.0fM(%.0f/s).. ", c / 1_000_000.0, rate);
        }
      }

      db.async().waitCompletion();
    }
    return System.currentTimeMillis() - begin;
  }

  private void tearDown() {
    if (lsmDb != null)
      lsmDb.close();
    if (hashDb != null)
      hashDb.close();
    FileUtils.deleteRecursively(new File(BASE_PATH));
  }

  private void runBenchmarks() {
    final long[][] lookupResults = new long[2][PASSES];
    final long[][] updateResults = new long[2][PASSES];
    final long[][] deleteInsertResults = new long[2][PASSES];
    final long[][] sqlLookupResults = new long[2][PASSES];

    for (int pass = 0; pass < PASSES; ++pass) {
      System.out.println("─── Pass " + (pass + 1) + "/" + PASSES + " ───");

      // Point lookups (random)
      lookupResults[0][pass] = benchPointLookup(lsmDb, "LSM_TREE", pass);
      lookupResults[1][pass] = benchPointLookup(hashDb, "HASH", pass);

      // SQL point lookups via query engine
      sqlLookupResults[0][pass] = benchSqlLookup(lsmDb, "LSM_TREE", pass);
      sqlLookupResults[1][pass] = benchSqlLookup(hashDb, "HASH", pass);

      // Update with key change (delete old key + insert new key)
      updateResults[0][pass] = benchUpdateKey(lsmDb, "LSM_TREE", pass);
      updateResults[1][pass] = benchUpdateKey(hashDb, "HASH", pass);

      // Delete + re-insert cycle
      deleteInsertResults[0][pass] = benchDeleteInsert(lsmDb, "LSM_TREE", pass);
      deleteInsertResults[1][pass] = benchDeleteInsert(hashDb, "HASH", pass);

      System.out.println();
    }

    // Print summary
    printSummary("Point Lookup (" + OPS_PER_PASS + " ops)", lookupResults);
    printSummary("SQL Lookup (" + OPS_PER_PASS + " ops)", sqlLookupResults);
    printSummary("Update Key (" + UPDATE_OPS + " ops)", updateResults);
    printSummary("Delete+Insert (" + DELETE_OPS + " ops)", deleteInsertResults);
  }

  // ── POINT LOOKUP ─────────────────────────────────────────

  private long benchPointLookup(final Database db, final String label, final int pass) {
    final Random rnd = new Random(42 + pass);
    final long begin = System.currentTimeMillis();

    db.transaction(() -> {
      for (int i = 0; i < OPS_PER_PASS; ++i) {
        final int key = rnd.nextInt(TOTAL_RECORDS);
        final IndexCursor cursor = db.getSchema().getIndexByName(TYPE_NAME + "[id]").get(new Object[] { key });
        if (!cursor.hasNext())
          throw new RuntimeException(label + ": key " + key + " not found");
        cursor.next();
      }
    });

    final long elapsed = System.currentTimeMillis() - begin;
    final double opsPerSec = OPS_PER_PASS / (elapsed / 1000.0);
    System.out.printf("  %-10s Point Lookup: %,7d ms  (%,.0f ops/sec)%n", label, elapsed, opsPerSec);
    return elapsed;
  }

  // ── SQL LOOKUP ───────────────────────────────────────────

  private long benchSqlLookup(final Database db, final String label, final int pass) {
    final Random rnd = new Random(42 + pass);
    final long begin = System.currentTimeMillis();

    db.transaction(() -> {
      for (int i = 0; i < OPS_PER_PASS; ++i) {
        final int key = rnd.nextInt(TOTAL_RECORDS);
        try (final var rs = db.query("sql", "SELECT FROM " + TYPE_NAME + " WHERE id = ?", key)) {
          if (!rs.hasNext())
            throw new RuntimeException(label + ": SQL key " + key + " not found");
          rs.next();
        }
      }
    });

    final long elapsed = System.currentTimeMillis() - begin;
    final double opsPerSec = OPS_PER_PASS / (elapsed / 1000.0);
    System.out.printf("  %-10s SQL Lookup:   %,7d ms  (%,.0f ops/sec)%n", label, elapsed, opsPerSec);
    return elapsed;
  }

  // ── UPDATE KEY ───────────────────────────────────────────

  private long benchUpdateKey(final Database db, final String label, final int pass) {
    // We update keys in the range [TOTAL_RECORDS .. TOTAL_RECORDS + UPDATE_OPS) from previous passes
    // by changing the 'id' field (which is the indexed key).
    // To keep the DB consistent across passes, each pass works on a dedicated key range.
    final int rangeStart = TOTAL_RECORDS + (pass * UPDATE_OPS);
    final long begin = System.currentTimeMillis();

    // First, insert the records we'll update (with old keys)
    for (int batch = 0; batch < UPDATE_OPS; batch += BATCH_SIZE) {
      final int bStart = batch;
      db.transaction(() -> {
        final int end = Math.min(bStart + BATCH_SIZE, UPDATE_OPS);
        for (int i = bStart; i < end; ++i) {
          final MutableDocument doc = db.newDocument(TYPE_NAME);
          doc.set("id", rangeStart + i);
          doc.set("name", "upd_" + i);
          doc.set("value", (long) i);
          doc.save();
        }
      });
    }

    // Now update: load by old key, change key, save (triggers index remove old + add new)
    final int newRangeStart = rangeStart + TOTAL_RECORDS;
    final long updateBegin = System.currentTimeMillis();

    for (int batch = 0; batch < UPDATE_OPS; batch += BATCH_SIZE) {
      final int bStart = batch;
      db.transaction(() -> {
        final int end = Math.min(bStart + BATCH_SIZE, UPDATE_OPS);
        for (int i = bStart; i < end; ++i) {
          final IndexCursor cursor = db.getSchema().getIndexByName(TYPE_NAME + "[id]").get(new Object[] { rangeStart + i });
          if (!cursor.hasNext())
            throw new RuntimeException(label + ": update key " + (rangeStart + i) + " not found");
          final Document doc = cursor.next().asDocument();
          final MutableDocument mutable = doc.modify();
          mutable.set("id", newRangeStart + i);
          mutable.save();
        }
      });
    }

    final long elapsed = System.currentTimeMillis() - updateBegin;
    final double opsPerSec = UPDATE_OPS / (elapsed / 1000.0);
    System.out.printf("  %-10s Update Key:   %,7d ms  (%,.0f ops/sec)%n", label, elapsed, opsPerSec);

    // Cleanup: delete the updated records so they don't interfere with next pass
    for (int batch = 0; batch < UPDATE_OPS; batch += BATCH_SIZE) {
      final int bStart = batch;
      db.transaction(() -> {
        final int end = Math.min(bStart + BATCH_SIZE, UPDATE_OPS);
        for (int i = bStart; i < end; ++i) {
          final IndexCursor cursor = db.getSchema().getIndexByName(TYPE_NAME + "[id]").get(new Object[] { newRangeStart + i });
          if (cursor.hasNext())
            cursor.next().asDocument().delete();
        }
      });
    }

    return elapsed;
  }

  // ── DELETE + INSERT ──────────────────────────────────────

  private long benchDeleteInsert(final Database db, final String label, final int pass) {
    // Use a dedicated range that won't collide with the base data or update ranges
    final int rangeStart = TOTAL_RECORDS * 3 + (pass * DELETE_OPS);
    final long begin = System.currentTimeMillis();

    // Insert records to delete
    for (int batch = 0; batch < DELETE_OPS; batch += BATCH_SIZE) {
      final int bStart = batch;
      db.transaction(() -> {
        final int end = Math.min(bStart + BATCH_SIZE, DELETE_OPS);
        for (int i = bStart; i < end; ++i) {
          final MutableDocument doc = db.newDocument(TYPE_NAME);
          doc.set("id", rangeStart + i);
          doc.set("name", "del_" + i);
          doc.set("value", (long) i);
          doc.save();
        }
      });
    }

    // Delete them
    final long deleteBegin = System.currentTimeMillis();

    for (int batch = 0; batch < DELETE_OPS; batch += BATCH_SIZE) {
      final int bStart = batch;
      db.transaction(() -> {
        final int end = Math.min(bStart + BATCH_SIZE, DELETE_OPS);
        for (int i = bStart; i < end; ++i) {
          final IndexCursor cursor = db.getSchema().getIndexByName(TYPE_NAME + "[id]").get(new Object[] { rangeStart + i });
          if (!cursor.hasNext())
            throw new RuntimeException(label + ": delete key " + (rangeStart + i) + " not found");
          cursor.next().asDocument().delete();
        }
      });
    }

    // Re-insert them
    for (int batch = 0; batch < INSERT_OPS; batch += BATCH_SIZE) {
      final int bStart = batch;
      db.transaction(() -> {
        final int end = Math.min(bStart + BATCH_SIZE, INSERT_OPS);
        for (int i = bStart; i < end; ++i) {
          final MutableDocument doc = db.newDocument(TYPE_NAME);
          doc.set("id", rangeStart + i);
          doc.set("name", "reins_" + i);
          doc.set("value", (long) i);
          doc.save();
        }
      });
    }

    final long elapsed = System.currentTimeMillis() - deleteBegin;
    final double opsPerSec = (DELETE_OPS + INSERT_OPS) / (elapsed / 1000.0);
    System.out.printf("  %-10s Del+Insert:  %,7d ms  (%,.0f ops/sec)%n", label, elapsed, opsPerSec);

    // Cleanup
    for (int batch = 0; batch < INSERT_OPS; batch += BATCH_SIZE) {
      final int bStart = batch;
      db.transaction(() -> {
        final int end = Math.min(bStart + BATCH_SIZE, INSERT_OPS);
        for (int i = bStart; i < end; ++i) {
          final IndexCursor cursor = db.getSchema().getIndexByName(TYPE_NAME + "[id]").get(new Object[] { rangeStart + i });
          if (cursor.hasNext())
            cursor.next().asDocument().delete();
        }
      });
    }

    return elapsed;
  }

  // ── SUMMARY ──────────────────────────────────────────────

  private void printSummary(final String label, final long[][] results) {
    // results[0] = LSM, results[1] = HASH
    final long lsmBest = min(results[0]);
    final long hashBest = min(results[1]);
    final long lsmAvgLast3 = avgLast(results[0], 3);
    final long hashAvgLast3 = avgLast(results[1], 3);

    System.out.println("┌─────────────────────────────────────────────────────────┐");
    System.out.printf("│ %-56s│%n", label);
    System.out.println("├──────────┬──────────┬──────────┬────────────────────────┤");
    System.out.println("│          │ Best(ms) │ Avg3(ms) │ Per-pass (ms)          │");
    System.out.println("├──────────┼──────────┼──────────┼────────────────────────┤");
    System.out.printf("│ LSM_TREE │ %,8d │ %,8d │ ", lsmBest, lsmAvgLast3);
    for (int i = 0; i < PASSES; ++i)
      System.out.printf("%,6d ", results[0][i]);
    System.out.println("│");
    System.out.printf("│ HASH     │ %,8d │ %,8d │ ", hashBest, hashAvgLast3);
    for (int i = 0; i < PASSES; ++i)
      System.out.printf("%,6d ", results[1][i]);
    System.out.println("│");
    System.out.println("├──────────┴──────────┴──────────┴────────────────────────┤");

    if (hashAvgLast3 > 0 && lsmAvgLast3 > 0) {
      final double ratio = (double) lsmAvgLast3 / hashAvgLast3;
      final String winner = ratio > 1.0 ? "HASH" : "LSM_TREE";
      System.out.printf("│ Winner: %-8s (%.2fx faster, avg last 3 passes)     │%n", winner, ratio > 1.0 ? ratio : 1.0 / ratio);
    }
    System.out.println("└─────────────────────────────────────────────────────────┘");
    System.out.println();
  }

  private static long min(final long[] arr) {
    long m = Long.MAX_VALUE;
    for (final long v : arr)
      if (v < m)
        m = v;
    return m;
  }

  private static long avgLast(final long[] arr, final int n) {
    long sum = 0;
    final int start = Math.max(0, arr.length - n);
    for (int i = start; i < arr.length; ++i)
      sum += arr[i];
    return sum / Math.min(n, arr.length);
  }
}
