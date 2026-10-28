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
package com.arcadedb.index.columnar;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.log.LogManager;
import com.arcadedb.utility.StallAwareStopwatch;
import com.sun.management.ThreadMXBean;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.logging.Level;

/**
 * Spec 2 goal "ingest cost unchanged" (spec 10 benchmarks), report only: 200,000 inserts then 100,000 updates of {@code Asset}, without
 * (A) and with (B) the five COLUMNAR indexes of {@link ColumnarTestData#createSchema}, on the same seeded data, each run on a fresh
 * database.
 * <p>
 * Method: one warm-up run of each variant, then {@value #MEASURED_PAIRS} measured A/B pairs whose order alternates (AB, BA, AB, ...),
 * so a drift of the machine over the run weighs on both variants alike. Per variant and phase the log line gives the median, min and
 * max over the pairs of the stall-discounted phase time ({@link StallAwareStopwatch#effectiveMs()}), and the median over the runs of
 * each run's median bytes allocated by the committing thread inside {@code commit()}. The projection's apply passes run on their own
 * pool after each commit, off the ingest path: variant B also logs an "apply catch-up" phase, the time from the end of the updates
 * until {@link ColumnarIndexManager#awaitCaughtUp} returns. Nothing is asserted.
 */
@Tag("benchmark")
class ColumnarIngestOverheadBenchmark extends TestHelper {
  private static final int    INSERTS          = 200_000;
  private static final int    UPDATES          = 100_000;
  private static final int    TRANSACTION_SIZE = 500;
  private static final int    MEASURED_PAIRS   = 5;
  private static final long   SEED             = 42;
  private static final long   DAY_MS           = 86_400_000L;
  private static final long   EPOCH_2020_MS    = 1_577_836_800_000L;
  private static final long   CATCH_UP_MS      = 600_000;
  private static final String TYPE             = ColumnarTestData.TYPE;
  // phases of one run
  private static final int    INSERT           = 0;
  private static final int    UPDATE           = 1;
  private static final int    CATCH_UP         = 2;

  private final ThreadMXBean threads = (ThreadMXBean) ManagementFactory.getThreadMXBean();

  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    return false;
  }

  @Test
  void ingestWithAndWithoutColumnarIndexes() throws Exception {
    run(false);
    recreateDatabase();
    run(true);

    // [variant][phase][pair]: effective ms; [variant][phase][pair]: median bytes per commit
    final long[][][] ms = new long[2][3][MEASURED_PAIRS];
    final long[][][] bytes = new long[2][2][MEASURED_PAIRS];
    for (int pair = 0; pair < MEASURED_PAIRS; pair++)
      for (int k = 0; k < 2; k++) {
        final boolean columnar = (pair + k) % 2 == 1;
        recreateDatabase();
        final long[] result = run(columnar);
        final int variant = columnar ? 1 : 0;
        ms[variant][INSERT][pair] = result[0];
        ms[variant][UPDATE][pair] = result[1];
        ms[variant][CATCH_UP][pair] = result[2];
        bytes[variant][INSERT][pair] = result[3];
        bytes[variant][UPDATE][pair] = result[4];
      }

    for (int variant = 0; variant < 2; variant++) {
      final String name = variant == 0 ? "A without COLUMNAR" : "B with 5 COLUMNAR";
      report(name, INSERTS + " inserts", ms[variant][INSERT], bytes[variant][INSERT]);
      report(name, UPDATES + " updates", ms[variant][UPDATE], bytes[variant][UPDATE]);
    }
    report("B with 5 COLUMNAR", "apply catch-up", ms[1][CATCH_UP], null);
  }

  /**
   * One run on the current (fresh) database.
   *
   * @return insert ms, update ms, catch-up ms (0 without COLUMNAR), median insert bytes per commit, median update bytes per commit
   */
  private long[] run(final boolean columnar) throws InterruptedException {
    createSchema(columnar);
    final Random random = new Random(SEED);
    final List<RID> rids = new ArrayList<>(INSERTS);

    final long[] insertBytes = new long[(INSERTS + TRANSACTION_SIZE - 1) / TRANSACTION_SIZE];
    final StallAwareStopwatch insertWatch = StallAwareStopwatch.start();
    for (int commit = 0; commit < insertBytes.length; commit++) {
      database.begin();
      for (int i = commit * TRANSACTION_SIZE; i < Math.min((commit + 1) * TRANSACTION_SIZE, INSERTS); i++) {
        final MutableDocument record = database.newDocument(TYPE);
        fill(record, random, i);
        record.save();
        rids.add(record.getIdentity());
      }
      insertBytes[commit] = commit();
    }
    final long insertMs = insertWatch.effectiveMs();

    final long[] updateBytes = new long[(UPDATES + TRANSACTION_SIZE - 1) / TRANSACTION_SIZE];
    final StallAwareStopwatch updateWatch = StallAwareStopwatch.start();
    for (int commit = 0; commit < updateBytes.length; commit++) {
      database.begin();
      for (int i = commit * TRANSACTION_SIZE; i < Math.min((commit + 1) * TRANSACTION_SIZE, UPDATES); i++) {
        final MutableDocument record = rids.get(random.nextInt(rids.size())).asDocument(true).modify();
        switch (random.nextInt(3)) {
        case 0 -> record.set("brand", "brand-" + random.nextInt(200));
        case 1 -> record.set("fileSize", 1024L * random.nextInt(4096));
        default -> record.set("tags", tags(random));
        }
        record.save();
      }
      updateBytes[commit] = commit();
    }
    final long updateMs = updateWatch.effectiveMs();

    long catchUpMs = 0;
    if (columnar) {
      final StallAwareStopwatch catchUpWatch = StallAwareStopwatch.start();
      if (!manager().awaitCaughtUp(CATCH_UP_MS))
        throw new IllegalStateException("The columnar projections did not catch up within " + CATCH_UP_MS + " ms");
      catchUpMs = catchUpWatch.effectiveMs();
    }
    return new long[] { insertMs, updateMs, catchUpMs, median(insertBytes), median(updateBytes) };
  }

  /** Commits the open transaction and returns the bytes the calling thread allocated inside the commit. */
  private long commit() {
    final long threadId = Thread.currentThread().threadId();
    final long before = threads.getThreadAllocatedBytes(threadId);
    database.commit();
    return threads.getThreadAllocatedBytes(threadId) - before;
  }

  private void report(final String variant, final String phase, final long[] ms, final long[] bytesPerCommit) {
    final long[] sorted = ms.clone();
    Arrays.sort(sorted);
    final String allocation = bytesPerCommit == null ? "" :
        String.format(Locale.ROOT, ", %,d bytes allocated per commit (median)", median(bytesPerCommit));
    LogManager.instance().log(this, Level.INFO,
        String.format(Locale.ROOT, "INGEST %-19s %-15s median %,6d ms effective (min %,d, max %,d, %d runs)%s", variant, phase,
            median(ms), sorted[0], sorted[sorted.length - 1], ms.length, allocation));
  }

  /** The median; of an even count, the mean of the middle two. */
  private static long median(final long[] values) {
    final long[] sorted = values.clone();
    Arrays.sort(sorted);
    final int middle = sorted.length / 2;
    return sorted.length % 2 == 1 ? sorted[middle] : (sorted[middle - 1] + sorted[middle]) / 2;
  }

  private void createSchema(final boolean columnar) {
    if (columnar) {
      new ColumnarTestData(SEED).createSchema(database);
      return;
    }
    database.command("sql", "CREATE DOCUMENT TYPE " + TYPE);
    database.command("sql", "CREATE PROPERTY " + TYPE + ".brand STRING");
    database.command("sql", "CREATE PROPERTY " + TYPE + ".tags LIST OF STRING");
    database.command("sql", "CREATE PROPERTY " + TYPE + ".fileSize LONG");
    database.command("sql", "CREATE PROPERTY " + TYPE + ".publishDate DATETIME");
    database.command("sql", "CREATE PROPERTY " + TYPE + ".author STRING");
    database.command("sql", "CREATE PROPERTY " + TYPE + ".body STRING");
  }

  private static void fill(final MutableDocument record, final Random random, final long sequence) {
    record.set("brand", "brand-" + random.nextInt(200));
    record.set("tags", tags(random));
    record.set("fileSize", 1024L * random.nextInt(4096));
    record.set("publishDate", new Date(EPOCH_2020_MS + random.nextInt(5 * 365) * DAY_MS));
    final String author = "author-" + random.nextInt(50);
    if (sequence % 10 != 0)
      record.set("author", author);
    record.set("body", "x".repeat(random.nextInt(200)));
  }

  private static List<String> tags(final Random random) {
    final int count = random.nextInt(9);
    final List<String> tags = new ArrayList<>(count);
    for (int i = 0; i < count; i++)
      tags.add("tag-" + random.nextInt(2_000));
    return tags;
  }

  private ColumnarIndexManager manager() {
    return ((LocalDatabase) ((DatabaseInternal) database).getEmbedded()).getColumnarIndexManager();
  }

  private void recreateDatabase() {
    database.drop();
    database = factory.create();
  }
}
