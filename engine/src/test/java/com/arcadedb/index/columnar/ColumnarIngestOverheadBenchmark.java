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

import java.lang.management.GarbageCollectorMXBean;
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
 * (A) and with (B) the five COLUMNAR indexes of {@link ColumnarTestData#createSchema}, and B' = B with every apply pass of the database
 * held ({@link ColumnarIndexManager#suspendApply()}) across both phases, on the same seeded data, each run on a fresh database. B' tells
 * the commit path (the page marking inside {@code commit()}, which B' still pays) from the background apply passes and flushes (which
 * B' defers to after the phases): B' close to A and B above both puts the overhead on the background.
 * <p>
 * Method: one warm-up run of each variant, then {@value #MEASURED_ROUNDS} measured A/B/B' triples whose order rotates (A B B', B B' A,
 * B' A B, ...), so a drift of the machine over the run weighs on every variant alike. Per variant and phase the log line gives the median,
 * min and max over the triples of the stall-discounted phase time ({@link StallAwareStopwatch#effectiveMs()}), the median of the CPU time
 * of the committing thread over the phase, the median of the GC count and GC time of the JVM over the phase, and the median over the runs
 * of each run's median bytes allocated by the committing thread inside {@code commit()}. B and B' also log an "apply catch-up" phase, the
 * time from the end of the updates (for B', from the release of the hold) until {@link ColumnarIndexManager#awaitCaughtUp} returns.
 * Nothing is asserted.
 */
@Tag("benchmark")
class ColumnarIngestOverheadBenchmark extends TestHelper {
  private static final int      INSERTS          = 200_000;
  private static final int      UPDATES          = 100_000;
  private static final int      TRANSACTION_SIZE = 500;
  private static final int      MEASURED_ROUNDS  = 5;
  private static final long     SEED             = 42;
  private static final long     DAY_MS           = 86_400_000L;
  private static final long     EPOCH_2020_MS    = 1_577_836_800_000L;
  private static final long     CATCH_UP_MS      = 600_000;
  private static final String   TYPE             = ColumnarTestData.TYPE;
  private static final String[] VARIANTS         = { "A without COLUMNAR", "B with 5 COLUMNAR", "B' 5 COLUMNAR held" };
  // variants
  private static final int      A                = 0;
  private static final int      B                = 1;
  private static final int      B_HELD           = 2;
  // phases of one run
  private static final int      INSERT           = 0;
  private static final int      UPDATE           = 1;
  private static final int      CATCH_UP         = 2;
  // figures of one phase
  private static final int      EFFECTIVE_MS     = 0;
  private static final int      CPU_MS           = 1;
  private static final int      GC_COUNT         = 2;
  private static final int      GC_MS            = 3;
  private static final int      BYTES            = 4;

  private final ThreadMXBean threads = (ThreadMXBean) ManagementFactory.getThreadMXBean();

  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    return false;
  }

  @Test
  void ingestWithAndWithoutColumnarIndexes() throws Exception {
    for (int variant = 0; variant < VARIANTS.length; variant++) {
      run(variant);
      recreateDatabase();
    }

    // [variant][phase][figure][round]
    final long[][][][] figures = new long[VARIANTS.length][3][5][MEASURED_ROUNDS];
    for (int round = 0; round < MEASURED_ROUNDS; round++)
      for (int k = 0; k < VARIANTS.length; k++) {
        final int variant = (round + k) % VARIANTS.length;
        final long[][] result = run(variant);
        recreateDatabase();
        for (int phase = 0; phase < 3; phase++)
          for (int figure = 0; figure < 5; figure++)
            figures[variant][phase][figure][round] = result[phase][figure];
      }

    for (int variant = 0; variant < VARIANTS.length; variant++) {
      report(VARIANTS[variant], INSERTS + " inserts", figures[variant][INSERT]);
      report(VARIANTS[variant], UPDATES + " updates", figures[variant][UPDATE]);
    }
    report(VARIANTS[B], "apply catch-up", figures[B][CATCH_UP]);
    report(VARIANTS[B_HELD], "apply catch-up", figures[B_HELD][CATCH_UP]);
  }

  /**
   * One run on the current (fresh) database.
   *
   * @return per phase (insert, update, catch-up): effective ms, committer CPU ms, GC count, GC ms, median bytes per commit (0 for the
   * catch-up); the catch-up is all zero without COLUMNAR
   */
  private long[][] run(final int variant) throws Exception {
    createSchema(variant != A);
    final Random random = new Random(SEED);
    final List<RID> rids = new ArrayList<>(INSERTS);
    final long[][] result = new long[3][5];

    final AutoCloseable hold = variant == B_HELD ? manager().suspendApply() : null;
    final long[] insertBytes = new long[(INSERTS + TRANSACTION_SIZE - 1) / TRANSACTION_SIZE];
    Phase phase = new Phase();
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
    phase.end(result[INSERT], median(insertBytes));

    final long[] updateBytes = new long[(UPDATES + TRANSACTION_SIZE - 1) / TRANSACTION_SIZE];
    phase = new Phase();
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
    phase.end(result[UPDATE], median(updateBytes));

    if (variant != A) {
      phase = new Phase();
      if (hold != null)
        hold.close();
      if (!manager().awaitCaughtUp(CATCH_UP_MS))
        throw new IllegalStateException("The columnar projections did not catch up within " + CATCH_UP_MS + " ms");
      phase.end(result[CATCH_UP], 0);
    }
    return result;
  }

  /** The figures of one phase, from its construction to {@link #end}. */
  private final class Phase {
    private final StallAwareStopwatch watch    = StallAwareStopwatch.start();
    private final long                cpuNanos = threads.getCurrentThreadCpuTime();
    private final long                gcCount  = gcCount();
    private final long                gcMs     = gcMs();

    void end(final long[] into, final long bytesPerCommit) {
      into[EFFECTIVE_MS] = watch.effectiveMs();
      into[CPU_MS] = (threads.getCurrentThreadCpuTime() - cpuNanos) / 1_000_000L;
      into[GC_COUNT] = gcCount() - gcCount;
      into[GC_MS] = gcMs() - gcMs;
      into[BYTES] = bytesPerCommit;
    }
  }

  private static long gcCount() {
    long total = 0;
    for (final GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans())
      total += Math.max(0, gc.getCollectionCount());
    return total;
  }

  private static long gcMs() {
    long total = 0;
    for (final GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans())
      total += Math.max(0, gc.getCollectionTime());
    return total;
  }

  /** Commits the open transaction and returns the bytes the calling thread allocated inside the commit. */
  private long commit() {
    final long threadId = Thread.currentThread().threadId();
    final long before = threads.getThreadAllocatedBytes(threadId);
    database.commit();
    return threads.getThreadAllocatedBytes(threadId) - before;
  }

  private void report(final String variant, final String phase, final long[][] figures) {
    final long[] sorted = figures[EFFECTIVE_MS].clone();
    Arrays.sort(sorted);
    LogManager.instance().log(this, Level.INFO, String.format(Locale.ROOT,
        "INGEST %-19s %-15s median %,6d ms effective (min %,d, max %,d, %d runs), committer CPU %,d ms, GC %d collections %,d ms, "
            + "%,d bytes allocated per commit (medians)", variant, phase, median(figures[EFFECTIVE_MS]), sorted[0],
        sorted[sorted.length - 1], sorted.length, median(figures[CPU_MS]), median(figures[GC_COUNT]), median(figures[GC_MS]),
        median(figures[BYTES])));
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
