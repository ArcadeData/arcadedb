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

import com.arcadedb.database.Binary;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.ImmutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PageManager;
import com.arcadedb.engine.PageSnapshot;
import com.arcadedb.engine.PaginatedComponentFile;
import com.arcadedb.engine.WALFile;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * The engine-side measurements issue #6075 asked for and PR #6100 did not produce, listed in #6116: what the
 * point-in-time snapshot machinery costs the write path when no window is open, what the t0 barrier costs, what a
 * window does to the flush thread's drain rate, and what the {@code TransactionManager} apply lock costs a
 * replicated transaction. The backup-side half - writer impact and shadow sizing during a real backup - lives in
 * {@code integration}'s {@code PageSnapshotBackupBenchmark}.
 * <p>
 * <b>On the "zero cost when no window is open" claim.</b> There is no honest way to measure a "before" from inside
 * the merged tree, so it is bounded from above instead. The write path's hook is one volatile read of
 * {@code activeSnapshots} plus a branch; the expensive shape of that branch is "a window IS open, but on another
 * database", which pays the volatile read, the array walk and a database comparison per page write, and still
 * captures nothing. If that configuration is indistinguishable from the no-window one, the no-window one - strictly
 * cheaper - cannot be costing anything either.
 * <p>
 * Excluded from the normal build ({@code @Tag("benchmark")}). Run it explicitly:
 * <pre>
 *   mvn -o -pl engine test -Dtest=PageSnapshotOverheadBenchmark -Dgroups=benchmark -DexcludedGroups=
 * </pre>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("benchmark")
class PageSnapshotOverheadBenchmark {
  private static final String DATABASE_PATH   = "target/databases/page-snapshot-overhead-benchmark";
  private static final String NEIGHBOUR_PATH  = "target/databases/page-snapshot-overhead-benchmark-neighbour";
  private static final String TYPE            = "Doc";
  private static final int    RECORDS         = Integer.parseInt(
      System.getProperty("arcadedb.snapshot.benchmark.records", "200000"));
  private static final int    PAYLOAD_SIZE    = 300;
  /** Records per transaction: a realistic batch, and the unit every latency percentile below is measured over. */
  private static final int    BATCH           = 20;
  private static final int    WARMUP_BATCHES  = 500;
  private static final int    MEASURE_BATCHES = 5_000;

  private static Database database;
  private static Database neighbour;

  @BeforeAll
  static void buildDatabases() {
    FileUtils.deleteRecursively(new File(DATABASE_PATH));
    FileUtils.deleteRecursively(new File(NEIGHBOUR_PATH));

    database = create(DATABASE_PATH, RECORDS);
    // A SECOND, TINY DATABASE: SOMETHING TO OPEN A WINDOW ON THAT IS NOT THE ONE BEING WRITTEN TO
    neighbour = create(NEIGHBOUR_PATH, 1_000);

    System.out.printf("[snapshot-benchmark] built %s (%,d records) and a %s neighbour%n",
        FileUtils.getSizeAsString(directorySize(new File(DATABASE_PATH))), RECORDS,
        FileUtils.getSizeAsString(directorySize(new File(NEIGHBOUR_PATH))));
  }

  @AfterAll
  static void dropDatabases() {
    if (database != null && database.isOpen())
      database.drop();
    if (neighbour != null && neighbour.isOpen())
      neighbour.drop();
    FileUtils.deleteRecursively(new File(DATABASE_PATH));
    FileUtils.deleteRecursively(new File(NEIGHBOUR_PATH));
  }

  /**
   * Steady-state write throughput and commit latency in the three states the write path can be in. The first two
   * lines are the "zero cost when idle" claim; the third is what a backup actually costs the writers it runs beside,
   * and is the number to compare against the suspend-and-freeze path measured in the integration benchmark.
   */
  @Test
  void steadyStateWriteThroughputAndCommitLatency() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;

    // WARM UP THE JIT AND THE PAGE CACHE ONCE, SO THE FIRST CONFIGURATION IS NOT PENALISED
    writeBatches(WARMUP_BATCHES);

    // EACH CONFIGURATION IS MEASURED TWICE, IN ROUNDS. THE DIFFERENCE BETWEEN THE THREE IS EXPECTED TO BE SMALL
    // ENOUGH TO BE COMPARABLE TO THE RUN-TO-RUN SPREAD OF A SINGLE MACHINE, SO PRINTING ONE SAMPLE EACH WOULD INVITE
    // READING NOISE AS SIGNAL - THE SECOND ROUND IS THERE TO SHOW HOW BIG THAT NOISE IS
    final List<String> report = new ArrayList<>();
    for (int round = 1; round <= 2; round++) {
      report.add(measureWrites("round " + round + ": no window open anywhere", null));

      try (final PageSnapshot ignored = ((DatabaseInternal) neighbour).getPageManager()
          .openSnapshot((DatabaseInternal) neighbour)) {
        report.add(measureWrites("round " + round + ": window open on ANOTHER database", null));
      }

      try (final PageSnapshot snapshot = db.getPageManager().openSnapshot(db)) {
        report.add(measureWrites("round " + round + ": window open on THIS database", snapshot));
      }
    }

    System.out.println("\n[snapshot-benchmark] steady-state write path, " + BATCH + " records per transaction:");
    report.forEach(line -> System.out.println("  " + line));
  }

  /**
   * The t0 barrier is the one stall the design keeps: it drains the flush queue in full, parks the flush thread on a
   * batch boundary, and takes the apply and page-manager locks. Its duration is therefore a function of how much
   * work the flush pipeline is carrying, which is what this sweeps - by loading the pipeline with an increasing
   * number of writer threads and recording the depth actually observed rather than a depth commanded in advance.
   */
  @Test
  void t0BarrierDurationAtVariousFlushQueueDepths() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final PageManager pageManager = db.getPageManager();

    System.out.println("\n[snapshot-benchmark] t0 barrier duration under increasing write pressure. The barrier drains "
        + "the flush queue IN FULL, so the queue depth it starts from is one input; the other is how fast commits "
        + "arrive, because a commit landing between the drain and the flush-thread suspension makes it retry:");

    for (final int writers : new int[] { 0, 1, 2, 4, 8 }) {
      final AtomicBoolean running = new AtomicBoolean(true);
      final AtomicLong maxQueueDepth = new AtomicLong();
      final List<Thread> threads = new ArrayList<>();
      for (int w = 0; w < writers; w++) {
        final Thread thread = new Thread(() -> {
          while (running.get())
            writeBatches(1);
        }, "snapshot-benchmark-writer-" + w);
        thread.setDaemon(true);
        thread.start();
        threads.add(thread);
      }

      final Thread depthSampler = new Thread(() -> {
        while (running.get()) {
          maxQueueDepth.accumulateAndGet(pageManager.getStats().pageFlushQueueLength, Math::max);
          Thread.onSpinWait();
        }
      }, "snapshot-benchmark-depth-sampler");
      depthSampler.setDaemon(true);
      depthSampler.start();

      try {
        // LET THE PIPELINE FILL BEFORE LOOKING AT IT
        Thread.sleep(1_500);

        // THE BARRIER IS VISIBLY VARIABLE UNDER LOAD (IT RETRIES), SO ONE SAMPLE WOULD BE A COIN TOSS
        final long[] durationsUs = new long[5];
        for (int i = 0; i < durationsUs.length; i++) {
          final long begin = System.nanoTime();
          try (final PageSnapshot ignored = pageManager.openSnapshot(db)) {
            durationsUs[i] = (System.nanoTime() - begin) / 1_000;
          }
        }
        Arrays.sort(durationsUs);

        System.out.printf("  %d concurrent writer(s): peak queue depth %,6d pages, barrier min=%,8d us median=%,8d us "
                + "max=%,8d us%n", writers, maxQueueDepth.get(), durationsUs[0], durationsUs[durationsUs.length / 2],
            durationsUs[durationsUs.length - 1]);
      } finally {
        running.set(false);
        for (final Thread thread : threads)
          thread.join(30_000);
        depthSampler.join(30_000);
      }
    }
  }

  /**
   * Challenge C5 of #6075: the pre-image capture happens inside the write slot the flush already holds, so it eats
   * into the flush thread's drain rate. This is how much of it - pages actually written to disk per second, with and
   * without a window open, under the same sustained load.
   */
  @Test
  void flushThreadDrainRateWithAndWithoutAWindowOpen() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;

    System.out.println("\n[snapshot-benchmark] flush-thread drain rate under a sustained writer:");
    System.out.println("  " + measureDrainRate("no window open", false));
    System.out.println("  " + measureDrainRate("window open", true));
  }

  /**
   * The apply lock #6075 added to {@code TransactionManager}: one uncontended {@code ReentrantReadWriteLock} read
   * acquisition per REPLICATED transaction (never on the local commit path), sitting next to a WAL parse and a page
   * write. Reasoned to be unmeasurable in the PR, measured here: the cost of an apply, against the cost of the bare
   * lock pair on the same machine.
   */
  @Test
  void haReplayApplyLockCost() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final int fileId = db.getSchema().getType(TYPE).getBuckets(false).get(0).getFileId();
    final PaginatedComponentFile file = (PaginatedComponentFile) db.getFileManager().getFile(fileId);
    final PageId pageId = new PageId(db, fileId, 0);
    final ImmutablePage page = db.getPageManager().getImmutablePage(pageId, file.getPageSize(), false, true);

    final WALFile.WALPage walPage = new WALFile.WALPage();
    walPage.fileId = fileId;
    walPage.pageNumber = 0;
    walPage.changesFrom = BasePage.PAGE_HEADER_SIZE;
    walPage.changesTo = BasePage.PAGE_HEADER_SIZE + 10;
    walPage.currentPageSize = page.getContentSize();
    final byte[] content = new byte[11];
    System.arraycopy(page.getContent().array(), walPage.changesFrom, content, 0, content.length);
    walPage.currentContent = new Binary(content);

    final WALFile.WALTransaction walTx = new WALFile.WALTransaction();
    walTx.timestamp = System.currentTimeMillis();
    walTx.pages = new WALFile.WALPage[] { walPage };

    int version = (int) page.getVersion();
    // WARM UP: THE FIRST APPLIES PAY FOR CLASS LOADING AND THE PAGE'S FIRST TOUCH
    for (int i = 0; i < 200; i++) {
      walTx.txId = i;
      walPage.currentPageVersion = ++version;
      db.getTransactionManager().applyChanges(walTx, Collections.emptyMap(), false);
    }

    final int applies = 20_000;
    final long applyBegin = System.nanoTime();
    for (int i = 0; i < applies; i++) {
      walTx.txId = 1_000_000 + i;
      walPage.currentPageVersion = ++version;
      db.getTransactionManager().applyChanges(walTx, Collections.emptyMap(), false);
    }
    final long applyNanos = System.nanoTime() - applyBegin;

    final ReentrantReadWriteLock lock = db.getTransactionManager().getApplyLock();
    final long lockBegin = System.nanoTime();
    for (int i = 0; i < applies; i++) {
      lock.readLock().lock();
      lock.readLock().unlock();
    }
    final long lockNanos = System.nanoTime() - lockBegin;

    System.out.printf("%n[snapshot-benchmark] replicated apply: %,d ns/tx; the uncontended apply-lock pair it now "
            + "carries: %,d ns/tx (%.4f%% of the apply)%n", applyNanos / applies, lockNanos / applies,
        100.0 * lockNanos / applyNanos);
  }

  // ------------------------------------------------------------------------------------------------------- HELPERS

  /**
   * Runs {@link #MEASURE_BATCHES} transactions and reports throughput and the commit-latency percentiles. When a
   * window is passed it also reports what that window had to capture, which is what the middle line of the report
   * has to show as zero for the "another database" case to mean anything.
   */
  private String measureWrites(final String label, final PageSnapshot snapshot) {
    final long[] latenciesUs = new long[MEASURE_BATCHES];
    final long begin = System.nanoTime();
    for (int i = 0; i < MEASURE_BATCHES; i++) {
      final long batchBegin = System.nanoTime();
      writeBatches(1);
      latenciesUs[i] = (System.nanoTime() - batchBegin) / 1_000;
    }
    final double seconds = (System.nanoTime() - begin) / 1e9;
    Arrays.sort(latenciesUs);

    return "%-46s %,9.0f rec/s  p50=%,6dus p95=%,7dus p99=%,7dus  shadowed=%,d pages (%s)".formatted(label,
        MEASURE_BATCHES * BATCH / seconds, percentile(latenciesUs, 50), percentile(latenciesUs, 95),
        percentile(latenciesUs, 99), snapshot != null ? snapshot.getShadowedPages() : 0,
        FileUtils.getSizeAsString(snapshot != null ? snapshot.getShadowSizeInBytes() : 0));
  }

  private String measureDrainRate(final String label, final boolean withWindow) throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final PageManager pageManager = db.getPageManager();

    final AtomicBoolean running = new AtomicBoolean(true);
    final Thread writer = new Thread(() -> {
      while (running.get())
        writeBatches(1);
    }, "snapshot-benchmark-drain-writer");
    writer.setDaemon(true);

    PageSnapshot snapshot = null;
    try {
      writer.start();
      Thread.sleep(1_000);
      if (withWindow)
        snapshot = pageManager.openSnapshot(db);

      final long pagesBefore = pageManager.getStats().pagesWritten;
      final long begin = System.nanoTime();
      Thread.sleep(5_000);
      final double seconds = (System.nanoTime() - begin) / 1e9;
      final long written = pageManager.getStats().pagesWritten - pagesBefore;

      return "%-16s %,9.0f pages/s  queue=%,d  shadowed=%,d pages".formatted(label, written / seconds,
          pageManager.getStats().pageFlushQueueLength, snapshot != null ? snapshot.getShadowedPages() : 0);
    } finally {
      running.set(false);
      writer.join(30_000);
      if (snapshot != null)
        snapshot.close();
    }
  }

  /**
   * The measured workload: half inserts, half updates of records picked at random from the ones that existed at
   * build time.
   * <p>
   * The update half is not decoration. An insert-only load appends to the tail of the bucket, and a page appended
   * after t0 needs no pre-image (challenge C7) - so it would measure the write path with the hook present but never
   * firing, and report a snapshot window as free when it is not. The random updates dirty pages scattered across the
   * whole file, which is what makes a window actually capture.
   */
  private static void writeBatches(final int batches) {
    for (int b = 0; b < batches; b++)
      database.transaction(() -> {
        for (int i = 0; i < BATCH / 2; i++)
          database.newDocument(TYPE).set("id", NEXT_ID.incrementAndGet()).set("payload", PAYLOAD).save();

        if (!MUTABLE_RIDS.isEmpty())
          for (int i = 0; i < BATCH / 2; i++) {
            final RID rid = MUTABLE_RIDS.get(ThreadLocalRandom.current().nextInt(MUTABLE_RIDS.size()));
            rid.asDocument(true).modify().set("payload", PAYLOAD).save();
          }
      });
  }

  private static final AtomicLong NEXT_ID      = new AtomicLong();
  private static final String     PAYLOAD      = "x".repeat(PAYLOAD_SIZE);
  /** RIDs of the records that existed before the measurement, the targets of the update half of the workload. */
  private static final List<RID>  MUTABLE_RIDS = new ArrayList<>();
  /** Enough randomness to hit pages all over the file without paying for a RID per record on a huge dataset. */
  private static final int        MAX_MUTABLE_RIDS = 100_000;

  private static Database create(final String path, final int records) {
    final Database db = new DatabaseFactory(path).create();
    db.getSchema().createDocumentType(TYPE);
    final boolean collectRids = MUTABLE_RIDS.isEmpty();
    db.transaction(() -> {
      for (int i = 0; i < records; i++) {
        final MutableDocument document = db.newDocument(TYPE).set("id", NEXT_ID.incrementAndGet()).set("payload", PAYLOAD);
        document.save();
        if (collectRids && MUTABLE_RIDS.size() < MAX_MUTABLE_RIDS)
          MUTABLE_RIDS.add(document.getIdentity());
      }
    });
    ((DatabaseInternal) db).getPageManager().waitAllPagesOfDatabaseAreFlushed(db);
    return db;
  }

  private static long percentile(final long[] sorted, final int percentile) {
    if (sorted.length == 0)
      return 0;
    return sorted[Math.min(sorted.length - 1, (int) ((long) sorted.length * percentile / 100))];
  }

  private static long directorySize(final File directory) {
    long size = 0;
    final File[] files = directory.listFiles();
    if (files != null)
      for (final File file : files)
        size += file.isDirectory() ? directorySize(file) : file.length();
    return size;
  }
}
