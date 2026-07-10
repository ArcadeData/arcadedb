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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

/**
 * #4929: measures the commit cost of the three arcadedb.txWalFlush levels on THIS machine's storage:
 * 0 = NO (write to the OS page cache only - fast, but committed transactions can be lost or the database
 * structurally corrupted on power loss/kernel panic, since the OS persists dirty pages in any order),
 * 1 = YES_NOMETADATA (fdatasync-class force(false) of the WAL at commit - the write-ahead durability
 * contract holds; POSIX fdatasync also flushes the size metadata needed to read back an append),
 * 2 = YES (full force(true), additionally flushing mtime-class metadata - no recovery value over 1).
 * <p>
 * Commit-heavy on purpose: one small insert per transaction, so the WAL flush dominates. Not a CI test -
 * run explicitly with: mvn -pl engine test -Dtest=WALFlushBenchmark
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("benchmark")
class WALFlushBenchmark {

  private static final String DB_PATH        = "target/databases/WALFlushBenchmark";
  private static final int    WARMUP_COMMITS = 500;
  private static final int    COMMITS        = 3_000;
  private static final int    THREADS        = 8;
  private static final int    COMMITS_PER_THREAD = 800;

  @Test
  void compareWalFlushLevels() throws Exception {
    System.out.printf("%n#4929 txWalFlush benchmark - single-insert transactions, commit-heavy%n");
    System.out.printf("%-18s %12s %14s %14s%n", "level", "commits", "TPS", "avg latency");

    for (final int level : new int[] { 0, 1, 2 }) {
      final double[] single = runSingleThreaded(level);
      System.out.printf("%-18s %12d %,14.0f %11.1f us%n",
          levelName(level) + " (1 thr)", COMMITS, single[0], single[1]);
    }
    for (final int level : new int[] { 0, 1, 2 }) {
      final double[] multi = runMultiThreaded(level);
      System.out.printf("%-18s %12d %,14.0f %11.1f us%n",
          levelName(level) + " (" + THREADS + " thr)", THREADS * COMMITS_PER_THREAD, multi[0], multi[1]);
    }
  }

  private static String levelName(final int level) {
    return switch (level) {
      case 0 -> "0=NO";
      case 1 -> "1=NOMETADATA";
      default -> "2=FULL";
    };
  }

  private double[] runSingleThreaded(final int level) {
    final Database db = freshDatabase(level);
    try {
      for (int i = 0; i < WARMUP_COMMITS; i++) {
        final int it = i;
        db.transaction(() -> db.newDocument("Doc").set("i", it).set("payload", "warmup-payload-" + it).save());
      }
      final long t0 = System.nanoTime();
      for (int i = 0; i < COMMITS; i++) {
        final int it = i;
        db.transaction(() -> db.newDocument("Doc").set("i", it).set("payload", "measured-payload-" + it).save());
      }
      final long elapsed = System.nanoTime() - t0;
      return new double[] { COMMITS / (elapsed / 1e9), (elapsed / 1e3) / COMMITS };
    } finally {
      db.drop();
    }
  }

  private double[] runMultiThreaded(final int level) throws Exception {
    final Database db = freshDatabase(level);
    try {
      // Warmup incl. thread-local WAL file creation.
      runThreads(db, WARMUP_COMMITS / THREADS + 1);
      final long t0 = System.nanoTime();
      runThreads(db, COMMITS_PER_THREAD);
      final long elapsed = System.nanoTime() - t0;
      final int total = THREADS * COMMITS_PER_THREAD;
      return new double[] { total / (elapsed / 1e9), (elapsed / 1e3) / (double) COMMITS_PER_THREAD };
    } finally {
      db.drop();
    }
  }

  private static void runThreads(final Database db, final int commitsPerThread) throws Exception {
    final CountDownLatch start = new CountDownLatch(1);
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final Thread[] threads = new Thread[THREADS];
    for (int t = 0; t < THREADS; t++) {
      final int tid = t;
      threads[t] = new Thread(() -> {
        try {
          start.await();
          // Per-thread type: zero cross-thread page contention, so the measurement isolates the WAL
          // flush cost instead of MVCC retry noise on a shared bucket.
          final String type = "Doc" + tid;
          for (int i = 0; i < commitsPerThread; i++) {
            final int it = i;
            db.transaction(() -> db.newDocument(type).set("t", tid).set("i", it)
                .set("payload", "measured-payload-" + tid + "-" + it).save());
          }
        } catch (final Throwable e) {
          failure.compareAndSet(null, e);
        }
      }, "wal-bench-" + t);
      threads[t].start();
    }
    start.countDown();
    for (final Thread t : threads)
      t.join(300_000);
    if (failure.get() != null)
      throw new AssertionError("benchmark worker failed", failure.get());
  }

  private static Database freshDatabase(final int walFlushLevel) {
    FileUtils.deleteRecursively(new File(DB_PATH));
    GlobalConfiguration.TX_WAL_FLUSH.setValue(walFlushLevel);
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    final Database db = factory.create();
    db.getSchema().createDocumentType("Doc");
    for (int t = 0; t < THREADS; t++)
      db.getSchema().createDocumentType("Doc" + t);
    factory.close();
    return db;
  }
}
