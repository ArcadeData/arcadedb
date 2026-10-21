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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.LockManager;
import com.sun.management.OperatingSystemMXBean;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Embedded commits racing for the same file locks, with and without the spin-then-park hand-off of the commit locks
 * (issue #10501, {@code arcadedb.commitLockSpin}): every thread inserts one document per transaction into a type with a
 * UNIQUE index, so each commit locks the type's bucket and index files. With one type every thread queues on the same
 * locks; with several, thread i writes type i % types and the contention is spread.
 * <p>
 * Reported per row: commits per second, process CPU per commit (what the spin costs), the hand-offs of the file locks
 * taken spinning / parked and the spins that ran out, and the transactions that failed after their retries. Tagged
 * {@code benchmark}: it prints a table and asserts nothing. Narrow it with {@code -Dbench.spinMicros=0,50
 * -Dbench.threads=1,8,50 -Dbench.walFlush=0,2 -Dbench.groupCommit=false,true -Dbench.types=1,8 -Dbench.seconds=3}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("benchmark")
class CommitLockConvoyBenchmark {
  private static final String DB_PATH = "target/databases/CommitLockConvoyBenchmark";

  @Test
  void commits() throws Exception {
    final long[] spins = longs(System.getProperty("bench.spinMicros", "0,50"));
    final long[] threadCounts = longs(System.getProperty("bench.threads", "1,8,50"));
    final long[] walFlushes = longs(System.getProperty("bench.walFlush", "0,2"));
    final String[] groupCommits = System.getProperty("bench.groupCommit", "false,true").split(",");
    final long[] typeCounts = longs(System.getProperty("bench.types", "1,8"));
    final long seconds = Long.parseLong(System.getProperty("bench.seconds", "3"));

    System.out.printf(Locale.ENGLISH, "%n%5s %8s %6s %5s %7s %9s %10s %11s %11s %10s %10s %8s %7s%n", "types", "walFlush",
        "group", "spin", "threads", "commits/s", "cpu us/tx", "spinHandOff", "parkHandOff", "spinExpire", "handOff us", "hold us",
        "failed");

    final Object previousSpin = GlobalConfiguration.COMMIT_LOCK_SPIN.getValue();
    int combination = 0;
    try {
      for (final long types : typeCounts)
        for (final long walFlush : walFlushes)
          for (final String groupCommit : groupCommits) {
            final boolean group = Boolean.parseBoolean(groupCommit.trim());
            if (group && walFlush == 0)
              // group commit only changes anything with a WAL flush
              continue;
            for (final long threads : threadCounts) {
              // the spin values rotate from one combination to the next, so drift over the run does not always favour
              // the same one; the rows print in the order they ran
              for (int s = 0; s < spins.length; s++)
                run((int) types, (int) walFlush, group, spins[(s + combination) % spins.length], (int) threads, seconds);
              ++combination;
            }
          }
    } finally {
      GlobalConfiguration.COMMIT_LOCK_SPIN.setValue(previousSpin);
    }
  }

  private static void run(final int types, final int walFlush, final boolean groupCommit, final long spinMicros, final int threads,
      final long seconds) throws Exception {
    FileUtils.deleteRecursively(new File(DB_PATH));
    // the transaction manager reads the spin when the database opens
    GlobalConfiguration.COMMIT_LOCK_SPIN.setValue(spinMicros);

    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      final Database database = factory.create();
      try {
        database.getConfiguration().setValue(GlobalConfiguration.TX_WAL_FLUSH, walFlush);
        database.getConfiguration().setValue(GlobalConfiguration.TX_WAL_GROUP_COMMIT, groupCommit);
        for (int t = 0; t < types; t++) {
          final DocumentType type = database.getSchema().createDocumentType("KV" + t);
          type.createProperty("id", String.class);
          type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");
        }

        final AtomicBoolean measuring = new AtomicBoolean();
        final AtomicBoolean stop = new AtomicBoolean();
        final AtomicLong commits = new AtomicLong();
        final AtomicLong failed = new AtomicLong();
        final CyclicBarrier start = new CyclicBarrier(threads + 1);
        final Thread[] workers = new Thread[threads];
        final String payload = "x".repeat(64);

        for (int i = 0; i < threads; i++) {
          final String typeName = "KV" + (i % types);
          final String prefix = i + "-";
          workers[i] = new Thread(() -> {
            try {
              start.await();
            } catch (final Exception e) {
              return;
            }
            long key = 0;
            while (!stop.get()) {
              final String id = prefix + key++;
              try {
                database.transaction(() -> database.newDocument(typeName).set("id", id).set("value", payload).save(), false, 50);
                if (measuring.get())
                  commits.incrementAndGet();
              } catch (final RuntimeException e) {
                if (measuring.get())
                  failed.incrementAndGet();
              }
            }
          }, "bench-" + i);
          workers[i].start();
        }

        final OperatingSystemMXBean os = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        start.await();
        Thread.sleep(1000);

        final Map<String, Object> statsBefore = stats(database);
        final long cpuBefore = os.getProcessCpuTime();
        final long begin = System.nanoTime();
        measuring.set(true);
        // samples the average hold of the contended file locks, the number the spin decides on
        long holdSum = 0;
        int holdSamples = 0;
        final long end = begin + TimeUnit.SECONDS.toNanos(seconds);
        while (System.nanoTime() - end < 0) {
          Thread.sleep(50);
          for (final LockManager.LockStats s : ((DatabaseInternal) database).getTransactionManager().getLockStats())
            if (s.waiters() > 0) {
              holdSum += s.averageHoldMicros();
              ++holdSamples;
            }
        }
        measuring.set(false);
        final long elapsed = System.nanoTime() - begin;
        final long cpu = os.getProcessCpuTime() - cpuBefore;
        final Map<String, Object> statsAfter = stats(database);

        stop.set(true);
        for (final Thread w : workers)
          w.join();

        final long n = commits.get();
        final long spinHandOffs = delta(statsBefore, statsAfter, "lockSpinHandOffs");
        final long parkHandOffs = delta(statsBefore, statsAfter, "lockParkHandOffs");
        final long handOffs = spinHandOffs + parkHandOffs;
        System.out.printf(Locale.ENGLISH, "%5d %8d %6s %5d %7d %9.0f %10.1f %11d %11d %10d %10.1f %8.1f %7d%n", types, walFlush,
            groupCommit, spinMicros, threads, n * 1e9 / elapsed, n == 0 ? 0 : cpu / 1000.0 / n, spinHandOffs, parkHandOffs,
            delta(statsBefore, statsAfter, "lockSpinsExpired"),
            handOffs == 0 ? 0 : delta(statsBefore, statsAfter, "lockHandOffNanos") / 1000.0 / handOffs,
            holdSamples == 0 ? 0 : (double) holdSum / holdSamples, failed.get());
      } finally {
        database.drop();
      }
    }
  }

  private static Map<String, Object> stats(final Database database) {
    return ((DatabaseInternal) database).getTransactionManager().getStats();
  }

  private static long delta(final Map<String, Object> before, final Map<String, Object> after, final String key) {
    return ((Number) after.get(key)).longValue() - ((Number) before.get(key)).longValue();
  }

  private static long[] longs(final String csv) {
    return Arrays.stream(csv.split(",")).map(String::trim).mapToLong(Long::parseLong).toArray();
  }
}
