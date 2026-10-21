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

import com.arcadedb.utility.LockManager;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.Arrays;
import java.util.Locale;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

/**
 * Hand-off cost of {@link LockManager} with and without the spin-then-park hand-off (issue #10501), on the shape of the
 * commit's file locks: every operation takes the same resources in the same order, holds them, releases them, then works
 * outside the lock. The hold is either CPU work (a commit appending its WAL) or a sleep (a commit holding the lock across
 * an fsync, where spinning cannot help and must not cost).
 * <p>
 * Reported per row: operations per second, CPU per operation (the workers' CPU time over the operations, the cost of the
 * spin), the wait for {@code tryLock} at p50 / p99 / max (fairness) and the timeouts. Tagged {@code benchmark}: it prints a
 * table and asserts nothing. Narrow it with {@code -Dbench.spinMicros=0,50 -Dbench.threads=1,50 -Dbench.seconds=3}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("benchmark")
class LockManagerHandOffBenchmark {
  private static final ThreadMXBean THREADS = ManagementFactory.getThreadMXBean();

  private record Shape(String name, int resources, long holdNanos, boolean holdSleeps, long outsideNanos) {
  }

  @Test
  void handOff() throws Exception {
    final long[] spins = longs(System.getProperty("bench.spinMicros", "0,20,50"));
    final long[] threadCounts = longs(System.getProperty("bench.threads", "1,4,16,50"));
    final long seconds = Long.parseLong(System.getProperty("bench.seconds", "2"));

    final Shape[] shapes = {
        new Shape("commit 1 file, hold 5us cpu", 1, 5_000, false, 20_000),
        new Shape("commit 1 file, hold 40us cpu", 1, 40_000, false, 80_000),
        new Shape("commit 3 files, hold 40us cpu", 3, 40_000, false, 80_000),
        new Shape("fsync in lock, hold 1ms sleep", 1, 1_000_000, true, 50_000) };

    System.out.printf(Locale.ENGLISH, "%n%-32s %7s %8s %12s %12s %10s %10s %10s %8s%n", "shape", "threads", "spin us", "ops/s",
        "cpu us/op", "p50 us", "p99 us", "max us", "timeouts");
    // The spin values rotate from one combination to the next, so drift over the run (heat, other load) does not always
    // favour the same one; the rows print in the order they ran
    int combination = 0;
    for (final Shape shape : shapes)
      for (final long threads : threadCounts) {
        for (int s = 0; s < spins.length; s++)
          run(shape, (int) threads, TimeUnit.MICROSECONDS.toNanos(spins[(s + combination) % spins.length]), seconds);
        ++combination;
      }
  }

  private static void run(final Shape shape, final int threads, final long spinNanos, final long seconds) throws Exception {
    final LockManager<Integer, Object> lockManager = new LockManager<>(spinNanos);
    final AtomicBoolean measuring = new AtomicBoolean();
    final AtomicBoolean stop = new AtomicBoolean();
    final CyclicBarrier start = new CyclicBarrier(threads + 1);
    final long[] ops = new long[threads];
    final long[] cpu = new long[threads];
    final long[] timeouts = new long[threads];
    final long[][] waits = new long[threads][];
    final Thread[] workers = new Thread[threads];

    for (int t = 0; t < threads; t++) {
      final int index = t;
      workers[t] = new Thread(() -> {
        final Object requester = new Object();
        long[] w = new long[1 << 16];
        int count = 0;
        long cpuStart = 0;
        boolean counted = false;
        try {
          start.await();
        } catch (final Exception e) {
          return;
        }
        while (!stop.get()) {
          if (!counted && measuring.get()) {
            counted = true;
            cpuStart = THREADS.getCurrentThreadCpuTime();
          }

          final long begin = System.nanoTime();
          int locked = 0;
          for (int r = 0; r < shape.resources; r++) {
            if (lockManager.tryLock(r, requester, 5_000) != LockManager.LOCK_STATUS.YES)
              break;
            ++locked;
          }
          final long waited = System.nanoTime() - begin;

          if (locked == shape.resources)
            hold(shape.holdNanos, shape.holdSleeps);
          for (int r = locked - 1; r >= 0; r--)
            lockManager.unlock(r, requester);

          if (counted) {
            if (locked < shape.resources)
              ++timeouts[index];
            else {
              if (count == w.length)
                w = Arrays.copyOf(w, w.length * 2);
              w[count++] = waited;
            }
          }

          busy(shape.outsideNanos);
        }
        ops[index] = count;
        cpu[index] = counted ? THREADS.getCurrentThreadCpuTime() - cpuStart : 0;
        waits[index] = Arrays.copyOf(w, count);
      }, "bench-" + t);
      workers[t].start();
    }

    start.await();
    Thread.sleep(500);
    measuring.set(true);
    final long begin = System.nanoTime();
    Thread.sleep(TimeUnit.SECONDS.toMillis(seconds));
    stop.set(true);
    final long elapsed = System.nanoTime() - begin;
    for (final Thread w : workers)
      w.join();
    lockManager.close();

    long totalOps = 0, totalCpu = 0, totalTimeouts = 0;
    int n = 0;
    for (int t = 0; t < threads; t++) {
      totalOps += ops[t];
      totalCpu += cpu[t];
      totalTimeouts += timeouts[t];
      n += waits[t].length;
    }
    final long[] all = new long[n];
    int at = 0;
    for (int t = 0; t < threads; t++) {
      System.arraycopy(waits[t], 0, all, at, waits[t].length);
      at += waits[t].length;
    }
    Arrays.sort(all);

    System.out.printf(Locale.ENGLISH, "%-32s %7d %8d %12.0f %12.1f %10.1f %10.1f %10.1f %8d%n", shape.name, threads,
        TimeUnit.NANOSECONDS.toMicros(spinNanos), totalOps * 1e9 / elapsed, totalOps == 0 ? 0 : totalCpu / 1000.0 / totalOps,
        percentile(all, 0.50) / 1000.0, percentile(all, 0.99) / 1000.0, n == 0 ? 0 : all[n - 1] / 1000.0, totalTimeouts);
  }

  private static void hold(final long nanos, final boolean sleeps) {
    if (sleeps)
      LockSupport.parkNanos(nanos);
    else
      busy(nanos);
  }

  private static void busy(final long nanos) {
    final long end = System.nanoTime() + nanos;
    while (System.nanoTime() - end < 0)
      Thread.onSpinWait();
  }

  private static long percentile(final long[] sorted, final double p) {
    if (sorted.length == 0)
      return 0;
    return sorted[Math.min(sorted.length - 1, (int) (sorted.length * p))];
  }

  private static long[] longs(final String csv) {
    return Arrays.stream(csv.split(",")).map(String::trim).mapToLong(Long::parseLong).toArray();
  }
}
