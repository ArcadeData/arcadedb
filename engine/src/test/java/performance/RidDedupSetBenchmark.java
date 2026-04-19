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

import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.RidSet;
import com.arcadedb.utility.RidHashSet;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Locale;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;

/**
 * Decides when to use the bitmap-based {@link RidSet} vs the primitive-packed {@link RidHashSet} for RID deduplication. The two structures have opposite
 * memory-scaling characteristics:
 * <ul>
 *   <li>{@link RidSet} stores 1 bit per <i>possible</i> offset position, so cost tracks the MAX offset per bucket, not the count. Wins when RIDs are
 *   densely packed (e.g. a type-scan producing sequential offsets in a handful of buckets).</li>
 *   <li>{@link RidHashSet} stores 12 bytes per <i>present</i> entry (open-addressing, int[]+long[]). Cost tracks the element count. Wins when RIDs are
 *   sparse (few entries over a wide offset range).</li>
 * </ul>
 * Tagged {@code benchmark} so it is excluded from default CI runs.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("benchmark")
class RidDedupSetBenchmark {
  private static final int WARMUP     = 2;
  private static final int ITERATIONS = 3;

  @Test
  void denseSingleBucket_1M() {
    System.out.println("\n=== DENSE: 1M RIDs in bucket 12, offsets 0..999_999 (type-scan shape) ===");
    final int n = 1_000_000;
    final int bucket = 12;

    run("RidSet     ",
        () -> {
          final RidSet s = new RidSet();
          for (int i = 0; i < n; i++)
            s.add(new RID(bucket, i));
          return s;
        },
        s -> {
          long found = 0;
          for (int i = 0; i < n; i++)
            if (((RidSet) s).contains(new RID(bucket, i))) found++;
          return found;
        });

    run("RidHashSet ",
        () -> {
          final RidHashSet s = new RidHashSet(n);
          for (int i = 0; i < n; i++)
            s.add(bucket, i);
          return s;
        },
        s -> {
          long found = 0;
          for (int i = 0; i < n; i++)
            if (((RidHashSet) s).contains(bucket, i)) found++;
          return found;
        });
  }

  @Test
  void denseMultipleBuckets_1M() {
    System.out.println("\n=== DENSE: 1M RIDs across 8 buckets, sequential offsets (multi-cluster type-scan) ===");
    final int n = 1_000_000;
    final int buckets = 8;

    run("RidSet     ",
        () -> {
          final RidSet s = new RidSet();
          for (int i = 0; i < n; i++)
            s.add(new RID(i % buckets, i / buckets));
          return s;
        },
        s -> {
          long found = 0;
          for (int i = 0; i < n; i++)
            if (((RidSet) s).contains(new RID(i % buckets, i / buckets))) found++;
          return found;
        });

    run("RidHashSet ",
        () -> {
          final RidHashSet s = new RidHashSet(n);
          for (int i = 0; i < n; i++)
            s.add(i % buckets, i / buckets);
          return s;
        },
        s -> {
          long found = 0;
          for (int i = 0; i < n; i++)
            if (((RidHashSet) s).contains(i % buckets, i / buckets)) found++;
          return found;
        });
  }

  @Test
  void sparseSmall_10k() {
    System.out.println("\n=== SPARSE SMALL: 10k RIDs scattered across 64 buckets, offsets up to 10M (filtered scan) ===");
    final int n = 10_000;
    final long[] offsets = new long[n];
    final int[] buckets = new int[n];
    final ThreadLocalRandom rnd = ThreadLocalRandom.current();
    for (int i = 0; i < n; i++) {
      buckets[i] = rnd.nextInt(64);
      offsets[i] = rnd.nextLong(10_000_000L);
    }

    run("RidSet     ",
        () -> {
          final RidSet s = new RidSet();
          for (int i = 0; i < n; i++)
            s.add(new RID(buckets[i], offsets[i]));
          return s;
        },
        s -> {
          long found = 0;
          for (int i = 0; i < n; i++)
            if (((RidSet) s).contains(new RID(buckets[i], offsets[i]))) found++;
          return found;
        });

    run("RidHashSet ",
        () -> {
          final RidHashSet s = new RidHashSet(n);
          for (int i = 0; i < n; i++)
            s.add(buckets[i], offsets[i]);
          return s;
        },
        s -> {
          long found = 0;
          for (int i = 0; i < n; i++)
            if (((RidHashSet) s).contains(buckets[i], offsets[i])) found++;
          return found;
        });
  }

  @Test
  void sparseWide_50k_over_10M() {
    System.out.println("\n=== SPARSE WIDE: 50k RIDs across 32 buckets, offsets up to 10M (graph traversal) ===");
    final int n = 50_000;
    final long[] offsets = new long[n];
    final int[] buckets = new int[n];
    final ThreadLocalRandom rnd = ThreadLocalRandom.current();
    for (int i = 0; i < n; i++) {
      buckets[i] = rnd.nextInt(32);
      offsets[i] = rnd.nextLong(10_000_000L);
    }

    run("RidSet     ",
        () -> {
          final RidSet s = new RidSet();
          for (int i = 0; i < n; i++)
            s.add(new RID(buckets[i], offsets[i]));
          return s;
        },
        s -> {
          long found = 0;
          for (int i = 0; i < n; i++)
            if (((RidSet) s).contains(new RID(buckets[i], offsets[i]))) found++;
          return found;
        });

    run("RidHashSet ",
        () -> {
          final RidHashSet s = new RidHashSet(n);
          for (int i = 0; i < n; i++)
            s.add(buckets[i], offsets[i]);
          return s;
        },
        s -> {
          long found = 0;
          for (int i = 0; i < n; i++)
            if (((RidHashSet) s).contains(buckets[i], offsets[i])) found++;
          return found;
        });
  }

  // --- plumbing ---------------------------------------------------------------

  private static void run(final String name, final Supplier<Object> build, final java.util.function.ToLongFunction<Object> contains) {
    for (int i = 0; i < WARMUP; i++) {
      final Object s = build.get();
      contains.applyAsLong(s);
    }

    long bestBuild = Long.MAX_VALUE;
    long bestContains = Long.MAX_VALUE;
    Object last = null;
    for (int i = 0; i < ITERATIONS; i++) {
      final long t0 = System.nanoTime();
      last = build.get();
      final long t1 = System.nanoTime();
      final long found = contains.applyAsLong(last);
      final long t2 = System.nanoTime();
      if (t1 - t0 < bestBuild) bestBuild = t1 - t0;
      if (t2 - t1 < bestContains) bestContains = t2 - t1;
      if (found < 0) throw new IllegalStateException();  // use
    }

    gcAndSettle();
    final long before = usedHeap();
    final Object held = build.get();
    gcAndSettle();
    final long after = usedHeap();
    final long mem = Math.max(0, after - before);
    if (held.hashCode() == 0) System.out.print("");  // keep held reachable

    System.out.printf(Locale.ROOT, "%s  build=%7.1f ms   contains=%7.1f ms   mem~%s%n",
        name, bestBuild / 1_000_000.0, bestContains / 1_000_000.0, formatMem(mem));
  }

  private static void gcAndSettle() {
    for (int i = 0; i < 3; i++) {
      System.gc();
      try { Thread.sleep(30); } catch (final InterruptedException ignored) { Thread.currentThread().interrupt(); }
    }
  }

  private static long usedHeap() {
    final Runtime rt = Runtime.getRuntime();
    return rt.totalMemory() - rt.freeMemory();
  }

  private static String formatMem(final long bytes) {
    if (bytes <= 0) return "<measurement noise>";
    if (bytes < 1024L) return bytes + " B";
    if (bytes < 1024L * 1024L) return String.format(Locale.ROOT, "%.1f KB", bytes / 1024.0);
    return String.format(Locale.ROOT, "%.1f MB", bytes / (1024.0 * 1024.0));
  }
}
