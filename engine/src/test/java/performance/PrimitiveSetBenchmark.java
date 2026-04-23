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
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
package performance;

import com.arcadedb.utility.IntHashSet;
import com.arcadedb.utility.LongHashSet;
import com.arcadedb.utility.LongObjectHashMap;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;

/**
 * Compares the primitive {@link IntHashSet} / {@link LongHashSet} against the JDK
 * {@link HashSet} (and {@link ConcurrentHashMap#newKeySet()} for the concurrent case)
 * on workloads that mirror real ArcadeDB usage:
 *
 * <ul>
 *   <li>{@code intSetBucketIdsSmall} - bucket-id sets in OpenCypher traversal
 *       ({@code PropagateChainOp.validBuckets}, {@code SQLFunctionVectorNeighbors.allowedBucketIds}):
 *       small (10-100s of ints), high frequency.</li>
 *   <li>{@code intSetFileIdsCommit} - per-commit file lock sets in
 *       {@code TransactionContext.modifiedFiles}: 10-100s of ints, called per commit.</li>
 *   <li>{@code longSetVertexKeysBulk} - vertex-key dedupe in
 *       {@code GraphBatch.knownNewVertexKeys} / {@code allKeys}: 100K+ entries, single-threaded.</li>
 * </ul>
 *
 * For each shape we measure: build cost, contains cost, retained heap. Tagged
 * {@code benchmark} so it stays out of default CI runs.
 */
@Tag("benchmark")
class PrimitiveSetBenchmark {
  private static final int WARMUP     = 3;
  private static final int ITERATIONS = 5;

  // ---------------- Int sets ----------------

  @Test
  void intSetBucketIdsSmall() {
    System.out.println("\n=== INT SET: 64 bucket-ids x 100k builds (OpenCypher validBuckets shape) ===");
    final int n = 64;
    final int containsLoops = 50_000;
    final int builds = 100_000;
    final int[] data = randomDistinctInts(n, 0, 100_000);

    runIntBuild("HashSet<Integer> ", () -> {
      final Set<Integer> s = new HashSet<>();
      for (final int v : data) s.add(v);
      return s;
    }, s -> {
      long c = 0;
      @SuppressWarnings("unchecked")
      final Set<Integer> hs = (Set<Integer>) s;
      for (int i = 0; i < containsLoops; i++)
        for (final int v : data) if (hs.contains(v)) c++;
      return c;
    });

    runIntBuild("IntHashSet       ", () -> {
      final IntHashSet s = new IntHashSet(n);
      for (final int v : data) s.add(v);
      return s;
    }, s -> {
      long c = 0;
      final IntHashSet hs = (IntHashSet) s;
      for (int i = 0; i < containsLoops; i++)
        for (final int v : data) if (hs.contains(v)) c++;
      return c;
    });

    // Population-of-instances heap measurement: build N copies and measure retained heap.
    measurePopulationHeap("HashSet<Integer> ", builds, () -> {
      final Set<Integer> s = new HashSet<>(n);
      for (final int v : data) s.add(v);
      return s;
    });
    measurePopulationHeap("IntHashSet       ", builds, () -> {
      final IntHashSet s = new IntHashSet(n);
      for (final int v : data) s.add(v);
      return s;
    });
  }

  @Test
  void intSetFileIdsCommit() {
    System.out.println("\n=== INT SET: 200 file-ids x 100k builds (TransactionContext.modifiedFiles shape) ===");
    final int n = 200;
    final int containsLoops = 50_000;
    final int builds = 100_000;
    final int[] data = randomDistinctInts(n, 0, 50_000);

    runIntBuild("HashSet<Integer> ", () -> {
      final Set<Integer> s = new HashSet<>();
      for (final int v : data) s.add(v);
      return s;
    }, s -> {
      long c = 0;
      @SuppressWarnings("unchecked")
      final Set<Integer> hs = (Set<Integer>) s;
      for (int i = 0; i < containsLoops; i++)
        for (final int v : data) if (hs.contains(v)) c++;
      return c;
    });

    runIntBuild("IntHashSet       ", () -> {
      final IntHashSet s = new IntHashSet(n);
      for (final int v : data) s.add(v);
      return s;
    }, s -> {
      long c = 0;
      final IntHashSet hs = (IntHashSet) s;
      for (int i = 0; i < containsLoops; i++)
        for (final int v : data) if (hs.contains(v)) c++;
      return c;
    });

    measurePopulationHeap("HashSet<Integer> ", builds, () -> {
      final Set<Integer> s = new HashSet<>(n);
      for (final int v : data) s.add(v);
      return s;
    });
    measurePopulationHeap("IntHashSet       ", builds, () -> {
      final IntHashSet s = new IntHashSet(n);
      for (final int v : data) s.add(v);
      return s;
    });
  }

  // ---------------- Long sets ----------------

  @Test
  void longSetVertexKeysBulk() {
    System.out.println("\n=== LONG SET: 100k vertex keys (GraphBatch.allKeys shape) ===");
    final int n = 100_000;
    final long[] data = new long[n];
    final ThreadLocalRandom rnd = ThreadLocalRandom.current();
    for (int i = 0; i < n; i++)
      // 24-bit bucket | 40-bit position - matches GraphBatch packing
      data[i] = ((long) rnd.nextInt(1 << 12) << 40) | (rnd.nextLong() & 0xFFFFFFFFFFL);

    runLongBuild("HashSet<Long>            ", () -> {
      final Set<Long> s = new HashSet<>();
      for (final long v : data) s.add(v);
      return s;
    }, s -> {
      long c = 0;
      @SuppressWarnings("unchecked")
      final Set<Long> hs = (Set<Long>) s;
      for (final long v : data) if (hs.contains(v)) c++;
      return c;
    });

    runLongBuild("ConcurrentHashMap.keySet ", () -> {
      final Set<Long> s = ConcurrentHashMap.newKeySet();
      for (final long v : data) s.add(v);
      return s;
    }, s -> {
      long c = 0;
      @SuppressWarnings("unchecked")
      final Set<Long> hs = (Set<Long>) s;
      for (final long v : data) if (hs.contains(v)) c++;
      return c;
    });

    runLongBuild("LongHashSet              ", () -> {
      final LongHashSet s = new LongHashSet(n);
      for (final long v : data) s.add(v);
      return s;
    }, s -> {
      long c = 0;
      final LongHashSet hs = (LongHashSet) s;
      for (final long v : data) if (hs.contains(v)) c++;
      return c;
    });
  }

  // ---------------- Long-keyed maps ----------------

  /** Marker value for the map benchmarks - mimics RID stored as the map value. */
  private static final Object MARKER = new Object();

  @Test
  void longObjectMapVertexHeadShape() {
    System.out.println("\n=== LONG-KEYED MAP: 100k vertex keys -> RID (GraphBatch.deferredOutHead shape) ===");
    final int n = 100_000;
    final long[] data = new long[n];
    final ThreadLocalRandom rnd = ThreadLocalRandom.current();
    for (int i = 0; i < n; i++)
      data[i] = ((long) rnd.nextInt(1 << 12) << 40) | (rnd.nextLong() & 0xFFFFFFFFFFL);

    runMeasure("HashMap<Long, V>             ", () -> {
      final HashMap<Long, Object> m = new HashMap<>();
      for (final long v : data) m.put(v, MARKER);
      return m;
    }, m -> {
      long c = 0;
      @SuppressWarnings("unchecked")
      final HashMap<Long, Object> hm = (HashMap<Long, Object>) m;
      for (final long v : data) if (hm.get(v) != null) c++;
      return c;
    });

    runMeasure("ConcurrentHashMap<Long, V>   ", () -> {
      final ConcurrentHashMap<Long, Object> m = new ConcurrentHashMap<>();
      for (final long v : data) m.put(v, MARKER);
      return m;
    }, m -> {
      long c = 0;
      @SuppressWarnings("unchecked")
      final ConcurrentHashMap<Long, Object> hm = (ConcurrentHashMap<Long, Object>) m;
      for (final long v : data) if (hm.get(v) != null) c++;
      return c;
    });

    runMeasure("LongObjectHashMap<V>         ", () -> {
      final LongObjectHashMap<Object> m = new LongObjectHashMap<>(n);
      for (final long v : data) m.put(v, MARKER);
      return m;
    }, m -> {
      long c = 0;
      @SuppressWarnings("unchecked")
      final LongObjectHashMap<Object> hm = (LongObjectHashMap<Object>) m;
      for (final long v : data) if (hm.get(v) != null) c++;
      return c;
    });
  }

  // --- plumbing ---------------------------------------------------------------

  private static int[] randomDistinctInts(final int n, final int min, final int max) {
    final int[] out = new int[n];
    final HashSet<Integer> seen = new HashSet<>(n * 2);
    final ThreadLocalRandom rnd = ThreadLocalRandom.current();
    int i = 0;
    while (i < n) {
      final int v = rnd.nextInt(min, max);
      if (seen.add(v))
        out[i++] = v;
    }
    return out;
  }

  private static void runIntBuild(final String name, final Supplier<Object> build, final ToLongFunction<Object> contains) {
    runMeasure(name, build, contains);
  }

  private static void runLongBuild(final String name, final Supplier<Object> build, final ToLongFunction<Object> contains) {
    runMeasure(name, build, contains);
  }

  private static void runMeasure(final String name, final Supplier<Object> build, final ToLongFunction<Object> contains) {
    for (int i = 0; i < WARMUP; i++) {
      final Object s = build.get();
      contains.applyAsLong(s);
    }

    long bestBuild = Long.MAX_VALUE;
    long bestContains = Long.MAX_VALUE;
    for (int i = 0; i < ITERATIONS; i++) {
      final long t0 = System.nanoTime();
      final Object s = build.get();
      final long t1 = System.nanoTime();
      final long found = contains.applyAsLong(s);
      final long t2 = System.nanoTime();
      if (t1 - t0 < bestBuild) bestBuild = t1 - t0;
      if (t2 - t1 < bestContains) bestContains = t2 - t1;
      if (found < 0) throw new IllegalStateException();
    }

    gcAndSettle();
    final long before = usedHeap();
    final Object held = build.get();
    gcAndSettle();
    final long after = usedHeap();
    final long mem = Math.max(0, after - before);
    if (held.hashCode() == 0) System.out.print("");

    System.out.printf(Locale.ROOT, "%s  build=%8.2f ms   contains=%8.2f ms   mem~%s%n",
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

  /**
   * Builds {@code count} populated instances and measures retained heap. Reports per-instance
   * cost. Forces GC to bracket the measurement so noise is small at population scale.
   */
  private static void measurePopulationHeap(final String name, final int count, final Supplier<Object> build) {
    // Warm up so JIT/escape-analysis state matches the timed run.
    for (int i = 0; i < 100; i++)
      build.get();

    gcAndSettle();
    final long before = usedHeap();
    final Object[] holder = new Object[count];
    for (int i = 0; i < count; i++)
      holder[i] = build.get();
    gcAndSettle();
    final long after = usedHeap();
    final long total = Math.max(0, after - before);

    System.out.printf(Locale.ROOT, "%s  population heap=%s for %d instances => ~%.1f bytes/instance%n",
        name, formatMem(total), count, total / (double) count);

    // Keep alive
    if (holder.hashCode() == 0) System.out.print("");
  }

  private static String formatMem(final long bytes) {
    if (bytes <= 0) return "<measurement noise>";
    if (bytes < 1024L) return bytes + " B";
    if (bytes < 1024L * 1024L) return String.format(Locale.ROOT, "%.1f KB", bytes / 1024.0);
    return String.format(Locale.ROOT, "%.1f MB", bytes / (1024.0 * 1024.0));
  }
}
