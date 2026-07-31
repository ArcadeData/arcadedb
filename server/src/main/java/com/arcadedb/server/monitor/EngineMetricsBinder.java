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
package com.arcadedb.server.monitor;

import com.arcadedb.Profiler;
import com.arcadedb.serializer.json.JSONObject;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Exposes engine-wide statistics from {@link Profiler} to Micrometer so they appear in {@code /prometheus} and any
 * other registered registry (e.g. OTLP). Each meter re-reads a cached {@link Profiler} snapshot on scrape, so values
 * track the running engine without keeping extra state.
 * <p>
 * The {@code Profiler} aggregates JVM-global counters ({@link com.arcadedb.engine.PageManager} cache and WAL totals)
 * plus the sum of per-database counters. Its JSON wraps every stat under an inner {@code count} / {@code space} /
 * {@code value} key (only {@code walTotalFiles} is a bare scalar), so the reader extracts the inner numeric rather
 * than treating the wrapper object as a number.
 * <p>
 * <b>Counter vs gauge (#5636).</b> A never-decreasing total registered as a {@code Gauge} exports with no
 * {@code _total} suffix and no type hint that {@code rate()} / {@code increase()} are the correct functions over the
 * series; a dashboard author reading {@code arcadedb_engine_mvcc_conflicts} as a gauge sees a line that only goes up
 * and learns nothing. Every monotonic total is therefore a {@link FunctionCounter} and only the genuinely
 * instantaneous readings ({@code wal.files}, {@code files.open}, {@code databases}) stay a {@link Gauge}. Note this
 * requires the underlying value to never go backwards: {@code Profiler} carries the counters of a closed database in
 * a retained baseline precisely so that holds - a decrease would be read as a counter reset and fabricate a rate
 * spike on the next scrape.
 * <p>
 * Building the snapshot is comparatively expensive (it queries JMX, free disk space and iterates the open databases),
 * and on a scrape all meters fire near-simultaneously; the snapshot is therefore memoized for a short TTL so one
 * scrape rebuilds it at most once instead of once per meter.
 * <p>
 * Per-database tagging is intentionally not done here: the binder is bound at server startup before databases are
 * loaded, and the page-cache / WAL counters are JVM-global singletons. A per-database-tagged breakdown would require
 * a periodically refreshed {@code MultiGauge}.
 */
public final class EngineMetricsBinder implements MeterBinder {

  private static final long SNAPSHOT_TTL_NANOS = TimeUnit.SECONDS.toNanos(1);

  /**
   * The memoized {@link Profiler} snapshot every meter reads through.
   * <p>
   * Static and therefore strongly reachable for the JVM's lifetime, on purpose. Micrometer's
   * {@code FunctionCounter}/{@code Gauge} {@code (obj, fn)} builders hold a WEAK reference to {@code obj}, and the
   * only caller does {@code new EngineMetricsBinder().bindTo(registry)} - so anchoring the meters to the binder
   * instance would let every one of them silently stop reporting at the next GC. Being static also means a server
   * that binds several registries (Prometheus + OTLP) rebuilds the expensive snapshot once per scrape window
   * instead of once per registry.
   * <p>
   * The consequence to be aware of is that the TTL is process-wide, not per binder: in an embedded host that binds
   * this more than once, they all read the same memoized snapshot. That is correct rather than merely tolerable -
   * what they are all reading is {@code Profiler.INSTANCE}, itself a JVM-wide singleton, so a per-binder cache
   * would only buy duplicated work over the same numbers.
   */
  private static final SnapshotCache CACHE = new SnapshotCache();

  @Override
  public void bindTo(final MeterRegistry registry) {
    counter(registry, "arcadedb.engine.page.cache.hits", "Page cache hits", "pageCacheHits");
    counter(registry, "arcadedb.engine.page.cache.misses", "Page cache misses", "pageCacheMiss");
    counter(registry, "arcadedb.engine.pages.read", "Pages read from disk", "pagesRead");
    counter(registry, "arcadedb.engine.pages.written", "Pages written to disk", "pagesWritten");
    counter(registry, "arcadedb.engine.wal.bytes.written", "WAL bytes written", "walBytesWritten");
    counter(registry, "arcadedb.engine.mvcc.conflicts", "Concurrent modification exceptions",
        "concurrentModificationExceptions");
    // #5608: the commit-time page merges are what keeps the conflict counter above from exploding under contention
    // (super-node edge appends, concurrent writes to disjoint slots of one page). A collapse of the merge rate, or a
    // rise in the declines, is a throughput regression no correctness signal catches - so it has to be alertable.
    counter(registry, "arcadedb.engine.page.merges.edge.append", "Commit-time edge-append page merges",
        "edgeAppendMerges");
    counter(registry, "arcadedb.engine.page.merges.slot", "Commit-time disjoint-slot page merges", "txPageSlotMerges");
    counter(registry, "arcadedb.engine.page.merges.declined", "Page merges declined for lack of declared coverage",
        "mergesDeclinedByCoverage");
    counter(registry, "arcadedb.engine.tx.write", "Write transactions", "writeTx");
    counter(registry, "arcadedb.engine.tx.read", "Read transactions", "readTx");
    counter(registry, "arcadedb.engine.tx.rollbacks", "Transaction rollbacks", "txRollbacks");
    counter(registry, "arcadedb.engine.queries", "Queries executed", "queries");
    counter(registry, "arcadedb.engine.commands", "Commands executed", "commands");

    // Instantaneous readings: these go up AND down, so they are the only ones that stay gauges.
    gauge(registry, "arcadedb.engine.wal.files", "WAL files", "walTotalFiles");
    gauge(registry, "arcadedb.engine.files.open", "Open file descriptors", "totalOpenFiles");
    gauge(registry, "arcadedb.engine.databases", "Open databases", "totalDatabases");
  }

  /**
   * Registers a monotonic total. The caller is asserting the underlying value never decreases: the per-database
   * counters get that from {@code Profiler}'s retained baseline, the {@code PageManager} globals from simply never
   * being reset (a note at their declarations says so). Break either and the reset artifact this change removed
   * comes straight back.
   */
  private void counter(final MeterRegistry registry, final String name, final String description, final String jsonKey) {
    // No baseUnit(): Micrometer's Prometheus renderer splices the base unit INTO the series name
    // (arcadedb_engine_wal_bytes_written_bytes_total), which is a second, gratuitous rename on top of the _total
    // suffix this change already introduces.
    FunctionCounter.builder(name, CACHE, c -> c.read(jsonKey))
        .description(description)
        .register(registry);
  }

  private void gauge(final MeterRegistry registry, final String name, final String description, final String jsonKey) {
    Gauge.builder(name, CACHE, c -> c.read(jsonKey))
        .description(description)
        .register(registry);
  }

  /**
   * Memoizes {@link Profiler#toJSON()} for {@link #SNAPSHOT_TTL_NANOS} and unwraps the requested stat.
   */
  static final class SnapshotCache {
    /** The snapshot and the timestamp it was taken at, swapped atomically so a reader never sees a new
     * snapshot paired with a stale timestamp. */
    private record CachedSnapshot(JSONObject json, long atNanos) {
    }

    private final AtomicReference<CachedSnapshot> cache = new AtomicReference<>();

    double read(final String jsonKey) {
      final JSONObject json = snapshot();
      if (!json.has(jsonKey))
        return 0d;

      final Object value = json.get(jsonKey);
      if (value instanceof JSONObject nested) {
        // Profiler wraps each stat under exactly one of these inner keys.
        if (nested.has("count"))
          return nested.getLong("count", 0L);
        if (nested.has("space"))
          return nested.getLong("space", 0L);
        if (nested.has("value"))
          return nested.getLong("value", 0L);
        return 0d;
      }
      if (value instanceof Number n)
        return n.doubleValue();
      return 0d;
    }

    private JSONObject snapshot() {
      final long now = System.nanoTime();
      final CachedSnapshot current = cache.get();
      if (current == null || now - current.atNanos() > SNAPSHOT_TTL_NANOS) {
        // Last-writer-wins: two threads may both rebuild on the TTL boundary (Profiler.toJSON() is
        // synchronized, so concurrent rebuilds are safe), but snapshot and timestamp stay consistent.
        final JSONObject snap = Profiler.INSTANCE.toJSON();
        cache.set(new CachedSnapshot(snap, now));
        return snap;
      }
      return current.json();
    }
  }
}
