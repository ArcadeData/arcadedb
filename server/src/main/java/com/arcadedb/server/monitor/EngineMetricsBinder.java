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

  /** The engine times the t0 barrier in milliseconds; a {@code _seconds} series is what Prometheus expects (#6125). */
  private static final double MILLIS_TO_SECONDS = 0.001d;

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
    // #6217: the read-path twin of the merges above. A record too big for its page is read by walking its chunk
    // chain, and the chunks of unrelated records share pages: a revalidation is a read that met one of those writes
    // and completed anyway, a retry is a read thrown away because the record itself had moved. Retries climbing
    // while revalidations stay flat is real contention on one record; the opposite is the mechanism working.
    counter(registry, "arcadedb.engine.record.chunked.read.revalidations",
        "Chunked reads that completed after a page they walked had moved", "chunkChainReadRevalidations");
    counter(registry, "arcadedb.engine.record.chunked.read.retries",
        "Chunked reads restarted because the record changed under them", "chunkChainReadRetries");
    // #6526: the cost side of #6511's fix. A durability-flag change no longer tears the async pool down, it closes
    // the affected worker's open batch transaction instead - so this climbing while async throughput falls is two
    // callers flipping setTransactionUseWAL()/setTransactionSync() against each other on one database, and the
    // batching collapsing toward one commit per task.
    counter(registry, "arcadedb.engine.async.boundary.commits",
        "Async batch transactions closed early by a durability-flag change", "asyncForcedBoundaryCommits");
    counter(registry, "arcadedb.engine.tx.write", "Write transactions", "writeTx");
    counter(registry, "arcadedb.engine.tx.read", "Read transactions", "readTx");
    counter(registry, "arcadedb.engine.tx.rollbacks", "Transaction rollbacks", "txRollbacks");
    counter(registry, "arcadedb.engine.queries", "Queries executed", "queries");
    counter(registry, "arcadedb.engine.commands", "Commands executed", "commands");

    // #6116: the copy-on-write work an open snapshot window (#6075) is doing, and how often a window loses its point
    // in time. An invalidated window is invisible from the outside - its consumer restarts on the suspend-and-freeze
    // path and still completes - so without this counter the only trace of a backup that fell back to throttling the
    // writers is a WARNING in the log.
    counter(registry, "arcadedb.engine.snapshot.windows.opened", "Point-in-time snapshot windows opened",
        "snapshotWindowsOpened");
    counter(registry, "arcadedb.engine.snapshot.windows.invalidated",
        "Snapshot windows that lost their point in time (shadow cap breach or I/O error)", "snapshotWindowsInvalidated");
    counter(registry, "arcadedb.engine.snapshot.preimages.captured",
        "Page pre-images copied into a snapshot shadow", "snapshotPreImagesCaptured");

    // #6125: the split of windows.invalidated above. The two reasons take an operator to different places - a cap
    // breach is answered by raising arcadedb.pageSnapshotMaxSize or giving the spill volume room, a capture failure
    // by looking at the disk - and a single summed counter cannot say which.
    counter(registry, "arcadedb.engine.snapshot.windows.overflowed",
        "Snapshot windows whose shadow reached its size cap", "snapshotWindowsOverflowed");
    counter(registry, "arcadedb.engine.snapshot.windows.failed",
        "Snapshot windows lost to an I/O error while capturing a pre-image", "snapshotWindowsFailed");

    // #6125: the t0 barrier, the ONE stall the snapshot path still has, exported as a Prometheus timer is: a count
    // and a summed duration, so rate(seconds)/rate(count) is the average latency. It is a pair of counters rather
    // than a real Timer because this binder reads scalars out of a memoized Profiler snapshot and never sees the
    // individual events; the pair carries the same information a Timer's _sum and _count do.
    counter(registry, "arcadedb.engine.snapshot.barrier.count", "Point-in-time snapshot t0 barriers executed",
        "snapshotBarriers");
    counter(registry, "arcadedb.engine.snapshot.barrier.seconds",
        "Total time spent in the snapshot t0 barrier", "snapshotBarrierTime", MILLIS_TO_SECONDS);
    counter(registry, "arcadedb.engine.snapshot.barrier.inexact",
        "Barriers that could not prove the flush pipeline was empty at t0", "snapshotBarriersInexact");
    // A HIGH-WATER MARK: MONOTONIC, BUT A rate() OVER IT WOULD BE MEANINGLESS, WHICH IS WHY IT IS THE ONE MONOTONIC
    // READING HERE THAT STAYS A GAUGE
    gauge(registry, "arcadedb.engine.snapshot.barrier.max.seconds",
        "Longest snapshot t0 barrier observed", "snapshotBarrierMaxTime", MILLIS_TO_SECONDS);

    // Instantaneous readings: these go up AND down, so they are the only ones that stay gauges.
    gauge(registry, "arcadedb.engine.wal.files", "WAL files", "walTotalFiles");
    gauge(registry, "arcadedb.engine.files.open", "Open file descriptors", "totalOpenFiles");
    gauge(registry, "arcadedb.engine.databases", "Open databases", "totalDatabases");

    // #6116: per-window state, so every one of these returns to zero when the last window closes - gauges, not
    // counters (#5636). The usage percentage is the alertable one: a window that reaches 100% of
    // arcadedb.pageSnapshotMaxSize is invalidated, and its backup restarts on the path that throttles writers.
    gauge(registry, "arcadedb.engine.snapshot.windows.open", "Point-in-time snapshot windows currently open",
        "snapshotWindowsOpen");
    gauge(registry, "arcadedb.engine.snapshot.shadow.pages", "Pages held in the open snapshot shadows",
        "snapshotShadowedPages");
    gauge(registry, "arcadedb.engine.snapshot.shadow.bytes", "Bytes held in the open snapshot shadows, RAM plus spill",
        "snapshotShadowSize");
    gauge(registry, "arcadedb.engine.snapshot.shadow.spilled.bytes", "Snapshot shadow bytes spilled to disk",
        "snapshotShadowSpilledSize");
    gauge(registry, "arcadedb.engine.snapshot.shadow.usage.percent",
        "Fullest open shadow as a percentage of arcadedb.pageSnapshotMaxSize", "snapshotShadowUsagePerc");
    gauge(registry, "arcadedb.engine.snapshot.window.age.ms", "Age of the oldest open snapshot window",
        "snapshotOldestWindowAge");

    // #6087: the companion reading for the OTHER path. While a reader freezes the files with a flush suspension,
    // dirty pages pile up here; crossing arcadedb.flushSuspendMaxDeferredRAM throttles the committing threads of the
    // SUSPENDED databases (since #6200 - before it the flush thread stopped draining its queue altogether, so the
    // committers of every OPEN database were throttled with them). This reading stays JVM-wide because the cap is,
    // and the per-database split that item 2 of #6087 wants to tag it by now exists as
    // PageManager.getDeferredRAMBytesOf.
    gauge(registry, "arcadedb.engine.flush.deferred.bytes", "Dirty page bytes deferred by a flush suspension",
        "deferredRAM");
  }

  /**
   * Registers a monotonic total. The caller is asserting the underlying value never decreases: the per-database
   * counters get that from {@code Profiler}'s retained baseline, the {@code PageManager} globals from simply never
   * being reset (a note at their declarations says so). Break either and the reset artifact this change removed
   * comes straight back.
   */
  private void counter(final MeterRegistry registry, final String name, final String description, final String jsonKey) {
    counter(registry, name, description, jsonKey, 1d);
  }

  /**
   * @param scale factor applied to the raw stat, for the readings the engine keeps in a different unit from the one
   *     the series name promises (#6125: the barrier durations are milliseconds in {@code Profiler} and seconds on
   *     the wire, which is what Prometheus dashboards and alert rules expect of a {@code _seconds} series).
   */
  private void counter(final MeterRegistry registry, final String name, final String description, final String jsonKey,
      final double scale) {
    // No baseUnit(): Micrometer's Prometheus renderer splices the base unit INTO the series name
    // (arcadedb_engine_wal_bytes_written_bytes_total), which is a second, gratuitous rename on top of the _total
    // suffix this change already introduces.
    FunctionCounter.builder(name, CACHE, c -> c.read(jsonKey) * scale)
        .description(description)
        .register(registry);
  }

  private void gauge(final MeterRegistry registry, final String name, final String description, final String jsonKey) {
    gauge(registry, name, description, jsonKey, 1d);
  }

  private void gauge(final MeterRegistry registry, final String name, final String description, final String jsonKey,
      final double scale) {
    Gauge.builder(name, CACHE, c -> c.read(jsonKey) * scale)
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
        // A percentage is the one stat shape that is genuinely fractional, so it is read as a double: rounding it to
        // a long would flatten every reading below 1% to zero, and "the shadow is at 0%" is exactly the wrong thing
        // to tell an operator watching it fill (#6116).
        if (nested.has("perc"))
          return nested.getDouble("perc", 0d);
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
