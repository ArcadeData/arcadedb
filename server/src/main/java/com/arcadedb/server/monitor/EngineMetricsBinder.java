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

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Exposes engine-wide statistics from {@link Profiler} as Micrometer gauges so they appear in
 * {@code /prometheus} and any other registered registry (e.g. OTLP). Each gauge re-reads a cached
 * {@link Profiler} snapshot on scrape, so values track the running engine without keeping extra state.
 * <p>
 * The {@code Profiler} aggregates JVM-global counters ({@link com.arcadedb.engine.PageManager} cache
 * and WAL totals) plus the sum of per-database counters. Its JSON wraps every stat under an inner
 * {@code count} / {@code space} / {@code value} key (only {@code walTotalFiles} is a bare scalar), so
 * the reader extracts the inner numeric rather than treating the wrapper object as a number.
 * <p>
 * Building the snapshot is comparatively expensive (it queries JMX, free disk space and iterates the
 * open databases), and on a scrape all gauges fire near-simultaneously; the snapshot is therefore
 * memoized for a short TTL so one scrape rebuilds it at most once instead of once per gauge.
 * <p>
 * Per-database tagging is intentionally not done here: the binder is bound at server startup before
 * databases are loaded, and the page-cache / WAL counters are JVM-global singletons. A
 * per-database-tagged breakdown would require a periodically refreshed {@code MultiGauge}.
 */
public final class EngineMetricsBinder implements MeterBinder {

  private static final long SNAPSHOT_TTL_NANOS = TimeUnit.SECONDS.toNanos(1);

  /** The snapshot and the timestamp it was taken at, swapped atomically so a reader never sees a new
   * snapshot paired with a stale timestamp. */
  private record CachedSnapshot(JSONObject json, long atNanos) {
  }

  private final AtomicReference<CachedSnapshot> cache = new AtomicReference<>();

  @Override
  public void bindTo(final MeterRegistry registry) {
    gauge(registry, "arcadedb.engine.page.cache.hits", "Page cache hits", "pageCacheHits");
    gauge(registry, "arcadedb.engine.page.cache.misses", "Page cache misses", "pageCacheMiss");
    gauge(registry, "arcadedb.engine.pages.read", "Pages read from disk", "pagesRead");
    gauge(registry, "arcadedb.engine.pages.written", "Pages written to disk", "pagesWritten");
    gauge(registry, "arcadedb.engine.wal.bytes.written", "WAL bytes written", "walBytesWritten");
    gauge(registry, "arcadedb.engine.wal.files", "WAL files", "walTotalFiles");
    gauge(registry, "arcadedb.engine.mvcc.conflicts", "Concurrent modification exceptions", "concurrentModificationExceptions");
    gauge(registry, "arcadedb.engine.files.open", "Open file descriptors", "totalOpenFiles");
    gauge(registry, "arcadedb.engine.tx.write", "Write transactions", "writeTx");
    gauge(registry, "arcadedb.engine.tx.read", "Read transactions", "readTx");
    gauge(registry, "arcadedb.engine.tx.rollbacks", "Transaction rollbacks", "txRollbacks");
    gauge(registry, "arcadedb.engine.queries", "Queries executed", "queries");
    gauge(registry, "arcadedb.engine.commands", "Commands executed", "commands");
    gauge(registry, "arcadedb.engine.databases", "Open databases", "totalDatabases");
  }

  private void gauge(final MeterRegistry registry, final String name, final String description, final String jsonKey) {
    // Use the Supplier form (not the weak-referenced (obj, fn) form): the lambda strongly captures
    // this binder so it is not garbage-collected after bindTo() returns, which would silently kill
    // the gauges. This mirrors PoolMetrics, whose suppliers capture long-lived singletons.
    Gauge.builder(name, () -> readMetric(jsonKey))
        .description(description)
        .register(registry);
  }

  private double readMetric(final String jsonKey) {
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
