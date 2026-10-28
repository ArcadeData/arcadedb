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

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.engine.block.BlockCache;
import com.arcadedb.index.Index;
import com.arcadedb.index.columnar.ColumnarIndexManager;
import com.arcadedb.index.columnar.ColumnarStats;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.Schema;
import com.arcadedb.server.ArcadeDBServer;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.FunctionTimer;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.ToDoubleFunction;
import java.util.function.ToLongFunction;
import java.util.logging.Level;

/**
 * Micrometer binding for the columnar projections ({@code arcadedb.columnar.*}), tagged {@code database} plus {@code type},
 * {@code property} or {@code reason} where the meter has them. The values live in each database's {@link ColumnarStats}; this binder
 * only registers function meters over them, so nothing is registered per event.
 * <p>
 * Every series a database can ever report exists from the first refresh: the type and property of each COLUMNAR index give the
 * expected series (with all rebuild reasons), and any other cell the stats hold is added too (a subtype of a hierarchy index).
 * A dashboard can alert on {@code increase()} without waiting for the first event to create the series.
 * <p>
 * Function meters hold their observed object weakly, so each database's {@code ColumnarStats} is kept strongly here until the
 * database goes away. A refresh every {@value #REFRESH_SECONDS} s (one daemon thread, as {@link HAReplicationMetrics}) registers the
 * series of new databases, replaces those of a database whose stats object changed (it was reopened) and removes those of a closed
 * one. The block cache is shared by every database, so its meters carry {@code database="*"}. Like {@link HAReplicationMetrics}, the
 * meters are keyed by database name on the JVM-wide registry: several servers in one JVM share the ids, and the first one to close
 * removes them.
 */
public final class ColumnarMetrics implements AutoCloseable {
  static final String SHARED_DATABASE = "*";

  private static final long   REFRESH_SECONDS = 5;
  private static final String PREFIX          = "arcadedb.columnar.";

  /** The rebuild reasons, all registered up front so each series exists before its first rebuild. */
  private static final String[] REBUILD_REASONS = { "create", "snapshot_install", "missing", "corrupt", "unclean_no_wal", "schema",
      "check_fix" };

  private static final String[] TYPE_KINDS = { "apply.pages", "apply.placeholders.resolved", "flush.count", "merge.count",
      "apply.records.reread", "apply.unreadable", "rebuild.pages.reused", "flush.duration", "merge.duration", "apply.lag.ms",
      "overlay.rows", "segments", "segments.dead.ratio", "files.open" };

  private static final String[] PROPERTY_KINDS = { "dictionary.compactions", "dictionary.ordinals.reclaimed", "conversion.failures",
      "dictionary.size", "cardinality.exceeded" };

  /** The meters of one database and the stats object they read. */
  private static final class Bound {
    final ColumnarStats        stats;
    /** Held strongly with the stats: the scrape-time gauges read through it, and a function meter keeps its object only weakly. */
    final ColumnarIndexManager manager;
    final Set<String>          keys = new HashSet<>();
    final List<Meter.Id>       ids  = new ArrayList<>();

    Bound(final ColumnarIndexManager manager) {
      this.manager = manager;
      this.stats = manager.getStats();
    }
  }

  private final ArcadeDBServer         server;
  private final MeterRegistry         registry = Metrics.globalRegistry;
  private final Map<String, Bound>    bound    = new HashMap<>();
  private final List<Meter.Id>        shared   = new ArrayList<>();
  private final ScheduledExecutorService scheduler;
  private boolean                    closed;

  public ColumnarMetrics(final ArcadeDBServer server) {
    this.server = server;
    bindBlockCache();
    refreshGuarded();

    scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
      final Thread thread = new Thread(r, "ArcadeDB-ColumnarMetrics");
      thread.setDaemon(true);
      return thread;
    });
    scheduler.scheduleWithFixedDelay(this::refreshGuarded, REFRESH_SECONDS, REFRESH_SECONDS, TimeUnit.SECONDS);
  }

  /** The block cache is JVM-wide, shared by every database: the one set of columnar meters without a real database name. */
  private void bindBlockCache() {
    final BlockCache cache = BlockCache.instance();
    final Tags tags = Tags.of("database", SHARED_DATABASE);
    shared.add(FunctionCounter.builder(PREFIX + "cache.hits", cache, BlockCache::hits)
        .description("Block cache hits, shared by every database").tags(tags).register(registry).getId());
    shared.add(FunctionCounter.builder(PREFIX + "cache.misses", cache, BlockCache::misses)
        .description("Block cache misses, shared by every database").tags(tags).register(registry).getId());
    shared.add(Gauge.builder(PREFIX + "cache.used.bytes", cache, BlockCache::usedBytes)
        .description("Bytes held by the block cache, shared by every database").baseUnit("bytes").tags(tags).register(registry).getId());
  }

  @Override
  public synchronized void close() {
    closed = true;
    scheduler.shutdownNow();
    for (final Bound b : bound.values())
      removeAll(b);
    bound.clear();
    for (final Meter.Id id : shared)
      registry.remove(id);
    shared.clear();
  }

  private synchronized void refresh() {
    // a refresh queued behind close() must not bind again what close() removed
    if (closed)
      return;
    final Set<String> open = new HashSet<>();
    for (final String name : server.getDatabaseNames()) {
      final LocalDatabase local = localOf(name);
      final ColumnarIndexManager manager = local != null && local.isOpen() ? local.getColumnarIndexManager() : null;
      // final review C-M2: a database that never had a COLUMNAR index gets no series (and no walk of its indexes every refresh)
      if (manager == null || !manager.hasColumnarState())
        continue;
      open.add(name);
      Bound current = bound.get(name);
      if (current != null && current.manager != manager) {
        // the database was reopened: new stats object, the meters over the old one are gone with it
        removeAll(current);
        current = null;
      }
      if (current == null) {
        current = new Bound(manager);
        bound.put(name, current);
      }
      bindNewSeries(name, current, local);
    }
    bound.entrySet().removeIf(e -> {
      if (open.contains(e.getKey()))
        return false;
      removeAll(e.getValue());
      return true;
    });
  }

  private void refreshGuarded() {
    // a task that throws is cancelled for good by the scheduler and nothing says so: this must never throw
    try {
      refresh();
    } catch (final Throwable t) {
      LogManager.instance().log(this, Level.FINE, "Failed to refresh the columnar metrics: %s", t.getMessage());
    }
  }

  private LocalDatabase localOf(final String name) {
    try {
      // never opens: a database that is closed but still registered (shutdown, a close command, a snapshot swap) stays closed
      final DatabaseInternal embedded = server.getDatabase(name, false, false).getEmbedded();
      return embedded instanceof LocalDatabase local ? local : null;
    } catch (final Exception e) {
      return null;
    }
  }

  private void removeAll(final Bound b) {
    for (final Meter.Id id : b.ids)
      registry.remove(id);
    b.ids.clear();
    b.keys.clear();
  }

  private void bindNewSeries(final String database, final Bound b, final LocalDatabase local) {
    b.stats.forEachKey((kind, type, property, reason) -> bind(database, b, kind, type, property, reason));
    for (final Index index : local.getSchema().getIndexes()) {
      if (index.getType() != Schema.INDEX_TYPE.COLUMNAR || index.getTypeName() == null || index.getPropertyNames().isEmpty())
        continue;
      final String type = index.getTypeName();
      for (final String kind : TYPE_KINDS)
        bind(database, b, kind, type, null, null);
      // final review C-I1: the column's property, the key of every reader ("tags" for "tags by item")
      final String property = Index.basePropertyName(index.getPropertyNames().getFirst());
      for (final String kind : PROPERTY_KINDS)
        bind(database, b, kind, type, property, null);
      for (final String reason : REBUILD_REASONS)
        bind(database, b, "rebuilds", type, null, reason);
    }
    bind(database, b, "recovery.pages", null, null, null);
  }

  private void bind(final String database, final Bound b, final String kind, final String type, final String property,
      final String reason) {
    if (!b.keys.add(kind + '|' + type + '|' + property + '|' + reason))
      return;
    Tags tags = Tags.of("database", database);
    if (type != null)
      tags = tags.and("type", type);
    if (property != null)
      tags = tags.and("property", property);
    if (reason != null)
      tags = tags.and("reason", reason);
    final String name = PREFIX + kind;
    switch (kind) {
    case "rebuilds" ->
        counter(b, name, tags, "Projection rebuilds by reason; a normal open and crash recovery never count", s -> s.rebuilds(type, reason));
    case "conversion.failures" ->
        counter(b, name, tags, "Values the column kind cannot hold, counted as missing", s -> s.conversionFailures(type, property));
    case "apply.pages" -> counter(b, name, tags, "Pages applied to the projection", s -> s.applyPagesCount(type));
    case "apply.placeholders.resolved" ->
        counter(b, name, tags, "Placeholder records resolved by apply passes", s -> s.placeholdersResolvedCount(type));
    case "flush.count" -> counter(b, name, tags, "Overlay flushes to segments", s -> s.flushCount(type));
    case "merge.count" -> counter(b, name, tags, "Segment merges", s -> s.mergeCount(type));
    case "recovery.pages" -> counter(b, name, tags, "Pages taken back from the recovery set at open", ColumnarStats::recoveryPagesCount);
    case "apply.records.reread" ->
        counter(b, name, tags, "Slots re-read by apply passes; over apply.pages it is the re-read amplification",
            s -> s.recordsRereadCount(type));
    case "apply.unreadable" -> counter(b, name, tags, "Pages or records an apply pass could not read", s -> s.unreadableCount(type));
    case "rebuild.pages.reused" ->
        counter(b, name, tags, "Pages a rebuild reused instead of re-reading", s -> s.rebuildPagesReusedCount(type));
    case "dictionary.compactions" ->
        counter(b, name, tags, "Dictionary compactions", s -> s.dictionaryCompactions(type, property));
    case "dictionary.ordinals.reclaimed" ->
        counter(b, name, tags, "Ordinals reclaimed by dictionary compactions", s -> s.dictionaryOrdinalsReclaimed(type, property));
    case "flush.duration" -> timer(b, name, tags, "Duration of overlay flushes", s -> s.flushDurationCount(type),
        s -> s.flushDurationTotalNanos(type));
    case "merge.duration" -> timer(b, name, tags, "Duration of segment merges", s -> s.mergeDurationCount(type),
        s -> s.mergeDurationTotalNanos(type));
    case "apply.lag.ms" ->
        // no base unit: the name carries it (as arcadedb.engine.snapshot.window.age.ms), Prometheus would append it a second time
        managerGauge(b, name, tags, "Age in milliseconds of the oldest committed page not yet applied, the worst bucket of the type",
            null, m -> m.applyLagMs(type));
    case "overlay.rows" -> gauge(b, name, tags, "Rows in the in-memory overlay", null, s -> s.overlayRows(type));
    case "segments" -> gauge(b, name, tags, "Segments of the projection", null, s -> s.segments(type));
    case "segments.dead.ratio" ->
        managerGauge(b, name, tags, "Dead rows over rows of the segments of every bucket of the type", null, m -> m.deadRatio(type));
    case "dictionary.size" ->
        managerGauge(b, name, tags, "Values of the largest bucket dictionary of the property", null, m -> m.dictionarySize(type, property));
    case "cardinality.exceeded" ->
        managerGauge(b, name, tags, "1 when the dictionary of any bucket passed the cardinality limit of the property "
            + "(the lowest limit when several indexes cover it)", null, m -> m.cardinalityExceeded(type, property) ? 1 : 0);
    case "files.open" -> gauge(b, name, tags, "Open files of the projection", null, s -> s.openFiles(type));
    default -> throw new IllegalArgumentException("Unknown columnar stat " + kind);
    }
  }

  private void counter(final Bound b, final String name, final Tags tags, final String description,
      final ToDoubleFunction<ColumnarStats> value) {
    b.ids.add(FunctionCounter.builder(name, b.stats, value).description(description).tags(tags).register(registry).getId());
  }

  private void timer(final Bound b, final String name, final Tags tags, final String description,
      final ToLongFunction<ColumnarStats> count, final ToDoubleFunction<ColumnarStats> totalNanos) {
    b.ids.add(FunctionTimer.builder(name, b.stats, count, totalNanos, TimeUnit.NANOSECONDS).description(description).tags(tags)
        .register(registry).getId());
  }

  private void managerGauge(final Bound b, final String name, final Tags tags, final String description, final String baseUnit,
      final ToDoubleFunction<ColumnarIndexManager> value) {
    final Gauge.Builder<ColumnarIndexManager> builder = Gauge.builder(name, b.manager, value).description(description).tags(tags);
    if (baseUnit != null)
      builder.baseUnit(baseUnit);
    b.ids.add(builder.register(registry).getId());
  }

  private void gauge(final Bound b, final String name, final Tags tags, final String description, final String baseUnit,
      final ToDoubleFunction<ColumnarStats> value) {
    final Gauge.Builder<ColumnarStats> builder = Gauge.builder(name, b.stats, value).description(description).tags(tags);
    if (baseUnit != null)
      builder.baseUnit(baseUnit);
    b.ids.add(builder.register(registry).getId());
  }
}
