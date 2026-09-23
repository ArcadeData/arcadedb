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

import com.arcadedb.engine.timeseries.AggregationMetrics;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * The sink for {@link AggregationMetrics}, the counters a TimeSeries read fills in as it decides what to read
 * (issue #7717).
 * <p>
 * Every engine read path has taken an optional {@link AggregationMetrics} for a long time, and until this class
 * existed nothing on a wire protocol ever passed one: the only production consumer was the SQL aggregation step,
 * so the numbers that say whether the push-downs are working - blocks skipped on their declared statistics,
 * blocks that had to be decompressed, mutable pages discarded on their header - existed for tests to assert on
 * and for nobody to see. Whether a given tenant's blocks are being skipped, or every one of them is falling back
 * because the tag column is not a {@code STRING} or because the blocks predate the tag-metadata section, was not
 * observable at all.
 * <p>
 * Micrometer rather than a {@code ?debug=true} block in the response, because the question an operator asks is a
 * RATIO OVER TIME - "what fraction of blocks is this dashboard decompressing this week" - and a per-request
 * field cannot be aggregated. It reaches whatever the server already publishes to (Prometheus, OTLP), alongside
 * {@code arcadedb.http.requests} and {@code arcadedb.query.duration}.
 * <p>
 * <b>Off costs nothing.</b> {@link #start()} answers {@code null} while the server's metrics subsystem is not
 * running, {@code null} is what every engine read path already accepts for "do not count", and
 * {@link #publish(AggregationMetrics, String, String, String)} ignores it. So a server started with
 * {@code arcadedb.server.metrics=false} allocates nothing per request and takes none of the {@code nanoTime}
 * calls the counters are gated on.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class TimeSeriesReadMetrics {

  /** The {@code surface} tag: which endpoint made the read. A small, fixed set of constants. */
  public static final String SURFACE_TS_QUERY          = "ts-query";
  public static final String SURFACE_TS_LATEST         = "ts-latest";
  public static final String SURFACE_GRAFANA           = "grafana";
  public static final String SURFACE_PROM_LABEL_VALUES = "prom-label-values";
  public static final String SURFACE_PROM_SERIES       = "prom-series";
  public static final String SURFACE_PROM_QUERY        = "prom-query";
  public static final String SURFACE_PROM_QUERY_RANGE  = "prom-query-range";
  public static final String SURFACE_GRPC              = "grpc";

  /**
   * The {@code type} tag for a read whose types are not one: a PromQL expression resolves its own selectors,
   * and one expression can span several metrics, so the handler that publishes has no single type to name.
   * A constant rather than a list, because the tag has to stay low-cardinality - and rather than the
   * expression text, which is caller-controlled and would be an unbounded-tag leak of the #5025 kind.
   */
  public static final String TYPE_EXPRESSION           = "(expression)";

  /**
   * Every value the {@code surface} tag can carry. Enumerated rather than left implicit because it is what
   * bounds the cache: the collapse keeps the surface and replaces only the db and type halves, so the overflow
   * tuples are one per surface and the ceiling test has to know how many that is. Package-private so the test
   * fills the cache across all of them rather than across one.
   */
  static final String[] SURFACES = { SURFACE_TS_QUERY, SURFACE_TS_LATEST, SURFACE_GRAFANA,
      SURFACE_PROM_LABEL_VALUES, SURFACE_PROM_SERIES, SURFACE_PROM_QUERY, SURFACE_PROM_QUERY_RANGE,
      SURFACE_GRPC };

  private static final String BLOCKS_METER   = "arcadedb.timeseries.read.blocks";
  private static final String PAGES_METER    = "arcadedb.timeseries.read.pages";
  private static final String ROWS_METER     = "arcadedb.timeseries.read.rows";
  private static final String OVERFLOW_METER = "arcadedb.timeseries.read.overflow.buckets";
  private static final String PHASE_METER    = "arcadedb.timeseries.read.phase";

  // The value substituted for the db and type halves of the tag tuple once the meter cache is full. Surface is
  // a constant from the list above and outcome/phase are fixed, so those two are the only halves that can grow
  // - a database or a TimeSeries type can be created and dropped in a loop - and they are what collapses.
  private static final String OVERFLOW_TAG = "other";
  // Ceiling on cached tuples. A MeterFilter can deny a meter but cannot stop computeIfAbsent from retaining the
  // key, so the cache needs a bound of its own (the leak of issues #5025/#6805, on these meters). Sized to give
  // a server with the maximum admissible number of databases several TimeSeries types each on every surface
  // before anything collapses: the collapse is a backstop, not a routine operating mode. Soft, like the sibling
  // caches: the size test and the computeIfAbsent are not one atomic step.
  static final         int    MAX_METER_SETS = 10_000;
  // Cache entries the collapse itself can add, above MAX_METER_SETS: the overflow tuple keeps the surface - it
  // is the half worth keeping, being what distinguishes a slow dashboard from a slow label picker - so there is
  // one "other|other|<surface>" entry per surface, not one in total. Named for the same reason
  // MicrometerQueryMetricsRecorder names its RESERVED_* counts: the ceiling and what sits above it are one
  // number, read by the guard test rather than restated in it.
  static final         int    RESERVED_OVERFLOW_METER_SETS = SURFACES.length;

  private static final ConcurrentHashMap<String, MeterSet> METER_SETS = new ConcurrentHashMap<>();

  // Read on every TimeSeries request, written twice in the life of a server. Volatile rather than synchronized
  // for exactly that ratio.
  private static volatile boolean enabled;

  private TimeSeriesReadMetrics() {
  }

  /**
   * An {@link AggregationMetrics} to hand to the engine, or {@code null} when nothing would read it.
   * <p>
   * The engine gates every counter on a {@code != null} test, so {@code null} is not merely accepted: it is the
   * path on which no {@code System.nanoTime()} is called and no per-shard instance is allocated.
   */
  public static AggregationMetrics start() {
    return enabled ? new AggregationMetrics() : null;
  }

  /**
   * Publishes what a finished read counted. A {@code null} {@code metrics} - the metrics-disabled path, or a
   * caller that chose not to count - is ignored.
   *
   * @param database the database the read ran against
   * @param type     the TimeSeries type read
   * @param surface  one of the {@code SURFACE_*} constants
   */
  public static void publish(final AggregationMetrics metrics, final String database, final String type,
      final String surface) {
    if (metrics == null || !enabled)
      return;

    final MeterSet meters = meterSet(database, type, surface);

    meters.skippedBlocks.increment(metrics.getSkippedBlocks());
    meters.vanishedBlocks.increment(metrics.getVanishedBlocks());
    meters.fastPathBlocks.increment(metrics.getFastPathBlocks());
    meters.slowPathBlocks.increment(metrics.getSlowPathBlocks());
    meters.scannedPages.increment(metrics.getScannedPages());
    meters.skippedPages.increment(metrics.getSkippedPages());
    meters.materializedRows.increment(metrics.getMaterializedRows());
    meters.overflowBuckets.increment(metrics.getOverflowBuckets());

    // Recorded as a Timer per phase rather than as a counter of nanoseconds, so a dashboard reads a rate and a
    // distribution rather than a monotonically growing number it has to differentiate itself. A phase that
    // contributed nothing is not recorded at all: a zero sample would pull the phase's mean toward zero and
    // claim the read did that work instantly, when it did not do it.
    recordIfPositive(meters.ioTime, metrics.getIoNanos());
    recordIfPositive(meters.decompressTsTime, metrics.getDecompTsNanos());
    recordIfPositive(meters.decompressValTime, metrics.getDecompValNanos());
    recordIfPositive(meters.accumulateTime, metrics.getAccumNanos());
  }

  private static void recordIfPositive(final Timer timer, final long nanos) {
    if (nanos > 0)
      timer.record(nanos, TimeUnit.NANOSECONDS);
  }

  /**
   * Turns publishing on or off. Called by {@code ArcadeDBServer} when it installs and dismantles the metrics
   * subsystem; disabling also drops the cache, because a cached meter is bound to the registries that backed
   * it when it was built and recording into one whose registry is gone discards the sample silently.
   * <p>
   * Disabling stops NEW resolutions, not a request already holding a resolved {@link MeterSet}: the cache is
   * cleared but the meters themselves are not deregistered, so a read in flight when the server dismantles its
   * metrics can still record into the old registry. That is tolerated rather than prevented - the alternative
   * is a read barrier on a path whose whole point is that it is cheap - and it is harmless, because the
   * registry being torn down is discarded with the sample. The sibling recorders make the same trade.
   */
  public static void setEnabled(final boolean value) {
    enabled = value;
    if (!value)
      METER_SETS.clear();
  }

  /** Whether reads are being counted. Package-private for the guard test. */
  static boolean isEnabled() {
    return enabled;
  }

  /** Number of tuples currently cached. Package-private for direct unit testing of the ceiling. */
  static int cachedMeterSetCount() {
    return METER_SETS.size();
  }

  /**
   * Drops the resolved meters without touching the enabled flag. Package-private: the ceiling test fills the
   * cache on purpose, and a full cache collapses every later tuple onto the overflow tag, so a test that leaves
   * it full would silently decide what its neighbours measure.
   */
  static void clearMeterCache() {
    METER_SETS.clear();
  }

  /**
   * Resolves (and caches) the meters for one {@code db|type|surface} tuple. An already-seen tuple costs one
   * concatenation and one hash lookup; only a miss pays for the bound, and it pays before anything is retained.
   * <p>
   * Recurses at most once, because the collapsed tuple is itself cacheable. Note the collapse keeps the
   * SURFACE, so the cache settles at {@link #MAX_METER_SETS} plus {@link #RESERVED_OVERFLOW_METER_SETS} rather
   * than plus one: bounded either way, and the extra entries are worth their cost because "which endpoint" is
   * the half of the tuple an operator still needs once the tenant half has been given up.
   */
  private static MeterSet meterSet(final String database, final String type, final String surface) {
    final String key = database + '|' + type + '|' + surface;
    final MeterSet cached = METER_SETS.get(key);
    if (cached != null)
      return cached;

    // The recursion guard tests the WHOLE collapsed tuple, not just its db half. "other" is a legal database
    // name, so a guard reading only the db would have treated a real database called "other" as though it were
    // already collapsed, skipped the ceiling for it entirely, and let its types grow the cache without bound -
    // the very leak the ceiling exists to stop (issues #5025, #6805). Testing both halves cannot be
    // fooled that way: a database named "other" holding a type named anything else still collapses, and the one
    // tuple that reads as already-collapsed is the one that genuinely is.
    if (METER_SETS.size() >= MAX_METER_SETS && !(OVERFLOW_TAG.equals(database) && OVERFLOW_TAG.equals(type)))
      return meterSet(OVERFLOW_TAG, OVERFLOW_TAG, surface);

    return METER_SETS.computeIfAbsent(key, k -> new MeterSet(database, type, surface));
  }

  /**
   * The meters of one {@code db|type|surface} tuple, resolved once and reused. Building them per request would
   * put a {@code Counter.Builder}/{@code Tags}/{@code Meter.Id} allocation and twelve registry lookups on a
   * path whose whole point is that it is cheap when the push-downs work.
   */
  private static final class MeterSet {
    final Counter skippedBlocks;
    final Counter vanishedBlocks;
    final Counter fastPathBlocks;
    final Counter slowPathBlocks;
    final Counter scannedPages;
    final Counter skippedPages;
    final Counter materializedRows;
    final Counter overflowBuckets;
    final Timer   ioTime;
    final Timer   decompressTsTime;
    final Timer   decompressValTime;
    final Timer   accumulateTime;

    MeterSet(final String database, final String type, final String surface) {
      skippedBlocks = blockCounter(database, type, surface, "skipped",
          "Sealed blocks discarded on their declared range, tag values or statistics, without being read");
      vanishedBlocks = blockCounter(database, type, surface, "vanished",
          "Sealed blocks a scan held a directory entry for and could no longer find, so its answer is short");
      fastPathBlocks = blockCounter(database, type, surface, "fast-path",
          "Sealed blocks answered from their declared statistics, without being decompressed");
      slowPathBlocks = blockCounter(database, type, surface, "slow-path",
          "Sealed blocks that had to be read and decompressed");

      scannedPages = pageCounter(database, type, surface, "scanned",
          "Mutable-bucket data pages whose rows were examined");
      skippedPages = pageCounter(database, type, surface, "skipped",
          "Mutable-bucket data pages discarded on their min/max timestamp header alone");

      materializedRows = Counter.builder(ROWS_METER)
          .description("Rows turned into objects, i.e. the ones that survived every push-down")
          .tag("db", database).tag("type", type).tag("surface", surface)
          .register(Metrics.globalRegistry);

      overflowBuckets = Counter.builder(OVERFLOW_METER)
          .description("Aggregation buckets parked outside the pre-allocated flat window: correct, but the "
              + "range estimate came up short")
          .tag("db", database).tag("type", type).tag("surface", surface)
          .register(Metrics.globalRegistry);

      ioTime = phaseTimer(database, type, surface, "io");
      decompressTsTime = phaseTimer(database, type, surface, "decompress-timestamps");
      decompressValTime = phaseTimer(database, type, surface, "decompress-values");
      accumulateTime = phaseTimer(database, type, surface, "accumulate");
    }

    private static Counter blockCounter(final String database, final String type, final String surface,
        final String outcome, final String description) {
      return Counter.builder(BLOCKS_METER).description(description)
          .tag("db", database).tag("type", type).tag("surface", surface).tag("outcome", outcome)
          .register(Metrics.globalRegistry);
    }

    private static Counter pageCounter(final String database, final String type, final String surface,
        final String outcome, final String description) {
      return Counter.builder(PAGES_METER).description(description)
          .tag("db", database).tag("type", type).tag("surface", surface).tag("outcome", outcome)
          .register(Metrics.globalRegistry);
    }

    private static Timer phaseTimer(final String database, final String type, final String surface,
        final String phase) {
      return Timer.builder(PHASE_METER).description("Time a TimeSeries read spent in one phase")
          .tag("db", database).tag("type", type).tag("surface", surface).tag("phase", phase)
          .register(Metrics.globalRegistry);
    }
  }
}
