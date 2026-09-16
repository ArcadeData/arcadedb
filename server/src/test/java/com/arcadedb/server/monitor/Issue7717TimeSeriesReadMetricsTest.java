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
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.search.Search;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7717: {@link AggregationMetrics} counts what a TimeSeries read actually did - blocks skipped on their
 * declared statistics, blocks that had to be decompressed, mutable pages discarded on their header, rows
 * materialised - and until {@link TimeSeriesReadMetrics} existed nothing on the HTTP or gRPC surface ever passed
 * one. Every {@code /ts/**} handler passed {@code null}, so the counters existed for tests to assert on and for
 * no operator to see: whether a tenant's blocks are being skipped, or every one of them is falling back because
 * the tag column is not a {@code STRING}, was not observable at all.
 * <p>
 * The two halves pinned here are the ones the issue asks for: the numbers reach the sink, and {@code null} stays
 * legal and allocation-free on the path that does not want counters.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7717TimeSeriesReadMetricsTest {

  // One registry for the whole class, attached to the global composite the sink publishes to. Per-test
  // registries would not work: a meter built while one was attached keeps recording into it, so the next test's
  // registry would answer 0 for a counter that had in fact been incremented. Each test uses a database name of
  // its own instead, which is what keeps the assertions independent.
  private static SimpleMeterRegistry registry;

  @BeforeAll
  static void addRegistry() {
    registry = new SimpleMeterRegistry();
    Metrics.addRegistry(registry);
    TimeSeriesReadMetrics.setEnabled(true);
  }

  /**
   * The ceiling test fills the cache on purpose, and a full cache collapses every later tuple onto the overflow
   * tag - so without this one test would decide what its neighbours are able to measure, depending on the order
   * JUnit happens to run them in.
   */
  @BeforeEach
  void clearCache() {
    TimeSeriesReadMetrics.clearMeterCache();
  }

  @AfterAll
  static void removeRegistry() {
    // Disabling clears the meter cache too: a cached meter is bound to the registries that backed it when it
    // was built, and recording into one whose registry is gone discards the sample silently.
    TimeSeriesReadMetrics.setEnabled(false);
    Metrics.removeRegistry(registry);
    registry.close();
  }

  /**
   * "Passing null must stay legal and allocation-free on the paths that do not want counters." With the sink
   * off, {@link TimeSeriesReadMetrics#start()} hands the engine the {@code null} it has always accepted, which
   * is the path on which no {@code System.nanoTime()} is called and no per-shard instance is allocated.
   */
  @Test
  void theSinkCostsNothingWhenMetricsAreOff() {
    TimeSeriesReadMetrics.setEnabled(false);
    try {
      assertThat(TimeSeriesReadMetrics.start()).as("null is what the engine takes for 'do not count'").isNull();
      assertThat(TimeSeriesReadMetrics.isEnabled()).isFalse();

      // Publishing that null is a no-op rather than an NPE, so a handler needs no branch of its own. And a
      // non-null one published while the sink is off registers nothing either, which is what makes the toggle a
      // real off switch rather than a hint.
      TimeSeriesReadMetrics.publish(null, "off-db", "Metric", TimeSeriesReadMetrics.SURFACE_TS_QUERY);
      final AggregationMetrics counted = new AggregationMetrics();
      counted.addSkippedBlock();
      TimeSeriesReadMetrics.publish(counted, "off-db", "Metric", TimeSeriesReadMetrics.SURFACE_TS_QUERY);

      assertThat(Search.in(registry).name("arcadedb.timeseries.read.blocks").tag("db", "off-db").counters())
          .isEmpty();
    } finally {
      TimeSeriesReadMetrics.setEnabled(true);
    }
  }

  @Test
  void theSinkHandsOutACollectorWhenMetricsAreOn() {
    assertThat(TimeSeriesReadMetrics.start()).isNotNull();
  }

  /**
   * Every counter {@link AggregationMetrics} carries reaches a meter, tagged so an operator can read the ratio
   * per database, per type and per endpoint - which is the question that made a per-request {@code ?debug=true}
   * field the wrong answer: "what fraction of blocks is this dashboard decompressing" is a rate over time.
   */
  @Test
  void everyCounterReachesATaggedMeter() {
    final AggregationMetrics metrics = new AggregationMetrics();
    metrics.addSkippedBlock();
    metrics.addSkippedBlock();
    metrics.addSkippedBlock();
    metrics.addFastPathBlock();
    metrics.addSlowPathBlock();
    metrics.addSlowPathBlock();
    metrics.addScannedPage();
    metrics.addSkippedPage();
    metrics.addMaterializedRows(17);
    metrics.addOverflowBuckets(2);
    metrics.addIo(1_000_000);
    metrics.addDecompTs(2_000_000);
    metrics.addDecompVal(3_000_000);
    metrics.addAccum(4_000_000);

    TimeSeriesReadMetrics.publish(metrics, "all-counters", "Readings",
        TimeSeriesReadMetrics.SURFACE_PROM_LABEL_VALUES);

    assertThat(blocks("all-counters", "skipped")).isEqualTo(3);
    assertThat(blocks("all-counters", "fast-path")).isEqualTo(1);
    assertThat(blocks("all-counters", "slow-path")).isEqualTo(2);
    assertThat(pages("all-counters", "scanned")).isEqualTo(1);
    assertThat(pages("all-counters", "skipped")).isEqualTo(1);
    assertThat(counter("all-counters", "arcadedb.timeseries.read.rows")).isEqualTo(17);
    assertThat(counter("all-counters", "arcadedb.timeseries.read.overflow.buckets")).isEqualTo(2);

    for (final String phase : new String[] { "io", "decompress-timestamps", "decompress-values", "accumulate" })
      assertThat(Search.in(registry).name("arcadedb.timeseries.read.phase").tag("db", "all-counters")
          .tag("phase", phase).timer()).as("phase %s", phase).isNotNull();
  }

  /**
   * A phase that contributed nothing is not recorded at all. A zero sample would pull the phase's mean toward
   * zero and claim the read did that work instantly, when what happened is that it did not do it: a read
   * answered entirely from block headers performs no I/O and decompresses nothing.
   */
  @Test
  void aPhaseThatDidNothingRecordsNoSample() {
    final AggregationMetrics headerOnly = new AggregationMetrics();
    headerOnly.addFastPathBlock();
    headerOnly.addAccum(5_000);

    TimeSeriesReadMetrics.publish(headerOnly, "header-only", "Readings", TimeSeriesReadMetrics.SURFACE_TS_QUERY);

    assertThat(Search.in(registry).name("arcadedb.timeseries.read.phase").tag("db", "header-only")
        .tag("phase", "io").timer().count())
        .as("a read that touched no block data did no I/O; a 0ns sample would claim it did it instantly")
        .isZero();
    assertThat(Search.in(registry).name("arcadedb.timeseries.read.phase").tag("db", "header-only")
        .tag("phase", "accumulate").timer().count()).isEqualTo(1);
  }

  /**
   * The surface tag separates the endpoints, so a slow Grafana panel and a slow label picker over the same type
   * are two different series rather than one.
   */
  @Test
  void theSurfaceTagSeparatesTheEndpoints() {
    final AggregationMetrics grafana = new AggregationMetrics();
    grafana.addSlowPathBlock();
    TimeSeriesReadMetrics.publish(grafana, "surfaces", "Readings", TimeSeriesReadMetrics.SURFACE_GRAFANA);

    final AggregationMetrics grpc = new AggregationMetrics();
    grpc.addSlowPathBlock();
    grpc.addSlowPathBlock();
    TimeSeriesReadMetrics.publish(grpc, "surfaces", "Readings", TimeSeriesReadMetrics.SURFACE_GRPC);

    assertThat(Search.in(registry).name("arcadedb.timeseries.read.blocks").tag("db", "surfaces")
        .tag("surface", TimeSeriesReadMetrics.SURFACE_GRAFANA).tag("outcome", "slow-path").counter().count())
        .isEqualTo(1);
    assertThat(Search.in(registry).name("arcadedb.timeseries.read.blocks").tag("db", "surfaces")
        .tag("surface", TimeSeriesReadMetrics.SURFACE_GRPC).tag("outcome", "slow-path").counter().count())
        .isEqualTo(2);
  }

  /**
   * The db and type halves of the tag tuple are the only ones that can grow - a database or a TimeSeries type
   * can be created and dropped in a loop - so past the ceiling they collapse onto a constant. A
   * {@code MeterFilter} can deny the meter but cannot stop {@code computeIfAbsent} from retaining the key, which
   * is the leak of issues #5025 and #6805 on these meters.
   */
  @Test
  void theMeterCacheIsBounded() {
    // Across EVERY surface, not one: the collapse keeps the surface half of the tuple, so overflowing a single
    // surface would leave the other five untested and the bound would look tighter than it is
    // (claude-review on PR #7728).
    for (int i = 0; i < TimeSeriesReadMetrics.MAX_METER_SETS + 500; i++) {
      final AggregationMetrics metrics = new AggregationMetrics();
      metrics.addSkippedBlock();
      TimeSeriesReadMetrics.publish(metrics, "db-" + i, "Readings",
          TimeSeriesReadMetrics.SURFACES[i % TimeSeriesReadMetrics.SURFACES.length]);
    }

    assertThat(TimeSeriesReadMetrics.cachedMeterSetCount())
        .as("the cache must not grow with the number of distinct databases seen")
        .isLessThanOrEqualTo(TimeSeriesReadMetrics.MAX_METER_SETS + TimeSeriesReadMetrics.RESERVED_OVERFLOW_METER_SETS);

    // And the collapse really does keep the surface, which is what that reserved count is paying for: once the
    // tenant half is given up, which endpoint is doing the reading is the half still worth having.
    for (final String surface : TimeSeriesReadMetrics.SURFACES) {
      final AggregationMetrics metrics = new AggregationMetrics();
      metrics.addSkippedBlock();
      TimeSeriesReadMetrics.publish(metrics, "db-past-the-ceiling", "Readings", surface);
      assertThat(Search.in(registry).name("arcadedb.timeseries.read.blocks").tag("db", "other")
          .tag("surface", surface).counters())
          .as("the overflow tuple for surface %s absorbs what the ceiling refused to cache", surface)
          .isNotEmpty();
    }
  }

  /**
   * A database literally named {@code other} - the string the collapse uses - must not be mistaken for the
   * collapsed tuple and handed a free pass past the ceiling. A guard reading only the db half did exactly that,
   * so once the cache was full that one database's types grew it without bound: the meter-cardinality leak of
   * issues #5025 and #6805, reintroduced by the sentinel (claude-review on PR #7728).
   */
  @Test
  void aDatabaseNamedLikeTheOverflowTagIsStillBounded() {
    for (int i = 0; i < TimeSeriesReadMetrics.MAX_METER_SETS + 100; i++) {
      final AggregationMetrics metrics = new AggregationMetrics();
      metrics.addSkippedBlock();
      TimeSeriesReadMetrics.publish(metrics, "filler-" + i, "Readings", TimeSeriesReadMetrics.SURFACE_TS_QUERY);
    }
    final int afterFilling = TimeSeriesReadMetrics.cachedMeterSetCount();

    // A real database that happens to be called "other", with many types of its own.
    for (int i = 0; i < 500; i++) {
      final AggregationMetrics metrics = new AggregationMetrics();
      metrics.addSkippedBlock();
      TimeSeriesReadMetrics.publish(metrics, "other", "Type-" + i, TimeSeriesReadMetrics.SURFACE_TS_QUERY);
    }

    assertThat(TimeSeriesReadMetrics.cachedMeterSetCount())
        .as("a database sharing the sentinel string must collapse like any other, not bypass the ceiling")
        .isEqualTo(afterFilling);
  }

  private static double blocks(final String database, final String outcome) {
    return Search.in(registry).name("arcadedb.timeseries.read.blocks").tag("db", database)
        .tag("outcome", outcome).counter().count();
  }

  private static double pages(final String database, final String outcome) {
    return Search.in(registry).name("arcadedb.timeseries.read.pages").tag("db", database)
        .tag("outcome", outcome).counter().count();
  }

  private static double counter(final String database, final String name) {
    return Search.in(registry).name(name).tag("db", database).counter().count();
  }
}
