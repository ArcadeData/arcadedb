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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.PageSnapshot;
import com.arcadedb.utility.FileUtils;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.function.DoublePredicate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit test for {@link EngineMetricsBinder}: every curated engine meter is registered, carries the right
 * <i>instrument type</i>, and reads a finite numeric value from the live {@code Profiler} snapshot, whose stats are
 * nested JSON objects.
 */
class EngineMetricsBinderTest {

  private static final String DATABASE_PATH = "target/databases/engine-metrics-binder";

  /**
   * #5636: every monotonic total must be a {@link FunctionCounter}, not a {@link Gauge}. As a gauge it exported with
   * no {@code _total} suffix and no type hint that {@code rate()}/{@code increase()} are the right functions over the
   * series, so a dashboard author saw a line that only goes up and learned nothing from it.
   */
  @Test
  void monotonicTotalsAreCountersNotGauges() {
    final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    new EngineMetricsBinder().bindTo(registry);

    for (final String name : new String[] { "arcadedb.engine.page.cache.hits", "arcadedb.engine.page.cache.misses",
        "arcadedb.engine.pages.read", "arcadedb.engine.pages.written", "arcadedb.engine.wal.bytes.written",
        "arcadedb.engine.mvcc.conflicts", "arcadedb.engine.page.merges.edge.append", "arcadedb.engine.page.merges.slot",
        "arcadedb.engine.page.merges.declined", "arcadedb.engine.tx.write", "arcadedb.engine.tx.read",
        "arcadedb.engine.tx.rollbacks", "arcadedb.engine.queries", "arcadedb.engine.commands",
        "arcadedb.engine.snapshot.windows.opened", "arcadedb.engine.snapshot.windows.invalidated",
        "arcadedb.engine.snapshot.preimages.captured",
        // #6125: the invalidation split and the t0 barrier timer, whose sum/count pair is what a Prometheus Timer
        // exports and what makes rate(seconds)/rate(count) the average barrier latency
        "arcadedb.engine.snapshot.windows.overflowed", "arcadedb.engine.snapshot.windows.failed",
        "arcadedb.engine.snapshot.barrier.count", "arcadedb.engine.snapshot.barrier.seconds",
        "arcadedb.engine.snapshot.barrier.inexact" }) {
      final FunctionCounter counter = registry.find(name).functionCounter();
      assertThat(counter).as(name).isNotNull();
      assertThat(Double.isNaN(counter.count())).as(name).isFalse();
      assertThat(registry.find(name).gauge()).as(name + " must no longer be a gauge").isNull();
    }
  }

  /**
   * The instantaneous readings go up AND down, so they must stay gauges: exported as counters, every database close
   * or WAL-file retirement would look like a counter reset.
   */
  @Test
  void instantaneousReadingsStayGauges() {
    final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    new EngineMetricsBinder().bindTo(registry);

    for (final String name : new String[] { "arcadedb.engine.wal.files", "arcadedb.engine.files.open",
        "arcadedb.engine.databases", "arcadedb.engine.snapshot.windows.open", "arcadedb.engine.snapshot.shadow.pages",
        "arcadedb.engine.snapshot.shadow.bytes", "arcadedb.engine.snapshot.shadow.spilled.bytes",
        "arcadedb.engine.snapshot.shadow.usage.percent", "arcadedb.engine.snapshot.window.age.ms",
        // #6125: a high-water mark. Monotonic, but a rate() over it would be meaningless, so it is the one
        // never-decreasing reading here that is deliberately a gauge
        "arcadedb.engine.snapshot.barrier.max.seconds",
        "arcadedb.engine.flush.deferred.bytes" }) {
      final Gauge gauge = registry.find(name).gauge();
      assertThat(gauge).as(name).isNotNull();
      assertThat(Double.isNaN(gauge.value())).as(name).isFalse();
      assertThat(registry.find(name).functionCounter()).as(name + " must not be a counter").isNull();
    }
  }

  /**
   * Profiler nests each stat as {@code {count|space|value: N}}; reading a meter must extract the inner numeric, not
   * throw on the wrapping object, and never return NaN.
   */
  @Test
  void metersReadNestedProfilerValuesWithoutThrowing() {
    final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    new EngineMetricsBinder().bindTo(registry);

    assertThat(Double.isNaN(registry.find("arcadedb.engine.page.cache.hits").functionCounter().count())).isFalse();
    // walTotalFiles is the one Profiler emits as a bare scalar rather than a nested object.
    assertThat(Double.isNaN(registry.find("arcadedb.engine.wal.files").gauge().value())).isFalse();
    assertThat(Double.isNaN(registry.find("arcadedb.engine.wal.bytes.written").functionCounter().count())).isFalse();
  }

  /**
   * #5608: a collapse of the commit-time page-merge rate (or a rise in the coverage declines) is a throughput
   * regression that no correctness signal catches, so the three counters have to be alertable - i.e. reach a
   * registry, not just {@code PageManager.getStats()}.
   */
  @Test
  void registersPageMergeCounters() {
    final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    new EngineMetricsBinder().bindTo(registry);

    for (final String name : new String[] { "arcadedb.engine.page.merges.edge.append",
        "arcadedb.engine.page.merges.slot", "arcadedb.engine.page.merges.declined" }) {
      final FunctionCounter counter = registry.find(name).functionCounter();
      assertThat(counter).as(name).isNotNull();
      assertThat(Double.isNaN(counter.count())).as(name).isFalse();
    }
  }

  /**
   * #6116: registration is not the property that matters - the meters have to move when a point-in-time snapshot
   * window (#6075) opens. An operator watching a backup needs to see the window, how much the copy-on-write shadow
   * is holding and how close it is to {@code arcadedb.pageSnapshotMaxSize}, before the breach forces the backup back
   * onto the path that throttles writers.
   * <p>
   * The shadow-usage assertion is the one that earns its keep: against a cap sized from the whole database a few
   * shadowed pages are a small FRACTION of a percent, so a reading truncated to a long would report a comfortable
   * zero for a window that is actually filling.
   */
  @Test
  void theSnapshotGaugesFollowAnOpenWindow() throws Exception {
    final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    new EngineMetricsBinder().bindTo(registry);

    FileUtils.deleteRecursively(new File(DATABASE_PATH));
    try (final Database database = new DatabaseFactory(DATABASE_PATH).create()) {
      database.getSchema().createDocumentType("Doc");
      database.transaction(() -> {
        for (int i = 0; i < 5_000; i++)
          database.newDocument("Doc").set("id", i).set("payload", "x".repeat(200)).save();
      });

      final DatabaseInternal db = (DatabaseInternal) database;
      db.getPageManager().waitAllPagesOfDatabaseAreFlushed(db);

      // NO ABSOLUTE ZERO HERE, AND NO ABSOLUTE ONE BELOW: the gauges read PageManager.INSTANCE, a JVM-WIDE
      // singleton, so another test class in this surefire fork can legitimately have a window open at the same
      // time. What must hold is the DELTA this test causes.
      final double windowsBefore = awaitGauge(registry, "arcadedb.engine.snapshot.windows.open", value -> true);
      final double shadowBytesBefore = registry.find("arcadedb.engine.snapshot.shadow.bytes").gauge().value();

      try (final PageSnapshot snapshot = db.getPageManager().openSnapshot(db)) {
        database.transaction(() -> database.iterateType("Doc", false).forEachRemaining(
            record -> record.asDocument().modify().set("payload", "y".repeat(200)).save()));
        db.getPageManager().waitAllPagesOfDatabaseAreFlushed(db);
        assertThat(snapshot.getShadowedPages()).as("the rewrite must have shadowed pages to report").isPositive();

        assertThat(awaitGauge(registry, "arcadedb.engine.snapshot.windows.open", value -> value >= windowsBefore + 1))
            .as("the open window must reach the gauge (%f were already open)", windowsBefore)
            .isGreaterThanOrEqualTo(windowsBefore + 1);
        assertThat(registry.find("arcadedb.engine.snapshot.shadow.pages").gauge().value())
            .isGreaterThanOrEqualTo(snapshot.getShadowedPages());
        assertThat(registry.find("arcadedb.engine.snapshot.shadow.bytes").gauge().value())
            .isGreaterThanOrEqualTo(snapshot.getShadowSizeInBytes());
        assertThat(registry.find("arcadedb.engine.snapshot.window.age.ms").gauge().value()).isNotNegative();

        final double usage = registry.find("arcadedb.engine.snapshot.shadow.usage.percent").gauge().value();
        assertThat(usage).as("a fraction of a percent is still a reading, not a zero").isPositive().isLessThan(100d);

        assertThat(registry.find("arcadedb.engine.snapshot.preimages.captured").functionCounter().count()).isPositive();
        assertThat(registry.find("arcadedb.engine.snapshot.windows.opened").functionCounter().count()).isPositive();

        // #6125: OPENING THE WINDOW RAN A t0 BARRIER, SO ITS TIMER MUST HAVE MOVED. THE DURATION IS ASSERTED AS
        // NON-NEGATIVE RATHER THAN POSITIVE ON PURPOSE - AN IDLE BARRIER TAKES TENS OF MICROSECONDS AND ROUNDS TO
        // ZERO MILLISECONDS, WHICH IS THE HEALTHY CASE, NOT A BROKEN METER
        assertThat(registry.find("arcadedb.engine.snapshot.barrier.count").functionCounter().count()).isPositive();
        assertThat(registry.find("arcadedb.engine.snapshot.barrier.seconds").functionCounter().count())
            .isNotNegative();
        assertThat(registry.find("arcadedb.engine.snapshot.barrier.max.seconds").gauge().value()).isNotNegative();
      }

      assertThat(awaitGauge(registry, "arcadedb.engine.snapshot.windows.open", value -> value <= windowsBefore))
          .as("the gauges must come back down when the window closes").isLessThanOrEqualTo(windowsBefore);
      assertThat(registry.find("arcadedb.engine.snapshot.shadow.bytes").gauge().value())
          .isLessThanOrEqualTo(shadowBytesBefore);
    } finally {
      FileUtils.deleteRecursively(new File(DATABASE_PATH));
    }
  }

  /**
   * The binder memoizes the {@link com.arcadedb.Profiler} snapshot for a second, so a freshly changed reading only
   * appears on the next rebuild. Polls rather than sleeping for the TTL so the test is not tied to its length, and
   * returns the last value it read - satisfying the condition or not - so the assertion reports the actual number
   * rather than a bare {@code false}.
   */
  private static double awaitGauge(final SimpleMeterRegistry registry, final String name,
      final DoublePredicate condition) throws InterruptedException {
    double value = registry.find(name).gauge().value();
    for (int attempt = 0; attempt < 60 && !condition.test(value); attempt++) {
      Thread.sleep(100);
      value = registry.find(name).gauge().value();
    }
    return value;
  }

  /**
   * Micrometer's {@code (obj, fn)} builders hold a WEAK reference to {@code obj}, and the only production caller does
   * {@code new EngineMetricsBinder().bindTo(registry)} - so anchoring the meters to the binder instance would let
   * every one of them silently stop reporting at the next GC.
   * <p>
   * Asserted structurally rather than by forcing a collection: {@code System.gc()} is only a hint, and because the
   * meters read through the static cache the values come back either way - so a GC-based test would keep passing if
   * someone regressed the cache to an instance field, which is the exact bug it is supposed to catch. What actually
   * has to hold is that whatever the meters are anchored to outlives the binder, i.e. that no instance field of the
   * binder can be the anchor.
   */
  @Test
  void theMetersAreAnchoredToSomethingThatOutlivesTheBinder() {
    for (final Field field : EngineMetricsBinder.class.getDeclaredFields())
      assertThat(Modifier.isStatic(field.getModifiers()))
          .as("EngineMetricsBinder.%s is an instance field. Micrometer holds the meter's read target by WEAK "
              + "reference and the binder is discarded right after bindTo(), so anchoring a meter here would make "
              + "every engine metric stop reporting at the next GC.", field.getName())
          .isTrue();

    final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    new EngineMetricsBinder().bindTo(registry);

    System.gc();

    final FunctionCounter counter = registry.find("arcadedb.engine.page.cache.hits").functionCounter();
    assertThat(counter).isNotNull();
    assertThat(Double.isNaN(counter.count())).isFalse();

    final Gauge gauge = registry.find("arcadedb.engine.databases").gauge();
    assertThat(gauge).isNotNull();
    assertThat(Double.isNaN(gauge.value())).isFalse();
  }
}
