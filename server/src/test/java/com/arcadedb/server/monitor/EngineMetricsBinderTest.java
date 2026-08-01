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

import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit test for {@link EngineMetricsBinder}: every curated engine meter is registered, carries the right
 * <i>instrument type</i>, and reads a finite numeric value from the live {@code Profiler} snapshot, whose stats are
 * nested JSON objects.
 */
class EngineMetricsBinderTest {

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
        "arcadedb.engine.tx.rollbacks", "arcadedb.engine.queries", "arcadedb.engine.commands" }) {
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
        "arcadedb.engine.databases" }) {
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
