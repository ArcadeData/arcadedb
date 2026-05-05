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

import com.arcadedb.index.sparsevector.SparseVectorScoringPool;
import com.arcadedb.query.QueryEngineManager;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.binder.MeterBinder;

/**
 * Micrometer binding for the engine's executor pools so they show up in {@code /api/v1/metrics}
 * and the {@link io.micrometer.core.instrument.logging.LoggingMeterRegistry} alongside JVM
 * thread / GC / memory gauges. Both the {@link QueryEngineManager} pool (general query
 * parallelism) and the {@link SparseVectorScoringPool} (sparse-vector top-K fan-out) are
 * exposed under the same shape - one set of gauges per pool, tagged with {@code pool=<name>} -
 * so a Grafana panel can switch between them with a single label selector.
 * <p>
 * Lives in the server module because the engine's pom only pulls Micrometer at test scope; the
 * pools themselves expose framework-agnostic {@code PoolStats} records, and this binder
 * translates them into Micrometer gauges.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class PoolMetrics implements MeterBinder {
  /**
   * Registers gauges for the {@link QueryEngineManager} pool and the
   * {@link SparseVectorScoringPool} on the given registry.
   * <p>
   * Each gauge re-reads its source pool's {@code PoolStats} record on scrape - the record is a
   * tiny allocation (six longs) and Micrometer scrape intervals are typically tens of seconds,
   * so the cost is negligible. {@code callerRunFallbacks} is a strictly-monotonic counter so we
   * register it as a gauge that exposes the cumulative count; downstream tools (Prometheus
   * {@code rate()}, etc.) can derive a rate as needed.
   */
  @Override
  public void bindTo(final MeterRegistry registry) {
    final QueryEngineManager qem = QueryEngineManager.getInstance();
    bindPool(registry, "query", "QueryEngineManager parallel-query pool",
        () -> qem.getExecutorStats().poolSize(),
        () -> qem.getExecutorStats().activeThreads(),
        () -> qem.getExecutorStats().queueDepth(),
        () -> qem.getExecutorStats().queueCapacityRemaining(),
        () -> qem.getExecutorStats().completedTasks(),
        () -> qem.getExecutorStats().callerRunFallbacks());

    final SparseVectorScoringPool svsp = SparseVectorScoringPool.getInstance();
    bindPool(registry, "sparse_vector", "SparseVectorScoringPool top-K fan-out pool",
        () -> svsp.getPoolStats().poolSize(),
        () -> svsp.getPoolStats().activeThreads(),
        () -> svsp.getPoolStats().queueDepth(),
        () -> svsp.getPoolStats().queueCapacityRemaining(),
        () -> svsp.getPoolStats().completedTasks(),
        () -> svsp.getPoolStats().callerRunFallbacks());
  }

  private static void bindPool(final MeterRegistry registry, final String poolTag, final String description,
      final java.util.function.IntSupplier poolSize, final java.util.function.IntSupplier activeThreads,
      final java.util.function.IntSupplier queueDepth, final java.util.function.IntSupplier queueCapacityRemaining,
      final java.util.function.LongSupplier completedTasks, final java.util.function.LongSupplier callerRunFallbacks) {
    final Tags tags = Tags.of(Tag.of("pool", poolTag));
    io.micrometer.core.instrument.Gauge.builder("arcadedb.executor.pool.size", poolSize::getAsInt)
        .description(description + ": currently allocated worker threads").tags(tags).register(registry);
    io.micrometer.core.instrument.Gauge.builder("arcadedb.executor.pool.active", activeThreads::getAsInt)
        .description(description + ": worker threads currently running a task").tags(tags).register(registry);
    io.micrometer.core.instrument.Gauge.builder("arcadedb.executor.queue.depth", queueDepth::getAsInt)
        .description(description + ": tasks waiting in the queue").tags(tags).register(registry);
    io.micrometer.core.instrument.Gauge.builder("arcadedb.executor.queue.capacity_remaining", queueCapacityRemaining::getAsInt)
        .description(description + ": queue slots free before saturation triggers caller-runs fallback").tags(tags)
        .register(registry);
    io.micrometer.core.instrument.Gauge.builder("arcadedb.executor.tasks.completed", completedTasks::getAsLong)
        .description(description + ": cumulative tasks finished by pool threads").tags(tags).register(registry);
    io.micrometer.core.instrument.Gauge.builder("arcadedb.executor.tasks.caller_run_fallbacks", callerRunFallbacks::getAsLong)
        .description(
            description + ": cumulative tasks that ran on the submitter's thread because the queue was full. Sustained growth means the pool is undersized for the workload.")
        .tags(tags).register(registry);
  }
}
