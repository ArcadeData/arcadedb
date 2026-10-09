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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.query.sql.executor.QueryAdmissionGate;
import com.arcadedb.utility.DedicatedThreadPool.PoolStats;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import org.junit.jupiter.api.Test;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.handler.GetServerHandler;

import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies the {@link PoolMetrics} {@code MeterBinder} registers the expected Micrometer gauges
 * for both the QueryEngineManager pool and the SparseVectorScoringPool. Uses a fresh
 * {@link SimpleMeterRegistry} so the test is isolated from the global registry that
 * {@link com.arcadedb.server.ArcadeDBServer} configures in production.
 */
class PoolMetricsTest {

  private static final Set<String> EXPECTED_GAUGE_NAMES = Set.of(
      "arcadedb.executor.pool.size",
      "arcadedb.executor.pool.active",
      "arcadedb.executor.queue.depth",
      "arcadedb.executor.queue.capacity_remaining",
      "arcadedb.executor.tasks.completed",
      "arcadedb.executor.tasks.caller_run_fallbacks",
      // #6568: queued tasks a waiting caller ran itself. On the same row as caller_run_fallbacks
      // deliberately - the two are read together and say opposite things (busy vs full).
      "arcadedb.executor.tasks.reclaimed");

  /**
   * Gauges only the sparse-vector pool publishes: they explain its per-query decision to split a
   * top-K into parallel RID ranges (issue #4085). Deliberately absent on the other pools, which take
   * work as it is handed to them - Studio renders a dash there rather than a zero that would read as
   * "nothing is splitting" when the concept does not apply.
   */
  private static final Set<String> SPARSE_ONLY_GAUGE_NAMES = Set.of(
      "arcadedb.executor.pool.reserved",
      "arcadedb.executor.queries.in_flight",
      "arcadedb.executor.queries.split");

  @Test
  void registersGaugesForBothPoolsWithPoolTag() {
    final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    new PoolMetrics().bindTo(registry);

    // Every expected gauge must exist for both the "query" and "sparse_vector" pools. Anything
    // missing means a downstream dashboard would have a hole.
    final Set<String> registeredNames = registry.getMeters().stream()
        .map(m -> m.getId().getName())
        .collect(Collectors.toSet());
    assertThat(registeredNames).as("every shared gauge should be registered (per-pool tagging adds duplicates)")
        .containsAll(EXPECTED_GAUGE_NAMES);

    for (final String poolTag : new String[] { "query", "sparse_vector", "async_command" }) {
      for (final String gaugeName : EXPECTED_GAUGE_NAMES) {
        final Meter meter = registry.find(gaugeName).tag("pool", poolTag).meter();
        assertThat(meter)
            .as("gauge '%s' tagged pool=%s must be registered", gaugeName, poolTag)
            .isNotNull();
      }
    }

    for (final String gaugeName : SPARSE_ONLY_GAUGE_NAMES) {
      assertThat(registry.find(gaugeName).tag("pool", "sparse_vector").meter())
          .as("split-decision gauge '%s' must be registered for the sparse-vector pool", gaugeName).isNotNull();
      assertThat(registry.find(gaugeName).tag("pool", "query").meter())
          .as("split-decision gauge '%s' must NOT be registered for pools that never split", gaugeName).isNull();
    }
  }

  /**
   * Issue #9518: the query admission gate is published on the pool row shape, so the Studio "Executor Pools" card shows
   * it without a card of its own: slots and running queries, queue depth and room, admitted and refused queries. The
   * pool-only gauges are absent, so the card renders a dash there rather than a zero.
   */
  @Test
  void theQueryAdmissionGateIsPublishedOnThePoolRowShape() {
    final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    new PoolMetrics().bindTo(registry);

    GlobalConfiguration.QUERY_MAX_CONCURRENT.setValue(3);
    try {
      final QueryAdmissionGate gate = QueryAdmissionGate.getInstance();
      final double runningBefore = registry.find("arcadedb.executor.pool.active").tag("pool", "query_admission").gauge().value();
      final double admittedBefore = registry.find("arcadedb.executor.tasks.completed").tag("pool", "query_admission").gauge().value();
      try (final QueryAdmissionGate.Ticket ignored = gate.admit()) {
        assertThat(registry.find("arcadedb.executor.pool.size").tag("pool", "query_admission").gauge().value()).isEqualTo(3.0);
        assertThat(registry.find("arcadedb.executor.pool.active").tag("pool", "query_admission").gauge().value())
            .isEqualTo(runningBefore + 1);
        assertThat(registry.find("arcadedb.executor.tasks.completed").tag("pool", "query_admission").gauge().value())
            .isEqualTo(admittedBefore + 1);
      }

      for (final String gaugeName : new String[] { "arcadedb.executor.queue.depth", "arcadedb.executor.queue.capacity_remaining",
          PoolMetrics.REJECTED_GAUGE })
        assertThat(registry.find(gaugeName).tag("pool", "query_admission").gauge()).as(gaugeName).isNotNull();
      for (final String gaugeName : new String[] { "arcadedb.executor.tasks.caller_run_fallbacks", "arcadedb.executor.tasks.reclaimed" })
        assertThat(registry.find(gaugeName).tag("pool", "query_admission").gauge()).as("a gate has no " + gaugeName).isNull();
      for (final String counterName : new String[] { "arcadedb.query.admission.waited", "arcadedb.query.admission.wait_time",
          "arcadedb.query.admission.heap_deferrals" })
        assertThat(registry.find(counterName).functionCounter()).as(counterName).isNotNull();

      final JSONObject row = GetServerHandler.buildExecutorsJSON(registry).getJSONObject("query_admission");
      assertThat(row.getDouble("pool.size")).isEqualTo(3.0);
      assertThat(row.has("tasks.rejected")).isTrue();
    } finally {
      GlobalConfiguration.QUERY_MAX_CONCURRENT.reset();
    }
  }

  /**
   * Each gauge re-reads its source pool's stats on scrape. The values must be sane (non-NaN,
   * non-negative for everything except potentially null counters which we treat as zero) on a
   * freshly-constructed singleton.
   */
  @Test
  void gaugesProduceSensibleValuesAtRest() {
    final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    new PoolMetrics().bindTo(registry);

    for (final String poolTag : new String[] { "query", "sparse_vector", "async_command" }) {
      for (final String gaugeName : EXPECTED_GAUGE_NAMES) {
        final double value = registry.find(gaugeName).tag("pool", poolTag).gauge().value();
        assertThat(value).as("%s pool=%s should report a finite, non-negative value", gaugeName, poolTag)
            .isFinite()
            .isGreaterThanOrEqualTo(0.0);
      }
    }
  }

  /**
   * Studio's {@code studio-server.js} reads {@code metrics.executors.<pool>.<gauge>} from the
   * {@code GET /api/v1/server} JSON response - so the wire format produced by
   * {@link com.arcadedb.server.http.handler.GetServerHandler#buildExecutorsJSON} has to match
   * what the JS expects: one object per pool tag, each holding the shared gauges keyed by their
   * post-prefix names ({@code "pool.size"}, {@code "pool.active"}, ...). This test pins that
   * contract without booting a full server - if the JSON shape ever changes, the dashboard
   * would silently render zeros and only this test would catch the regression.
   */
  @Test
  void executorsJsonMatchesStudioContract() {
    final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    new PoolMetrics().bindTo(registry);

    final JSONObject executors =
        GetServerHandler.buildExecutorsJSON(registry);

    for (final String poolTag : new String[] { "query", "sparse_vector", "async_command" }) {
      assertThat(executors.has(poolTag))
          .as("executors JSON must have key '%s' (Studio dashboard reads this exact tag)", poolTag).isTrue();
      final JSONObject pool = executors.getJSONObject(poolTag);
      for (final String gaugeName : EXPECTED_GAUGE_NAMES) {
        // Studio reads e.g. metrics.executors.query["pool.size"] - the post-prefix name.
        final String shortName = gaugeName.substring("arcadedb.executor.".length());
        assertThat(pool.has(shortName))
            .as("pool=%s must expose gauge '%s' in JSON", poolTag, shortName).isTrue();
        // Sanity: numeric, finite. Studio rounds with Math.round, so a non-numeric value would
        // render as NaN.
        assertThat(pool.getDouble(shortName))
            .as("pool=%s gauge '%s' must be a finite number", poolTag, shortName).isFinite();
      }
    }

    // The split-decision gauges reach Studio through the same grouping, on the sparse-vector row
    // only. The JS renders a dash where a key is absent, so "present for sparse_vector, absent
    // elsewhere" is the contract - a zero on the wrong row would read as "nothing is splitting".
    final JSONObject sparse = executors.getJSONObject("sparse_vector");
    final JSONObject query = executors.getJSONObject("query");
    for (final String gaugeName : SPARSE_ONLY_GAUGE_NAMES) {
      final String shortName = gaugeName.substring("arcadedb.executor.".length());
      assertThat(sparse.has(shortName))
          .as("pool=sparse_vector must expose split gauge '%s' in JSON", shortName).isTrue();
      assertThat(sparse.getDouble(shortName))
          .as("split gauge '%s' must be a finite number", shortName).isFinite();
      assertThat(query.has(shortName))
          .as("pool=query must NOT expose split gauge '%s'", shortName).isFalse();
    }
  }

  /**
   * The instance-scoped registration path (issue #7856): a pool owned by a server or a state machine rather than
   * the JVM gets the same row every singleton pool gets, plus {@code tasks.coalesced}, and each gauge reads its
   * supplier on scrape rather than a value captured at registration.
   */
  @Test
  void instancePoolPublishesTheSharedRowPlusCoalescedAndReadsLive() throws Exception {
    final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    final AtomicReference<PoolStats> stats = new AtomicReference<>(new PoolStats(0, 0, 0, 1, 0, 0, 0));
    final AtomicLong coalesced = new AtomicLong();

    try (final Closeable ignored = PoolMetrics.bindInstancePool(registry, "test_instance", "Test instance pool",
        stats::get, coalesced::get)) {
      for (final String gaugeName : EXPECTED_GAUGE_NAMES)
        assertThat(registry.find(gaugeName).tag("pool", "test_instance").gauge())
            .as("instance pool must publish '%s'", gaugeName).isNotNull();
      assertThat(registry.find(PoolMetrics.COALESCED_GAUGE).tag("pool", "test_instance").gauge()).isNotNull();

      stats.set(new PoolStats(1, 1, 1, 0, 7, 0, 0));
      coalesced.set(3);

      assertThat(registry.find("arcadedb.executor.queue.depth").tag("pool", "test_instance").gauge().value())
          .isEqualTo(1.0);
      assertThat(registry.find("arcadedb.executor.tasks.completed").tag("pool", "test_instance").gauge().value())
          .isEqualTo(7.0);
      assertThat(registry.find(PoolMetrics.COALESCED_GAUGE).tag("pool", "test_instance").gauge().value())
          .isEqualTo(3.0);

      // Studio reads the coalesced count through the same per-pool grouping, and only where it is published.
      final JSONObject executors = GetServerHandler.buildExecutorsJSON(registry);
      assertThat(executors.getJSONObject("test_instance").getDouble("tasks.coalesced")).isEqualTo(3.0);
      assertThat(executors.getJSONObject("test_instance").getDouble("queue.depth")).isEqualTo(1.0);
    }
  }

  /** Closing the handle deregisters exactly its own meters, and a second close is harmless. */
  @Test
  void closingTheInstanceHandleRemovesItsMetersOnly() throws Exception {
    final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    new PoolMetrics().bindTo(registry);
    final int singletonMeters = registry.getMeters().size();

    final Closeable handle = PoolMetrics.bindInstancePool(registry, "test_instance", "Test instance pool",
        () -> new PoolStats(0, 0, 0, 1, 0, 0, 0), () -> 0L);
    assertThat(registry.getMeters().size()).isEqualTo(singletonMeters + EXPECTED_GAUGE_NAMES.size() + 1);

    handle.close();
    assertThat(registry.find("arcadedb.executor.pool.size").tag("pool", "test_instance").gauge()).isNull();
    assertThat(registry.find(PoolMetrics.COALESCED_GAUGE).tag("pool", "test_instance").gauge()).isNull();
    assertThat(registry.getMeters().size()).as("the singleton pools' rows must survive").isEqualTo(singletonMeters);

    handle.close();
    assertThat(registry.getMeters().size()).isEqualTo(singletonMeters);
  }

  /**
   * Two servers in one JVM publish the same meter ids, and Micrometer answers the second registration with the
   * first one's meter. The second handle must therefore own nothing: closing it - the second server stopping -
   * must not take the first server's row away with it.
   */
  @Test
  void aSecondBindingOfTheSameTagOwnsNothing() throws Exception {
    final SimpleMeterRegistry registry = new SimpleMeterRegistry();

    try (final Closeable first = PoolMetrics.bindInstancePool(registry, "test_instance", "first",
        () -> new PoolStats(0, 0, 0, 1, 11, 0, 0), () -> 0L)) {
      final Closeable second = PoolMetrics.bindInstancePool(registry, "test_instance", "second",
          () -> new PoolStats(0, 0, 0, 1, 22, 0, 0), () -> 0L);
      second.close();

      assertThat(registry.find("arcadedb.executor.tasks.completed").tag("pool", "test_instance").gauge())
          .as("the first binding's row must survive the second one closing").isNotNull();
      assertThat(registry.find("arcadedb.executor.tasks.completed").tag("pool", "test_instance").gauge().value())
          .isEqualTo(11.0);
    }
  }

  /**
   * {@link PoolMetrics#statsOf} turns a plain {@link ThreadPoolExecutor} into the row's record: the two numbers the
   * one-slot security pools exist to show are the queue depth and the slot left, and they are read live.
   */
  @Test
  void statsOfReadsAPlainExecutor() throws Exception {
    final ThreadPoolExecutor executor = new ThreadPoolExecutor(0, 1, 30L, TimeUnit.SECONDS,
        new ArrayBlockingQueue<>(1));
    final CountDownLatch release = new CountDownLatch(1);
    final CountDownLatch started = new CountDownLatch(1);
    try {
      assertThat(PoolMetrics.statsOf(executor)).isEqualTo(new PoolStats(0, 0, 0, 1, 0, 0, 0));

      executor.execute(() -> {
        started.countDown();
        try {
          release.await();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      });
      assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();
      executor.execute(() -> {
      });

      final PoolStats busy = PoolMetrics.statsOf(executor);
      assertThat(busy.poolSize()).isEqualTo(1);
      assertThat(busy.activeThreads()).isEqualTo(1);
      assertThat(busy.queueDepth()).isEqualTo(1);
      assertThat(busy.queueCapacityRemaining()).isZero();
      assertThat(busy.callerRunFallbacks()).as("a dropping pool never runs on the caller").isZero();
    } finally {
      release.countDown();
      executor.shutdown();
      executor.awaitTermination(10, TimeUnit.SECONDS);
    }
  }

  /**
   * Issue #8856: each instance pool publishes only the extra gauges its saturation semantics give a meaning to, so a
   * pool that never coalesces shows no {@code tasks.coalesced} and one that never refuses no {@code tasks.rejected}.
   */
  @Test
  void instancePoolPublishesOnlyTheExtraGaugesItIsGiven() throws Exception {
    final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    final AtomicLong rejected = new AtomicLong();
    final Supplier<PoolStats> idle = () -> new PoolStats(0, 0, 0, 1, 0, 0, 0);

    try (final Closeable abortPool = PoolMetrics.bindInstancePool(registry, "abort_pool", "Abort pool", idle, null,
        rejected::get);
        final Closeable plainPool = PoolMetrics.bindInstancePool(registry, "plain_pool", "Plain pool", idle, null, null);
        final Closeable bothPool = PoolMetrics.bindInstancePool(registry, "both_pool", "Both pool", idle, () -> 4L,
            () -> 5L)) {
      assertThat(registry.find(PoolMetrics.REJECTED_GAUGE).tag("pool", "abort_pool").gauge()).isNotNull();
      assertThat(registry.find(PoolMetrics.COALESCED_GAUGE).tag("pool", "abort_pool").gauge()).isNull();
      assertThat(registry.find(PoolMetrics.REJECTED_GAUGE).tag("pool", "plain_pool").gauge()).isNull();
      assertThat(registry.find(PoolMetrics.COALESCED_GAUGE).tag("pool", "plain_pool").gauge()).isNull();
      for (final String gaugeName : EXPECTED_GAUGE_NAMES)
        assertThat(registry.find(gaugeName).tag("pool", "plain_pool").gauge()).as("the shared row: '%s'", gaugeName)
            .isNotNull();

      rejected.set(2);
      final JSONObject executors = GetServerHandler.buildExecutorsJSON(registry);
      assertThat(executors.getJSONObject("abort_pool").getDouble("tasks.rejected")).isEqualTo(2.0);
      assertThat(executors.getJSONObject("abort_pool").has("tasks.coalesced")).isFalse();
      assertThat(executors.getJSONObject("plain_pool").has("tasks.rejected")).isFalse();
      assertThat(executors.getJSONObject("both_pool").getDouble("tasks.coalesced")).isEqualTo(4.0);
      assertThat(executors.getJSONObject("both_pool").getDouble("tasks.rejected")).isEqualTo(5.0);
    }
    assertThat(registry.find(PoolMetrics.REJECTED_GAUGE).gauge()).as("closing removes the rejected gauge too").isNull();
  }

  /**
   * The "already published" check used to look for {@code tasks.coalesced}, which a pool without one never has: a
   * second binding of such a tag would have taken the first one's meters as its own, and its close deleted them.
   */
  @Test
  void aSecondBindingOfATagWithoutExtrasOwnsNothing() throws Exception {
    final SimpleMeterRegistry registry = new SimpleMeterRegistry();

    try (final Closeable first = PoolMetrics.bindInstancePool(registry, "plain_pool", "first",
        () -> new PoolStats(0, 0, 0, 1, 11, 0, 0), null, null)) {
      PoolMetrics.bindInstancePool(registry, "plain_pool", "second", () -> new PoolStats(0, 0, 0, 1, 22, 0, 0), null,
          null).close();

      assertThat(registry.find("arcadedb.executor.tasks.completed").tag("pool", "plain_pool").gauge())
          .as("the first binding's row must survive the second one closing").isNotNull();
      assertThat(registry.find("arcadedb.executor.tasks.completed").tag("pool", "plain_pool").gauge().value())
          .isEqualTo(11.0);
    }
  }

  /** A caller-runs {@link CountingRejectionPolicy}'s count is the {@code caller_run_fallbacks} statsOf reads. */
  @Test
  void statsOfReadsACountingCallerRunsPolicy() throws Exception {
    final ThreadPoolExecutor executor = new ThreadPoolExecutor(0, 1, 30L, TimeUnit.SECONDS, new ArrayBlockingQueue<>(1),
        CountingRejectionPolicy.callerRuns());
    final CountDownLatch release = new CountDownLatch(1);
    final CountDownLatch started = new CountDownLatch(1);
    try {
      executor.execute(() -> {
        started.countDown();
        try {
          release.await();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      });
      assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();
      executor.execute(() -> {
      });
      executor.execute(() -> {
      });

      assertThat(PoolMetrics.statsOf(executor).callerRunFallbacks()).isEqualTo(1);
      assertThat(PoolMetrics.statsOf(executor, 7).callerRunFallbacks()).as("an owner-counted fallback").isEqualTo(7);
    } finally {
      release.countDown();
      executor.shutdown();
      executor.awaitTermination(10, TimeUnit.SECONDS);
    }
  }

  /** An abort {@link CountingRejectionPolicy}'s count is not a caller-runs fallback: it belongs to tasks.rejected. */
  @Test
  void statsOfDoesNotReadAnAbortPolicyAsFallbacks() throws Exception {
    final CountingRejectionPolicy policy = CountingRejectionPolicy.abort();
    final ThreadPoolExecutor executor = new ThreadPoolExecutor(0, 1, 30L, TimeUnit.SECONDS, new ArrayBlockingQueue<>(1),
        policy);
    final CountDownLatch release = new CountDownLatch(1);
    final CountDownLatch started = new CountDownLatch(1);
    try {
      executor.execute(() -> {
        started.countDown();
        try {
          release.await();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      });
      assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();
      executor.execute(() -> {
      });
      assertThatThrownBy(() -> executor.execute(() -> {
      })).isInstanceOf(RejectedExecutionException.class);

      assertThat(policy.getSaturations()).isEqualTo(1);
      assertThat(PoolMetrics.statsOf(executor).callerRunFallbacks()).isZero();
    } finally {
      release.countDown();
      executor.shutdown();
      executor.awaitTermination(10, TimeUnit.SECONDS);
    }
  }

  /**
   * An unbounded queue's remaining capacity is {@code Integer.MAX_VALUE} minus what it holds, so a check against
   * {@code MAX_VALUE} alone reports an occupied unbounded queue as having two billion free slots (issue #8856, found
   * on the HA lifecycle worker, the first unbounded instance pool).
   */
  @Test
  void statsOfReportsAnOccupiedUnboundedQueueAsUnbounded() throws Exception {
    final ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<>());
    final CountDownLatch release = new CountDownLatch(1);
    final CountDownLatch started = new CountDownLatch(1);
    try {
      assertThat(PoolMetrics.statsOf(executor).queueCapacityRemaining()).isEqualTo(-1);
      executor.execute(() -> {
        started.countDown();
        try {
          release.await();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      });
      assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();
      executor.execute(() -> {
      });

      final PoolStats busy = PoolMetrics.statsOf(executor);
      assertThat(busy.queueDepth()).isEqualTo(1);
      assertThat(busy.queueCapacityRemaining()).isEqualTo(-1);
    } finally {
      release.countDown();
      executor.shutdown();
      executor.awaitTermination(10, TimeUnit.SECONDS);
    }
  }

  /**
   * Servers starting at once bind the same tag concurrently: exactly one binding must own the row, or either server
   * stopping would remove the row the other still publishes (review of PR #9417).
   */
  @Test
  void concurrentBindingsOfOneTagLeaveExactlyOneOwner() throws Exception {
    final int binders = 8;
    for (int round = 0; round < 50; round++) {
      final SimpleMeterRegistry registry = new SimpleMeterRegistry();
      final CountDownLatch go = new CountDownLatch(1);
      final List<Closeable> handles = Collections.synchronizedList(new ArrayList<>());
      final List<Thread> threads = new ArrayList<>();
      for (int i = 0; i < binders; i++) {
        final Thread t = new Thread(() -> {
          try {
            go.await();
            handles.add(PoolMetrics.bindInstancePool(registry, "racing_pool", "Racing pool",
                () -> new PoolStats(0, 0, 0, 1, 0, 0, 0), null, () -> 0L));
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        });
        threads.add(t);
        t.start();
      }
      go.countDown();
      for (final Thread t : threads)
        t.join(10_000);

      assertThat(handles).hasSize(binders);
      assertThat(handles.stream().filter(h -> h != PoolMetrics.NOTHING_OWNED).count()).as("owners in round %d", round)
          .isEqualTo(1);
    }
  }
}
