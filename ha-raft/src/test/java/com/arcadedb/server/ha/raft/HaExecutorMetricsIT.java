/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.server.ha.raft;

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.handler.GetServerHandler;
import com.arcadedb.server.monitor.PoolMetrics;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A started HA node publishes its per-instance executors as executor rows, reaching both the Micrometer registry and the
 * JSON Studio's "Executor Pools" card renders, each row reading the pool of the instance running now. End to end on ONE
 * three-node cluster shared by every test ({@link SharedClusterRaftHATest}). Merged from
 * {@code Issue7856SecurityWorkerPoolMetricsIT} and the first test of {@code Issue8856HAExecutorPoolMetricsIT}, which ran
 * on the same fixture: three servers, in-memory Raft storage, no configuration override.
 * <ul>
 * <li><b>#7856</b>: the two security workers (membership seed, security catch-up) are published.</li>
 * <li><b>#8856</b>: the rest of the HA executors are published, each with the extra gauges its saturation semantics give
 * a meaning to and none of the others.</li>
 * </ul>
 * Every server of this in-process cluster shares one registry and the same meter ids, so the rows belong to the first
 * server that registered them - server 0, the first one started - and the readings are compared with its pools.
 * <p>
 * Why these share a cluster: both tests only read the registry and the state of server 0, and assert nothing about a
 * count that another test could move. No test stops, restarts or isolates a node, moves leadership or changes a setting.
 * <p>
 * Sibling: {@link HaExecutorMetricsDisruptiveIT} (#8856's test that stops server 0, the owner of the rows).
 */
class HaExecutorMetricsIT extends SharedClusterRaftHATest {

  @Override
  protected int getServerCount() {
    return 3;
  }

  /** Issue #7856, from {@code Issue7856SecurityWorkerPoolMetricsIT}. */
  @Test
  void bothSecurityWorkersArePublishedAsExecutorRows() {
    final ArcadeStateMachine stateMachine = getRaftPlugin(0).getRaftHAServer().getStateMachine();

    final Gauge seedCoalesced = coalesced("security_seed");
    assertThat(seedCoalesced).as("the membership seed worker must have an executor row").isNotNull();
    assertThat(seedCoalesced.value())
        .isEqualTo((double) stateMachine.getMembershipSecuritySeeder().getCoalescedSeeds());

    final Gauge catchUpCoalesced = coalesced("security_catch_up");
    assertThat(catchUpCoalesced).as("the security catch-up worker must have an executor row").isNotNull();
    assertThat(catchUpCoalesced.value())
        .isEqualTo((double) stateMachine.getSecurityCatchUp().getCoalescedRequests());

    assertThat(Metrics.globalRegistry.find("arcadedb.executor.queue.capacity_remaining").tag("pool", "security_seed")
        .gauge().value()).as("the seed worker's single slot").isEqualTo(1.0);

    final JSONObject executors = GetServerHandler.buildExecutorsJSON(Metrics.globalRegistry);
    for (final String pool : new String[] { "security_refresh", "security_seed", "security_catch_up" }) {
      assertThat(executors.has(pool)).as("Studio's card must receive the %s row", pool).isTrue();
      assertThat(executors.getJSONObject(pool).has("tasks.coalesced")).isTrue();
      assertThat(executors.getJSONObject(pool).has("queue.depth")).isTrue();
    }
  }

  /** Issue #8856, from {@code Issue8856HAExecutorPoolMetricsIT}. */
  @Test
  void everyHAExecutorIsPublishedWithTheGaugesItsPolicyGivesAMeaningTo() {
    final RaftHAServer raft = getRaftPlugin(0).getRaftHAServer();
    final ArcadeStateMachine stateMachine = raft.getStateMachine();

    final JSONObject executors = GetServerHandler.buildExecutorsJSON(Metrics.globalRegistry);
    for (final String pool : new String[] { "snapshot_install", "sm_lifecycle", "database_deleter", "channel_recovery",
        "stalled_resync", "self_dial_probe" }) {
      assertThat(executors.has(pool)).as("Studio's card must receive the %s row", pool).isTrue();
      assertThat(executors.getJSONObject(pool).has("queue.depth")).isTrue();
      assertThat(executors.getJSONObject(pool).has("tasks.caller_run_fallbacks")).isTrue();
    }

    // abort pools: a refusal is loss, published as rejected
    assertThat(executors.getJSONObject("snapshot_install").has("tasks.rejected")).isTrue();
    assertThat(executors.getJSONObject("snapshot_install").has("tasks.coalesced")).isFalse();
    assertThat(executors.getJSONObject("channel_recovery").has("tasks.rejected")).isTrue();
    assertThat(executors.getJSONObject("self_dial_probe").has("tasks.rejected")).isTrue();
    assertThat(executors.getJSONObject("self_dial_probe").has("tasks.coalesced")).isFalse();
    // ...and the channel-recovery pool also skips a #8491 hand-off already queued: coalesced
    assertThat(executors.getJSONObject("channel_recovery").has("tasks.coalesced")).isTrue();
    // caller-runs or unbounded pools: neither extra applies
    for (final String pool : new String[] { "sm_lifecycle", "database_deleter", "stalled_resync" }) {
      assertThat(executors.getJSONObject(pool).has("tasks.rejected")).as(pool).isFalse();
      assertThat(executors.getJSONObject(pool).has("tasks.coalesced")).as(pool).isFalse();
    }

    // each row reads the live owner of server 0
    assertThat(gauge("arcadedb.executor.queue.capacity_remaining", "sm_lifecycle").value()).as("unbounded")
        .isEqualTo(-1.0);
    assertThat(gauge("arcadedb.executor.queue.capacity_remaining", "snapshot_install").value())
        .isEqualTo((double) stateMachine.getSnapshotInstallPoolStats().queueCapacityRemaining());
    assertThat(gauge(PoolMetrics.REJECTED_GAUGE, "snapshot_install").value())
        .isEqualTo((double) stateMachine.getSnapshotInstallRejections());
    assertThat(gauge("arcadedb.executor.queue.capacity_remaining", "channel_recovery").value())
        .isEqualTo((double) raft.getChannelRecoveryPoolStats().queueCapacityRemaining());
    assertThat(gauge("arcadedb.executor.queue.capacity_remaining", "stalled_resync").value())
        .isEqualTo((double) raft.getStalledResyncPoolStats().queueCapacityRemaining());
    assertThat(gauge("arcadedb.executor.queue.capacity_remaining", "database_deleter").value())
        .isEqualTo((double) stateMachine.getDatabaseDeleterPoolStats().queueCapacityRemaining());
  }

  /** #7856. */
  private static Gauge coalesced(final String pool) {
    return Metrics.globalRegistry.find(PoolMetrics.COALESCED_GAUGE).tag("pool", pool).gauge();
  }

  /** #8856. */
  private static Gauge gauge(final String name, final String pool) {
    return Metrics.globalRegistry.find(name).tag("pool", pool).gauge();
  }
}
