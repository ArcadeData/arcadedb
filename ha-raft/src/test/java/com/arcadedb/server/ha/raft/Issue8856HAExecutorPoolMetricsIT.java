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
package com.arcadedb.server.ha.raft;

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.handler.GetServerHandler;
import com.arcadedb.server.monitor.PoolMetrics;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8856, end to end: a started HA node publishes the rest of its per-instance executors as executor rows,
 * reaching both the Micrometer registry and the JSON Studio's "Executor Pools" card renders, each with the extra
 * gauges its saturation semantics give a meaning to and none of the others.
 * <p>
 * Every server of this in-process cluster shares one registry and the same meter ids, so the rows belong to the
 * first server that registered them - server 0 - and the readings are compared with its pools.
 */
class Issue8856HAExecutorPoolMetricsIT extends BaseRaftHATest {

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void everyHAExecutorIsPublishedWithTheGaugesItsPolicyGivesAMeaningTo() {
    final RaftHAServer raft = getRaftPlugin(0).getRaftHAServer();
    final ArcadeStateMachine stateMachine = raft.getStateMachine();

    final JSONObject executors = GetServerHandler.buildExecutorsJSON(Metrics.globalRegistry);
    for (final String pool : new String[] { "snapshot_install", "sm_lifecycle", "database_deleter", "channel_recovery",
        "stalled_resync" }) {
      assertThat(executors.has(pool)).as("Studio's card must receive the %s row", pool).isTrue();
      assertThat(executors.getJSONObject(pool).has("queue.depth")).isTrue();
      assertThat(executors.getJSONObject(pool).has("tasks.caller_run_fallbacks")).isTrue();
    }

    // abort pools: a refusal is loss, published as rejected
    assertThat(executors.getJSONObject("snapshot_install").has("tasks.rejected")).isTrue();
    assertThat(executors.getJSONObject("snapshot_install").has("tasks.coalesced")).isFalse();
    assertThat(executors.getJSONObject("channel_recovery").has("tasks.rejected")).isTrue();
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

  /**
   * Stopping the owner removes its rows, so a stopped node does not keep publishing pools that no longer exist - and,
   * since the siblings' bindings own nothing, the rows go even though they still run (the documented limitation).
   */
  @Test
  void stoppingTheOwnerRemovesItsRows() {
    assertThat(gauge("arcadedb.executor.pool.size", "channel_recovery")).isNotNull();

    getServer(0).stop();

    for (final String pool : new String[] { "snapshot_install", "sm_lifecycle", "database_deleter", "channel_recovery",
        "stalled_resync", "security_seed", "security_catch_up" })
      assertThat(gauge("arcadedb.executor.pool.size", pool)).as("the %s row must go with its owner", pool).isNull();
  }

  private static Gauge gauge(final String name, final String pool) {
    return Metrics.globalRegistry.find(name).tag("pool", pool).gauge();
  }
}
