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
 * Issue #7856, end to end: a started HA node publishes its two security workers as executor rows, reaching both
 * the Micrometer registry and the JSON Studio's "Executor Pools" card renders, and each row reads the worker of
 * the state machine that is running now.
 * <p>
 * Every server of this in-process cluster shares one registry and the same meter ids, so the rows belong to the
 * first server that registered them - server 0, the first one started - and the readings are compared with its
 * workers.
 */
class Issue7856SecurityWorkerPoolMetricsIT extends BaseRaftHATest {

  @Override
  protected int getServerCount() {
    return 3;
  }

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

  private static Gauge coalesced(final String pool) {
    return Metrics.globalRegistry.find(PoolMetrics.COALESCED_GAUGE).tag("pool", pool).gauge();
  }
}
