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

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8856, end to end: the executor rows a started HA node publishes go with the instance that owns them.
 * <p>
 * Every server of this in-process cluster shares one registry and the same meter ids, so the rows belong to the first
 * server that registered them - server 0.
 * <p>
 * Sibling of {@link HaExecutorMetricsIT}, merged from {@code Issue8856HAExecutorPoolMetricsIT}. It keeps a fresh cluster
 * per test ({@link BaseRaftHATest}): three servers, no configuration override, and the test stops server 0 for good.
 */
class HaExecutorMetricsDisruptiveIT extends BaseRaftHATest {

  @Override
  protected int getServerCount() {
    return 3;
  }

  /**
   * Issue #8856, from {@code Issue8856HAExecutorPoolMetricsIT}.
   * <p>
   * Stopping the owner removes its rows, so a stopped node does not keep publishing pools that no longer exist - and,
   * since the siblings' bindings own nothing, the rows go even though they still run (the documented limitation).
   */
  @Test
  void stoppingTheOwnerRemovesItsRows() {
    assertThat(gauge("arcadedb.executor.pool.size", "channel_recovery")).isNotNull();

    getServer(0).stop();

    for (final String pool : new String[] { "snapshot_install", "sm_lifecycle", "database_deleter", "channel_recovery",
        "stalled_resync", "self_dial_probe", "security_seed", "security_catch_up" })
      assertThat(gauge("arcadedb.executor.pool.size", pool)).as("the %s row must go with its owner", pool).isNull();
  }

  private static Gauge gauge(final String name, final String pool) {
    return Metrics.globalRegistry.find(name).tag("pool", pool).gauge();
  }
}
