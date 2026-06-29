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

/**
 * Framework-agnostic source of High-Availability replication health, implemented by the HA server
 * plugin and translated into Micrometer gauges by {@link HAReplicationMetrics}. Kept in the server
 * module (which has no compile dependency on the ha-raft module) so the binder can discover the HA
 * plugin via {@code instanceof} without coupling to Raft types, mirroring how {@link PoolMetrics}
 * translates the engine's framework-agnostic {@code PoolStats} records.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public interface HAReplicationStatsProvider {
  /**
   * Snapshot of the leader's view of replication health.
   *
   * @param leader                    whether this node is currently the Raft leader (the only role
   *                                  for which the follower fields are meaningful)
   * @param maxFollowerLastContactMs  worst (largest) time, in milliseconds, since the leader last
   *                                  successfully exchanged an RPC with any follower. This is the
   *                                  leading indicator of imminent election churn: when it approaches
   *                                  {@code arcadedb.ha.electionTimeoutMin}, a follower is about to
   *                                  start a new election. {@code -1} when not leader or unknown.
   * @param maxFollowerReplicationLag worst (largest) number of committed entries a follower is behind
   *                                  the leader's commit index. {@code -1} when not leader or unknown.
   * @param trackedFollowers          number of followers the leader is currently tracking
   *                                  ({@code 0} when not leader)
   */
  record HAReplicationStats(boolean leader, long maxFollowerLastContactMs, long maxFollowerReplicationLag,
                            int trackedFollowers) {
  }

  /**
   * Returns a live snapshot of replication health. Called on each metrics scrape, so implementations
   * must be cheap and non-blocking.
   */
  HAReplicationStats getHAReplicationStats();
}
