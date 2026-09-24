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

package com.arcadedb.containers.ha.chaos;

import java.time.Duration;

/**
 * Everything a fault can do to the cluster. The container-backed implementation lives in {@code HaChaosIT}; tests use
 * {@code FakeNodeControl}.
 */
public interface NodeControl {
  void kill(int node) throws Exception;

  void stopGracefully(int node) throws Exception;

  /** Starts a stopped or killed node and returns once it answers its health check. */
  void start(int node) throws Exception;

  void pause(int node) throws Exception;

  void unpause(int node) throws Exception;

  void disconnect(int node) throws Exception;

  void reconnect(int node) throws Exception;

  /** Adds latency to the outbound Raft traffic of the node through its proxy. */
  void addLatency(int node, int latencyMs, int jitterMs) throws Exception;

  /** Drops the outbound Raft traffic of the node through its proxy with the given probability. */
  void addLoss(int node, float toxicity) throws Exception;

  void clearToxics(int node) throws Exception;

  /** @return the index of the current leader, or -1 when no node reports being leader */
  int findLeader();

  /** @return true when a leader was elected and every node knows it within the timeout */
  boolean awaitLeader(Duration timeout);

  /**
   * @return what each node reports about the leader right now, one entry per node (for example
   * {@code "node 0: HTTP 200 leader=proxy:8671; node 1: connect failed on host:32918 (Connection refused)"}), so a
   * failed {@link #awaitLeader} names the node and the reason
   */
  String leaderView();

  /**
   * @return null when every node the state believes UP or DEGRADED is actually running, otherwise a description of the
   * node that exited on its own (for example {@code "node 1 exited unexpectedly: exitCode=137 OOMKilled=true"})
   */
  String unexpectedExit(ClusterState state);
}
