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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9554: {@code ArcadeDBServer.stopInternal()} stops the HA plugin before the HTTP listener, and
 * {@code RaftHAPlugin.stopService()} dropped its Raft server - the only place it read the cluster token from. For the
 * rest of the shutdown every peer's forward that still reached the node, the node it believed to be the leader, was
 * answered 401 "Invalid cluster token" rather than the retryable refusal a node that no longer leads gives.
 */
class Issue9554ClusterTokenRetainedAfterStopTest {

  @Test
  void theClusterTokenOutlivesThePluginsRaftServer() {
    final RaftHAPlugin plugin = new RaftHAPlugin();
    plugin.setRaftHAServer(FakeRaftHAServer.detached().clusterToken("issue9554-token"));
    assertThat(plugin.getClusterToken()).isEqualTo("issue9554-token");

    plugin.stopService();

    assertThat(plugin.getRaftHAServer()).as("the Raft server is gone once the plugin stops").isNull();
    assertThat(plugin.getClusterToken())
        .as("the token peers authenticate with does not change when this node stops, so neither may the answer")
        .isEqualTo("issue9554-token");

    // stopService() runs twice on every HA shutdown (issue #5890): the second call must not lose it either.
    plugin.stopService();
    assertThat(plugin.getClusterToken()).isEqualTo("issue9554-token");
  }

  @Test
  void aPluginThatNeverStartedHasNoToken() {
    assertThat(new RaftHAPlugin().getClusterToken()).isNull();
  }
}
