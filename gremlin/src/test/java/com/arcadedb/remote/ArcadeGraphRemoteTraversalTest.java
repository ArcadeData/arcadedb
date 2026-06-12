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
package com.arcadedb.remote;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.gremlin.ArcadeGraph;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that ArcadeGraph.traversal() degrades to the embedded fallback when the remote cluster topology
 * is unknown (no leader and no replicas), instead of propagating a NullPointerException from
 * RemoteHttpComponent.getLeaderAddress(). Lives in com.arcadedb.remote so the stub can override the
 * package-private requestClusterConfiguration() and leave leaderServer null.
 */
class ArcadeGraphRemoteTraversalTest {

  /**
   * RemoteDatabase whose cluster configuration is never fetched, so leaderServer stays null and the
   * replica list stays empty - the exact state that triggered the #4550 NPE.
   */
  static class NoLeaderRemoteDatabase extends RemoteDatabase {
    NoLeaderRemoteDatabase() {
      super("localhost", 2480, "test", "root", "test", new ContextConfiguration());
    }

    @Override
    void requestClusterConfiguration() {
      // No-op: leave leaderServer null and replicaServerList empty.
    }
  }

  @Test
  void traversalFallsBackWhenNoLeaderAndNoReplicas() {
    final NoLeaderRemoteDatabase database = new NoLeaderRemoteDatabase();
    assertThat(database.getLeaderAddress()).isNull();
    assertThat(database.getReplicaAddresses()).isEmpty();

    // ArcadeGraph.close() closes the wrapped database, so the try-with-resources covers both.
    try (final ArcadeGraph graph = ArcadeGraph.open(database)) {
      final GraphTraversalSource source = graph.traversal();
      assertThat(source).isNotNull();
    }
  }
}
