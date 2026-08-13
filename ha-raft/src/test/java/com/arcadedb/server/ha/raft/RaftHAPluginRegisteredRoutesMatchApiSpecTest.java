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

import com.arcadedb.server.http.RecordingPathHandler;
import com.arcadedb.server.http.RoutePathNormalizer;
import com.arcadedb.server.http.handler.openapi.PluginApiSpec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #4896: RaftHAPlugin holds arcadedb-server at provided scope, so it cannot declare its own
 * OpenAPI paths - PluginApiSpec declares them on its behalf, in the server module. This test fails
 * the moment a route is added to, removed from, or renamed in registerAPI() without a matching
 * update to PluginApiSpec.HA_RAFT_PATHS, in either direction.
 */
class RaftHAPluginRegisteredRoutesMatchApiSpecTest {

  private RaftHAPlugin plugin;

  @AfterEach
  void tearDown() {
    if (plugin != null)
      plugin.stopService();
  }

  @Test
  void registeredRoutesMatchTheDeclaredApiSpecPaths() {
    plugin = new RaftHAPlugin();
    final RecordingPathHandler routes = new RecordingPathHandler();

    plugin.registerAPI(null, routes);

    assertThat(routes.getRegisteredPaths())
        .as("RaftHAPlugin.registerAPI() and PluginApiSpec.HA_RAFT_PATHS have drifted apart")
        .containsExactlyInAnyOrderElementsOf(RoutePathNormalizer.normalize(PluginApiSpec.HA_RAFT_PATHS));
  }

  /**
   * Issue #4896's required self-test, exercised against the real production plugin and the real
   * recording handler: proves the anti-drift comparison genuinely fails when an undeclared route is
   * actually registered, rather than only proving AssertJ can compare two hand-written sets.
   */
  @Test
  void theAntiDriftCheckCatchesARouteThatIsNotDeclared() {
    plugin = new RaftHAPlugin();
    final RecordingPathHandler routes = new RecordingPathHandler();
    plugin.registerAPI(null, routes);
    routes.addExactPath("/api/v1/cluster/issue-4896-undeclared", (exchange) -> {
    });

    assertThatThrownBy(() -> assertThat(routes.getRegisteredPaths())
        .containsExactlyInAnyOrderElementsOf(RoutePathNormalizer.normalize(PluginApiSpec.HA_RAFT_PATHS)))
        .isInstanceOf(AssertionError.class);
  }
}
