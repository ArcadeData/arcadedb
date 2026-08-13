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
package com.arcadedb.metrics.prometheus;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.server.http.RecordingPathHandler;
import com.arcadedb.server.http.RoutePathNormalizer;
import com.arcadedb.server.http.handler.openapi.PluginApiSpec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #4896: same structural gap as RaftHAPlugin - PrometheusMetricsPlugin holds arcadedb-server
 * at provided scope, so PluginApiSpec.METRICS_PATHS declares its route on its behalf, in the server
 * module. This test fails if the two ever drift apart.
 */
class PrometheusMetricsPluginRegisteredRoutesMatchApiSpecTest {

  private PrometheusMetricsPlugin plugin;

  @AfterEach
  void tearDown() {
    if (plugin != null)
      plugin.stopService();
  }

  @Test
  void registeredRoutesMatchTheDeclaredApiSpecPaths() {
    plugin = new PrometheusMetricsPlugin();
    // SERVER_METRICS defaults to true, so a default ContextConfiguration enables the plugin exactly
    // as a production server would.
    plugin.configure(null, new ContextConfiguration());
    final RecordingPathHandler routes = new RecordingPathHandler();

    plugin.registerAPI(null, routes);

    assertThat(routes.getRegisteredPaths())
        .as("PrometheusMetricsPlugin.registerAPI() and PluginApiSpec.METRICS_PATHS have drifted apart")
        .containsExactlyInAnyOrderElementsOf(RoutePathNormalizer.normalize(PluginApiSpec.METRICS_PATHS));
  }
}
