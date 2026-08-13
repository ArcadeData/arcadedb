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
package com.arcadedb.server.http.handler.openapi;

import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Paths;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * PluginApiSpec.HA_RAFT_PATHS/METRICS_PATHS and McpApiSpec.MCP_PATHS are literal duplicates of the
 * paths each class's contribute() method declares - kept separate rather than derived, because
 * contribute() pairs each literal with a specific PathItem builder method and iterating a Set would
 * lose that pairing. Asking the next editor to change both is not enforcement; this is. It fails the
 * moment contribute() and the constants diverge (issue #4896).
 */
class ApiSpecPathConstantsTest {

  @Test
  void pluginApiSpecConstantsMatchWhatContributeDeclares() {
    final OpenAPI openAPI = new OpenAPI();
    openAPI.setPaths(new Paths());
    openAPI.setComponents(new Components());
    new PluginApiSpec().contribute(openAPI);

    final Set<String> expected = new HashSet<>(PluginApiSpec.HA_RAFT_PATHS);
    expected.addAll(PluginApiSpec.METRICS_PATHS);

    assertThat(openAPI.getPaths().keySet())
        .as("PluginApiSpec.contribute() and its HA_RAFT_PATHS/METRICS_PATHS constants have drifted apart")
        .containsExactlyInAnyOrderElementsOf(expected);
  }

  @Test
  void mcpApiSpecConstantsMatchWhatContributeDeclares() {
    final OpenAPI openAPI = new OpenAPI();
    openAPI.setPaths(new Paths());
    openAPI.setComponents(new Components());
    new McpApiSpec().contribute(openAPI);

    assertThat(openAPI.getPaths().keySet())
        .as("McpApiSpec.contribute() and its MCP_PATHS constant have drifted apart")
        .containsExactlyInAnyOrderElementsOf(McpApiSpec.MCP_PATHS);
  }
}
