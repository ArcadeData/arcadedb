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
package com.arcadedb.server.plugin;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.server.ServerPlugin;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins one thing only: the {@code ServerPlugin.isAutoDiscovered} default answers false, so a plugin that does
 * not override it stays gated behind an explicit SERVER_PLUGINS entry. The default carries the whole weight
 * here - flipping it would silently activate every ServerPlugin implementation that happens to be on the
 * classpath, including test-only plugins and RaftHAPlugin.
 * <p>
 * This does NOT cover PluginManager honouring an opt-in: activation depends on what the running classpath
 * offers to ServiceLoader, which cannot be arranged from inside the server module without registering a
 * genuinely auto-discovered plugin in the server's own META-INF/services - and that would then start in every
 * server test and in every module that consumes the server test-jar. The end-to-end route is covered where a
 * real auto-discovered plugin ships, by {@code com.arcadedb.mcp.MCPPluginDiscoveryTest} in the arcadedb-mcp
 * module.
 */
class ServerPluginAutoDiscoveryDefaultTest {

  private static class ConfiguredOnlyPlugin implements ServerPlugin {
    @Override
    public void startService() {
      // NO-OP
    }
  }

  @Test
  void theDefaultIsToRequireAnExplicitServerPluginsEntry() {
    assertThat(new ConfiguredOnlyPlugin().isAutoDiscovered(new ContextConfiguration())).isFalse();
  }
}
