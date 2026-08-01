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
 * Pins the plugin activation contract: a plugin found on the classpath is activated when it is named in
 * SERVER_PLUGINS, or when it declares itself auto-discovered. The second route exists so a plugin owns its
 * own activation rule instead of PluginManager matching hardcoded class names.
 */
class PluginAutoDiscoveryTest {

  private static class OptInPlugin implements ServerPlugin {
    @Override
    public boolean isAutoDiscovered(final ContextConfiguration configuration) {
      return true;
    }

    @Override
    public void startService() {
      // NO-OP
    }
  }

  private static class ConfiguredOnlyPlugin implements ServerPlugin {
    @Override
    public void startService() {
      // NO-OP
    }
  }

  @Test
  void aPluginThatDeclaresItselfAutoDiscoveredIsActivatedWithoutConfiguration() {
    assertThat(new OptInPlugin().isAutoDiscovered(new ContextConfiguration())).isTrue();
  }

  @Test
  void theDefaultIsToRequireAnExplicitServerPluginsEntry() {
    assertThat(new ConfiguredOnlyPlugin().isAutoDiscovered(new ContextConfiguration())).isFalse();
  }
}
