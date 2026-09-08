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
package com.arcadedb.tracing;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end half of issue #7281 for tracing: a real server boot driven only by
 * {@code arcadedb.serverMetrics.tracing.enabled}, with nothing named in {@code arcadedb.server.plugins}. The
 * configured endpoint points nowhere on purpose - the plugin has to install regardless, exactly as
 * {@link TracingBadEndpointIT} pins for the export path.
 */
class Issue7281TracingPluginDiscoveryIT extends BaseGraphServerTest {

  @Override
  protected int getServerCount() {
    return 1;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    config.setValue(GlobalConfiguration.SERVER_METRICS_TRACING_ENABLED.getKey(), true);
    // This test never speaks HTTP - it asks the running server which plugins it installed. Give it a port range of
    // its own so it neither contends with, nor is answered by, anything already listening on the default 2480-2489.
    config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_PORT, "2590-2599");
  }

  @Test
  void theTracingPluginIsInstalledWithoutAServerPluginsEntry() {
    assertThat(GlobalConfiguration.SERVER_PLUGINS.getValueAsString()).doesNotContain("Tracing");

    assertThat(getServer(0).getPlugins())
        .filteredOn(plugin -> plugin instanceof TracingPlugin)
        .hasSize(1);
  }
}
