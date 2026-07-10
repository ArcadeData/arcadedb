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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URI;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that enabling tracing with a malformed OTLP endpoint degrades gracefully: the
 * {@link TracingPlugin} catches the exporter-init failure, disables itself, and the server starts
 * and serves requests normally instead of failing startup.
 */
@Tag("slow")
class TracingBadEndpointIT extends BaseGraphServerTest {

  @Override
  protected int getServerCount() {
    return 1;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    config.setValue(GlobalConfiguration.SERVER_METRICS_TRACING_ENABLED, true);
    config.setValue(GlobalConfiguration.SERVER_METRICS_TRACING_ENDPOINT, "not-a-valid-url");
  }

  @Test
  void serverStartsDespiteInvalidTracingEndpoint() throws Exception {
    // If TracingPlugin.configure() did not catch the exporter-init failure, the server would have
    // failed to start and @BeforeEach would have thrown. Reaching here means it degraded gracefully.
    assertThat(getServer(0).isStarted()).isTrue();

    // And the server still serves requests with tracing silently disabled.
    final HttpURLConnection c = (HttpURLConnection) URI.create("http://localhost:2480/api/v1/ready").toURL().openConnection();
    c.setRequestMethod("GET");
    c.connect();
    assertThat(c.getResponseCode()).isEqualTo(204);
    c.disconnect();
  }
}
