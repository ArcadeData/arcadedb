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
package com.arcadedb.mcp;

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The MCP endpoint must keep answering on a server that names no plugin at all, which is what an existing
 * installation looks like after upgrading to a distribution where MCP is a separate module. Excluding the
 * module from the build is the only thing that removes the endpoint.
 */
class MCPPluginDiscoveryTest extends BaseGraphServerTest {

  @Test
  void theMcpPluginIsInstalledWithoutAServerPluginsEntry() {
    assertThat(MCPPlugin.of(getServer(0))).isNotNull();
  }

  @Test
  void theConfigurationIsLoadedByTheTimeTheServerIsUp() {
    assertThat(MCPPlugin.of(getServer(0)).getConfiguration()).isNotNull();
  }

  @Test
  void theConfigRouteAnswersOnADefaultServer() throws Exception {
    final HttpRequest request = HttpRequest.newBuilder(
            new URI("http://127.0.0.1:" + getServer(0).getHttpServer().getPort() + "/api/v1/mcp/config"))
        .header("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
        .GET()
        .build();

    final HttpResponse<String> response = HttpClient.newHttpClient()
        .send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));

    assertThat(response.statusCode()).isEqualTo(200);
    assertThat(new JSONObject(response.body()).has("enabled")).isTrue();
  }
}
