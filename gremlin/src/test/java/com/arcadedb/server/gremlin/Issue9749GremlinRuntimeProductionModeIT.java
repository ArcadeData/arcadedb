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
package com.arcadedb.server.gremlin;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.ServerControlPlane;
import org.apache.tinkerpop.gremlin.server.channel.WsAndHttpChannelizer;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9749, end to end: a server started in development mode whose Gremlin Server runs with {@code WsAndHttpChannelizer}
 * (which answers with the engine's own text) refuses {@code set server setting arcadedb.server.mode production}, and stays
 * in development mode, instead of reporting production mode while its Gremlin port keeps leaking.
 */
class Issue9749GremlinRuntimeProductionModeIT extends AbstractGremlinServerIT {
  private final HttpClient client = HttpClient.newHttpClient();

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.SERVER_MODE, "development");
    config.setValue("gremlin.channelizer", WsAndHttpChannelizer.class.getName());
  }

  @Test
  void httpSwitchToProductionIsRefused() throws Exception {
    final HttpResponse<String> response = executeServerCommand("set server setting arcadedb.server.mode production");

    assertThat(response.statusCode()).isEqualTo(400);
    assertThat(response.body()).contains(WsAndHttpChannelizer.class.getName());
    assertThat(getServer(0).isProductionMode()).isFalse();
    assertThat(getServer(0).getMode()).isEqualTo("development");
  }

  @Test
  void controlPlaneSwitchToProductionIsRefused() {
    // The path gRPC's SetServerSetting takes
    assertThatThrownBy(() -> new ServerControlPlane(getServer(0)).setServerSetting("arcadedb.server.mode", "production"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(WsAndHttpChannelizer.class.getName());
    assertThat(getServer(0).isProductionMode()).isFalse();
  }

  private HttpResponse<String> executeServerCommand(final String command) throws Exception {
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI(getServerHttpUrl("/api/v1/server")))
        .POST(HttpRequest.BodyPublishers.ofString(new JSONObject().put("command", command).toString()))
        .setHeader("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
        .build();
    return client.send(request, HttpResponse.BodyHandlers.ofString());
  }
}
