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
package com.arcadedb.server.http;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.StaticBaseServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10385, end to end: a server that holds two plugins claiming one route, or a plugin claiming a core route or the
 * {@code /api/v1} mount, does not start. The claim used to succeed silently, and the loser of the race answered a 405
 * naming the other plugin's method.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10385ServerRefusesCollidingPluginsTest extends StaticBaseServerTest {

  private ArcadeDBServer server;

  @AfterEach
  void stopServer() {
    CollidingRoutesTestPlugin.mode = CollidingRoutesTestPlugin.Mode.SAME_ROUTE_AS_ANOTHER_PLUGIN;
    if (server != null)
      server.stop();
  }

  @Test
  void twoPluginsDeclaringTheSameRouteFailTheStartNamingBoth() {
    CollidingRoutesTestPlugin.mode = CollidingRoutesTestPlugin.Mode.SAME_ROUTE_AS_ANOTHER_PLUGIN;

    assertThatThrownBy(this::startWithBothPlugins).isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("CollidingRoutesTestPlugin")
        .hasMessageContaining("PluginRoutesTestPlugin")
        .hasMessageContaining("POST " + PluginRoutesTestPlugin.PATH);
  }

  @Test
  void aPluginShadowingACoreRouteFailsTheStart() {
    CollidingRoutesTestPlugin.mode = CollidingRoutesTestPlugin.Mode.SHADOW_A_CORE_ROUTE;

    assertThatThrownBy(this::startWithBothPlugins).isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("CollidingRoutesTestPlugin")
        .hasMessageContaining("GET /api/v1/server");
  }

  @Test
  void aPluginTakingOverTheApiPrefixFailsTheStart() {
    CollidingRoutesTestPlugin.mode = CollidingRoutesTestPlugin.Mode.TAKE_OVER_THE_API_PREFIX;

    assertThatThrownBy(this::startWithBothPlugins).isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("CollidingRoutesTestPlugin")
        .hasMessageContaining("/api/v1");
  }

  @Test
  void anotherMethodOnAnotherPluginsPathIsServedAlongsideIt() throws Exception {
    CollidingRoutesTestPlugin.mode = CollidingRoutesTestPlugin.Mode.DISTINCT_METHOD_ON_ANOTHER_PLUGINS_PATH;

    startWithBothPlugins();

    // PluginRoutesTestPlugin declared POST, CollidingRoutesTestPlugin declared GET on the same path: both answer
    final String url = getServerHttpUrl(server, PluginRoutesTestPlugin.PATH);
    try (final HttpClient client = HttpClient.newHttpClient()) {
      final HttpResponse<String> get = client.send(HttpRequest.newBuilder(URI.create(url))
          .GET().build(), HttpResponse.BodyHandlers.ofString());
      assertThat(get.statusCode()).isEqualTo(200);
      assertThat(get.body()).isEqualTo("B");

      final HttpResponse<String> put = client.send(HttpRequest.newBuilder(URI.create(url))
          .PUT(HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.ofString());
      assertThat(put.statusCode()).isEqualTo(405);
      // in the order the plugins registered, whichever it was
      assertThat(put.headers().firstValue("Allow").orElseThrow().split(", ")).containsExactlyInAnyOrder("POST", "GET", "HEAD");
    }
  }

  private void startWithBothPlugins() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_NAME, "ArcadeDB_0");
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, "./target/databases");
    config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, "./target");
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, DEFAULT_PASSWORD_FOR_TESTS);
    config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_HOST, "localhost");
    config.setValue(GlobalConfiguration.SERVER_PLUGINS,
        PluginRoutesTestPlugin.class.getName() + "," + CollidingRoutesTestPlugin.class.getName());

    server = new ArcadeDBServer(config);
    server.start();
  }
}
