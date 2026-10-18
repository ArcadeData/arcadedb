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
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.ServerPlugin;
import io.undertow.server.handlers.PathHandler;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10296: {@code ServerPlugin.registerAPI(HttpServer, PathHandler)} handed every plugin the server's raw
 * {@code PathHandler}, on which a route answers every HTTP method. The server now calls
 * {@link ServerPlugin#registerAPI(HttpServer, PluginRoutes)} first, so a plugin written against it can only declare
 * method-restricted routes; the deprecated {@code PathHandler} overload is still called after it, so a plugin built
 * against it keeps working and keeps being named.
 */
class Issue10296PluginRoutesRegisterApiTest extends BaseGraphServerTest {
  private static final HttpClient CLIENT = HttpClient.newHttpClient();

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    config.setValue(GlobalConfiguration.SERVER_PLUGINS,
        PluginRoutesTestPlugin.class.getName() + "," + ArrayPayloadTestPlugin.class.getName());
  }

  @Test
  void aRouteDeclaredThroughThePluginRoutesOverloadIsServedOnItsMethod() throws Exception {
    final HttpResponse<String> response = send("POST", PluginRoutesTestPlugin.PATH, "{}");

    assertThat(response.statusCode()).isEqualTo(200);
    assertThat(response.body()).isEqualTo("{\"ok\":true}");
  }

  @Test
  void anUndeclaredMethodOnThatRouteIsAnswered405WithAllow() throws Exception {
    for (final String method : new String[] { "GET", "PUT", "DELETE" }) {
      final HttpResponse<String> response = send(method, PluginRoutesTestPlugin.PATH, method.equals("GET") ? null : "{}");

      assertThat(response.statusCode()).as(method + " " + PluginRoutesTestPlugin.PATH).isEqualTo(405);
      assertThat(response.headers().firstValue("Allow")).as(method + " " + PluginRoutesTestPlugin.PATH).hasValue("POST");
    }
  }

  @Test
  void onlyTheLegacyPluginsRouteIsNamedUnrestricted() {
    assertThat(getServer(0).getHttpServer().getUnrestrictedPluginRoutes())
        .containsExactly("ArrayPayloadTestPlugin /api/v1/test/array");
  }

  @Test
  void aPluginStillImplementingTheDeprecatedOverloadIsStillServed() throws Exception {
    final HttpResponse<String> response = send("POST", "/api/v1/test/array", "[{\"id\":1}]");

    assertThat(response.statusCode()).isEqualTo(200);
  }

  @Test
  void theServerTellsWhichOverloadAPluginImplements() {
    assertThat(PluginPathHandler.implementsDeprecatedRegisterAPI(new ArrayPayloadTestPlugin())).isTrue();
    assertThat(PluginPathHandler.implementsDeprecatedRegisterAPI(new PluginRoutesTestPlugin())).isFalse();
    // A plugin with no HTTP routes at all overrides neither overload
    final ServerPlugin noRoutes = () -> {
    };
    assertThat(PluginPathHandler.implementsDeprecatedRegisterAPI(noRoutes)).isFalse();
    assertThat(PluginPathHandler.implementsPluginRoutesRegisterAPI(noRoutes)).isFalse();

    assertThat(PluginPathHandler.implementsPluginRoutesRegisterAPI(new PluginRoutesTestPlugin())).isTrue();
    assertThat(PluginPathHandler.implementsPluginRoutesRegisterAPI(new ArrayPayloadTestPlugin())).isFalse();
  }

  @Test
  void aPluginImplementingBothOverloadsIsDetectedAsSuch() {
    final ServerPlugin both = new BothOverloadsPlugin();

    assertThat(PluginPathHandler.implementsPluginRoutesRegisterAPI(both)).isTrue();
    assertThat(PluginPathHandler.implementsDeprecatedRegisterAPI(both)).isTrue();
  }

  @Test
  void anOverrideInheritedFromASuperclassIsStillDetected() {
    assertThat(PluginPathHandler.implementsDeprecatedRegisterAPI(new ArrayPayloadTestPlugin() {
    })).isTrue();
  }

  @Test
  void anOverrideInheritedFromASubInterfaceDefaultIsStillDetected() {
    final ServerPlugin plugin = new LegacySubInterface() {
      @Override
      public void startService() {
        // NO-OP
      }
    };

    assertThat(PluginPathHandler.implementsDeprecatedRegisterAPI(plugin)).isTrue();
    assertThat(PluginPathHandler.implementsPluginRoutesRegisterAPI(plugin)).isFalse();
  }

  @SuppressWarnings("removal")
  private interface LegacySubInterface extends ServerPlugin {
    @Override
    default void registerAPI(final HttpServer httpServer, final PathHandler routes) {
      // NO-OP
    }
  }

  @SuppressWarnings("removal")
  private static final class BothOverloadsPlugin implements ServerPlugin {
    @Override
    public void startService() {
      // NO-OP
    }

    @Override
    public void registerAPI(final HttpServer httpServer, final PluginRoutes routes) {
      // NO-OP
    }

    @Override
    public void registerAPI(final HttpServer httpServer, final PathHandler routes) {
      // NO-OP
    }
  }

  private HttpResponse<String> send(final String method, final String path, final String body) throws Exception {
    final HttpRequest request = HttpRequest.newBuilder(new URI(getServerHttpUrl(path)))
        .header("Authorization", "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
        .header("Content-Type", "application/json")
        .method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
        .build();
    return CLIENT.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
  }
}
