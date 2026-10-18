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

import io.undertow.server.HttpHandler;
import io.undertow.util.Methods;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10157: {@code ServerPlugin.registerAPI} still hands a plugin the raw {@code PathHandler}, so a plugin built
 * against it keeps working - and keeps answering every method on a route it registers there directly. The server's
 * outer handler names every such route instead of letting it pass unseen.
 */
class Issue10157PluginPathHandlerTest {

  private static final HttpHandler HANDLER = exchange -> {
  };

  @Test
  void aRouteAPluginAddsDirectlyIsNamed() {
    final PluginPathHandler paths = new PluginPathHandler();
    paths.setRegisteringPlugin("LegacyPlugin");
    paths.addExactPath("/api/v1/legacy", HANDLER);
    paths.addPrefixPath("/api/v1/legacy/items/", HANDLER);
    paths.setRegisteringPlugin(null);

    assertThat(paths.getUnrestrictedPluginRoutes())
        .containsExactly("LegacyPlugin /api/v1/legacy", "LegacyPlugin /api/v1/legacy/items/");
  }

  @Test
  void aRouteDeclaredThroughPluginRoutesIsNot() {
    final PluginPathHandler paths = new PluginPathHandler();
    paths.setRegisteringPlugin("WellBehavedPlugin");
    new PluginRoutes(paths).post("/api/v1/good", HANDLER).prefix(Methods.DELETE, "/api/v1/good/", HANDLER);
    paths.setRegisteringPlugin(null);

    assertThat(paths.getUnrestrictedPluginRoutes()).isEmpty();
  }

  @Test
  void theServersOwnRoutesAreNotAPluginsConcern() {
    final PluginPathHandler paths = new PluginPathHandler();
    // HttpServer registers /ws, /api/v1 and / itself, outside any plugin, onto method-keyed routers or raw handlers
    paths.addPrefixPath("/ws", HANDLER);

    assertThat(paths.getUnrestrictedPluginRoutes()).isEmpty();
  }
}
