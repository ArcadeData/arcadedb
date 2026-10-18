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

import com.arcadedb.server.ServerPlugin;
import io.undertow.util.Headers;

/**
 * Declares its one route through {@link ServerPlugin#registerAPI(HttpServer, PluginRoutes)}, the overload issue #10296
 * added, and never sees the server's raw {@code PathHandler}. Pins that the server calls that overload and that the route
 * it declares is method-restricted end to end.
 * <p>
 * Not auto-discovered: the route appears only for a test that names this plugin in SERVER_PLUGINS.
 */
public class PluginRoutesTestPlugin implements ServerPlugin {
  public static final String PATH = "/api/v1/test/plugin-routes";

  @Override
  public void startService() {
    // NO-OP
  }

  @Override
  public void registerAPI(final HttpServer httpServer, final PluginRoutes routes) {
    routes.post(PATH, exchange -> {
      exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
      exchange.getResponseSender().send("{\"ok\":true}");
    });
  }
}
