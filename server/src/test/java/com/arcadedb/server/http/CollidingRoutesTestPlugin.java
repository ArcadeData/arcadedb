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
import io.undertow.util.Methods;

/**
 * Declares the route {@link PluginRoutesTestPlugin} already declares (issue #10385), and a route on a core path, so a test
 * can assert that a server holding it next to that plugin does not start. Which of the two claims it makes is chosen by
 * {@link #mode}, a static the test sets before the server starts.
 * <p>
 * Not auto-discovered: the routes appear only for a test that names this plugin in SERVER_PLUGINS.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class CollidingRoutesTestPlugin implements ServerPlugin {
  public enum Mode {
    SAME_ROUTE_AS_ANOTHER_PLUGIN, SHADOW_A_CORE_ROUTE, TAKE_OVER_THE_API_PREFIX, DISTINCT_METHOD_ON_ANOTHER_PLUGINS_PATH
  }

  public static volatile Mode mode = Mode.SAME_ROUTE_AS_ANOTHER_PLUGIN;

  @Override
  public void startService() {
    // NO-OP
  }

  @Override
  public void registerAPI(final HttpServer httpServer, final PluginRoutes routes) {
    switch (mode) {
    case SAME_ROUTE_AS_ANOTHER_PLUGIN -> routes.post(PluginRoutesTestPlugin.PATH, exchange -> exchange.getResponseSender().send("B"));
    case SHADOW_A_CORE_ROUTE -> routes.get("/api/v1/server", exchange -> exchange.getResponseSender().send("B"));
    case TAKE_OVER_THE_API_PREFIX -> routes.prefix(Methods.GET, "/api/v1", exchange -> exchange.getResponseSender().send("B"));
    case DISTINCT_METHOD_ON_ANOTHER_PLUGINS_PATH ->
        routes.get(PluginRoutesTestPlugin.PATH, exchange -> exchange.getResponseSender().send("B"));
    }
  }
}
