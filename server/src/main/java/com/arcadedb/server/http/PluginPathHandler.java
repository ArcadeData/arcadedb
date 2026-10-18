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

import com.arcadedb.log.LogManager;
import io.undertow.server.HttpHandler;
import io.undertow.server.handlers.PathHandler;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

/**
 * The outer {@link PathHandler} {@code HttpServer} routes through, which notices a plugin route registered without a
 * method restriction (issue #10157).
 * <p>
 * {@code ServerPlugin.registerAPI} keeps handing plugins a {@link PathHandler}, so a plugin built against it keeps
 * working. But a handler added to it directly answers every HTTP method, which turned {@code GET} into a way to run
 * state-changing cluster operations. Every plugin in this repository declares its routes through {@link PluginRoutes};
 * a plugin that does not is named here, once per path, at {@code WARNING}, so the gap is visible at startup rather than
 * in a code review. Routing is unchanged: the route is served as registered.
 */
final class PluginPathHandler extends PathHandler {
  private final List<String> unrestricted = new ArrayList<>();
  private       String       registeringPlugin;

  /** Marks the routes added from now on as {@code plugin}'s, or as the server's own when {@code null}. */
  synchronized void setRegisteringPlugin(final String plugin) {
    this.registeringPlugin = plugin;
  }

  @Override
  public synchronized PathHandler addExactPath(final String path, final HttpHandler handler) {
    audit(path, handler);
    return super.addExactPath(path, handler);
  }

  @Override
  public synchronized PathHandler addPrefixPath(final String path, final HttpHandler handler) {
    audit(path, handler);
    return super.addPrefixPath(path, handler);
  }

  /** The {@code plugin path} of every plugin route registered without a method restriction, in registration order. */
  synchronized List<String> getUnrestrictedPluginRoutes() {
    return List.copyOf(unrestricted);
  }

  private void audit(final String path, final HttpHandler handler) {
    if (registeringPlugin == null || handler instanceof PluginRoutes.MethodRestrictedHandler)
      return;
    unrestricted.add(registeringPlugin + " " + path);
    LogManager.instance().log(this, Level.WARNING,
        "Plugin '%s' registered the HTTP route %s without declaring its method, so it answers every method (GET included). "
            + "Declare it through PluginRoutes instead", registeringPlugin, path);
  }
}
