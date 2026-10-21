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
import io.undertow.server.handlers.PathHandler;
import io.undertow.util.HttpString;
import io.undertow.util.Methods;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * A {@link PathHandler} that records every path registered through {@code addExactPath}/
 * {@code addPrefixPath}, so a plugin's {@code registerAPI} can be exercised directly and its actual
 * routes compared against the paths declared for it in the OpenAPI spec (issue #4896's
 * per-plugin-module anti-drift check). Behaves identically to a plain {@code PathHandler} as a
 * router; recording is a side effect only.
 * <p>
 * The handler INSTANCES are recorded alongside the paths (issue #7861), so a test can also ask a question of the
 * handlers themselves - "does every route that blocks dispatch off the Undertow IO thread" - against the real
 * inventory {@code registerAPI} installs rather than a list somebody maintains by hand and forgets.
 */
public class RecordingPathHandler extends PathHandler {

  private final Map<String, HttpHandler> registered = new LinkedHashMap<>();

  @Override
  public synchronized PathHandler addExactPath(final String path, final HttpHandler handler) {
    registered.put(path, handler);
    return super.addExactPath(path, handler);
  }

  @Override
  public synchronized PathHandler addPrefixPath(final String path, final HttpHandler handler) {
    registered.put(path, handler);
    return super.addPrefixPath(path, handler);
  }

  public synchronized Set<String> getRegisteredPaths() {
    return new LinkedHashSet<>(registered.keySet());
  }

  /**
   * Every registered route's handler, keyed by the path it was registered at, in registration order. A path declared
   * through {@link PluginRoutes} is reported by the plugin handler behind its method restriction when every declared
   * method shares one handler (issue #10157), so a question asked of the handlers keeps reaching them. A path whose
   * methods have different handlers is reported by its {@link PluginRoutes.MethodRestrictedHandler}.
   */
  public synchronized Map<String, HttpHandler> getRegisteredHandlers() {
    final Map<String, HttpHandler> handlers = new LinkedHashMap<>();
    for (final Map.Entry<String, HttpHandler> entry : registered.entrySet())
      handlers.put(entry.getKey(), unwrap(entry.getValue()));
    return handlers;
  }

  /**
   * The {@code "METHOD path"} of every operation the registered routes answer, the method dimension of the anti-drift
   * check (issue #10157). A path declared through {@link PluginRoutes} contributes one entry per declared method. A
   * handler added with {@code addExactPath}/{@code addPrefixPath} directly answers every method, and contributes
   * {@code "* path"}, which no declared operation can match.
   */
  public synchronized Set<String> getRegisteredOperations() {
    final Set<String> operations = new LinkedHashSet<>();
    for (final Map.Entry<String, HttpHandler> entry : registered.entrySet()) {
      if (entry.getValue() instanceof PluginRoutes.MethodRestrictedHandler restricted)
        for (final String method : restricted.getAllowedMethods())
          operations.add(method + " " + entry.getKey());
      else
        operations.add("* " + entry.getKey());
    }
    return operations;
  }

  /**
   * The paths a plugin declared {@code GET} on that do not answer {@code HEAD} (issue #10384): RFC 9110 requires a server
   * that supports GET on a resource to support HEAD on it, and container healthchecks such as {@code wget --spider} send it.
   * {@link PluginRoutes} derives HEAD from the GET entry, so this is empty for every plugin that registers through it, and
   * the plugin conformance tests assert it. A handler added to the raw {@code PathHandler} answers every method and is
   * never listed.
   */
  public synchronized Set<String> getGetRoutesNotAnsweringHead() {
    final Set<String> missing = new LinkedHashSet<>();
    for (final Map.Entry<String, HttpHandler> entry : registered.entrySet())
      if (entry.getValue() instanceof PluginRoutes.MethodRestrictedHandler restricted
          && restricted.getHandler(Methods.GET) != null && restricted.getHandler(Methods.HEAD) == null)
        missing.add(entry.getKey());
    return missing;
  }

  private static HttpHandler unwrap(final HttpHandler handler) {
    if (!(handler instanceof PluginRoutes.MethodRestrictedHandler restricted))
      return handler;
    HttpHandler shared = null;
    for (final String method : restricted.getAllowedMethods()) {
      final HttpHandler candidate = restricted.getHandler(new HttpString(method));
      if (shared == null)
        shared = candidate;
      else if (shared != candidate)
        return handler;
    }
    return shared != null ? shared : handler;
  }
}
