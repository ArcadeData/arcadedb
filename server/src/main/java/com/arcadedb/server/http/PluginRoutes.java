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

import com.arcadedb.serializer.json.JSONObject;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.server.handlers.PathHandler;
import io.undertow.util.Headers;
import io.undertow.util.HttpString;
import io.undertow.util.Methods;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The method-keyed registrar a {@link com.arcadedb.server.ServerPlugin} declares its HTTP routes through (issue #10157).
 * <p>
 * {@code registerAPI} hands a plugin the server's outer {@link PathHandler}, which matches on the path alone: a handler
 * added with {@code addExactPath}/{@code addPrefixPath} answers EVERY method, so a route declared as {@code POST} was
 * also served on {@code GET}, {@code PUT}, {@code DELETE} and {@code HEAD} - and a {@code GET} is what link prefetchers,
 * crawlers and a browser replaying cached Basic credentials send. The core routes never had the problem, because
 * {@code HttpServer} registers them on a method-keyed {@code RoutingHandler}. This class gives plugin routes the same
 * guarantee: a route is declared with its method ({@code routes.post("/api/v1/cluster/stepdown", handler)}), and any
 * other method is answered {@code 405 Method Not Allowed} with an {@code Allow} header naming the declared ones, before
 * the plugin's handler runs.
 * <p>
 * Each path is registered on the underlying {@link PathHandler} once, as a {@link MethodRestrictedHandler} holding one
 * handler per declared method, so declaring a second method on a path already declared adds to the same entry. Exact and
 * prefix paths keep {@link PathHandler}'s own semantics: {@link #prefix} is for a route whose path carries a parameter
 * ({@code /api/v1/cluster/peer/{peerId}}), which {@link PathHandler} can only match as a prefix.
 * <p>
 * Registration is meant to happen once, from {@code registerAPI}, before the listener serves requests. It is not
 * synchronized against concurrent registration on the same instance.
 */
public final class PluginRoutes {
  private final PathHandler                          paths;
  private final Map<String, MethodRestrictedHandler> exactPaths  = new HashMap<>();
  private final Map<String, MethodRestrictedHandler> prefixPaths = new HashMap<>();

  public PluginRoutes(final PathHandler paths) {
    if (paths == null)
      throw new IllegalArgumentException("A plugin's routes need the PathHandler they are registered on");
    this.paths = paths;
  }

  /** Declares {@code GET} on the exact {@code path}. */
  public PluginRoutes get(final String path, final HttpHandler handler) {
    return exact(Methods.GET, path, handler);
  }

  /** Declares {@code POST} on the exact {@code path}. */
  public PluginRoutes post(final String path, final HttpHandler handler) {
    return exact(Methods.POST, path, handler);
  }

  /** Declares {@code PUT} on the exact {@code path}. */
  public PluginRoutes put(final String path, final HttpHandler handler) {
    return exact(Methods.PUT, path, handler);
  }

  /** Declares {@code DELETE} on the exact {@code path}. */
  public PluginRoutes delete(final String path, final HttpHandler handler) {
    return exact(Methods.DELETE, path, handler);
  }

  /** Declares {@code method} on the exact {@code path}, the {@link PathHandler#addExactPath} match. */
  public PluginRoutes exact(final HttpString method, final String path, final HttpHandler handler) {
    register(exactPaths, method, path, handler, false);
    return this;
  }

  /**
   * Declares {@code method} on every path under {@code prefix}, the {@link PathHandler#addPrefixPath} match. For a route
   * whose path carries a parameter, registered up to the slash before it.
   */
  public PluginRoutes prefix(final HttpString method, final String prefix, final HttpHandler handler) {
    register(prefixPaths, method, prefix, handler, true);
    return this;
  }

  /**
   * Replaces the default {@code 405} body for {@code path}, already declared through this instance, with
   * {@code rejection}: for a protocol that defines its own error envelope, such as JSON-RPC over MCP. The rejection runs
   * with the {@code 405} status and the {@code Allow} header already set; it writes the body, and must not act on the
   * request.
   */
  public PluginRoutes onMethodNotAllowed(final String path, final HttpHandler rejection) {
    MethodRestrictedHandler restricted = exactPaths.get(path);
    if (restricted == null)
      restricted = prefixPaths.get(path);
    if (restricted == null)
      throw new IllegalArgumentException("No route is declared at " + path);
    restricted.rejection = rejection;
    return this;
  }

  private void register(final Map<String, MethodRestrictedHandler> registered, final HttpString method, final String path,
      final HttpHandler handler, final boolean isPrefix) {
    if (method == null || path == null || handler == null)
      throw new IllegalArgumentException("A plugin route needs a method, a path and a handler");

    final MethodRestrictedHandler existing = registered.get(path);
    if (existing != null) {
      existing.allow(method, handler);
      return;
    }

    final MethodRestrictedHandler restricted = new MethodRestrictedHandler(path);
    restricted.allow(method, handler);
    registered.put(path, restricted);
    if (isPrefix)
      paths.addPrefixPath(path, restricted);
    else
      paths.addExactPath(path, restricted);
  }

  /**
   * The handler a {@link PluginRoutes} path is registered under: it dispatches to the handler declared for the request's
   * method, and answers {@code 405} with an {@code Allow} header for any other method without invoking a plugin handler.
   * Public so that a recording {@link PathHandler} in a test can report the methods a path actually answers.
   */
  public static final class MethodRestrictedHandler implements HttpHandler {
    private final    String                       path;
    // Copy-on-write: written while registerAPI runs, read by every request afterwards without a lock.
    private volatile Map<HttpString, HttpHandler> byMethod    = Map.of();
    private volatile String                       allowHeader = "";
    private volatile HttpHandler                  rejection;

    private MethodRestrictedHandler(final String path) {
      this.path = path;
    }

    private synchronized void allow(final HttpString method, final HttpHandler handler) {
      final Map<HttpString, HttpHandler> next = new LinkedHashMap<>(byMethod);
      next.put(method, handler);
      byMethod = Collections.unmodifiableMap(next);
      allowHeader = String.join(", ", getAllowedMethods());
    }

    @Override
    public void handleRequest(final HttpServerExchange exchange) throws Exception {
      final HttpHandler handler = byMethod.get(exchange.getRequestMethod());
      if (handler != null) {
        handler.handleRequest(exchange);
        return;
      }

      // Answered here, on whatever thread the request arrived on: nothing below blocks, and no plugin code runs. The
      // body names the registered path, never the request's, so nothing the caller sent is reflected back.
      final String allow = allowHeader;
      exchange.setStatusCode(405);
      exchange.getResponseHeaders().put(Headers.ALLOW, allow);
      final HttpHandler customRejection = rejection;
      if (customRejection != null) {
        customRejection.handleRequest(exchange);
        return;
      }
      exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
      exchange.getResponseSender().send(new JSONObject()//
          .put("error", "Method Not Allowed")//
          .put("detail", "The route " + path + " accepts " + allow + " only")//
          .toString());
    }

    /** The path this handler is registered at, exactly as declared. */
    public String getPath() {
      return path;
    }

    /** The declared methods, in declaration order. */
    public List<String> getAllowedMethods() {
      final List<String> methods = new ArrayList<>(byMethod.size());
      for (final HttpString method : byMethod.keySet())
        methods.add(method.toString());
      return methods;
    }

    /** The handler declared for {@code method}, or {@code null} when that method is answered 405. */
    public HttpHandler getHandler(final HttpString method) {
      return byMethod.get(method);
    }
  }
}
