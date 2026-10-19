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
 * The method-keyed registrar a {@link com.arcadedb.server.ServerPlugin} declares its HTTP routes through (issue #10157),
 * handed to it by {@code ServerPlugin.registerAPI(HttpServer, PluginRoutes)} (issue #10296).
 * <p>
 * The deprecated {@code registerAPI(HttpServer, PathHandler)} hands a plugin the server's outer {@link PathHandler},
 * which matches on the path alone: a handler added with {@code addExactPath}/{@code addPrefixPath} answers EVERY method,
 * so a route declared as {@code POST} was also served on {@code GET}, {@code PUT}, {@code DELETE} and {@code HEAD} - and
 * a {@code GET} is what link prefetchers, crawlers and a browser replaying cached Basic credentials send. The core routes
 * never had the problem, because {@code HttpServer} registers them on a method-keyed {@code RoutingHandler}. This class
 * gives plugin routes the same guarantee: a route is declared with its method
 * ({@code routes.post("/api/v1/cluster/stepdown", handler)}), and any other method is answered
 * {@code 405 Method Not Allowed} with an {@code Allow} header naming the declared ones, before the plugin's handler runs.
 * <p>
 * A route declared {@code GET} answers {@code HEAD} too, with the same handler (issue #10384): RFC 9110 requires a server
 * that supports GET on a resource to support HEAD on it, container healthchecks such as {@code wget --spider} send it, and
 * a plugin route matched on the path alone answered it before this class existed. Undertow discards the body of the answer
 * at the connector, so the status and the headers are the ones GET would have returned. A plugin that declares
 * {@code HEAD} itself keeps its own handler, and the derived method is never reported as declared.
 * <p>
 * Each path is registered on the underlying {@link PathHandler} once, as a {@link MethodRestrictedHandler} holding one
 * handler per declared method, so declaring a second method on a path already declared adds to the same entry. On the
 * server that holds across plugins (issue #10385): the entry lives in a table the server's {@code PluginPathHandler} shares
 * with every {@code PluginRoutes} it hands out, so a second plugin declaring ANOTHER method on a path merges into it, where
 * a table per instance made it replace the first plugin's entry and answer its route with a 405 naming the other plugin's
 * method. Declaring a method a plugin already declared on that path, or a path that would shadow a route the server
 * registered for itself, is refused with an {@link IllegalStateException} naming both parties, which fails the start of the
 * server instead of leaving a route that silently does not exist. Exact and prefix paths keep {@link PathHandler}'s own
 * semantics: {@link #prefix} is for a route whose path carries a parameter ({@code /api/v1/cluster/peer/{peerId}}), which
 * {@link PathHandler} can only match as a prefix.
 * <p>
 * Registration is meant to happen once, from {@code registerAPI}, before the listener serves requests. Every read and write of
 * the shared table, and the registration on the {@link PathHandler} that goes with it, happens under one lock on the table, so
 * plugins registering from different threads cannot interleave a lookup and the entry it creates.
 */
public final class PluginRoutes {
  private static final String UNNAMED_PLUGIN = "a plugin";

  private final PathHandler       paths;
  private final PluginPathHandler serverPaths;
  private final RouteTable        table;

  public PluginRoutes(final PathHandler paths) {
    if (paths == null)
      throw new IllegalArgumentException("A plugin's routes need the PathHandler they are registered on");
    this.paths = paths;
    // On the server the table is the one every plugin shares; anywhere else (a test over a plain PathHandler) it is private
    this.serverPaths = paths instanceof PluginPathHandler server ? server : null;
    this.table = serverPaths != null ? serverPaths.getRouteTable() : new RouteTable();
  }

  /**
   * Declares {@code GET} on the exact {@code path}, which answers {@code HEAD} too (issue #10384). {@code HEAD} runs this
   * handler in full and the connector discards the body, so a handler that does heavy work for a probe to hit declares
   * {@link #exact} with {@code Methods.HEAD} for a cheaper answer.
   */
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
    register(table.exactPaths, method, path, handler, false);
    return this;
  }

  /**
   * Declares {@code method} on every path under {@code prefix}, the {@link PathHandler#addPrefixPath} match. For a route
   * whose path carries a parameter, registered up to the slash before it.
   */
  public PluginRoutes prefix(final HttpString method, final String prefix, final HttpHandler handler) {
    register(table.prefixPaths, method, prefix, handler, true);
    return this;
  }

  /**
   * Replaces the default {@code 405} body for {@code path}, already declared by this plugin, with {@code rejection}: for a
   * protocol that defines its own error envelope, such as JSON-RPC over MCP. The rejection runs with the {@code 405} status
   * and the {@code Allow} header already set; it writes the body, and must not act on the request.
   */
  public PluginRoutes onMethodNotAllowed(final String path, final HttpHandler rejection) {
    final String key = PluginPathHandler.normalize(path);
    synchronized (table) {
      MethodRestrictedHandler restricted = table.exactPaths.get(key);
      if (restricted == null)
        restricted = table.prefixPaths.get(key);
      if (restricted == null)
        throw new IllegalArgumentException("No route is declared at " + path);

      final String owner = owner();
      if (!restricted.isDeclaredBy(owner))
        throw new IllegalArgumentException(
            "The route " + path + " is declared by " + restricted.getDeclaringPlugins() + ", not by " + describe(owner)
                + ": only the plugin that declared a route replaces its rejection");
      restricted.rejection = rejection;
    }
    return this;
  }

  private void register(final Map<String, MethodRestrictedHandler> registered, final HttpString method, final String path,
      final HttpHandler handler, final boolean isPrefix) {
    if (method == null || path == null || handler == null)
      throw new IllegalArgumentException("A plugin route needs a method, a path and a handler");

    final String owner = owner();
    // Keyed the way PathHandler keys it, so a path declared with a trailing slash (RaftHAPlugin's prefixes) and the same path
    // without one are one entry. The handler keeps the path as it was first declared, for the messages that name it
    final String key = PluginPathHandler.normalize(path);
    synchronized (table) {
      final MethodRestrictedHandler existing = registered.get(key);
      if (existing != null) {
        existing.allow(method, handler, owner);
        return;
      }

      final MethodRestrictedHandler restricted = new MethodRestrictedHandler(path);
      restricted.allow(method, handler, owner);
      // Registered on the PathHandler first: a claim the server refuses must leave no entry behind in the table
      if (isPrefix)
        paths.addPrefixPath(path, restricted);
      else
        paths.addExactPath(path, restricted);
      registered.put(key, restricted);
    }
  }

  /** The plugin these routes are declared for: the one the server is registering, or {@code null} off the server. */
  private String owner() {
    return serverPaths != null ? serverPaths.getRegisteringPlugin() : null;
  }

  private static String describe(final String owner) {
    return owner != null ? "plugin '" + owner + "'" : UNNAMED_PLUGIN;
  }

  /**
   * The method-keyed entries registered so far, by path: shared by every {@link PluginRoutes} built on the same
   * {@code PluginPathHandler}, so a path is one entry however many plugins declare a method on it (issue #10385).
   */
  static final class RouteTable {
    private final Map<String, MethodRestrictedHandler> exactPaths  = new HashMap<>();
    private final Map<String, MethodRestrictedHandler> prefixPaths = new HashMap<>();
  }

  /**
   * The handler a {@link PluginRoutes} path is registered under: it dispatches to the handler declared for the request's
   * method, and answers {@code 405} with an {@code Allow} header for any other method without invoking a plugin handler.
   * Public so that a recording {@link PathHandler} in a test can report the methods a path actually answers.
   */
  public static final class MethodRestrictedHandler implements HttpHandler {
    private final    String                        path;
    // Copy-on-write: written while registerAPI runs, read by every request afterwards without a lock.
    private volatile Map<HttpString, HttpHandler>  byMethod    = Map.of();
    private volatile String                        allowHeader = "";
    private volatile HttpHandler                   rejection;
    // Who declared each method, written under the same lock as byMethod and only read while registering
    private final    Map<HttpString, String>       declaredBy  = new LinkedHashMap<>();

    private MethodRestrictedHandler(final String path) {
      this.path = path;
    }

    private synchronized void allow(final HttpString method, final HttpHandler handler, final String owner) {
      if (declaredBy.containsKey(method)) {
        final String previous = declaredBy.get(method);
        final boolean sameOwner = previous == null ? owner == null : previous.equals(owner);
        throw new IllegalStateException(
            "Cannot declare " + method + " " + path + " for " + describe(owner) + ": " + (sameOwner ? "it" : describe(previous))
                + " already declares it, so one of the two routes would silently replace the other");
      }
      declaredBy.put(method, owner);

      final Map<HttpString, HttpHandler> next = new LinkedHashMap<>(byMethod);
      next.put(method, handler);
      byMethod = Collections.unmodifiableMap(next);
      allowHeader = String.join(", ", getAnsweredMethods());
    }

    @Override
    public void handleRequest(final HttpServerExchange exchange) throws Exception {
      final HttpHandler handler = getHandler(exchange.getRequestMethod());
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

    /** The declared methods, in declaration order: {@code HEAD} derived from a declared {@code GET} is not among them. */
    public List<String> getAllowedMethods() {
      final List<String> methods = new ArrayList<>(byMethod.size());
      for (final HttpString method : byMethod.keySet())
        methods.add(method.toString());
      return methods;
    }

    /**
     * The methods this route answers, as the {@code Allow} header names them: the declared ones in declaration order, and
     * {@code HEAD} right after a declared {@code GET} unless the plugin declared it too (issue #10384).
     */
    private List<String> getAnsweredMethods() {
      final List<String> methods = new ArrayList<>(byMethod.size() + 1);
      for (final HttpString method : byMethod.keySet()) {
        methods.add(method.toString());
        if (Methods.GET.equals(method) && !byMethod.containsKey(Methods.HEAD))
          methods.add(Methods.HEAD.toString());
      }
      return methods;
    }

    /**
     * The handler that answers {@code method}, or {@code null} when that method is answered 405: the one declared for it, and
     * for {@code HEAD} that of {@code GET} unless the plugin declared {@code HEAD} itself (issue #10384).
     */
    public HttpHandler getHandler(final HttpString method) {
      final HttpHandler handler = byMethod.get(method);
      return handler == null && Methods.HEAD.equals(method) ? byMethod.get(Methods.GET) : handler;
    }

    private synchronized boolean isDeclaredBy(final String owner) {
      for (final String declaring : declaredBy.values())
        if (owner == null ? declaring == null : owner.equals(declaring))
          return true;
      return false;
    }

    private synchronized String getDeclaringPlugins() {
      final List<String> plugins = new ArrayList<>();
      for (final String declaring : declaredBy.values()) {
        final String description = describe(declaring);
        if (!plugins.contains(description))
          plugins.add(description);
      }
      return String.join(" and ", plugins);
    }
  }
}
