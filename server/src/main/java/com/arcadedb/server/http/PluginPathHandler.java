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
import com.arcadedb.server.ServerPlugin;
import io.undertow.server.HttpHandler;
import io.undertow.server.handlers.PathHandler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

/**
 * The outer {@link PathHandler} {@code HttpServer} routes through. It is the one object every plugin registers on, so it is
 * where a claim on a name another party holds is noticed (issues #10157, #10385).
 * <p>
 * A {@link PathHandler} is a last-writer-wins map, which makes three plugin registrations silently destructive: a second
 * plugin on a path replaces the first plugin's entry, an exact path that a core route serves wins over the {@code /api/v1}
 * prefix the core {@code RoutingHandler} is mounted under, and a prefix equal to that mount replaces the whole core router.
 * Every path registered here is therefore recorded with its owner, the server or a plugin, and a plugin's claim is refused
 * with an {@link IllegalStateException} naming both parties when it is equal to a path already claimed, or when it would
 * shadow a route the server registered for itself ({@link #reserveServerRoutes}). The refusal propagates out of
 * {@code registerAPI}, so the server does not start with a route that silently does not exist, rather than starting with a
 * line in a log. {@link PluginRoutes} shares its method-keyed table through {@link #getRouteTable()}, so a second plugin
 * declaring ANOTHER method on a path merges into the same entry instead of replacing it.
 * <p>
 * The deprecated {@code ServerPlugin.registerAPI(HttpServer, PathHandler)} keeps handing plugins a {@link PathHandler},
 * so a plugin built against it keeps working. But a handler added to it directly answers every HTTP method, which turned
 * {@code GET} into a way to run state-changing cluster operations. Every plugin in this repository implements
 * {@code registerAPI(HttpServer, PluginRoutes)} instead (issue #10296); a route a plugin adds to the raw handler is named
 * here, once per path, at {@code WARNING}, so the gap is visible at startup rather than in a code review. Routing is
 * unchanged: the route is served as registered, once the claim itself is accepted.
 */
final class PluginPathHandler extends PathHandler {
  private static final String SERVER = "the server";

  private final List<String>              unrestricted = new ArrayList<>();
  private final PluginRoutes.RouteTable   routeTable   = new PluginRoutes.RouteTable();
  // Normalized path -> who registered it ("the server" or "plugin 'name'"), for the exact and for the prefix registrations
  private final Map<String, String>       exactClaims  = new HashMap<>();
  private final Map<String, String>       prefixClaims = new HashMap<>();
  // The routes the server answers through a router mounted under a prefix: a plugin path that fills one of them shadows it
  private final List<ServerRoute>         serverRoutes = new ArrayList<>();
  private       String                    registeringPlugin;

  /** A route of the server's: its path split in segments, how many of them are the router's mount, and how it is named. */
  private record ServerRoute(String[] segments, int mountLength, String description) {
  }

  /** Marks the routes added from now on as {@code plugin}'s, or as the server's own when {@code null}. */
  synchronized void setRegisteringPlugin(final String plugin) {
    this.registeringPlugin = plugin;
  }

  /** The plugin whose routes are being registered, or {@code null} when it is the server's own. */
  synchronized String getRegisteringPlugin() {
    return registeringPlugin;
  }

  /** The method-keyed table every {@link PluginRoutes} built on this handler shares (issue #10385). */
  PluginRoutes.RouteTable getRouteTable() {
    return routeTable;
  }

  /**
   * Records the routes the server answers through a {@code RoutingHandler} it mounted at {@code prefix}, so a plugin path
   * that would shadow one is refused (issue #10385). The paths are the templates the router was given, relative to the
   * prefix, e.g. {@code /query/{database}}.
   */
  synchronized void reserveServerRoutes(final String prefix, final List<RouteRecordingRoutingHandler.RouteDescriptor> routes) {
    final String normalizedPrefix = normalize(prefix);
    final int mountLength = segmentsOf(normalizedPrefix).length;
    for (final RouteRecordingRoutingHandler.RouteDescriptor route : routes) {
      final String full = "/".equals(normalizedPrefix) ? route.path() : normalizedPrefix + route.path();
      serverRoutes.add(new ServerRoute(segmentsOf(normalize(full)), mountLength, route.method() + " " + full));
    }
  }

  @Override
  public synchronized PathHandler addExactPath(final String path, final HttpHandler handler) {
    judge(path, false);
    final PathHandler result = super.addExactPath(path, handler);
    record(path, handler, false);
    return result;
  }

  @Override
  public synchronized PathHandler addPrefixPath(final String path, final HttpHandler handler) {
    judge(path, true);
    final PathHandler result = super.addPrefixPath(path, handler);
    record(path, handler, true);
    return result;
  }

  /** The {@code plugin path} of every plugin route registered without a method restriction, in registration order. */
  synchronized List<String> getUnrestrictedPluginRoutes() {
    return List.copyOf(unrestricted);
  }

  /**
   * Whether {@code plugin}'s class (or a superclass or interface of it) overrides the deprecated
   * {@code registerAPI(HttpServer, PathHandler)}, rather than inheriting the {@link ServerPlugin} no-op default (issue
   * #10296).
   */
  static boolean implementsDeprecatedRegisterAPI(final ServerPlugin plugin) {
    return overridesRegisterAPI(plugin, PathHandler.class);
  }

  /** Whether {@code plugin} overrides {@code registerAPI(HttpServer, PluginRoutes)} (issue #10296). */
  static boolean implementsPluginRoutesRegisterAPI(final ServerPlugin plugin) {
    return overridesRegisterAPI(plugin, PluginRoutes.class);
  }

  private static boolean overridesRegisterAPI(final ServerPlugin plugin, final Class<?> routesType) {
    try {
      return plugin.getClass().getMethod("registerAPI", HttpServer.class, routesType).getDeclaringClass() != ServerPlugin.class;
    } catch (final NoSuchMethodException e) {
      // ServerPlugin declares both overloads, so this means the SPI changed without this lookup: failing here is what
      // keeps a renamed overload from silently dropping every third-party plugin's routes
      throw new IllegalStateException(
          "ServerPlugin no longer declares registerAPI(HttpServer, " + routesType.getSimpleName() + ")", e);
    }
  }

  /** Refuses a plugin's claim on a name that is taken, before the {@link PathHandler} sees it. The server's own are never judged. */
  private void judge(final String path, final boolean isPrefix) {
    final String owner = registeringPlugin;
    if (owner != null)
      refuseIfTaken("plugin '" + owner + "'", path, normalize(path), isPrefix);
  }

  /**
   * Records who holds a path once the {@link PathHandler} has accepted it, so a registration it refuses leaves no claim behind,
   * and names a plugin route that was added without its methods.
   */
  private void record(final String path, final HttpHandler handler, final boolean isPrefix) {
    final String owner = registeringPlugin;
    final Map<String, String> claims = isPrefix ? prefixClaims : exactClaims;
    if (owner == null) {
      // The server's own mounts: recorded so a plugin cannot claim them
      claims.put(normalize(path), SERVER);
      return;
    }

    claims.put(normalize(path), "plugin '" + owner + "'");

    // A route declared through PluginRoutes carries its methods, which is what the warning below is about
    if (handler instanceof PluginRoutes.MethodRestrictedHandler)
      return;
    unrestricted.add(owner + " " + path);
    LogManager.instance().log(this, Level.WARNING,
        "Plugin '%s' registered the HTTP route %s without declaring its method, so it answers every method (GET included). "
            + "Implement registerAPI(HttpServer, PluginRoutes) and declare it there instead", owner, path);
  }

  /** Throws when {@code key} is a name somebody already holds, or when it would shadow a route the server registered. */
  private void refuseIfTaken(final String claimant, final String path, final String key, final boolean isPrefix) {
    final String held = (isPrefix ? prefixClaims : exactClaims).get(key);
    if (held != null)
      throw new IllegalStateException(claimant + " cannot register the HTTP route " + path + ": " + held + " already registered "
          + (isPrefix ? "that prefix" : "that path") + ", so one of the two would silently lose it");

    // An exact path equal to the path a server mount is registered at answers that path instead of the mount
    if (!isPrefix && SERVER.equals(prefixClaims.get(key)))
      throw new IllegalStateException(
          claimant + " cannot register the HTTP route " + path + ": it is the path of a prefix the server mounts, which the "
              + "plugin's route would answer instead");

    final String[] segments = segmentsOf(key);
    for (final ServerRoute route : serverRoutes)
      if (shadows(segments, isPrefix, route))
        throw new IllegalStateException(
            claimant + " cannot register the HTTP route " + path + ": it would shadow the server's own route " + route.description
                + ", which the plugin's route would answer instead");
  }

  /**
   * Whether a plugin's {@code pluginSegments} - an exact path, or a prefix - are matched BEFORE the server's {@code route}:
   * every segment equal, or the server's a {@code {parameter}}. An exact path wins over any prefix, so it has to have as
   * many segments as the route; a prefix wins over the router's mount only when it is at least as long as the mount, and it
   * covers the route when it is no longer than the route. A shorter prefix loses to the mount, which is more specific.
   */
  private static boolean shadows(final String[] pluginSegments, final boolean isPrefix, final ServerRoute route) {
    final String[] serverSegments = route.segments;
    if (isPrefix ?
        pluginSegments.length > serverSegments.length || pluginSegments.length < route.mountLength :
        pluginSegments.length != serverSegments.length)
      return false;
    for (int i = 0; i < pluginSegments.length; i++)
      if (!pluginSegments[i].equals(serverSegments[i]) && !(serverSegments[i].startsWith("{") && serverSegments[i].endsWith("}")))
        return false;
    return true;
  }

  private static String[] segmentsOf(final String normalizedPath) {
    return "/".equals(normalizedPath) ? new String[0] : normalizedPath.substring(1).split("/");
  }

  /**
   * A path as {@link PathHandler} keys it: a leading slash and no trailing one. {@link PluginRoutes} keys its table the same
   * way, so {@code /api/v1/cluster/peer/} and {@code /api/v1/cluster/peer} are one entry there as they are one path here.
   */
  static String normalize(final String path) {
    String normalized = path.startsWith("/") ? path : "/" + path;
    while (normalized.length() > 1 && normalized.endsWith("/"))
      normalized = normalized.substring(0, normalized.length() - 1);
    return normalized;
  }
}
