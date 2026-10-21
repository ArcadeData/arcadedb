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
import io.undertow.predicate.Predicate;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.server.RoutingHandler;
import io.undertow.util.Headers;
import io.undertow.util.HttpString;
import io.undertow.util.Methods;
import io.undertow.util.PathTemplateMatcher;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * A {@link RoutingHandler} that records every (method, path template) pair registered through it,
 * so a live server can report exactly which routes it actually wired up. Used by
 * {@link HttpServer#setupRoutes()} in place of a plain {@code Handlers.routing()} instance -
 * recording is a side effect only, and apart from the HEAD and 405 handling described below the behavior as a router is unchanged
 * because every overridden method delegates to {@code super} before returning. All eight verb convenience methods ({@code get()},
 * {@code post()}, ...) and both {@code add(...)} overloads funnel through the two terminal
 * {@code add(HttpString, String, HttpHandler)} / {@code add(HttpString, String, Predicate, HttpHandler)}
 * methods overridden here, so every registration path is captured, including future verbs (e.g.
 * PATCH) that have no dedicated convenience method. {@code addAll(RoutingHandler)} is the one
 * remaining gap: it merges another handler's internal route map directly and bypasses both terminal
 * methods, so routes registered that way would not appear in {@link #getRegisteredRoutes()} - not a
 * concern today since {@link HttpServer#setupRoutes()} never calls it.
 * <p>
 * The handler INSTANCE is recorded with each route (issue #9849), so a test can ask a question of the handlers themselves - "does
 * every route that may block dispatch off the Undertow IO thread" - against the real inventory instead of a list somebody maintains
 * by hand. It is a separate map and not a component of {@link RouteDescriptor}: the descriptor is the route's identity, compared
 * and printed by the OpenAPI anti-drift checks, and must not change with the object that serves it.
 * <p>
 * RFC 9110 requires a server that supports GET on a resource to support HEAD on it (section 9.3.2), and a 405 to carry the
 * {@code Allow} header naming what the resource does accept (section 15.5.6). Both are derived here, once, for every route the
 * router carries (issue #10531), the way {@link PluginRoutes} does for a plugin's: a GET registration also answers HEAD with the same
 * handler, unless the template has a HEAD entry of its own, and a request for a registered template with a method it does not
 * answer gets a 405 with {@code Allow}. The derived HEAD is not a registered route: it is an answer to the GET entry, so the
 * inventory ({@link #getRegisteredRoutes()}) and the OpenAPI document it is compared with name only what somebody declared. Undertow's
 * HEAD handling at the connector discards whatever body the handler writes, so the headers and the status match the GET.
 */
public class RouteRecordingRoutingHandler extends RoutingHandler {

  public record RouteDescriptor(String method, String path) {
  }

  private final Map<RouteDescriptor, HttpHandler> registeredRoutes = new LinkedHashMap<>();
  // Every template with the methods it answers, the derived HEAD included, written while the routes are registered
  private final Map<String, Set<String>>          methodsByTemplate = new HashMap<>();
  private final PathTemplateMatcher<String>       templates         = new PathTemplateMatcher<>();
  // Template -> the Allow header of its 405, republished whole after each registration so a request reads it without a lock
  private volatile Map<String, String>            allowByTemplate   = Map.of();
  // True while the HEAD of a GET registration is being added: that one is derived, not declared, so it is not recorded
  private boolean                                 derivingHead;

  public RouteRecordingRoutingHandler() {
    setInvalidMethodHandler(this::answerMethodNotAllowed);
  }

  @Override
  public synchronized RoutingHandler add(final HttpString method, final String template, final HttpHandler handler) {
    if (!derivingHead)
      registeredRoutes.put(new RouteDescriptor(method.toString(), template), handler);
    super.add(method, template, handler);
    answers(template, method.toString());

    // A HEAD declared for the template (before or after its GET) is the one that answers; otherwise the GET entry does
    if (!derivingHead && Methods.GET.equals(method)
        && !registeredRoutes.containsKey(new RouteDescriptor(Methods.HEAD.toString(), template))) {
      derivingHead = true;
      try {
        add(Methods.HEAD, template, handler);
      } finally {
        derivingHead = false;
      }
    }
    return this;
  }

  /** A predicated entry: recorded and counted for the {@code Allow} header, but no HEAD is derived from it. */
  @Override
  public synchronized RoutingHandler add(final HttpString method, final String template, final Predicate predicate,
      final HttpHandler handler) {
    registeredRoutes.put(new RouteDescriptor(method.toString(), template), handler);
    super.add(method, template, predicate, handler);
    answers(template, method.toString());
    return this;
  }

  private void answers(final String template, final String method) {
    Set<String> methods = methodsByTemplate.get(template);
    if (methods == null) {
      methods = new TreeSet<>();
      methodsByTemplate.put(template, methods);
      templates.add(template, template);
    }
    methods.add(method);

    final Map<String, String> allow = new HashMap<>(allowByTemplate);
    allow.put(template, String.join(", ", methods));
    allowByTemplate = allow;
  }

  /**
   * The router's answer for a request whose path is a registered template and whose method is not one of its methods (Undertow
   * calls it only then; any other path goes to the fallback handler and is a 404 as before). Answered on the thread the request
   * arrived on, like the plugin registrar's 405: nothing below blocks and no route handler runs. The body names the registered
   * template, never the request's path, so nothing the caller sent is reflected back.
   */
  private void answerMethodNotAllowed(final HttpServerExchange exchange) {
    final PathTemplateMatcher.PathMatchResult<String> match = templates.match(exchange.getRelativePath());
    final String template = match != null ? match.getValue() : null;
    final String allow = template != null ? allowByTemplate.get(template) : null;

    exchange.setStatusCode(405);
    if (allow == null) {
      exchange.endExchange();
      return;
    }

    exchange.getResponseHeaders().put(Headers.ALLOW, allow);
    exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
    exchange.getResponseSender().send(new JSONObject()//
        .put("error", "Method Not Allowed")//
        .put("detail", "The route " + template + " accepts " + allow + " only")//
        .toString());
  }

  public synchronized List<RouteDescriptor> getRegisteredRoutes() {
    return List.copyOf(registeredRoutes.keySet());
  }

  /** Every registered route's handler, in registration order. A route registered twice keeps the handler registered last. */
  public synchronized Map<RouteDescriptor, HttpHandler> getRegisteredHandlers() {
    return new LinkedHashMap<>(registeredRoutes);
  }
}
