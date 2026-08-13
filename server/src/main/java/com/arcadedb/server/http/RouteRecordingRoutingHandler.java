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

import io.undertow.predicate.Predicate;
import io.undertow.server.HttpHandler;
import io.undertow.server.RoutingHandler;
import io.undertow.util.HttpString;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * A {@link RoutingHandler} that records every (method, path template) pair registered through it,
 * so a live server can report exactly which routes it actually wired up. Used by
 * {@link HttpServer#setupRoutes()} in place of a plain {@code Handlers.routing()} instance -
 * recording is a side effect only, behavior as a router is unchanged because every overridden method
 * delegates to {@code super} before returning. All eight verb convenience methods ({@code get()},
 * {@code post()}, ...) and both {@code add(...)} overloads funnel through the two terminal
 * {@code add(HttpString, String, HttpHandler)} / {@code add(HttpString, String, Predicate, HttpHandler)}
 * methods overridden here, so every registration path is captured, including future verbs (e.g.
 * PATCH) that have no dedicated convenience method. {@code addAll(RoutingHandler)} is the one
 * remaining gap: it merges another handler's internal route map directly and bypasses both terminal
 * methods, so routes registered that way would not appear in {@link #getRegisteredRoutes()} - not a
 * concern today since {@link HttpServer#setupRoutes()} never calls it.
 */
public class RouteRecordingRoutingHandler extends RoutingHandler {

  public record RouteDescriptor(String method, String path) {
  }

  private final Set<RouteDescriptor> registeredRoutes = new LinkedHashSet<>();

  @Override
  public synchronized RoutingHandler add(final HttpString method, final String template, final HttpHandler handler) {
    registeredRoutes.add(new RouteDescriptor(method.toString(), template));
    return super.add(method, template, handler);
  }

  @Override
  public synchronized RoutingHandler add(final HttpString method, final String template, final Predicate predicate,
      final HttpHandler handler) {
    registeredRoutes.add(new RouteDescriptor(method.toString(), template));
    return super.add(method, template, predicate, handler);
  }

  public synchronized List<RouteDescriptor> getRegisteredRoutes() {
    return List.copyOf(registeredRoutes);
  }
}
