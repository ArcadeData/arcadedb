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
import io.undertow.server.RoutingHandler;

import java.util.ArrayList;
import java.util.List;

/**
 * A {@link RoutingHandler} that records every (method, path template) pair registered through it,
 * so a live server can report exactly which routes it actually wired up. Used by
 * {@link HttpServer#setupRoutes()} in place of a plain {@code Handlers.routing()} instance -
 * recording is a side effect only, behavior as a router is unchanged because every overridden method
 * delegates to {@code super} before returning. Note: only routes registered via
 * {@code get()}, {@code post()}, {@code put()}, and {@code delete()} are recorded; routes registered
 * via {@code add(...)} or predicate-based overloads would silently not appear in {@link #getRegisteredRoutes()}.
 */
public class RouteRecordingRoutingHandler extends RoutingHandler {

  public record RouteDescriptor(String method, String path) {
  }

  private final List<RouteDescriptor> registeredRoutes = new ArrayList<>();

  @Override
  public synchronized RoutingHandler get(final String template, final HttpHandler handler) {
    registeredRoutes.add(new RouteDescriptor("GET", template));
    return super.get(template, handler);
  }

  @Override
  public synchronized RoutingHandler post(final String template, final HttpHandler handler) {
    registeredRoutes.add(new RouteDescriptor("POST", template));
    return super.post(template, handler);
  }

  @Override
  public synchronized RoutingHandler put(final String template, final HttpHandler handler) {
    registeredRoutes.add(new RouteDescriptor("PUT", template));
    return super.put(template, handler);
  }

  @Override
  public synchronized RoutingHandler delete(final String template, final HttpHandler handler) {
    registeredRoutes.add(new RouteDescriptor("DELETE", template));
    return super.delete(template, handler);
  }

  public synchronized List<RouteDescriptor> getRegisteredRoutes() {
    return List.copyOf(registeredRoutes);
  }
}
