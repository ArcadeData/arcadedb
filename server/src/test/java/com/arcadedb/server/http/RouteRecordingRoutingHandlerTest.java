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
import io.undertow.server.HttpServerExchange;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RouteRecordingRoutingHandlerTest {

  private static final HttpHandler NOOP_HANDLER = (HttpServerExchange exchange) -> {
  };

  @Test
  void recordsEveryRegisteredMethodAndPath() {
    final RouteRecordingRoutingHandler handler = new RouteRecordingRoutingHandler();

    handler.get("/databases", NOOP_HANDLER)
        .post("/begin/{database}", NOOP_HANDLER)
        .put("/server/users", NOOP_HANDLER)
        .delete("/server/users", NOOP_HANDLER);

    assertThat(handler.getRegisteredRoutes()).containsExactlyInAnyOrder(
        new RouteRecordingRoutingHandler.RouteDescriptor("GET", "/databases"),
        new RouteRecordingRoutingHandler.RouteDescriptor("POST", "/begin/{database}"),
        new RouteRecordingRoutingHandler.RouteDescriptor("PUT", "/server/users"),
        new RouteRecordingRoutingHandler.RouteDescriptor("DELETE", "/server/users"));
  }

  @Test
  void chainedCallsReturnTheSameRecordingInstance() {
    final RouteRecordingRoutingHandler handler = new RouteRecordingRoutingHandler();

    final var result = handler.get("/a", NOOP_HANDLER).post("/b", NOOP_HANDLER);

    assertThat(result).isSameAs(handler);
    assertThat(handler.getRegisteredRoutes()).hasSize(2);
  }

  /**
   * The class Javadoc claims every verb - not just get/post/put/delete - is captured, because they
   * all funnel through the two overridden terminal add() methods. Nothing in HttpServer.setupRoutes()
   * uses a verb without its own convenience method today, so this proves the broader claim directly
   * rather than leaving it implied by the four convenience methods alone.
   */
  @Test
  void recordsAVerbRegisteredThroughTheGenericAddOverloadWithNoConvenienceMethod() {
    final RouteRecordingRoutingHandler handler = new RouteRecordingRoutingHandler();

    handler.add("PATCH", "/server/settings", NOOP_HANDLER);

    assertThat(handler.getRegisteredRoutes())
        .containsExactly(new RouteRecordingRoutingHandler.RouteDescriptor("PATCH", "/server/settings"));
  }

  /**
   * Issue #4896's required self-test, exercised against the real recording mechanism: proves the
   * actual-vs-declared comparison every core anti-drift check relies on genuinely fails when a route
   * is actually registered but missing from the declared set.
   */
  @Test
  void theAntiDriftCheckCatchesARouteThatIsNotDeclared() {
    final RouteRecordingRoutingHandler handler = new RouteRecordingRoutingHandler();
    handler.get("/databases", NOOP_HANDLER).post("/begin/{database}", NOOP_HANDLER);

    final var declared = List.of(new RouteRecordingRoutingHandler.RouteDescriptor("GET", "/databases"));

    assertThatThrownBy(() -> assertThat(handler.getRegisteredRoutes())
        .containsExactlyInAnyOrderElementsOf(declared))
        .isInstanceOf(AssertionError.class);
  }
}
