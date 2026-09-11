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
package com.arcadedb.metrics.prometheus;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.handler.AbstractServerHttpHandler;
import com.arcadedb.server.http.handler.ExecutionResponse;
import com.arcadedb.server.security.ServerSecurityUser;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import io.undertow.util.MimeMappings;

import java.util.Objects;

public class GetPrometheusMetricsHandler extends AbstractServerHttpHandler {

  /**
   * The authentication decision together with the raw configured value it was taken from, so the strict
   * parse - and the WARNING it logs for a value it cannot read - runs once per CHANGE rather than once per
   * scrape. Written and read as one object so a reader can never pair a stale value with a fresh decision.
   */
  private record AuthenticationDecision(Object configuredValue, boolean required) {
  }

  private final PrometheusMeterRegistry registry;
  private final ContextConfiguration    configuration;
  // Written by any request thread that observes a changed setting, read by all of them: the two fields
  // travel together in one immutable object, so the worst a race costs is the parse being run twice.
  private volatile AuthenticationDecision decision;

  public GetPrometheusMetricsHandler(final HttpServer httpServer, final PrometheusMeterRegistry registry,
      final ContextConfiguration configuration) {
    super(httpServer);
    this.registry = registry;
    this.configuration = configuration;
  }

  @Override
  public ExecutionResponse execute(HttpServerExchange exchange, ServerSecurityUser user, final JSONObject payload) {
    String response = registry.scrape();

    exchange.getResponseHeaders()
        .put(Headers.CONTENT_TYPE, MimeMappings.DEFAULT.getMimeType("txt"));

    return new ExecutionResponse(200, response);
  }

  /**
   * Issue #7159: reads the setting on EVERY request instead of the value captured when the route was
   * registered. {@code arcadedb.serverMetrics.prometheus.requireAuthentication} is a SCOPE.SERVER setting, so
   * {@code SET SERVER SETTING} and the {@code set_server_setting} MCP tool write it into the server's
   * {@link ContextConfiguration} and answer 200; before this the live route kept whatever it had been
   * registered with until the next restart. The failure mode was asymmetric: an operator TIGHTENING the
   * switch believed they had closed the endpoint and had not.
   * <p>
   * The strict, fail-closed parse of {@link PrometheusMetricsPlugin#isAuthenticationRequired(Object)} stays
   * off the scrape path in the common case: the decision is cached against the raw configured value it was
   * taken from, so a scrape re-parses only when that value actually changed - which is also what keeps the
   * WARNING for an unreadable value out of a per-scrape log loop.
   */
  @Override
  public boolean isRequireAuthentication() {
    final Object configuredValue = PrometheusMetricsPlugin.readConfiguredValue(configuration);

    final AuthenticationDecision current = decision;
    if (current != null && Objects.equals(current.configuredValue(), configuredValue))
      return current.required();

    final boolean required = PrometheusMetricsPlugin.isAuthenticationRequired(configuredValue);
    decision = new AuthenticationDecision(configuredValue, required);
    return required;
  }
}
