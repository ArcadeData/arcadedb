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

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.handler.AbstractServerHttpHandler;
import com.arcadedb.server.http.handler.ExecutionResponse;
import com.arcadedb.server.security.ServerSecurityUser;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import io.undertow.util.MimeMappings;

public class GetPrometheusMetricsHandler extends AbstractServerHttpHandler {

  private final PrometheusMeterRegistry registry;
  private final Boolean                 requireAuthentication;

  public GetPrometheusMetricsHandler(HttpServer httpServer, PrometheusMeterRegistry registry, Boolean requireAuthentication) {
    super(httpServer);
    this.registry = registry;
    this.requireAuthentication = requireAuthentication;
  }

  @Override
  public ExecutionResponse execute(HttpServerExchange exchange, ServerSecurityUser user, final JSONObject payload) {
    String response = registry.scrape();

    exchange.getResponseHeaders()
        .put(Headers.CONTENT_TYPE, MimeMappings.DEFAULT.getMimeType("txt"));

    return new ExecutionResponse(200, response);
  }

  @Override
  public boolean isRequireAuthentication() {
    return requireAuthentication;
  }
}
