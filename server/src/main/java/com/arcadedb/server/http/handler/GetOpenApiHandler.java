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
package com.arcadedb.server.http.handler;

import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;
import io.micrometer.core.instrument.Metrics;
import io.swagger.v3.core.util.Json;
import io.swagger.v3.oas.models.OpenAPI;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;

import java.util.logging.Level;

/**
 * HTTP handler that serves the OpenAPI specification at /api/v1/openapi.json.
 * This endpoint provides a comprehensive OpenAPI 3.0+ specification for the ArcadeDB HTTP API.
 */
public class GetOpenApiHandler extends AbstractServerHttpHandler {

  private final OpenApiSpecGenerator specGenerator;
  private volatile String cachedSpec;

  public GetOpenApiHandler(final HttpServer httpServer) {
    super(httpServer);
    this.specGenerator = new OpenApiSpecGenerator(httpServer);
  }

  @Override
  protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user, final JSONObject payload)
      throws Exception {

    Metrics.counter("http.openapi").increment();

    try {
      // Generate or use cached OpenAPI spec
      String specJson = getCachedOrGenerateSpec();

      // Set proper content type for OpenAPI spec
      exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");

      LogManager.instance().log(this, Level.FINE, "Serving OpenAPI specification");

      return new ExecutionResponse(200, specJson);

    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error generating OpenAPI specification: %s", e, e.getMessage());
      throw e;
    }
  }

  @Override
  public boolean isRequireAuthentication() {
    // OpenAPI spec should be accessible with authentication for security
    return true;
  }

  /**
   * Gets the cached OpenAPI specification or generates a new one if not cached.
   * The specification is cached to improve performance for subsequent requests.
   *
   * @return OpenAPI specification as JSON string
   * @throws Exception if spec generation fails
   */
  private String getCachedOrGenerateSpec() throws Exception {
    if (cachedSpec == null) {
      synchronized (this) {
        if (cachedSpec == null) {
          final OpenAPI openAPI = specGenerator.generateSpec();
          cachedSpec = Json.mapper().writeValueAsString(openAPI);
          LogManager.instance().log(this, Level.INFO, "Generated and cached OpenAPI specification");
        }
      }
    }
    return cachedSpec;
  }

  /**
   * Clears the cached OpenAPI specification, forcing regeneration on next request.
   * This can be useful during development or when the API structure changes.
   */
  public void clearCache() {
    synchronized (this) {
      cachedSpec = null;
      LogManager.instance().log(this, Level.FINE, "Cleared OpenAPI specification cache");
    }
  }
}
