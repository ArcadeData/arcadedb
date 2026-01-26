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

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpAuthSession;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;
import io.micrometer.core.instrument.Metrics;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HeaderValues;

/**
 * Handles POST /api/v1/login endpoint.
 * <p>
 * Authenticates a user with Basic Auth credentials and returns an authentication token
 * that can be used for subsequent requests instead of sending credentials each time.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/1691">GitHub Issue #1691</a>
 */
public class PostLoginHandler extends AbstractServerHttpHandler {
  // Cloudflare headers for geolocation
  // See: https://developers.cloudflare.com/fundamentals/reference/http-request-headers/
  private static final String CF_CONNECTING_IP = "CF-Connecting-IP";
  private static final String CF_IPCOUNTRY     = "CF-IPCountry";
  private static final String CF_IPCITY        = "CF-IPCity";
  private static final String X_FORWARDED_FOR  = "X-Forwarded-For";
  private static final String USER_AGENT       = "User-Agent";

  public PostLoginHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  @Override
  protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user, final JSONObject payload) {
    // Extract client metadata from request headers
    final String sourceIp = extractSourceIp(exchange);
    final String userAgent = getHeaderValue(exchange, USER_AGENT);
    final String country = getHeaderValue(exchange, CF_IPCOUNTRY);
    final String city = getHeaderValue(exchange, CF_IPCITY);

    // User is already authenticated by AbstractServerHttpHandler
    // Create a new authentication session with metadata
    final HttpAuthSession session = httpServer.getAuthSessionManager().createSession(user, sourceIp, userAgent, country, city);

    final JSONObject response = new JSONObject();
    response.put("token", session.getToken());
    response.put("user", user.getName());

    Metrics.counter("http.login").increment();

    return new ExecutionResponse(200, response.toString());
  }

  /**
   * Extracts the client's source IP address from the request.
   * Checks Cloudflare header first, then X-Forwarded-For, then falls back to connection source.
   */
  private String extractSourceIp(final HttpServerExchange exchange) {
    // First check Cloudflare header
    String ip = getHeaderValue(exchange, CF_CONNECTING_IP);
    if (ip != null && !ip.isEmpty())
      return ip;

    // Then check X-Forwarded-For (take first IP if multiple)
    ip = getHeaderValue(exchange, X_FORWARDED_FOR);
    if (ip != null && !ip.isEmpty()) {
      final int commaIndex = ip.indexOf(',');
      return commaIndex > 0 ? ip.substring(0, commaIndex).trim() : ip.trim();
    }

    // Fall back to direct connection source
    final var sourceAddress = exchange.getSourceAddress();
    return sourceAddress != null ? sourceAddress.getAddress().getHostAddress() : null;
  }

  /**
   * Gets a header value from the request, returning null if not present.
   */
  private String getHeaderValue(final HttpServerExchange exchange, final String headerName) {
    final HeaderValues values = exchange.getRequestHeaders().get(headerName);
    return (values != null && !values.isEmpty()) ? values.getFirst() : null;
  }
}
