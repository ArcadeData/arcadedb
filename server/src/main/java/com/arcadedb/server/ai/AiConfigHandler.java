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
package com.arcadedb.server.ai;

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.handler.AbstractServerHttpHandler;
import com.arcadedb.server.http.handler.ExecutionResponse;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;

/**
 * GET /api/v1/ai/config - Returns AI configuration status (whether subscription is active).
 */
public class AiConfigHandler extends AbstractServerHttpHandler {
  private final AiConfiguration config;
  private final AiPortal        portal;

  public AiConfigHandler(final HttpServer httpServer, final AiConfiguration config) {
    this(httpServer, config, null);
  }

  /** @param portal the customer portal this server may be connected to, or null for a gateway-only configuration */
  public AiConfigHandler(final HttpServer httpServer, final AiConfiguration config, final AiPortal portal) {
    super(httpServer);
    this.config = config;
    this.portal = portal;
  }

  @Override
  protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user, final JSONObject payload) {
    final JSONObject json = config.toJSON();
    if (portal != null) {
      // "configured" means "the assistant can answer": a connected portal whose plan includes it, or a legacy gateway key
      final JSONObject portalStatus = portal.toJSON();
      json.put("portal", portalStatus);
      json.put("source", portalStatus.getBoolean("enabled", false) ? "portal" : config.isConfigured() ? "gateway" : "none");
      json.put("configured", portalStatus.getBoolean("enabled", false) || config.isConfigured());
    }
    return new ExecutionResponse(200, json.toString());
  }
}
