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
package com.arcadedb.server.mcp;

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.handler.AbstractServerHttpHandler;
import com.arcadedb.server.http.handler.ExecutionResponse;
import com.arcadedb.server.mcp.MCPDispatcher.MCPResponse;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;

/**
 * HTTP transport for MCP. Owns the HTTP envelope only; all protocol routing lives in {@link MCPDispatcher}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class MCPHttpHandler extends AbstractServerHttpHandler {
  private final MCPDispatcher dispatcher;

  public MCPHttpHandler(final HttpServer httpServer, final ArcadeDBServer server, final MCPConfiguration config) {
    super(httpServer);
    this.dispatcher = new MCPDispatcher(server, config, "http");
  }

  @Override
  protected boolean mustExecuteOnWorkerThread() {
    return true;
  }

  @Override
  public boolean isRequireAuthentication() {
    return true;
  }

  @Override
  protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user, final JSONObject payload) {
    final MCPResponse response = dispatcher.dispatch(payload, user);

    // A null body is a JSON-RPC notification, which takes no response at all.
    if (response.json() == null)
      return new ExecutionResponse(204, "");

    return new ExecutionResponse(response.httpStatus(), response.json().toString());
  }
}
