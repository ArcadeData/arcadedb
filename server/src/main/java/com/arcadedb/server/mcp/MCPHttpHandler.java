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

import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.handler.AbstractServerHttpHandler;
import com.arcadedb.server.http.handler.ExecutionResponse;
import com.arcadedb.server.mcp.MCPDispatcher.MCPResponse;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import io.undertow.util.Methods;

import java.net.URI;
import java.util.Locale;
import java.util.logging.Level;

/**
 * HTTP transport for MCP. Owns the HTTP envelope only; all protocol routing lives in {@link MCPDispatcher}.
 * Implements the envelope rules of the MCP 2025-03-26 Streamable HTTP transport: POST-only (no Server-Sent
 * Events stream is offered, so any other method is answered with {@code 405}), {@code Origin} validation
 * against DNS rebinding, JSON-RPC batches, and {@code 202 Accepted} with no body for a POST that carried
 * only notifications and/or responses.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class MCPHttpHandler extends AbstractServerHttpHandler {
  private final MCPDispatcher    dispatcher;
  private final MCPConfiguration config;

  public MCPHttpHandler(final HttpServer httpServer, final ArcadeDBServer server, final MCPConfiguration config) {
    super(httpServer);
    this.dispatcher = new MCPDispatcher(server, config, "http");
    this.config = config;
  }

  @Override
  protected boolean mustExecuteOnWorkerThread() {
    return true;
  }

  @Override
  public boolean isRequireAuthentication() {
    return true;
  }

  /**
   * A JSON-RPC batch is a top-level JSON array, so this endpoint opts in to the array-body support of the base
   * handler (issue #5415) instead of re-parsing the raw body itself.
   */
  @Override
  protected boolean acceptsArrayPayload() {
    return true;
  }

  @Override
  protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user, final JSONObject payload) {
    // This endpoint carries JSON-RPC over POST only. MCP allows a GET to open an SSE stream instead, which
    // this server does not implement, and requires the endpoint to answer 405 when it does not.
    if (!Methods.POST.equals(exchange.getRequestMethod())) {
      exchange.getResponseHeaders().put(Headers.ALLOW, "POST");
      return new ExecutionResponse(405,
          MCPDispatcher.errorObject(null, -32600, "Method Not Allowed: the MCP endpoint accepts POST only").toString());
    }

    if (!isOriginAllowed(exchange))
      return new ExecutionResponse(403, MCPDispatcher.errorObject(null, -32600, "Origin not allowed").toString());

    // A top-level array is a JSON-RPC batch, parsed once by the shared request pipeline (issue #5415).
    final JSONArray batch = getPayloadAsArray(exchange);
    if (batch != null)
      return executeBatch(batch, user);

    // Neither an object nor an array came back from the shared parsing while the client did send a body: the
    // body is not valid JSON. JSON-RPC answers that with -32700 over HTTP 200, never with a transport error.
    if (payload == null) {
      final String raw = exchange.getAttachment(RAW_PAYLOAD);
      if (raw != null && !raw.isBlank())
        return new ExecutionResponse(200,
            MCPDispatcher.errorObject(null, -32700, "Parse error: the request body is not valid JSON").toString());
    }

    final MCPResponse response = dispatcher.dispatch(payload, user);

    // A null body is a one-way notification or response: 202 Accepted, no content.
    if (response.json() == null)
      return new ExecutionResponse(response.httpStatus(), "");

    return new ExecutionResponse(response.httpStatus(), response.json().toString());
  }

  private ExecutionResponse executeBatch(final JSONArray batch, final ServerSecurityUser user) {
    if (batch.isEmpty())
      return new ExecutionResponse(200, MCPDispatcher.errorObject(null, -32600, "Invalid Request: empty batch").toString());

    final JSONArray responses = dispatcher.dispatchBatch(batch, user);

    // Nothing to correlate means the batch held only notifications and/or responses: accepted, with no body.
    if (responses.isEmpty())
      return new ExecutionResponse(202, "");

    return new ExecutionResponse(200, responses.toString());
  }

  /**
   * Validates the {@code Origin} header, which MCP requires of a Streamable HTTP server to mitigate DNS
   * rebinding: a page loaded from an attacker origin resolving a hostname to a loopback address would
   * otherwise be able to drive a locally bound MCP endpoint.
   * <p>
   * A request with no {@code Origin} is accepted, because a non-browser MCP client sends none and it is the
   * browser that attaches the header a rebinding attack cannot forge. When present, the origin is accepted
   * only if it is explicitly configured or is a loopback address. The request {@code Host} header is not a
   * trust source: in a DNS-rebinding attack, the attacker-controlled Origin and Host names deliberately match.
   */
  private boolean isOriginAllowed(final HttpServerExchange exchange) {
    final String origin = exchange.getRequestHeaders().getFirst(Headers.ORIGIN);
    if (origin == null || origin.isBlank())
      return true;

    if (config.isOriginAllowed(origin))
      return true;

    final String originHost = hostOf(origin);
    if (originHost == null) {
      LogManager.instance().log(this, Level.FINE, "MCP[http] rejected malformed Origin '%s'", origin);
      return false;
    }

    if (isLoopback(originHost))
      return true;

    LogManager.instance().log(this, Level.FINE, "MCP[http] rejected cross-origin request from '%s'", origin);
    return false;
  }

  private static String hostOf(final String origin) {
    try {
      final String host = URI.create(origin.trim()).getHost();
      return host == null ? null : host.toLowerCase(Locale.ROOT);
    } catch (final IllegalArgumentException e) {
      return null;
    }
  }

  private static boolean isLoopback(final String host) {
    return "localhost".equals(host) || "127.0.0.1".equals(host) || "::1".equals(host) || "[::1]".equals(host);
  }
}
