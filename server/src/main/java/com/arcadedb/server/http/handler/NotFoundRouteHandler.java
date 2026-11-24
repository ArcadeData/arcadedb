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
import com.arcadedb.server.http.IdempotencyCache;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import io.undertow.util.HttpString;
import io.undertow.util.StatusCodes;

/**
 * The answer for a path the server does not serve: the fallback of the routers, so that a request that reaches no
 * route is answered like every other answer of the API (issue #12410).
 * <p>
 * Undertow's own fallback is an empty body with no header at all: the request never reaches a handler, so the
 * envelope the handlers put on every response - the correlation id and the replay-protection header - was never
 * written. The HTTP API reference promises that an erroneous request has an {@code error} field and that every
 * response carries the {@code X-Request-Id} and the {@code X-ArcadeDB-Replay-Protection} headers, so this answer
 * writes them.
 * <p>
 * The message names no path and repeats nothing the caller sent: the request path is the caller's, and a 404 that
 * quoted it would turn the answer into a reflection of the request. The correlation id is the one the client sent
 * in {@code X-Request-Id}, or a generated one, exactly as a handler's answer does.
 */
public class NotFoundRouteHandler implements HttpHandler {
  /**
   * The {@code error} of the answer. It says what the server could not find, not what the client asked for.
   */
  public static final String ERROR_MESSAGE = "The requested resource does not exist";

  private static final HttpString REQUEST_ID_HEADER       = HttpString.tryFromString(IdempotencyCache.HEADER_REQUEST_ID);
  private static final HttpString REPLAY_PROTECTION_HEADER = HttpString.tryFromString(IdempotencyCache.HEADER_REPLAY_PROTECTION);

  @Override
  public void handleRequest(final HttpServerExchange exchange) {
    final String requestId = AbstractServerHttpHandler.sanitizeRequestId(
        exchange.getRequestHeaders().getFirst(IdempotencyCache.HEADER_REQUEST_ID));
    final String correlationId = requestId != null ? requestId : AbstractServerHttpHandler.generateCorrelationId();

    exchange.getResponseHeaders().put(REQUEST_ID_HEADER, correlationId);
    exchange.getResponseHeaders().put(REPLAY_PROTECTION_HEADER, IdempotencyCache.PROCESS_ID);
    exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
    exchange.setStatusCode(StatusCodes.NOT_FOUND);

    final JSONObject json = new JSONObject();
    json.put("error", ERROR_MESSAGE);
    json.put("requestId", correlationId);

    exchange.getResponseSender().send(json.toString());
  }
}
