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
package com.arcadedb.server.http.ws;

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.handler.AbstractServerHttpHandler;
import com.arcadedb.server.http.handler.ExecutionResponse;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;
import io.undertow.websockets.WebSocketConnectionCallback;
import io.undertow.websockets.WebSocketProtocolHandshakeHandler;

import java.util.*;

public class WebSocketConnectionHandler extends AbstractServerHttpHandler {
  private final WebSocketEventBus webSocketEventBus;

  public WebSocketConnectionHandler(final HttpServer httpServer, final WebSocketEventBus webSocketEventBus) {
    super(httpServer);
    this.webSocketEventBus = webSocketEventBus;
  }

  @Override
  protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
      final JSONObject payload) throws Exception {
    final var handler = new WebSocketProtocolHandshakeHandler((WebSocketConnectionCallback) (webSocketHttpExchange, channel) -> {
      channel.getReceiveSetter().set(new WebSocketReceiveListener(this.httpServer, webSocketEventBus));
      channel.setAttribute(WebSocketEventBus.CHANNEL_ID, UUID.randomUUID());
      channel.resumeReceives();
    });
    handler.handleRequest(exchange);
    return null;
  }
}
