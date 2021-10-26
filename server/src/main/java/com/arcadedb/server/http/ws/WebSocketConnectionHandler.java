package com.arcadedb.server.http.ws;

import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.handler.AbstractHandler;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;
import io.undertow.websockets.WebSocketConnectionCallback;
import io.undertow.websockets.WebSocketProtocolHandshakeHandler;

import java.util.UUID;

public class WebSocketConnectionHandler extends AbstractHandler {
  private final WebSocketEventBus webSocketEventBus;

  public WebSocketConnectionHandler(final HttpServer httpServer, final WebSocketEventBus webSocketEventBus) {
    super(httpServer);
    this.webSocketEventBus = webSocketEventBus;
  }

  @Override
  protected void execute(HttpServerExchange exchange, ServerSecurityUser user) throws Exception {
    var handler = new WebSocketProtocolHandshakeHandler((WebSocketConnectionCallback) (webSocketHttpExchange, channel) -> {
      channel.getReceiveSetter().set(new WebSocketReceiveListener(this.httpServer, webSocketEventBus));
      channel.setAttribute(WebSocketEventBus.CHANNEL_ID, UUID.randomUUID());
      channel.resumeReceives();
    });
    handler.handleRequest(exchange);
  }
}
