package com.arcadedb.server.http.handler;

import com.arcadedb.server.http.WebSocketEventBus;
import io.undertow.websockets.core.*;
import org.json.JSONObject;

import java.io.IOException;
import java.util.UUID;

public class WebSocketReceiveListener extends AbstractReceiveListener {
  private final WebSocketEventBus webSocketEventBus;

  public enum ACTION {UNKNOWN, SUBSCRIBE, UNSUBSCRIBE}

  public WebSocketReceiveListener(WebSocketEventBus webSocketEventBus) {
    this.webSocketEventBus = webSocketEventBus;
  }

  @Override
  protected void onFullTextMessage(WebSocketChannel channel, BufferedTextMessage textMessage) throws IOException {
    JSONObject message = new JSONObject(textMessage.getData());
    String rawAction = message.getString("action");
    ACTION action = ACTION.UNKNOWN;
    try {
      action = ACTION.valueOf(rawAction.toUpperCase());
    } catch (IllegalArgumentException ignored) {
    }

    switch (action) {
      case SUBSCRIBE:
        this.webSocketEventBus.subscribe(message.getString("database"), message.optString("type", null), channel);
        this.sendAck(action, channel);
        break;
      case UNSUBSCRIBE:
        this.webSocketEventBus.unsubscribe(message.getString("database"), message.optString("type", null),
            (UUID) channel.getAttribute(WebSocketEventBus.CHANNEL_ID));
        this.sendAck(action, channel);
        break;
      default:
        sendError(String.format("Unknown action: %s", rawAction), channel);
        break;
    }
  }

  @Override
  protected void onClose(WebSocketChannel channel, StreamSourceFrameChannel frameChannel) throws IOException {
    // TODO: Make sure this gets called for zombie connections
    this.webSocketEventBus.unsubscribeAll((UUID) channel.getAttribute(WebSocketEventBus.CHANNEL_ID));
  }

  private void sendAck(ACTION action, WebSocketChannel channel) {
    String message = String.format("{\"action\": \"%s\", \"result\": \"ok\"}", action.toString().toLowerCase());
    WebSockets.sendText(message, channel, null);
  }

  private void sendError(String detail, WebSocketChannel channel) {
    String message = String.format("{\"result\": \"error\", \"detail\":\"%s\"}", detail);
    WebSockets.sendText(message, channel, null);
  }
}
