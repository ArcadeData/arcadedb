package com.arcadedb.server.http.ws;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.log.LogManager;
import com.arcadedb.server.http.HttpServer;
import io.undertow.websockets.core.*;
import org.json.JSONObject;

import java.io.IOException;
import java.util.UUID;
import java.util.logging.Level;

public class WebSocketReceiveListener extends AbstractReceiveListener {
  private final HttpServer        httpServer;
  private final WebSocketEventBus webSocketEventBus;

  public enum ACTION {UNKNOWN, SUBSCRIBE, UNSUBSCRIBE}

  public WebSocketReceiveListener(final HttpServer httpServer, final WebSocketEventBus webSocketEventBus) {
    this.httpServer = httpServer;
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

    try {
      switch (action) {
        case SUBSCRIBE:
          var changeTypes = message.optJSONArray("changeTypes");
          this.webSocketEventBus.subscribe(new EventWatcherSubscription(message.getString("database"),
              message.optString("type", null), changeTypes == null ? null : changeTypes.toList(), channel));
          this.sendAck(channel, action);
          break;
        case UNSUBSCRIBE:
          this.webSocketEventBus.unsubscribe(message.getString("database"), (UUID) channel.getAttribute(WebSocketEventBus.CHANNEL_ID));
          this.sendAck(channel, action);
          break;
        default:
          sendError(channel, "Unknown action", String.format("%s is not a valid action.", rawAction), null);
          break;
      }
    } catch (Exception e) {
      LogManager.instance().log(this, getErrorLogLevel(), "Error on command execution (%s)", e, getClass().getSimpleName());
      sendError(channel, "Internal error", e.getMessage(), e);
    }
  }

  @Override
  protected void onClose(final WebSocketChannel channel, final StreamSourceFrameChannel frameChannel) throws IOException {
    // TODO: Make sure unsubscribeAll gets called for zombie connections
    this.webSocketEventBus.unsubscribeAll((UUID) channel.getAttribute(WebSocketEventBus.CHANNEL_ID));
  }

  private void sendAck(final WebSocketChannel channel, final ACTION action) {
    final var json = new JSONObject("{\"result\": \"ok\"}");
    json.put("action", action.toString().toLowerCase());
    WebSockets.sendText(json.toString(), channel, null);
  }

  private void sendError(final WebSocketChannel channel, final String error, final String detail, final Throwable exception) {
    final var json = new JSONObject("{\"result\": \"error\"}");
    json.put("error", error);
    if (detail != null) json.put("detail", encodeError(detail));
    if (exception != null) json.put("exception", exception.getClass().getName());
    WebSockets.sendText(json.toString(), channel, null);
  }

  private String encodeError(final String message) {
    return message.replaceAll("\\\\", " ").replaceAll("\n", " ");
  }

  private Level getErrorLogLevel() {
    return "development".equals(httpServer.getServer().getConfiguration().getValueAsString(GlobalConfiguration.SERVER_MODE)) ? Level.INFO : Level.FINE;
  }
}
