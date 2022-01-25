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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.log.LogManager;
import com.arcadedb.server.http.HttpServer;
import io.undertow.websockets.core.*;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.UUID;
import java.util.logging.Level;
import java.util.stream.Collectors;

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
    try {
      var message = new JSONObject(textMessage.getData());
      var rawAction = message.optString("action");
      var action = ACTION.UNKNOWN;
      try {
        action = ACTION.valueOf(rawAction.toUpperCase());
      } catch (IllegalArgumentException ignored) {
      }

      switch (action) {
      case SUBSCRIBE:
        var jsonChangeTypes = message.optJSONArray("changeTypes");
        var changeTypes = jsonChangeTypes == null ?
            null :
            jsonChangeTypes.toList().stream().map(t -> ChangeEvent.TYPE.valueOf(t.toString().toUpperCase())).collect(Collectors.toSet());
        this.webSocketEventBus.subscribe(message.getString("database"), message.optString("type", null), changeTypes, channel);
        this.sendAck(channel, action);
        break;
      case UNSUBSCRIBE:
        this.webSocketEventBus.unsubscribe(message.getString("database"), (UUID) channel.getAttribute(WebSocketEventBus.CHANNEL_ID));
        this.sendAck(channel, action);
        break;
      default:
        if (rawAction.equals("")) {
          sendError(channel, "Message error", "Property 'action' is required.", null);
        } else {
          sendError(channel, "Unknown action", String.format("%s is not a valid action.", rawAction), null);
        }
        break;
      }
    } catch (JSONException e) {
      sendError(channel, "Unable to parse JSON", e.getMessage(), e);
    } catch (DatabaseOperationException e) {
      sendError(channel, "Database error", e.getMessage(), e);
    } catch (Exception e) {
      LogManager.instance().log(this, getErrorLogLevel(), "Error on command execution (%s)", e, getClass().getSimpleName());
      sendError(channel, "Internal error", e.getMessage(), e);
    }
  }

  @Override
  protected void onClose(final WebSocketChannel channel, final StreamSourceFrameChannel frameChannel) throws IOException {
    final var channelId = (UUID) channel.getAttribute(WebSocketEventBus.CHANNEL_ID);
    this.webSocketEventBus.unsubscribeAll(channelId);
  }

  private void sendAck(final WebSocketChannel channel, final ACTION action) {
    final var json = new JSONObject("{\"result\": \"ok\"}");
    json.put("action", action.toString().toLowerCase());
    WebSockets.sendText(json.toString(), channel, null);
  }

  private void sendError(final WebSocketChannel channel, final String error, final String detail, final Throwable exception) {
    final var json = new JSONObject("{\"result\": \"error\"}");
    json.put("error", error);
    if (detail != null)
      json.put("detail", encodeError(detail));
    if (exception != null)
      json.put("exception", exception.getClass().getName());
    WebSockets.sendText(json.toString(), channel, null);
  }

  private String encodeError(final String message) {
    return message.replaceAll("\\\\", " ").replaceAll("\n", " ");
  }

  private Level getErrorLogLevel() {
    return "development".equals(httpServer.getServer().getConfiguration().getValueAsString(GlobalConfiguration.SERVER_MODE)) ? Level.INFO : Level.FINE;
  }
}
