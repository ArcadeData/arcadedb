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
import com.arcadedb.serializer.json.JSONException;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.websockets.core.AbstractReceiveListener;
import io.undertow.websockets.core.BufferedTextMessage;
import io.undertow.websockets.core.StreamSourceFrameChannel;
import io.undertow.websockets.core.WebSocketChannel;
import io.undertow.websockets.core.WebSockets;

import java.io.IOException;
import java.util.Locale;
import java.util.Set;
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
  protected void onFullTextMessage(final WebSocketChannel channel, final BufferedTextMessage textMessage) throws IOException {
    try {
      final var message = new JSONObject(textMessage.getData());
      final var rawAction = message.getString("action", "");
      var action = ACTION.UNKNOWN;
      try {
        action = ACTION.valueOf(rawAction.toUpperCase(Locale.ENGLISH));
      } catch (final IllegalArgumentException ignored) {
      }

      switch (action) {
      case SUBSCRIBE:
        final var database = message.getString("database");
        final var user = (ServerSecurityUser) channel.getAttribute(WebSocketEventBus.USER);
        if (user == null || !user.canAccessToDatabase(database)) {
          sendError(channel, "Security error", "User does not have access to database '%s'.".formatted(database), null);
          break;
        }
        final var jsonChangeTypes = !message.isNull("changeTypes") ? message.getJSONArray("changeTypes") : null;
        final var changeTypes = jsonChangeTypes == null ?
            null :
            jsonChangeTypes.toList().stream().map(t -> ChangeEvent.TYPE.valueOf(t.toString().toUpperCase(Locale.ENGLISH))).collect(Collectors.toSet());
        this.webSocketEventBus.subscribe(database, message.getString("type", null), changeTypes, channel);
        this.sendAck(channel, action);
        break;
      case UNSUBSCRIBE:
        this.webSocketEventBus.unsubscribe(message.getString("database"), (UUID) channel.getAttribute(WebSocketEventBus.CHANNEL_ID));
        this.sendAck(channel, action);
        break;
      default:
        if ("".equals(rawAction)) {
          sendError(channel, "Message error", "Property 'action' is required.", null);
        } else {
          sendError(channel, "Unknown action", "%s is not a valid action.".formatted(rawAction), null);
        }
        break;
      }
    } catch (final JSONException e) {
      sendError(channel, "Unable to parse JSON", e.getMessage(), e);
    } catch (final DatabaseOperationException e) {
      sendError(channel, "Database error", e.getMessage(), e);
    } catch (final Exception e) {
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
    json.put("action", action.toString().toLowerCase(Locale.ENGLISH));
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
    return message.replace("\\\\", " ").replace("\n", " ");
  }

  private Level getErrorLogLevel() {
    return "development".equals(httpServer.getServer().getConfiguration().getValueAsString(GlobalConfiguration.SERVER_MODE)) ? Level.INFO : Level.FINE;
  }
}
