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
import com.arcadedb.server.http.ws.insert.WebSocketInsertProtocol;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.websockets.core.AbstractReceiveListener;
import io.undertow.websockets.core.BufferedTextMessage;
import io.undertow.websockets.core.StreamSourceFrameChannel;
import io.undertow.websockets.core.WebSocketChannel;

import java.io.IOException;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Level;
import java.util.stream.Collectors;

public class WebSocketReceiveListener extends AbstractReceiveListener {
  private final    HttpServer              httpServer;
  private final    WebSocketEventBus       webSocketEventBus;
  private final    WebSocketInsertProtocol insertProtocol;
  /**
   * Whether this connection is currently entitled to the larger insert-frame budget (issue #7403). Raised when
   * the connection's {@code start} frame is seen and dropped again when its {@code commit}/{@code rollback} is.
   * <p>
   * Read and written on the Undertow I/O thread only, which delivers the frames of one connection serially: the
   * {@code start} frame is therefore fully handled before the next frame begins accumulating, so a client that
   * pipelines {@code start} and its first {@code chunk} without waiting for {@code started} still gets the
   * larger budget for that chunk. It is deliberately NOT keyed off a registered session, which is created
   * asynchronously on a worker and would lose that race.
   */
  private volatile boolean                insertFrameBudget;

  public enum ACTION {UNKNOWN, SUBSCRIBE, UNSUBSCRIBE}

  public WebSocketReceiveListener(final HttpServer httpServer, final WebSocketEventBus webSocketEventBus) {
    this.httpServer = httpServer;
    this.webSocketEventBus = webSocketEventBus;
    this.insertProtocol = httpServer.getInsertProtocol();
  }

  /**
   * Bounds what one text frame may accumulate on the heap before {@link #onFullTextMessage} sees it (issue
   * #7403).
   * <p>
   * Undertow's default is {@code -1}, unbounded: an authenticated client that opened a connection and never sent
   * the final fragment of a text frame could pin an arbitrary amount of heap for as long as the connection
   * lived. {@code BufferedTextMessage} checks the cap as it reads rather than after the frame is whole, and
   * answers a breach with a {@code 1009 TOO_BIG} close frame followed by an {@code IOException} that
   * {@link #onError} turns into a channel close - so the bound is on what is actually allocated, not on what is
   * reported afterwards.
   * <p>
   * Two budgets rather than one: the control frames of {@code /ws} are a few hundred bytes and have no reason
   * ever to be large, while a {@code chunk} frame carries a whole batch of records. A connection is charged the
   * control budget until it dispatches a {@code start} frame, which is what makes the tight bound safe to keep
   * tight. Re-read per frame, so an operator raising the setting on a running server does not have to reconnect
   * its loaders.
   */
  @Override
  protected long getMaxTextBufferSize() {
    final GlobalConfiguration setting = insertFrameBudget ?
        GlobalConfiguration.SERVER_WS_MAX_INSERT_FRAME_SIZE :
        GlobalConfiguration.SERVER_WS_MAX_CONTROL_FRAME_SIZE;

    final long max = httpServer.getServer().getConfiguration().getValueAsLong(setting);
    // BufferedTextMessage treats anything <= 0 as unbounded, which is what the settings document 0 to mean.
    return max > 0 ? max : -1;
  }

  @Override
  protected void onFullTextMessage(final WebSocketChannel channel, final BufferedTextMessage textMessage) throws IOException {
    try {
      final var message = new JSONObject(textMessage.getData());
      final var rawAction = message.getString("action", "");

      // The duplex insert-session frames (issue #7382) run off this I/O thread: they touch the database, and a
      // commit taken here would stall every other connection this thread serves.
      final var insertAction = rawAction.toLowerCase(Locale.ENGLISH);
      if (WebSocketInsertProtocol.handles(insertAction)) {
        // The frame-size budget follows the session's lifetime on the wire rather than on the worker: see
        // getMaxTextBufferSize(). Raised before dispatch and dropped after it, both on this I/O thread.
        if ("start".equals(insertAction))
          insertFrameBudget = true;
        insertProtocol.dispatch(channel, insertAction, message);
        if ("commit".equals(insertAction) || "rollback".equals(insertAction))
          insertFrameBudget = false;
        return;
      }

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
    // An insert session the client walked away from without committing is rolled back, never left open.
    this.insertProtocol.onChannelClosed(channel, channelId);
    // This override has never called super since the class was written, so the CLOSE frame was never read and
    // the closing handshake never completed: the server simply stopped answering and left the peer to notice.
    // Undertow's own onClose buffers the frame and replies with a close of its own, which is what a client
    // waiting for the handshake needs - the JDK WebSocket client the new RemoteInsertSession is built on waits
    // 30 seconds for it before giving up (issue #7403). The Undertow-based test client never noticed, because
    // its close() follows sendCloseBlocking with a forced channel close.
    super.onClose(channel, frameChannel);
  }

  private void sendAck(final WebSocketChannel channel, final ACTION action) {
    final var json = new JSONObject("{\"result\": \"ok\"}");
    json.put("action", action.toString().toLowerCase(Locale.ENGLISH));
    WebSocketFrameSender.send(channel, json.toString(), null);
  }

  private void sendError(final WebSocketChannel channel, final String error, final String detail, final Throwable exception) {
    final var json = new JSONObject("{\"result\": \"error\"}");
    json.put("error", error);
    if (detail != null)
      json.put("detail", encodeError(detail));
    if (exception != null)
      json.put("exception", exception.getClass().getName());
    WebSocketFrameSender.send(channel, json.toString(), null);
  }

  private String encodeError(final String message) {
    return message.replace("\\\\", " ").replace("\n", " ");
  }

  private Level getErrorLogLevel() {
    return "development".equals(httpServer.getServer().getConfiguration().getValueAsString(GlobalConfiguration.SERVER_MODE)) ? Level.INFO : Level.FINE;
  }
}
