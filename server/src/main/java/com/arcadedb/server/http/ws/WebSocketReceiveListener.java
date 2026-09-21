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
import io.undertow.websockets.core.BufferedBinaryMessage;
import io.undertow.websockets.core.BufferedTextMessage;
import io.undertow.websockets.core.StreamSourceFrameChannel;
import io.undertow.websockets.core.WebSocketChannel;

import java.io.IOException;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.stream.Collectors;

public class WebSocketReceiveListener extends AbstractReceiveListener {
  private final    HttpServer              httpServer;
  private final    WebSocketEventBus       webSocketEventBus;
  private final    WebSocketInsertProtocol insertProtocol;
  /**
   * The connection this listener serves, attached by {@code WebSocketConnectionHandler} before the channel is
   * resumed, so {@link #getMaxTextBufferSize()} - a no-argument hook Undertow calls per message, before the
   * message exists - has something to ask the insert protocol about (issue #7909). One listener is created per
   * connection, so this never names two.
   */
  private volatile WebSocketChannel       channel;
  /**
   * Whether this connection has already been told that {@code /ws} carries no binary frames. One listener per
   * connection, so this is per connection - see {@link #onFullBinaryMessage} for why the answer is sent once and
   * not once per frame.
   */
  private final    AtomicBoolean          binaryFrameRefused = new AtomicBoolean();

  public enum ACTION {UNKNOWN, SUBSCRIBE, UNSUBSCRIBE}

  public WebSocketReceiveListener(final HttpServer httpServer, final WebSocketEventBus webSocketEventBus) {
    this.httpServer = httpServer;
    this.webSocketEventBus = webSocketEventBus;
    this.insertProtocol = httpServer.getInsertProtocol();
  }

  /**
   * Binds this listener to the connection it serves. Called once, on the handshake callback, before receives are
   * resumed - so the first frame's budget is already answerable. See {@link #channel}.
   */
  void attachTo(final WebSocketChannel channel) {
    this.channel = channel;
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
   * control budget until it has an insert session open or a {@code start} frame in flight - the question
   * {@code WebSocketInsertProtocol.hasInsertFrameBudget} answers, and the reason the tight bound is safe to keep
   * tight. Re-read per frame, so an operator raising the setting on a running server does not have to reconnect
   * its loaders, and so a connection whose session has ended is charged the control budget again from its very
   * next frame (issue #7909).
   */
  @Override
  protected long getMaxTextBufferSize() {
    return frameBudget(insertProtocol.hasInsertFrameBudget(channel) ?
        GlobalConfiguration.SERVER_WS_MAX_INSERT_FRAME_SIZE :
        GlobalConfiguration.SERVER_WS_MAX_CONTROL_FRAME_SIZE);
  }

  /**
   * Bounds what one BINARY frame may accumulate on the heap before {@link #onFullBinaryMessage} sees it (issue
   * #8065).
   * <p>
   * {@link #getMaxTextBufferSize()} bounded the text opcode and this class overrode nothing else, so BINARY kept
   * {@code AbstractReceiveListener}'s own default of {@code -1}: an authenticated client could open a binary
   * frame, never send its final fragment, and pin heap without ceiling for as long as the connection lived. Same
   * denial of service the text caps exist to prevent, through the other opcode.
   * <p>
   * Always the CONTROL budget, never the larger insert one, and that asymmetry with the text hook is the point.
   * The duplex insert protocol is dispatched from {@link #onFullTextMessage} alone, so no binary frame can ever
   * be a {@code chunk} and the larger budget would be a hole with no legitimate user - not even on a connection
   * that does have an insert session open. {@code /ws} in fact carries no binary frames at all, which is what
   * {@link #onFullBinaryMessage} answers; this bound is what keeps the refusal cheap, since
   * {@code BufferedBinaryMessage} checks the cap as it reads rather than after the frame is whole, and answers a
   * breach with a {@code 1009 TOO_BIG} close followed by an {@code IOException} that {@link #onError} turns into
   * a channel close.
   * <p>
   * PING, PONG and CLOSE need no override: {@code AbstractReceiveListener} caps those at RFC 6455's 125 bytes
   * through {@code final} accessors this class cannot widen.
   */
  @Override
  protected long getMaxBinaryBufferSize() {
    return frameBudget(GlobalConfiguration.SERVER_WS_MAX_CONTROL_FRAME_SIZE);
  }

  /**
   * The configured value of {@code setting}, translated into what Undertow's buffered messages want: they treat
   * anything {@code <= 0} as unbounded, which is what both settings document 0 to mean.
   */
  private long frameBudget(final GlobalConfiguration setting) {
    final long max = httpServer.getServer().getConfiguration().getValueAsLong(setting);
    return max > 0 ? max : -1;
  }

  /**
   * Answers a binary frame that stayed inside its budget (issue #8065).
   * <p>
   * {@code /ws} carries JSON text frames only, so there is nothing to parse here. Undertow's own default frees
   * the payload and returns, which left a client that picked the wrong opcode waiting on an answer that was
   * never coming; an error frame names the contract instead, and is the same shape the listener already uses for
   * a text frame whose {@code action} it does not know. The connection survives it deliberately: a stray binary
   * frame is a client mistake, not the attack - the attack is the SIZE of one, and that is refused by
   * {@link #getMaxBinaryBufferSize()} before this method is ever reached.
   * <p>
   * Once per connection, not once per frame, and that is a bound and not a convenience. A one-byte binary frame
   * costs a client six bytes on the wire and would cost the server a ~150-byte frame queued towards a peer that
   * may never read it - outbound frames are queued on the heap and nothing here charges them against a budget -
   * so answering every one of them would trade a buffering amplification for a queueing one. The client is told
   * the contract on its first binary frame; after that they are freed and dropped in silence.
   * <p>
   * The payload is pooled, so it is handed back before anything else happens, exactly as the overridden default
   * does.
   */
  @Override
  protected void onFullBinaryMessage(final WebSocketChannel channel, final BufferedBinaryMessage message) throws IOException {
    message.getData().free();
    if (binaryFrameRefused.compareAndSet(false, true))
      sendError(channel, "Binary frames are not supported",
          "The /ws protocol carries JSON text frames only. Send this payload as a text frame.", null);
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
        // The frame-size budget is decided by the protocol, from the session registry and the count of 'start'
        // frames still in flight, rather than raised and lowered from the action string here: see
        // getMaxTextBufferSize() and WebSocketInsertProtocol.hasInsertFrameBudget (issue #7909).
        insertProtocol.dispatch(channel, insertAction, message);
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
