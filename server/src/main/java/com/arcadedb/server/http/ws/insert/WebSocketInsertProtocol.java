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
package com.arcadedb.server.http.ws.insert;

import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONException;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.ws.WebSocketEventBus;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.websockets.core.WebSocketChannel;
import io.undertow.websockets.core.WebSockets;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.UUID;
import java.util.concurrent.RejectedExecutionException;
import java.util.logging.Level;

/**
 * The {@code /ws} half of the gRPC {@code InsertBidirectional} shape: the control frames issue #7382 was opened
 * about.
 * <p>
 * {@code POST /api/v1/batch} with {@code Accept: application/x-ndjson} (issue #7311) already carries the
 * acknowledgement half - chunk <i>n</i>'s outcome reaching the client while chunk <i>n+1</i> is still being sent.
 * What a request/response exchange cannot express is the other half: the client reacting to what the server said
 * and changing what it sends next INSIDE the same session. That is what these frames are for, and it is why
 * {@code /ws} is where they live rather than behind another HTTP route.
 *
 * <h2>Frames</h2>
 * Client to server, on the existing {@code /ws} connection, as the {@code action} of a JSON text frame:
 * <ul>
 * <li>{@code start} - {@code database} (required), {@code sessionId} (optional; the server generates one when it
 *     is absent) and {@code options} ({@code targetType}, {@code transactionMode}). Answered with
 *     {@code started}. A client-chosen id lives in ONE server-wide namespace, not one per user or per database -
 *     that is what makes "the same session id is refused to a second concurrent {@code start}" true whichever
 *     connection the second one arrives on. Two unrelated clients that both pick a house convention like
 *     {@code batch-1} will therefore collide; a client that does not need to name its own session should leave
 *     the field out and use the id the server generates.</li>
 * <li>{@code chunk} - {@code sessionId}, {@code chunkSeq} and {@code records}, an array of JSON objects. The
 *     sequence starts at 1 and is contiguous: re-sending one already applied is acknowledged as a replay,
 *     skipping ahead of the next one due is refused. A record takes its type from its own {@code @class} or from
 *     the session's {@code targetType}; an edge record names its endpoints with {@code @from} / {@code @to}
 *     (gRPC's {@code out} / {@code in} are accepted too). Answered with {@code batchAck}.</li>
 * <li>{@code commit} / {@code rollback} - {@code sessionId}. Answered with {@code committed}, whose
 *     {@code outcome} says which of the two it was and whose {@code summary} carries the full-session totals.</li>
 * </ul>
 * Server to client: {@code started}, {@code batchAck}, {@code committed}, and an {@code error} the server is free
 * to push unsolicited - which is what the idle sweep does when it rolls back a session the client walked away
 * from.
 *
 * <h2>Ordering and threads</h2>
 * Undertow delivers the frames of one connection serially, on an I/O thread that must not be blocked with
 * database work. Each frame is therefore handed to the Undertow worker pool through a per-connection queue that
 * runs one frame at a time in arrival order: a client may pipeline {@code start} and its first chunks without
 * waiting for {@code started}, and no session ever has two of its frames in flight at once. No thread is
 * dedicated to a session - the session's transaction is bound to whichever worker thread is running its current
 * frame, exactly as an {@code arcadedb-session-id} transaction is bound to whichever thread is running its
 * current HTTP request.
 *
 * @author Arcade Data Ltd
 */
public class WebSocketInsertProtocol {
  /**
   * How many frames one connection may have waiting to run. The acknowledgements ARE the flow control of this
   * protocol - a client is meant to look at {@code batchAck} before deciding what to send next - so a client
   * this far ahead of them is not being served, it is filling the server's heap with parsed chunks. Refused
   * with an {@code error} frame naming the limit rather than queued; the session itself stays open, so a client
   * that catches up can carry on. Bounding the SIZE of one frame is issue #7405.
   */
  private static final int    MAX_PENDING_FRAMES = 64;
  /** Channel attribute holding this connection's serial frame queue. */
  private static final String FRAME_QUEUE = "arcadedb.ws.insert.frameQueue";
  /** Channel attribute recording that the connection-close hook is already registered. */
  private static final String CLOSE_HOOK  = "arcadedb.ws.insert.closeHook";

  private final WebSocketInsertSessionManager sessionManager;

  public WebSocketInsertProtocol(final WebSocketInsertSessionManager sessionManager) {
    this.sessionManager = sessionManager;
    sessionManager.setExpiryListener((session, reason) -> {
      final WebSocketChannel channel = session.getChannel();
      if (channel != null && channel.isOpen())
        send(channel, error("Insert session expired", reason, session.id, null));
    });
  }

  /** The actions this protocol owns. Anything else is left to the subscription actions of {@code /ws}. */
  public static boolean handles(final String action) {
    return switch (action) {
    case "start", "chunk", "commit", "rollback" -> true;
    default -> false;
    };
  }

  /**
   * Queues one frame for execution on the Undertow worker pool. Returns immediately: the caller is the I/O thread
   * and must stay free to read the next frame.
   */
  public void dispatch(final WebSocketChannel channel, final String action, final JSONObject message) {
    final ServerSecurityUser user = (ServerSecurityUser) channel.getAttribute(WebSocketEventBus.USER);
    final UUID channelId = (UUID) channel.getAttribute(WebSocketEventBus.CHANNEL_ID);

    registerCloseHook(channel, channelId);

    if (!frameQueue(channel).submit(() -> execute(channel, channelId, user, action, message)))
      send(channel, error("Too many frames in flight",
          "This connection already has " + MAX_PENDING_FRAMES + " frames waiting to be applied. Wait for the"
              + " acknowledgements of the frames already sent before sending more", message.getString("sessionId", null),
          null));
  }

  /**
   * Rolls back whatever the connection left open. Idempotent, so both close paths may call it.
   * <p>
   * Runs on the worker rather than on the caller's thread: rolling a session back waits for the frame it may
   * still be applying, and both close paths (the receive listener's {@code onClose} and the channel's close
   * task) arrive on an I/O thread that other connections are waiting on.
   */
  public void onChannelClosed(final WebSocketChannel channel, final UUID channelId) {
    if (channelId == null)
      return;
    try {
      channel.getWorker().execute(() -> sessionManager.closeChannelSessions(channelId));
    } catch (final RejectedExecutionException e) {
      // The worker is going away with the connection; roll back here rather than not at all.
      sessionManager.closeChannelSessions(channelId);
    }
  }

  private void execute(final WebSocketChannel channel, final UUID channelId, final ServerSecurityUser user,
      final String action, final JSONObject message) {
    try {
      switch (action) {
      case "start" -> {
        final WebSocketInsertSession session = sessionManager.start(user, channelId, message.getString("database", null),
            message.getString("sessionId", null), message.getJSONObject("options", null));
        session.setChannel(channel);

        final JSONObject started = new JSONObject();
        started.put("result", "ok");
        started.put("action", "started");
        started.put("sessionId", session.id);
        started.put("database", session.databaseName);
        started.put("transactionMode", session.options.transactionModeName());
        send(channel, started);
      }
      case "chunk" -> {
        final WebSocketInsertSession session = sessionManager.resolve(user, channelId,
            message.getString("sessionId", null));
        final JSONArray records = message.getJSONArray("records", null);
        if (records == null)
          throw new IllegalArgumentException("Property 'records' is required and must be an array");

        // Refused rather than defaulted: the sequence is what makes a replayed chunk idempotent, and a missing
        // one would land on the initial watermark and be acknowledged as a duplicate - silently dropping every
        // record the frame carried.
        final long chunkSeq = message.getLong("chunkSeq", 0);
        if (chunkSeq < 1)
          throw new IllegalArgumentException("Property 'chunkSeq' is required and must be 1 or greater");

        send(channel, session.applyChunk(chunkSeq, records));
      }
      case "commit", "rollback" -> {
        final WebSocketInsertSession session = sessionManager.resolve(user, channelId,
            message.getString("sessionId", null));
        send(channel, sessionManager.finish(session, "commit".equals(action)));
      }
      // Unreachable through dispatch(), which only ever passes an action handles() claimed. It is here for the
      // next action added to handles() without a case of its own: answering an error frame is a great deal
      // easier to diagnose than falling through and silently acknowledging nothing.
      default -> send(channel, error("Unknown action", "%s is not a valid action.".formatted(action), null, null));
      }
    } catch (final SecurityException e) {
      send(channel, error("Security error", e.getMessage(), message.getString("sessionId", null), e));
    } catch (final JSONException | IllegalArgumentException | IllegalStateException e) {
      // JSONException joins them because a frame whose 'options' carries a value of the wrong JSON TYPE is the
      // same class of mistake as one carrying a value of the wrong content, and answering the first with
      // "Internal error" and the second with "Insert session error" told a client the server had broken when it
      // had not.
      send(channel, error("Insert session error", e.getMessage(), message.getString("sessionId", null), e));
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.FINE, "Error on /ws insert session action '%s'", e, action);
      send(channel, error("Internal error", e.getMessage(), message.getString("sessionId", null), e));
    }
  }

  private static JSONObject error(final String error, final String detail, final String sessionId, final Throwable e) {
    final JSONObject json = new JSONObject();
    json.put("result", "error");
    json.put("action", "error");
    json.put("error", error);
    // Flattened to one line so an error stays greppable next to the log entry that describes it. Only the line
    // breaks: the sibling encodeError() in WebSocketReceiveListener also collapses a literal double backslash,
    // which JSONObject.toString() already escapes on its own, so there is nothing left for that to fix here.
    if (detail != null)
      json.put("detail", detail.replace("\r", " ").replace("\n", " "));
    if (sessionId != null)
      json.put("sessionId", sessionId);
    if (e != null)
      json.put("exception", e.getClass().getName());
    return json;
  }

  private static void send(final WebSocketChannel channel, final JSONObject message) {
    if (channel.isOpen())
      WebSockets.sendText(message.toString(), channel, null);
  }

  private void registerCloseHook(final WebSocketChannel channel, final UUID channelId) {
    // The receive listener's onClose only fires on a courteous close frame. A connection that simply goes away
    // has to reach the same rollback, or its transaction is held until the idle sweep notices.
    if (channel.getAttribute(CLOSE_HOOK) != null)
      return;
    channel.setAttribute(CLOSE_HOOK, Boolean.TRUE);
    channel.addCloseTask(ch -> onChannelClosed(ch, channelId));
  }

  private static FrameQueue frameQueue(final WebSocketChannel channel) {
    FrameQueue queue = (FrameQueue) channel.getAttribute(FRAME_QUEUE);
    if (queue == null) {
      // Created on the I/O thread, which Undertow runs one frame at a time per connection, so no two threads
      // can reach this branch for the same channel.
      queue = new FrameQueue(channel);
      channel.setAttribute(FRAME_QUEUE, queue);
    }
    return queue;
  }

  /**
   * Runs a connection's frames one at a time, in arrival order, on the Undertow worker pool. Adds no thread of
   * its own: it holds a worker only while it has frames to run, and hands it back the moment it runs dry.
   */
  private static final class FrameQueue {
    private final WebSocketChannel channel;
    private final Deque<Runnable>  pending = new ArrayDeque<>();
    private       boolean          draining;

    private FrameQueue(final WebSocketChannel channel) {
      this.channel = channel;
    }

    /**
     * @return {@code false} when the connection is already {@link #MAX_PENDING_FRAMES} frames behind, in which
     * case the frame was NOT queued and the caller answers with an error
     */
    private boolean submit(final Runnable frame) {
      synchronized (this) {
        if (pending.size() >= MAX_PENDING_FRAMES)
          return false;
        pending.addLast(frame);
        if (draining)
          return true;
        draining = true;
      }
      try {
        channel.getWorker().execute(this::drain);
      } catch (final RejectedExecutionException e) {
        // The worker is shutting down: drop the queue rather than throwing out of the I/O callback. The
        // connection is going away with it, and the close hook rolls the session back.
        synchronized (this) {
          pending.clear();
          draining = false;
        }
      }
      return true;
    }

    private void drain() {
      while (true) {
        final Runnable next;
        synchronized (this) {
          next = pending.pollFirst();
          if (next == null) {
            draining = false;
            return;
          }
        }
        try {
          next.run();
        } catch (final Exception e) {
          LogManager.instance().log(this, Level.FINE, "Error on running a /ws insert frame", e);
        }
      }
    }
  }
}
