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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONException;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.ws.WebSocketEventBus;
import com.arcadedb.server.http.ws.WebSocketFrameSender;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.websockets.core.WebSocketChannel;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.UUID;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
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
 *     is absent), {@code transactionId} (optional; the {@code arcadedb-session-id} of a transaction already
 *     begun with {@code POST /api/v1/begin}, which goes with {@code transactionMode: "none"} - see issue #7403)
 *     and {@code options} ({@code targetType}, {@code transactionMode}, and the conflict options of
 *     issue #7404: {@code conflictMode}, {@code keyColumns}, {@code updateColumnsOnConflict},
 *     {@code validateOnly}). Answered with {@code started}, which echoes the modes the session runs under. A client-chosen id lives in ONE server-wide namespace, not one per user or per database -
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
 *     {@code outcome} says which of the two it was and whose {@code summary} carries the full-session totals -
 *     each chunk counted once, as its LATEST attempt left it, so a chunk that failed as a whole and was then
 *     replayed is counted only as the replay wrote it (issue #7471).
 *     A session running on an externally-managed transaction ({@code transactionMode: "none"}) answers
 *     {@code outcome: "detached"} to BOTH, because neither frame decides anything: the HTTP {@code /commit} or
 *     {@code /rollback} that owns the transaction does, and saying {@code "commit"} there would tell a client
 *     its rows were durable when nothing had been committed.</li>
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
  private static final String FRAME_QUEUE    = "arcadedb.ws.insert.frameQueue";
  /** Channel attribute recording that the connection-close hook is already registered. */
  private static final String CLOSE_HOOK     = "arcadedb.ws.insert.closeHook";
  /**
   * Channel attribute counting this connection's {@code start} frames that have been accepted for execution and
   * have not resolved yet - into a session or into a refusal (issue #7909). See
   * {@link #hasInsertFrameBudget(WebSocketChannel)}.
   */
  private static final String PENDING_STARTS = "arcadedb.ws.insert.pendingStarts";

  private final WebSocketInsertSessionManager sessionManager;
  private final ContextConfiguration          configuration;

  public WebSocketInsertProtocol(final WebSocketInsertSessionManager sessionManager,
      final ContextConfiguration configuration) {
    this.sessionManager = sessionManager;
    this.configuration = configuration;
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

    // Counted BEFORE the frame is queued, which is what holds the larger frame budget open across the gap
    // between this I/O thread and the worker that applies the frame (issue #7909). Released by the worker the
    // instant the start has resolved - see the 'start' arm of execute().
    final boolean start = "start".equals(action);
    if (start)
      pendingStarts(channel).incrementAndGet();

    if (!frameQueue(channel).submit(() -> execute(channel, channelId, user, action, message))) {
      // Refused for a full queue: the frame will never reach a worker, so its grant is released here instead.
      if (start)
        pendingStarts(channel).decrementAndGet();

      send(channel, error("Too many frames in flight",
          "This connection already has " + MAX_PENDING_FRAMES + " frames waiting to be applied. Wait for the"
              + " acknowledgements of the frames already sent before sending more", message.getString("sessionId", null),
          null));
    }
  }

  /**
   * Whether {@code channel} is currently entitled to the larger insert-frame heap budget - {@code
   * wsMaxInsertFrameSize} rather than {@code wsMaxControlFrameSize} (issue #7403), asked once per text frame by
   * {@code WebSocketReceiveListener.getMaxTextBufferSize()}.
   * <p>
   * Two things earn it, and nothing else does: a session open on this connection, and a {@code start} frame
   * accepted for execution whose outcome is not known yet. The second is what makes the first usable at all -
   * {@code start} is applied on a worker, and a client is free to pipeline its first {@code chunk} behind it
   * without waiting for {@code started}, so a budget keyed only on a registered session would lose that race.
   * <p>
   * The budget it replaces was a boolean raised from the frame's {@code action} string alone, before anything had
   * looked at the frame (issue #7909). It therefore survived every way a {@code start} can be refused - an
   * unknown database, a principal without access, a session id already taken, unparseable options, a dead
   * external transaction, a full frame queue - and was lowered only by a later frame whose action was literally
   * {@code commit} or {@code rollback}, so an idle sweep, a channel close or a failed {@code finish()} left it
   * raised for the life of the connection. One refused {@code start} frame multiplied that connection's
   * per-frame heap budget by 256 permanently, against documentation promising the opposite. Both halves of the
   * answer here are read from state that IS the truth rather than tracked alongside it, so there is nothing left
   * to leave stale.
   */
  public boolean hasInsertFrameBudget(final WebSocketChannel channel) {
    if (channel == null)
      return false;

    final AtomicInteger starts = (AtomicInteger) channel.getAttribute(PENDING_STARTS);
    if (starts != null && starts.get() > 0)
      return true;

    return sessionManager.hasSessionOnChannel((UUID) channel.getAttribute(WebSocketEventBus.CHANNEL_ID));
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
      channel.getWorker().execute(() -> sessionManager.closeChannelSessions(channel, channelId));
    } catch (final RejectedExecutionException e) {
      // The worker is going away with the connection; roll back here rather than not at all.
      sessionManager.closeChannelSessions(channel, channelId);
    }
  }

  private void execute(final WebSocketChannel channel, final UUID channelId, final ServerSecurityUser user,
      final String action, final JSONObject message) {
    try {
      switch (action) {
      case "start" -> {
        final WebSocketInsertSession session;
        try {
          session = sessionManager.start(user, channel, channelId,
              message.getString("database", null), message.getString("sessionId", null),
              message.getJSONObject("options", null), message.getString("transactionId", null));
        } finally {
          // The frame-budget grant this start was given on the I/O thread is released HERE, the instant the
          // outcome is known, and not after the answer is written (issue #7909). On success the session is
          // already registered, so the budget passes from the grant to the session with no gap; on a refusal
          // the budget is back down BEFORE the client is told, so a client that reacts to the error frame
          // cannot slip a large one in behind a grant it never earned.
          pendingStarts(channel).decrementAndGet();
        }

        final JSONObject started = new JSONObject();
        started.put("result", "ok");
        started.put("action", "started");
        started.put("sessionId", session.id);
        started.put("database", session.databaseName);
        started.put("transactionMode", session.options.transactionModeName());
        started.put("conflictMode", session.options.conflictModeName());
        started.put("validateOnly", session.options.validateOnly);
        if (session.getExternalTransactionId() != null)
          started.put("transactionId", session.getExternalTransactionId());
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

        // The cap a client actually reasons about (issue #7403): 'wsMaxInsertFrameSize' bounds the BYTES before
        // they are parsed, this one the ROWS after they are. Refused with an error frame that leaves the session
        // open and the watermark where it was, so a client that splits the batch resends it under the same
        // sequence and carries on.
        final int maxRows = configuration.getValueAsInteger(GlobalConfiguration.SERVER_WS_MAX_INSERT_CHUNK_ROWS);
        if (maxRows > 0 && records.length() > maxRows)
          throw new IllegalArgumentException(
              "Chunk " + chunkSeq + " carries " + records.length() + " records, more than the " + maxRows
                  + " allowed by '" + GlobalConfiguration.SERVER_WS_MAX_INSERT_CHUNK_ROWS.getKey()
                  + "'. Split it into smaller chunks");

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
    } catch (final JSONException | IllegalArgumentException | IllegalStateException | DuplicatedKeyException e) {
      // JSONException joins them because a frame whose 'options' carries a value of the wrong JSON TYPE is the
      // same class of mistake as one carrying a value of the wrong content, and answering the first with
      // "Internal error" and the second with "Insert session error" told a client the server had broken when it
      // had not. DuplicatedKeyException is the per_stream commit refusing a key the client sent twice (issue
      // #7404): the client's data, not the server's fault.
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

  /**
   * One of several independent senders on a {@code /ws} channel; see {@link WebSocketFrameSender} for why they
   * need no lock between them (issue #7423). Charged against the connection's shared answer-frame budget so a
   * client that never reads its {@code started}/{@code batchAck}/{@code committed}/error answers cannot pin
   * them on the server's heap forever (issue #8085).
   */
  private void send(final WebSocketChannel channel, final JSONObject message) {
    WebSocketFrameSender.sendBudgeted(channel, message.toString(),
        configuration.getValueAsLong(GlobalConfiguration.SERVER_WS_MAX_PENDING_CONTROL_BYTES));
  }

  private void registerCloseHook(final WebSocketChannel channel, final UUID channelId) {
    // The receive listener's onClose only fires on a courteous close frame. A connection that simply goes away
    // has to reach the same rollback, or its transaction is held until the idle sweep notices.
    if (channel.getAttribute(CLOSE_HOOK) != null)
      return;
    channel.setAttribute(CLOSE_HOOK, Boolean.TRUE);
    channel.addCloseTask(ch -> onChannelClosed(ch, channelId));
  }

  /**
   * This connection's in-flight {@code start} counter, created on demand. Created on the I/O thread, which
   * Undertow runs one frame at a time per connection, so no two threads can reach the creating branch for the
   * same channel - the same argument {@link #frameQueue} relies on.
   */
  private static AtomicInteger pendingStarts(final WebSocketChannel channel) {
    AtomicInteger starts = (AtomicInteger) channel.getAttribute(PENDING_STARTS);
    if (starts == null) {
      starts = new AtomicInteger();
      channel.setAttribute(PENDING_STARTS, starts);
    }
    return starts;
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
        // The dropped frames never run, so the finally that would release their frame-budget grant never runs
        // either. Released here instead, or a connection whose worker pool went away would hold the larger
        // budget for whatever life it has left (issue #7909).
        pendingStarts(channel).set(0);
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
