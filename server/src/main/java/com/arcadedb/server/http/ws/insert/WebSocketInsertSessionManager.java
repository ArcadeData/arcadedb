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

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.http.HttpSession;
import com.arcadedb.server.http.HttpSessionManager;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.websockets.core.WebSocketChannel;

import java.util.ArrayList;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.logging.Level;

/**
 * Registry of the duplex insert sessions open on {@code /ws} (issue #7382).
 * <p>
 * A session is identified by an id the client may choose or leave to the server, and lives until the client ends
 * it with a {@code commit} or {@code rollback} frame. Every other way it can end - the connection dropping, the
 * idle sweep, the server stopping - rolls it back, because none of them is the client saying what it wanted.
 * <p>
 * At most ONE session is open per WebSocket channel at a time, which is the constraint gRPC gets for free (a
 * session there IS a stream). It bounds the number of live transactions by the number of connections instead of
 * leaving it to whatever a client chooses to open, and it makes "the same session id is rejected by a second
 * concurrent start" true for both shapes of the race: a second {@code start} on the same channel, and one on a
 * different channel naming an id that is already taken.
 *
 * @author Arcade Data Ltd
 */
public class WebSocketInsertSessionManager {
  /**
   * Channel attribute set by {@link #closeChannelSessions}, so a {@code start} frame drained after the connection
   * closed is refused instead of opening a session nothing will ever deregister (issue #7471).
   */
  private static final String CHANNEL_CLOSED_ATTRIBUTE = "arcadedb.ws.insert.channelClosed";

  private final ArcadeDBServer                          server;
  /**
   * Where a {@code start} frame's {@code transactionId} is resolved (issue #7403). The same registry
   * {@code POST /api/v1/begin} mints into and {@code DatabaseAbstractHandler} resolves against, reached through
   * {@link HttpSessionManager#getSessionById} rather than around it so a {@code /ws} client can only adopt a
   * transaction its own principal opened.
   */
  private final HttpSessionManager                      httpSessionManager;
  private final Map<String, WebSocketInsertSession>     sessions = new ConcurrentHashMap<>();
  /** One session per channel, so a channel that opens a second one is refused rather than tracked. */
  private final Map<UUID, String>                       byChannel = new ConcurrentHashMap<>();
  private final long                                    idleTimeoutMs;
  private final Timer                                   timer;
  /** Set by {@link #close()}, so a frame racing the shutdown cannot open a session nothing will ever roll back. */
  private volatile boolean                              closed;
  /**
   * Notified when a session is rolled back by something other than its own client, so the client can be told.
   * Its write is one of several independent senders on a {@code /ws} channel; {@code WebSocketFrameSender}
   * records why they need no lock between them (issue #7423).
   */
  private volatile ExpiryListener                       expiryListener = (session, reason) -> {
  };

  /** Called with a session the server ended on its own, so the protocol layer can push an unsolicited error. */
  public interface ExpiryListener {
    void onSessionCancelled(WebSocketInsertSession session, String reason);
  }

  public WebSocketInsertSessionManager(final ArcadeDBServer server, final HttpSessionManager httpSessionManager,
      final long idleTimeoutMs) {
    this.server = server;
    this.httpSessionManager = httpSessionManager;
    this.idleTimeoutMs = idleTimeoutMs;

    this.timer = new Timer("arcadedb-ws-insert-session-sweep", true);
    this.timer.schedule(new TimerTask() {
      @Override
      public void run() {
        try {
          checkSessionsValidity();
        } catch (final Exception e) {
          LogManager.instance().log(this, Level.FINE, "Error on sweeping /ws insert sessions", e);
        }
      }
    }, idleTimeoutMs, idleTimeoutMs);
  }

  public void setExpiryListener(final ExpiryListener expiryListener) {
    this.expiryListener = expiryListener;
  }

  /**
   * Opens a session and begins its transaction.
   *
   * @param channel     the connection the session belongs to, attached before the session is registered so the
   *                    idle sweep always has a worker to dispatch an expiry to
   * @param requestedId           the id the client asked for, or {@code null}/blank to have the server generate one
   * @param externalTransactionId the {@code arcadedb-session-id} of a transaction already begun over HTTP that
   *                              this session is to write into instead of opening one of its own (issue #7403),
   *                              or {@code null}/blank for a server-managed session
   *
   * @throws IllegalStateException    when the channel already has a session, the channel has closed (or is
   *                                  closing) underneath a frame that was still queued, the requested id is taken,
   *                                  or the named external transaction is unknown or expired - which is the
   *                                  {@code FAILED_PRECONDITION} the gRPC path answers, never a silent
   *                                  fall-through to a server-managed transaction
   * @throws SecurityException        when the principal cannot access the database
   * @throws IllegalArgumentException when the options are not ones this server implements
   */
  public WebSocketInsertSession start(final ServerSecurityUser user, final WebSocketChannel channel,
      final UUID channelId, final String databaseName, final String requestedId, final JSONObject rawOptions,
      final String externalTransactionId) {
    if (closed)
      throw new IllegalStateException("The server is shutting down and is not opening new insert sessions");

    if (databaseName == null || databaseName.isBlank())
      throw new IllegalArgumentException("Property 'database' is required to start an insert session");

    if (user == null || !user.canAccessToDatabase(databaseName))
      throw new SecurityException("User does not have access to database '" + databaseName + "'.");

    final String externalId =
        externalTransactionId == null || externalTransactionId.isBlank() ? null : externalTransactionId;

    final InsertSessionOptions options = InsertSessionOptions.parse(rawOptions, externalId != null);

    // Resolved BEFORE the channel is claimed, so a start refused over its transaction leaves nothing registered.
    // getSessionById() is the ownership gate: it answers null for a session owned by another principal exactly as
    // it does for one that never existed, which is why an unknown id and someone else's id are refused alike.
    final HttpSession externalSession = externalId == null ? null : resolveExternalTransaction(user, externalId, databaseName);

    final String id = requestedId == null || requestedId.isBlank() ? UUID.randomUUID().toString() : requestedId;

    // Claim the channel BEFORE the id: a client that pipelines two starts must be refused on the second one
    // whichever id it chose, and claiming the id first would leave it registered to a session that is refused.
    //
    // Through compute() rather than putIfAbsent() because this claim has a SECOND invariant to keep, and the two
    // have to be decided together (issue #7471): the channel must still be one this connection can have a session
    // on. A 'start' frame still queued when the connection dies is drained by the frame queue's worker task, while
    // closeChannelSessions() runs on a worker task of its own, and nothing orders the two - so the start could open
    // a session on a dead channel AFTER the sweep that would have cleaned it up had already been and gone, leaving
    // an open transaction held until the idle sweep reclaimed it, once per abrupt disconnect. compute() and the
    // compute() in closeChannelSessions() are mutually exclusive on this key, which decides the race either way
    // round: a start that wins registers a session the close then rolls back, and a start that loses finds the
    // marker the close left on the channel and is refused.
    byChannel.compute(channelId, (key, alreadyOnChannel) -> {
      if (alreadyOnChannel != null)
        throw new IllegalStateException(
            "This connection already has insert session '" + alreadyOnChannel + "' open. Commit or roll it back first");

      if (channelIsGone(channel))
        throw new IllegalStateException("This connection is closing and is not opening new insert sessions");

      return id;
    });

    final DatabaseInternal database;
    final WebSocketInsertSession session;
    try {
      database = server.getDatabase(databaseName, false, false);
      session = new WebSocketInsertSession(id, database, user, channelId, options, externalId, externalSession);
      // Before it is registered, not after: a session the sweep can see must already know where to send its
      // expiry, or the sweep would have nothing to dispatch to and would roll it back on its own thread.
      session.setChannel(channel);

      if (sessions.putIfAbsent(id, session) != null)
        throw new IllegalStateException("Insert session '" + id + "' already exists");
    } catch (final RuntimeException e) {
      byChannel.remove(channelId, id);
      throw e;
    }

    try {
      session.begin();
    } catch (final RuntimeException e) {
      sessions.remove(id, session);
      byChannel.remove(channelId, id);
      throw e;
    }

    return session;
  }

  /**
   * Resolves the HTTP transaction a {@code start} frame named, refusing every way it can fail to be one this
   * client may write into (issue #7403).
   * <p>
   * The database check is not redundant with the access check above it: a principal with access to two databases
   * could otherwise open a transaction on one with {@code /begin} and have the session write into the other,
   * since the frame names the database and the session id independently.
   */
  private HttpSession resolveExternalTransaction(final ServerSecurityUser user, final String transactionId,
      final String databaseName) {
    final HttpSession externalSession = httpSessionManager.getSessionById(user, transactionId);
    if (externalSession == null)
      throw new IllegalStateException("Transaction '" + transactionId
          + "' not found or expired. Begin one with 'POST /api/v1/begin' and name the id it returns");

    if (externalSession.transaction == null || !externalSession.transaction.isActive())
      throw new IllegalStateException("Transaction '" + transactionId + "' is no longer active");

    final String transactionDatabase = externalSession.transaction.getDatabase().getName();
    if (!transactionDatabase.equals(databaseName))
      throw new IllegalArgumentException("Transaction '" + transactionId + "' belongs to database '"
          + transactionDatabase + "', not to '" + databaseName + "'");

    return externalSession;
  }

  /**
   * Resolves a session for a frame that claims to belong to it. A session is only reachable from the connection
   * that opened it and by the principal that opened it: a frame that names someone else's session is refused
   * rather than served, the same rule {@code HttpSessionManager.getSessionById} enforces for {@code /begin}
   * transactions.
   */
  public WebSocketInsertSession resolve(final ServerSecurityUser user, final UUID channelId, final String sessionId) {
    if (sessionId == null || sessionId.isBlank())
      throw new IllegalArgumentException("Property 'sessionId' is required");

    final WebSocketInsertSession session = sessions.get(sessionId);
    if (session == null || session.isClosed())
      throw new IllegalStateException("Insert session '" + sessionId + "' not found or expired");

    if (!session.channelId.equals(channelId))
      throw new SecurityException("Insert session '" + sessionId + "' belongs to another connection");

    if (user == null || !user.equals(session.user))
      throw new SecurityException("Insert session '" + sessionId + "' belongs to another user");

    return session;
  }

  /** Ends a session the client itself terminated, and answers with the {@code committed} frame. */
  public JSONObject finish(final WebSocketInsertSession session, final boolean commit) {
    try {
      return session.finish(commit);
    } finally {
      unregister(session);
    }
  }

  /**
   * Rolls back and forgets every session opened on a channel, and marks the channel so no LATER {@code start} can
   * open one on it. Called when the connection closes, whether the client said goodbye or the socket simply went
   * away. Idempotent: both close paths may call it.
   * <p>
   * The marker is what makes this safe against a {@code start} frame that is still queued when the connection dies
   * (issue #7471). It is set BEFORE the registry is cleared, and read inside {@link #start}'s claim of the same
   * key, so the two orderings are the only two possible: this call sees a session and rolls it back, or the start
   * sees the marker and is refused. It lives on the CHANNEL rather than in a set here, so it is reclaimed with the
   * connection instead of accumulating one entry per connection the server has ever served.
   *
   * @param channel   the connection that closed, or {@code null} when it is not reachable - in which case the
   *                  marker cannot be set and only the sessions already registered are rolled back
   * @param channelId the id the sessions of that connection are registered under
   */
  public void closeChannelSessions(final WebSocketChannel channel, final UUID channelId) {
    if (channel != null)
      channel.setAttribute(CHANNEL_CLOSED_ATTRIBUTE, Boolean.TRUE);

    // compute(), not remove(), to be explicit that this is the same per-key critical section start() claims under.
    final String[] removed = new String[1];
    byChannel.compute(channelId, (key, sessionId) -> {
      removed[0] = sessionId;
      return null;
    });

    if (removed[0] == null)
      return;

    final WebSocketInsertSession session = sessions.remove(removed[0]);
    if (session != null && session.cancel())
      LogManager.instance().log(this, Level.FINE,
          "Rolled back /ws insert session %s: its connection closed before it was committed", removed[0]);
  }

  /**
   * Whether {@code channel} can no longer carry a session: either it is already closed, or
   * {@link #closeChannelSessions} has marked it as closing.
   * <p>
   * Both tests are needed. {@code isOpen()} alone misses the courteous-close path, where the receive listener's
   * {@code onClose} reaches {@link #closeChannelSessions} while Undertow is still completing the closing handshake
   * and the channel therefore still reports itself open. The marker alone would miss a channel that died before
   * any close handler had run.
   */
  private static boolean channelIsGone(final WebSocketChannel channel) {
    return channel != null && (!channel.isOpen() || channel.getAttribute(CHANNEL_CLOSED_ATTRIBUTE) != null);
  }

  /**
   * Hands every session idle for longer than the configured timeout to the worker pool to be rolled back and its
   * client told. The sweep itself decides and dispatches only: rolling back a large {@code PER_STREAM}
   * transaction and writing to a socket both take as long as they take, and there is one sweep thread for the
   * whole server, so doing either here would delay every other session's expiry behind the slowest one. Same
   * reason no frame is applied on the I/O thread.
   *
   * @return how many sessions this tick handed over. A session that turns out to be busy is handed over and then
   * left registered, so this counts decisions, not rollbacks
   */
  public int checkSessionsValidity() {
    if (sessions.isEmpty())
      return 0;

    int dispatched = 0;
    for (final WebSocketInsertSession session : new ArrayList<>(sessions.values()))
      if (session.elapsedFromLastUse() > idleTimeoutMs) {
        dispatched++;
        dispatchExpiry(session);
      }
    return dispatched;
  }

  private void dispatchExpiry(final WebSocketInsertSession session) {
    final WebSocketChannel channel = session.getChannel();
    if (channel == null)
      // Not reachable: start() attaches the channel before registering the session. Left as a skip rather than a
      // fallback because the fallback would be to expire on the sweep thread, which is the one thing this class
      // promises not to do; a session somehow without a channel is left for the next tick instead.
      return;

    try {
      channel.getWorker().execute(() -> expire(session));
    } catch (final RejectedExecutionException e) {
      // The worker is going away with the connection: expire here rather than not at all. Bounded by that
      // connection dying, unlike the missing-channel case above, which would repeat every tick.
      expire(session);
    }
  }

  /**
   * Rolls a session back BEFORE forgetting it, and only if it is genuinely idle: a session whose frame is still
   * running is left registered so the next tick can look again. Removing it first and then finding it busy would
   * untrack a live transaction that nothing else would ever roll back. Safe to run twice for the same session -
   * a later tick can dispatch one that an earlier tick is still expiring - because only the call that actually
   * closes it reports anything.
   */
  private void expire(final WebSocketInsertSession session) {
    final WebSocketInsertSession.CancelOutcome outcome = session.cancelIfIdle();
    if (outcome == WebSocketInsertSession.CancelOutcome.BUSY)
      return;

    sessions.remove(session.id, session);
    byChannel.remove(session.channelId, session.id);

    if (outcome == WebSocketInsertSession.CancelOutcome.CANCELLED) {
      LogManager.instance().log(this, Level.FINE, "Rolled back /ws insert session %s after %dms of inactivity",
          session.id, idleTimeoutMs);
      try {
        expiryListener.onSessionCancelled(session,
            "Insert session '" + session.id + "' was rolled back after " + idleTimeoutMs + "ms of inactivity");
      } catch (final Exception e) {
        // Best effort: the connection this session belonged to may already be gone.
      }
    }
  }

  /** Rolls back every open session. The server is stopping, so no client is going to say what it wanted. */
  public void close() {
    closed = true;
    timer.cancel();
    for (final WebSocketInsertSession session : new ArrayList<>(sessions.values())) {
      sessions.remove(session.id, session);
      byChannel.remove(session.channelId, session.id);
      session.cancel();
    }
  }

  public int getOpenSessionCount() {
    return sessions.size();
  }

  private void unregister(final WebSocketInsertSession session) {
    sessions.remove(session.id, session);
    byChannel.remove(session.channelId, session.id);
  }
}
