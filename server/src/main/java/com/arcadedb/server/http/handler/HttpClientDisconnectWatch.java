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
package com.arcadedb.server.http.handler;

import com.arcadedb.log.LogManager;
import com.arcadedb.query.RunningQuery;
import io.undertow.server.HttpServerExchange;
import io.undertow.server.ServerConnection;
import io.undertow.server.protocol.http.HttpServerConnection;
import io.undertow.util.ImmediatePooledByteBuffer;
import org.xnio.ChannelListener;
import org.xnio.Options;
import org.xnio.conduits.ConduitStreamSourceChannel;
import org.xnio.conduits.StreamSourceConduit;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.logging.Level;

/**
 * Terminates the statement of an HTTP request whose client has gone away (issue #9689): a client that gives up - its own
 * deadline, a killed process, {@code curl --max-time} - no longer leaves the server working on an answer nobody will
 * read until the statement completes or reaches {@code arcadedb.command.timeout}.
 * <p>
 * <b>How a disconnect is seen.</b> While a request runs, Undertow reads nothing from its connection, so a peer closing it
 * goes unnoticed until the response is written. Once the request body has been read, the watch listens on the connection
 * for the one thing a client may still send: nothing, until the response comes. A read that answers end-of-stream or
 * fails is the client gone, which terminates the statement - Undertow itself takes end-of-stream for a closed connection.
 * Anything else is the next request of a client that pipelines: it is handed back to Undertow untouched
 * ({@code ungetRequestBytes}) and the watch stops, the client being evidently still there.
 * <p>
 * <b>What it does not cover.</b> TLS connections, whose engine must not be read on the I/O thread while a worker writes
 * through it, HTTP/2 streams, a request still reading its body, and a client that already pipelined its next request:
 * for those it does not arm, and the request runs as before. A client that half-closes its side of the connection after
 * sending the request and still waits for the response cannot be told from one that left, and is taken for one that
 * left; {@code arcadedb.server.httpTerminateOnClientDisconnect=false} turns the watch off for such clients.
 * <p>
 * <b>Why the swap is safe.</b> Undertow suspends reads on the connection once it has parsed the request and resumes them
 * only when the exchange completes, and the watch is disarmed before that, under the same monitor its reads take: when
 * the statement's entry closes, before a buffered response is written, or - for an answer the handler streams itself,
 * which completes the exchange inside the handler - by an exchange-completion listener, which Undertow runs before it
 * hands the connection to the next request. No read of the watch can therefore overlap Undertow's own, and bytes the
 * watch did read are back in the connection before Undertow looks for the next request.
 * <p>
 * <b>The read timeout.</b> The connection carries {@code arcadedb.network.socketTimeout} as its read timeout, which
 * Undertow enforces with a timer while reads are resumed: once that much time passes with no byte from the client, it fails
 * the next read and closes the connection. A client waiting for its answer sends nothing, so a statement longer than the
 * socket timeout lost its client to the timer (issue #9953). The watch lifts the read timeout while it is armed and puts it
 * back when it is disarmed, before Undertow resumes reads for the next request: an idle keep-alive connection is still
 * closed after the timeout, and a client waiting for a long statement is not. A connection whose read timeout cannot be
 * lifted is not watched. The timeout is put back on every path that ends the watch, a client gone or a pipelined request
 * included: both stop the reads, and {@link #disarm()} - which always runs - restores it whether or not the watch was
 * still armed. The trade: a client that vanishes without closing its connection (no FIN or RST reaches the server) is
 * no longer cut off by the read timeout while its statement runs, as it was not before the watch existed. The HTTP
 * listener turns TCP keep-alive on for that case: once the operating system finds the peer dead, the watch's read fails
 * and the statement is terminated. The keep-alive timing is the operating system's (on Linux, two hours of silence by
 * default), so in practice such a statement still runs to its end or to {@code arcadedb.command.timeout}, which is what
 * bounds it; the keep-alive only stops it from holding the connection forever when no command timeout is set.
 * <p>
 * Costs one listener swap per request and nothing while the client is quiet: the selector wakes the watch only when the
 * client sends or closes.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class HttpClientDisconnectWatch implements ChannelListener<ConduitStreamSourceChannel> {
  private final HttpServerConnection                                   connection;
  private final ConduitStreamSourceChannel                             channel;
  private final StreamSourceConduit                                    socket;
  private final ChannelListener<? super ConduitStreamSourceChannel> previous;
  private final RunningQuery                                           runningQuery;
  // One byte is all a probe reads: end-of-stream, or the first byte of a pipelined request, handed to Undertow whole
  private final ByteBuffer                                             probe = ByteBuffer.allocate(1);
  private       boolean                                              armed = true;
  // The read timeout the connection had before the watch lifted it, put back on disarm(); null when there is none to restore
  // Volatile: set by arm() on the worker thread, read by disarm(), which the exchange-completion listener may run on the I/O
  // thread
  private volatile Integer                                             liftedReadTimeout;

  private HttpClientDisconnectWatch(final HttpServerConnection connection, final RunningQuery runningQuery) {
    this.connection = connection;
    this.channel = connection.getChannel().getSourceChannel();
    this.socket = connection.getOriginalSourceConduit();
    this.previous = channel.getReadListener();
    this.runningQuery = runningQuery;
  }

  /**
   * Starts watching the connection of {@code exchange} for its client going away, or answers {@code null} when the
   * request is not one it can watch (see the class comment). The caller {@link #disarm() disarms} it once the
   * statement's work is over, before the response is written.
   */
  static HttpClientDisconnectWatch arm(final HttpServerExchange exchange, final RunningQuery runningQuery) {
    final ServerConnection serverConnection = exchange.getConnection();
    if (!(serverConnection instanceof HttpServerConnection connection) || connection.getSslSession() != null
        || exchange.isInIoThread() || !exchange.isRequestComplete() || connection.getExtraBytes() != null
        || connection.getOriginalSourceConduit() == null)
      return null;

    final HttpClientDisconnectWatch watch = new HttpClientDisconnectWatch(connection, runningQuery);
    if (!watch.liftReadTimeout())
      return null;

    // Disarmed when the exchange completes at the latest: Undertow runs these listeners before it resumes reads for the next
    // request. A streamed answer completes the exchange inside the handler, before the statement's entry is closed
    exchange.addExchangeCompleteListener((completed, next) -> {
      watch.disarm();
      next.proceed();
    });
    watch.channel.setReadListener(watch);
    // On the socket itself: the request's own conduits are exhausted, and an empty body's one never reads the socket
    watch.socket.resumeReads();
    return watch;
  }

  /**
   * Sets the connection's read timeout to none, keeping the value to put back on {@link #disarm()}. Answers {@code false}
   * when it cannot be lifted: reads resumed under it would end every statement longer than it (issue #9953).
   */
  private boolean liftReadTimeout() {
    try {
      final Integer timeout = connection.getOption(Options.READ_TIMEOUT);
      if (timeout != null && timeout > 0) {
        connection.setOption(Options.READ_TIMEOUT, 0);
        liftedReadTimeout = timeout;
      }
      return true;
    } catch (final IOException | RuntimeException e) {
      LogManager.instance()
          .log(this, Level.FINE, "Cannot lift the read timeout of the connection of query %s, not watching it: %s",
              runningQuery.getId(), e.getMessage());
      return false;
    }
  }

  /** Puts back the read timeout {@link #liftReadTimeout()} lifted: the connection is kept alive for the next request. */
  private void restoreReadTimeout() {
    if (liftedReadTimeout == null)
      return;
    final Integer timeout = liftedReadTimeout;
    liftedReadTimeout = null;
    try {
      connection.setOption(Options.READ_TIMEOUT, timeout);
    } catch (final IOException | RuntimeException e) {
      // Not FINE: the connection keeps no read timeout for the rest of its keep-alive life, so an idle client is never cut off
      LogManager.instance()
          .log(this, Level.WARNING, "Cannot restore the read timeout of the connection of query %s, it stays without one: %s",
              runningQuery.getId(), e.getMessage());
    }
  }

  /** Runs on the connection's I/O thread when the client sent something or closed the connection. */
  @Override
  public synchronized void handleEvent(final ConduitStreamSourceChannel ignored) {
    // An event dispatched before disarm() and run after it: nothing to do, and nothing to touch - reads were suspended when
    // the watch stopped, and by now Undertow may have resumed them for the next request
    if (!armed)
      return;

    final int read;
    try {
      read = socket.read(probe);
    } catch (final IOException e) {
      clientGone("the connection failed: " + e.getMessage());
      return;
    }

    if (read < 0)
      clientGone("the client closed the connection");
    else if (read > 0) {
      // The next request of a client that pipelines: it is Undertow's, and the client is alive
      probe.flip();
      // Given away: the watch stops here and never reads into it again
      connection.ungetRequestBytes(new ImmediatePooledByteBuffer(probe));
      stopWatching();
    }
    // 0: a wakeup with nothing to read, keep watching
  }

  /**
   * Stops watching and gives the connection back to Undertow as it was: no read of the watch can happen after it
   * returns, so whatever the connection carries next is Undertow's to read.
   */
  void disarm() {
    synchronized (this) {
      // Reads are this watch's to suspend only while it is the connection's listener: once Undertow has taken the
      // connection back for the next request, they are Undertow's
      if (armed) {
        armed = false;
        if (channel.getReadListener() == this)
          socket.suspendReads();
      }
      // With reads suspended, and before Undertow resumes them for the next request: it reads the timeout as it resumes
      restoreReadTimeout();
    }
    // Outside the monitor, and still exact: nothing but this watch and Undertow sets the listener, and Undertow sets it
    // only when the exchange completes, which disarms this watch first
    if (channel.getReadListener() == this)
      channel.setReadListener(previous);
  }

  private void clientGone(final String reason) {
    stopWatching();
    LogManager.instance()
        .log(this, Level.FINE, "HTTP client of query %s went away (%s): terminating it", runningQuery.getId(), reason);
    // Off the I/O thread: a termination runs the statement's listeners, which may take a while (a guest language
    // unwinding), and this thread serves every other connection assigned to it
    connection.getWorker().execute(() -> runningQuery.terminate("client disconnected"));
  }

  private void stopWatching() {
    armed = false;
    socket.suspendReads();
  }
}
