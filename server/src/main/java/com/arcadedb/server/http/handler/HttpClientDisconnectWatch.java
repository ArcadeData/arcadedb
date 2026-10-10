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
 * only when the exchange completes, which is after {@link #disarm()}: the watch is disarmed, under the same monitor its
 * reads take, before the response is written. No read of the watch can therefore overlap Undertow's own, and bytes the
 * watch did read are back in the connection before Undertow looks for the next request.
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
  private       boolean                                              armed = true;

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
    watch.channel.setReadListener(watch);
    // On the socket itself: the request's own conduits are exhausted, and an empty body's one never reads the socket
    watch.socket.resumeReads();
    return watch;
  }

  /** Runs on the connection's I/O thread when the client sent something or closed the connection. */
  @Override
  public synchronized void handleEvent(final ConduitStreamSourceChannel ignored) {
    if (!armed) {
      socket.suspendReads();
      return;
    }

    final ByteBuffer buffer = ByteBuffer.allocate(1);
    final int read;
    try {
      read = socket.read(buffer);
    } catch (final IOException e) {
      clientGone("the connection failed: " + e.getMessage());
      return;
    }

    if (read < 0)
      clientGone("the client closed the connection");
    else if (read > 0) {
      // The next request of a client that pipelines: it is Undertow's, and the client is alive
      buffer.flip();
      connection.ungetRequestBytes(new ImmediatePooledByteBuffer(buffer));
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
      if (armed)
        stopWatching();
    }
    if (channel.getReadListener() == this)
      channel.setReadListener(previous);
  }

  private void clientGone(final String reason) {
    stopWatching();
    LogManager.instance()
        .log(this, Level.FINE, "HTTP client of query %s went away (%s): terminating it", runningQuery.getId(), reason);
    runningQuery.terminate("client disconnected");
  }

  private void stopWatching() {
    armed = false;
    socket.suspendReads();
  }
}
