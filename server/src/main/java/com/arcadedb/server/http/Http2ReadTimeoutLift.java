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
package com.arcadedb.server.http;

import com.arcadedb.log.LogManager;
import io.undertow.protocols.http2.Http2Channel;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.AttachmentKey;
import org.xnio.Options;

import java.io.IOException;
import java.util.logging.Level;

/**
 * Lifts the read timeout of an HTTP/2 connection while one of its streams is being served, and puts it back when the last
 * of them ends (issue #12606).
 * <p>
 * <b>Why.</b> The connection carries {@code arcadedb.network.socketTimeout} as its read timeout, which Undertow enforces
 * with a timer while reads are resumed: once that much time passes with no byte from the client, it closes the connection.
 * On HTTP/1.1 Undertow suspends reads while a request runs, so the timer only ever measures an idle connection. An HTTP/2
 * connection is read for as long as it is open, because any of its streams may send a frame at any time, and a client
 * waiting for an answer has none to send: a statement longer than the socket timeout lost its connection to the timer,
 * with every other stream on it, where the same statement on HTTP/1.1 was answered.
 * <p>
 * <b>What is lifted, and for whom.</b> The timeout belongs to the connection, which its streams share, so the streams
 * being served are counted per connection: the first one lifts the timeout, the last one to end puts it back, and a
 * connection none of whose streams is being served keeps it, so an idle one is closed as before. A stream counts from the
 * point its request is authenticated and its body read to the point its handler returns: a request still sending its body
 * is not covered, nor is one an I/O thread answers on the spot. The trade is the one the HTTP/1.1 path makes (issue
 * #9953): a client that vanishes without closing its connection is not cut off while its statement runs. TCP keep-alive,
 * which the listener turns on, and {@code arcadedb.command.timeout} bound that case.
 * <p>
 * <b>How.</b> The option is set on the {@link Http2Channel}: {@code Http2ServerConnection} answers {@code null} to
 * {@code getOption} and ignores {@code setOption}. Setting it is not enough, though. Undertow's read-timeout conduit
 * computes the expiry when a read happens and its timer, once scheduled, fires at that expiry whatever the option says by
 * then; the timer is dropped only when reads are suspended, and scheduled anew, from the option's value at that moment,
 * when they are resumed. So each change of the option is followed by a suspend and resume of the channel's reads on its
 * I/O thread, which is where Undertow itself does both: nothing is lost in between, bytes that arrived meanwhile are still
 * in the socket when reads resume.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
public final class Http2ReadTimeoutLift {
  private static final AttachmentKey<Http2ReadTimeoutLift> KEY = AttachmentKey.create(Http2ReadTimeoutLift.class);

  private final Http2Channel channel;
  private final Runnable     rearmReadTimer = this::rearmReadTimer;
  // Streams of the connection being served. Guarded by this
  private       int          streams;
  // The read timeout the connection had before it was lifted, null when there is none to put back. Guarded by this
  private       Integer      lifted;

  private Http2ReadTimeoutLift(final Http2Channel channel) {
    this.channel = channel;
  }

  /**
   * Counts the stream of {@code exchange} as being served, lifting the read timeout of its connection when it is the first.
   * Answers the lift to {@link #release()} once the stream has been served, or {@code null} when there is nothing to lift:
   * a request that is not on HTTP/2, one answered on the I/O thread, which cannot wait long enough for the timer to matter,
   * or one whose body is still being sent, which the read timeout keeps guarding.
   */
  public static Http2ReadTimeoutLift acquire(final HttpServerExchange exchange) {
    if (exchange.isInIoThread() || !exchange.isRequestComplete())
      return null;
    final Http2Channel channel = Http2StreamReset.channelOf(exchange.getConnection());
    if (channel == null)
      return null;

    Http2ReadTimeoutLift lift = channel.getAttachment(KEY);
    if (lift == null)
      // Once per connection
      synchronized (KEY) {
        lift = channel.getAttachment(KEY);
        if (lift == null) {
          lift = new Http2ReadTimeoutLift(channel);
          channel.putAttachment(KEY, lift);
        }
      }
    lift.enter();
    return lift;
  }

  private synchronized void enter() {
    if (streams++ > 0)
      return;
    try {
      final Integer timeout = channel.getOption(Options.READ_TIMEOUT);
      if (timeout != null && timeout > 0) {
        channel.setOption(Options.READ_TIMEOUT, 0);
        lifted = timeout;
        channel.getIoThread().execute(rearmReadTimer);
      }
    } catch (final IOException | RuntimeException e) {
      LogManager.instance().log(this, Level.FINE,
          "Cannot lift the read timeout of the HTTP/2 connection, a statement longer than it may lose the connection: %s",
          e.getMessage());
    }
  }

  /** Ends what {@link #acquire} started, once for each: the last stream to end puts the connection's read timeout back. */
  public synchronized void release() {
    if (--streams > 0 || lifted == null)
      return;
    final Integer timeout = lifted;
    lifted = null;
    try {
      channel.setOption(Options.READ_TIMEOUT, timeout);
      channel.getIoThread().execute(rearmReadTimer);
    } catch (final IOException | RuntimeException e) {
      // Not FINE while the connection is open: it keeps no read timeout for the rest of its life, so an idle client is never
      // cut off
      LogManager.instance().log(this, channel.isOpen() ? Level.WARNING : Level.FINE,
          "Cannot restore the read timeout of the HTTP/2 connection, it stays without one: %s", e.getMessage());
    }
  }

  /**
   * Runs on the connection's I/O thread: drops the read timer Undertow scheduled under the previous value of the option
   * and has it scheduled again under the current one. Nothing else suspends the receives of a server channel, so the pair
   * cannot undo somebody else's suspension, and reads Undertow holds back for back-pressure stay held back: the resume
   * resumes only what nothing else suspends, and the timer is scheduled from the option when they do resume.
   */
  private void rearmReadTimer() {
    if (channel.isOpen() && channel.isReceivesResumed()) {
      channel.suspendReceives();
      channel.resumeReceives();
    }
  }
}
