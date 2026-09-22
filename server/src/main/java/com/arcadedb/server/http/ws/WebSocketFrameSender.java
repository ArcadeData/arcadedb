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

import com.arcadedb.log.LogManager;
import io.undertow.websockets.core.WebSocketCallback;
import io.undertow.websockets.core.WebSocketChannel;
import io.undertow.websockets.core.WebSockets;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

/**
 * The one place a {@code /ws} text frame is written from, so what the connection's several senders rely on is
 * written down once (issue #7423).
 * <p>
 * A {@code /ws} connection has more than one writer and nothing of ours serializing them: the change-stream
 * fan-out of {@link WebSocketEventBus} runs on the per-database watcher thread, the subscription acknowledgements
 * of {@code WebSocketReceiveListener} on the I/O thread that read the frame, the insert-session answers of
 * {@code WebSocketInsertProtocol} on an Undertow worker thread, and the idle sweep's unsolicited {@code error}
 * on whichever thread expired the session. That is safe because of how Undertow 2.4.3.Final builds a frame,
 * established from its source rather than assumed:
 * <ul>
 * <li>{@link WebSockets#sendText(String, WebSocketChannel, WebSocketCallback)} calls {@code WebSocketChannel.send(TEXT)},
 *     which creates a NEW {@code StreamSinkFrameChannel} per call and appends it to the channel's
 *     {@code WebSocketFramePriority.strictOrderQueue}, a {@code ConcurrentLinkedDeque}. The whole payload is
 *     then attached to that sink as its single body and {@code AbstractFramedChannel.queueFrame} adds the sink
 *     to {@code newFrames}, a {@code LinkedBlockingDeque}. Both queues take concurrent adds.</li>
 * <li>Bytes reach the socket only from {@code AbstractFramedChannel.flushSenders()}, which is
 *     {@code synchronized} and runs on the channel's I/O thread ({@code runNowOrInIoThread}). It drains
 *     {@code newFrames} in order and the frame priority releases the next sink only when the one ahead of it in
 *     {@code strictOrderQueue} has been written to the end, so one frame's bytes are never split around
 *     another's.</li>
 * </ul>
 * So concurrent callers each get a complete frame on the wire, in the order their {@code send(TEXT)} calls
 * happened. What they do NOT get is an ordering between senders - a change event and a {@code batchAck} may
 * arrive in either order - which the protocols on {@code /ws} already tolerate: every frame carries its own
 * {@code action}. The one plain read in the path is the {@code closeFrameSent} / {@code closeFrameReceived} pair
 * in {@code WebSocketChannel.send()}: a sender racing a close can be refused with an {@code IOException} from
 * {@code queueFrame} instead of the "channel closed" message, and that is handed to the callback, or closes the
 * channel when there is none, exactly as any other send failure is.
 * <p>
 * Every sender goes through here so the next one added inherits the note above instead of re-deriving it, and
 * so that if a future Undertow changes the guarantee there is one method to put a lock in.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class WebSocketFrameSender {
  /**
   * Channel attribute holding the per-connection outstanding-bytes counter {@link #sendBudgeted} charges
   * against (issue #8085). Package-private, not private, so a test in this package can seed an outstanding
   * balance directly to simulate a frame still in flight, the same way
   * {@code Issue6762WebSocketEventBusTest.neverCompletingSubscription()} does for the event bus's budget.
   */
  static final String PENDING_BYTES = "arcadedb.ws.pendingBytes";

  private WebSocketFrameSender() {
  }

  /**
   * Queues one complete text frame on the channel. Returns as soon as the frame is queued: the write itself
   * happens on the channel's I/O thread.
   *
   * @param callback notified when the frame has been written or has failed, or {@code null} to let Undertow close
   *                 the channel on a failed write
   */
  public static void send(final WebSocketChannel channel, final String text, final WebSocketCallback<Void> callback) {
    WebSockets.sendText(text, channel, callback);
  }

  /**
   * {@link #send(WebSocketChannel, String, WebSocketCallback)}, but first charges {@code text}'s size against a
   * per-connection pending-bytes budget shared by every sender that answers a request made ON this channel -
   * {@code WebSocketReceiveListener}'s subscription acknowledgements and errors, and
   * {@code WebSocketInsertProtocol}'s insert-session answers (issue #8085).
   * <p>
   * {@link WebSocketEventBus}'s change-stream fan-out is charged against its own, per-subscription budget
   * instead (issue #6762, {@code EventWatcherSubscription.reservePending}) and keeps calling {@link #send}
   * directly: it is a push a client opted into rather than an answer to something it sent, and one connection
   * can carry several such subscriptions, so accounting per subscription is what lets a slow subscriber on one
   * database be evicted without punishing its subscription to another.
   * <p>
   * A peer that stops reading otherwise accumulates these answer frames in Undertow's send buffer without
   * bound: an authenticated connection sending malformed requests as fast as its socket allows gets one small
   * error frame queued per request, forever, which is heap the connection holds until the peer reads it or the
   * connection dies - and the peer is the one refusing to read. Past the budget the frame is dropped instead of
   * queued and the channel is closed, which is what the peer would eventually experience anyway.
   *
   * @param maxPendingBytes the budget, or {@code 0} (or less) to disable the cap
   *
   * @return {@code true} if the frame was queued, {@code false} if the budget was exceeded and the channel was
   * closed instead
   */
  public static boolean sendBudgeted(final WebSocketChannel channel, final String text, final long maxPendingBytes) {
    if (!channel.isOpen())
      return false;

    // The UTF-8 BYTE length, not text.length(): see WebSocketEventBus's use of the same method for why chars
    // would undercount a non-ASCII payload against a cap expressed in bytes.
    final int messageSize = utf8Length(text);
    final AtomicLong pending = pendingBytes(channel);
    final long outstanding = pending.addAndGet(messageSize);
    if (maxPendingBytes > 0 && outstanding > maxPendingBytes) {
      pending.addAndGet(-messageSize);
      LogManager.instance().log(WebSocketFrameSender.class, Level.WARNING,
          "Closing /ws connection: more than %d bytes of unread answers are still outstanding towards it. Raise "
              + "arcadedb.server.wsMaxPendingControlBytes if this is a legitimately slow consumer", null, maxPendingBytes);
      try {
        channel.close();
      } catch (final IOException e) {
        // IGNORE: the channel is already going away, which is the outcome this branch wanted anyway.
      }
      return false;
    }

    try {
      WebSockets.sendText(text, channel, new WebSocketCallback<>() {
        @Override
        public void complete(final WebSocketChannel webSocketChannel, final Void unused) {
          pending.addAndGet(-messageSize);
        }

        @Override
        public void onError(final WebSocketChannel webSocketChannel, final Void unused, final Throwable throwable) {
          pending.addAndGet(-messageSize);
        }
      });
    } catch (final RuntimeException e) {
      // The frame never reached Undertow's own queue, so nothing is outstanding: give the reservation back
      // rather than let a channel that fails synchronously leak budget for the rest of the connection's life.
      // Mirrors WebSocketEventBus.publish()'s same guard around its own WebSocketFrameSender.send() call.
      pending.addAndGet(-messageSize);
      throw e;
    }
    return true;
  }

  /**
   * This connection's {@link #sendBudgeted} counter, created on demand. Guarded by a lock on {@code channel}
   * rather than left to a plain check-then-act: unlike {@code WebSocketInsertProtocol}'s per-connection
   * attributes, which are only ever created from the single I/O thread Undertow runs one frame of a connection
   * on at a time, this one can be reached from that I/O thread ({@code WebSocketReceiveListener}) AND from an
   * Undertow worker thread ({@code WebSocketInsertProtocol.execute}) concurrently, so two racing first sends
   * could otherwise each create and install their own counter and silently split the budget in two.
   */
  private static AtomicLong pendingBytes(final WebSocketChannel channel) {
    AtomicLong pending = (AtomicLong) channel.getAttribute(PENDING_BYTES);
    if (pending != null)
      return pending;
    synchronized (channel) {
      pending = (AtomicLong) channel.getAttribute(PENDING_BYTES);
      if (pending == null) {
        pending = new AtomicLong();
        channel.setAttribute(PENDING_BYTES, pending);
      }
      return pending;
    }
  }

  /**
   * The number of bytes {@code s} occupies once UTF-8 encoded, without allocating the encoded copy.
   */
  static int utf8Length(final String s) {
    int bytes = 0;
    for (int i = 0; i < s.length(); i++) {
      final char c = s.charAt(i);
      if (c < 0x80)
        bytes += 1;
      else if (c < 0x800)
        bytes += 2;
      else if (Character.isHighSurrogate(c) && i + 1 < s.length() && Character.isLowSurrogate(s.charAt(i + 1))) {
        // one code point spread over a surrogate pair encodes as 4 bytes, not 3 + 3
        bytes += 4;
        i++;
      } else
        bytes += 3;
    }
    return bytes;
  }
}
