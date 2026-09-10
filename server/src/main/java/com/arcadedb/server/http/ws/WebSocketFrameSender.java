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

import io.undertow.websockets.core.WebSocketCallback;
import io.undertow.websockets.core.WebSocketChannel;
import io.undertow.websockets.core.WebSockets;

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

  /** {@link #send(WebSocketChannel, String, WebSocketCallback)} with no callback, skipped when the channel is gone. */
  public static void sendIfOpen(final WebSocketChannel channel, final String text) {
    if (channel.isOpen())
      WebSockets.sendText(text, channel, null);
  }
}
