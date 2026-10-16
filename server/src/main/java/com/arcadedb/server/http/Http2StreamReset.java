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
import io.undertow.protocols.http2.Http2StreamSourceChannel;
import io.undertow.server.HttpServerExchange;
import io.undertow.server.ServerConnection;
import io.undertow.server.protocol.http2.Http2ServerConnection;
import org.xnio.IoUtils;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.logging.Level;

/**
 * Resets the HTTP/2 stream of an exchange whose response is complete while its request body is not, with
 * {@code NO_ERROR}: the code RFC 9113, section 8.1, gives a server that answered before the request ended, and the one a
 * client reads as "stop sending, the response stands" (issue #8081).
 * <p>
 * Undertow resets a stream only with {@code CANCEL} through its public API ({@code Http2ServerConnection.close()}), and
 * a client is entitled to read {@code CANCEL} as a failed request: the JDK client discards the response it has already
 * received and reports "RST_STREAM received". The stream id and the frame channel {@code NO_ERROR} needs are private
 * fields of {@link Http2ServerConnection} (verified against Undertow 2.4.4), so they are read through {@link VarHandle}s
 * resolved once with
 * {@link MethodHandles#privateLookupIn}, the JDK's supported way to read another class's private state, which neither
 * alters any member's accessibility nor costs a reflective call per request. Should a later Undertow rename
 * them, the reset falls back to {@code CANCEL}, which still stops the body - the server-side half of the guarantee -
 * and {@code Issue7772ChunkedBodySizeLimitTest} fails on the client-side half, so the upgrade cannot pass unnoticed.
 * <p>
 * <b>The connection's flow-control window is credited for the stream as well.</b> Undertow returns connection-level
 * window to the client only as the application READS a stream ({@code Http2StreamSourceChannel.updateFlowControlWindow}),
 * so what the refused stream had buffered, and what was still in flight when the reset went out, is discarded without
 * ever being credited. Every HTTP/2 stream of the connection shares that window, so after one refused body the next
 * request's body could not be sent at all and the connection hung. What the stream can hold uncredited is bounded by
 * one initial stream window - a stream is never granted more than that beyond what was read - so that is what is
 * credited: an upper bound, which at worst lets the client run that much ahead of the window, where a lower one would
 * leak window on every refusal until the connection stalls.
 * <p>
 * <b>The client's connection may end anyway.</b> RFC 9113, section 5.1, has an endpoint ignore the frames that arrive
 * on a stream it reset, and the client may well have DATA in flight when the reset goes out. Undertow 2.4 does not keep
 * a client-initiated stream it reset in its reset-stream tracker, so such a frame reads as DATA for an unknown stream,
 * which it answers with {@code GOAWAY(PROTOCOL_ERROR)}, ending the client's other streams on that connection. That is
 * why the refusal reads what is left of the body for a bounded budget first and resets only past it
 * ({@code AbstractServerHttpHandler.DISCARD_REFUSED_BODY_THEN_END_EXCHANGE}): only a client that keeps sending well
 * past the cap after being refused can lose its connection, and the cost to it is a reconnect.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class Http2StreamReset {
  private static final VarHandle CHANNEL         = field("channel", Http2Channel.class);
  private static final VarHandle REQUEST_CHANNEL = field("requestChannel", Http2StreamSourceChannel.class);

  private Http2StreamReset() {
  }

  /** Resets the exchange's stream with {@code NO_ERROR}, or with {@code CANCEL} when that cannot be done. */
  public static void resetWithNoError(final HttpServerExchange exchange) {
    final ServerConnection connection = exchange.getConnection();
    if (connection instanceof Http2ServerConnection && CHANNEL != null && REQUEST_CHANNEL != null)
      try {
        final Http2Channel channel = (Http2Channel) CHANNEL.get((Http2ServerConnection) connection);
        final Http2StreamSourceChannel stream = (Http2StreamSourceChannel) REQUEST_CHANNEL.get(
            (Http2ServerConnection) connection);
        if (channel != null && stream != null) {
          channel.sendRstStream(stream.getStreamId(), Http2Channel.ERROR_NO_ERROR);
          channel.updateReceiveFlowControlWindow(channel.getInitialReceiveWindowSize());
          return;
        }
      } catch (final IOException | RuntimeException e) {
        LogManager.instance().log(Http2StreamReset.class, Level.FINE, "Cannot reset the HTTP/2 stream with NO_ERROR: %s", e,
            e.getMessage());
      }

    // Http2ServerConnection.close() resets this stream alone, with CANCEL, and leaves the connection's other streams be
    IoUtils.safeClose(connection);
  }

  private static VarHandle field(final String name, final Class<?> type) {
    try {
      return MethodHandles.privateLookupIn(Http2ServerConnection.class, MethodHandles.lookup())
          .findVarHandle(Http2ServerConnection.class, name, type);
    } catch (final ReflectiveOperationException | RuntimeException e) {
      LogManager.instance().log(Http2StreamReset.class, Level.WARNING,
          "Undertow's Http2ServerConnection has no accessible '%s': a request body refused on HTTP/2 is reset with "
              + "CANCEL rather than NO_ERROR, which some clients report as a failed request instead of the 413", name);
      return null;
    }
  }
}
