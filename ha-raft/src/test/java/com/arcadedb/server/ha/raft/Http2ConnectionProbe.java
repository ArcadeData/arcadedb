/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.server.ha.raft;

import com.arcadedb.utility.StallAwareStopwatch;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * An HTTP/2 client that watches a Raft gRPC listener's connection-lifetime bounds from the outside, written for
 * issue #7339 and used both against a customizer applied to a bare {@code NettyServerBuilder} and against the
 * listener of a live cluster node.
 * <p>
 * It exists because the two bounds cannot be told apart by a stopwatch. gRPC names the timer that fired in the
 * GOAWAY's debug data - {@code "max_age"} or {@code "max_idle"} - so a test can assert <i>which</i> window closed a
 * connection instead of inferring it from how long that took.
 * <p>
 * The other half of issue #7339 is that a connection which keeps <i>starting</i> RPCs is never idle: a stream that
 * opens and closes restarts the idle window whether the RPC succeeds, is refused by the peer allowlist interceptor,
 * or comes back UNIMPLEMENTED. {@link #drumUntilClosed} is that peer.
 * <p>
 * A reader thread pumps frames continuously and acknowledges SETTINGS and PING - gRPC's graceful shutdown waits on
 * its own PING before closing the socket, so answering it turns an eleven-second wait into a prompt one. Headers are
 * HPACK literals without indexing, so the dynamic table stays empty on both sides and neither has to track state.
 * <p>
 * <b>What it is not.</b> The flow control here is connection-level only: it replenishes the stream 0 window and
 * never a per-stream one. That is sound for what it does - a HEADERS-only request whose answer is a trailers-only
 * refusal or UNIMPLEMENTED, which carries no DATA at all - and it is a trap for anything else. Pointed at an
 * endpoint that streams a response larger than the 64 KiB initial stream window, this probe would stall on a
 * per-stream window it never updates, and the stall would read as "the server went quiet". Replenishing per stream
 * is deliberately not done rather than left undone: RFC 9113 lets a peer treat a WINDOW_UPDATE on a closed stream
 * as a connection error, and every stream this probe opens is closed by the server almost immediately.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
final class Http2ConnectionProbe implements AutoCloseable {

  private static final byte[] PREFACE     = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".getBytes(StandardCharsets.US_ASCII);
  private static final int    DATA        = 0x0;
  private static final int    HEADERS     = 0x1;
  private static final int    SETTINGS    = 0x4;
  private static final int    PING        = 0x6;
  private static final int    GOAWAY      = 0x7;
  private static final int    WINDOW_UPD  = 0x8;
  private static final int    ACK         = 0x1;
  private static final int    END_STREAM  = 0x1;
  private static final int    END_HEADERS = 0x4;

  private final Socket                  socket;
  private final InputStream             in;
  private final OutputStream            out;
  private final String                  authority;
  private final Object                  writeLock = new Object();
  private final CountDownLatch          closed    = new CountDownLatch(1);
  private final AtomicReference<String> goAway    = new AtomicReference<>();
  private final Thread                  pump;

  private int  nextStreamId = 1;
  private int  beats;
  private long widestGapMs;
  private long stallMs;

  /** Connects to {@code host:port} and completes the HTTP/2 handshake. */
  static Http2ConnectionProbe connectTo(final String host, final int port) throws IOException {
    final Socket socket = new Socket();
    socket.connect(new InetSocketAddress(host, port), 10_000);
    return new Http2ConnectionProbe(socket, host + ":" + port);
  }

  private Http2ConnectionProbe(final Socket socket, final String authority) throws IOException {
    this.socket = socket;
    this.authority = authority;
    this.in = socket.getInputStream();
    this.out = socket.getOutputStream();
    writeFrame(SETTINGS, 0, 0, new byte[0], PREFACE);

    this.pump = new Thread(this::pumpFrames, "issue7339-http2-probe");
    this.pump.setDaemon(true);
    this.pump.start();
  }

  /**
   * Opens and closes one stream every {@code beatMs} until the server closes the connection or {@code budgetMs}
   * elapses, whichever comes first. Stops beating as soon as a GOAWAY arrives: a stream created after the server
   * has announced its last accepted id is a protocol error, and a connection closed for <i>that</i> reason would
   * look like a pass to a test that only checked whether it closed.
   */
  void drumUntilClosed(final long budgetMs, final long beatMs) throws InterruptedException {
    final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(budgetMs);
    final long stallAtStart = StallAwareStopwatch.jvmStallNanos();
    long lastBeatNanos = System.nanoTime();

    while (!closed.await(beatMs, TimeUnit.MILLISECONDS) && System.nanoTime() < deadline) {
      if (goAway.get() != null)
        continue; // the server is shutting the connection down; let the pump see it through to EOF
      try {
        openAndCloseOneStream();
      } catch (final IOException closedUnderneathUs) {
        break;
      }
      final long now = System.nanoTime();
      widestGapMs = Math.max(widestGapMs, TimeUnit.NANOSECONDS.toMillis(now - lastBeatNanos));
      lastBeatNanos = now;
      beats++;
    }
    stallMs = TimeUnit.NANOSECONDS.toMillis(Math.max(0L, StallAwareStopwatch.jvmStallNanos() - stallAtStart));
  }

  /** Waits without sending anything, for the case where the question is what an untouched connection is worth. */
  void awaitClose(final long budgetMs) throws InterruptedException {
    final long stallAtStart = StallAwareStopwatch.jvmStallNanos();
    closed.await(budgetMs, TimeUnit.MILLISECONDS);
    stallMs = TimeUnit.NANOSECONDS.toMillis(Math.max(0L, StallAwareStopwatch.jvmStallNanos() - stallAtStart));
  }

  /** How many RPCs the drumbeat actually started. */
  int beats() {
    return beats;
  }

  /** The GOAWAY debug string - {@code "max_age"} or {@code "max_idle"} - or null while the connection is open. */
  String closeReason() {
    return goAway.get();
  }

  /** The numbers a red run needs to tell a stalled harness from a server that reaped a busy connection. */
  String timing() {
    return "Probe: %d RPCs, widest gap between two of them %,d ms, JVM stalled %,d ms during the run."
        .formatted(beats, widestGapMs, stallMs);
  }

  private void openAndCloseOneStream() throws IOException {
    final byte[] block = headerBlock();
    final int streamId;
    synchronized (writeLock) {
      streamId = nextStreamId;
      nextStreamId += 2; // client-initiated stream ids are odd and strictly increasing
    }
    writeFrame(HEADERS, END_HEADERS | END_STREAM, streamId, block, null);
  }

  private byte[] headerBlock() {
    final ByteArrayOutputStream block = new ByteArrayOutputStream();
    literalHeader(block, ":method", "POST");
    literalHeader(block, ":scheme", "http");
    literalHeader(block, ":path", "/arcadedb.issue7339.Drumbeat/Beat");
    literalHeader(block, ":authority", authority);
    literalHeader(block, "content-type", "application/grpc");
    literalHeader(block, "te", "trailers");
    return block.toByteArray();
  }

  /** HPACK "literal header field without indexing - new name": neither side indexes, so neither has to remember. */
  private static void literalHeader(final ByteArrayOutputStream block, final String name, final String value) {
    block.write(0x00);
    writeHpackString(block, name);
    writeHpackString(block, value);
  }

  private static void writeHpackString(final ByteArrayOutputStream block, final String text) {
    final byte[] raw = text.getBytes(StandardCharsets.US_ASCII);
    if (raw.length >= 127)
      throw new IllegalArgumentException("this encoder only writes 7-bit-prefix lengths: " + text);
    block.write(raw.length); // H=0, no Huffman coding
    block.write(raw, 0, raw.length);
  }

  private void pumpFrames() {
    try {
      while (true)
        readOneFrame();
    } catch (final IOException endOfConnection) {
      // EOF or a reset: either way the server let the connection go, which is what the tests measure.
    } finally {
      closed.countDown();
    }
  }

  private void readOneFrame() throws IOException {
    final byte[] header = readFully(9);
    final int length = ((header[0] & 0xff) << 16) | ((header[1] & 0xff) << 8) | (header[2] & 0xff);
    final int type = header[3] & 0xff;
    final int flags = header[4] & 0xff;
    final byte[] payload = readFully(length);

    if (type == SETTINGS && (flags & ACK) == 0)
      writeFrame(SETTINGS, ACK, 0, new byte[0], null);
    else if (type == PING && (flags & ACK) == 0)
      writeFrame(PING, ACK, 0, payload, null);
    else if (type == GOAWAY && payload.length > 8)
      // 4 bytes last-stream-id, 4 bytes error code, then the debug data gRPC uses to name the timer that fired.
      goAway.compareAndSet(null, new String(payload, 8, payload.length - 8, StandardCharsets.US_ASCII));
    else if (type == DATA && length > 0)
      // Keep the connection-level flow-control window open: the server's answers are small, but a drumbeat of
      // hundreds of them must not be able to stall on a window this client never updates.
      writeFrame(WINDOW_UPD, 0, 0, new byte[] { 0, 0, (byte) (length >>> 8), (byte) length }, null);
  }

  private byte[] readFully(final int length) throws IOException {
    final byte[] buffer = new byte[length];
    int read = 0;
    while (read < length) {
      final int n = in.read(buffer, read, length - read);
      if (n < 0)
        throw new EOFException("the server closed the connection");
      read += n;
    }
    return buffer;
  }

  private void writeFrame(final int type, final int flags, final int streamId, final byte[] payload,
      final byte[] prologue) throws IOException {
    final byte[] frame = new byte[9 + payload.length];
    frame[0] = (byte) (payload.length >>> 16);
    frame[1] = (byte) (payload.length >>> 8);
    frame[2] = (byte) payload.length;
    frame[3] = (byte) type;
    frame[4] = (byte) flags;
    frame[5] = (byte) (streamId >>> 24);
    frame[6] = (byte) (streamId >>> 16);
    frame[7] = (byte) (streamId >>> 8);
    frame[8] = (byte) streamId;
    System.arraycopy(payload, 0, frame, 9, payload.length);
    synchronized (writeLock) {
      if (prologue != null)
        out.write(prologue);
      out.write(frame);
      out.flush();
    }
  }

  @Override
  public void close() throws IOException {
    socket.close();
    pump.interrupt();
  }
}
