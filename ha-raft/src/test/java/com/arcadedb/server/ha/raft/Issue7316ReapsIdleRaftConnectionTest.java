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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.ArcadeDBServer;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.grpc.server.GrpcServices;
import org.apache.ratis.thirdparty.io.grpc.Server;
import org.apache.ratis.thirdparty.io.grpc.netty.NettyServerBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Regression tests for issue #7316, a follow-up to #7250.
 * <p>
 * #7250 made a peer that stops being admitted lose its <i>reach</i> on a connection it had already established: the
 * in-flight Raft RPCs are closed with {@code PERMISSION_DENIED} and every later one is refused. It could not close
 * the connection - {@code ServerTransportFilter} is handed no reference to the transport it admits, and a
 * {@code ServerCall} reaches only its own HTTP/2 stream - so the socket lived until the peer, the kernel or a
 * network event dropped it. Ratis sets none of gRPC's builder-wide connection-lifetime knobs, so the Raft listener
 * had no server-side bound at all.
 * <p>
 * These tests drive the real production path: {@code RaftHAServer.buildParameters} publishes a
 * {@code GrpcServices.Customizer}, the test applies that customizer to a {@link NettyServerBuilder} exactly as
 * {@code GrpcServicesImpl.buildServer} does, starts the server, and then watches a real HTTP/2 connection from the
 * outside. Asserting on the builder would have proved only that a setter was called.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue7316ReapsIdleRaftConnectionTest {

  private static final String SERVER_LIST = "localhost:2434:2480,localhost:2435:2481";

  /**
   * gRPC clamps any window below one second up to one second ({@code MIN_MAX_CONNECTION_IDLE_NANO}), so this is the
   * shortest window the server will actually honour.
   */
  private static final long   MIN_IDLE_MS  = 1_000L;

  // ---------------------------------------------------------------------------
  // Row 1: the reported case - the allowlist is on, so a revocation can happen
  // ---------------------------------------------------------------------------

  @Test
  @Timeout(120)
  void anIdleConnectionIsReapedWhenTheAllowlistIsOn() throws IOException {
    final ContextConfiguration config = allowlistConfiguration();
    config.setValue(GlobalConfiguration.HA_GRPC_MAX_CONNECTION_IDLE_MS, MIN_IDLE_MS);

    try (final RaftGrpcServer server = start(config); final Http2Peer peer = server.connect()) {
      assertThat(peer.awaitClose(60_000))
          .as("a connection carrying no RPC must be closed by the server, not left to the peer to drop")
          .isTrue();
    }
  }

  // ---------------------------------------------------------------------------
  // Row 2: the window is a connection-lifetime setting, not an allowlist setting
  // ---------------------------------------------------------------------------

  @Test
  @Timeout(120)
  void anIdleConnectionIsReapedWithTheAllowlistDisabled() throws IOException {
    final ContextConfiguration config = allowlistConfiguration();
    config.setValue(GlobalConfiguration.HA_PEER_ALLOWLIST_ENABLED, false);
    config.setValue(GlobalConfiguration.HA_GRPC_MAX_CONNECTION_IDLE_MS, MIN_IDLE_MS);

    final RaftHAServer haServer = detachedServer();
    final Parameters parameters = haServer.buildParameters(config);
    assertThat(haServer.allowlistFilterForTest())
        .as("the allowlist is off, so no filter is built")
        .isNull();
    assertThat(GrpcConfigKeys.Server.servicesCustomizer(parameters))
        .as("the idle window must still reach the builder, or it is a setting that silently does nothing")
        .isNotNull();

    try (final RaftGrpcServer server = start(parameters); final Http2Peer peer = server.connect()) {
      assertThat(peer.awaitClose(60_000)).isTrue();
    }
  }

  // ---------------------------------------------------------------------------
  // Row 3: zero means unbounded, which is the behaviour every release before this
  // ---------------------------------------------------------------------------

  @Test
  @Timeout(120)
  void aZeroIdleWindowLeavesTheConnectionOpen() throws IOException {
    final ContextConfiguration config = allowlistConfiguration();
    config.setValue(GlobalConfiguration.HA_GRPC_MAX_CONNECTION_IDLE_MS, 0L);

    try (final RaftGrpcServer server = start(config); final Http2Peer peer = server.connect()) {
      // A short wait that is EXPECTED to time out: a stall can only make it more true.
      assertThat(peer.awaitClose(3_000))
          .as("with the window disabled nothing may close the connection")
          .isFalse();
    }
  }

  // ---------------------------------------------------------------------------
  // Row 4: neither half configured - Ratis's builder is left exactly as it was
  // ---------------------------------------------------------------------------

  @Test
  void noCustomizerIsInstalledWhenNothingNeedsOne() {
    final ContextConfiguration config = allowlistConfiguration();
    config.setValue(GlobalConfiguration.HA_PEER_ALLOWLIST_ENABLED, false);
    config.setValue(GlobalConfiguration.HA_GRPC_MAX_CONNECTION_IDLE_MS, 0L);

    final Parameters parameters = detachedServer().buildParameters(config);

    assertThat(GrpcConfigKeys.Server.servicesCustomizer(parameters))
        .as("nothing to customize means nothing installed")
        .isNull();
  }

  // ---------------------------------------------------------------------------
  // Row 5: the #7250 halves still reach the builder alongside the new window
  // ---------------------------------------------------------------------------

  @Test
  void theAllowlistFilterAndInterceptorAreStillInstalled() {
    final ContextConfiguration config = allowlistConfiguration();
    config.setValue(GlobalConfiguration.HA_GRPC_MAX_CONNECTION_IDLE_MS, MIN_IDLE_MS);

    final RaftHAServer haServer = detachedServer();
    final Parameters parameters = haServer.buildParameters(config);

    assertThat(haServer.allowlistFilterForTest()).isNotNull();
    assertThat(haServer.allowlistInterceptorForTest()).isNotNull();
    assertThat(GrpcConfigKeys.Server.servicesCustomizer(parameters)).isNotNull();
  }

  // ---------------------------------------------------------------------------
  // Harness
  // ---------------------------------------------------------------------------

  private static ContextConfiguration allowlistConfiguration() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_SERVER_LIST, SERVER_LIST);
    return config;
  }

  private static RaftHAServer detachedServer() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_SERVER_LIST, SERVER_LIST);

    final ArcadeDBServer arcadeServer = mock(ArcadeDBServer.class);
    when(arcadeServer.getServerName()).thenReturn("ArcadeDB_0");

    return new RaftHAServer(arcadeServer, config);
  }

  private static RaftGrpcServer start(final ContextConfiguration config) throws IOException {
    return start(detachedServer().buildParameters(config));
  }

  /**
   * Applies the published customizer to a fresh {@link NettyServerBuilder} the same way
   * {@code GrpcServicesImpl.buildServer} does, and starts the result on an ephemeral port.
   */
  private static RaftGrpcServer start(final Parameters parameters) throws IOException {
    final GrpcServices.Customizer customizer = GrpcConfigKeys.Server.servicesCustomizer(parameters);
    assertThat(customizer).as("no customizer was published, so there is nothing to start").isNotNull();

    final NettyServerBuilder builder = NettyServerBuilder.forPort(0);
    final Server server = customizer.customize(builder, EnumSet.allOf(GrpcServices.Type.class)).build().start();
    return new RaftGrpcServer(server);
  }

  /** A started gRPC server plus the connections a test opened to it, all closed together. */
  private record RaftGrpcServer(Server server) implements AutoCloseable {

    Http2Peer connect() throws IOException {
      final Socket socket = new Socket();
      socket.connect(new InetSocketAddress("localhost", server.getPort()), 10_000);
      return new Http2Peer(socket);
    }

    @Override
    public void close() {
      server.shutdownNow();
    }
  }

  /**
   * The smallest thing that counts as an HTTP/2 client to gRPC: it sends the connection preface and an empty
   * SETTINGS frame, then reads frames and acknowledges SETTINGS and PING.
   * <p>
   * The PING acknowledgement is what makes this fast rather than correct-but-slow. gRPC's graceful shutdown sends
   * the first GOAWAY, then a PING, and only closes the socket once that PING is acknowledged or its own ten-second
   * timeout expires; a client that never answers turns a one-second idle window into an eleven-second test.
   */
  private static final class Http2Peer implements AutoCloseable {

    private static final byte[] PREFACE  = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".getBytes(StandardCharsets.US_ASCII);
    private static final int    SETTINGS = 0x4;
    private static final int    PING     = 0x6;
    private static final int    ACK      = 0x1;

    private final Socket       socket;
    private final InputStream  in;
    private final OutputStream out;

    Http2Peer(final Socket socket) throws IOException {
      this.socket = socket;
      this.in = socket.getInputStream();
      this.out = socket.getOutputStream();
      out.write(PREFACE);
      writeFrame(SETTINGS, 0, new byte[0]);
    }

    /**
     * Reads and answers frames until the server closes the connection or until {@code timeoutMs} elapses with the
     * connection silent. The handshake frames the server sends straight away are consumed by the same loop, which
     * is why this cannot be "read one frame and see what it is".
     * <p>
     * The timeout is per read, and it is a hang detector rather than a latency bound on either side of the
     * assertion: where the expected answer is true it is sized far above the window under test, and where it is
     * false the wait is one that is SUPPOSED to expire, which a stalled JVM can only make more true.
     *
     * @return true when the server closed the connection, false when it went quiet instead
     */
    boolean awaitClose(final int timeoutMs) throws IOException {
      socket.setSoTimeout(timeoutMs);
      while (true) {
        try {
          readOneFrame();
        } catch (final EOFException closedByServer) {
          return true;
        } catch (final SocketTimeoutException stillOpen) {
          return false;
        }
      }
    }

    private void readOneFrame() throws IOException {
      final byte[] header = readFully(9);
      final int length = ((header[0] & 0xff) << 16) | ((header[1] & 0xff) << 8) | (header[2] & 0xff);
      final int type = header[3] & 0xff;
      final int flags = header[4] & 0xff;
      final byte[] payload = readFully(length);

      if (type == SETTINGS && (flags & ACK) == 0)
        writeFrame(SETTINGS, ACK, new byte[0]);
      else if (type == PING && (flags & ACK) == 0)
        writeFrame(PING, ACK, payload);
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

    private void writeFrame(final int type, final int flags, final byte[] payload) throws IOException {
      final byte[] frame = new byte[9 + payload.length];
      frame[0] = (byte) (payload.length >>> 16);
      frame[1] = (byte) (payload.length >>> 8);
      frame[2] = (byte) payload.length;
      frame[3] = (byte) type;
      frame[4] = (byte) flags;
      System.arraycopy(payload, 0, frame, 9, payload.length);
      out.write(frame);
      out.flush();
    }

    @Override
    public void close() throws IOException {
      socket.close();
    }
  }
}
