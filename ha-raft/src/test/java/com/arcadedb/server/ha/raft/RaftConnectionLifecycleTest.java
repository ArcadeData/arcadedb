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
import com.arcadedb.server.TestServerHelper;
import com.arcadedb.server.ha.raft.ratis.RemoteAddressCallInterceptor;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.grpc.server.GrpcServices;
import org.apache.ratis.thirdparty.io.grpc.Server;
import org.apache.ratis.thirdparty.io.grpc.netty.NettyServerBuilder;
import org.junit.jupiter.api.Nested;
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
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Lifetime of the Raft gRPC connections: idle reaping and bounds on a busy one.
 */
class RaftConnectionLifecycleTest {

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
   */
  @Nested
  class ReapsIdleConnection {

    private static final String SERVER_LIST = "localhost:2434:2480,localhost:2435:2481";

    /**
     * gRPC clamps any window below one second up to one second ({@code MIN_MAX_CONNECTION_IDLE_NANO}), so this is the
     * shortest window the server will actually honour.
     */
    private static final long   MIN_IDLE_MS  = 1_000L;

    // ---------------------------------------------------------------------------
    // Row 1: the reported case - the allowlist is on, so a revocation can happen
    // ---------------------------------------------------------------------------

    // Regression for #7316: an idle connection is reaped when the allowlist is on
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

    // Regression for #7316: an idle connection is reaped with the allowlist disabled
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

    // Regression for #7316: a zero idle window leaves the connection open
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
    // Row 4: neither half configured - no filter and no window, only the remote-address interceptor (issue #10225)
    // ---------------------------------------------------------------------------

    // Regression for #7316: neither half configured installs only the remote address interceptor
    @Test
    void neitherHalfConfiguredInstallsOnlyTheRemoteAddressInterceptor() {
      final ContextConfiguration config = allowlistConfiguration();
      config.setValue(GlobalConfiguration.HA_PEER_ALLOWLIST_ENABLED, false);
      config.setValue(GlobalConfiguration.HA_GRPC_MAX_CONNECTION_IDLE_MS, 0L);

      final Parameters parameters = detachedServer().buildParameters(config);

      final RaftGrpcServicesCustomizer installed = (RaftGrpcServicesCustomizer) GrpcConfigKeys.Server.servicesCustomizer(
          parameters);
      assertThat(installed.filters()).as("the allowlist is off").isEmpty();
      assertThat(installed.boundsConnections()).as("no window is configured").isFalse();
      assertThat(installed.interceptors()).hasExactlyElementsOfTypes(RemoteAddressCallInterceptor.class);
    }

    // ---------------------------------------------------------------------------
    // Row 5: the #7250 halves still reach the builder alongside the new window
    // ---------------------------------------------------------------------------

    // Regression for #7316: the allowlist filter and interceptor are still installed
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

      final ArcadeDBServer arcadeServer = TestServerHelper.unstartedServer("ArcadeDB_0");

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

  /**
   * Regression tests for issue #7339, a follow-up to #7316.
   * <p>
   * #7316 bounded a Raft gRPC connection that goes <i>quiet</i>: {@code arcadedb.ha.grpcMaxConnectionIdleMs} reaches
   * {@code NettyServerBuilder.maxConnectionIdle}, and gRPC measures that window from the most recent moment the
   * transport's active-stream count reached zero. Every RPC opens and closes an HTTP/2 stream and therefore pushes the
   * deadline forward by the whole window - including the RPCs {@code PeerAllowlistCallInterceptor} answers with
   * {@code PERMISSION_DENIED}. So a revoked peer that keeps <i>retrying</i>, which a removed peer whose Ratis division
   * has not learned it was removed does at every election timeout, keeps its connection forever.
   * <p>
   * {@code arcadedb.ha.grpcMaxConnectionAgeMs} is the unconditional bound for that case: the timer is scheduled once,
   * in {@code NettyServerHandler.handlerAdded}, and fires whatever the connection is carrying.
   * <p>
   * The tests drive the real production path - {@code RaftHAServer.buildParameters} publishes a
   * {@code GrpcServices.Customizer}, the test applies it to a {@link NettyServerBuilder} exactly as
   * {@code GrpcServicesImpl.buildServer} does, starts the server, and watches a real HTTP/2 connection from the
   * outside - and they read the GOAWAY debug string rather than a stopwatch to say <i>which</i> window closed a
   * connection: gRPC names the timer that fired, {@code "max_age"} or {@code "max_idle"}.
   */
  @Nested
  class BoundsBusyConnection {

    private static final String SERVER_LIST = "localhost:2434:2480,localhost:2435:2481";

    /** gRPC raises anything below a second to a second, for both windows, so this is the shortest honoured value. */
    private static final long MIN_WINDOW_MS = 1_000L;

    /**
     * One refused RPC every 100 ms against a 1 000 ms idle window. The margin is what keeps the two tests below from
     * being a bet on the scheduler: for the drumbeat to lapse and let {@code max_idle} fire, the JVM would have to
     * stop for nine consecutive beats. Both tests report the widest gap they actually saw, so a red run says whether
     * the harness stalled or the server reaped a busy connection.
     */
    private static final long BEAT_MS = 100L;

    // ---------------------------------------------------------------------------
    // Row 1: the defect - the idle window cannot reach a connection that stays busy
    // ---------------------------------------------------------------------------

    // Regression for #7339: a busy connection survives the idle window when the age is disabled
    @Test
    @Timeout(120)
    void aBusyConnectionSurvivesTheIdleWindowWhenTheAgeIsDisabled() throws Exception {
      final ContextConfiguration config = baseConfiguration();
      config.setValue(GlobalConfiguration.HA_GRPC_MAX_CONNECTION_IDLE_MS, MIN_WINDOW_MS);
      config.setValue(GlobalConfiguration.HA_GRPC_MAX_CONNECTION_AGE_MS, 0L);

      try (final RaftGrpcServer server = start(config); final Http2ConnectionProbe peer = server.connect()) {
        peer.drumUntilClosed(5_000L, BEAT_MS);

        // Not a vacuous pass: if the drumbeat were malformed HTTP/2 the server would have answered with a GOAWAY of
        // its own, and if it never opened a stream the 1 000 ms idle window would have reaped the connection.
        assertThat(peer.beats())
            .as("the drumbeat has to have actually opened streams, or this test proves nothing")
            .isGreaterThan(20);
        assertThat(peer.closeReason())
            .as("this is the defect: a peer that keeps starting RPCs pushes the idle deadline forward by the whole "
                + "window every time, refused RPCs included, so nothing closes its connection. %s", peer.timing())
            .isNull();
      }
    }

    // ---------------------------------------------------------------------------
    // Row 2: the fix - the age window closes it, and it is the age that does it
    // ---------------------------------------------------------------------------

    // Regression for #7339: the age bound closes a connection the idle window cannot reach
    @Test
    @Timeout(120)
    void theAgeBoundClosesAConnectionTheIdleWindowCannotReach() throws Exception {
      final ContextConfiguration config = baseConfiguration();
      config.setValue(GlobalConfiguration.HA_GRPC_MAX_CONNECTION_IDLE_MS, MIN_WINDOW_MS);
      config.setValue(GlobalConfiguration.HA_GRPC_MAX_CONNECTION_AGE_MS, 3_000L);
      config.setValue(GlobalConfiguration.HA_GRPC_MAX_CONNECTION_AGE_GRACE_MS, 500L);

      try (final RaftGrpcServer server = start(config); final Http2ConnectionProbe peer = server.connect()) {
        peer.drumUntilClosed(60_000L, BEAT_MS);

        assertThat(peer.beats()).as("the drumbeat has to have actually opened streams").isGreaterThan(20);
        assertThat(peer.closeReason())
            .as("the same drumbeat that defeats the idle window must not defeat the age window, and the GOAWAY has "
                + "to name the age timer rather than the idle one - max_idle here would mean the harness stalled "
                + "long enough for the idle window to fire, not that the fix worked. %s", peer.timing())
            .isEqualTo("max_age");
      }
    }

    // ---------------------------------------------------------------------------
    // Row 3: the age is a connection-lifetime setting, not an allowlist setting
    // ---------------------------------------------------------------------------

    // Regression for #7339: the age bound is installed with the allowlist disabled
    @Test
    @Timeout(120)
    void theAgeBoundIsInstalledWithTheAllowlistDisabled() throws Exception {
      final ContextConfiguration config = baseConfiguration();
      config.setValue(GlobalConfiguration.HA_PEER_ALLOWLIST_ENABLED, false);
      config.setValue(GlobalConfiguration.HA_GRPC_MAX_CONNECTION_IDLE_MS, 0L);
      config.setValue(GlobalConfiguration.HA_GRPC_MAX_CONNECTION_AGE_MS, MIN_WINDOW_MS);

      final RaftHAServer haServer = detachedServer();
      final Parameters parameters = haServer.buildParameters(config);
      assertThat(haServer.allowlistFilterForTest()).as("the allowlist is off, so no filter is built").isNull();
      assertThat(GrpcConfigKeys.Server.servicesCustomizer(parameters))
          .as("the age window must still reach the builder, or it is a setting that silently does nothing on "
              + "exactly the clusters that turned the allowlist off")
          .isNotNull();

      try (final RaftGrpcServer server = start(parameters); final Http2ConnectionProbe peer = server.connect()) {
        peer.drumUntilClosed(60_000L, BEAT_MS);
        assertThat(peer.closeReason()).as("%s", peer.timing()).isEqualTo("max_age");
      }
    }

    // ---------------------------------------------------------------------------
    // Row 4: nothing configured at all - no filter and no window, only the remote-address interceptor (issue #10225)
    // ---------------------------------------------------------------------------

    // Regression for #7339: nothing configured installs only the remote address interceptor
    @Test
    void nothingConfiguredInstallsOnlyTheRemoteAddressInterceptor() {
      final ContextConfiguration config = baseConfiguration();
      config.setValue(GlobalConfiguration.HA_PEER_ALLOWLIST_ENABLED, false);
      config.setValue(GlobalConfiguration.HA_GRPC_MAX_CONNECTION_IDLE_MS, 0L);
      config.setValue(GlobalConfiguration.HA_GRPC_MAX_CONNECTION_AGE_MS, 0L);

      final GrpcServices.Customizer customizer = GrpcConfigKeys.Server.servicesCustomizer(
          detachedServer().buildParameters(config));
      assertThat(customizer).as("the misdirected-request guard needs each call's remote address on every cluster")
          .isInstanceOf(RaftGrpcServicesCustomizer.class);
      final RaftGrpcServicesCustomizer installed = (RaftGrpcServicesCustomizer) customizer;
      assertThat(installed.filters()).as("the allowlist is off").isEmpty();
      assertThat(installed.boundsConnections()).as("a window that nothing turned on must not start bounding connections")
          .isFalse();
      assertThat(installed.interceptors()).hasExactlyElementsOfTypes(RemoteAddressCallInterceptor.class);
    }

    /** With the allowlist on, its interceptor and the remote-address one are both installed (issue #10225). */
    // Regression for #7339: the remote address interceptor is installed beside the allowlist
    @Test
    void theRemoteAddressInterceptorIsInstalledBesideTheAllowlist() {
      final ContextConfiguration config = baseConfiguration();
      config.setValue(GlobalConfiguration.HA_PEER_ALLOWLIST_ENABLED, true);

      final RaftGrpcServicesCustomizer installed = (RaftGrpcServicesCustomizer) GrpcConfigKeys.Server.servicesCustomizer(
          detachedServer().buildParameters(config));
      assertThat(installed.interceptors()).hasExactlyElementsOfTypes(PeerAllowlistCallInterceptor.class,
          RemoteAddressCallInterceptor.class);
    }

    // ---------------------------------------------------------------------------
    // Row 5: the grace reaches the builder, and a typo in it does not stop the node
    // ---------------------------------------------------------------------------

    // Regression for #7339: a negative grace is clamped rather than crashing startup
    @Test
    void aNegativeGraceIsClampedRatherThanCrashingStartup() {
      // NettyServerBuilder.maxConnectionAgeGrace checkArgument()s a non-negative value, so an unclamped -1 from a
      // configuration file would throw out of the customizer while Ratis is building its server - i.e. the node
      // would not start. Clamping keeps a typo in a tuning knob from being a startup failure.
      final RaftGrpcServicesCustomizer customizer = new RaftGrpcServicesCustomizer(null, null, 0L, MIN_WINDOW_MS, -1L);

      assertThatCode(() -> customizer.customize(NettyServerBuilder.forPort(0), EnumSet.allOf(GrpcServices.Type.class)))
          .as("a negative grace must be clamped, not rethrown as a startup failure")
          .doesNotThrowAnyException();
    }

    // Regression for #7339: a zero grace still closes the connection
    @Test
    @Timeout(120)
    void aZeroGraceStillClosesTheConnection() throws Exception {
      // The other end of the grace range: 0 means "cancel whatever is still running at the age boundary". gRPC's own
      // default for the grace is infinite, which this code never passes - see RaftGrpcServicesCustomizer.
      final ContextConfiguration config = baseConfiguration();
      config.setValue(GlobalConfiguration.HA_GRPC_MAX_CONNECTION_IDLE_MS, 0L);
      config.setValue(GlobalConfiguration.HA_GRPC_MAX_CONNECTION_AGE_MS, MIN_WINDOW_MS);
      config.setValue(GlobalConfiguration.HA_GRPC_MAX_CONNECTION_AGE_GRACE_MS, 0L);

      try (final RaftGrpcServer server = start(config); final Http2ConnectionProbe peer = server.connect()) {
        peer.drumUntilClosed(60_000L, BEAT_MS);
        assertThat(peer.closeReason()).as("%s", peer.timing()).isEqualTo("max_age");
      }
    }

    // ---------------------------------------------------------------------------
    // Row 6: the defaults promise no behaviour change for a cluster that opts out
    // ---------------------------------------------------------------------------

    // Regression for #7339: the age window is off by default
    @Test
    void theAgeWindowIsOffByDefault() {
      assertThat(GlobalConfiguration.HA_GRPC_MAX_CONNECTION_AGE_MS.getDefValue())
          .as("the age window recycles healthy connections too, so it cannot default on without the measurement "
              + "issue #7339 asks for - see RaftConnectionAgeRecyclingIT")
          .isEqualTo(0L);
      assertThat(GlobalConfiguration.HA_GRPC_MAX_CONNECTION_AGE_MS.getType()).isEqualTo(Long.class);
      assertThat(GlobalConfiguration.HA_GRPC_MAX_CONNECTION_AGE_GRACE_MS.getDefValue()).isEqualTo(5_000L);
      assertThat(GlobalConfiguration.HA_GRPC_MAX_CONNECTION_AGE_GRACE_MS.getType()).isEqualTo(Long.class);
    }

    // ---------------------------------------------------------------------------
    // Harness
    // ---------------------------------------------------------------------------

    private static ContextConfiguration baseConfiguration() {
      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.HA_SERVER_LIST, SERVER_LIST);
      return config;
    }

    private static RaftHAServer detachedServer() {
      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.HA_SERVER_LIST, SERVER_LIST);

      final ArcadeDBServer arcadeServer = TestServerHelper.unstartedServer("ArcadeDB_0");

      return new RaftHAServer(arcadeServer, config);
    }

    private static RaftGrpcServer start(final ContextConfiguration config) throws IOException {
      return start(detachedServer().buildParameters(config));
    }

    private static RaftGrpcServer start(final Parameters parameters) throws IOException {
      final GrpcServices.Customizer customizer = GrpcConfigKeys.Server.servicesCustomizer(parameters);
      assertThat(customizer).as("no customizer was published, so there is nothing to start").isNotNull();

      final NettyServerBuilder builder = NettyServerBuilder.forPort(0);
      final Server server = customizer.customize(builder, EnumSet.allOf(GrpcServices.Type.class)).build().start();
      return new RaftGrpcServer(server);
    }

    private record RaftGrpcServer(Server server) implements AutoCloseable {

      Http2ConnectionProbe connect() throws IOException {
        return Http2ConnectionProbe.connectTo("localhost", server.getPort());
      }

      @Override
      public void close() {
        server.shutdownNow();
      }
    }
  }
}
