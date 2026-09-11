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

import java.io.IOException;
import java.util.EnumSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue7339BoundsBusyRaftConnectionTest {

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
  // Row 4: nothing configured at all - Ratis's builder is left exactly as it was
  // ---------------------------------------------------------------------------

  @Test
  void noCustomizerIsInstalledWhenNothingNeedsOne() {
    final ContextConfiguration config = baseConfiguration();
    config.setValue(GlobalConfiguration.HA_PEER_ALLOWLIST_ENABLED, false);
    config.setValue(GlobalConfiguration.HA_GRPC_MAX_CONNECTION_IDLE_MS, 0L);
    config.setValue(GlobalConfiguration.HA_GRPC_MAX_CONNECTION_AGE_MS, 0L);

    assertThat(GrpcConfigKeys.Server.servicesCustomizer(detachedServer().buildParameters(config)))
        .as("a third window that nothing turned on must not start installing a customizer on its own")
        .isNull();
  }

  // ---------------------------------------------------------------------------
  // Row 5: the grace reaches the builder, and a typo in it does not stop the node
  // ---------------------------------------------------------------------------

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

  @Test
  void theAgeWindowIsOffByDefault() {
    assertThat(GlobalConfiguration.HA_GRPC_MAX_CONNECTION_AGE_MS.getDefValue())
        .as("the age window recycles healthy connections too, so it cannot default on without the measurement "
            + "issue #7339 asks for - see Issue7339RaftConnectionAgeRecyclingIT")
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

    final ArcadeDBServer arcadeServer = mock(ArcadeDBServer.class);
    when(arcadeServer.getServerName()).thenReturn("ArcadeDB_0");

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
