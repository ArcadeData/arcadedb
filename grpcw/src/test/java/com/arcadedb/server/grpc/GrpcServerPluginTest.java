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
package com.arcadedb.server.grpc;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.exception.ConfigurationException;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.StaticBaseServerTest;
import com.arcadedb.server.TestServerHelper;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ClientInterceptors;
import io.grpc.DecompressorRegistry;
import io.grpc.ForwardingClientCall;
import io.grpc.ForwardingClientCallListener;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthGrpc;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketAddress;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.arcadedb.server.grpc.GrpcTransportSecurityInterceptorTest.PROXY_IP;
import static com.arcadedb.server.grpc.GrpcTransportSecurityInterceptorTest.decisionFor;
import static com.arcadedb.server.grpc.GrpcTransportSecurityInterceptorTest.forwardedProto;
import static com.arcadedb.server.grpc.GrpcTransportSecurityInterceptorTest.proxy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class GrpcServerPluginTest {

  @TempDir
  Path tempDir;

  @Test
  void pluginConfiguresWithDefaults() {
    GrpcServerPlugin plugin = new GrpcServerPlugin();
    ContextConfiguration config = new ContextConfiguration();

    final ArcadeDBServer server = TestServerHelper.unstartedServer(tempDir, config);

    plugin.configure(server, config);

    // Plugin should be configured without errors
    GrpcServerPlugin.ServerStatus status = plugin.getStatus();
    assertThat(status.standardServerRunning).isFalse(); // Not started yet
  }

  @Test
  void pluginDisabledWhenConfigured() {
    GrpcServerPlugin plugin = new GrpcServerPlugin();
    ContextConfiguration config = new ContextConfiguration();
    config.setValue("arcadedb.grpc.enabled", "false");

    final ArcadeDBServer server = TestServerHelper.unstartedServer(tempDir, config);

    plugin.configure(server, config);
    plugin.startService();

    GrpcServerPlugin.ServerStatus status = plugin.getStatus();
    assertThat(status.standardServerRunning).isFalse();
    assertThat(status.xdsServerRunning).isFalse();
  }

  @Test
  void serverStatusReportsCorrectPorts() {
    GrpcServerPlugin.ServerStatus status = new GrpcServerPlugin.ServerStatus(
        true, false, 50051, -1);

    assertThat(status.standardServerRunning).isTrue();
    assertThat(status.xdsServerRunning).isFalse();
    assertThat(status.standardPort).isEqualTo(50051);
    assertThat(status.xdsPort).isEqualTo(-1);
  }

  /** Issue #8935: "yes" must not silently start a plaintext endpoint, only true/false are booleans. */
  @Test
  void tlsEnabledWithANonBooleanValueRefusesToStart() {
    final GrpcServerPlugin plugin = new GrpcServerPlugin();
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue("arcadedb.grpc.tls.enabled", "yes");

    final ArcadeDBServer server = TestServerHelper.unstartedServer(tempDir, config);

    plugin.configure(server, config);
    assertThatThrownBy(plugin::startService)
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("arcadedb.grpc.tls.enabled")
        .hasMessageContaining("yes");
    assertThat(plugin.getStatus().standardServerRunning).isFalse();
  }

  @Test
  void enabledAcceptsTrimmedCaseInsensitiveBooleans() {
    final GrpcServerPlugin plugin = new GrpcServerPlugin();
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue("arcadedb.grpc.enabled", " FALSE ");

    final ArcadeDBServer server = TestServerHelper.unstartedServer(tempDir, config);

    plugin.configure(server, config);
    plugin.startService();
    assertThat(plugin.getStatus().standardServerRunning).isFalse();
  }

  /*
   * Regression tests for issue #5050 (gRPC plugin lifecycle & cleanup):
   * <ul>
   *   <li>CON-2: in "both" mode {@code configureServer} is invoked once per server builder; it must build exactly one
   *       {@link ArcadeDbGrpcService} (and one health manager) and reuse it, otherwise the first service's idle-reaper
   *       thread and transaction registry leak forever.</li>
   *   <li>CON-6: {@code stopService} must be idempotent so the JVM shutdown hook and the plugin-lifecycle stop cannot
   *       double-run the cleanup.</li>
   * </ul>
   * These tests exercise the builder-configuration path directly (no ports are bound) so they stay fast and deterministic.
   */
  private ContextConfiguration config;

  private GrpcServerPlugin newConfiguredPlugin() {
    final GrpcServerPlugin plugin = new GrpcServerPlugin();
    config = new ContextConfiguration();
    final ArcadeDBServer server = TestServerHelper.unstartedServer(tempDir, config);
    plugin.configure(server, config);
    return plugin;
  }

  // Regression: #5050
  @Test
  void bothModeReusesSingleServiceInstance() {
    final GrpcServerPlugin plugin = newConfiguredPlugin();

    // Simulate "both" mode: configure the standard builder, then the xDS builder, against the same plugin.
    plugin.configureServer(NettyServerBuilder.forPort(0), config);
    final ArcadeDbGrpcService first = plugin.getService();
    assertThat(first).isNotNull();
    assertThat(first.isIdleReaperActive()).isTrue();

    plugin.configureServer(NettyServerBuilder.forPort(0), config);
    final ArcadeDbGrpcService second = plugin.getService();

    // The single service must be reused across both builders - no orphaned second instance whose reaper thread leaks.
    assertThat(second).isSameAs(first);

    plugin.stopService();
  }

  // Regression: #5050
  @Test
  void stopServiceIsIdempotent() {
    final GrpcServerPlugin plugin = newConfiguredPlugin();

    plugin.configureServer(NettyServerBuilder.forPort(0), config);
    assertThat(plugin.getService()).isNotNull();

    // First stop performs cleanup; every subsequent stop must be a safe no-op (shutdown hook vs. lifecycle race).
    assertThatCode(() -> {
      plugin.stopService();
      plugin.stopService();
      plugin.stopService();
    }).doesNotThrowAnyException();
  }

  // Regression: #6756
  /**
   * Regression test for issue #6756 (1): {@code GrpcServerPlugin.startService()} constructs the
   * {@link ArcadeDbGrpcService} (which starts its idle-transaction reaper thread) inside
   * {@code configureServer()}, before the Netty server actually binds the port. If binding fails (e.g. the
   * port is already in use), {@code ArcadeDBServer.start()} deliberately never calls {@code stopService()}
   * on a plugin whose {@code startService()} threw - so the already-created service and its reaper thread
   * leaked with no teardown path.
   * <p>
   * The fix wraps {@code startService()}'s body so any failure calls the (idempotent) {@code stopService()}
   * before rethrowing.
   */
  @Nested
  class StartServiceFailureCleanup {
    @TempDir
    Path tempDir;

    private ServerSocket portHog;
    private GrpcServerPlugin plugin;

    @AfterEach
    void cleanup() throws IOException {
      if (plugin != null)
        plugin.stopService();
      if (portHog != null)
        portHog.close();
    }

    @Test
    void startServiceFailureStopsTheLeakedServiceAndReaper() throws IOException {
      // Occupy a real port so the plugin's own bind attempt fails with a genuine "address already in use".
      portHog = new ServerSocket(0);
      final int occupiedPort = portHog.getLocalPort();

      plugin = new GrpcServerPlugin();
      final ContextConfiguration config = new ContextConfiguration();
      // GrpcServerPlugin reads this back via ContextConfiguration.getValueAsString, which casts the stored
      // value directly to String - it must be stored as a String, not the boxed Integer overload would give.
      config.setValue(GlobalConfiguration.GRPC_PORT.getKey(), String.valueOf(occupiedPort));
      final ArcadeDBServer server = TestServerHelper.unstartedServer(tempDir, config);
      plugin.configure(server, config);

      assertThatThrownBy(plugin::startService).isInstanceOf(RuntimeException.class);

      // configureServer() built the service (and its reaper) before the failing bind - confirm the leak
      // scenario is real, not a no-op test.
      final ArcadeDbGrpcService service = plugin.getService();
      assertThat(service).as("the service must have been constructed before the bind failure").isNotNull();

      // The fix: startService()'s catch block must have called stopService(), which shuts the reaper down.
      assertThat(service.isIdleReaperShutdown())
          .as("the idle-transaction reaper thread must not leak past a failed startService()").isTrue();
    }
  }

  // Regression: #7035
  /**
   * Regression test for the second residue of issue #7035: {@code GrpcServerPlugin.startService()}'s cleanup added by
   * issue #6756 caught {@code IOException} only, so a runtime failure out of the server build - after
   * {@code configureServer()} had already created the service and started its idle-transaction reaper - still
   * escaped without {@code stopService()}, leaking both. The catch now covers runtime failures too, and rethrows them
   * with their own type.
   * <p>
   * The failure is injected at {@code getMaxMetadataSizeBytes()}, which {@code startStandardServer()} asks right after
   * {@code configureServer()} and right before {@code build().start()}: the same point in the sequence a runtime
   * failure of the builder would land on.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class StartServiceRuntimeFailureCleanup {
    @TempDir
    Path tempDir;

    private GrpcServerPlugin plugin;

    @AfterEach
    void cleanup() {
      if (plugin != null)
        plugin.stopService();
    }

    @Test
    void runtimeFailureWhileBuildingTheServerStopsTheLeakedServiceAndReaper() throws IOException {
      final int freePort = StaticBaseServerTest.allocateFreePorts(1)[0];

      plugin = new GrpcServerPlugin() {
        @Override
        int getMaxMetadataSizeBytes(final ContextConfiguration config) {
          throw new IllegalStateException("simulated runtime failure while building the gRPC server");
        }
      };
      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.GRPC_PORT.getKey(), String.valueOf(freePort));
      final ArcadeDBServer server = TestServerHelper.unstartedServer(tempDir, config);
      plugin.configure(server, config);

      assertThatThrownBy(plugin::startService)
          .as("a runtime failure keeps its own type on the way out")
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("simulated runtime failure");

      final ArcadeDbGrpcService service = plugin.getService();
      assertThat(service).as("the service must have been constructed before the failure, or this proves nothing").isNotNull();
      assertThat(service.isIdleReaperShutdown())
          .as("the idle-transaction reaper thread must not leak past a failed startService()").isTrue();
    }
  }

  /*
   * Issue #7233: the gRPC row caps and stream write timeout are SCOPE.SERVER, so they live in the server's
   * {@link ContextConfiguration} - written by the server configuration file, {@code SET SERVER SETTING} and the MCP
   * tool - and used to be read off the {@link GlobalConfiguration} enum, which only a system property or an
   * environment variable ever writes.
   * <p>
   * Every one of those five reads resolves through {@code serverConfiguration()}, so asserting on it asserts on all
   * of them, including that a service built without a server still reads a value rather than throwing.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  // Regression: #7233
  @Test
  void theSettingsResolveAgainstTheServersOwnConfiguration() {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.fromJSON("{\"configuration\":{\"server.grpcQueryMaxResultRows\":4242}}");

    final ArcadeDBServer server = TestServerHelper.unstartedServer(tempDir, configuration);

    final ArcadeDbGrpcService service = new ArcadeDbGrpcService("./target/databases", server);

    assertThat(service.serverConfiguration()).isSameAs(configuration);
    assertThat(service.serverConfiguration().getValueAsInteger(GlobalConfiguration.SERVER_GRPC_QUERY_MAX_RESULT_ROWS))
        .isEqualTo(4242);
  }

  // Regression: #7233
  @Test
  void aServiceWithNoServerStillReadsTheProcessWideValue() {
    final ArcadeDbGrpcService service = new ArcadeDbGrpcService("./target/databases", null);

    assertThat(service.serverConfiguration().getValueAsLong(GlobalConfiguration.SERVER_GRPC_STREAM_WRITE_TIMEOUT_MS))
        .isEqualTo(GlobalConfiguration.SERVER_GRPC_STREAM_WRITE_TIMEOUT_MS.getValueAsLong());
  }

  /*
   * Issue #7821, reachability: the transport-security interceptor the gRPC plugin actually registers must be the one
   * that reads the server's trusted-proxy list. A plugin registering the no-argument interceptor would still pass every
   * loopback integration test while silently ignoring the list.
   */
  // Regression: #7821
  @Test
  void theRegisteredInterceptorHonorsTheServersTrustedProxyList() {
    final ContextConfiguration serverConfiguration = new ContextConfiguration();
    final ArcadeDBServer server = TestServerHelper.unstartedServer(tempDir, serverConfiguration);

    final GrpcServerPlugin plugin = new GrpcServerPlugin();
    plugin.configure(server, serverConfiguration);

    final ServerBuilder<?> builder = mock(ServerBuilder.class, RETURNS_SELF);
    try {
      plugin.configureServer(builder, serverConfiguration);

      final ArgumentCaptor<ServerInterceptor> registered = ArgumentCaptor.forClass(ServerInterceptor.class);
      verify(builder, atLeastOnce()).intercept(registered.capture());
      final List<GrpcTransportSecurityInterceptor> security = registered.getAllValues().stream()
          .filter(GrpcTransportSecurityInterceptor.class::isInstance)
          .map(GrpcTransportSecurityInterceptor.class::cast)
          .toList();
      assertThat(security).hasSize(1);
      final GrpcTransportSecurityInterceptor interceptor = security.getFirst();

      assertThat(decisionFor(interceptor, proxy(), false, forwardedProto("https"))).isFalse();

      // What SET SERVER SETTING does: change the server's configuration after the interceptor was registered.
      serverConfiguration.setValue(GlobalConfiguration.SERVER_API_TOKEN_TRUSTED_PROXIES, PROXY_IP);
      assertThat(decisionFor(interceptor, proxy(), false, forwardedProto("https"))).isTrue();
    } finally {
      plugin.stopService();
    }
  }

  /*
   * Issue #9315: {@code ContextConfiguration.fromJSON} stores a declared setting under its declared type, so an integer setting
   * written in {@code server-configuration.json} (as a number or as a string) is an {@link Integer} in the overlay, and the
   * plugin used to read it as text and take the whole server down with a {@link ClassCastException}. Every setting the plugin
   * reads now goes through the same typed resolution (issue #9316); the metadata cap is a declared integer read through it.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  // Regression: #9315
  @Test
  void anIntegerWrittenAsANumberIsRead() {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.fromJSON("{\"configuration\":{\"grpc.maxMetadataSize\":8}}");

    assertThat(new GrpcServerPlugin().getMaxMetadataSizeBytes(configuration)).isEqualTo(8 * 1024);
  }

  // Regression: #9315
  @Test
  void anIntegerWrittenAsAStringIsRead() {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.fromJSON("{\"configuration\":{\"grpc.maxMetadataSize\":\"8\"}}");

    assertThat(new GrpcServerPlugin().getMaxMetadataSizeBytes(configuration)).isEqualTo(8 * 1024);
  }

  /*
   * Regression test for issue #9318: {@code arcadedb.grpc.host} was read and logged but never bound, so the listener was
   * always on the wildcard address.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  // Regression: #9318
  @Test
  void loopbackHostBindsOnlyLoopback() throws Exception {
    final Server server = GrpcServerPlugin.newListeningBuilder("127.0.0.1", 0).build().start();
    try {
      assertThat(server.getListenSockets()).isNotEmpty();
      for (final SocketAddress address : server.getListenSockets()) {
        final InetSocketAddress inet = (InetSocketAddress) address;
        assertThat(inet.getAddress().isLoopbackAddress()).as("bound to %s", inet).isTrue();
        assertThat(inet.getAddress().isAnyLocalAddress()).isFalse();
      }
    } finally {
      server.shutdownNow().awaitTermination(10, TimeUnit.SECONDS);
    }
  }

  // Regression: #9318
  @Test
  void wildcardHostStillBindsWildcard() throws Exception {
    final Server server = GrpcServerPlugin.newListeningBuilder("0.0.0.0", 0).build().start();
    try {
      assertThat(((InetSocketAddress) server.getListenSockets().get(0)).getAddress().isAnyLocalAddress()).isTrue();
    } finally {
      server.shutdownNow().awaitTermination(10, TimeUnit.SECONDS);
    }
  }

  // Regression: #9318
  @Test
  void unresolvableHostIsRefusedByName() {
    assertThatThrownBy(() -> GrpcServerPlugin.newListeningBuilder("no-such-host.invalid", 0))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("no-such-host.invalid");
  }

  // Regression: #9782
  /**
   * Regression test for issue #9782: {@code arcadedb.grpc.compression.enabled} was read and printed in the startup line but never
   * applied. Both registries were installed whatever it said, and {@code compression.force} was not gated on it, so with
   * {@code enabled=false, force=true} every response was compressed and the startup line did not mention compression.
   * <p>
   * The wiring is exercised on a real listening server (the health service, which every build exposes), because what the setting
   * promises is what a client sees on the wire: whether a compressed request is understood and whether a response is compressed.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class CompressionEnabled {
    private static final Metadata.Key<String> GRPC_ENCODING = Metadata.Key.of("grpc-encoding", Metadata.ASCII_STRING_MARSHALLER);

    @TempDir
    Path tempDir;

    private GrpcServerPlugin plugin;
    private Server           server;
    private ManagedChannel   channel;

    @AfterEach
    void tearDown() throws InterruptedException {
      if (channel != null)
        channel.shutdownNow().awaitTermination(10, TimeUnit.SECONDS);
      if (server != null)
        server.shutdownNow().awaitTermination(10, TimeUnit.SECONDS);
      if (plugin != null)
        plugin.stopService();
    }

    private void start(final boolean enabled, final boolean force) throws Exception {
      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.GRPC_COMPRESSION_ENABLED, enabled);
      config.setValue(GlobalConfiguration.GRPC_COMPRESSION_FORCE, force);

      final ArcadeDBServer arcadeServer = TestServerHelper.unstartedServer(tempDir, config);
      plugin = new GrpcServerPlugin();
      plugin.configure(arcadeServer, config);

      final NettyServerBuilder builder = NettyServerBuilder.forPort(0);
      plugin.configureServer(builder, config);
      server = builder.build().start();
      channel = ManagedChannelBuilder.forAddress("localhost", server.getPort()).usePlaintext().build();
    }

    /** Calls the health service, asking for a compressed request when {@code requestCodec} is set; returns the response encoding. */
    private String check(final String requestCodec) {
      final AtomicReference<String> responseEncoding = new AtomicReference<>();
      final ClientInterceptor capture = new ClientInterceptor() {
        @Override
        public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(final MethodDescriptor<ReqT, RespT> method,
            final CallOptions callOptions, final Channel next) {
          return new ForwardingClientCall.SimpleForwardingClientCall<>(next.newCall(method, callOptions)) {
            @Override
            public void start(final Listener<RespT> responseListener, final Metadata headers) {
              super.start(new ForwardingClientCallListener.SimpleForwardingClientCallListener<>(responseListener) {
                @Override
                public void onHeaders(final Metadata responseHeaders) {
                  responseEncoding.set(responseHeaders.get(GRPC_ENCODING));
                  super.onHeaders(responseHeaders);
                }
              }, headers);
            }
          };
        }
      };

      HealthGrpc.HealthBlockingStub stub = HealthGrpc.newBlockingStub(ClientInterceptors.intercept(channel, capture))
          .withDeadlineAfter(30, TimeUnit.SECONDS);
      if (requestCodec != null)
        stub = stub.withCompression(requestCodec);

      final HealthCheckResponse response = stub.check(HealthCheckRequest.getDefaultInstance());
      assertThat(response.getStatus()).isEqualTo(HealthCheckResponse.ServingStatus.SERVING);
      return responseEncoding.get();
    }

    @Test
    void compressionEnabledAcceptsACompressedRequestAndDoesNotCompressUnlessForced() throws Exception {
      start(true, false);

      assertThat(check("gzip")).as("a compressed request is understood").isNotEqualTo("gzip");
      assertThat(check(null)).as("nothing forces the response").isNotEqualTo("gzip");
    }

    @Test
    void compressionEnabledAndForcedCompressesTheResponse() throws Exception {
      start(true, true);

      assertThat(check(null)).as("force compresses a response to a client that accepts gzip").isEqualTo("gzip");
    }

    @Test
    void compressionDisabledRefusesACompressedRequest() throws Exception {
      start(false, false);

      assertThatThrownBy(() -> check("gzip")).isInstanceOf(StatusRuntimeException.class)
          .satisfies(e -> assertThat(((StatusRuntimeException) e).getStatus().getCode()).isEqualTo(Status.Code.UNIMPLEMENTED));
      assertThat(check(null)).as("an uncompressed call still works, and is answered uncompressed").isNotEqualTo("gzip");
    }

    @Test
    void compressionDisabledRefusesACompressedStreamingCallToo() throws Exception {
      start(false, false);

      // THE REFUSAL IS MADE WHEN THE STREAM IS CREATED, WHATEVER THE KIND OF CALL: A SERVER-STREAMING ONE (WATCH) IS REFUSED AS WELL
      final HealthGrpc.HealthBlockingStub stub = HealthGrpc.newBlockingStub(channel).withDeadlineAfter(30, TimeUnit.SECONDS)
          .withCompression("gzip");
      assertThatThrownBy(() -> stub.watch(HealthCheckRequest.getDefaultInstance()).hasNext()).isInstanceOf(StatusRuntimeException.class)
          .satisfies(e -> assertThat(((StatusRuntimeException) e).getStatus().getCode()).isEqualTo(Status.Code.UNIMPLEMENTED));

      final HealthGrpc.HealthBlockingStub plain = HealthGrpc.newBlockingStub(channel).withDeadlineAfter(30, TimeUnit.SECONDS);
      assertThat(plain.watch(HealthCheckRequest.getDefaultInstance()).next().getStatus())
          .isEqualTo(HealthCheckResponse.ServingStatus.SERVING);
    }

    @Test
    void disabledRegistriesStillUnderstandIdentityWithoutAdvertisingAnything() {
      // grpc-java never sends "grpc-encoding: identity" itself, but other clients do, and the spec has every server accept it
      final DecompressorRegistry disabled = GrpcServerPlugin.decompressorRegistry(false);
      assertThat(disabled.lookupDecompressor("identity")).isNotNull();
      assertThat(disabled.lookupDecompressor("gzip")).isNull();
      assertThat(disabled.getAdvertisedMessageEncodings()).isEmpty();
      assertThat(GrpcServerPlugin.compressorRegistry(false).lookupCompressor("gzip")).isNull();

      final DecompressorRegistry enabled = GrpcServerPlugin.decompressorRegistry(true);
      assertThat(enabled.lookupDecompressor("gzip")).isNotNull();
      assertThat(enabled.getAdvertisedMessageEncodings()).contains("gzip");
      assertThat(GrpcServerPlugin.compressorRegistry(true).lookupCompressor("gzip")).isNotNull();
    }

    @Test
    void forceIsIgnoredWhenCompressionIsDisabled() throws Exception {
      start(false, true);

      assertThat(check(null)).as("compression is off, so nothing is compressed whatever force says").isNotEqualTo("gzip");
    }
  }
}
