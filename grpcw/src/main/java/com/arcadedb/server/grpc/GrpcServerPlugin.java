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
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerPlugin;
import com.arcadedb.utility.StringUtils;
import com.arcadedb.server.http.HttpAuthSessionManager;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.network.MultiAddressServerSocket;
import com.arcadedb.server.security.ServerSecurity;
import com.arcadedb.server.security.credential.DefaultCredentialsValidator;
import io.grpc.CompressorRegistry;
import io.grpc.DecompressorRegistry;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerCredentials;
import io.grpc.TlsServerCredentials;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.services.HealthStatusManager;
import io.grpc.protobuf.services.ProtoReflectionService;
import io.grpc.xds.XdsServerBuilder;
import io.micrometer.core.instrument.Metrics;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

/**
 * ArcadeDB gRPC Server Plugin
 * <p>
 * Configuration options:
 * - grpc.enabled: Enable/disable gRPC server (default: true)
 * - grpc.port: Port for standard gRPC server (default: 50051)
 * - grpc.host: Host to bind, every local address it resolves to (default: 0.0.0.0). Applies to the standard server only;
 *   the xDS server has no host setting and listens on every interface
 * - grpc.mode: Server mode - "standard", "xds", or "both" (default: standard)
 * - grpc.xds.port: Port for XDS server (default: 50052)
 * - grpc.tls.enabled: Enable TLS (default: false)
 * - grpc.tls.cert: Path to TLS certificate
 * - grpc.tls.key: Path to TLS private key
 * - grpc.maxMessageSize: Max message size in MB (default: 100)
 * - grpc.reflection.enabled: Enable gRPC reflection (default: true)
 * - grpc.health.enabled: Enable health checking (default: true)
 */
public class GrpcServerPlugin implements ServerPlugin {

  private          ArcadeDBServer      arcadeServer;
  private volatile Server              grpcServer;
  private volatile Server              xdsServer;
  private volatile HealthStatusManager healthManager;
  private volatile ArcadeDbGrpcService grpcService;  // Keep reference for cleanup
  private volatile Thread              shutdownHook;

  // Guards stopService() so the JVM shutdown hook and the plugin-lifecycle stop cannot run the cleanup twice. The
  // plugin is intentionally single-use: stop is terminal and is never reset, matching the create-once/destroy-once
  // ServerPlugin lifecycle. Restart-in-place is not supported (it would require nulling grpcService/healthManager too).
  private final AtomicBoolean stopped = new AtomicBoolean(false);

  @Override
  public void configure(ArcadeDBServer server, ContextConfiguration configuration) {
    this.arcadeServer = server;
  }

  @Override
  public void startService() {
    ContextConfiguration config = arcadeServer.getConfiguration();

    // Get configuration values with defaults
    boolean enabled = getConfigBoolean(config, GlobalConfiguration.GRPC_ENABLED);
    if (!enabled) {
      LogManager.instance().log(this, Level.INFO, "gRPC server is disabled");
      return;
    }

    String mode = getConfigString(config, GlobalConfiguration.GRPC_MODE).toLowerCase(Locale.ROOT);

    try {
      switch (mode) {
      case "standard" -> startStandardServer(config);
      case "xds" -> startXdsServer(config);
      case "both" -> {
        startStandardServer(config);
        startXdsServer(config);
      }
      default -> throw new ConfigurationException(
          "Invalid gRPC mode '" + mode + "' in " + GlobalConfiguration.GRPC_MODE.getKey() + ": use 'standard', 'xds' or 'both'");
      }

      registerShutdownHook();

    } catch (final IOException e) {
      stopAfterFailedStart(e);
      throw new RuntimeException("Failed to start gRPC server", e);
    } catch (final RuntimeException e) {
      // A runtime failure out of build() or start(), or inside startXdsServer() in "both" mode, left the same
      // partially-started server behind as the IOException did, with the same missing teardown (issue #7035). It
      // keeps its own type on the way out; only the checked exception is wrapped.
      stopAfterFailedStart(e);
      throw e;
    }
  }

  /**
   * Issue #6756 (1): ArcadeDBServer.start() deliberately does not call stopService() on a plugin whose
   * startService() threw, so a partially-started server (the service/reaper created by configureServer() before the
   * failing build().start(), or - in "both" mode - a fully running standard server left behind when the xDS server
   * fails afterward) would otherwise leak with no teardown path. stopService() is idempotent (guarded by the
   * "stopped" CAS), so this is safe even when nothing was actually started yet.
   */
  private void stopAfterFailedStart(final Exception e) {
    LogManager.instance().log(this, Level.SEVERE, "Failed to start gRPC server", e);
    stopService();
  }

  private void startStandardServer(ContextConfiguration config) throws IOException {

    // The default comes from the SERVER's configuration, not from the GlobalConfiguration enum, which carries
    // only what a system property or an environment variable put there: arcadedb.grpc.port is SCOPE.SERVER, so a
    // port named in the server configuration file used to be ignored in favour of the compiled-in one (#7233).
    int port = getConfigInt(config, GlobalConfiguration.GRPC_PORT);
    String host = getConfigString(config, GlobalConfiguration.GRPC_HOST);

    NettyServerBuilder serverBuilder;

    // Configure TLS if enabled
    if (getConfigBoolean(config, GlobalConfiguration.GRPC_TLS_ENABLED)) {
      serverBuilder = configureStandardTls(host, port, config);
    } else {
      serverBuilder = newListeningBuilder(host, port);
    }

    // Configure keepalive settings to prevent GOAWAY ENHANCE_YOUR_CALM errors
    // Allow clients to send keepalive pings every 10 seconds (client sends every 30s)
    serverBuilder
        .permitKeepAliveTime(10, TimeUnit.SECONDS)
        .permitKeepAliveWithoutCalls(true)
        .keepAliveTime(30, TimeUnit.SECONDS)
        .keepAliveTimeout(10, TimeUnit.SECONDS);

    // Configure the server
    configureServer(serverBuilder, config);

    // Inbound message size is set from grpc.maxMessageSize inside configureServer(); do not override it here so the
    // configured limit wins. The metadata cap is lowered from the former 32MB to a sane, configurable value.
    grpcServer = serverBuilder
        .maxInboundMetadataSize(getMaxMetadataSizeBytes(config))
        .build().start();

    // Build status message
    StringBuilder status = new StringBuilder();
    // The bound port, not the configured one: 0 asks the operating system for a free port (issue #8209)
    status.append("gRPC server started on ").append(host).append(":").append(grpcServer.getPort());
    status.append(" (mode: standard");

    if (getConfigBoolean(config, GlobalConfiguration.GRPC_TLS_ENABLED)) {
      status.append(", TLS enabled");
    }

    if (getConfigBoolean(config, GlobalConfiguration.GRPC_COMPRESSION_ENABLED)) {

      status.append(", compression: ");

      if (getConfigBoolean(config, GlobalConfiguration.GRPC_COMPRESSION_FORCE)) {
        status.append("forced-").append(getConfigString(config, GlobalConfiguration.GRPC_COMPRESSION_TYPE));
      } else {
        status.append("available");
      }
    }

    status.append(")");
    LogManager.instance().log(this, Level.INFO, status.toString());
  }

  private void startXdsServer(ContextConfiguration config) throws IOException {
    int port = getConfigInt(config, GlobalConfiguration.GRPC_XDS_PORT);

    // XDS server for service mesh integration. Credentials are derived from grpc.tls.* and fail closed when TLS is
    // requested but misconfigured, so xds/both modes honor TLS instead of always running insecure.
    final ServerCredentials xdsCredentials = resolveXdsCredentials(config);
    XdsServerBuilder xdsBuilder = XdsServerBuilder.forPort(port, xdsCredentials);

    // Configure the XDS server as a ServerBuilder
    configureServer(xdsBuilder, config);

    xdsServer = xdsBuilder
        .maxInboundMetadataSize(getMaxMetadataSizeBytes(config))
        .build().start();

    LogManager.instance().log(this, Level.INFO, "gRPC XDS server started on all interfaces, port %s (xDS management enabled; %s does not apply)",
        port, GlobalConfiguration.GRPC_HOST.getKey());
    final String host = getConfigString(config, GlobalConfiguration.GRPC_HOST);
    if (host != null && !host.isEmpty() && !"0.0.0.0".equals(host) && !"::".equals(host))
      LogManager.instance().log(this, Level.WARNING, "%s=%s restricts the standard gRPC server only: the xDS server listens on every interface",
          GlobalConfiguration.GRPC_HOST.getKey(), host);
  }

  // synchronized so the check-then-act initialization of the shared grpcService/healthManager fields stays thread-safe
  // even though this method is package-private (invoked from tests); in production startService drives it sequentially.
  synchronized void configureServer(ServerBuilder<?> serverBuilder, ContextConfiguration config) {

    // Get database directory path
    String databasePath = arcadeServer.getRootPath() + File.separator + "databases";

    // Build the main service only once and reuse it across every server builder. In "both" mode this method is
    // invoked twice (standard + xDS); constructing a fresh service per call would start a second idle-transaction
    // reaper thread and a second transaction registry that stopService() would never close, leaking both (issue #5050).
    if (this.grpcService == null) {
      // Idle-transaction reaper thresholds (issue #4802): reclaim abandoned transactions left open by clients that
      // disconnected without committing or rolling back.
      final long txMaxIdleMs = getConfigLong(config, GlobalConfiguration.GRPC_TX_MAX_IDLE_MS);
      final long txMaxAgeMs = getConfigLong(config, GlobalConfiguration.GRPC_TX_MAX_AGE_MS);
      final long txReaperPeriodMs = getConfigLong(config, GlobalConfiguration.GRPC_TX_REAPER_PERIOD_MS);

      // Concurrent-transaction caps (issue #5048): bound the per-transaction executor allocation so an authenticated
      // client cannot loop beginTransaction to exhaust threads/memory. A non-positive value disables the corresponding bound.
      final int maxConcurrentTx = getConfigInt(config, GlobalConfiguration.GRPC_MAX_CONCURRENT_TRANSACTIONS);
      final int maxConcurrentTxPerPrincipal = getConfigInt(config, GlobalConfiguration.GRPC_MAX_CONCURRENT_TRANSACTIONS_PER_PRINCIPAL);

      this.grpcService = new ArcadeDbGrpcService(databasePath, arcadeServer, txMaxIdleMs, txMaxAgeMs, txReaperPeriodMs,
          maxConcurrentTx, maxConcurrentTxPerPrincipal);
    }

    // Add the main service. In "both" mode the same BindableService instance is added to both the standard and xDS
    // builders; gRPC calls bindService() per server at build time, so sharing one service (and thus one tx registry
    // and reaper) across both transports is safe and is exactly the intended semantics.
    serverBuilder.addService(grpcService);

    // Create the Admin service
    ArcadeDbGrpcAdminService adminService = new ArcadeDbGrpcAdminService(arcadeServer, new DefaultCredentialsValidator());

    // Add the Admin service
    serverBuilder.addService(adminService);

    // Add health service if enabled. Reuse a single manager across both server builders (issue #5050).
    if (getConfigBoolean(config, GlobalConfiguration.GRPC_HEALTH_ENABLED)) {
      if (healthManager == null) {
        healthManager = new HealthStatusManager();

        // Set initial health status
        healthManager.setStatus(
            ArcadeDbGrpcService.class.getName(),
            HealthCheckResponse.ServingStatus.SERVING
        );
      }
      serverBuilder.addService(healthManager.getHealthService());
    }

    // Add reflection service if enabled
    if (getConfigBoolean(config, GlobalConfiguration.GRPC_REFLECTION_ENABLED)) {
      serverBuilder.addService(ProtoReflectionService.newInstance());
    }

    serverBuilder.compressorRegistry(CompressorRegistry.getDefaultInstance())
        .decompressorRegistry(DecompressorRegistry.getDefaultInstance());

    // Configure max message size. Clamp the lower bound to 1 MB (a non-positive value would be rejected by gRPC at
    // startup) and compute in long so a large MB value (>= 2048) does not overflow int and wrap negative.
    final int maxMessageSizeMB = Math.max(1, getConfigInt(config, GlobalConfiguration.GRPC_MAX_MESSAGE_SIZE));
    final long maxMessageSizeBytes = (long) maxMessageSizeMB * 1024 * 1024;

    serverBuilder.maxInboundMessageSize((int) Math.min(maxMessageSizeBytes, Integer.MAX_VALUE));

    // Add interceptors for logging, metrics, auth, etc.
    serverBuilder.intercept(new GrpcLoggingInterceptor());
    // Records whether each call's transport can carry secret material back to the caller. Registered
    // unconditionally, and on this shared path so both the standard and the xDS builder get it:
    // CreateApiToken refuses to mint when its context key is absent, so dropping this line disables
    // the minting of API tokens rather than silently disabling the check (issue #7309).
    // Built from the server's configuration, not the `config` argument: SET SERVER SETTING writes there, and the
    // trusted-proxy list is read live.
    serverBuilder.intercept(GrpcTransportSecurityInterceptor.forConfiguration(arcadeServer.getConfiguration()));
    // Publish gRPC metrics into the server's shared JVM-wide registry so the same exporters that
    // scrape the rest of the server (Prometheus, OTLP, JMX, Studio) also see gRPC telemetry.
    serverBuilder.intercept(new GrpcMetricsInterceptor(Metrics.globalRegistry));
    // Carries back the verdict "your own transaction published a commit under this call", which the retry loop
    // RemoteGrpcDatabase inherits from RemoteDatabase needs in order NOT to replay a block whose earlier half
    // is already durable (issue #8134). Registered unconditionally and on this shared path, so both the
    // standard and the xDS builder get it: dropping this line does not fail a call, it silently returns the
    // guard to the state issue #8134 reports - a BATCH-boundary block replayed after a conflict.
    serverBuilder.intercept(new GrpcSessionPartialCommitInterceptor());
    // Tags every call as a gRPC client request (issue #8363). RaftReplicatedDatabase refuses client requests on a
    // database whose directory is being replaced from the leader's snapshot and tells a client from the engine's own
    // threads by this tag, so an untagged RPC is served from the copy the cluster is discarding. Registered
    // unconditionally and on this shared path, so both the standard and the xDS builder get it.
    serverBuilder.intercept(new GrpcProtocolContextInterceptor());

    // Add compression interceptor if force compression is enabled
    if (getConfigBoolean(config, GlobalConfiguration.GRPC_COMPRESSION_FORCE)) {
      String compressionType = getConfigString(config, GlobalConfiguration.GRPC_COMPRESSION_TYPE);
      serverBuilder.intercept(new GrpcCompressionInterceptor(true, compressionType));
    }

    // Add authentication interceptor if security is configured
    final ServerSecurity serverSecurity = arcadeServer.getSecurity();
    if (serverSecurity != null) {
      HttpAuthSessionManager authSessionManager = null;
      final HttpServer httpServer = arcadeServer.getHttpServer();
      if (httpServer != null) {
        authSessionManager = httpServer.getAuthSessionManager();
      } else {
        LogManager.instance().log(this, Level.INFO,
            "HTTP server not available - token authentication disabled for gRPC");
      }
      serverBuilder.intercept(new GrpcAuthInterceptor(serverSecurity, authSessionManager));
    }
  }

  private NettyServerBuilder configureStandardTls(final String host, final int port, final ContextConfiguration config) {
    final File[] certKey = resolveTlsCertKey(config);
    try {
      // Configure Netty with TLS using SslContext
      return newListeningBuilder(host, port)
          .sslContext(GrpcSslContexts
              .forServer(certKey[0], certKey[1])
              .build());
    } catch (final Exception e) {
      // Fail closed: TLS was explicitly requested, so refuse to start rather than downgrade to cleartext.
      throw new SecurityException(
          "gRPC TLS is enabled but the SSL context could not be built from cert '" + certKey[0] + "' and key '" + certKey[1]
              + "'. Refusing to start with cleartext.", e);
    }
  }

  /**
   * Creates the builder listening on every local address {@code host} resolves to (issue #9318). {@code forPort(int)}
   * binds the wildcard address, so {@code grpc.host=127.0.0.1} used to leave the endpoint open on every interface
   * while the startup line reported the restriction. Same resolution as the other wire listeners (issue #9224).
   */
  static NettyServerBuilder newListeningBuilder(final String host, final int port) {
    final List<String> hosts = MultiAddressServerSocket.resolveListenHosts(host);
    NettyServerBuilder builder = null;
    for (final String listenHost : hosts) {
      final InetSocketAddress address = new InetSocketAddress(listenHost, port);
      if (builder == null)
        builder = NettyServerBuilder.forAddress(address);
      else
        builder.addListenAddress(address);
    }
    // never fall back to forPort(): it binds the wildcard address, the failure this method exists to prevent (issue #9318)
    if (builder == null)
      throw new ConfigurationException("gRPC host '" + host + "' resolved to no address to bind");
    return builder;
  }

  private ServerCredentials configureTlsCredentials(ContextConfiguration config) {
    final File[] certKey = resolveTlsCertKey(config);
    try {
      return TlsServerCredentials.create(certKey[0], certKey[1]);
    } catch (final Exception e) {
      // Fail closed: TLS was explicitly requested, so refuse to start rather than downgrade to cleartext.
      throw new SecurityException(
          "gRPC TLS is enabled but TLS server credentials could not be built from cert '" + certKey[0] + "' and key '"
              + certKey[1] + "'. Refusing to start with cleartext.", e);
    }
  }

  /**
   * Resolves and validates the configured TLS certificate/key when TLS is enabled. Fails closed: a request for TLS
   * with a missing path or absent file throws instead of silently downgrading to cleartext, so the server never
   * accepts plaintext while an operator believes TLS is active.
   */
  private File[] resolveTlsCertKey(ContextConfiguration config) {
    final String certPath = getConfigString(config, GlobalConfiguration.GRPC_TLS_CERT);
    final String keyPath = getConfigString(config, GlobalConfiguration.GRPC_TLS_KEY);

    if (certPath == null || keyPath == null)
      throw new SecurityException(
          "gRPC TLS is enabled (" + GlobalConfiguration.GRPC_TLS_ENABLED.getKey() + "=true) but the certificate ("
              + GlobalConfiguration.GRPC_TLS_CERT.getKey() + ") or key (" + GlobalConfiguration.GRPC_TLS_KEY.getKey()
              + ") path is not configured. Refusing to start with cleartext.");

    final File certFile = new File(certPath);
    final File keyFile = new File(keyPath);

    if (!certFile.exists() || !keyFile.exists())
      throw new SecurityException("gRPC TLS is enabled but the certificate or key file does not exist (cert='" + certPath
          + "', key='" + keyPath + "'). Refusing to start with cleartext.");

    return new File[] { certFile, keyFile };
  }

  /**
   * Derives the XDS server credentials from {@code grpc.tls.*}. When TLS is enabled the credentials are built
   * fail-closed from the configured cert/key; when TLS is disabled the XDS transport is intentionally insecure and
   * relies on the service mesh to provide mTLS at the transport layer.
   */
  private ServerCredentials resolveXdsCredentials(ContextConfiguration config) {
    if (getConfigBoolean(config, GlobalConfiguration.GRPC_TLS_ENABLED))
      return configureTlsCredentials(config);
    return InsecureServerCredentials.create();
  }

  private void registerShutdownHook() {
    shutdownHook = new Thread(() -> {
      LogManager.instance().log(GrpcServerPlugin.this, Level.INFO, "Shutting down gRPC server...");
      stopService();
    });
    Runtime.getRuntime().addShutdownHook(shutdownHook);
  }

  @Override
  public void stopService() {
    // Idempotency / concurrency guard: the JVM shutdown hook and the plugin-lifecycle stop may both call this. Only
    // the first invocation performs the cleanup; later ones return immediately (issue #5050).
    if (!stopped.compareAndSet(false, true))
      return;

    try {
      // Update health status to NOT_SERVING
      if (healthManager != null) {
        healthManager.setStatus(
            ArcadeDbGrpcService.class.getName(),
            HealthCheckResponse.ServingStatus.NOT_SERVING
        );
      }

      // Close the gRPC service to release database connections
      if (grpcService != null) {
        try {
          grpcService.close();
          LogManager.instance().log(this, Level.INFO, "gRPC service closed and database connections released");
        } catch (Exception e) {
          LogManager.instance().log(this, Level.SEVERE, "Error closing gRPC service", e);
        }
      }

      // Shutdown servers gracefully
      if (grpcServer != null) {
        grpcServer.shutdown();
        if (!grpcServer.awaitTermination(30, TimeUnit.SECONDS)) {
          grpcServer.shutdownNow();
          grpcServer.awaitTermination(5, TimeUnit.SECONDS);
        }
        LogManager.instance().log(this, Level.INFO, "Standard gRPC server stopped");
      }

      if (xdsServer != null) {
        xdsServer.shutdown();
        if (!xdsServer.awaitTermination(30, TimeUnit.SECONDS)) {
          xdsServer.shutdownNow();
          xdsServer.awaitTermination(5, TimeUnit.SECONDS);
        }
        LogManager.instance().log(this, Level.INFO, "XDS gRPC server stopped");
      }

      // Remove shutdown hook if it exists
      if (shutdownHook != null) {
        try {
          Runtime.getRuntime().removeShutdownHook(shutdownHook);
        } catch (IllegalStateException e) {
          // Already shutting down
        }
      }

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LogManager.instance().log(this, Level.SEVERE, "Interrupted while shutting down gRPC server", e);
    }
  }

  /**
   * Returns the underlying gRPC service instance (used for monitoring and testing).
   */
  public ArcadeDbGrpcService getService() {
    return grpcService;
  }

  /**
   * The port the standard gRPC server ACTUALLY bound, which is not necessarily the configured one: {@code 0} asks the
   * operating system for a free port (issue #8209). Returns -1 when the server is not running.
   */
  public int getPort() {
    final Server s = grpcServer;
    return s != null && !s.isShutdown() ? s.getPort() : -1;
  }

  /**
   * Get the status of the gRPC servers
   */
  public ServerStatus getStatus() {
    return new ServerStatus(
        grpcServer != null && !grpcServer.isShutdown(),
        xdsServer != null && !xdsServer.isShutdown(),
        grpcServer != null ? grpcServer.getPort() : -1,
        xdsServer != null ? xdsServer.getPort() : -1
    );
  }

  // Configuration helper methods. Every key is a declared GlobalConfiguration setting (issue #9316), so the server
  // configuration file, the environment and -D all reach it; the plugin never reads a bare key.
  /**
   * The value of a declared setting: the overlay first (server configuration file, SET SERVER SETTING), then a system
   * property read NOW, then what the enum holds (a -D at JVM start, an environment variable, the default). The live
   * system property is what the plugin always honoured while the keys were bare strings: the enum alone reads -D once, at
   * class load, so a property set afterwards (the transaction-reaper integration tests do) would be silently ignored.
   */
  private Object resolve(final ContextConfiguration config, final GlobalConfiguration setting) {
    final Object overlayOrProperty = config.getValue(setting.getKey(), (Object) null);
    return overlayOrProperty != null ? overlayOrProperty : config.getValue(setting);
  }

  private String getConfigString(final ContextConfiguration config, final GlobalConfiguration setting) {
    final Object value = resolve(config, setting);
    return value == null ? null : value.toString();
  }

  private int getConfigInt(final ContextConfiguration config, final GlobalConfiguration setting) {
    final Object value = resolve(config, setting);
    if (value instanceof Number number)
      return number.intValue();
    if (value != null) {
      try {
        return Integer.parseInt(value.toString().trim());
      } catch (final NumberFormatException e) {
        LogManager.instance().log(this, Level.WARNING, "Invalid integer value for %s: %s", setting.getKey(), value);
      }
    }
    return ((Number) setting.getDefValue()).intValue();
  }

  private long getConfigLong(final ContextConfiguration config, final GlobalConfiguration setting) {
    final Object value = resolve(config, setting);
    if (value instanceof Number number)
      return number.longValue();
    if (value != null) {
      try {
        return Long.parseLong(value.toString().trim());
      } catch (final NumberFormatException e) {
        LogManager.instance().log(this, Level.WARNING, "Invalid long value for %s: %s", setting.getKey(), value);
      }
    }
    return ((Number) setting.getDefValue()).longValue();
  }

  /**
   * Resolves the inbound metadata cap in bytes from {@code grpc.maxMetadataSize} (KB, default 16). gRPC headers are
   * small; a cap far above a few KB only invites metadata-flood memory pressure. A non-positive configured value is
   * clamped to 1 KB.
   */
  int getMaxMetadataSizeBytes(final ContextConfiguration config) {
    final int kb = getConfigInt(config, GlobalConfiguration.GRPC_MAX_METADATA_SIZE);
    // Guard against int overflow for an absurdly large configured value (kb * 1024 would wrap negative and make
    // gRPC reject the builder at startup).
    if (kb >= Integer.MAX_VALUE / 1024)
      return Integer.MAX_VALUE;
    return Math.max(1, kb) * 1024;
  }

  private boolean getConfigBoolean(final ContextConfiguration config, final GlobalConfiguration setting) {
    // The enum swallows a -D or an environment value that is not boolean text (it reports it and keeps the default), which
    // for arcadedb.grpc.tls.enabled, whose default is false, would start a plaintext endpoint. The raw text of those two
    // sources is therefore checked here as well (issue #8935).
    rejectNonBooleanText(setting, System.getProperty(setting.getKey()));
    rejectNonBooleanText(setting, System.getenv(setting.getKey()));

    final Object value = resolve(config, setting);
    if (value instanceof Boolean bool)
      return bool;
    if (value == null)
      return (Boolean) setting.getDefValue();

    // Boolean.parseBoolean() turns "yes", "1" or a typo into false without a word, which for arcadedb.grpc.tls.enabled
    // starts a plaintext endpoint: only the two spellings are a boolean, anything else refuses to start (issue #8935)
    return parseStrictBoolean(setting, value.toString());
  }

  private static void rejectNonBooleanText(final GlobalConfiguration setting, final String text) {
    if (text != null)
      parseStrictBoolean(setting, text);
  }

  private static boolean parseStrictBoolean(final GlobalConfiguration setting, final String text) {
    try {
      return StringUtils.parseStrictBoolean(text);
    } catch (final IllegalArgumentException e) {
      throw new ConfigurationException("Invalid boolean value for " + setting.getKey() + ": '" + text + "', expected true or false");
    }
  }

  public static class ServerStatus {
    public final boolean standardServerRunning;
    public final boolean xdsServerRunning;
    public final int     standardPort;
    public final int     xdsPort;

    public ServerStatus(boolean standardServerRunning, boolean xdsServerRunning,
        int standardPort, int xdsPort) {
      this.standardServerRunning = standardServerRunning;
      this.xdsServerRunning = xdsServerRunning;
      this.standardPort = standardPort;
      this.xdsPort = xdsPort;
    }
  }
}
