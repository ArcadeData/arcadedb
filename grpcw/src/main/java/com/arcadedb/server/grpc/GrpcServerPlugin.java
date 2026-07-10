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
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerPlugin;
import com.arcadedb.server.http.HttpAuthSessionManager;
import com.arcadedb.server.http.HttpServer;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

/**
 * ArcadeDB gRPC Server Plugin
 * <p>
 * Configuration options:
 * - grpc.enabled: Enable/disable gRPC server (default: true)
 * - grpc.port: Port for standard gRPC server (default: 50051)
 * - grpc.host: Host to bind (default: 0.0.0.0)
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
  private          Thread              shutdownHook;

  // Guards stopService() so the JVM shutdown hook and the plugin-lifecycle stop cannot run the cleanup twice. The
  // plugin is intentionally single-use: stop is terminal and is never reset, matching the create-once/destroy-once
  // ServerPlugin lifecycle. Restart-in-place is not supported (it would require nulling grpcService/healthManager too).
  private final AtomicBoolean stopped = new AtomicBoolean(false);

  // Configuration keys as simple strings
  private static final String CONFIG_PREFIX              = "arcadedb.grpc.";
  private static final String CONFIG_ENABLED             = CONFIG_PREFIX + "enabled";
  private static final String CONFIG_PORT                = CONFIG_PREFIX + "port";
  private static final String CONFIG_HOST                = CONFIG_PREFIX + "host";
  private static final String CONFIG_MODE                = CONFIG_PREFIX + "mode";
  private static final String CONFIG_XDS_PORT            = CONFIG_PREFIX + "xds.port";
  private static final String CONFIG_TLS_ENABLED         = CONFIG_PREFIX + "tls.enabled";
  private static final String CONFIG_TLS_CERT            = CONFIG_PREFIX + "tls.cert";
  private static final String CONFIG_TLS_KEY             = CONFIG_PREFIX + "tls.key";
  private static final String CONFIG_MAX_MESSAGE_SIZE    = CONFIG_PREFIX + "maxMessageSize";
  private static final String CONFIG_MAX_METADATA_SIZE   = CONFIG_PREFIX + "maxMetadataSize";
  private static final String CONFIG_MAX_CONCURRENT_TX   = CONFIG_PREFIX + "maxConcurrentTransactions";
  private static final String CONFIG_MAX_CONCURRENT_TX_PER_PRINCIPAL = CONFIG_PREFIX + "maxConcurrentTransactionsPerPrincipal";
  private static final String CONFIG_REFLECTION_ENABLED  = CONFIG_PREFIX + "reflection.enabled";
  private static final String CONFIG_HEALTH_ENABLED      = CONFIG_PREFIX + "health.enabled";
  private static final String CONFIG_COMPRESSION_ENABLED = CONFIG_PREFIX + "compression.enabled";
  private static final String CONFIG_COMPRESSION_FORCE   = CONFIG_PREFIX + "compression.force";
  private static final String CONFIG_COMPRESSION_TYPE    = CONFIG_PREFIX + "compression.type";
  private static final String CONFIG_TX_MAX_IDLE_MS      = CONFIG_PREFIX + "tx.maxIdleMs";
  private static final String CONFIG_TX_MAX_AGE_MS       = CONFIG_PREFIX + "tx.maxAgeMs";
  private static final String CONFIG_TX_REAPER_PERIOD_MS = CONFIG_PREFIX + "tx.reaperPeriodMs";

  @Override
  public void configure(ArcadeDBServer server, ContextConfiguration configuration) {
    this.arcadeServer = server;
  }

  @Override
  public void startService() {
    ContextConfiguration config = arcadeServer.getConfiguration();

    // Get configuration values with defaults
    boolean enabled = getConfigBoolean(config, CONFIG_ENABLED, true);
    if (!enabled) {
      LogManager.instance().log(this, Level.INFO, "gRPC server is disabled");
      return;
    }

    String mode = getConfigString(config, CONFIG_MODE, "standard").toLowerCase();

    try {
      switch (mode) {
      case "standard" -> startStandardServer(config);
      case "xds" -> startXdsServer(config);
      case "both" -> {
        startStandardServer(config);
        startXdsServer(config);
      }
      default -> LogManager.instance().log(this, Level.SEVERE, "Invalid gRPC mode: %s. Use 'standard', 'xds', or 'both'", mode);
      }

      registerShutdownHook();

    } catch (IOException e) {
      LogManager.instance().log(this, Level.SEVERE, "Failed to start gRPC server", e);
      throw new RuntimeException("Failed to start gRPC server", e);
    }
  }

  private void startStandardServer(ContextConfiguration config) throws IOException {

    int port = getConfigInt(config, CONFIG_PORT, 50051);
    String host = getConfigString(config, CONFIG_HOST, "0.0.0.0");

    NettyServerBuilder serverBuilder;

    // Configure TLS if enabled
    if (getConfigBoolean(config, CONFIG_TLS_ENABLED, false)) {
      serverBuilder = configureStandardTls(port, config);
    } else {
      serverBuilder = NettyServerBuilder.forPort(port);
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
    status.append("gRPC server started on ").append(host).append(":").append(port);
    status.append(" (mode: standard");

    if (getConfigBoolean(config, CONFIG_TLS_ENABLED, false)) {
      status.append(", TLS enabled");
    }

    if (getConfigBoolean(config, CONFIG_COMPRESSION_ENABLED, true)) {

      status.append(", compression: ");

      if (getConfigBoolean(config, CONFIG_COMPRESSION_FORCE, false)) {
        status.append("forced-").append(getConfigString(config, CONFIG_COMPRESSION_TYPE, "gzip"));
      } else {
        status.append("available");
      }
    }

    status.append(")");
    LogManager.instance().log(this, Level.INFO, status.toString());
  }

  private void startXdsServer(ContextConfiguration config) throws IOException {
    int port = getConfigInt(config, CONFIG_XDS_PORT, 50052);

    // XDS server for service mesh integration. Credentials are derived from grpc.tls.* and fail closed when TLS is
    // requested but misconfigured, so xds/both modes honor TLS instead of always running insecure.
    final ServerCredentials xdsCredentials = resolveXdsCredentials(config);
    XdsServerBuilder xdsBuilder = XdsServerBuilder.forPort(port, xdsCredentials);

    // Configure the XDS server as a ServerBuilder
    configureServer(xdsBuilder, config);

    xdsServer = xdsBuilder
        .maxInboundMetadataSize(getMaxMetadataSizeBytes(config))
        .build().start();

    LogManager.instance().log(this, Level.INFO, "gRPC XDS server started on port %s (xDS management enabled)", port);
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
      final long txMaxIdleMs = getConfigLong(config, CONFIG_TX_MAX_IDLE_MS, ArcadeDbGrpcService.DEFAULT_TX_MAX_IDLE_MS);
      final long txMaxAgeMs = getConfigLong(config, CONFIG_TX_MAX_AGE_MS, ArcadeDbGrpcService.DEFAULT_TX_MAX_AGE_MS);
      final long txReaperPeriodMs = getConfigLong(config, CONFIG_TX_REAPER_PERIOD_MS,
          ArcadeDbGrpcService.DEFAULT_TX_REAPER_PERIOD_MS);

      // Concurrent-transaction caps (issue #5048): bound the per-transaction executor allocation so an authenticated
      // client cannot loop beginTransaction to exhaust threads/memory. A non-positive value disables the corresponding bound.
      final int maxConcurrentTx = getConfigInt(config, CONFIG_MAX_CONCURRENT_TX,
          ArcadeDbGrpcService.DEFAULT_MAX_CONCURRENT_TX);
      final int maxConcurrentTxPerPrincipal = getConfigInt(config, CONFIG_MAX_CONCURRENT_TX_PER_PRINCIPAL,
          ArcadeDbGrpcService.DEFAULT_MAX_CONCURRENT_TX_PER_PRINCIPAL);

      this.grpcService = new ArcadeDbGrpcService(databasePath, arcadeServer, txMaxIdleMs, txMaxAgeMs, txReaperPeriodMs,
          maxConcurrentTx, maxConcurrentTxPerPrincipal);
    }

    // Add the main service
    serverBuilder.addService(grpcService);

    // Create the Admin service
    ArcadeDbGrpcAdminService adminService = new ArcadeDbGrpcAdminService(arcadeServer, new DefaultCredentialsValidator());

    // Add the Admin service
    serverBuilder.addService(adminService);

    // Add health service if enabled. Reuse a single manager across both server builders (issue #5050).
    if (getConfigBoolean(config, CONFIG_HEALTH_ENABLED, true)) {
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
    if (getConfigBoolean(config, CONFIG_REFLECTION_ENABLED, true)) {
      serverBuilder.addService(ProtoReflectionService.newInstance());
    }

    serverBuilder.compressorRegistry(CompressorRegistry.getDefaultInstance())
        .decompressorRegistry(DecompressorRegistry.getDefaultInstance());

    // Configure max message size. Clamp the lower bound to 1 MB (a non-positive value would be rejected by gRPC at
    // startup) and compute in long so a large MB value (>= 2048) does not overflow int and wrap negative.
    final int maxMessageSizeMB = Math.max(1, getConfigInt(config, CONFIG_MAX_MESSAGE_SIZE, 100));
    final long maxMessageSizeBytes = (long) maxMessageSizeMB * 1024 * 1024;

    serverBuilder.maxInboundMessageSize((int) Math.min(maxMessageSizeBytes, Integer.MAX_VALUE));

    // Add interceptors for logging, metrics, auth, etc.
    serverBuilder.intercept(new GrpcLoggingInterceptor());
    // Publish gRPC metrics into the server's shared JVM-wide registry so the same exporters that
    // scrape the rest of the server (Prometheus, OTLP, JMX, Studio) also see gRPC telemetry.
    serverBuilder.intercept(new GrpcMetricsInterceptor(Metrics.globalRegistry));

    // Add compression interceptor if force compression is enabled
    if (getConfigBoolean(config, CONFIG_COMPRESSION_FORCE, false)) {
      String compressionType = getConfigString(config, CONFIG_COMPRESSION_TYPE, "gzip");
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

  private NettyServerBuilder configureStandardTls(int port, ContextConfiguration config) {
    final File[] certKey = resolveTlsCertKey(config);
    try {
      // Configure Netty with TLS using SslContext
      return NettyServerBuilder.forPort(port)
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
    final String certPath = getConfigString(config, CONFIG_TLS_CERT, null);
    final String keyPath = getConfigString(config, CONFIG_TLS_KEY, null);

    if (certPath == null || keyPath == null)
      throw new SecurityException("gRPC TLS is enabled (" + CONFIG_TLS_ENABLED + "=true) but the certificate ("
          + CONFIG_TLS_CERT + ") or key (" + CONFIG_TLS_KEY + ") path is not configured. Refusing to start with cleartext.");

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
    if (getConfigBoolean(config, CONFIG_TLS_ENABLED, false))
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

  // Configuration helper methods
  private String getConfigString(ContextConfiguration config, String key, String defaultValue) {

    return config.getValueAsString(key, defaultValue);
  }

  private int getConfigInt(ContextConfiguration config, String key, int defaultValue) {

    String value = getConfigString(config, key, null);
    if (value != null) {
      try {
        return Integer.parseInt(value);
      } catch (NumberFormatException e) {
        LogManager.instance().log(this, Level.WARNING, "Invalid integer value for %s: %s", key, value);
      }
    }
    return defaultValue;
  }

  private long getConfigLong(ContextConfiguration config, String key, long defaultValue) {

    String value = getConfigString(config, key, null);
    if (value != null) {
      try {
        return Long.parseLong(value.trim());
      } catch (NumberFormatException e) {
        LogManager.instance().log(this, Level.WARNING, "Invalid long value for %s: %s", key, value);
      }
    }
    return defaultValue;
  }

  /**
   * Resolves the inbound metadata cap in bytes from {@code grpc.maxMetadataSize} (KB, default 16). gRPC headers are
   * small; a cap far above a few KB only invites metadata-flood memory pressure. A non-positive configured value is
   * clamped to 1 KB.
   */
  int getMaxMetadataSizeBytes(ContextConfiguration config) {
    final int kb = getConfigInt(config, CONFIG_MAX_METADATA_SIZE, 16);
    // Guard against int overflow for an absurdly large configured value (kb * 1024 would wrap negative and make
    // gRPC reject the builder at startup).
    if (kb >= Integer.MAX_VALUE / 1024)
      return Integer.MAX_VALUE;
    return Math.max(1, kb) * 1024;
  }

  private boolean getConfigBoolean(ContextConfiguration config, String key, boolean defaultValue) {
    String value = getConfigString(config, key, null);
    if (value != null) {
      return Boolean.parseBoolean(value);
    }
    return defaultValue;
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
