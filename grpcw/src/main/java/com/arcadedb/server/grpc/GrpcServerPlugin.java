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
import com.arcadedb.server.security.ServerSecurity;
import com.arcadedb.server.security.credential.DefaultCredentialsValidator;
import io.grpc.CompressorRegistry;
import io.grpc.DecompressorRegistry;
import io.grpc.Grpc;
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

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
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

  private ArcadeDBServer      arcadeServer;
  private Server              grpcServer;
  private Server              xdsServer;
  private HealthStatusManager healthManager;
  private ArcadeDbGrpcService grpcService;  // Keep reference for cleanup
  private Thread              shutdownHook;

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
  private static final String CONFIG_REFLECTION_ENABLED  = CONFIG_PREFIX + "reflection.enabled";
  private static final String CONFIG_HEALTH_ENABLED      = CONFIG_PREFIX + "health.enabled";
  private static final String CONFIG_COMPRESSION_ENABLED = CONFIG_PREFIX + "compression.enabled";
  private static final String CONFIG_COMPRESSION_FORCE   = CONFIG_PREFIX + "compression.force";
  private static final String CONFIG_COMPRESSION_TYPE    = CONFIG_PREFIX + "compression.type";

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

    grpcServer = serverBuilder
        .maxInboundMessageSize(256 * 1024 * 1024)
        .maxInboundMetadataSize(32 * 1024 * 1024)
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

    // XDS server for service mesh integration
    // XdsServerBuilder requires ServerCredentials
    XdsServerBuilder xdsBuilder = XdsServerBuilder.forPort(port, InsecureServerCredentials.create());

    // Configure the XDS server as a ServerBuilder
    configureServer(xdsBuilder, config);

    xdsServer = xdsBuilder
        .maxInboundMessageSize(256 * 1024 * 1024)
        .maxInboundMetadataSize(32 * 1024 * 1024)
        .build().start();

    LogManager.instance().log(this, Level.INFO, "gRPC XDS server started on port %s (xDS management enabled)", port);
  }

  private void configureServer(ServerBuilder<?> serverBuilder, ContextConfiguration config) {

    // Get database directory path
    String databasePath = arcadeServer.getRootPath() + File.separator + "databases";

    // Create the main service
    ArcadeDbGrpcService mainService = new ArcadeDbGrpcService(databasePath, arcadeServer);

    // Add the main service
    serverBuilder.addService(mainService);

    // Create the Admin service
    ArcadeDbGrpcAdminService adminService = new ArcadeDbGrpcAdminService(arcadeServer, new DefaultCredentialsValidator());

    // Add the Admin service
    serverBuilder.addService(adminService);

    // Add health service if enabled
    if (getConfigBoolean(config, CONFIG_HEALTH_ENABLED, true)) {
      healthManager = new HealthStatusManager();
      serverBuilder.addService(healthManager.getHealthService());

      // Set initial health status
      healthManager.setStatus(
          ArcadeDbGrpcService.class.getName(),
          HealthCheckResponse.ServingStatus.SERVING
      );
    }

    // Add reflection service if enabled
    if (getConfigBoolean(config, CONFIG_REFLECTION_ENABLED, true)) {
      serverBuilder.addService(ProtoReflectionService.newInstance());
    }

    serverBuilder.compressorRegistry(CompressorRegistry.getDefaultInstance())
        .decompressorRegistry(DecompressorRegistry.getDefaultInstance());

    // Configure max message size
    int maxMessageSizeMB = getConfigInt(config, CONFIG_MAX_MESSAGE_SIZE, 100);

    serverBuilder.maxInboundMessageSize(maxMessageSizeMB * 1024 * 1024);

    // Add interceptors for logging, metrics, auth, etc.
    serverBuilder.intercept(new GrpcLoggingInterceptor());
    serverBuilder.intercept(new GrpcMetricsInterceptor(arcadeServer));

    // Add compression interceptor if force compression is enabled
    if (getConfigBoolean(config, CONFIG_COMPRESSION_FORCE, false)) {
      String compressionType = getConfigString(config, CONFIG_COMPRESSION_TYPE, "gzip");
      serverBuilder.intercept(new GrpcCompressionInterceptor(true, compressionType, 1024));
    }

    // Add authentication interceptor if security is configured
    ServerSecurity serverSecurity = arcadeServer.getSecurity();
    if (serverSecurity != null) {
      serverBuilder.intercept(new GrpcAuthInterceptor(serverSecurity));
    }
  }

  private NettyServerBuilder configureStandardTls(int port, ContextConfiguration config) {
    String certPath = getConfigString(config, CONFIG_TLS_CERT, null);
    String keyPath = getConfigString(config, CONFIG_TLS_KEY, null);

    if (certPath == null || keyPath == null) {
      LogManager.instance()
          .log(this, Level.WARNING, "TLS enabled but certificate or key path not provided. Falling back to insecure.");
      return NettyServerBuilder.forPort(port);
    }

    File certFile = new File(certPath);
    File keyFile = new File(keyPath);

    if (!certFile.exists() || !keyFile.exists()) {
      LogManager.instance().log(this, Level.WARNING, "TLS certificate or key file not found. Falling back to insecure.");
      return NettyServerBuilder.forPort(port);
    }

    try {
      // Configure Netty with TLS using SslContext
      return NettyServerBuilder.forPort(port)
          .sslContext(GrpcSslContexts
              .forServer(certFile, keyFile)
              .build());
    } catch (Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Failed to configure TLS", e);
      return NettyServerBuilder.forPort(port);
    }
  }

  private ServerCredentials configureTlsCredentials(ContextConfiguration config) {
    String certPath = getConfigString(config, CONFIG_TLS_CERT, null);
    String keyPath = getConfigString(config, CONFIG_TLS_KEY, null);

    if (certPath == null || keyPath == null) {
      LogManager.instance()
          .log(this, Level.WARNING, "TLS enabled but certificate or key path not provided. Using insecure credentials.");
      return InsecureServerCredentials.create();
    }

    File certFile = new File(certPath);
    File keyFile = new File(keyPath);

    if (!certFile.exists() || !keyFile.exists()) {
      LogManager.instance().log(this, Level.WARNING, "TLS certificate or key file not found. Using insecure credentials.");
      return InsecureServerCredentials.create();
    }

    try {
      return TlsServerCredentials.create(certFile, keyFile);
    } catch (Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Failed to configure TLS credentials", e);
      return InsecureServerCredentials.create();
    }
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
