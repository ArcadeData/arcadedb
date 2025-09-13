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
package com.arcadedb.remote.grpc;

import com.arcadedb.server.grpc.ArcadeDbAdminServiceGrpc;
import com.arcadedb.server.grpc.ArcadeDbServiceGrpc;
import com.arcadedb.server.grpc.CreateDatabaseRequest;
import com.arcadedb.server.grpc.DatabaseCredentials;
import com.arcadedb.server.grpc.DropDatabaseRequest;
import com.arcadedb.server.grpc.ListDatabasesRequest;
import com.arcadedb.server.grpc.ListDatabasesResponse;
import io.grpc.CallCredentials;
import io.grpc.Channel;
import io.grpc.ClientInterceptor;
import io.grpc.ClientInterceptors;
import io.grpc.CompressorRegistry;
import io.grpc.DecompressorRegistry;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.StatusException;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.channel.EventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.nio.NioEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.socket.nio.NioSocketChannel;
import io.grpc.stub.AbstractStub;

import javax.annotation.PreDestroy;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * Minimal server-scope gRPC wrapper (HTTP RemoteServer equivalent), implemented
 * ONLY with RPCs present in your current proto.
 * <p>
 * Features: - listDatabases() - existsDatabase(name) via listDatabases() -
 * createDatabase(name, type) - createDatabaseIfMissing(name, type) -
 * dropDatabase(name, force?) // 'force' only if defined in your proto;
 * otherwise ignored
 * <p>
 * Add more methods later when you extend the proto (ping, serverInfo, user
 * mgmt, etc.).
 */
public class RemoteGrpcServer implements AutoCloseable {

  private final String host;
  private final int    port;
  private final String userName;
  private final String userPassword;

  private final long defaultTimeoutMs;

  private final List<ClientInterceptor> interceptors;
  private final boolean                 plaintext;

  private ManagedChannel channel;

  private ArcadeDbAdminServiceGrpc.ArcadeDbAdminServiceBlockingV2Stub adminServiceBlockingV2Stub;

  public RemoteGrpcServer(final String host, final int port, final String user, final String pass, boolean plaintext,
      List<ClientInterceptor> interceptors) {
    this(host, port, user, pass, plaintext, interceptors, 30_000);
  }

  public RemoteGrpcServer(final String host, final int port, final String user, final String pass, boolean plaintext,
      List<ClientInterceptor> interceptors, final long defaultTimeoutMs) {

    this.host = Objects.requireNonNull(host, "host");

    this.port = port;

    this.plaintext = plaintext;
    this.interceptors = interceptors == null ? List.of() : List.copyOf(interceptors);

    this.userName = Objects.requireNonNull(user, "user");

    this.userPassword = Objects.requireNonNull(pass, "pass");

    this.defaultTimeoutMs = defaultTimeoutMs > 0 ? defaultTimeoutMs : 30_000;
  }

  public synchronized void start() {

    if (channel != null)
      return;

//		NettyChannelBuilder b = NettyChannelBuilder.forAddress(host, port)
//				.maxInboundMessageSize(100 * 1024 * 1024) // 100MB max message size
//				.keepAliveTime(30, TimeUnit.SECONDS) // Keep-alive configuration
//				.keepAliveTimeout(10, TimeUnit.SECONDS)
//				.keepAliveWithoutCalls(true)
//				.decompressorRegistry(DecompressorRegistry.getDefaultInstance())
//				.compressorRegistry(CompressorRegistry.getDefaultInstance());

    EventLoopGroup elg = new NioEventLoopGroup();

    NettyChannelBuilder chBuilder = NettyChannelBuilder.forAddress(host, port)
        .eventLoopGroup(elg)                           // share across clients
        .channelType(NioSocketChannel.class)
        .maxInboundMessageSize(150 * 1024 * 1024)
        .maxInboundMetadataSize(32 * 1024 * 1024)
        .keepAliveTime(30, TimeUnit.SECONDS)
        .keepAliveTimeout(10, TimeUnit.SECONDS)
        .keepAliveWithoutCalls(true)
        .decompressorRegistry(DecompressorRegistry.getDefaultInstance())
        .compressorRegistry(CompressorRegistry.getDefaultInstance())
        .flowControlWindow(8 * 1024 * 1024);

    // .proxyDetector(myProxyDetector)

    if (plaintext)
      chBuilder.usePlaintext();

    channel = chBuilder.build();
  }

  /**
   * Returns a Channel (wrapped with interceptors if provided).
   */
  public Channel channel() {

    if (channel == null) {
      start();
    }

    return interceptors.isEmpty() ? channel : ClientInterceptors.intercept(channel, interceptors);
  }

  public ArcadeDbServiceGrpc.ArcadeDbServiceBlockingV2Stub newBlockingStub(int timeout) {

    return ArcadeDbServiceGrpc.newBlockingV2Stub(channel())
        .withCallCredentials(createCredentials())
        .withDeadlineAfter(timeout, TimeUnit.MILLISECONDS)
        .withCompression("gzip");
  }

  public ArcadeDbServiceGrpc.ArcadeDbServiceStub newAsyncStub(int timeout) {

    return ArcadeDbServiceGrpc.newStub(channel())
        .withCallCredentials(createCredentials())
        .withDeadlineAfter(timeout, TimeUnit.MILLISECONDS)
        .withCompression("gzip");
  }

  private ArcadeDbAdminServiceGrpc.ArcadeDbAdminServiceBlockingV2Stub adminServiceBlockingV2Stub() {

    if (this.adminServiceBlockingV2Stub == null) {

      this.adminServiceBlockingV2Stub = ArcadeDbAdminServiceGrpc.newBlockingV2Stub(channel())
          .withCallCredentials(createCallCredentials(userName, userPassword));
    }

    return this.adminServiceBlockingV2Stub;
  }

  @PreDestroy
  @Override
  public void close() {
    if (channel == null)
      return;
    try {
      channel.shutdown();
      if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
        channel.shutdownNow();
        channel.awaitTermination(2, TimeUnit.SECONDS);
      }
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      channel.shutdownNow();
    } finally {
      // log.info("gRPC channel to {}:{} closed.", host, port);
    }
  }

  private <S extends AbstractStub<S>> S withDeadline(S stub, long timeoutMs) {
    long t = (timeoutMs > 0) ? timeoutMs : defaultTimeoutMs;
    return stub.withDeadlineAfter(t, TimeUnit.MILLISECONDS);
  }

  public DatabaseCredentials buildCredentials() {
    return DatabaseCredentials.newBuilder().setUsername(userName == null ? "" : userName)
        .setPassword(userPassword == null ? "" : userPassword).build();
  }

  /**
   * Returns the list of database names from the server.
   */
  public List<String> listDatabases() {

    ListDatabasesResponse resp = null;

    try {

      resp = withDeadline(adminServiceBlockingV2Stub(), defaultTimeoutMs)
          .listDatabases(ListDatabasesRequest.newBuilder().setCredentials(buildCredentials()).build());
      return resp.getDatabasesList();
    } catch (StatusException e) {

      throw new RuntimeException("Failed to list databases: " + e.getMessage(), e);
    }
  }

  /**
   * Checks existence by listing (no ExistsDatabaseRequest needed).
   */
  public boolean existsDatabase(final String database) {
    return listDatabases().stream().anyMatch(n -> n.equalsIgnoreCase(database));
  }

  /**
   * Creates a database with type "graph" or "document".
   */
  public void createDatabase(final String database) {

    try {
      withDeadline(adminServiceBlockingV2Stub(), defaultTimeoutMs)
          .createDatabase(CreateDatabaseRequest.newBuilder().setName(database).setCredentials(buildCredentials()).build());
    } catch (StatusException e) {
      throw new RuntimeException("Failed to create database: " + e.getMessage(), e);
    }
  }

  /**
   * No-op if already present; creates otherwise.
   */
  public void createDatabaseIfMissing(final String database) {
    if (!existsDatabase(database)) {
      createDatabase(database);
    }
  }

  /**
   * Drops a database. If your proto supports 'force', add it here.
   */
  public void dropDatabase(final String database) {

    try {
      withDeadline(adminServiceBlockingV2Stub(), defaultTimeoutMs)
          .dropDatabase(DropDatabaseRequest.newBuilder().setName(database).setCredentials(buildCredentials()).build());
    } catch (StatusException e) {
      throw new RuntimeException("Failed to drop database: " + e.getMessage(), e);
    }
  }

  public String endpoint() {
    return host + ":" + port;
  }

  @Override
  public String toString() {
    return "RemoteGrpcServer{endpoint=" + endpoint() + ", userName='" + userName + "'}";
  }

  /**
   * Creates call credentials for authentication
   */
  protected CallCredentials createCallCredentials(String userName, String userPassword) {
    return new CallCredentials() {
      @Override
      public void applyRequestMetadata(RequestInfo requestInfo, Executor appExecutor, MetadataApplier applier) {
        Metadata headers = new Metadata();
        headers.put(Metadata.Key.of("username", Metadata.ASCII_STRING_MARSHALLER), userName);
        headers.put(Metadata.Key.of("password", Metadata.ASCII_STRING_MARSHALLER), userPassword);
        headers.put(Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER), userName);
        headers.put(Metadata.Key.of("x-arcade-password", Metadata.ASCII_STRING_MARSHALLER), userPassword);
        applier.apply(headers);
      }

      @Override
      public void thisUsesUnstableApi() {
        // Required by the interface
      }
    };
  }

  protected CallCredentials createCredentials() {
    return new CallCredentials() {
      @Override
      public void applyRequestMetadata(RequestInfo requestInfo, Executor appExecutor, MetadataApplier applier) {
        Metadata headers = new Metadata();
        headers.put(Metadata.Key.of("username", Metadata.ASCII_STRING_MARSHALLER), userName);
        headers.put(Metadata.Key.of("password", Metadata.ASCII_STRING_MARSHALLER), userPassword);
        headers.put(Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER), userName);
        headers.put(Metadata.Key.of("x-arcade-password", Metadata.ASCII_STRING_MARSHALLER), userPassword);
        applier.apply(headers);
      }

      // x-arcade-user: root" -H "x-arcade-password: oY9uU2uJ8nD8iY7t" -H
      // "x-arcade-database: local_shakeiq_curonix_poc-app"
      @Override
      public void thisUsesUnstableApi() {
        // Required by the interface
      }
    };
  }
}
