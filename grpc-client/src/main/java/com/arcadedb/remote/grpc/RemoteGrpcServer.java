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

import com.arcadedb.log.LogManager;
import com.arcadedb.remote.RemoteException;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.grpc.AlignDatabaseRequest;
import com.arcadedb.server.grpc.ApiTokenInfo;
import com.arcadedb.server.grpc.ArcadeDbAdminServiceGrpc;
import com.arcadedb.server.grpc.ArcadeDbServiceGrpc;
import com.arcadedb.server.grpc.BackupInfo;
import com.arcadedb.server.grpc.CloseDatabaseRequest;
import com.arcadedb.server.grpc.ConnectClusterRequest;
import com.arcadedb.server.grpc.CreateApiTokenRequest;
import com.arcadedb.server.grpc.CreateApiTokenResponse;
import com.arcadedb.server.grpc.CreateDatabaseRequest;
import com.arcadedb.server.grpc.CreateUserRequest;
import com.arcadedb.server.grpc.DatabaseCredentials;
import com.arcadedb.server.grpc.DeleteApiTokenRequest;
import com.arcadedb.server.grpc.DeleteBackupRequest;
import com.arcadedb.server.grpc.DeleteGroupRequest;
import com.arcadedb.server.grpc.DeleteUserRequest;
import com.arcadedb.server.grpc.DisconnectClusterRequest;
import com.arcadedb.server.grpc.DropDatabaseRequest;
import com.arcadedb.server.grpc.GetBackupConfigRequest;
import com.arcadedb.server.grpc.GetBackupConfigResponse;
import com.arcadedb.server.grpc.GetProgressRequest;
import com.arcadedb.server.grpc.GetServerEventsRequest;
import com.arcadedb.server.grpc.GetServerEventsResponse;
import com.arcadedb.server.grpc.HealthRequest;
import com.arcadedb.server.grpc.ImportDatabaseRequest;
import com.arcadedb.server.grpc.ImportProgress;
import com.arcadedb.server.grpc.ListApiTokensRequest;
import com.arcadedb.server.grpc.ListBackupsRequest;
import com.arcadedb.server.grpc.ListDatabasesRequest;
import com.arcadedb.server.grpc.ListDatabasesResponse;
import com.arcadedb.server.grpc.ListGroupsRequest;
import com.arcadedb.server.grpc.ListSessionsRequest;
import com.arcadedb.server.grpc.ListUsersRequest;
import com.arcadedb.server.grpc.OpenDatabaseRequest;
import com.arcadedb.server.grpc.OperationProgressInfo;
import com.arcadedb.server.grpc.ProfilerDocumentResponse;
import com.arcadedb.server.grpc.ProfilerListRequest;
import com.arcadedb.server.grpc.ProfilerLoadRequest;
import com.arcadedb.server.grpc.ProfilerResetRequest;
import com.arcadedb.server.grpc.ProfilerResultsRequest;
import com.arcadedb.server.grpc.ProfilerRunInfo;
import com.arcadedb.server.grpc.ProfilerStartRequest;
import com.arcadedb.server.grpc.ProfilerStopRequest;
import com.arcadedb.server.grpc.ReadyRequest;
import com.arcadedb.server.grpc.ReadyResponse;
import com.arcadedb.server.grpc.RestoreBackupRequest;
import com.arcadedb.server.grpc.RestoreDatabaseRequest;
import com.arcadedb.server.grpc.RestoreProgress;
import com.arcadedb.server.grpc.SaveGroupRequest;
import com.arcadedb.server.grpc.SessionInfo;
import com.arcadedb.server.grpc.SetBackupConfigRequest;
import com.arcadedb.server.grpc.SetDatabaseSettingRequest;
import com.arcadedb.server.grpc.SetServerSettingRequest;
import com.arcadedb.server.grpc.ShutdownRequest;
import com.arcadedb.server.grpc.TriggerBackupRequest;
import com.arcadedb.server.grpc.UpdateUserRequest;
import com.arcadedb.server.grpc.UserDatabases;
import com.arcadedb.server.grpc.UserGroups;
import com.arcadedb.server.grpc.UserInfo;
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
import io.grpc.stub.BlockingClientCall;

import javax.annotation.PreDestroy;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.logging.Level;

/**
 * Server-scope gRPC client: the {@code RemoteServer} of this transport, one method per RPC of
 * {@code ArcadeDbAdminService}.
 * <p>
 * It covers the discovery and database-lifecycle RPCs, and, since issue #7304, the rest of the
 * control plane: settings, backup, users, the query profiler, server events, shutdown, cluster
 * disconnect, and the two container probes. Issue #7310 added the last two discovery reads,
 * {@link #getProgress(String)} and {@link #listSessions()}; #7309 added groups and API tokens;
 * and #7308 added restore and import, the only methods here that stream: they block until the
 * operation finishes, handing each progress message to a callback on the way.
 * <p>
 * Every method carries the credentials this instance was built with in the request body, except
 * {@link #health()} and {@link #ready()}: their requests have no credentials field, and the server
 * exempts those two methods from the admin authentication gate, so a probe works whatever this
 * instance was constructed with. (The shared stub still sends this instance's call credentials as
 * headers on every call, probes included; the server does not read them for the admin service.)
 */
public class RemoteGrpcServer implements AutoCloseable {

  // Hoisted out of the credential-attach path: Metadata.Key.of() validates and lower-cases the name on every
  // call, and these four (five, with a database) are attached to EVERY RPC.
  private static final Metadata.Key<String> USERNAME_KEY         =
      Metadata.Key.of("username", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> PASSWORD_KEY         =
      Metadata.Key.of("password", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> ARCADE_USER_KEY      =
      Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> ARCADE_PASSWORD_KEY  =
      Metadata.Key.of("x-arcade-password", Metadata.ASCII_STRING_MARSHALLER);
  /**
   * The key {@code GrpcAuthInterceptor} reads to decide which database to authenticate the call against.
   */
  private static final Metadata.Key<String> ARCADE_DATABASE_KEY  =
      Metadata.Key.of("x-arcade-database", Metadata.ASCII_STRING_MARSHALLER);

  private final String host;
  private final int    port;
  private final String userName;
  private final String userPassword;

  private final long defaultTimeoutMs;

  private final List<ClientInterceptor> interceptors;
  private final boolean                 plaintext;
  private final boolean                 allowInsecureCredentials;
  // Whole decision is fixed at construction (host/plaintext/opt-in are all final): true means credentials would
  // travel in cleartext to a non-loopback host and must be refused. Resolving it once keeps the credential-attach
  // path off any DNS lookup.
  private final boolean                 refuseCredentialsOverChannel;

  // Volatile: mutated only under this object's monitor by start()/close(), but channel() and the stub factories
  // read it WITHOUT the monitor, so without volatile a reader racing the first start() or a close() has no
  // happens-before edge and may observe a stale (or half-published) value (issue #6762).
  private volatile ManagedChannel channel;
  /**
   * Bumped every time {@link #start()} builds a channel and every time {@link #close()} shuts one down. A gRPC
   * stub binds the channel it was built on, so a
   * {@link RemoteGrpcDatabase} that cached its stubs compares this against the generation it built them under
   * and rebuilds them when a {@link #close()} / {@link #start()} cycle has replaced the channel (issue #7416).
   */
  private volatile long           channelGeneration;
  private volatile EventLoopGroup eventLoopGroup;
  /**
   * Set the moment {@link #close()} begins and cleared by the next explicit {@link #start()}. Without it,
   * {@code channel()} could still hand out the channel that {@code close()} had already begun shutting down, and
   * worse: once {@code close()} nulled the field, {@code channel()} would silently BUILD A NEW CHANNEL for a
   * server the caller has explicitly closed. An explicit restart is still allowed - that is what clears it - a
   * lazy resurrection from a stale reference is not (PR #6783 review).
   */
  private volatile boolean       closing;

  private ArcadeDbAdminServiceGrpc.ArcadeDbAdminServiceBlockingV2Stub adminServiceBlockingV2Stub;

  public RemoteGrpcServer(final String host, final int port, final String user, final String pass, boolean plaintext,
      List<ClientInterceptor> interceptors) {
    this(host, port, user, pass, plaintext, interceptors, 30_000);
  }

  public RemoteGrpcServer(final String host, final int port, final String user, final String pass, boolean plaintext,
      List<ClientInterceptor> interceptors, final long defaultTimeoutMs) {
    this(host, port, user, pass, plaintext, interceptors, defaultTimeoutMs, false);
  }

  public RemoteGrpcServer(final String host, final int port, final String user, final String pass, boolean plaintext,
      List<ClientInterceptor> interceptors, final long defaultTimeoutMs, final boolean allowInsecureCredentials) {

    this.host = Objects.requireNonNull(host, "host");

    this.port = port;

    this.plaintext = plaintext;
    this.allowInsecureCredentials = allowInsecureCredentials;
    this.refuseCredentialsOverChannel = plaintext && !allowInsecureCredentials && !isLoopbackHost(this.host);
    this.interceptors = interceptors == null ? List.of() : List.copyOf(interceptors);

    this.userName = Objects.requireNonNull(user, "user");

    this.userPassword = Objects.requireNonNull(pass, "pass");

    this.defaultTimeoutMs = defaultTimeoutMs > 0 ? defaultTimeoutMs : 30_000;
  }

  public synchronized void start() {
    closing = false;

    if (channel != null)
      return;

//		NettyChannelBuilder b = NettyChannelBuilder.forAddress(host, port)
//				.maxInboundMessageSize(100 * 1024 * 1024) // 100MB max message size
//				.keepAliveTime(30, TimeUnit.SECONDS) // Keep-alive configuration
//				.keepAliveTimeout(10, TimeUnit.SECONDS)
//				.keepAliveWithoutCalls(true)
//				.decompressorRegistry(DecompressorRegistry.getDefaultInstance())
//				.compressorRegistry(CompressorRegistry.getDefaultInstance());

    eventLoopGroup = new NioEventLoopGroup();

    NettyChannelBuilder chBuilder = NettyChannelBuilder.forAddress(host, port)
        .eventLoopGroup(eventLoopGroup)                // share across clients
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
    channelGeneration++;
  }

  /**
   * Identifies the channel {@link #channel()} currently hands out: the value changes whenever {@link #start()}
   * builds a new one and whenever {@link #close()} shuts one down. Cheaper than comparing channels, which
   * {@link #channel()} may wrap in interceptors on every call.
   */
  public long channelGeneration() {
    return channelGeneration;
  }

  /**
   * Returns a Channel (wrapped with interceptors if provided).
   * <p>
   * Reads the field ONCE per decision: re-reading it after the null check let a concurrent {@link #close()} turn the
   * return value into {@code null} between the two reads, surfacing as an NPE inside gRPC rather than as a usable
   * error (issue #6762).
   */
  public Channel channel() {
    if (closing)
      throw new IllegalStateException("The gRPC channel to '" + host + "' is closed");
    ManagedChannel current = channel;
    if (current == null) {
      start();
      current = channel;
      if (current == null)
        throw new IllegalStateException("The gRPC channel to '" + host + "' is not available: the connection is closed");
    }

    return interceptors.isEmpty() ? current : ClientInterceptors.intercept(current, interceptors);
  }

  /**
   * A data-plane stub that names no database. The server then authenticates the credentials at server
   * level; per-database authorization still happens against the database named in each request body.
   * Prefer {@link #newBlockingStub(int, String)} when the target database is known.
   */
  public ArcadeDbServiceGrpc.ArcadeDbServiceBlockingV2Stub newBlockingStub(final int timeout) {
    return newBlockingStub(timeout, null);
  }

  /**
   * A data-plane stub whose every call carries {@code database} on the {@code x-arcade-database}
   * metadata the server's auth interceptor reads.
   * <p>
   * Issue #7320: without that key the interceptor fell back to the literal name {@code "default"} and
   * authenticated the caller against a database it had never named, so any principal not granted
   * {@code "*"} was refused on its first RPC.
   *
   * @param database the database the calls target, or {@code null}/blank to send no database at all
   */
  public ArcadeDbServiceGrpc.ArcadeDbServiceBlockingV2Stub newBlockingStub(final int timeout, final String database) {

    return newBlockingStub(timeout, database, null, null);
  }

  /**
   * A data-plane stub for a principal that is not necessarily this server's own.
   * <p>
   * Issue #7374: {@link RemoteGrpcDatabase} takes a user of its own and puts it in every request body, but
   * built its stubs through the database-only overload above - so the call metadata carried THIS server's
   * account, the server authenticated that one, and the user the caller passed to the database was
   * discarded on gRPC while the HTTP half of the same object still used it. A database opened by a scoped
   * user on a channel built for root is a supported combination, so the stub has to be able to name which.
   *
   * @param userName     the principal the calls authenticate as, or {@code null}/blank to use this
   *                     server's own account
   * @param userPassword that principal's password
   */
  public ArcadeDbServiceGrpc.ArcadeDbServiceBlockingV2Stub newBlockingStub(final int timeout, final String database,
      final String userName, final String userPassword) {

    return ArcadeDbServiceGrpc.newBlockingV2Stub(channel())
        .withCallCredentials(createCredentials(database, userName, userPassword))
        .withDeadlineAfter(timeout, TimeUnit.MILLISECONDS)
        .withCompression("gzip");
  }

  /**
   * @see #newBlockingStub(int)
   */
  public ArcadeDbServiceGrpc.ArcadeDbServiceStub newAsyncStub(final int timeout) {
    return newAsyncStub(timeout, null);
  }

  /**
   * @see #newBlockingStub(int, String)
   */
  public ArcadeDbServiceGrpc.ArcadeDbServiceStub newAsyncStub(final int timeout, final String database) {

    return newAsyncStub(timeout, database, null, null);
  }

  /**
   * @see #newBlockingStub(int, String, String, String)
   */
  public ArcadeDbServiceGrpc.ArcadeDbServiceStub newAsyncStub(final int timeout, final String database,
      final String userName, final String userPassword) {

    return ArcadeDbServiceGrpc.newStub(channel())
        .withCallCredentials(createCredentials(database, userName, userPassword))
        .withDeadlineAfter(timeout, TimeUnit.MILLISECONDS)
        .withCompression("gzip");
  }

  /**
   * A fresh admin stub on this instance's channel, under {@code timeout} milliseconds of deadline.
   * <p>
   * Exists for {@link RemoteGrpcDatabase}, which shares this server's channel but not necessarily its
   * account - the two are constructed with separate credentials and a scoped database user against a root
   * server is a supported combination - so it must put its OWN credentials in the request body rather than
   * borrow {@link #getProgress(String)}'s (issue #7310). The cached {@link #adminServiceBlockingV2Stub()}
   * is this instance's own and is not shared for that reason.
   */
  public ArcadeDbAdminServiceGrpc.ArcadeDbAdminServiceBlockingV2Stub newAdminBlockingStub(final int timeout) {
    return newAdminBlockingStub(timeout, null, null);
  }

  /**
   * An admin stub for a principal that is not necessarily this server's own - the metadata half of what
   * {@link #newAdminBlockingStub(int)}'s caller already does with the request body (issue #7374).
   *
   * @see #newBlockingStub(int, String, String, String)
   */
  public ArcadeDbAdminServiceGrpc.ArcadeDbAdminServiceBlockingV2Stub newAdminBlockingStub(final int timeout,
      final String userName, final String userPassword) {
    return ArcadeDbAdminServiceGrpc.newBlockingV2Stub(channel())
        .withCallCredentials(createCredentials(null, userName, userPassword))
        .withDeadlineAfter(timeout, TimeUnit.MILLISECONDS);
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
  public synchronized void close() {
    if (channel == null)
      return;
    // Set BEFORE the shutdown begins, so channel() refuses from this point rather than handing out a channel that
    // is already terminating (PR #6783 review).
    closing = true;
    try {
      channel.shutdown();
      if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
        channel.shutdownNow();
        channel.awaitTermination(2, TimeUnit.SECONDS);
      }
    } catch (final InterruptedException ie) {
      Thread.currentThread().interrupt();
      channel.shutdownNow();
    } finally {
      channel = null;
      // Bumped on close as well as on start, so a stub cached against the channel just shut down is rebuilt on
      // its next use - and, while the server stays closed, that rebuild is refused by channel() with the reason
      // instead of the dead channel's "Channel shutdown invoked" (issue #7416).
      channelGeneration++;
      adminServiceBlockingV2Stub = null;
      if (eventLoopGroup != null) {
        eventLoopGroup.shutdownGracefully(0, 2, TimeUnit.SECONDS);
        eventLoopGroup = null;
      }
    }
  }

  private <S extends AbstractStub<S>> S withDeadline(S stub, long timeoutMs) {
    long t = timeoutMs > 0 ? timeoutMs : defaultTimeoutMs;
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
   * Creates a database. A name already taken is refused with {@code ALREADY_EXISTS}, the same answer the HTTP
   * {@code create database} command gives (issue #7413); use {@link #createDatabaseIfMissing(String)} for the
   * idempotent form.
   */
  public void createDatabase(final String database) {
    createDatabase(database, false);
  }

  /**
   * Creates the database unless one of that name is already there, and says which happened. The decision is
   * the server's, in one call: the previous exists-then-create pair left a window in which another client's
   * create made the second half fail.
   *
   * @return {@code true} when this call created it, {@code false} when it already existed
   */
  public boolean createDatabaseIfMissing(final String database) {
    return createDatabase(database, true);
  }

  private boolean createDatabase(final String database, final boolean ifNotExists) {
    try {
      return withDeadline(adminServiceBlockingV2Stub(), defaultTimeoutMs)
          .createDatabase(CreateDatabaseRequest.newBuilder().setName(database).setCredentials(buildCredentials())
              .setIfNotExists(ifNotExists).build())
          .getCreated();
    } catch (StatusException e) {
      throw new RuntimeException("Failed to create database: " + e.getMessage(), e);
    }
  }

  /**
   * Drops a database. A name that does not exist is refused with {@code NOT_FOUND}, the same answer the HTTP
   * {@code drop database} command gives (issue #7413); use {@link #dropDatabaseIfExists(String)} for the
   * idempotent form.
   */
  public void dropDatabase(final String database) {
    dropDatabase(database, false);
  }

  /**
   * Drops the database if there is one of that name, and says which happened.
   *
   * @return {@code true} when this call dropped it, {@code false} when there was nothing to drop
   */
  public boolean dropDatabaseIfExists(final String database) {
    return dropDatabase(database, true);
  }

  private boolean dropDatabase(final String database, final boolean ifExists) {
    try {
      return withDeadline(adminServiceBlockingV2Stub(), defaultTimeoutMs)
          .dropDatabase(DropDatabaseRequest.newBuilder().setName(database).setCredentials(buildCredentials())
              .setIfExists(ifExists).build())
          .getDropped();
    } catch (StatusException e) {
      throw new RuntimeException("Failed to drop database: " + e.getMessage(), e);
    }
  }

  // -------------------------------------------------------------------------------------------
  // Control plane (issue #7304). Each method is one RPC of ArcadeDbAdminService; every one of them
  // reaches the same com.arcadedb.server.ServerControlPlane the HTTP control plane reaches, so the
  // behaviour matches RemoteServer's over HTTP.
  // -------------------------------------------------------------------------------------------

  /**
   * Opens a database on the server, loading it if it was closed.
   */
  public void openDatabase(final String database) {
    call("open database", stub -> stub.openDatabase(
        OpenDatabaseRequest.newBuilder().setCredentials(buildCredentials()).setName(database).build()));
  }

  /**
   * Closes a database on the server and removes it from the server's cache. The files stay on disk.
   */
  public void closeDatabase(final String database) {
    call("close database", stub -> stub.closeDatabase(
        CloseDatabaseRequest.newBuilder().setCredentials(buildCredentials()).setName(database).build()));
  }

  public void alignDatabase(final String database) {
    call("align database", stub -> stub.alignDatabase(
        AlignDatabaseRequest.newBuilder().setCredentials(buildCredentials()).setName(database).build()));
  }

  public void setServerSetting(final String key, final String value) {
    call("set server setting", stub -> stub.setServerSetting(
        SetServerSettingRequest.newBuilder().setCredentials(buildCredentials()).setKey(key).setValue(value).build()));
  }

  public void setDatabaseSetting(final String database, final String key, final String value) {
    call("set database setting", stub -> stub.setDatabaseSetting(
        SetDatabaseSettingRequest.newBuilder().setCredentials(buildCredentials()).setDatabase(database).setKey(key)
            .setValue(value).build()));
  }

  /**
   * Creates a server user with no grants beyond the server default.
   */
  public void createUser(final String user, final String password) {
    createUser(user, password, Map.of());
  }

  /**
   * Creates a server user holding {@code databases} - a database name (or {@code "*"}) to the list of
   * groups the user holds on it, the same shape the HTTP {@code create user} document carries.
   */
  public void createUser(final String user, final String password, final Map<String, List<String>> databases) {
    final CreateUserRequest.Builder request = CreateUserRequest.newBuilder().setCredentials(buildCredentials())
        .setUser(user).setPassword(password);
    databases.forEach((database, groups) -> request.putDatabases(database,
        UserGroups.newBuilder().addAllGroups(groups).build()));

    call("create user", stub -> stub.createUser(request.build()));
  }

  public void dropUser(final String user) {
    call("drop user", stub -> stub.deleteUser(
        DeleteUserRequest.newBuilder().setCredentials(buildCredentials()).setUser(user).build()));
  }

  /**
   * The server's users, as {@code name -> (database -> groups)}. Password hashes are never returned.
   */
  public List<UserInfo> listUsers() {
    return call("list users", stub -> stub.listUsers(
        ListUsersRequest.newBuilder().setCredentials(buildCredentials()).build())).getUsersList();
  }

  /**
   * Updates an existing user. Both arguments are independently optional: a null leaves that part of
   * the user alone, so changing a password does not clear the user's grants and vice versa. Passing
   * an empty (non-null) map DOES clear them - that is the caller saying so.
   */
  public void updateUser(final String user, final String password, final Map<String, List<String>> databases) {
    final UpdateUserRequest.Builder request = UpdateUserRequest.newBuilder().setCredentials(buildCredentials())
        .setUser(user);
    if (password != null)
      request.setPassword(password);
    if (databases != null) {
      final UserDatabases.Builder grants = UserDatabases.newBuilder();
      databases.forEach((database, groups) -> grants.putDatabases(database,
          UserGroups.newBuilder().addAllGroups(groups).build()));
      request.setDatabases(grants.build());
    }

    call("update user", stub -> stub.updateUser(request.build()));
  }

  /**
   * Changes only a user's password, leaving its per-database grants as they are.
   */
  public void updateUserPassword(final String user, final String password) {
    updateUser(user, password, null);
  }

  /**
   * Replaces only a user's per-database grants, leaving its password as it is.
   */
  public void updateUserGrants(final String user, final Map<String, List<String>> databases) {
    updateUser(user, null, Objects.requireNonNull(databases, "databases"));
  }

  /**
   * The whole group/permission document, as {@code GET /server/groups} returns it.
   */
  public JSONObject listGroups() {
    return new JSONObject(call("list groups", stub -> stub.listGroups(
        ListGroupsRequest.newBuilder().setCredentials(buildCredentials()).build())).getGroupsJson());
  }

  /**
   * Creates or replaces one group on {@code database} ({@code "*"} for every database), and refreshes
   * the permissions of the open databases it applies to. Replaces: the definition becomes exactly
   * {@code groupConfig}, it is not merged into an existing group of the same name.
   */
  public void saveGroup(final String database, final String name, final JSONObject groupConfig) {
    call("save group", stub -> stub.saveGroup(SaveGroupRequest.newBuilder().setCredentials(buildCredentials())
        .setDatabase(database).setName(name).setGroupJson(groupConfig.toString()).build()));
  }

  public void deleteGroup(final String database, final String name) {
    call("delete group", stub -> stub.deleteGroup(DeleteGroupRequest.newBuilder().setCredentials(buildCredentials())
        .setDatabase(database).setName(name).build()));
  }

  /**
   * The issued API tokens: metadata plus each token's hash, which is the handle
   * {@link #deleteApiToken(String)} takes. Never the token material - the server does not keep it.
   */
  public List<ApiTokenInfo> listApiTokens() {
    return call("list api tokens", stub -> stub.listApiTokens(
        ListApiTokensRequest.newBuilder().setCredentials(buildCredentials()).build())).getTokensList();
  }

  /**
   * Mints an API token. <b>The returned {@code token} field is the only copy of the token that will
   * ever exist</b>: the server keeps its SHA-256 and cannot produce the plaintext again.
   * <p>
   * Two refusals guard it, and they are independent. Client-side, this call cannot even be attempted
   * over a plaintext channel to a non-loopback host, because every admin RPC attaches call credentials
   * and {@code createCallCredentials} refuses that combination. Server-side, the mint is refused with
   * {@code FAILED_PRECONDITION} unless the connection is TLS or loopback - which is the one that also
   * holds for a caller that opted out with {@code allowInsecureCredentials}, or that is not this
   * client at all.
   *
   * @param expiresAt   epoch millis at which the token stops working; 0 for a token that does not expire
   * @param permissions the permission document, or null for none
   */
  public CreateApiTokenResponse createApiToken(final String name, final String database, final long expiresAt,
      final JSONObject permissions) {
    final CreateApiTokenRequest.Builder request = CreateApiTokenRequest.newBuilder()
        .setCredentials(buildCredentials()).setName(name).setDatabase(database == null ? "" : database)
        .setExpiresAt(expiresAt);
    if (permissions != null)
      request.setPermissionsJson(permissions.toString());

    return call("create api token", stub -> stub.createApiToken(request.build()));
  }

  /**
   * Revokes a token by its hash - the {@code tokenHash} of a {@link #listApiTokens()} entry, or of a
   * {@link #createApiToken} response's {@code info}. The plaintext token is deliberately not accepted
   * by the server.
   */
  public void deleteApiToken(final String tokenHash) {
    call("delete api token", stub -> stub.deleteApiToken(DeleteApiTokenRequest.newBuilder()
        .setCredentials(buildCredentials()).setTokenHash(tokenHash).build()));
  }

  public GetBackupConfigResponse getBackupConfig() {
    return call("get backup config", stub -> stub.getBackupConfig(
        GetBackupConfigRequest.newBuilder().setCredentials(buildCredentials()).build()));
  }

  public void setBackupConfig(final JSONObject config) {
    call("set backup config", stub -> stub.setBackupConfig(
        SetBackupConfigRequest.newBuilder().setCredentials(buildCredentials()).setConfigJson(config.toString()).build()));
  }

  public List<BackupInfo> listBackups(final String database) {
    return call("list backups", stub -> stub.listBackups(
        ListBackupsRequest.newBuilder().setCredentials(buildCredentials()).setDatabase(database).build())).getBackupsList();
  }

  /**
   * Runs a full backup inline and returns the archive path.
   */
  public String triggerBackup(final String database) {
    return call("trigger backup", stub -> stub.triggerBackup(
        TriggerBackupRequest.newBuilder().setCredentials(buildCredentials()).setDatabase(database).build())).getBackupFile();
  }

  public void deleteBackup(final String database, final String fileName) {
    call("delete backup", stub -> stub.deleteBackup(
        DeleteBackupRequest.newBuilder().setCredentials(buildCredentials()).setDatabase(database).setFileName(fileName)
            .build()));
  }

  /**
   * Starts the query profiler. {@code timeoutSeconds} of 0 records until {@link #profilerStop()}.
   */
  public void profilerStart(final int timeoutSeconds) {
    call("profiler start", stub -> stub.profilerStart(
        ProfilerStartRequest.newBuilder().setCredentials(buildCredentials()).setTimeoutSeconds(timeoutSeconds).build()));
  }

  public JSONObject profilerStop() {
    return profilerDocument(call("profiler stop", stub -> stub.profilerStop(
        ProfilerStopRequest.newBuilder().setCredentials(buildCredentials()).build())));
  }

  public void profilerReset() {
    call("profiler reset", stub -> stub.profilerReset(
        ProfilerResetRequest.newBuilder().setCredentials(buildCredentials()).build()));
  }

  public JSONObject profilerResults() {
    return profilerDocument(call("profiler results", stub -> stub.profilerResults(
        ProfilerResultsRequest.newBuilder().setCredentials(buildCredentials()).build())));
  }

  /**
   * The profiler runs saved on the server, newest first.
   */
  public List<ProfilerRunInfo> profilerList() {
    return call("profiler list", stub -> stub.profilerList(
        ProfilerListRequest.newBuilder().setCredentials(buildCredentials()).build())).getRunsList();
  }

  public JSONObject profilerLoad(final String fileName) {
    return profilerDocument(call("profiler load", stub -> stub.profilerLoad(
        ProfilerLoadRequest.newBuilder().setCredentials(buildCredentials()).setFileName(fileName).build())));
  }

  public GetServerEventsResponse getServerEvents(final String fileName) {
    return call("get server events", stub -> stub.getServerEvents(
        GetServerEventsRequest.newBuilder().setCredentials(buildCredentials()).setFileName(fileName).build()));
  }

  /**
   * Stops the server that answers this call, or - when {@code serverName} is not empty - the named
   * HA peer. The local shutdown is scheduled a second out server-side, so this call returns before
   * the process exits.
   */
  public void shutdown(final String serverName) {
    call("shutdown", stub -> stub.shutdown(
        ShutdownRequest.newBuilder().setCredentials(buildCredentials()).setServerName(serverName).build()));
  }

  public void disconnectCluster() {
    call("disconnect cluster", stub -> stub.disconnectCluster(
        DisconnectClusterRequest.newBuilder().setCredentials(buildCredentials()).build()));
  }

  /**
   * Asks this server to join the cluster reachable at {@code serverAddress} ({@code <host>:<port>}),
   * the client half of the pair {@link #disconnectCluster()} completes (issue #7400).
   * <p>
   * Reaches the same {@code ServerControlPlane.connectCluster} the HTTP {@code connect cluster} verb
   * calls. The current HA stack does not implement a client-initiated join, so today this raises the
   * server's own refusal through {@code GrpcClientErrorMapper} rather than joining anything; issue
   * #7401 carries that decision. The address is sent as given - the server refuses before reading it,
   * exactly as the HTTP verb does.
   */
  public void connectCluster(final String serverAddress) {
    call("connect cluster", stub -> stub.connectCluster(
        ConnectClusterRequest.newBuilder().setCredentials(buildCredentials()).setServerAddress(serverAddress).build()));
  }

  /**
   * The long-running maintenance operations the server is running for {@code database}, oldest first, or
   * an empty list when it is running none. Safe to poll: the server answers from a lock-free in-memory
   * snapshot without touching the database.
   * <p>
   * Authorized per database rather than root-only, as {@code GET /api/v1/progress/{database}} is, so the
   * account that started an operation can watch it. Reports what THIS node is doing: the registry behind
   * it is process-local, so in a cluster each node is polled for its own work.
   * <p>
   * {@link RemoteGrpcDatabase#getProgress()} is the database-scoped form of this call, and returns the
   * same operations as JSON documents.
   */
  public List<OperationProgressInfo> getProgress(final String database) {
    return call("get progress", stub -> stub.getProgress(
        GetProgressRequest.newBuilder().setCredentials(buildCredentials()).setDatabase(database).build()))
        .getOperationsList();
  }

  /**
   * The server's open HTTP authentication sessions - root only, as {@code GET /api/v1/sessions} is.
   * <p>
   * A read of server state, not a session API: gRPC authenticates every call from the credentials on the
   * request body and has no session of its own, so there is no login or logout to pair with this. Empty
   * on a server running without the HTTP listener, which has no HTTP sessions to report.
   */
  public List<SessionInfo> listSessions() {
    return call("list sessions", stub -> stub.listSessions(
        ListSessionsRequest.newBuilder().setCredentials(buildCredentials()).build())).getSessionsList();
  }

  /**
   * Liveness probe. Needs no credentials, like {@code GET /api/v1/health}.
   */
  public boolean health() {
    return call("health", stub -> stub.health(HealthRequest.newBuilder().build())).getOk();
  }

  /**
   * Readiness probe. Needs no credentials, like {@code GET /api/v1/ready}. A node that is not ready
   * is a successful answer carrying {@code ready=false} and the reason, not an error.
   */
  public ReadyResponse ready() {
    return call("ready", stub -> stub.ready(ReadyRequest.newBuilder().build()));
  }

  // ------------------------------------------------------------------------------------
  // Restore and import (server-streaming, issue #7308)
  // ------------------------------------------------------------------------------------

  /**
   * Restores a backup archive this server produced into {@code targetDatabase}, blocking until the
   * restore finishes and handing every progress message to {@code onProgress} on the way. The
   * archive is named, not uploaded: the server resolves {@code fileName} inside {@code database}'s
   * own backup directory.
   * <p>
   * {@code overwrite} replaces an existing target. Without it an existing target fails the call, and
   * with it the existing database is dropped only once the restore has succeeded, so a failed
   * restore leaves it intact.
   *
   * @param onProgress called on this thread as each message arrives, or null to ignore progress
   *
   * @throws RuntimeException the failure the server raised, mapped by {@link GrpcClientErrorMapper}.
   *                          A restore that fails ends the stream with an error status, so a normal
   *                          return means the restore completed.
   */
  public void restoreBackup(final String database, final String fileName, final String targetDatabase,
      final boolean overwrite, final Consumer<RestoreProgress> onProgress) {
    drain("restore backup", onProgress,
        stub -> stub.restoreBackup(RestoreBackupRequest.newBuilder().setCredentials(buildCredentials())
            .setDatabase(database).setFileName(fileName).setTargetDatabase(targetDatabase).setOverwrite(overwrite)
            .build()));
  }

  /**
   * Creates {@code database} by restoring the archive at {@code url} into it, blocking until the
   * restore finishes.
   * <p>
   * The server fetches the URL, so unless the operator enabled
   * {@code arcadedb.server.restoreImportAllowLocalUrls} only http/https URLs to non-private hosts
   * are accepted; anything else fails the call rather than being fetched.
   *
   * @param onProgress called on this thread as each message arrives, or null to ignore progress
   */
  public void restoreDatabase(final String database, final String url, final Consumer<RestoreProgress> onProgress) {
    drain("restore database", onProgress,
        stub -> stub.restoreDatabase(RestoreDatabaseRequest.newBuilder().setCredentials(buildCredentials())
            .setDatabase(database).setUrl(url).build()));
  }

  /**
   * Creates {@code database} and imports {@code url} into it in one step, blocking until the import
   * finishes and returning the importer's own final report.
   * <p>
   * Progress messages carry either a log line or the importer's running counters, never both.
   *
   * @param onProgress called on this thread as each message arrives, or null to ignore progress
   *
   * @return the importer's report, or an empty document when this server sent none
   */
  public JSONObject importDatabase(final String database, final String url, final Consumer<ImportProgress> onProgress) {
    final ImportProgress last = drain("import database", onProgress,
        stub -> stub.importDatabase(ImportDatabaseRequest.newBuilder().setCredentials(buildCredentials())
            .setDatabase(database).setUrl(url).build()));
    return last == null || last.getResultJson().isEmpty() ? new JSONObject() : new JSONObject(last.getResultJson());
  }

  /**
   * Opens a server-streaming admin call, feeds every message to {@code onProgress} and returns the
   * last one - the {@code completed} message, since the server sends exactly one and sends it last.
   * <p>
   * The deadline is deliberately <b>not</b> the default admin one: a restore or an import runs for
   * as long as the data takes, and a call that outlives a 30-second deadline is the normal case, not
   * a fault. The operation's own end is what ends the call.
   */
  private <T> T drain(final String operation, final Consumer<T> onProgress,
      final StreamingAdminCall<T> body) {
    T last = null;
    try {
      final BlockingClientCall<?, T> stream = body.run(adminServiceBlockingV2Stub());
      boolean drained = false;
      try {
        while (stream.hasNext()) {
          last = stream.read();
          if (onProgress != null)
            onProgress.accept(last);
        }
        drained = true;
      } finally {
        // The caller's progress consumer is arbitrary code, and a throw from it leaves this call half
        // read. Cancel it rather than let it leak: these RPCs deliberately carry no deadline, because
        // a restore runs for as long as the data takes, so a call nobody is reading has nothing that
        // would ever end it. Cancelling stops the reporting, not the restore, which runs to its end
        // server-side either way.
        if (!drained)
          cancelQuietly(stream, operation);
      }
      return last;
    } catch (final StatusException e) {
      final RuntimeException mapped = GrpcClientErrorMapper.toException(e);
      if (mapped.getMessage() == null || mapped.getMessage().isBlank())
        throw new RemoteException("Failed to " + operation, e);
      throw mapped;
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RemoteException("Interrupted while waiting for '" + operation + "' to finish", e);
    }
  }

  /**
   * Cancels a stream that will not be read to its end. Never throws: it runs from a {@code finally}
   * while another failure is already on its way out, and masking that failure with a cancellation
   * problem would hide the reason the stream was abandoned in the first place.
   * <p>
   * The reason it gives is deliberately neutral about <i>who</i> ended it. This runs both when the
   * caller's progress consumer threw and when the read itself threw because the server had already
   * ended the call with an error status; in the second case the cancel is a no-op, and a message
   * blaming the client would be read by whoever is debugging a failed restore as evidence of
   * something that did not happen.
   */
  private static void cancelQuietly(final BlockingClientCall<?, ?> stream, final String operation) {
    try {
      stream.cancel("'" + operation + "' stream was not read to its end", null);
    } catch (final Exception e) {
      LogManager.instance().log(RemoteGrpcServer.class, Level.FINE,
          "Exception while cancelling the '%s' progress stream: %s", operation, e.getMessage());
    }
  }

  @FunctionalInterface
  private interface StreamingAdminCall<T> {
    BlockingClientCall<?, T> run(ArcadeDbAdminServiceGrpc.ArcadeDbAdminServiceBlockingV2Stub stub) throws StatusException;
  }

  private static JSONObject profilerDocument(final ProfilerDocumentResponse response) {
    return new JSONObject(response.getResultsJson());
  }

  /**
   * Runs one admin RPC under the default deadline and turns a failure into the typed ArcadeDB
   * exception the server raised, through the same {@link GrpcClientErrorMapper} the data plane uses.
   * <p>
   * The mapper, not a wrapped {@code RuntimeException}, is what makes the control plane's leader
   * refusal actionable: a follower answers {@code FAILED_PRECONDITION} with the leader's address on
   * the {@link com.arcadedb.server.grpc.LeaderRedirectProtocol} trailers, and the mapper rebuilds a
   * {@code ServerIsNotTheLeaderException} carrying that address. Flattening the status to a message
   * string would throw the address away, which is the one thing the caller needs (issue #7304).
   *
   * @param operation the operation name, used only when the failure carries no description of its own
   */
  private <T> T call(final String operation, final AdminCall<T> body) {
    try {
      return body.run(withDeadline(adminServiceBlockingV2Stub(), defaultTimeoutMs));
    } catch (final StatusException e) {
      final RuntimeException mapped = GrpcClientErrorMapper.toException(e);
      if (mapped.getMessage() == null || mapped.getMessage().isBlank())
        throw new RemoteException("Failed to " + operation, e);
      throw mapped;
    }
  }

  @FunctionalInterface
  private interface AdminCall<T> {
    T run(ArcadeDbAdminServiceGrpc.ArcadeDbAdminServiceBlockingV2Stub stub) throws StatusException;
  }

  public String endpoint() {
    return host + ":" + port;
  }

  @Override
  public String toString() {
    return "RemoteGrpcServer{endpoint=" + endpoint() + ", userName='" + userName + "'}";
  }

  /**
   * Refuses to attach credentials when they would travel in cleartext. Sending username/password over a
   * {@code usePlaintext()} channel to a non-loopback host exposes them on the wire on every RPC. Loopback targets
   * (no wire exposure) and an explicit {@code allowInsecureCredentials} opt-in are still permitted; otherwise this
   * throws so the caller enables TLS instead of leaking credentials.
   */
  private void ensureCredentialsAllowedOverChannel() {
    if (refuseCredentialsOverChannel)
      throw new SecurityException("Refusing to send credentials over a plaintext gRPC channel to non-loopback host '"
          + host + "'. Enable TLS, or explicitly opt in with allowInsecureCredentials=true.");
  }

  private static boolean isLoopbackHost(final String host) {
    if (host == null || host.isBlank())
      return false;
    final String h = host.trim();
    if (h.equalsIgnoreCase("localhost"))
      return true;
    try {
      return InetAddress.getByName(h).isLoopbackAddress();
    } catch (final UnknownHostException e) {
      // Unresolvable host: treat as non-loopback and fail closed.
      return false;
    }
  }

  /**
   * Creates call credentials for authentication, naming no database. Used by the admin plane, whose
   * RPCs authenticate from the request body and never read the database metadata.
   */
  protected CallCredentials createCallCredentials(final String userName, final String userPassword) {
    return credentials(userName, userPassword, null);
  }

  /**
   * @see #createCredentials(String)
   */
  protected CallCredentials createCredentials() {
    return createCredentials(null);
  }

  /**
   * Credentials for this server's user, naming the database the calls target so the server's auth
   * interceptor authenticates against that database rather than against a name nobody sent (#7320).
   *
   * @param database the target database, or {@code null}/blank to omit the key entirely
   */
  protected CallCredentials createCredentials(final String database) {
    return credentials(userName, userPassword, database);
  }

  /**
   * Credentials for {@code user}, or for this server's own account when {@code user} is {@code null} or
   * blank. The fallback is what keeps a {@link RemoteGrpcDatabase} constructed without credentials of its
   * own speaking as the server it was opened on, exactly as it did before issue #7374 - metadata carries
   * no null, so nothing has to special-case one.
   *
   * @param database the target database, or {@code null}/blank to omit the key entirely
   */
  protected CallCredentials createCredentials(final String database, final String user, final String password) {
    if (user == null || user.isBlank())
      return createCredentials(database);
    return credentials(user, password == null ? "" : password, database);
  }

  private CallCredentials credentials(final String user, final String password, final String database) {
    ensureCredentialsAllowedOverChannel();
    // Blank is the same as absent: an empty header would only make the server special-case it.
    final String targetDatabase = database == null || database.isBlank() ? null : database;

    return new CallCredentials() {
      @Override
      public void applyRequestMetadata(final RequestInfo requestInfo, final Executor appExecutor,
          final MetadataApplier applier) {
        final Metadata headers = new Metadata();
        headers.put(USERNAME_KEY, user);
        headers.put(PASSWORD_KEY, password);
        headers.put(ARCADE_USER_KEY, user);
        headers.put(ARCADE_PASSWORD_KEY, password);
        if (targetDatabase != null)
          headers.put(ARCADE_DATABASE_KEY, targetDatabase);
        applier.apply(headers);
      }

      @Override
      public void thisUsesUnstableApi() {
        // Required by the interface
      }
    };
  }
}
