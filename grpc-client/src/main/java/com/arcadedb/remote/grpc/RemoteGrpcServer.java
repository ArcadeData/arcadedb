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
import com.arcadedb.server.grpc.ArcadeDbAdminServiceGrpc;
import com.arcadedb.server.grpc.ArcadeDbServiceGrpc;
import com.arcadedb.server.grpc.BackupInfo;
import com.arcadedb.server.grpc.CloseDatabaseRequest;
import com.arcadedb.server.grpc.CreateDatabaseRequest;
import com.arcadedb.server.grpc.CreateUserRequest;
import com.arcadedb.server.grpc.DatabaseCredentials;
import com.arcadedb.server.grpc.DeleteBackupRequest;
import com.arcadedb.server.grpc.DeleteUserRequest;
import com.arcadedb.server.grpc.DisconnectClusterRequest;
import com.arcadedb.server.grpc.DropDatabaseRequest;
import com.arcadedb.server.grpc.GetBackupConfigRequest;
import com.arcadedb.server.grpc.GetBackupConfigResponse;
import com.arcadedb.server.grpc.GetServerEventsRequest;
import com.arcadedb.server.grpc.GetServerEventsResponse;
import com.arcadedb.server.grpc.HealthRequest;
import com.arcadedb.server.grpc.ImportDatabaseRequest;
import com.arcadedb.server.grpc.ImportProgress;
import com.arcadedb.server.grpc.ListBackupsRequest;
import com.arcadedb.server.grpc.ListBackupsResponse;
import com.arcadedb.server.grpc.ListDatabasesRequest;
import com.arcadedb.server.grpc.ListDatabasesResponse;
import com.arcadedb.server.grpc.ListUsersRequest;
import com.arcadedb.server.grpc.OpenDatabaseRequest;
import com.arcadedb.server.grpc.ProfilerDocumentResponse;
import com.arcadedb.server.grpc.ProfilerListRequest;
import com.arcadedb.server.grpc.ProfilerLoadRequest;
import com.arcadedb.server.grpc.ProfilerResetRequest;
import com.arcadedb.server.grpc.ProfilerRunInfo;
import com.arcadedb.server.grpc.ProfilerResultsRequest;
import com.arcadedb.server.grpc.ProfilerStartRequest;
import com.arcadedb.server.grpc.ProfilerStopRequest;
import com.arcadedb.server.grpc.ReadyRequest;
import com.arcadedb.server.grpc.ReadyResponse;
import com.arcadedb.server.grpc.RestoreBackupRequest;
import com.arcadedb.server.grpc.RestoreDatabaseRequest;
import com.arcadedb.server.grpc.RestoreProgress;
import com.arcadedb.server.grpc.SetBackupConfigRequest;
import com.arcadedb.server.grpc.SetDatabaseSettingRequest;
import com.arcadedb.server.grpc.SetServerSettingRequest;
import com.arcadedb.server.grpc.ShutdownRequest;
import com.arcadedb.server.grpc.TriggerBackupRequest;
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
 * disconnect, and the two container probes. What it does not cover is what the proto does not carry
 * either - groups and API tokens (#7309), progress and sessions (#7310). Restore and import came
 * with #7308 and are the only methods here that stream: they block until the operation finishes,
 * handing each progress message to a callback on the way.
 * <p>
 * Every method carries the credentials this instance was built with in the request body, except
 * {@link #health()} and {@link #ready()}: their requests have no credentials field, and the server
 * exempts those two methods from the admin authentication gate, so a probe works whatever this
 * instance was constructed with. (The shared stub still sends this instance's call credentials as
 * headers on every call, probes included; the server does not read them for the admin service.)
 */
public class RemoteGrpcServer implements AutoCloseable {

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
   * Creates call credentials for authentication
   */
  protected CallCredentials createCallCredentials(String userName, String userPassword) {
    ensureCredentialsAllowedOverChannel();
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
    ensureCredentialsAllowedOverChannel();
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
