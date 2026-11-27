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
import com.arcadedb.database.Database;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ha.raft.BaseRaftHATest;
import com.arcadedb.server.ha.raft.RaftHAServer;
import com.arcadedb.server.security.ServerSecurity;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ClientInterceptors;
import io.grpc.ForwardingClientCall;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * Where a gRPC call lands on a three-node Raft cluster that declares each node's gRPC port, and what a follower does
 * with a call only the leader may run: forward it, or refuse it naming the leader. gRPC has no request proxy, so every
 * leader-only RPC (graph batch load, the admin mutations, restore/import, the security mutations) refuses on a follower
 * with {@code FAILED_PRECONDITION} and the {@link LeaderRedirectProtocol} trailers, and each group pairs that refusal
 * with the control that the same call still succeeds on the leader.
 * <p>
 * Every test method restarts the cluster ({@code BaseRaftHATest} is per test), so the nested groups share this
 * fixture without sharing state.
 */
class GrpcLeaderRoutingIT extends BaseRaftHATest {

  // The Raft and HTTP ports are BaseRaftHATest's own: the base class binds those regardless of what
  // this server list says, so naming any others here would advertise addresses nobody listens on and
  // no leader would ever be elected. Only the gRPC port is this class's to choose.
  private static final int BASE_HTTP_PORT = 2480;
  // One gRPC port per server, drawn free for every test instance rather than fixed (issue #7496), from the same
  // ledger as the Raft ports so the two can never coincide (issue #8203).
  private final int[] grpcPorts = allocateFixturePorts(3);

  private static final Metadata.Key<String> USER_HEADER     = Metadata.Key.of("x-arcade-user",
      Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> PASSWORD_HEADER = Metadata.Key.of("x-arcade-password",
      Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> DATABASE_HEADER = Metadata.Key.of("x-arcade-database",
      Metadata.ASCII_STRING_MARSHALLER);

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected String getServerAddresses() {
    // Object form so each node declares the gRPC port it actually binds: three nodes on one host cannot share
    // one, which is also the shape where deriving a peer's port from this node's would be ambiguous (#6183).
    final StringBuilder sb = new StringBuilder();
    for (int i = 0; i < getServerCount(); i++) {
      if (i > 0)
        sb.append(",");
      sb.append("localhost:{raft:").append(raftPort(i))
          .append(",http:").append(BASE_HTTP_PORT + i)
          .append(",grpc:").append(grpcPorts[i]).append("}");
    }
    return sb.toString();
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);

    final String serverName = config.getValueAsString(GlobalConfiguration.SERVER_NAME);
    final int index = Integer.parseInt(serverName.substring(serverName.lastIndexOf('_') + 1));

    config.setValue("arcadedb.grpc.enabled", "true");
    config.setValue(GlobalConfiguration.GRPC_PORT.getKey(), String.valueOf(grpcPorts[index]));
    config.setValue("arcadedb.grpc.host", "localhost");
    config.setValue("arcadedb.grpc.reflection.enabled", "false");
    config.setValue("arcadedb.grpc.health.enabled", "false");

    final String existingPlugins = config.getValueAsString(GlobalConfiguration.SERVER_PLUGINS);
    final String pluginEntry = "GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin";
    if (existingPlugins == null || existingPlugins.isEmpty())
      config.setValue(GlobalConfiguration.SERVER_PLUGINS, pluginEntry);
    else if (!existingPlugins.contains(pluginEntry))
      config.setValue(GlobalConfiguration.SERVER_PLUGINS, existingPlugins + "," + pluginEntry);
  }

  private int anyFollowerOf(final int leaderIndex) {
    for (int i = 0; i < getServerCount(); i++)
      if (i != leaderIndex)
        return i;
    throw new IllegalStateException("At least one follower must exist");
  }

  private static DatabaseCredentials root() {
    return DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build();
  }

  private final class AuthClientInterceptor implements ClientInterceptor {
    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(final MethodDescriptor<ReqT, RespT> method,
        final CallOptions callOptions, final Channel next) {
      return new ForwardingClientCall.SimpleForwardingClientCall<>(next.newCall(method, callOptions)) {
        @Override
        public void start(final Listener<RespT> responseListener, final Metadata headers) {
          headers.put(USER_HEADER, "root");
          headers.put(PASSWORD_HEADER, DEFAULT_PASSWORD_FOR_TESTS);
          headers.put(DATABASE_HEADER, getDatabaseName());
          super.start(responseListener, headers);
        }
      };
    }
  }

  // Regression: #6183
  /**
   * Where a leader-only refusal can actually arise over gRPC, and where it cannot. Issue #6183 asked whether every
   * gRPC RPC that refuses on a follower should name the leader the way {@code graphBatchLoad} does; the answer is
   * that almost none of them ever refuse. {@code RaftReplicatedDatabase.command} <b>forwards</b> a DDL or otherwise
   * non-idempotent statement to the leader instead of rejecting it, so a schema change sent to a follower over
   * {@code executeCommand} succeeds and replicates. Only {@code graphBatchLoad} refuses outright, deliberately:
   * relaying a bulk load through a follower would double the traffic of the transport chosen to avoid exactly that.
   * <p>
   * That is what makes the {@code ServerIsNotTheLeaderException} handling in {@code GrpcErrorMapper} a consistency
   * guarantee rather than a fix for a routine failure - it covers the leadership change that lands between the
   * forwarding decision and the schema write - and this test pins the premise so a future change that turns
   * forwarding into a refusal is noticed here rather than by a user.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class FollowerCommandRouting {
    private static final String VERTEX_TYPE = "Issue6183ForwardedType";

    private ManagedChannel channel;

    @AfterEach
    void teardownGrpcClient() throws InterruptedException {
      closeChannel();
    }

    @Test
    void aSchemaChangeSentToAFollowerIsForwardedRatherThanRefused() throws Exception {
      final int leaderIndex = findLeaderIndex();
      assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);
      final int followerIndex = anyFollowerOf(leaderIndex);
      waitForAllServers();

      final ExecuteCommandResponse response = execute("localhost:" + grpcPorts[followerIndex],
          "CREATE VERTEX TYPE " + VERTEX_TYPE);

      assertThat(response.getSuccess()).as("a follower forwards a DDL to the leader instead of refusing it: %s",
          response.getMessage()).isTrue();

      waitForAllServers();
      for (int i = 0; i < getServerCount(); i++)
        assertThat(getServerDatabase(i, getDatabaseName()).getSchema().existsType(VERTEX_TYPE))
            .as("server %d must hold the forwarded type", i).isTrue();
    }

    /**
     * A refused DDL needs the trailers to name the node to go to instead, but that is no longer what sets it apart:
     * every {@code executeCommand} failure - including an ordinary one like a SQL syntax error - now surfaces as a
     * gRPC error status through {@code GrpcErrorMapper}, the same as every other RPC (issue #6192). The
     * {@code success=false} in-band envelope this test used to pin is exactly the defect that issue fixed.
     */
    @Test
    void anOrdinaryCommandFailureSurfacesAsAGrpcError() {
      final int leaderIndex = findLeaderIndex();
      assertThat(leaderIndex).isGreaterThanOrEqualTo(0);

      assertThatThrownBy(() -> execute("localhost:" + grpcPorts[leaderIndex], "SELECTT FROM Nothing"))
          .isInstanceOf(StatusRuntimeException.class);
    }

    /** Runs one SQL command against the given {@code host:port} gRPC target and returns the response as-is. */
    private ExecuteCommandResponse execute(final String target, final String sql) throws InterruptedException {
      channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();
      try {
        return ArcadeDbServiceGrpc.newBlockingStub(ClientInterceptors.intercept(channel, new AuthClientInterceptor()))
            .withDeadlineAfter(30, TimeUnit.SECONDS)
            .executeCommand(ExecuteCommandRequest.newBuilder()
                .setDatabase(getDatabaseName())
                .setLanguage("sql")
                .setCommand(sql)
                .setCredentials(DatabaseCredentials.newBuilder()
                    .setUsername("root")
                    .setPassword(DEFAULT_PASSWORD_FOR_TESTS)
                    .build())
                .build());
      } finally {
        closeChannel();
      }
    }

    private void closeChannel() throws InterruptedException {
      if (channel != null) {
        channel.shutdown();
        channel.awaitTermination(5, TimeUnit.SECONDS);
        channel = null;
      }
    }
  }

  // Regression: #6091
  /**
   * A follower refuses a graph batch load (issue #6070) and tells the caller where to go instead. Until issue #6091
   * the only address HA could expose was the leader's <b>HTTP</b> one, so the refusal had to say "use its gRPC port"
   * and a client redirecting itself had to already know the deployment's port-mapping convention. It now names an
   * address that can be dialled, on the trailers as well as in the message.
   * <p>
   * The assertion that matters is the last one: the refused load is retried <b>against the address the refusal
   * named</b>, taken from the trailer and dialled as-is, and it succeeds. A test that only compared the string
   * against the port it configured would pass just as well if the server had named the follower's own port - and
   * that is precisely the mistake the derive-from-local-port fallback makes on a heterogeneous cluster, which is
   * this cluster.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class GraphBatchLoadLeaderRedirect {
    private static final String VERTEX_TYPE = "Issue6091RedirectNode";

    private ManagedChannel channel;

    @AfterEach
    void teardownGrpcClient() throws InterruptedException {
      closeChannel();
    }

    @Test
    void theRefusalNamesAnAddressTheLoadActuallySucceedsOn() throws Exception {
      final int leaderIndex = findLeaderIndex();
      assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);
      final int followerIndex = anyFollowerOf(leaderIndex);

      final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
      leaderDb.transaction(() -> {
        if (!leaderDb.getSchema().existsType(VERTEX_TYPE))
          leaderDb.getSchema().createVertexType(VERTEX_TYPE);
      });
      waitForAllServers();

      final Throwable refusal = loadOneVertex("localhost:" + grpcPorts[followerIndex], "onFollower");
      assertThat(refusal).as("a load aimed at a follower must be refused").isInstanceOf(StatusRuntimeException.class);

      final StatusRuntimeException failure = (StatusRuntimeException) refusal;
      assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);

      final Metadata trailers = failure.getTrailers();
      assertThat(trailers).as("the refusal must carry trailers, not only prose").isNotNull();

      final String advertised = trailers.get(LeaderRedirectProtocol.LEADER_GRPC_ADDRESS);
      assertThat(advertised).as("the leader's gRPC address must be advertised in a form a client can dial")
          .isEqualTo("localhost:" + grpcPorts[leaderIndex]);
      assertThat(advertised).as("naming the refusing node's own port would send the caller straight back here")
          .isNotEqualTo("localhost:" + grpcPorts[followerIndex]);

      // The HTTP address stays available as the human-readable fallback, and is a different endpoint entirely.
      assertThat(trailers.get(LeaderRedirectProtocol.LEADER_HTTP_ADDRESS))
          .as("the HTTP address must still be reported").isNotNull().isNotEqualTo(advertised);

      // The typed redirect: the same class the HTTP protocol raises for this, carrying the same address.
      assertThat(trailers.get(GrpcErrorMapper.EXCEPTION_CLASS_KEY))
          .isEqualTo(ServerIsNotTheLeaderException.class.getName());

      // The message says it too, in the gRPC wording rather than the HTTP-with-a-caveat one it used to.
      assertThat(failure.getStatus().getDescription()).contains("not the cluster leader")
          .contains("'" + advertised + "' (gRPC address)").doesNotContain("use its gRPC port");

      // Nothing was written before the refusal: there is no partial load to reconcile anywhere.
      waitForAllServers();
      for (int i = 0; i < getServerCount(); i++)
        assertThat(getServerDatabase(i, getDatabaseName()).countType(VERTEX_TYPE, true))
            .as("server %d must hold nothing from the refused load", i).isZero();

      // And now the point of all of it: dial the advertised address verbatim and the load goes through.
      assertThat(loadOneVertex(advertised, "onRedirect")).as("the advertised address must accept the load").isNull();

      waitForAllServers();
      for (int i = 0; i < getServerCount(); i++)
        assertThat(getServerDatabase(i, getDatabaseName()).countType(VERTEX_TYPE, true))
            .as("server %d must see the redirected load", i).isEqualTo(1);
    }

    /**
     * Runs a one-vertex load against the given {@code host:port} gRPC target and returns the failure it ended with,
     * or null if it succeeded. The target is a string on purpose: it is what the server advertised.
     */
    private Throwable loadOneVertex(final String target, final String tempId) throws Exception {
      channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();
      try {
        final Channel authenticated = ClientInterceptors.intercept(channel, new AuthClientInterceptor());

        final AtomicReference<Throwable> errorRef = new AtomicReference<>();
        final CountDownLatch done = new CountDownLatch(1);

        final StreamObserver<GraphBatchChunk> observer = ArcadeDbServiceGrpc.newStub(authenticated).graphBatchLoad(
            new StreamObserver<>() {
              @Override
              public void onNext(final GraphBatchResult result) {
              }

              @Override
              public void onError(final Throwable t) {
                errorRef.set(t);
                done.countDown();
              }

              @Override
              public void onCompleted() {
                done.countDown();
              }
            });

        observer.onNext(GraphBatchChunk.newBuilder()
            .setDatabase(getDatabaseName())
            .setCredentials(DatabaseCredentials.newBuilder()
                .setUsername("root")
                .setPassword(DEFAULT_PASSWORD_FOR_TESTS)
                .build())
            .addRecords(GraphBatchRecord.newBuilder()
                .setKind(GraphBatchRecord.Kind.VERTEX)
                .setTypeName(VERTEX_TYPE)
                .setTempId(tempId)
                .putProperties("name", GrpcValue.newBuilder().setStringValue(tempId).build()))
            .build());
        observer.onCompleted();

        assertThat(done.await(30, TimeUnit.SECONDS)).as("the load must terminate one way or the other").isTrue();
        return errorRef.get();
      } finally {
        closeChannel();
      }
    }

    private void closeChannel() throws InterruptedException {
      if (channel != null) {
        channel.shutdown();
        channel.awaitTermination(5, TimeUnit.SECONDS);
        channel = null;
      }
    }
  }

  // Regression: #7304
  /**
   * Issue #7304: the HTTP control plane forwards create/drop database and create/drop user to the
   * leader ({@code PostServerCommandHandler.forwardToLeaderIfReplica}), so those commands never run on
   * a follower. gRPC has no request-proxying equivalent, and before this change its admin service ran
   * them wherever the call landed - which would have taken {@code createUserClusterWide} into
   * {@code HAServerPlugin.replicateSecurityUsers} on a follower.
   * <p>
   * The gRPC gate is therefore a refusal that names the leader, the pattern {@code graphBatchLoad}
   * established on this transport (issues #6091, #6183). This test drives it against a real follower:
   * the refusal must be FAILED_PRECONDITION and must carry the leader address in the
   * {@link LeaderRedirectProtocol} trailers, and the same call against the leader must still succeed -
   * without that positive control, a gate that refused everywhere would pass.
   */
  @Nested
  @Tag("slow")
  class AdminLeaderRouting {
    private ManagedChannel channel;

    @AfterEach
    void closeChannelAfterEach() {
      closeChannel();
    }

    /**
     * A blocking admin stub aimed at one node. Every call here is blocking and has returned by the time
     * the next stub is asked for, so the previous channel is closed rather than left behind - a test
     * that dials two nodes would otherwise leak the first one past the {@code @AfterEach}, which can
     * only see the last.
     */
    private ArcadeDbAdminServiceGrpc.ArcadeDbAdminServiceBlockingStub adminStubOn(final int serverIndex) {
      closeChannel();
      channel = ManagedChannelBuilder.forTarget("localhost:" + grpcPorts[serverIndex]).usePlaintext().build();
      return ArcadeDbAdminServiceGrpc.newBlockingStub(channel).withDeadlineAfter(30, TimeUnit.SECONDS);
    }

    private void closeChannel() {
      if (channel != null) {
        channel.shutdown();
        try {
          channel.awaitTermination(5, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        channel = null;
      }
    }

    /**
     * Both leader-only mutations, in one cluster: gRPC has no request proxy, so a follower answers
     * FAILED_PRECONDITION and names the leader on the {@link LeaderRedirectProtocol} trailers instead
     * of running the work where the call happened to land. Nothing may be written on any node on the
     * way to the refusal.
     * <p>
     * The two RPCs share a test because each method of this class restarts a three-node cluster, and
     * the assertions are the same gate seen through two entry points rather than two behaviours.
     */
    @Test
    void leaderOnlyAdminRpcsAreRefusedOnAFollowerAndNameTheLeader() {
      final int leaderIndex = findLeaderIndex();
      assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);
      waitForAllServers();

      final int followerIndex = anyFollowerOf(leaderIndex);
      final String user = "issue7304leaderuser";
      final String database = "issue7304_follower_db";

      final StatusRuntimeException userRefusal = catchStatus(() -> adminStubOn(followerIndex).createUser(
          CreateUserRequest.newBuilder().setCredentials(root()).setUser(user).setPassword("issue7304password").build()));

      assertThat(userRefusal.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
      assertThat(userRefusal.getTrailers()).isNotNull();
      assertThat(userRefusal.getTrailers().get(LeaderRedirectProtocol.LEADER_HTTP_ADDRESS))
          .as("the refusal must name where to go instead").isNotBlank();

      final StatusRuntimeException databaseRefusal = catchStatus(() -> adminStubOn(followerIndex).createDatabase(
          CreateDatabaseRequest.newBuilder().setCredentials(root()).setName(database).setType("document").build()));

      assertThat(databaseRefusal.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
      assertThat(databaseRefusal.getTrailers().get(LeaderRedirectProtocol.LEADER_HTTP_ADDRESS)).isNotBlank();

      for (int i = 0; i < getServerCount(); i++) {
        assertThat(getServer(i).getSecurity().existsUser(user))
            .as("server %d must not hold a user the follower refused to create", i).isFalse();
        assertThat(getServer(i).existsDatabase(database))
            .as("server %d must not hold a database the follower refused to create", i).isFalse();
      }
    }

    /**
     * The two controls the refusals need to mean anything: a gate that refused on every node, or one
     * that had widened to reads, would satisfy the test above and fail this one.
     * <p>
     * Reading the control plane is leader-agnostic on both transports - {@code GET /databases} answers
     * from whichever node is asked - so {@code ListDatabases} must still answer on a follower.
     */
    @Test
    void theSameCallSucceedsOnTheLeaderWhileReadsStillAnswerOnAFollower() {
      final int leaderIndex = findLeaderIndex();
      assertThat(leaderIndex).isGreaterThanOrEqualTo(0);
      waitForAllServers();

      final String user = "issue7304leaderokuser";
      adminStubOn(leaderIndex).createUser(CreateUserRequest.newBuilder().setCredentials(root())
          .setUser(user).setPassword("issue7304password").build());

      assertThat(getServer(leaderIndex).getSecurity().existsUser(user)).isTrue();

      final ListDatabasesResponse response = adminStubOn(anyFollowerOf(leaderIndex))
          .listDatabases(ListDatabasesRequest.newBuilder().setCredentials(root()).build());

      assertThat(response.getDatabasesList()).contains(getDatabaseName());
    }

    private static StatusRuntimeException catchStatus(final Runnable call) {
      return catchThrowableOfType(StatusRuntimeException.class, call::run);
    }
  }

  // Regression: #7308
  /**
   * Issue #7308: {@code restore backup}, {@code restore database} and {@code import database} are in
   * the set the HTTP control plane forwards to the leader
   * ({@code PostServerCommandHandler.forwardToLeaderIfReplica}), so over HTTP they never execute on a
   * follower. gRPC has no request proxy, so the equivalent gate is a refusal that names the leader -
   * the same {@code requireLeader} the four #7304 mutations use, extended to these three.
   * <p>
   * Without it, a restore accepted on a follower would swap a database directory into place behind the
   * cluster's back, and an import would create a database on one node only.
   * <p>
   * The sibling of {@link AdminLeaderRouting} for the RPCs that stream. They need their
   * own assertions because a server-streaming call reports nothing until its response is read: the
   * refusal arrives when the stream is drained, not when the stub method returns.
   */
  @Nested
  @Tag("slow")
  class RestoreImportLeaderRouting {
    private ManagedChannel channel;

    @AfterEach
    void closeChannelAfterEach() {
      closeChannel();
    }

    /**
     * All three RPCs in one test method: each method of this class restarts a three-node cluster, and
     * the assertions are the same gate seen through three entry points rather than three behaviours.
     * <p>
     * Every request names a database that does not exist and a URL that is never fetched. If the gate
     * holds, none of that is reached; the final loop is what proves it, by checking no node was
     * touched.
     */
    @Test
    void restoreAndImportRpcsAreRefusedOnAFollowerAndNameTheLeader() {
      final int leaderIndex = findLeaderIndex();
      assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);
      waitForAllServers();

      final int followerIndex = anyFollowerOf(leaderIndex);
      final String database = "issue7308_follower_db";

      assertRefusedAndNamesTheLeader(catchStatus(() -> drain(adminStubOn(followerIndex).restoreBackup(
          RestoreBackupRequest.newBuilder().setCredentials(root()).setDatabase(getDatabaseName())
              .setFileName("anything-backup-20260101.zip").setTargetDatabase(database).build()))));

      assertRefusedAndNamesTheLeader(catchStatus(() -> drain(adminStubOn(followerIndex).restoreDatabase(
          RestoreDatabaseRequest.newBuilder().setCredentials(root()).setDatabase(database)
              .setUrl("https://example.invalid/archive.zip").build()))));

      assertRefusedAndNamesTheLeader(catchStatus(() -> drain(adminStubOn(followerIndex).importDatabase(
          ImportDatabaseRequest.newBuilder().setCredentials(root()).setDatabase(database)
              .setUrl("https://example.invalid/data.csv").build()))));

      for (int i = 0; i < getServerCount(); i++)
        assertThat(getServer(i).existsDatabase(database))
            .as("server %d must not hold a database the follower refused to create", i).isFalse();
    }

    /**
     * The control the refusals need to mean anything: a gate that refused on every node would satisfy
     * the test above and fail this one. The leader runs the same call and gets past the gate, failing
     * instead on the archive that is genuinely not there - INVALID_ARGUMENT, not FAILED_PRECONDITION.
     */
    @Test
    void theSameCallGetsPastTheGateOnTheLeader() {
      final int leaderIndex = findLeaderIndex();
      assertThat(leaderIndex).isGreaterThanOrEqualTo(0);
      waitForAllServers();

      final StatusRuntimeException failure = catchStatus(() -> drain(adminStubOn(leaderIndex).restoreBackup(
          RestoreBackupRequest.newBuilder().setCredentials(root()).setDatabase(getDatabaseName())
              .setFileName("anything-backup-20260101.zip").setTargetDatabase("issue7308_leader_db").build())));

      assertThat(failure.getStatus().getCode())
          .as("the leader must reach the operation and fail on its arguments, not on the leader gate")
          .isEqualTo(Status.Code.INVALID_ARGUMENT);
    }

    private static void assertRefusedAndNamesTheLeader(final StatusRuntimeException refusal) {
      assertThat(refusal.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
      assertThat(refusal.getTrailers()).isNotNull();
      assertThat(refusal.getTrailers().get(LeaderRedirectProtocol.LEADER_HTTP_ADDRESS))
          .as("the refusal must name where to go instead").isNotBlank();
    }

    private static StatusRuntimeException catchStatus(final Runnable call) {
      final StatusRuntimeException e = catchThrowableOfType(StatusRuntimeException.class, call::run);
      assertThat(e).as("the call must have been refused").isNotNull();
      return e;
    }

    private static void drain(final Iterator<?> stream) {
      while (stream.hasNext())
        stream.next();
    }

    /**
     * A blocking admin stub aimed at one node. Every call here is blocking and has returned by the time
     * the next stub is asked for, so the previous channel is closed rather than left behind.
     */
    private ArcadeDbAdminServiceGrpc.ArcadeDbAdminServiceBlockingStub adminStubOn(final int serverIndex) {
      closeChannel();
      channel = ManagedChannelBuilder.forTarget("localhost:" + grpcPorts[serverIndex]).usePlaintext().build();
      return ArcadeDbAdminServiceGrpc.newBlockingStub(channel).withDeadlineAfter(30, TimeUnit.SECONDS);
    }

    private void closeChannel() {
      if (channel != null) {
        channel.shutdown();
        try {
          channel.awaitTermination(5, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        channel = null;
      }
    }
  }

  // Regression: #8109
  /**
   * Issue #8109 (absorbing #7826): the gRPC {@code CreateUser}, {@code DeleteUser} and {@code UpdateUser} RPCs refuse
   * off-leader ({@code requireLeader}, issue #7304), but {@code SaveGroup}, {@code DeleteGroup}, {@code CreateApiToken}
   * and {@code DeleteApiToken} did not: they ran their cluster-wide security mutation on whichever node the client
   * reached. They are now refused on a follower with the leader's address, like the user RPCs, and the HTTP routes for
   * the same operations forward to the leader.
   */
  @Nested
  @Tag("slow")
  class SecurityLeaderRouting {
    private ManagedChannel channel;

    @AfterEach
    void closeChannelAfterEach() {
      closeChannel();
    }

    /**
     * All four RPCs in one method, since each method restarts a three-node cluster. The DELETE RPCs aim at state that
     * exists, created through the leader first, so a follower that ran them would visibly remove it.
     */
    @Test
    void groupAndApiTokenRpcsAreRefusedOnAFollowerAndNameTheLeader() {
      final int leaderIndex = findLeaderIndex();
      assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);
      waitForAllServers();
      final int followerIndex = anyFollowerOf(leaderIndex);

      final String existingGroup = "issue8109grpcgroup";
      adminStubOn(leaderIndex).saveGroup(SaveGroupRequest.newBuilder().setCredentials(root()).setDatabase("*")
          .setName(existingGroup).setGroupJson("{}").build());
      final String existingToken = "issue8109grpctoken";
      assertThat(adminStubOn(leaderIndex).createApiToken(
          CreateApiTokenRequest.newBuilder().setCredentials(root()).setName(existingToken).build()).getToken())
          .isNotBlank();
      final String existingTokenHash = tokenHashOf(getServer(leaderIndex).getSecurity(), existingToken);
      assertThat(existingTokenHash).isNotNull();

      assertRefusedAndNamesTheLeader(catchStatus(() -> adminStubOn(followerIndex).saveGroup(
          SaveGroupRequest.newBuilder().setCredentials(root()).setDatabase("*").setName("issue8109nevergroup")
              .setGroupJson("{}").build())));
      assertRefusedAndNamesTheLeader(catchStatus(() -> adminStubOn(followerIndex).deleteGroup(
          DeleteGroupRequest.newBuilder().setCredentials(root()).setDatabase("*").setName(existingGroup).build())));
      assertRefusedAndNamesTheLeader(catchStatus(() -> adminStubOn(followerIndex).createApiToken(
          CreateApiTokenRequest.newBuilder().setCredentials(root()).setName("issue8109nevertoken").build())));
      assertRefusedAndNamesTheLeader(catchStatus(() -> adminStubOn(followerIndex).deleteApiToken(
          DeleteApiTokenRequest.newBuilder().setCredentials(root()).setTokenHash(existingTokenHash).build())));

      for (int i = 0; i < getServerCount(); i++) {
        final ServerSecurity security = getServer(i).getSecurity();
        assertThat(hasGroup(security, "issue8109nevergroup")).as("server %d must not hold the refused group", i)
            .isFalse();
        assertThat(tokenHashOf(security, "issue8109nevertoken")).as("server %d must not hold the refused token", i)
            .isNull();
      }
      // The two deletes: the state they aimed at is still in force on the leader, the node every follower copies.
      assertThat(hasGroup(getServer(leaderIndex).getSecurity(), existingGroup)).isTrue();
      assertThat(tokenHashOf(getServer(leaderIndex).getSecurity(), existingToken)).isNotNull();
    }

    /** The control: the same RPCs get past the gate on the leader. */
    @Test
    void theSameRpcsSucceedOnTheLeader() {
      final int leaderIndex = findLeaderIndex();
      assertThat(leaderIndex).isGreaterThanOrEqualTo(0);
      waitForAllServers();

      assertThat(adminStubOn(leaderIndex).saveGroup(SaveGroupRequest.newBuilder().setCredentials(root())
          .setDatabase("*").setName("issue8109leadergroup").setGroupJson("{}").build()).getSuccess()).isTrue();
      assertThat(adminStubOn(leaderIndex).deleteGroup(DeleteGroupRequest.newBuilder().setCredentials(root())
          .setDatabase("*").setName("issue8109leadergroup").build()).getSuccess()).isTrue();

      assertThat(adminStubOn(leaderIndex).createApiToken(CreateApiTokenRequest.newBuilder().setCredentials(root())
          .setName("issue8109leadertoken").build()).getToken()).isNotBlank();
      final String hash = tokenHashOf(getServer(leaderIndex).getSecurity(), "issue8109leadertoken");
      assertThat(adminStubOn(leaderIndex).deleteApiToken(DeleteApiTokenRequest.newBuilder().setCredentials(root())
          .setTokenHash(hash).build()).getSuccess()).isTrue();
    }

    // ---------------------------------------------------------------------------------------------

    private static boolean hasGroup(final ServerSecurity security, final String name) {
      return security.groupsToJSON().getJSONObject("databases").getJSONObject("*", new JSONObject())
          .getJSONObject("groups", new JSONObject()).has(name);
    }

    private static String tokenHashOf(final ServerSecurity security, final String name) {
      for (final JSONObject token : security.getApiTokenConfiguration().listTokens())
        if (name.equals(token.getString("name", null)))
          return token.getString("tokenHash");
      return null;
    }

    private static void assertRefusedAndNamesTheLeader(final StatusRuntimeException refusal) {
      assertThat(refusal.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
      assertThat(refusal.getTrailers()).isNotNull();
      assertThat(refusal.getTrailers().get(LeaderRedirectProtocol.LEADER_HTTP_ADDRESS))
          .as("the refusal must name where to go instead").isNotBlank();
    }

    private static StatusRuntimeException catchStatus(final Runnable call) {
      final StatusRuntimeException e = catchThrowableOfType(StatusRuntimeException.class, call::run);
      assertThat(e).as("the call must have been refused").isNotNull();
      return e;
    }

    private ArcadeDbAdminServiceGrpc.ArcadeDbAdminServiceBlockingStub adminStubOn(final int serverIndex) {
      closeChannel();
      channel = ManagedChannelBuilder.forTarget("localhost:" + grpcPorts[serverIndex]).usePlaintext().build();
      return ArcadeDbAdminServiceGrpc.newBlockingStub(channel).withDeadlineAfter(30, TimeUnit.SECONDS);
    }

    private void closeChannel() {
      if (channel != null) {
        channel.shutdown();
        try {
          channel.awaitTermination(5, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        channel = null;
      }
    }
  }

  // Regression: #10066
  /**
   * Issue #10066: the gRPC admin gate ({@code requireLeader}) named the leader by {@code HAServerPlugin.getLeaderAddress()},
   * unvetted, and the redirect trailers the mapper attaches to the answer were read from the same address. On a cluster whose
   * peers share a host and declare no HTTP port (issue #6191) that is the follower's OWN address, so both the text and the trailer
   * sent the client back to the node that had just refused it. They now name the leader the way the schema refusal does (issue
   * #10060): by its HTTP address when that identifies it, by its server name when it does not.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  @Tag("slow")
  class AdminRefusalNamesTheLeader {
    private ManagedChannel channel;

    @AfterEach
    void closeChannelAfterEach() {
      closeChannel();
    }

    @Test
    @Timeout(120)
    void anAdminRefusalOnAFollowerThatCannotTellItsPeersApartNamesTheLeaderByName() {
      final int leader = requireLeaderIndex();
      final int follower = firstFollowerOf(leader);
      final RaftHAServer raft = getRaftPlugin(follower).getRaftHAServer();
      final Runnable restore = refuseForwardedWritesOn(follower);
      try {
        final StatusRuntimeException refusal = createUserRefusedOn(follower, "issue10066grpcambiguous");
        final String leaderName = raft.getLeaderName();
        assertThat(leaderName).as("the follower must know which node leads").isNotBlank();

        final String named = refusal.getTrailers().get(LeaderRedirectProtocol.LEADER_HTTP_ADDRESS);
        assertThat(named).as("the redirect trailer must not send the client back to the follower that refused it").isNotNull();
        assertThat(raft.isOwnHttpAddress(named)).as("trailer named '%s'", named).isFalse();
        assertThat(named).isEqualTo(leaderName);
      } finally {
        restore.run();
      }
    }

    @Test
    @Timeout(120)
    void anAdminRefusalOnAFollowerOfAWellConfiguredClusterNamesTheLeadersHttpAddress() {
      final int leader = requireLeaderIndex();
      final int follower = firstFollowerOf(leader);
      final String leaderHttpAddress = getRaftPlugin(follower).getRaftHAServer().getLeaderHttpAddress();

      final StatusRuntimeException refusal = createUserRefusedOn(follower, "issue10066grpcconfigured");
      assertThat(refusal.getTrailers().get(LeaderRedirectProtocol.LEADER_HTTP_ADDRESS)).isEqualTo(leaderHttpAddress);
    }

    private StatusRuntimeException createUserRefusedOn(final int follower, final String user) {
      closeChannel();
      channel = ManagedChannelBuilder.forTarget("localhost:" + grpcPorts[follower]).usePlaintext().build();
      final ArcadeDbAdminServiceGrpc.ArcadeDbAdminServiceBlockingStub stub = ArcadeDbAdminServiceGrpc.newBlockingStub(channel)
          .withDeadlineAfter(30, TimeUnit.SECONDS);
      final DatabaseCredentials root = DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS)
          .build();

      final StatusRuntimeException refusal = catchThrowableOfType(StatusRuntimeException.class,
          () -> stub.createUser(CreateUserRequest.newBuilder().setCredentials(root).setUser(user).setPassword("issue10066password")
              .build()));
      assertThat(refusal).as("the call must have been refused").isNotNull();
      assertThat(refusal.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
      assertThat(refusal.getTrailers()).isNotNull();
      return refusal;
    }

    private void closeChannel() {
      if (channel != null) {
        channel.shutdown();
        try {
          channel.awaitTermination(5, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        channel = null;
      }
    }
  }
}
