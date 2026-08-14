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
import com.arcadedb.server.ha.raft.BaseRaftHATest;
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
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

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
class Issue6091GraphBatchLoadLeaderRedirectIT extends BaseRaftHATest {

  private static final int    BASE_RAFT_PORT = 2434;
  private static final int    BASE_HTTP_PORT = 2480;
  private static final int    BASE_GRPC_PORT = 51101;
  private static final String VERTEX_TYPE    = "Issue6091RedirectNode";

  private static final Metadata.Key<String> USER_HEADER     = Metadata.Key.of("x-arcade-user",
      Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> PASSWORD_HEADER = Metadata.Key.of("x-arcade-password",
      Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> DATABASE_HEADER = Metadata.Key.of("x-arcade-database",
      Metadata.ASCII_STRING_MARSHALLER);

  private ManagedChannel channel;

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected String getServerAddresses() {
    // Object form so each node declares the gRPC port it actually binds. Three nodes on one host cannot share a
    // port, so this is also the deployment shape where deriving a peer's port from this node's would be wrong.
    final StringBuilder sb = new StringBuilder();
    for (int i = 0; i < getServerCount(); i++) {
      if (i > 0)
        sb.append(",");
      sb.append("localhost:{raft:").append(BASE_RAFT_PORT + i)
          .append(",http:").append(BASE_HTTP_PORT + i)
          .append(",grpc:").append(BASE_GRPC_PORT + i).append("}");
    }
    return sb.toString();
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);

    final String serverName = config.getValueAsString(GlobalConfiguration.SERVER_NAME);
    final int index = Integer.parseInt(serverName.substring(serverName.lastIndexOf('_') + 1));

    config.setValue("arcadedb.grpc.enabled", "true");
    config.setValue(GlobalConfiguration.GRPC_PORT.getKey(), String.valueOf(BASE_GRPC_PORT + index));
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

    final Throwable refusal = loadOneVertex("localhost:" + (BASE_GRPC_PORT + followerIndex), "onFollower");
    assertThat(refusal).as("a load aimed at a follower must be refused").isInstanceOf(StatusRuntimeException.class);

    final StatusRuntimeException failure = (StatusRuntimeException) refusal;
    assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);

    final Metadata trailers = failure.getTrailers();
    assertThat(trailers).as("the refusal must carry trailers, not only prose").isNotNull();

    final String advertised = trailers.get(LeaderRedirectProtocol.LEADER_GRPC_ADDRESS);
    assertThat(advertised).as("the leader's gRPC address must be advertised in a form a client can dial")
        .isEqualTo("localhost:" + (BASE_GRPC_PORT + leaderIndex));
    assertThat(advertised).as("naming the refusing node's own port would send the caller straight back here")
        .isNotEqualTo("localhost:" + (BASE_GRPC_PORT + followerIndex));

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

  private int anyFollowerOf(final int leaderIndex) {
    for (int i = 0; i < getServerCount(); i++)
      if (i != leaderIndex)
        return i;
    throw new IllegalStateException("At least one follower must exist");
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
}
