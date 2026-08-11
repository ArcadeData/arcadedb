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
 * A graph batch load must not run on a follower (issue #6070). The bulk path mutates state that only the leader
 * can serialize - the schema dictionary above all - so running it on a follower races the local state-machine
 * apply in {@code Dictionary.getIdByName}, which is the corruption issue #4122 was about and the reason the
 * HTTP endpoint relays the payload to the leader instead of loading it locally.
 * <p>
 * {@code GraphBatchLoad} had no such guard: it took the load wherever the client happened to connect. It cannot
 * relay the way HTTP does, because the HA plugin exposes the leader's HTTP address only and relaying a bulk load
 * through a follower would double the traffic of the transport chosen to avoid exactly that, so it refuses -
 * before writing anything, and naming the leader, so the caller can redirect rather than reconcile a load that
 * got half-way.
 * <p>
 * What makes this worth a cluster test rather than a unit test is the second assertion: the same load, over the
 * same RPC, must still succeed against the leader. A guard that refused everywhere would pass a test that only
 * checked the follower.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6070GraphBatchLoadLeaderGuardIT extends BaseRaftHATest {

  private static final int    BASE_GRPC_PORT = 51091;
  private static final String VERTEX_TYPE    = "Issue6070LeaderGuardNode";

  private static final Metadata.Key<String> USER_HEADER     = Metadata.Key.of("x-arcade-user",
      Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> PASSWORD_HEADER = Metadata.Key.of("x-arcade-password",
      Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> DATABASE_HEADER = Metadata.Key.of("x-arcade-database",
      Metadata.ASCII_STRING_MARSHALLER);

  private ManagedChannel channel;

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);

    final String serverName = config.getValueAsString(GlobalConfiguration.SERVER_NAME);
    final int index = Integer.parseInt(serverName.substring(serverName.lastIndexOf('_') + 1));

    config.setValue("arcadedb.grpc.enabled", "true");
    config.setValue("arcadedb.grpc.port", String.valueOf(BASE_GRPC_PORT + index));
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

  @Override
  protected int getServerCount() {
    return 3;
  }

  @AfterEach
  void teardownGrpcClient() throws InterruptedException {
    if (channel != null) {
      channel.shutdown();
      channel.awaitTermination(5, TimeUnit.SECONDS);
      channel = null;
    }
  }

  @Test
  void aLoadOnAFollowerIsRefusedAndTheSameLoadOnTheLeaderSucceeds() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    int followerIndex = -1;
    for (int i = 0; i < getServerCount(); i++)
      if (i != leaderIndex) {
        followerIndex = i;
        break;
      }
    assertThat(followerIndex).as("At least one follower must exist").isGreaterThanOrEqualTo(0);

    // Schema through the leader, so the guard under test is the only thing the load depends on.
    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    leaderDb.transaction(() -> {
      if (!leaderDb.getSchema().existsType(VERTEX_TYPE))
        leaderDb.getSchema().createVertexType(VERTEX_TYPE);
    });
    waitForAllServers();

    // The follower refuses, and says where to go instead.
    final Throwable refusal = loadOneVertex(followerIndex, "onFollower");
    assertThat(refusal).as("a load aimed at a follower must be refused, not silently taken")
        .isInstanceOf(StatusRuntimeException.class);

    final StatusRuntimeException failure = (StatusRuntimeException) refusal;
    assertThat(failure.getStatus().getCode()).as("the caller has to be able to tell this from an engine fault")
        .isEqualTo(Status.Code.FAILED_PRECONDITION);
    assertThat(failure.getStatus().getDescription()).contains("not the cluster leader");

    // Refused before writing anything: nothing to reconcile anywhere in the cluster.
    waitForAllServers();
    for (int i = 0; i < getServerCount(); i++)
      assertThat(getServerDatabase(i, getDatabaseName()).countType(VERTEX_TYPE, true))
          .as("server %d must hold nothing from the refused load", i)
          .isZero();

    // The very same load against the leader goes through, and replicates.
    assertThat(loadOneVertex(leaderIndex, "onLeader")).as("the leader must still accept the load").isNull();

    waitForAllServers();
    for (int i = 0; i < getServerCount(); i++)
      assertThat(getServerDatabase(i, getDatabaseName()).countType(VERTEX_TYPE, true))
          .as("server %d (leader=%d) must see the vertex loaded on the leader", i, leaderIndex)
          .isEqualTo(1);
  }

  /**
   * The refusal names the leader, which is a fact about the cluster's layout. It must therefore come after the
   * caller has been resolved against the database it asked for, the way every other RPC on this service starts.
   * A follower asked to load into a database that cannot be reached has to answer about that database, not
   * volunteer where the leader lives - and the difference is only observable on a follower, because that is the
   * only place the leadership branch is reached at all.
   */
  @Test
  void aFollowerResolvesTheDatabaseBeforeNamingTheLeader() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    int followerIndex = -1;
    for (int i = 0; i < getServerCount(); i++)
      if (i != leaderIndex) {
        followerIndex = i;
        break;
      }
    assertThat(followerIndex).as("At least one follower must exist").isGreaterThanOrEqualTo(0);

    final Throwable refusal = loadOneVertexInto(followerIndex, "unauthorized", "Issue6070NoSuchDatabase");

    assertThat(refusal).as("a load naming a database that cannot be reached must be refused").isNotNull();
    assertThat(refusal.getMessage())
        .as("the refusal must be about the database, not about where the leader is")
        .doesNotContain("not the cluster leader")
        .doesNotContain("Reconnect to the leader");
  }

  /**
   * Runs a one-vertex load against the given server and returns the failure it ended with, or null if it
   * succeeded.
   */
  private Throwable loadOneVertex(final int serverIndex, final String tempId) throws Exception {
    return loadOneVertexInto(serverIndex, tempId, getDatabaseName());
  }

  private Throwable loadOneVertexInto(final int serverIndex, final String tempId, final String databaseName)
      throws Exception {
    channel = ManagedChannelBuilder.forAddress("localhost", BASE_GRPC_PORT + serverIndex).usePlaintext().build();
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
          .setDatabase(databaseName)
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
