/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ha.raft.BaseRaftHATest;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #7389: {@code CreateDatabase} and {@code DropDatabase} over gRPC used to
 * create and drop the database on the local node only, never submitting the Raft entry the HTTP
 * {@code POST /server} commands submit. On an HA cluster that diverged the leader - which is where
 * the leader-only gate guarantees the operation lands - from its followers.
 * <p>
 * The four tests drive the same invariant through the four transport entry points that can violate
 * it, so a fix that covers only the transport the reporter named cannot pass: gRPC create, gRPC
 * drop, and the two HTTP commands that were already correct and now share the implementation.
 */
class Issue7389GrpcCreateDropDatabaseReplicationIT extends BaseRaftHATest {

  private static final int    SERVER_COUNT     = 2;
  private static final String GRPC_CREATED_DB  = "issue7389GrpcCreated";
  private static final String GRPC_DROPPED_DB  = "issue7389GrpcDropped";
  private static final String HTTP_CREATED_DB  = "issue7389HttpCreated";
  private static final String HTTP_DROPPED_DB  = "issue7389HttpDropped";
  /** The Raft entry is committed synchronously but applied on the peers asynchronously, so every assertion polls. */
  private static final long   PROPAGATION_WAIT = TimeUnit.SECONDS.toMillis(60);

  private final int[] grpcPorts = new int[SERVER_COUNT];

  private ManagedChannel channel;

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);

    final String serverName = config.getValueAsString(GlobalConfiguration.SERVER_NAME);
    final int index = Integer.parseInt(serverName.substring(serverName.lastIndexOf('_') + 1));

    if (grpcPorts[index] == 0)
      grpcPorts[index] = allocateFreePort();

    config.setValue("arcadedb.grpc.enabled", "true");
    config.setValue("arcadedb.grpc.port", String.valueOf(grpcPorts[index]));
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
    return SERVER_COUNT;
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
  void createDatabaseOverGrpcReplicatesToEveryPeer() throws Exception {
    final int leaderIndex = leaderIndex();

    final ArcadeDbAdminServiceGrpc.ArcadeDbAdminServiceBlockingStub adminStub = adminStubOn(leaderIndex);
    adminStub.createDatabase(CreateDatabaseRequest.newBuilder()
        .setCredentials(credentials())
        .setName(GRPC_CREATED_DB)
        .setType("graph")
        .build());

    awaitDatabaseOnEveryServer(GRPC_CREATED_DB, leaderIndex, "created over gRPC");

    // The 'graph' variant still initialises the default types on the node that served the call.
    final Database leaderDb = getServer(leaderIndex).getDatabase(GRPC_CREATED_DB);
    assertThat(leaderDb.getSchema().existsType("V")).as("the 'graph' type must still create V").isTrue();
    assertThat(leaderDb.getSchema().existsType("E")).as("the 'graph' type must still create E").isTrue();
  }

  @Test
  void dropDatabaseOverGrpcReplicatesToEveryPeer() throws Exception {
    final int leaderIndex = leaderIndex();

    // Seeded over HTTP on purpose: the drop must be proved broken on a database the whole cluster
    // has, independently of whether gRPC's create replicates.
    createDatabaseOverHttp(leaderIndex, GRPC_DROPPED_DB);
    awaitDatabaseOnEveryServer(GRPC_DROPPED_DB, leaderIndex, "seeded over HTTP");

    final ArcadeDbAdminServiceGrpc.ArcadeDbAdminServiceBlockingStub adminStub = adminStubOn(leaderIndex);
    adminStub.dropDatabase(DropDatabaseRequest.newBuilder()
        .setCredentials(credentials())
        .setName(GRPC_DROPPED_DB)
        .build());

    awaitDatabaseGoneFromEveryServer(GRPC_DROPPED_DB, leaderIndex, "dropped over gRPC");
  }

  @Test
  void createDatabaseOverHttpStillReplicates() throws Exception {
    final int leaderIndex = leaderIndex();

    createDatabaseOverHttp(leaderIndex, HTTP_CREATED_DB);

    awaitDatabaseOnEveryServer(HTTP_CREATED_DB, leaderIndex, "created over HTTP");
  }

  @Test
  void dropDatabaseOverHttpStillReplicates() throws Exception {
    final int leaderIndex = leaderIndex();

    createDatabaseOverHttp(leaderIndex, HTTP_DROPPED_DB);
    awaitDatabaseOnEveryServer(HTTP_DROPPED_DB, leaderIndex, "created over HTTP");

    serverCommandOnLeader(leaderIndex, "drop database " + HTTP_DROPPED_DB);

    awaitDatabaseGoneFromEveryServer(HTTP_DROPPED_DB, leaderIndex, "dropped over HTTP");
  }

  // ------------------------------------------------------------------------------------------
  // Helpers
  // ------------------------------------------------------------------------------------------

  private int leaderIndex() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);
    return leaderIndex;
  }

  private ArcadeDbAdminServiceGrpc.ArcadeDbAdminServiceBlockingStub adminStubOn(final int serverIndex) {
    channel = ManagedChannelBuilder.forAddress("localhost", grpcPorts[serverIndex]).usePlaintext().build();
    return ArcadeDbAdminServiceGrpc.newBlockingStub(channel);
  }

  private DatabaseCredentials credentials() {
    return DatabaseCredentials.newBuilder()
        .setUsername("root")
        .setPassword(DEFAULT_PASSWORD_FOR_TESTS)
        .build();
  }

  private void createDatabaseOverHttp(final int leaderIndex, final String databaseName) throws Exception {
    serverCommandOnLeader(leaderIndex, "create database " + databaseName);
  }

  private void serverCommandOnLeader(final int leaderIndex, final String command) throws Exception {
    final int httpPort = getServer(leaderIndex).getHttpServer().getPort();
    final HttpURLConnection connection = (HttpURLConnection) new URI(
        "http://127.0.0.1:" + httpPort + "/api/v1/server").toURL().openConnection();
    try {
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      connection.setDoOutput(true);

      final byte[] payload = new JSONObject().put("command", command).toString().getBytes(StandardCharsets.UTF_8);
      connection.setRequestProperty("Content-Length", Integer.toString(payload.length));
      try (final DataOutputStream out = new DataOutputStream(connection.getOutputStream())) {
        out.write(payload);
      }

      connection.connect();
      assertThat(connection.getResponseCode()).as("'%s' over HTTP must succeed", command).isEqualTo(200);
    } finally {
      connection.disconnect();
    }
  }

  private void awaitDatabaseOnEveryServer(final String databaseName, final int leaderIndex, final String how)
      throws InterruptedException {
    for (int i = 0; i < getServerCount(); i++) {
      final int serverIndex = i;
      final boolean present = await(() -> getServer(serverIndex).existsDatabase(databaseName));
      assertThat(present)
          .as("Database '%s' %s on the leader (server %d) must exist on server %d too", databaseName, how, leaderIndex,
              serverIndex)
          .isTrue();
    }
  }

  private void awaitDatabaseGoneFromEveryServer(final String databaseName, final int leaderIndex, final String how)
      throws InterruptedException {
    for (int i = 0; i < getServerCount(); i++) {
      final int serverIndex = i;
      final boolean gone = await(() -> !getServer(serverIndex).existsDatabase(databaseName));
      assertThat(gone)
          .as("Database '%s' %s on the leader (server %d) must be gone from server %d too", databaseName, how,
              leaderIndex, serverIndex)
          .isTrue();
    }
  }

  /**
   * Polls until the condition holds or the propagation budget runs out. The budget is a tripwire
   * between a replicated operation and a purely local one, not a latency bound: a local-only
   * create or drop never satisfies the condition however long the poll waits.
   */
  private boolean await(final java.util.function.BooleanSupplier condition) throws InterruptedException {
    final long deadline = System.currentTimeMillis() + PROPAGATION_WAIT;
    while (System.currentTimeMillis() < deadline) {
      if (condition.getAsBoolean())
        return true;
      Thread.sleep(200);
    }
    return condition.getAsBoolean();
  }

  private static int allocateFreePort() {
    try (final ServerSocket socket = new ServerSocket(0)) {
      socket.setReuseAddress(true);
      return socket.getLocalPort();
    } catch (final IOException e) {
      throw new IllegalStateException("Cannot allocate a free gRPC port for the test", e);
    }
  }
}
