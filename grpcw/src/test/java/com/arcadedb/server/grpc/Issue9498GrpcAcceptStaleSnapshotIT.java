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
import com.arcadedb.server.ha.raft.BaseRaftHATest;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9498 over gRPC: {@code AcceptStaleSnapshot} is the twin of {@code POST /api/v1/cluster/accept-stale-snapshot} and
 * reaches the same {@code HAServerPlugin.acceptStaleSnapshot}, on a real one-voter Raft cluster.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue9498GrpcAcceptStaleSnapshotIT extends BaseRaftHATest {

  private final int[] grpcPorts = allocateFixturePorts(1);

  private ManagedChannel                                            channel;
  private ArcadeDbAdminServiceGrpc.ArcadeDbAdminServiceBlockingStub adminStub;

  @Override
  protected int getServerCount() {
    return 1;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    // The base class turns HA on only for two servers or more; this cluster is one voter on purpose
    config.setValue(GlobalConfiguration.HA_SERVER_LIST, getServerAddresses());
    config.setValue(GlobalConfiguration.HA_ENABLED, true);

    config.setValue("arcadedb.grpc.enabled", "true");
    config.setValue("arcadedb.grpc.port", String.valueOf(grpcPorts[0]));
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

  @BeforeEach
  void openChannel() {
    channel = ManagedChannelBuilder.forAddress("localhost", grpcPorts[0]).usePlaintext().build();
    adminStub = ArcadeDbAdminServiceGrpc.newBlockingStub(channel);
  }

  @AfterEach
  void closeChannel() throws InterruptedException {
    if (channel != null) {
      channel.shutdown();
      channel.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  @Test
  @Timeout(120)
  void theRpcLiftsTheFloorOnASoleVoter() {
    raiseStaleSnapshotFloor(0, 0);

    final AcceptStaleSnapshotResponse response = accept(root());

    assertThat(response.getLocalServer()).isEqualTo(getServer(0).getServerName());
    assertThat(response.getReadFloor()).isEqualTo(0L);
    assertThat(getStaleSnapshotFloor(0)).isNegative();

    assertThatThrownBy(() -> accept(root()))
        .as("nothing is left to accept")
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("NOT_FOUND");
  }

  @Test
  @Timeout(120)
  void theRpcIsAuthenticated() {
    raiseStaleSnapshotFloor(0, 0);

    assertThatThrownBy(() -> accept(DatabaseCredentials.newBuilder().setUsername("root").setPassword("wrong").build()))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("UNAUTHENTICATED");
    assertThat(getStaleSnapshotFloor(0)).as("a refused caller lifts nothing").isEqualTo(0L);
  }

  private AcceptStaleSnapshotResponse accept(final DatabaseCredentials credentials) {
    return adminStub.acceptStaleSnapshot(AcceptStaleSnapshotRequest.newBuilder().setCredentials(credentials).build());
  }

  private static DatabaseCredentials root() {
    return DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build();
  }
}
