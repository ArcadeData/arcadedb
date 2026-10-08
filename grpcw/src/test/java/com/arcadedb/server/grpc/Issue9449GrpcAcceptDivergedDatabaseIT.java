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
 * Issue #9449 over gRPC: {@code AcceptDivergedDatabase} is the twin of {@code POST /api/v1/cluster/accept-diverged/{database}}
 * and reaches the same {@code HAServerPlugin.acceptDivergedDatabase}, on a real one-voter Raft cluster.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue9449GrpcAcceptDivergedDatabaseIT extends BaseRaftHATest {

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
  void theRpcLiftsAStandingQuarantineOnASoleVoter() {
    quarantineDatabase(0, getDatabaseName());

    final AcceptDivergedDatabaseResponse response = accept(root(), getDatabaseName());

    assertThat(response.getDatabase()).isEqualTo(getDatabaseName());
    assertThat(response.getDivergenceCause()).isEqualTo("APPLY_ERROR");
    assertThat(response.getLocalServer()).isEqualTo(getServer(0).getServerName());
    assertThat(response.getReadFloor()).as("no read floor stood").isEqualTo(-1L);
    assertThat(isDatabaseQuarantined(0, getDatabaseName())).isFalse();

    assertThatThrownBy(() -> accept(root(), getDatabaseName()))
        .as("nothing is left to accept")
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("NOT_FOUND");
  }

  @Test
  @Timeout(120)
  void theRpcIsRootOnlyAndValidatesTheName() {
    quarantineDatabase(0, getDatabaseName());

    assertThatThrownBy(() -> accept(DatabaseCredentials.newBuilder().setUsername("root").setPassword("wrong").build(),
        getDatabaseName()))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("UNAUTHENTICATED");
    assertThat(isDatabaseQuarantined(0, getDatabaseName())).as("a refused caller lifts nothing").isTrue();

    assertThatThrownBy(() -> accept(root(), "../etc"))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("INVALID_ARGUMENT");
  }

  private AcceptDivergedDatabaseResponse accept(final DatabaseCredentials credentials, final String database) {
    return adminStub.acceptDivergedDatabase(
        AcceptDivergedDatabaseRequest.newBuilder().setCredentials(credentials).setDatabase(database).build());
  }

  private static DatabaseCredentials root() {
    return DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build();
  }
}
