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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerDatabase;
import com.arcadedb.utility.FileUtils;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10533, through a real call: a gRPC client that names a database an {@code overwrite} restore holds, or that a
 * {@code close database} closed, is refused with {@code UNAVAILABLE} - the database exists, so it is neither {@code NOT_FOUND} nor
 * a server fault ({@code INTERNAL}) - and the same call opens it once nothing holds the name.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue10533GrpcHeldDatabaseIT extends BaseGrpcServerTest {
  private static final String HELD = "grpcheld10533";

  private ManagedChannel                                channel;
  private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub stub;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @BeforeEach
  void openChannel() {
    channel = ManagedChannelBuilder.forAddress("localhost", getServerGrpcPort()).usePlaintext().build();
    stub = ArcadeDbServiceGrpc.newBlockingStub(
        ClientInterceptors.intercept(channel, new GrpcTestAuthInterceptor("root", DEFAULT_PASSWORD_FOR_TESTS, getDatabaseName())));
  }

  @AfterEach
  void closeChannel() throws InterruptedException {
    if (channel != null) {
      channel.shutdown();
      channel.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  @Test
  void aCallOnADatabaseARestoreHoldsIsUnavailable() {
    final ArcadeDBServer server = getServer(0);
    onDiskNotOpen(server);

    server.reserveDatabaseNameForRestore(HELD);
    try {
      assertThatThrownBy(() -> stub.executeQuery(query(HELD))).isInstanceOfSatisfying(StatusRuntimeException.class,
          e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.UNAVAILABLE));
      assertThat(server.existsDatabase(HELD)).as("the call opened nothing").isFalse();
    } finally {
      server.releaseDatabaseNameReservedForRestore(HELD);
    }

    try {
      assertThat(stub.executeQuery(query(HELD))).as("a released reservation holds nothing").isNotNull();
    } finally {
      dropIfPresent(server);
    }
  }

  @Test
  void aCallOnADatabaseACloseCommandClosedIsUnavailable() {
    final ArcadeDBServer server = getServer(0);
    onDiskNotOpen(server);

    server.markClosedByCommand(HELD);
    try {
      assertThatThrownBy(() -> stub.executeQuery(query(HELD))).isInstanceOfSatisfying(StatusRuntimeException.class,
          e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.UNAVAILABLE));
    } finally {
      server.unmarkClosedByCommand(HELD);
      dropIfPresent(server);
    }
  }

  private static ExecuteQueryRequest query(final String database) {
    return ExecuteQueryRequest.newBuilder().setDatabase(database).setQuery("select 1 as one")
        .setCredentials(DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build()).build();
  }

  /** Leaves {@link #HELD} on disk, closed and deregistered: what an {@code overwrite} restore finds on its target. */
  private static void onDiskNotOpen(final ArcadeDBServer server) {
    final ServerDatabase created = server.createDatabase(HELD, ComponentFile.MODE.READ_WRITE);
    created.getEmbedded().close();
    server.removeDatabase(HELD);
    assertThat(server.existsDatabase(HELD)).isFalse();
  }

  private static void dropIfPresent(final ArcadeDBServer server) {
    if (server.existsDatabase(HELD))
      server.dropLocalDatabase(server.getDatabaseForOperator(HELD).getEmbedded(), HELD);
    FileUtils.deleteRecursively(
        new File(server.getConfiguration().getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY), HELD));
  }
}
