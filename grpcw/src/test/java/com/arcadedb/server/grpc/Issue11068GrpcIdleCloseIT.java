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
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.DatabaseIdleCloser;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11068 over gRPC: a database the server closed for idleness is reopened by the next RPC, from the admin service's listing
 * and existence check too, and a client transaction open on it - which lives on its own executor thread between two RPCs - keeps
 * it open until the client ends it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue11068GrpcIdleCloseIT extends BaseGrpcServerTest {
  private ManagedChannel                                  channel;
  private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub stub;
  private DatabaseCredentials                             credentials;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    // Driven by the test: the scheduled check waits an hour
    config.setValue(GlobalConfiguration.SERVER_DATABASE_IDLE_CHECK_INTERVAL, TimeUnit.HOURS.toMillis(1));
  }

  @BeforeEach
  void connect() {
    channel = ManagedChannelBuilder.forAddress("localhost", getServerGrpcPort()).usePlaintext().build();
    final Metadata headers = new Metadata();
    headers.put(Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER), "root");
    headers.put(Metadata.Key.of("x-arcade-password", Metadata.ASCII_STRING_MARSHALLER), DEFAULT_PASSWORD_FOR_TESTS);
    headers.put(Metadata.Key.of("x-arcade-database", Metadata.ASCII_STRING_MARSHALLER), getDatabaseName());
    stub = ArcadeDbServiceGrpc.newBlockingStub(channel).withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));
    credentials = DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build();
  }

  @AfterEach
  void disconnectAndReopen() throws InterruptedException {
    if (channel != null) {
      channel.shutdown();
      channel.awaitTermination(5, TimeUnit.SECONDS);
    }
    final ArcadeDBServer server = getServer(0);
    if (server != null && server.isStarted()) {
      server.getConfiguration().setValue(GlobalConfiguration.SERVER_DATABASE_IDLE_TIMEOUT, 0L);
      server.getDatabaseForOperator(getDatabaseName());
    }
  }

  @Test
  void theNextRpcReopensADatabaseClosedForIdleness() throws Exception {
    assertThat(count()).isGreaterThan(0);
    assertThat(closeIdle()).isEqualTo(1);
    assertThat(getServer(0).existsDatabase(getDatabaseName())).isFalse();

    final ArcadeDbAdminServiceGrpc.ArcadeDbAdminServiceBlockingStub admin = ArcadeDbAdminServiceGrpc.newBlockingStub(channel);
    assertThat(admin.listDatabases(ListDatabasesRequest.newBuilder().setCredentials(credentials).build()).getDatabasesList())
        .contains(getDatabaseName());
    assertThat(admin.existsDatabase(
        ExistsDatabaseRequest.newBuilder().setCredentials(credentials).setName(getDatabaseName()).build()).getExists()).isTrue();

    assertThat(count()).isGreaterThan(0);
    assertThat(getServer(0).existsDatabase(getDatabaseName())).isTrue();
  }

  @Test
  void aClientTransactionKeepsTheDatabaseOpen() throws Exception {
    final String transactionId = stub.beginTransaction(
        BeginTransactionRequest.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials).build()).getTransactionId();
    try {
      assertThat(closeIdle()).as("a quiet client transaction is still a transaction").isZero();
      assertThat(getServer(0).existsDatabase(getDatabaseName())).isTrue();
    } finally {
      assertThat(stub.rollbackTransaction(RollbackTransactionRequest.newBuilder().setCredentials(credentials).setTransaction(
          TransactionContext.newBuilder().setTransactionId(transactionId).setDatabase(getDatabaseName()).build()).build())
          .getSuccess()).isTrue();
    }
    assertThat(closeIdle()).isEqualTo(1);
  }

  private long count() {
    final ExecuteQueryResponse response = stub.executeQuery(ExecuteQueryRequest.newBuilder().setDatabase(getDatabaseName())
        .setCredentials(credentials).setQuery("SELECT count(*) AS n FROM V1").build());
    return response.getResults(0).getRecords(0).getPropertiesMap().get("n").getInt64Value();
  }

  /**
   * Checks a little apart, until one closes something: the first samples the database, a later one finds it unused past a 1 ms
   * timeout. Several, so that background work landing between two of them delays the close instead of failing the test.
   */
  private int closeIdle() throws InterruptedException {
    getServer(0).getConfiguration().setValue(GlobalConfiguration.SERVER_DATABASE_IDLE_TIMEOUT, 1L);
    final DatabaseIdleCloser closer = getServer(0).getDatabaseIdleCloser();
    int closed = closer.check();
    for (int attempt = 0; attempt < 10 && closed == 0; attempt++) {
      Thread.sleep(20);
      closed += closer.check();
    }
    return closed;
  }
}
