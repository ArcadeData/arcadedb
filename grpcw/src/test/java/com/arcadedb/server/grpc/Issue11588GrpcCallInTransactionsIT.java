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
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11588 over gRPC: CALL { ... } IN TRANSACTIONS keeps running in batches in the transaction
 * {@code executeCommandWithRetries} opens around an auto-commit ExecuteCommand, an implicit transaction, and is refused
 * inside a transaction the client opened with BeginTransaction, whose rollback could not undo the batches.
 */
public class Issue11588GrpcCallInTransactionsIT extends BaseGrpcServerTest {
  private static final String STATEMENT = "UNWIND range(1, 25) AS i CALL { WITH i CREATE (:%s {i: i}) } IN TRANSACTIONS OF 10 ROWS";

  private ManagedChannel                                  channel;
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
  void runsInBatchesInTheAutoCommitTransactionOfExecuteCommand() {
    final String label = "GrpcAutoCommit11588";
    final ExecuteCommandResponse response = stub.executeCommand(request(STATEMENT.formatted(label)).build());

    assertThat(response.getSuccess()).isTrue();
    assertThat(count(label)).isEqualTo(25);
  }

  @Test
  void refusedInTheTransactionTheClientBegan() {
    final String label = "GrpcClientTx11588";
    final String txId = stub.beginTransaction(
        BeginTransactionRequest.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials()).build()).getTransactionId();
    final TransactionContext tx = TransactionContext.newBuilder().setTransactionId(txId).setDatabase(getDatabaseName()).build();

    assertThatThrownBy(() -> stub.executeCommand(request(STATEMENT.formatted(label)).setTransaction(tx).build()))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("can only be executed in an implicit transaction");

    stub.rollbackTransaction(RollbackTransactionRequest.newBuilder().setTransaction(tx).setCredentials(credentials()).build());
    assertThat(count(label)).isZero();
  }

  private long count(final String label) {
    return getServerDatabase(0, getDatabaseName()).query("opencypher", "MATCH (x:" + label + ") RETURN count(x) AS c").next()
        .<Number>getProperty("c").longValue();
  }

  private ExecuteCommandRequest.Builder request(final String command) {
    return ExecuteCommandRequest.newBuilder().setDatabase(getDatabaseName()).setLanguage("opencypher").setCommand(command)
        .setCredentials(credentials());
  }

  private static DatabaseCredentials credentials() {
    return DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build();
  }
}
