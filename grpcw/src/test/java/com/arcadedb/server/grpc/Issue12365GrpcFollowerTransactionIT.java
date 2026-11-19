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
import com.arcadedb.query.sql.executor.ResultSet;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * Issue #12365 over gRPC: a client was told its commit failed with a {@code ConcurrentModificationException} while the write it
 * carried was on every node. Up to 26.10.1 a write sent to a FOLLOWER inside a transaction the client opened with
 * {@code BeginTransaction} was forwarded to the leader and committed there on its own, at once, so neither the refusal of the
 * later {@code CommitTransaction} nor a {@code RollbackTransaction} could take it back. Since #10040 it runs in the client's
 * transaction on the follower: these pin that for gRPC, which had no test of it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue12365GrpcFollowerTransactionIT extends BaseRaftHATest {
  private static final String LEDGER  = "Issue12365Ledger";
  private static final String ACCOUNT = "Issue12365Account";

  private static final Metadata.Key<String> USER_HEADER     = Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> PASSWORD_HEADER = Metadata.Key.of("x-arcade-password", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> DATABASE_HEADER = Metadata.Key.of("x-arcade-database", Metadata.ASCII_STRING_MARSHALLER);

  // One gRPC port per server, drawn from the same ledger as the Raft ports (issues #7496, #8203)
  private final int[] grpcPorts = allocateFixturePorts(3);

  private ManagedChannel                              channel;
  private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub stub;

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    final int index = serverIndexOf(config);
    config.setValue("arcadedb.grpc.enabled", "true");
    config.setValue("arcadedb.grpc.port", String.valueOf(grpcPorts[index]));
    config.setValue("arcadedb.grpc.host", "localhost");
    config.setValue("arcadedb.grpc.reflection.enabled", "false");
    config.setValue("arcadedb.grpc.health.enabled", "false");
    final String plugins = config.getValueAsString(GlobalConfiguration.SERVER_PLUGINS);
    final String entry = "GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin";
    if (plugins == null || plugins.isEmpty())
      config.setValue(GlobalConfiguration.SERVER_PLUGINS, entry);
    else if (!plugins.contains(entry))
      config.setValue(GlobalConfiguration.SERVER_PLUGINS, plugins + "," + entry);
  }

  @Override
  protected int getServerCount() {
    return 3;
  }

  @AfterEach
  void closeChannel() throws InterruptedException {
    if (channel != null) {
      channel.shutdown();
      channel.awaitTermination(5, TimeUnit.SECONDS);
      channel = null;
    }
  }

  /** The deterministic shape: a rolled back transaction of a follower leaves nothing on any node. */
  @Test
  @Timeout(180)
  void aRollbackOnAFollowerUndoesTheWritesOfTheTransaction() {
    final int leader = requireLeaderIndex();
    createTypes(leader);
    connect(firstFollowerOf(leader));

    final String txId = begin();
    execute(txId, "INSERT INTO " + LEDGER + " SET key = 'rolled-back'");
    assertThat(count(leader, LEDGER, "rolled-back")).as("nothing reaches the leader before the transaction ends").isZero();
    stub.rollbackTransaction(RollbackTransactionRequest.newBuilder().setCredentials(credentials()).setTransaction(txRef(txId)).build());

    waitForAllServers();
    for (int i = 0; i < getServerCount(); i++)
      assertThat(count(i, LEDGER, "rolled-back")).as("server %d holds no row of the rolled back transaction", i).isZero();

    final String committed = begin();
    execute(committed, "INSERT INTO " + LEDGER + " SET key = 'committed'");
    final CommitTransactionResponse ok = stub.commitTransaction(
        CommitTransactionRequest.newBuilder().setCredentials(credentials()).setTransaction(txRef(committed)).build());
    assertThat(ok.getCommitted()).isTrue();

    waitForAllServers();
    for (int i = 0; i < getServerCount(); i++)
      assertThat(count(i, LEDGER, "committed")).as("server %d holds the committed row", i).isEqualTo(1L);
  }

  /**
   * The reporter's shape: the transaction writes a ledger row and updates a record that another writer updates before the
   * commit. The commit is refused as a conflict, and then none of the transaction is anywhere, its ledger row included.
   */
  @Test
  @Timeout(180)
  void aCommitRefusedAsAConflictLeavesNoWriteOfTheTransactionOnAnyNode() {
    final int leader = requireLeaderIndex();
    createTypes(leader);
    final Database leaderDb = getServerDatabase(leader, getDatabaseName());
    leaderDb.transaction(() -> leaderDb.command("sql", "INSERT INTO " + ACCOUNT + " SET id = 1, balance = 100"));
    waitForAllServers();
    connect(firstFollowerOf(leader));

    final String txId = begin();
    execute(txId, "INSERT INTO " + LEDGER + " SET key = 'conflicting'");
    execute(txId, "UPDATE " + ACCOUNT + " SET balance = balance + 1 WHERE id = 1");

    // a concurrent writer moves the same record and commits first
    leaderDb.transaction(() -> leaderDb.command("sql", "UPDATE " + ACCOUNT + " SET balance = 500 WHERE id = 1"));
    waitForAllServers();

    final StatusRuntimeException refused = catchThrowableOfType(StatusRuntimeException.class, () -> stub.commitTransaction(
        CommitTransactionRequest.newBuilder().setCredentials(credentials()).setTransaction(txRef(txId)).build()));
    assertThat(refused).as("the commit is refused").isNotNull();
    assertThat(refused.getStatus().getCode()).isEqualTo(Status.Code.ABORTED);

    waitForAllServers();
    for (int i = 0; i < getServerCount(); i++) {
      assertThat(count(i, LEDGER, "conflicting")).as("server %d holds no row of the refused transaction", i).isZero();
      try (final ResultSet rs = getServerDatabase(i, getDatabaseName()).query("sql", "SELECT balance FROM " + ACCOUNT + " WHERE id = 1")) {
        assertThat(rs.next().<Number>getProperty("balance").intValue()).as("server %d keeps the other writer's update", i).isEqualTo(500);
      }
    }
  }

  private void createTypes(final int leader) {
    final Database leaderDb = getServerDatabase(leader, getDatabaseName());
    leaderDb.command("sql", "CREATE DOCUMENT TYPE " + LEDGER + " IF NOT EXISTS");
    leaderDb.command("sql", "CREATE DOCUMENT TYPE " + ACCOUNT + " IF NOT EXISTS");
    waitForAllServers();
  }

  private void connect(final int serverIndex) {
    channel = ManagedChannelBuilder.forAddress("localhost", grpcPorts[serverIndex]).usePlaintext().build();
    final Channel authenticated = ClientInterceptors.intercept(channel, new AuthClientInterceptor());
    stub = ArcadeDbServiceGrpc.newBlockingStub(authenticated);
  }

  private String begin() {
    return stub.beginTransaction(BeginTransactionRequest.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials()).build())
        .getTransactionId();
  }

  private void execute(final String txId, final String sql) {
    stub.executeCommand(ExecuteCommandRequest.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials()).setLanguage("sql")
        .setCommand(sql).setTransaction(txRef(txId)).build());
  }

  private TransactionContext txRef(final String txId) {
    return TransactionContext.newBuilder().setTransactionId(txId).setDatabase(getDatabaseName()).build();
  }

  private DatabaseCredentials credentials() {
    return DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build();
  }

  private long count(final int serverIndex, final String type, final String key) {
    try (final ResultSet rs = getServerDatabase(serverIndex, getDatabaseName()).query("sql",
        "SELECT count(@rid) AS c FROM " + type + " WHERE key = ?", key)) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  private final class AuthClientInterceptor implements ClientInterceptor {
    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(final MethodDescriptor<ReqT, RespT> method, final CallOptions callOptions,
        final Channel next) {
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
