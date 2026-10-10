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
import com.arcadedb.exception.QueryTerminatedException;
import com.arcadedb.query.RunningQuery;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Issue #9689 over gRPC: an RPC that runs a statement is listed in the server's running statements, a terminate stops it
 * with {@code CANCELLED}, a client that gives up on the call (here: its deadline expires) stops the work on the server
 * too, and a statement stopped inside a client transaction ends the transaction, so its writes cannot be committed later.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue9689GrpcRunningQueryIT extends BaseGrpcServerTest {
  /** About 16 s on one core when left alone, all of it inside one aggregation. */
  private static final String LONG_CYPHER =
      "UNWIND range(1, 10000) AS i UNWIND range(1, 10000) AS j RETURN sum(sin(toFloat(i * j))) AS total";

  private static final Metadata.Key<String> USER_HEADER     = Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> PASSWORD_HEADER = Metadata.Key.of("x-arcade-password", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> DATABASE_HEADER = Metadata.Key.of("x-arcade-database", Metadata.ASCII_STRING_MARSHALLER);

  private ManagedChannel                                  channel;
  private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub stub;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @BeforeEach
  void setupGrpcClient() {
    channel = ManagedChannelBuilder.forAddress("localhost", getServerGrpcPort()).usePlaintext().build();
    final Channel authenticated = ClientInterceptors.intercept(channel, new ClientInterceptor() {
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
    });
    stub = ArcadeDbServiceGrpc.newBlockingStub(authenticated);
  }

  @AfterEach
  void teardownGrpcClient() throws InterruptedException {
    if (channel != null) {
      channel.shutdown();
      channel.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  @Test
  void aTerminatedRpcIsCancelled() throws Exception {
    final CompletableFuture<ExecuteQueryResponse> running = CompletableFuture.supplyAsync(() -> stub.executeQuery(longQuery(null)));

    final RunningQuery entry = awaitRunning();
    assertThat(entry.getProtocol()).isEqualTo("grpc");
    assertThat(entry.getUser()).isEqualTo("root");
    assertThat(entry.getDatabase()).isEqualTo(getDatabaseName());
    assertThat(entry.getLanguage()).isEqualTo("opencypher");

    entry.terminate("root");
    assertThatThrownBy(() -> running.get(30, TimeUnit.SECONDS)).satisfies(e -> {
      assertThat(e.getCause()).isInstanceOf(StatusRuntimeException.class);
      assertThat(((StatusRuntimeException) e.getCause()).getStatus().getCode()).isEqualTo(Status.Code.CANCELLED);
      // The trailer the Java client rebuilds the typed exception from
      assertThat(Status.trailersFromThrowable(e.getCause()).get(GrpcErrorMapper.EXCEPTION_CLASS_KEY))
          .isEqualTo(QueryTerminatedException.class.getName());
    });
    assertThat(entry.awaitEnd(30_000)).isTrue();
    assertThat(entry.getOutcome()).isEqualTo(RunningQuery.Outcome.TERMINATED);
  }

  @Test
  void aClientThatGivesUpStopsTheWorkOnTheServer() throws Exception {
    // The client's deadline expires long before the statement would end: the server stops working on it as well
    assertThatThrownBy(() -> stub.withDeadlineAfter(1, TimeUnit.SECONDS).executeQuery(longQuery(null)))
        .isInstanceOf(StatusRuntimeException.class)
        .satisfies(e -> assertThat(((StatusRuntimeException) e).getStatus().getCode()).isEqualTo(Status.Code.DEADLINE_EXCEEDED));

    await().atMost(Duration.ofSeconds(30)).until(() -> findRunning() == null);
  }

  @Test
  void aStatementStoppedInAClientTransactionEndsTheTransaction() throws Exception {
    final String txId = stub.beginTransaction(
        BeginTransactionRequest.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials()).build()).getTransactionId();
    final TransactionContext tx = TransactionContext.newBuilder().setTransactionId(txId).setDatabase(getDatabaseName()).build();

    stub.executeCommand(ExecuteCommandRequest.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials())
        .setLanguage("sql").setCommand("CREATE DOCUMENT TYPE Written9689 IF NOT EXISTS").build());
    stub.executeCommand(ExecuteCommandRequest.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials())
        .setLanguage("sql").setCommand("INSERT INTO Written9689 SET x = 1").setTransaction(tx).build());

    final CompletableFuture<ExecuteQueryResponse> running = CompletableFuture.supplyAsync(() -> stub.executeQuery(longQuery(tx)));
    final RunningQuery entry = awaitRunning();
    entry.terminate("root");
    assertThatThrownBy(() -> running.get(30, TimeUnit.SECONDS)).satisfies(
        e -> assertThat(((StatusRuntimeException) e.getCause()).getStatus().getCode()).isEqualTo(Status.Code.CANCELLED));

    // The transaction went with the statement: a commit finds nothing to commit, and what it wrote is not there
    assertThat(stub.commitTransaction(CommitTransactionRequest.newBuilder().setTransaction(tx).setCredentials(credentials()).build())
        .getCommitted()).isFalse();
    assertThat(entry.getOutcome()).isEqualTo(RunningQuery.Outcome.TERMINATED);
    assertThat(stub.executeQuery(ExecuteQueryRequest.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials())
        .setQuery("SELECT count(*) AS c FROM Written9689").build()).getResults(0).getRecords(0).getPropertiesMap().get("c")
        .getInt64Value()).isZero();
  }

  private RunningQuery awaitRunning() {
    final RunningQuery[] found = new RunningQuery[1];
    await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(20)).until(() -> (found[0] = findRunning()) != null);
    return found[0];
  }

  private RunningQuery findRunning() {
    for (final RunningQuery q : getServer(0).getRunningQueries().getRunning())
      if (LONG_CYPHER.equals(q.getText()))
        return q;
    return null;
  }

  private ExecuteQueryRequest longQuery(final TransactionContext tx) {
    final ExecuteQueryRequest.Builder builder = ExecuteQueryRequest.newBuilder().setDatabase(getDatabaseName())
        .setCredentials(credentials()).setLanguage("opencypher").setQuery(LONG_CYPHER);
    if (tx != null)
      builder.setTransaction(tx);
    return builder.build();
  }

  private DatabaseCredentials credentials() {
    return DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build();
  }
}
