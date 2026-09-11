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
import com.arcadedb.database.Database;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.ProtocolContext;
import com.arcadedb.query.sql.SQLQueryEngine;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.function.sql.SQLFunctionAbstract;
import com.arcadedb.server.BaseGraphServerTest;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7394 item 1: the vector, hybrid and full-text search RPCs ran their SQL without establishing the
 * protocol context, so nothing that reads {@link ProtocolContext} on that thread could tell the traffic came
 * from gRPC.
 * <p>
 * The assertion is made from inside the SQL itself, by a custom function called from the vector leg's own
 * {@code filter} predicate that records what {@code ProtocolContext.get()} answers on the thread evaluating
 * it. That is the exact invariant - the thread that runs the search reports "grpc" - and it is the only way
 * to see it today: the search legs execute through {@code AnalyzedQuery.execute()} rather than
 * {@code Database.query}, so {@code arcadedb.query.duration} does not fire for them on ANY protocol (see
 * issue #7418) and a metric-count assertion would pass or fail for reasons unrelated to this fix.
 * <p>
 * The in-transaction case is the one that makes an integration test worth its cost. {@code ProtocolContext}
 * is a thread-local, and a search naming a live transaction runs on that transaction's own executor thread
 * (issue #7326), not on the calling gRPC worker - so a fix applied at the RPC method would have left exactly
 * that path untagged, and only really dispatching through the transaction registry shows it.
 *
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/7394">issue #7394</a>
 */
public class Issue7394SearchProtocolContextIT extends BaseGraphServerTest {

  private static final int    GRPC_PORT    = 50051;
  private static final String VECTOR_TYPE  = "Vec7394";
  private static final String VECTOR_INDEX = VECTOR_TYPE + "[embedding]";

  /** What {@code ProtocolContext.get()} answered on the thread that evaluated the search's SQL. */
  private static final AtomicReference<String> OBSERVED_PROTOCOL = new AtomicReference<>();

  private static final Metadata.Key<String> USER_HEADER     =
      Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> PASSWORD_HEADER =
      Metadata.Key.of("x-arcade-password", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> DATABASE_HEADER =
      Metadata.Key.of("x-arcade-database", Metadata.ASCII_STRING_MARSHALLER);

  private ManagedChannel                                  channel;
  private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub stub;

  /** The gRPC auth interceptor authenticates from headers, so every call carries them (see GrpcQueryMetricsIT). */
  private class AuthClientInterceptor implements ClientInterceptor {
    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
        final MethodDescriptor<ReqT, RespT> method, final CallOptions callOptions, final Channel next) {
      return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(next.newCall(method, callOptions)) {
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

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GRPC:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @Override
  protected void populateDatabase() {
    super.populateDatabase();

    final Database db = getDatabase(0);
    db.transaction(() -> {
      db.command("sql", "CREATE VERTEX TYPE " + VECTOR_TYPE);
      db.command("sql", "CREATE PROPERTY " + VECTOR_TYPE + ".embedding ARRAY_OF_FLOATS");
      db.command("sql", "CREATE INDEX ON " + VECTOR_TYPE + " (embedding) LSM_VECTOR "
          + "METADATA { dimensions: 3, similarity: 'COSINE' }");

      db.newVertex(VECTOR_TYPE).set("embedding", new float[] { 1.0f, 0.0f, 0.0f }).save();
      db.newVertex(VECTOR_TYPE).set("embedding", new float[] { 0.0f, 1.0f, 0.0f }).save();
    });

    // Registered on the reusable SQL engine of the server's own database, which is the very instance the gRPC
    // worker and the transaction executor resolve (LocalDatabase caches reusable engines per database), so the
    // function is in scope wherever the search actually runs.
    ((SQLQueryEngine) db.getQueryEngine("sql")).getFunctionFactory().register(
        new SQLFunctionAbstract("observedprotocol") {
          @Override
          public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult,
              final Object[] params, final CommandContext context) {
            OBSERVED_PROTOCOL.set(ProtocolContext.get());
            return ProtocolContext.get();
          }

          @Override
          public String getSyntax() {
            return "records ProtocolContext.get() on the evaluating thread and returns it";
          }
        });
  }

  @BeforeEach
  void setupGrpcClient() {
    OBSERVED_PROTOCOL.set(null);
    channel = ManagedChannelBuilder.forAddress("localhost", GRPC_PORT).usePlaintext().build();
    stub = ArcadeDbServiceGrpc.newBlockingStub(
        ClientInterceptors.intercept(channel, new AuthClientInterceptor()));
  }

  @AfterEach
  void teardownGrpcClient() throws InterruptedException {
    if (channel != null) {
      channel.shutdown();
      channel.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  private DatabaseCredentials root() {
    return DatabaseCredentials.newBuilder()
        .setUsername("root")
        .setPassword(DEFAULT_PASSWORD_FOR_TESTS)
        .build();
  }

  /**
   * The predicate is always true, so it never changes which records the search answers - it is there to be
   * evaluated, on the thread the search runs on.
   */
  private VectorSearchRequest.Builder vectorRequest() {
    return VectorSearchRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(root())
        .setIndexName(VECTOR_INDEX)
        .addAllQueryVector(List.of(1.0f, 0.0f, 0.0f))
        .setK(2)
        .setFilter("observedprotocol() IS NOT NULL");
  }

  @Test
  void vectorSearchRunsItsSqlUnderTheGrpcProtocolTag() {
    stub.vectorSearch(vectorRequest().build());

    assertThat(OBSERVED_PROTOCOL.get()).isEqualTo("grpc");
  }

  @Test
  void hybridSearchRunsItsSqlUnderTheGrpcProtocolTag() {
    stub.hybridSearch(HybridSearchRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(root())
        .setVectorIndexName(VECTOR_INDEX)
        .addAllQueryVector(List.of(1.0f, 0.0f, 0.0f))
        .setK(2)
        .setFilter("observedprotocol() IS NOT NULL")
        .build());

    assertThat(OBSERVED_PROTOCOL.get()).isEqualTo("grpc");
  }

  /**
   * The path the thread-local placement exists for: the body runs on the transaction's executor thread, so
   * the tag has to be set there and not on the gRPC worker that dispatched it.
   */
  @Test
  void aVectorSearchInsideAClientTransactionRunsItsSqlUnderTheGrpcProtocolTagToo() {
    final BeginTransactionResponse tx = stub.beginTransaction(BeginTransactionRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(root())
        .build());

    try {
      stub.vectorSearch(vectorRequest()
          .setTransaction(TransactionContext.newBuilder().setTransactionId(tx.getTransactionId()).build())
          .build());

      assertThat(OBSERVED_PROTOCOL.get()).isEqualTo("grpc");
    } finally {
      stub.rollbackTransaction(RollbackTransactionRequest.newBuilder()
          .setCredentials(root())
          .setTransaction(TransactionContext.newBuilder().setTransactionId(tx.getTransactionId()).build())
          .build());
    }
  }

  /**
   * The control: the same SQL, run by the embedded API on a thread no wire protocol tagged, still reports
   * {@code internal}. Without this the test would pass just as well against a {@code ProtocolContext} that
   * answered "grpc" for everything.
   */
  @Test
  void theSameSqlRunOutsideAnyProtocolStillReportsInternal() {
    // The server's own live handle: the one populateDatabase() used is closed by the time a test body runs.
    // The result set is lazy, so it has to be consumed or the projection - and the function in it - never runs.
    try (final ResultSet rs = getServer(0).getDatabase(getDatabaseName())
        .query("sql", "SELECT observedprotocol() AS p FROM " + VECTOR_TYPE + " LIMIT 1")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat((String) rs.next().getProperty("p")).isEqualTo(ProtocolContext.INTERNAL);
    }

    assertThat(OBSERVED_PROTOCOL.get()).isEqualTo(ProtocolContext.INTERNAL);
  }
}
