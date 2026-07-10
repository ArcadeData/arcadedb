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
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * Regression test for issue #4803: gRPC unbounded result materialization. The unary {@code ExecuteQuery}
 * used to build the full result into one response when the request gave no positive {@code limit}, and the
 * {@code StreamQuery} {@code MATERIALIZE_ALL} retrieval mode buffered the entire result set into memory before
 * emitting anything. A limitless query against a large type could exhaust heap (DoS).
 *
 * <p>The fix bounds both paths with server-scoped configuration:
 * <ul>
 *   <li>{@code arcadedb.server.grpcQueryMaxResultRows} is a hard ceiling on the unary {@code ExecuteQuery}:
 *       a result that would exceed it fails with {@code RESOURCE_EXHAUSTED} instead of silently truncating,
 *       and a client cannot bypass it with an oversized explicit limit;</li>
 *   <li>{@code arcadedb.server.grpcStreamMaxMaterializedRows} caps {@code MATERIALIZE_ALL} buffering and fails
 *       the call with {@code RESOURCE_EXHAUSTED} when exceeded.</li>
 * </ul>
 */
public class Issue4803GrpcResultBoundingIT extends BaseGraphServerTest {

  private static final int GRPC_PORT = 50051;

  private static final int MAX_QUERY_ROWS       = 5;
  private static final int MAX_MATERIALIZED_ROWS = 5;
  private static final int ROWS_INSERTED         = 40;

  private static final Metadata.Key<String> USER_HEADER     = Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> PASSWORD_HEADER = Metadata.Key.of("x-arcade-password", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> DATABASE_HEADER = Metadata.Key.of("x-arcade-database", Metadata.ASCII_STRING_MARSHALLER);

  private ManagedChannel                                 channel;
  private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub authenticatedStub;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
    // Tight caps so the bound is reached with a small, fast dataset.
    GlobalConfiguration.SERVER_GRPC_QUERY_MAX_RESULT_ROWS.setValue(MAX_QUERY_ROWS);
    GlobalConfiguration.SERVER_GRPC_STREAM_MAX_MATERIALIZED_ROWS.setValue(MAX_MATERIALIZED_ROWS);
  }

  @BeforeEach
  void setupGrpcClient() {
    // Seed enough rows to exceed both caps.
    final Database db = getServerDatabase(0, getDatabaseName());
    db.transaction(() -> {
      for (int i = 0; i < ROWS_INSERTED; i++)
        db.newVertex(VERTEX1_TYPE_NAME).set("id", 1000L + i).save();
    });

    channel = ManagedChannelBuilder.forAddress("localhost", GRPC_PORT).usePlaintext().build();
    final Channel authenticatedChannel = ClientInterceptors.intercept(channel, new AuthClientInterceptor());
    authenticatedStub = ArcadeDbServiceGrpc.newBlockingStub(authenticatedChannel);
  }

  @AfterEach
  void shutdownGrpcClient() throws InterruptedException {
    if (channel != null) {
      channel.shutdown();
      channel.awaitTermination(5, TimeUnit.SECONDS);
    }
    GlobalConfiguration.SERVER_GRPC_QUERY_MAX_RESULT_ROWS.reset();
    GlobalConfiguration.SERVER_GRPC_STREAM_MAX_MATERIALIZED_ROWS.reset();
  }

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

  private DatabaseCredentials credentials() {
    return DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build();
  }

  @Test
  void unaryExecuteQueryWithoutLimitExceedingCapFailsWithResourceExhausted() {
    final StatusRuntimeException ex = catchThrowableOfType(StatusRuntimeException.class,
        () -> authenticatedStub.executeQuery(
            ExecuteQueryRequest.newBuilder()
                .setDatabase(getDatabaseName())
                .setCredentials(credentials())
                .setQuery("SELECT FROM " + VERTEX1_TYPE_NAME) // deliberately no LIMIT
                .build()));

    // The dataset is larger than the cap. A limitless query must fail loudly rather than silently truncating
    // and dropping data without telling the caller (issue #4803).
    assertThat(ex)
        .as("limitless gRPC executeQuery over a result larger than the cap must fail, not silently truncate")
        .isNotNull();
    assertThat(ex.getStatus().getCode())
        .as("the cap breach must surface as RESOURCE_EXHAUSTED so the client knows the result was not complete")
        .isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
  }

  @Test
  void unaryExecuteQueryWithExplicitLimitLargerThanCapFailsWithResourceExhausted() {
    // A client must not be able to bypass the DoS-protection ceiling by requesting an oversized explicit limit:
    // the configured cap is a hard ceiling, so a larger limit over a larger result still fails loudly.
    final StatusRuntimeException ex = catchThrowableOfType(StatusRuntimeException.class,
        () -> authenticatedStub.executeQuery(
            ExecuteQueryRequest.newBuilder()
                .setDatabase(getDatabaseName())
                .setCredentials(credentials())
                .setQuery("SELECT FROM " + VERTEX1_TYPE_NAME)
                .setLimit(MAX_QUERY_ROWS + 10)
                .build()));

    assertThat(ex)
        .as("an explicit limit larger than the cap must not bypass the hard ceiling")
        .isNotNull();
    assertThat(ex.getStatus().getCode())
        .as("exceeding the hard ceiling must surface as RESOURCE_EXHAUSTED")
        .isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
  }

  @Test
  void unaryExecuteQueryWithCapDisabledReturnsAllRows() {
    // The -1 opt-out must restore the legacy unlimited behavior so disabling DoS protection does not
    // silently keep capping (or now, failing) large results.
    GlobalConfiguration.SERVER_GRPC_QUERY_MAX_RESULT_ROWS.setValue(-1);
    try {
      final ExecuteQueryResponse response = authenticatedStub.executeQuery(
          ExecuteQueryRequest.newBuilder()
              .setDatabase(getDatabaseName())
              .setCredentials(credentials())
              .setQuery("SELECT FROM " + VERTEX1_TYPE_NAME) // no limit, cap disabled
              .build());

      // The seeded rows alone exceed the (now disabled) cap, so the opt-out is proven by the call returning
      // the full result without a RESOURCE_EXHAUSTED failure and without truncating at the former ceiling.
      assertThat(response.getResults(0).getRecordsCount())
          .as("a disabled cap (-1) must opt back into unlimited and return the full result, not cap at the ceiling")
          .isGreaterThanOrEqualTo(ROWS_INSERTED)
          .isGreaterThan(MAX_QUERY_ROWS);
    } finally {
      GlobalConfiguration.SERVER_GRPC_QUERY_MAX_RESULT_ROWS.setValue(MAX_QUERY_ROWS);
    }
  }

  @Test
  void unaryExecuteQueryHonorsExplicitSmallerLimit() {
    final int explicitLimit = 3;
    final ExecuteQueryResponse response = authenticatedStub.executeQuery(
        ExecuteQueryRequest.newBuilder()
            .setDatabase(getDatabaseName())
            .setCredentials(credentials())
            .setQuery("SELECT FROM " + VERTEX1_TYPE_NAME)
            .setLimit(explicitLimit)
            .build());

    assertThat(response.getResults(0).getRecordsCount())
        .as("an explicit limit smaller than the cap must still be honored")
        .isEqualTo(explicitLimit);
  }

  @Test
  void materializeAllStreamExceedingCapFailsWithResourceExhausted() {
    final StreamQueryRequest request = StreamQueryRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setQuery("SELECT FROM " + VERTEX1_TYPE_NAME)
        .setBatchSize(10)
        .setRetrievalMode(StreamQueryRequest.RetrievalMode.MATERIALIZE_ALL)
        .build();

    final StatusRuntimeException ex = catchThrowableOfType(StatusRuntimeException.class, () -> {
      final Iterator<QueryResult> results = authenticatedStub.streamQuery(request);
      while (results.hasNext())
        results.next();
    });

    assertThat(ex)
        .as("MATERIALIZE_ALL over a result larger than the cap must fail, not buffer everything (issue #4803)")
        .isNotNull();
    assertThat(ex.getStatus().getCode())
        .as("the cap breach must surface as RESOURCE_EXHAUSTED so clients can fall back to CURSOR/PAGED")
        .isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
  }
}
