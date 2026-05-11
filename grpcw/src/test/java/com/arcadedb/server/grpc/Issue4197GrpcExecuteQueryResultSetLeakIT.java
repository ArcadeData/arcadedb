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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.query.QueryEngine;
import com.arcadedb.query.QueryEngineManager;
import com.arcadedb.query.sql.SQLQueryEngine;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.test.BaseGraphServerTest;
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

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #4197: gRPC {@code executeQuery} leaked the underlying {@link ResultSet}
 * (never closed it) because the handler did not use try-with-resources. The leaked execution-plan
 * state accumulated on the shared {@code Database} instance and degraded subsequent {@code InsertStream}
 * throughput by 20-30x after only a few hundred unary calls.
 *
 * <p>The fix wraps the {@code database.query(...)} call in a try-with-resources block, matching every
 * other handler in {@link ArcadeDbGrpcService}. This test verifies the contract directly: it installs a
 * {@link QueryEngine} that counts open/close pairs and asserts that every gRPC {@code executeQuery}
 * closes its result set.
 */
public class Issue4197GrpcExecuteQueryResultSetLeakIT extends BaseGraphServerTest {

  private static final int GRPC_PORT = 50051;

  private static final Metadata.Key<String> USER_HEADER     = Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> PASSWORD_HEADER = Metadata.Key.of("x-arcade-password", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> DATABASE_HEADER = Metadata.Key.of("x-arcade-database", Metadata.ASCII_STRING_MARSHALLER);

  private static final String TRACKED_LANGUAGE = "sql-tracked-4197";

  private static final AtomicInteger opened = new AtomicInteger();
  private static final AtomicInteger closed = new AtomicInteger();

  private ManagedChannel                                       channel;
  private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub      authenticatedStub;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
    GlobalConfiguration.QUERY_PARALLEL_SCAN.setValue(false);
  }

  @BeforeEach
  void setupGrpcClient() {
    // Register the tracking engine. register() overwrites existing entries safely; the language name
    // is unique to this test so it does not interfere with regular "sql" usage elsewhere.
    QueryEngineManager.getInstance().register(new TrackingQueryEngineFactory());
    opened.set(0);
    closed.set(0);

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
    return DatabaseCredentials.newBuilder()
        .setUsername("root")
        .setPassword(DEFAULT_PASSWORD_FOR_TESTS)
        .build();
  }

  @Test
  void executeQueryClosesResultSet() {
    // A small number of iterations is enough: the bug is deterministic - any call to gRPC
    // executeQuery that does not use try-with-resources leaks one ResultSet per call.
    final int iterations = 25;

    for (int i = 0; i < iterations; i++) {
      final ExecuteQueryResponse response = authenticatedStub.executeQuery(
          ExecuteQueryRequest.newBuilder()
              .setDatabase(getDatabaseName())
              .setCredentials(credentials())
              .setLanguage(TRACKED_LANGUAGE)
              .setQuery("SELECT FROM V1 LIMIT 1")
              .build());

      // Sanity: the response should still be well-formed.
      assertThat(response.getResultsList()).isNotEmpty();
    }

    // Every gRPC executeQuery must have opened exactly one ResultSet and then closed it.
    assertThat(opened.get())
        .as("Tracking engine should have been invoked %d times", iterations)
        .isEqualTo(iterations);
    assertThat(closed.get())
        .as("Every ResultSet produced by gRPC executeQuery must be closed (issue #4197)")
        .isEqualTo(iterations);
  }

  // ---------------------------------------------------------------------------------------------
  // Tracking engine: delegates to the real SQL engine, wraps the returned ResultSet to count
  // open vs. close. Same language registered under a unique name to avoid touching other tests.
  // ---------------------------------------------------------------------------------------------

  private static final class TrackingQueryEngineFactory implements QueryEngine.QueryEngineFactory {
    private final QueryEngine.QueryEngineFactory delegateFactory = new SQLQueryEngine.SQLQueryEngineFactory();

    @Override
    public String getLanguage() {
      return TRACKED_LANGUAGE;
    }

    @Override
    public QueryEngine getInstance(final DatabaseInternal database) {
      return new TrackingQueryEngine(delegateFactory.getInstance(database));
    }
  }

  private static final class TrackingQueryEngine implements QueryEngine {
    private final QueryEngine delegate;

    TrackingQueryEngine(final QueryEngine delegate) {
      this.delegate = delegate;
    }

    @Override
    public String getLanguage() {
      return TRACKED_LANGUAGE;
    }

    @Override
    public AnalyzedQuery analyze(final String query) {
      return delegate.analyze(query);
    }

    @Override
    public ResultSet query(final String query, final ContextConfiguration configuration, final Map<String, Object> parameters) {
      return track(delegate.query(query, configuration, parameters));
    }

    @Override
    public ResultSet query(final String query, final ContextConfiguration configuration, final Object... parameters) {
      return track(delegate.query(query, configuration, parameters));
    }

    @Override
    public ResultSet command(final String query, final ContextConfiguration configuration, final Map<String, Object> parameters) {
      return track(delegate.command(query, configuration, parameters));
    }

    @Override
    public ResultSet command(final String query, final ContextConfiguration configuration, final Object... parameters) {
      return track(delegate.command(query, configuration, parameters));
    }

    private static ResultSet track(final ResultSet delegate) {
      opened.incrementAndGet();
      return new TrackingResultSet(delegate);
    }
  }

  private static final class TrackingResultSet implements ResultSet {
    private final ResultSet delegate;

    TrackingResultSet(final ResultSet delegate) {
      this.delegate = delegate;
    }

    @Override
    public boolean hasNext() {
      return delegate.hasNext();
    }

    @Override
    public Result next() {
      return delegate.next();
    }

    @Override
    public void close() {
      delegate.close();
      closed.incrementAndGet();
    }
  }
}
