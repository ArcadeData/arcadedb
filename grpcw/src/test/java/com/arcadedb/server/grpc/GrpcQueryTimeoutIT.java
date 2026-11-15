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
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11813: {@code ExecuteQueryRequest.timeout_ms} was read by nothing, so a query sent with a 200 ms budget ran to its end.
 * It is now the budget the engine enforces on the query, as the Postgres statement_timeout and the Bolt transaction timeout are.
 */
public class GrpcQueryTimeoutIT extends BaseGrpcServerTest {
  private static final Metadata.Key<String> USER_HEADER     = Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> PASSWORD_HEADER = Metadata.Key.of("x-arcade-password", Metadata.ASCII_STRING_MARSHALLER);
  // A cartesian product of two types compared row by row: seconds of work, none of it served by an index
  private static final String               SLOW_QUERY      = "MATCH (a:SlowA), (b:SlowB) WHERE a.s CONTAINS b.s RETURN count(*) AS n";

  private ManagedChannel                               channel;
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
            super.start(responseListener, headers);
          }
        };
      }
    });
    stub = ArcadeDbServiceGrpc.newBlockingStub(authenticated);

    final Database database = getServerDatabase(0, getDatabaseName());
    database.getSchema().createVertexType("SlowA");
    database.getSchema().createVertexType("SlowB");
    database.transaction(() -> {
      for (int i = 0; i < 3_000; i++) {
        database.newVertex("SlowA").set("s", "a" + i + "xyz").save();
        database.newVertex("SlowB").set("s", "b" + i).save();
      }
    });
  }

  @AfterEach
  void teardownGrpcClient() throws InterruptedException {
    if (channel != null) {
      channel.shutdown();
      channel.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  @Test
  void aQueryStopsAtItsTimeoutMs() {
    final ExecuteQueryRequest request = ExecuteQueryRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build())
        .setLanguage("opencypher")
        .setQuery(SLOW_QUERY)
        .setTimeoutMs(200)
        .build();

    final long start = System.nanoTime();
    assertThatThrownBy(() -> stub.withDeadlineAfter(60, TimeUnit.SECONDS).executeQuery(request))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("imeout");
    assertThat(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start)).as("stopped near its 200 ms budget")
        .isLessThan(10_000);
  }
}
