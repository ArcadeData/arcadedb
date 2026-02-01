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
import com.arcadedb.test.BaseGraphServerTest;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
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

public class GrpcServerIT extends BaseGraphServerTest {

  private static final int GRPC_PORT = 50051;

  private static final Metadata.Key<String> USER_HEADER =
      Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> PASSWORD_HEADER =
      Metadata.Key.of("x-arcade-password", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> DATABASE_HEADER =
      Metadata.Key.of("x-arcade-database", Metadata.ASCII_STRING_MARSHALLER);

  private ManagedChannel channel;
  private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub blockingStub;
  private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub authenticatedStub;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue(
        "GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @BeforeEach
  public void setupGrpcClient() {
    channel = ManagedChannelBuilder.forAddress("localhost", GRPC_PORT)
        .usePlaintext()
        .build();
    blockingStub = ArcadeDbServiceGrpc.newBlockingStub(channel);

    // Create an authenticated channel using a client interceptor
    Channel authenticatedChannel = io.grpc.ClientInterceptors.intercept(channel, new AuthClientInterceptor());
    authenticatedStub = ArcadeDbServiceGrpc.newBlockingStub(authenticatedChannel);
  }

  /**
   * Client interceptor that adds authentication headers to every request
   */
  private class AuthClientInterceptor implements ClientInterceptor {
    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
        MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
      return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
          next.newCall(method, callOptions)) {
        @Override
        public void start(Listener<RespT> responseListener, Metadata headers) {
          headers.put(USER_HEADER, "root");
          headers.put(PASSWORD_HEADER, DEFAULT_PASSWORD_FOR_TESTS);
          headers.put(DATABASE_HEADER, getDatabaseName());
          super.start(responseListener, headers);
        }
      };
    }
  }

  @AfterEach
  public void teardownGrpcClient() throws InterruptedException {
    if (channel != null) {
      channel.shutdown();
      channel.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  private DatabaseCredentials credentials() {
    return DatabaseCredentials.newBuilder()
        .setUsername("root")
        .setPassword(DEFAULT_PASSWORD_FOR_TESTS)
        .build();
  }

  private GrpcValue stringValue(final String s) {
    return GrpcValue.newBuilder().setStringValue(s).build();
  }

  private GrpcValue intValue(final int i) {
    return GrpcValue.newBuilder().setInt32Value(i).build();
  }

  private GrpcValue longValue(final long l) {
    return GrpcValue.newBuilder().setInt64Value(l).build();
  }

  @Test
  void executeQuerySelectsExistingData() {
    ExecuteQueryRequest request = ExecuteQueryRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setQuery("SELECT FROM V1 WHERE id = 0")
        .build();

    ExecuteQueryResponse response = authenticatedStub.executeQuery(request);

    assertThat(response.getResultsList()).isNotEmpty();
    assertThat(response.getResultsList().get(0).getRecordsList()).isNotEmpty();

    GrpcRecord record = response.getResultsList().get(0).getRecordsList().get(0);
    assertThat(record.getPropertiesMap()).containsKey("name");
    assertThat(record.getPropertiesMap().get("name").getStringValue()).isEqualTo("V1");
  }

  @Test
  void executeQueryWithParametersWorks() {
    ExecuteQueryRequest request = ExecuteQueryRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setQuery("SELECT FROM V1 WHERE id = :id")
        .putParameters("id", longValue(0))
        .build();

    ExecuteQueryResponse response = authenticatedStub.executeQuery(request);

    assertThat(response.getResultsList()).isNotEmpty();
    assertThat(response.getResultsList().get(0).getRecordsList()).isNotEmpty();
  }

  @Test
  void executeQueryReturnsEmptyForNoMatches() {
    ExecuteQueryRequest request = ExecuteQueryRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setQuery("SELECT FROM V1 WHERE id = 99999")
        .build();

    ExecuteQueryResponse response = authenticatedStub.executeQuery(request);

    assertThat(response.getResultsList()).isNotEmpty();
    assertThat(response.getResultsList().get(0).getRecordsList()).isEmpty();
  }

  @Test
  void executeQueryWithoutCredentialsFails() {
    ExecuteQueryRequest request = ExecuteQueryRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setQuery("SELECT FROM V1")
        .build();

    // Using blockingStub without authentication headers
    assertThatThrownBy(() -> blockingStub.executeQuery(request))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("UNAUTHENTICATED");
  }
}
