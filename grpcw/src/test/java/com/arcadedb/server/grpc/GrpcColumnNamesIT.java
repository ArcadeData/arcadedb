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

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #13464: {@code GrpcRecord.properties} is a protobuf map and carries no order, so the response names the columns of a
 * projection in order in {@code column_names}.
 */
public class GrpcColumnNamesIT extends BaseGrpcServerTest {

  private static final Metadata.Key<String> USER_HEADER =
      Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> PASSWORD_HEADER =
      Metadata.Key.of("x-arcade-password", Metadata.ASCII_STRING_MARSHALLER);

  private ManagedChannel channel;
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
        return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(next.newCall(method, callOptions)) {
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
  }

  @AfterEach
  void teardownGrpcClient() throws InterruptedException {
    if (channel != null) {
      channel.shutdown();
      channel.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  private static DatabaseCredentials credentials() {
    return DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build();
  }

  @Test
  void executeCommandNamesTheColumnsInProjectionOrder() {
    final ExecuteCommandResponse response = stub.executeCommand(ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setLanguage("opencypher")
        .setCommand("RETURN 1 AS z, 2 AS a, 3 AS m")
        .setReturnRows(true)
        .build());

    assertThat(response.getColumnNamesList()).containsExactly("z", "a", "m");
  }

  @Test
  void executeQueryNamesTheColumnsInProjectionOrder() {
    final ExecuteQueryResponse response = stub.executeQuery(ExecuteQueryRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setLanguage("opencypher")
        .setQuery("RETURN 1 AS z, 2 AS a, 3 AS m")
        .build());

    assertThat(response.getResults(0).getColumnNamesList()).containsExactly("z", "a", "m");
  }

  @Test
  void aResultWithoutRowsHasNoColumnNames() {
    final ExecuteCommandResponse response = stub.executeCommand(ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setLanguage("opencypher")
        .setCommand("UNWIND [] AS x RETURN x AS z, x AS a")
        .setReturnRows(true)
        .build());

    assertThat(response.getColumnNamesList()).isEmpty();
  }

  @Test
  void aRowThatIsOneNodeNamesItsColumn() {
    stub.executeCommand(ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setLanguage("opencypher")
        .setCommand("CREATE (:ColNode {id: 1})")
        .build());

    final ExecuteCommandResponse response = stub.executeCommand(ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setLanguage("opencypher")
        .setCommand("MATCH (dev:ColNode) RETURN dev")
        .setReturnRows(true)
        .build());

    assertThat(response.getColumnNamesList()).containsExactly("dev");
    assertThat(response.getRecords(0).getPropertiesMap()).containsKey("id");
  }
}
