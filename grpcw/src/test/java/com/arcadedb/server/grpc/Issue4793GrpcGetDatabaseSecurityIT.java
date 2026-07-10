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

import java.io.File;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #4793.
 * <p>
 * {@code ArcadeDbGrpcService.getDatabase} previously accepted any request-supplied database name
 * after merely checking that a username was resolvable. A name containing {@code ..}, {@code /} or
 * {@code \} escaped the configured databases directory (path traversal), and the service could
 * open/create arbitrary on-disk databases. These tests assert that path-traversal database names
 * are rejected before any filesystem access while legitimate, authorized access keeps working.
 */
public class Issue4793GrpcGetDatabaseSecurityIT extends BaseGraphServerTest {

  private static final int GRPC_PORT = 50051;

  private static final Metadata.Key<String> USER_HEADER     =
      Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> PASSWORD_HEADER =
      Metadata.Key.of("x-arcade-password", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> DATABASE_HEADER =
      Metadata.Key.of("x-arcade-database", Metadata.ASCII_STRING_MARSHALLER);

  private ManagedChannel                                  channel;
  private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub authenticatedStub;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
    GlobalConfiguration.QUERY_PARALLEL_SCAN.setValue(false);
  }

  @BeforeEach
  void setupGrpcClient() {
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
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(final MethodDescriptor<ReqT, RespT> method,
        final CallOptions callOptions, final Channel next) {
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

  private ExecuteQueryRequest queryOn(final String databaseName) {
    return ExecuteQueryRequest.newBuilder()
        .setDatabase(databaseName)
        .setCredentials(credentials())
        .setQuery("SELECT FROM V1 LIMIT 1")
        .build();
  }

  /**
   * Asserts the request is rejected with the INVALID_ARGUMENT status code and a message that names
   * the offending database name, so the client-visible API contract is pinned (not masked as
   * INTERNAL).
   */
  private void assertRejectedAsInvalidName(final ExecuteQueryRequest request) {
    assertThatThrownBy(() -> authenticatedStub.executeQuery(request))
        .isInstanceOfSatisfying(StatusRuntimeException.class,
            e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT))
        .hasMessageContaining("Invalid database name");
  }

  @Test
  void parentDirectoryTraversalDatabaseNameIsRejected() {
    final String traversalTarget = "escaped4793";
    assertRejectedAsInvalidName(queryOn(".." + File.separator + traversalTarget));

    // The traversal must never have escaped the databases directory and created a database on disk.
    final File databasesDir = new File(GlobalConfiguration.SERVER_DATABASE_DIRECTORY.getValueAsString());
    final File escaped = new File(databasesDir.getParentFile(), traversalTarget);
    assertThat(escaped).doesNotExist();
  }

  @Test
  void forwardSlashDatabaseNameIsRejected() {
    assertRejectedAsInvalidName(queryOn("sub/evil4793"));
  }

  @Test
  void backslashDatabaseNameIsRejected() {
    assertRejectedAsInvalidName(queryOn("sub\\evil4793"));
  }

  @Test
  void currentDirectoryDatabaseNameIsRejected() {
    // A bare "." resolves to the databases directory itself; it must be rejected.
    assertRejectedAsInvalidName(queryOn("."));
  }

  @Test
  void blankDatabaseNameIsRejected() {
    assertRejectedAsInvalidName(queryOn("   "));
  }

  @Test
  void legitimateAuthorizedDatabaseAccessStillWorks() {
    // Positive control: a plain, authorized database name must keep working after the hardening.
    final ExecuteQueryResponse response = authenticatedStub.executeQuery(queryOn(getDatabaseName()));
    assertThat(response).isNotNull();
  }
}
