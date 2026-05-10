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
import com.google.protobuf.Timestamp;
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

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #4181: DATE column written via gRPC Timestamp parameter binding
 * produced a corrupt binary record (BufferUnderflowException on every subsequent read).
 *
 * Root causes:
 * 1. Type.convert(Instant, LocalDate.class) had no Instant branch and returned the Instant
 *    unchanged, bypassing the LocalDate conversion.
 * 2. BinarySerializer.serializeValue TYPE_DATE had no else branch for unrecognised types,
 *    so it wrote the type marker byte but no content bytes, corrupting the record layout.
 */
public class Issue4181GrpcDateCorruptionIT extends BaseGraphServerTest {

  private static final int GRPC_PORT = 50051;

  private static final Metadata.Key<String> USER_HEADER =
      Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> PASSWORD_HEADER =
      Metadata.Key.of("x-arcade-password", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> DATABASE_HEADER =
      Metadata.Key.of("x-arcade-database", Metadata.ASCII_STRING_MARSHALLER);

  private static final LocalDate TEST_DATE          = LocalDate.of(2026, 5, 9);
  private static final long      TEST_DATE_EPOCH_S  = TEST_DATE.atStartOfDay(ZoneOffset.UTC).toEpochSecond();

  private ManagedChannel channel;
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

  private void executeCommand(final String command) {
    authenticatedStub.executeCommand(
        ExecuteCommandRequest.newBuilder()
            .setDatabase(getDatabaseName())
            .setCredentials(credentials())
            .setCommand(command)
            .build());
  }

  @Test
  void dateInsertViaGrpcTimestampRoundTrips() {
    // Create schema
    executeCommand("CREATE DOCUMENT TYPE DateTest4181 IF NOT EXISTS");
    executeCommand("CREATE PROPERTY DateTest4181.d IF NOT EXISTS DATE");

    // Midnight UTC on a known date - unambiguous, no zone-edge case
    final Timestamp ts = Timestamp.newBuilder().setSeconds(TEST_DATE_EPOCH_S).setNanos(0).build();

    // Insert via gRPC parameter binding (the path that was producing corrupt records)
    authenticatedStub.executeCommand(
        ExecuteCommandRequest.newBuilder()
            .setDatabase(getDatabaseName())
            .setCredentials(credentials())
            .setCommand("INSERT INTO DateTest4181 SET d = :v")
            .putParameters("v", GrpcValue.newBuilder().setTimestampValue(ts).build())
            .build());

    // Read back - before the fix this threw BufferUnderflowException and returned an empty record
    final ExecuteQueryResponse response = authenticatedStub.executeQuery(
        ExecuteQueryRequest.newBuilder()
            .setDatabase(getDatabaseName())
            .setCredentials(credentials())
            .setQuery("SELECT d FROM DateTest4181")
            .build());

    assertThat(response.getResultsList()).as("query must return at least one result group").isNotEmpty();
    assertThat(response.getResultsList().get(0).getRecordsList()).as("result group must contain records").isNotEmpty();

    final GrpcRecord record = response.getResultsList().get(0).getRecordsList().get(0);
    assertThat(record.getPropertiesMap()).as("'d' property must be present (not missing due to corruption)").containsKey("d");
    final GrpcValue dValue = record.getPropertiesMap().get("d");
    assertThat(dValue.hasTimestampValue()).as("DATE property must come back as a Timestamp").isTrue();
    assertThat(dValue.getTimestampValue().getSeconds()).isEqualTo(TEST_DATE_EPOCH_S);
  }

  @Test
  void dateInsertViaGrpcTimestampDoesNotCorruptSiblingProperties() {
    // Verify that subsequent reads do not throw (i.e. no record corruption occurred).
    // The corruption caused the varint reader to mis-align, making sibling properties unreadable.
    executeCommand("CREATE DOCUMENT TYPE DateCorrupt4181 IF NOT EXISTS");
    executeCommand("CREATE PROPERTY DateCorrupt4181.d IF NOT EXISTS DATE");
    executeCommand("CREATE PROPERTY DateCorrupt4181.name IF NOT EXISTS STRING");

    final Timestamp ts = Timestamp.newBuilder().setSeconds(TEST_DATE_EPOCH_S).setNanos(0).build();
    authenticatedStub.executeCommand(
        ExecuteCommandRequest.newBuilder()
            .setDatabase(getDatabaseName())
            .setCredentials(credentials())
            .setCommand("INSERT INTO DateCorrupt4181 SET d = :v, name = :n")
            .putParameters("v", GrpcValue.newBuilder().setTimestampValue(ts).build())
            .putParameters("n", GrpcValue.newBuilder().setStringValue("test").build())
            .build());

    final ExecuteQueryResponse response = authenticatedStub.executeQuery(
        ExecuteQueryRequest.newBuilder()
            .setDatabase(getDatabaseName())
            .setCredentials(credentials())
            .setQuery("SELECT d, name FROM DateCorrupt4181")
            .build());

    assertThat(response.getResultsList()).isNotEmpty();
    assertThat(response.getResultsList().get(0).getRecordsList()).isNotEmpty();
    final GrpcRecord record = response.getResultsList().get(0).getRecordsList().get(0);

    // The sibling 'name' property must be readable (corruption broke this too)
    assertThat(record.getPropertiesMap()).containsKey("name");
    assertThat(record.getPropertiesMap().get("name").getStringValue()).isEqualTo("test");
  }
}
