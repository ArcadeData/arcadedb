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

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #4805: writing a DATETIME_MICROS / DATETIME_NANOS column through the
 * gRPC record write path ({@code createRecord} -> {@code convertWithSchemaType}) truncated the
 * proto Timestamp to millisecond precision via {@code new Date(tsToMillis(...))}, discarding the
 * sub-millisecond precision the column type is declared to keep.
 * <p>
 * The fix converts the inbound proto Timestamp to {@link java.time.Instant} (carrying full
 * nanosecond precision) and lets {@code Type.convert()} truncate to the column's declared
 * precision, matching the parameter-binding path ({@code fromGrpcValue}, issue #4149).
 */
public class Issue4805GrpcDatetimePrecisionIT extends BaseGraphServerTest {

  private static final int GRPC_PORT = 50051;

  private static final Metadata.Key<String> USER_HEADER =
      Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> PASSWORD_HEADER =
      Metadata.Key.of("x-arcade-password", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> DATABASE_HEADER =
      Metadata.Key.of("x-arcade-database", Metadata.ASCII_STRING_MARSHALLER);

  // 2026-05-09T12:34:56.123456789Z - carries distinct millis, micros and nanos digits
  private static final LocalDateTime TEST_TIME = LocalDateTime.of(2026, 5, 9, 12, 34, 56, 123_456_789);
  private static final long          TEST_SECONDS = TEST_TIME.toEpochSecond(ZoneOffset.UTC);
  private static final int           TEST_NANOS = 123_456_789;

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

  private int readBackNanos(final String type) {
    final ExecuteQueryResponse response = authenticatedStub.executeQuery(
        ExecuteQueryRequest.newBuilder()
            .setDatabase(getDatabaseName())
            .setCredentials(credentials())
            .setQuery("SELECT t FROM " + type)
            .build());

    assertThat(response.getResultsList()).as("query must return at least one result group").isNotEmpty();
    assertThat(response.getResultsList().getFirst().getRecordsList()).as("result group must contain records").isNotEmpty();

    final GrpcRecord record = response.getResultsList().getFirst().getRecordsList().getFirst();
    assertThat(record.getPropertiesMap()).as("'t' property must be present").containsKey("t");
    final GrpcValue tValue = record.getPropertiesMap().get("t");
    assertThat(tValue.hasTimestampValue()).as("datetime property must come back as a Timestamp").isTrue();
    assertThat(tValue.getTimestampValue().getSeconds()).isEqualTo(TEST_SECONDS);
    return tValue.getTimestampValue().getNanos();
  }

  @Test
  void createRecordPreservesNanosForDatetimeNanos() {
    executeCommand("CREATE DOCUMENT TYPE NanosTest4805 IF NOT EXISTS");
    executeCommand("CREATE PROPERTY NanosTest4805.t IF NOT EXISTS DATETIME_NANOS");

    final Timestamp ts = Timestamp.newBuilder().setSeconds(TEST_SECONDS).setNanos(TEST_NANOS).build();

    final GrpcRecord record = GrpcRecord.newBuilder()
        .setType("NanosTest4805")
        .putProperties("t", GrpcValue.newBuilder().setTimestampValue(ts).build())
        .build();

    // Write through the createRecord path (convertWithSchemaType), the path with the bug.
    final CreateRecordResponse createResp = authenticatedStub.createRecord(
        CreateRecordRequest.newBuilder()
            .setDatabase(getDatabaseName())
            .setCredentials(credentials())
            .setType("NanosTest4805")
            .setRecord(record)
            .build());
    assertThat(createResp.getRid()).isNotEmpty();

    // Before the fix the write path collapsed to millis (123_000_000); DATETIME_NANOS keeps full nanos.
    assertThat(readBackNanos("NanosTest4805")).isEqualTo(TEST_NANOS);
  }

  @Test
  void createRecordPreservesMicrosForDatetimeMicros() {
    executeCommand("CREATE DOCUMENT TYPE MicrosTest4805 IF NOT EXISTS");
    executeCommand("CREATE PROPERTY MicrosTest4805.t IF NOT EXISTS DATETIME_MICROS");

    final Timestamp ts = Timestamp.newBuilder().setSeconds(TEST_SECONDS).setNanos(TEST_NANOS).build();

    final GrpcRecord record = GrpcRecord.newBuilder()
        .setType("MicrosTest4805")
        .putProperties("t", GrpcValue.newBuilder().setTimestampValue(ts).build())
        .build();

    final CreateRecordResponse createResp = authenticatedStub.createRecord(
        CreateRecordRequest.newBuilder()
            .setDatabase(getDatabaseName())
            .setCredentials(credentials())
            .setType("MicrosTest4805")
            .setRecord(record)
            .build());
    assertThat(createResp.getRid()).isNotEmpty();

    // DATETIME_MICROS truncates to microsecond precision: 123_456_789 -> 123_456_000.
    // Before the fix the write path collapsed to millis (123_000_000).
    assertThat(readBackNanos("MicrosTest4805")).isEqualTo(123_456_000);
  }

  @Test
  void updateRecordPreservesNanosForDatetimeNanos() {
    executeCommand("CREATE DOCUMENT TYPE NanosUpdate4805 IF NOT EXISTS");
    executeCommand("CREATE PROPERTY NanosUpdate4805.t IF NOT EXISTS DATETIME_NANOS");

    // Create with a placeholder timestamp, then update through the updateRecord path
    // (which also routes through convertWithSchemaType).
    final Timestamp placeholder = Timestamp.newBuilder().setSeconds(TEST_SECONDS).setNanos(0).build();
    final String rid = authenticatedStub.createRecord(
        CreateRecordRequest.newBuilder()
            .setDatabase(getDatabaseName())
            .setCredentials(credentials())
            .setType("NanosUpdate4805")
            .setRecord(GrpcRecord.newBuilder()
                .setType("NanosUpdate4805")
                .putProperties("t", GrpcValue.newBuilder().setTimestampValue(placeholder).build())
                .build())
            .build())
        .getRid();
    assertThat(rid).isNotEmpty();

    final Timestamp ts = Timestamp.newBuilder().setSeconds(TEST_SECONDS).setNanos(TEST_NANOS).build();
    final UpdateRecordResponse updateResp = authenticatedStub.updateRecord(
        UpdateRecordRequest.newBuilder()
            .setDatabase(getDatabaseName())
            .setCredentials(credentials())
            .setRid(rid)
            .setRecord(GrpcRecord.newBuilder()
                .setType("NanosUpdate4805")
                .putProperties("t", GrpcValue.newBuilder().setTimestampValue(ts).build())
                .build())
            .build());
    assertThat(updateResp.getSuccess()).isTrue();

    assertThat(readBackNanos("NanosUpdate4805")).isEqualTo(TEST_NANOS);
  }
}
