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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.grpc.StatusRuntimeException;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #5045 (COR-12): the {@code DATETIME_SECOND/MICROS/NANOS} STRING branch
 * in {@code convertWithSchemaType} previously did {@code new Date(Long.parseLong(s))}, which threw
 * {@link NumberFormatException} on an ISO-8601 string and capped high-precision columns at
 * millisecond precision.
 * <p>
 * The fix parses ISO-8601 (numeric strings stay epoch-millis for backward compatibility) and keeps
 * sub-millisecond precision via {@link java.time.Instant}, letting {@code Type.convert()} truncate
 * to the column's declared precision.
 */
public class Issue5045GrpcDatetimeStringPrecisionIT extends BaseGraphServerTest {

  private static final int GRPC_PORT = 50051;

  private static final Metadata.Key<String> USER_HEADER =
      Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> PASSWORD_HEADER =
      Metadata.Key.of("x-arcade-password", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> DATABASE_HEADER =
      Metadata.Key.of("x-arcade-database", Metadata.ASCII_STRING_MARSHALLER);

  // 2026-05-09T12:34:56.123456789 - distinct millis, micros and nanos digits
  private static final LocalDateTime TEST_TIME    = LocalDateTime.of(2026, 5, 9, 12, 34, 56, 123_456_789);
  private static final long          TEST_SECONDS = TEST_TIME.toEpochSecond(ZoneOffset.UTC);

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
    assertThat(response.getResultsList().get(0).getRecordsList()).as("result group must contain records").isNotEmpty();

    final GrpcRecord record = response.getResultsList().get(0).getRecordsList().get(0);
    assertThat(record.getPropertiesMap()).as("'t' property must be present").containsKey("t");
    final GrpcValue tValue = record.getPropertiesMap().get("t");
    assertThat(tValue.hasTimestampValue()).as("datetime property must come back as a Timestamp").isTrue();
    assertThat(tValue.getTimestampValue().getSeconds()).isEqualTo(TEST_SECONDS);
    return tValue.getTimestampValue().getNanos();
  }

  @Test
  void isoStringPreservesNanosForDatetimeNanos() {
    executeCommand("CREATE DOCUMENT TYPE NanosStr5045 IF NOT EXISTS");
    executeCommand("CREATE PROPERTY NanosStr5045.t IF NOT EXISTS DATETIME_NANOS");

    // ISO-8601 string with nanosecond precision. Before the fix this threw NumberFormatException.
    final GrpcRecord record = GrpcRecord.newBuilder()
        .setType("NanosStr5045")
        .putProperties("t", GrpcValue.newBuilder().setStringValue("2026-05-09T12:34:56.123456789").build())
        .build();

    final CreateRecordResponse createResp = authenticatedStub.createRecord(
        CreateRecordRequest.newBuilder()
            .setDatabase(getDatabaseName())
            .setCredentials(credentials())
            .setType("NanosStr5045")
            .setRecord(record)
            .build());
    assertThat(createResp.getRid()).isNotEmpty();

    assertThat(readBackNanos("NanosStr5045")).isEqualTo(123_456_789);
  }

  @Test
  void isoStringPreservesMicrosForDatetimeMicros() {
    executeCommand("CREATE DOCUMENT TYPE MicrosStr5045 IF NOT EXISTS");
    executeCommand("CREATE PROPERTY MicrosStr5045.t IF NOT EXISTS DATETIME_MICROS");

    final GrpcRecord record = GrpcRecord.newBuilder()
        .setType("MicrosStr5045")
        .putProperties("t", GrpcValue.newBuilder().setStringValue("2026-05-09T12:34:56.123456789").build())
        .build();

    final CreateRecordResponse createResp = authenticatedStub.createRecord(
        CreateRecordRequest.newBuilder()
            .setDatabase(getDatabaseName())
            .setCredentials(credentials())
            .setType("MicrosStr5045")
            .setRecord(record)
            .build());
    assertThat(createResp.getRid()).isNotEmpty();

    // DATETIME_MICROS truncates to microsecond precision: 123_456_789 -> 123_456_000.
    assertThat(readBackNanos("MicrosStr5045")).isEqualTo(123_456_000);
  }

  @Test
  void malformedStringSurfacesAnErrorInsteadOfSilentlyStoringNull() {
    // Documents the decision for point 2 of the #5196 review: a string that is neither numeric nor a
    // parseable datetime is surfaced as a loud gRPC error (DateUtils.parseIsoDateTime throws
    // DateTimeParseException, which propagates), not silently coerced to null and stored.
    executeCommand("CREATE DOCUMENT TYPE NanosBad5045 IF NOT EXISTS");
    executeCommand("CREATE PROPERTY NanosBad5045.t IF NOT EXISTS DATETIME_NANOS");

    final GrpcRecord record = GrpcRecord.newBuilder()
        .setType("NanosBad5045")
        .putProperties("t", GrpcValue.newBuilder().setStringValue("not-a-datetime").build())
        .build();

    assertThatThrownBy(() -> authenticatedStub.createRecord(
        CreateRecordRequest.newBuilder()
            .setDatabase(getDatabaseName())
            .setCredentials(credentials())
            .setType("NanosBad5045")
            .setRecord(record)
            .build()))
        .isInstanceOf(StatusRuntimeException.class);

    // Nothing was persisted.
    final ExecuteQueryResponse response = authenticatedStub.executeQuery(
        ExecuteQueryRequest.newBuilder()
            .setDatabase(getDatabaseName())
            .setCredentials(credentials())
            .setQuery("SELECT count(*) AS c FROM NanosBad5045")
            .build());
    final GrpcValue count = response.getResultsList().get(0).getRecordsList().get(0).getPropertiesMap().get("c");
    assertThat(((Number) GrpcTypeConverter.fromGrpcValue(count)).longValue()).isZero();
  }

  @Test
  void numericStringStaysEpochMillisForDatetimeNanos() {
    executeCommand("CREATE DOCUMENT TYPE NanosEpoch5045 IF NOT EXISTS");
    executeCommand("CREATE PROPERTY NanosEpoch5045.t IF NOT EXISTS DATETIME_NANOS");

    // A numeric string remains epoch milliseconds for backward compatibility.
    final long epochMillis = TEST_SECONDS * 1000L + 123L;
    final GrpcRecord record = GrpcRecord.newBuilder()
        .setType("NanosEpoch5045")
        .putProperties("t", GrpcValue.newBuilder().setStringValue(Long.toString(epochMillis)).build())
        .build();

    final CreateRecordResponse createResp = authenticatedStub.createRecord(
        CreateRecordRequest.newBuilder()
            .setDatabase(getDatabaseName())
            .setCredentials(credentials())
            .setType("NanosEpoch5045")
            .setRecord(record)
            .build());
    assertThat(createResp.getRid()).isNotEmpty();

    // Epoch-millis input carries only millisecond precision.
    assertThat(readBackNanos("NanosEpoch5045")).isEqualTo(123_000_000);
  }
}
