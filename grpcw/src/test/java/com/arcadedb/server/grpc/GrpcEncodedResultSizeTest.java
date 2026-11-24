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
 * The size of the messages the gRPC value encoders build: a result whose encoding would be larger than
 * {@code arcadedb.grpc.maxMessageSize} is refused with {@code RESOURCE_EXHAUSTED} by every RPC that answers rows, and the server
 * keeps answering afterwards. A result that fits is answered as it always was.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class GrpcEncodedResultSizeTest extends BaseGrpcServerTest {

  private static final Metadata.Key<String> USER_HEADER     = Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> PASSWORD_HEADER = Metadata.Key.of("x-arcade-password", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> DATABASE_HEADER = Metadata.Key.of("x-arcade-database", Metadata.ASCII_STRING_MARSHALLER);

  // 2^19 leaves: a result of a few MB in the encoding, which the 1 MB ceiling of this test refuses and a client that accepts large
  // messages would otherwise receive in full
  private static final int NESTING = 19;

  private ManagedChannel                                  channel;
  private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub stub;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
    GlobalConfiguration.GRPC_MAX_MESSAGE_SIZE.setValue(1);
  }

  @BeforeEach
  void connect() {
    channel = ManagedChannelBuilder.forAddress("localhost", getServerGrpcPort()).usePlaintext()
        .maxInboundMessageSize(256 * 1024 * 1024).build();
    final Channel authenticated = ClientInterceptors.intercept(channel, new ClientInterceptor() {
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
    });
    stub = ArcadeDbServiceGrpc.newBlockingStub(authenticated);
  }

  @AfterEach
  void disconnect() throws InterruptedException {
    if (channel != null) {
      channel.shutdown();
      channel.awaitTermination(5, TimeUnit.SECONDS);
    }
    GlobalConfiguration.GRPC_MAX_MESSAGE_SIZE.reset();
  }

  private DatabaseCredentials credentials() {
    return DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build();
  }

  /** A row whose single column is a list nested {@code depth} times, each level holding the one below it twice. */
  private static String nestedListQuery(final int depth) {
    final StringBuilder query = new StringBuilder("WITH [1] AS a");
    for (int i = 0; i < depth; i++)
      query.append(" WITH [a, a] AS a");
    return query.append(" RETURN a").toString();
  }

  private static String nestedMapQuery(final int depth) {
    final StringBuilder query = new StringBuilder("WITH {k: 1} AS a");
    for (int i = 0; i < depth; i++)
      query.append(" WITH {x: a, y: a} AS a");
    return query.append(" RETURN a").toString();
  }

  private ExecuteQueryRequest queryRequest(final String query) {
    return ExecuteQueryRequest.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials()).setLanguage("opencypher")
        .setQuery(query).build();
  }

  private void assertRefusedAsTooLarge(final StatusRuntimeException refusal) {
    assertThat(refusal).as("the result must be refused").isNotNull();
    assertThat(refusal.getStatus().getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
    assertThat(refusal.getStatus().getDescription()).contains("too large to encode");
  }

  private void assertServerStillAnswers() {
    final ExecuteQueryResponse response = stub.executeQuery(queryRequest("RETURN 1 AS one"));
    assertThat(response.getResults(0).getRecords(0).getPropertiesMap().get("one").getInt64Value()).isEqualTo(1L);
  }

  @Test
  void executeQueryRefusesAListLargerThanTheCeiling() {
    final ExecuteQueryRequest request = queryRequest(nestedListQuery(NESTING));

    assertRefusedAsTooLarge(catchThrowableOfType(StatusRuntimeException.class, () -> stub.executeQuery(request)));
    assertServerStillAnswers();
  }

  @Test
  void executeQueryRefusesAMapLargerThanTheCeiling() {
    final ExecuteQueryRequest request = queryRequest(nestedMapQuery(NESTING));

    assertRefusedAsTooLarge(catchThrowableOfType(StatusRuntimeException.class, () -> stub.executeQuery(request)));
    assertServerStillAnswers();
  }

  @Test
  void executeCommandRefusesAListLargerThanTheCeiling() {
    final ExecuteCommandRequest request = ExecuteCommandRequest.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials())
        .setLanguage("opencypher").setCommand(nestedListQuery(NESTING)).setReturnRows(true).build();

    assertRefusedAsTooLarge(catchThrowableOfType(StatusRuntimeException.class, () -> stub.executeCommand(request)));
    assertServerStillAnswers();
  }

  @Test
  void streamQueryRefusesAListLargerThanTheCeiling() {
    for (final StreamQueryRequest.RetrievalMode mode : new StreamQueryRequest.RetrievalMode[] {
        StreamQueryRequest.RetrievalMode.CURSOR, StreamQueryRequest.RetrievalMode.MATERIALIZE_ALL }) {
      final StreamQueryRequest request = StreamQueryRequest.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials())
          .setLanguage("opencypher").setQuery(nestedListQuery(NESTING)).setBatchSize(10).setRetrievalMode(mode).build();

      assertRefusedAsTooLarge(catchThrowableOfType(StatusRuntimeException.class, () -> {
        final Iterator<QueryResult> batches = stub.streamQuery(request);
        while (batches.hasNext())
          batches.next();
      }));
    }
    assertServerStillAnswers();
  }

  @Test
  void aResultThatFitsIsAnsweredAsBefore() {
    // 2^8 leaves: a few KB in the encoding, far below the ceiling
    final ExecuteQueryResponse response = stub.executeQuery(queryRequest(nestedListQuery(8)));

    assertThat(response.getResults(0).getRecordsCount()).isEqualTo(1);
    final GrpcValue list = response.getResults(0).getRecords(0).getPropertiesMap().get("a");
    assertThat(list.getListValue().getValuesCount()).isEqualTo(2);

    // and a wide result: thousands of values in one row
    final ExecuteQueryResponse wide = stub.executeQuery(queryRequest("RETURN range(1, 5000) AS numbers"));
    assertThat(wide.getResults(0).getRecords(0).getPropertiesMap().get("numbers").getListValue().getValuesCount()).isEqualTo(5000);
  }
}
