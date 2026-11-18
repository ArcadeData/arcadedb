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
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * A server-streaming call the client cancels stops on the server at once. The handler runs inside the call's half-close
 * callback, and gRPC delivers the cancel handler on the same serialized executor, so the cancel was only seen once the stream
 * was over: the stream went on waiting for the transport until arcadedb.server.grpcStreamWriteTimeoutMs, with its worker
 * thread, its result set and its query admission slot. arcadedb.queryMaxConcurrent cancelled streams then made every remote
 * query, HTTP included, wait in the admission queue.
 * <p>
 * The write timeout is raised far above the waits of these tests, so a stream still listed as running a few seconds after its
 * client cancelled can only be one that did not see the cancel.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class GrpcCancelledStreamReleasesItsSlotIT extends BaseGrpcServerTest {
  private static final String DOCUMENT_TYPE = "Many";
  private static final String SERIES        = "Samples";
  private static final int    ROWS          = 20_000;
  private static final int    SERIES_FIELDS = 24;

  private ManagedChannel                          channel;
  private ArcadeDbServiceGrpc.ArcadeDbServiceStub stub;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
    GlobalConfiguration.SERVER_GRPC_STREAM_WRITE_TIMEOUT_MS.setValue(120_000L);
  }

  @BeforeEach
  void setupGrpcClient() {
    final Database db = getServerDatabase(0, getDatabaseName());
    db.getSchema().createDocumentType(DOCUMENT_TYPE);
    // About 4 MB of rows, well above the 1 MB flow-control window: a client that stops reading leaves the server waiting for
    // the transport
    db.transaction(() -> {
      for (int i = 0; i < ROWS; i++)
        db.newDocument(DOCUMENT_TYPE).set("i", i, "pad", "x".repeat(200)).save();
    });

    channel = ManagedChannelBuilder.forAddress("localhost", getServerGrpcPort()).usePlaintext().build();
    stub = ArcadeDbServiceGrpc.newStub(channel).withInterceptors(authentication());
  }

  @AfterEach
  void shutdownGrpcClient() throws InterruptedException {
    try {
      if (channel != null) {
        channel.shutdownNow();
        channel.awaitTermination(5, TimeUnit.SECONDS);
      }
    } finally {
      GlobalConfiguration.SERVER_GRPC_STREAM_WRITE_TIMEOUT_MS.reset();
    }
  }

  @Test
  void aCancelledStreamQueryStopsAndLeavesNoRunningQuery() throws Exception {
    final CancelOnFirstMessage<StreamQueryRequest, QueryResult> client = new CancelOnFirstMessage<>();
    stub.streamQuery(streamQuery(null), client);

    assertCancelledStreamIsGone(client);
  }

  /**
   * Inside a client-managed transaction the stream runs on the transaction's own thread while the handler waits for it: the
   * producer reads the same flag, so it has to see the cancel as well.
   */
  @Test
  void aCancelledStreamQueryInsideATransactionStopsAndLeavesNoRunningQuery() throws Exception {
    final ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub blocking = ArcadeDbServiceGrpc.newBlockingStub(channel)
        .withInterceptors(authentication());
    final TransactionContext transaction = TransactionContext.newBuilder()
        .setTransactionId(blocking.beginTransaction(
            BeginTransactionRequest.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials()).build()).getTransactionId())
        .setDatabase(getDatabaseName())
        .build();
    try {
      final CancelOnFirstMessage<StreamQueryRequest, QueryResult> client = new CancelOnFirstMessage<>();
      stub.streamQuery(streamQuery(transaction), client);

      assertCancelledStreamIsGone(client);
    } finally {
      blocking.rollbackTransaction(RollbackTransactionRequest.newBuilder().setCredentials(credentials()).setTransaction(transaction).build());
    }
  }

  @Test
  void aCancelledTimeSeriesQueryStopsAndLeavesNoRunningQuery() throws Exception {
    createSeries();

    final CancelOnFirstMessage<TimeSeriesQueryRequest, TimeSeriesQueryResult> client = new CancelOnFirstMessage<>();
    stub.timeSeriesQuery(TimeSeriesQueryRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setType(SERIES)
        .setBatchSize(10)
        .build(), client);

    assertCancelledStreamIsGone(client);
  }

  private StreamQueryRequest streamQuery(final TransactionContext transaction) {
    final StreamQueryRequest.Builder request = StreamQueryRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setQuery("SELECT FROM " + DOCUMENT_TYPE)
        .setBatchSize(10);
    if (transaction != null)
      request.setTransaction(transaction);
    return request.build();
  }

  /** A series of {@code ROWS} points of {@code SERIES_FIELDS} double fields, a stream of several MB, written in chunks. */
  private void createSeries() {
    final ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub blocking = ArcadeDbServiceGrpc.newBlockingStub(channel)
        .withInterceptors(authentication());

    final StringBuilder fields = new StringBuilder();
    for (int f = 0; f < SERIES_FIELDS; f++)
      fields.append(f > 0 ? ", " : "").append("f").append(f).append(" DOUBLE");
    blocking.executeCommand(ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setCommand("CREATE TIMESERIES TYPE " + SERIES + " TIMESTAMP ts TAGS (host STRING) FIELDS (" + fields + ")")
        .build());

    final long base = 1_700_000_000_000L;
    final int chunk = 1_000;
    for (int from = 0; from < ROWS; from += chunk) {
      final TimeSeriesWriteRequest.Builder write = TimeSeriesWriteRequest.newBuilder()
          .setDatabase(getDatabaseName())
          .setCredentials(credentials())
          .setType(SERIES);
      for (int i = from; i < from + chunk; i++) {
        final TimeSeriesPoint.Builder point = TimeSeriesPoint.newBuilder()
            .setTimestamp(base + i * 1_000L)
            .putTags("host", GrpcValue.newBuilder().setStringValue("web1").build());
        for (int f = 0; f < SERIES_FIELDS; f++)
          point.putFields("f" + f, GrpcValue.newBuilder().setDoubleValue(i + f / 10.0).build());
        write.addPoints(point);
      }
      assertThat(blocking.timeSeriesWrite(write.build()).getWritten()).isEqualTo(chunk);
    }
  }

  private void assertCancelledStreamIsGone(final CancelOnFirstMessage<?, ?> client) throws InterruptedException {
    assertThat(client.firstMessage.await(30, TimeUnit.SECONDS)).as("the stream answered a first batch").isTrue();
    assertThat(client.closed.await(30, TimeUnit.SECONDS)).as("the client saw its call end").isTrue();

    await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(getServer(0).getRunningQueries().getRunning())
        .as("the cancelled stream is still running on the server, holding its admission slot").isEmpty());
  }

  private GrpcTestAuthInterceptor authentication() {
    return new GrpcTestAuthInterceptor("root", DEFAULT_PASSWORD_FOR_TESTS, getDatabaseName());
  }

  private DatabaseCredentials credentials() {
    return DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build();
  }

  /** Cancels its call as soon as the first message arrives and reports when it saw that message and when the call ended. */
  private static final class CancelOnFirstMessage<ReqT, RespT> implements ClientResponseObserver<ReqT, RespT> {
    private final CountDownLatch firstMessage = new CountDownLatch(1);
    private final CountDownLatch closed       = new CountDownLatch(1);

    private ClientCallStreamObserver<ReqT> call;

    @Override
    public void beforeStart(final ClientCallStreamObserver<ReqT> requestStream) {
      call = requestStream;
    }

    @Override
    public void onNext(final RespT value) {
      firstMessage.countDown();
      call.cancel("the client is done", null);
    }

    @Override
    public void onError(final Throwable t) {
      closed.countDown();
    }

    @Override
    public void onCompleted() {
      closed.countDown();
    }
  }
}
