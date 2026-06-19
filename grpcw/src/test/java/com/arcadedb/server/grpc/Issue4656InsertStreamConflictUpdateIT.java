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
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #4656: gRPC {@code InsertStream} {@code CONFLICT_UPDATE} upsert gaps.
 * <ul>
 *   <li>(2) an empty {@code update_columns_on_conflict} used to be reported as {@code updated} while
 *   leaving the record unchanged; it must now merge all non-key columns of the incoming record.</li>
 *   <li>(3) edge targets used to ignore {@code conflict_mode}/{@code key_columns} entirely; they must
 *   now honour {@code CONFLICT_UPDATE} and {@code CONFLICT_IGNORE} like documents and vertices.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue4656InsertStreamConflictUpdateIT extends BaseGraphServerTest {

  private static final int GRPC_PORT = 50051;

  private static final Metadata.Key<String> USER_HEADER     = Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> PASSWORD_HEADER = Metadata.Key.of("x-arcade-password", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> DATABASE_HEADER = Metadata.Key.of("x-arcade-database", Metadata.ASCII_STRING_MARSHALLER);

  private ManagedChannel                                  channel;
  private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub authenticatedStub;
  private ArcadeDbServiceGrpc.ArcadeDbServiceStub         asyncAuthenticatedStub;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @BeforeEach
  void setupGrpcClient() {
    channel = ManagedChannelBuilder.forAddress("localhost", GRPC_PORT).usePlaintext().build();
    final Channel authenticatedChannel = ClientInterceptors.intercept(channel, new AuthClientInterceptor());
    authenticatedStub = ArcadeDbServiceGrpc.newBlockingStub(authenticatedChannel);
    asyncAuthenticatedStub = ArcadeDbServiceGrpc.newStub(authenticatedChannel);
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
    return DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build();
  }

  private GrpcValue stringValue(final String s) {
    return GrpcValue.newBuilder().setStringValue(s).build();
  }

  private void cmd(final String sql) {
    authenticatedStub.executeCommand(ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName()).setCredentials(credentials()).setCommand(sql).build());
  }

  private String firstString(final String query, final String prop) {
    final ExecuteQueryResponse resp = authenticatedStub.executeQuery(ExecuteQueryRequest.newBuilder()
        .setDatabase(getDatabaseName()).setCredentials(credentials()).setQuery(query).build());
    return resp.getResultsList().get(0).getRecordsList().get(0).getPropertiesMap().get(prop).getStringValue();
  }

  private String firstRid(final String query) {
    final ExecuteQueryResponse resp = authenticatedStub.executeQuery(ExecuteQueryRequest.newBuilder()
        .setDatabase(getDatabaseName()).setCredentials(credentials()).setQuery(query).build());
    return resp.getResultsList().get(0).getRecordsList().get(0).getRid();
  }

  private long firstLong(final String query, final String prop) {
    final ExecuteQueryResponse resp = authenticatedStub.executeQuery(ExecuteQueryRequest.newBuilder()
        .setDatabase(getDatabaseName()).setCredentials(credentials()).setQuery(query).build());
    return resp.getResultsList().get(0).getRecordsList().get(0).getPropertiesMap().get(prop).getInt64Value();
  }

  /** Sends a single-row {@code CONFLICT_UPDATE} {@code InsertStream} and returns the resulting summary. */
  private InsertSummary upsert(final String typeName, final GrpcRecord row, final List<String> updateColumns)
      throws Exception {
    final CountDownLatch done = new CountDownLatch(1);
    final AtomicReference<InsertSummary> summaryRef = new AtomicReference<>();
    final AtomicReference<Throwable> errorRef = new AtomicReference<>();

    final StreamObserver<InsertChunk> req = asyncAuthenticatedStub.insertStream(new StreamObserver<>() {
      @Override public void onNext(final InsertSummary s) { summaryRef.set(s); }
      @Override public void onError(final Throwable t) { errorRef.set(t); done.countDown(); }
      @Override public void onCompleted() { done.countDown(); }
    });

    final InsertOptions.Builder opts = InsertOptions.newBuilder()
        .setDatabase(getDatabaseName()).setCredentials(credentials()).setTargetClass(typeName)
        .setConflictMode(InsertOptions.ConflictMode.CONFLICT_UPDATE)
        .addKeyColumns("k")
        .setTransactionMode(InsertOptions.TransactionMode.PER_STREAM);
    opts.addAllUpdateColumnsOnConflict(updateColumns);

    req.onNext(InsertChunk.newBuilder()
        .setSessionId("issue-4656").setChunkSeq(0).setLast(true)
        .setOptions(opts.build())
        .addRows(row)
        .build());
    req.onCompleted();

    assertThat(done.await(30, TimeUnit.SECONDS)).isTrue();
    assertThat(errorRef.get()).isNull();
    assertThat(summaryRef.get()).isNotNull();
    return summaryRef.get();
  }

  // ---------------------------------------------------------------------------------------------
  // (2) Empty update_columns_on_conflict
  // ---------------------------------------------------------------------------------------------

  @Test
  void conflictUpdateWithEmptyUpdateColumnsMergesAllNonKeyColumns() throws Exception {
    final String typeName = "Issue4656EmptyCols_" + System.currentTimeMillis();
    cmd("CREATE DOCUMENT TYPE " + typeName);
    cmd("CREATE PROPERTY " + typeName + ".k STRING");
    cmd("CREATE PROPERTY " + typeName + ".v STRING");
    cmd("CREATE INDEX ON " + typeName + "(k) UNIQUE");
    cmd("INSERT INTO " + typeName + " SET k = 'a', v = 'orig'");
    try {
      final GrpcRecord row = GrpcRecord.newBuilder().setType(typeName)
          .putProperties("k", stringValue("a")).putProperties("v", stringValue("changed")).build();

      final InsertSummary summary = upsert(typeName, row, List.of()); // empty update columns

      assertThat(summary.getUpdated()).isEqualTo(1);
      assertThat(summary.getFailed()).isEqualTo(0);
      // Issue #4656 (2): the row must actually be merged, not silently left unchanged.
      assertThat(firstString("SELECT v FROM " + typeName + " WHERE k = 'a'", "v")).isEqualTo("changed");
      assertThat(firstLong("SELECT count(*) AS cnt FROM " + typeName, "cnt")).isEqualTo(1);
    } finally {
      cmd("DROP TYPE " + typeName + " IF EXISTS UNSAFE");
    }
  }

  @Test
  void conflictUpdateWithSameKeyTwiceInOneStreamDoesNotLoseTheRow() throws Exception {
    // Issue #4656 (1): the same (new) key appearing twice in a single CONFLICT_UPDATE stream must
    // resolve to one stored row (insert then update/merge), never a lost-row CONFLICT error.
    final String typeName = "Issue4656DupInStream_" + System.currentTimeMillis();
    cmd("CREATE DOCUMENT TYPE " + typeName);
    cmd("CREATE PROPERTY " + typeName + ".k STRING");
    cmd("CREATE PROPERTY " + typeName + ".v STRING");
    cmd("CREATE INDEX ON " + typeName + "(k) UNIQUE");
    try {
      final CountDownLatch done = new CountDownLatch(1);
      final AtomicReference<InsertSummary> summaryRef = new AtomicReference<>();
      final AtomicReference<Throwable> errorRef = new AtomicReference<>();

      final StreamObserver<InsertChunk> req = asyncAuthenticatedStub.insertStream(new StreamObserver<>() {
        @Override public void onNext(final InsertSummary s) { summaryRef.set(s); }
        @Override public void onError(final Throwable t) { errorRef.set(t); done.countDown(); }
        @Override public void onCompleted() { done.countDown(); }
      });

      req.onNext(InsertChunk.newBuilder()
          .setSessionId("issue-4656-dup-in-stream").setChunkSeq(0).setLast(true)
          .setOptions(InsertOptions.newBuilder()
              .setDatabase(getDatabaseName()).setCredentials(credentials()).setTargetClass(typeName)
              .setConflictMode(InsertOptions.ConflictMode.CONFLICT_UPDATE)
              .addKeyColumns("k")
              .setTransactionMode(InsertOptions.TransactionMode.PER_STREAM).build())
          .addRows(GrpcRecord.newBuilder().setType(typeName)
              .putProperties("k", stringValue("a")).putProperties("v", stringValue("first")).build())
          .addRows(GrpcRecord.newBuilder().setType(typeName)
              .putProperties("k", stringValue("a")).putProperties("v", stringValue("second")).build())
          .build());
      req.onCompleted();

      assertThat(done.await(30, TimeUnit.SECONDS)).isTrue();
      assertThat(errorRef.get()).isNull();
      assertThat(summaryRef.get()).isNotNull();

      final InsertSummary summary = summaryRef.get();
      // Exactly one row stored, the second occurrence merged on top of the first, no lost-row error.
      assertThat(summary.getFailed()).isEqualTo(0);
      assertThat(summary.getInserted() + summary.getUpdated()).isEqualTo(2);
      assertThat(firstLong("SELECT count(*) AS cnt FROM " + typeName, "cnt")).isEqualTo(1);
      assertThat(firstString("SELECT v FROM " + typeName + " WHERE k = 'a'", "v")).isEqualTo("second");
    } finally {
      cmd("DROP TYPE " + typeName + " IF EXISTS UNSAFE");
    }
  }

  @Test
  void conflictUpdateWithExplicitUpdateColumnsMerges() throws Exception {
    final String typeName = "Issue4656WithCols_" + System.currentTimeMillis();
    cmd("CREATE DOCUMENT TYPE " + typeName);
    cmd("CREATE PROPERTY " + typeName + ".k STRING");
    cmd("CREATE PROPERTY " + typeName + ".v STRING");
    cmd("CREATE INDEX ON " + typeName + "(k) UNIQUE");
    cmd("INSERT INTO " + typeName + " SET k = 'a', v = 'orig'");
    try {
      final GrpcRecord row = GrpcRecord.newBuilder().setType(typeName)
          .putProperties("k", stringValue("a")).putProperties("v", stringValue("changed")).build();

      final InsertSummary summary = upsert(typeName, row, List.of("v"));

      assertThat(summary.getUpdated()).isEqualTo(1);
      assertThat(summary.getFailed()).isEqualTo(0);
      assertThat(firstString("SELECT v FROM " + typeName + " WHERE k = 'a'", "v")).isEqualTo("changed");
      assertThat(firstLong("SELECT count(*) AS cnt FROM " + typeName, "cnt")).isEqualTo(1);
    } finally {
      cmd("DROP TYPE " + typeName + " IF EXISTS UNSAFE");
    }
  }

  @Test
  void conflictUpdateMergesAllNonKeyColumnsForVertex() throws Exception {
    final String typeName = "Issue4656Vertex_" + System.currentTimeMillis();
    cmd("CREATE VERTEX TYPE " + typeName);
    cmd("CREATE PROPERTY " + typeName + ".k STRING");
    cmd("CREATE PROPERTY " + typeName + ".v STRING");
    cmd("CREATE INDEX ON " + typeName + "(k) UNIQUE");
    cmd("CREATE VERTEX " + typeName + " SET k = 'a', v = 'orig'");
    try {
      final GrpcRecord row = GrpcRecord.newBuilder().setType(typeName)
          .putProperties("k", stringValue("a")).putProperties("v", stringValue("changed")).build();

      final InsertSummary summary = upsert(typeName, row, List.of()); // empty update columns

      assertThat(summary.getUpdated()).isEqualTo(1);
      assertThat(summary.getFailed()).isEqualTo(0);
      assertThat(firstString("SELECT v FROM " + typeName + " WHERE k = 'a'", "v")).isEqualTo("changed");
      assertThat(firstLong("SELECT count(*) AS cnt FROM " + typeName, "cnt")).isEqualTo(1);
    } finally {
      cmd("DROP TYPE " + typeName + " IF EXISTS UNSAFE");
    }
  }

  // ---------------------------------------------------------------------------------------------
  // (3) Edges honour conflict_mode
  // ---------------------------------------------------------------------------------------------

  @Test
  void edgeConflictUpdateUpdatesExistingEdgeInsteadOfFailing() throws Exception {
    final long suffix = System.currentTimeMillis();
    final String vType = "Issue4656Node_" + suffix;
    final String eType = "Issue4656Edge_" + suffix;
    cmd("CREATE VERTEX TYPE " + vType);
    cmd("CREATE PROPERTY " + vType + ".name STRING");
    cmd("CREATE VERTEX " + vType + " SET name = 'A'");
    cmd("CREATE VERTEX " + vType + " SET name = 'B'");
    cmd("CREATE EDGE TYPE " + eType);
    cmd("CREATE PROPERTY " + eType + ".k STRING");
    cmd("CREATE PROPERTY " + eType + ".v STRING");
    cmd("CREATE INDEX ON " + eType + "(k) UNIQUE");

    final String ridA = firstRid("SELECT FROM " + vType + " WHERE name = 'A'");
    final String ridB = firstRid("SELECT FROM " + vType + " WHERE name = 'B'");
    cmd("CREATE EDGE " + eType + " FROM " + ridA + " TO " + ridB + " SET k = 'e1', v = 'orig'");
    try {
      final GrpcRecord row = GrpcRecord.newBuilder().setType(eType)
          .putProperties("out", stringValue(ridA)).putProperties("in", stringValue(ridB))
          .putProperties("k", stringValue("e1")).putProperties("v", stringValue("changed")).build();

      final InsertSummary summary = upsert(eType, row, List.of("v"));

      // Issue #4656 (3): the edge must be updated, not inserted, and not failed.
      assertThat(summary.getUpdated()).isEqualTo(1);
      assertThat(summary.getInserted()).isEqualTo(0);
      assertThat(summary.getFailed()).isEqualTo(0);
      assertThat(firstString("SELECT v FROM " + eType + " WHERE k = 'e1'", "v")).isEqualTo("changed");
      assertThat(firstLong("SELECT count(*) AS cnt FROM " + eType, "cnt")).isEqualTo(1);
    } finally {
      cmd("DROP TYPE " + eType + " IF EXISTS UNSAFE");
      cmd("DROP TYPE " + vType + " IF EXISTS UNSAFE");
    }
  }

  @Test
  void edgeConflictIgnoreSkipsExistingEdge() throws Exception {
    final long suffix = System.currentTimeMillis();
    final String vType = "Issue4656NodeIgn_" + suffix;
    final String eType = "Issue4656EdgeIgn_" + suffix;
    cmd("CREATE VERTEX TYPE " + vType);
    cmd("CREATE PROPERTY " + vType + ".name STRING");
    cmd("CREATE VERTEX " + vType + " SET name = 'A'");
    cmd("CREATE VERTEX " + vType + " SET name = 'B'");
    cmd("CREATE EDGE TYPE " + eType);
    cmd("CREATE PROPERTY " + eType + ".k STRING");
    cmd("CREATE PROPERTY " + eType + ".v STRING");
    cmd("CREATE INDEX ON " + eType + "(k) UNIQUE");

    final String ridA = firstRid("SELECT FROM " + vType + " WHERE name = 'A'");
    final String ridB = firstRid("SELECT FROM " + vType + " WHERE name = 'B'");
    cmd("CREATE EDGE " + eType + " FROM " + ridA + " TO " + ridB + " SET k = 'e1', v = 'orig'");
    try {
      final CountDownLatch done = new CountDownLatch(1);
      final AtomicReference<InsertSummary> summaryRef = new AtomicReference<>();
      final AtomicReference<Throwable> errorRef = new AtomicReference<>();

      final StreamObserver<InsertChunk> req = asyncAuthenticatedStub.insertStream(new StreamObserver<>() {
        @Override public void onNext(final InsertSummary s) { summaryRef.set(s); }
        @Override public void onError(final Throwable t) { errorRef.set(t); done.countDown(); }
        @Override public void onCompleted() { done.countDown(); }
      });

      req.onNext(InsertChunk.newBuilder()
          .setSessionId("issue-4656-edge-ignore").setChunkSeq(0).setLast(true)
          .setOptions(InsertOptions.newBuilder()
              .setDatabase(getDatabaseName()).setCredentials(credentials()).setTargetClass(eType)
              .setConflictMode(InsertOptions.ConflictMode.CONFLICT_IGNORE)
              .addKeyColumns("k")
              .setTransactionMode(InsertOptions.TransactionMode.PER_STREAM).build())
          .addRows(GrpcRecord.newBuilder().setType(eType)
              .putProperties("out", stringValue(ridA)).putProperties("in", stringValue(ridB))
              .putProperties("k", stringValue("e1")).putProperties("v", stringValue("changed")).build())
          .build());
      req.onCompleted();

      assertThat(done.await(30, TimeUnit.SECONDS)).isTrue();
      assertThat(errorRef.get()).isNull();
      assertThat(summaryRef.get()).isNotNull();

      final InsertSummary summary = summaryRef.get();
      assertThat(summary.getIgnored()).isEqualTo(1);
      assertThat(summary.getInserted()).isEqualTo(0);
      assertThat(summary.getFailed()).isEqualTo(0);
      // The pre-existing edge must be left untouched.
      assertThat(firstString("SELECT v FROM " + eType + " WHERE k = 'e1'", "v")).isEqualTo("orig");
      assertThat(firstLong("SELECT count(*) AS cnt FROM " + eType, "cnt")).isEqualTo(1);
    } finally {
      cmd("DROP TYPE " + eType + " IF EXISTS UNSAFE");
      cmd("DROP TYPE " + vType + " IF EXISTS UNSAFE");
    }
  }
}
