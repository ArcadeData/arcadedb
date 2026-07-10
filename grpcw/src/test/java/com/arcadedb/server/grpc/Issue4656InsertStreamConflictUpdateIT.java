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
import org.junit.jupiter.api.Tag;
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
 * <p>
 * Coverage gap: fix (1) - retrying a {@code DuplicatedKeyException} as an update - resolves the race
 * between two <i>concurrent</i> streams inserting the same new key. That race surfaces the conflict
 * at commit time and is not deterministically reproducible single-threaded, so the retry branch in
 * the {@code DuplicatedKeyException} handler is not directly exercised here; the tests cover the
 * reachable same-stream (read-your-own-writes) and same-key edge paths instead.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
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
    final ExecuteCommandResponse resp = authenticatedStub.executeCommand(ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName()).setCredentials(credentials()).setCommand(sql).build());
    assertThat(resp.getSuccess()).as("setup command failed: %s -> %s", sql, resp.getMessage()).isTrue();
  }

  private GrpcRecord firstRecord(final String query) {
    final ExecuteQueryResponse resp = authenticatedStub.executeQuery(ExecuteQueryRequest.newBuilder()
        .setDatabase(getDatabaseName()).setCredentials(credentials()).setQuery(query).build());
    assertThat(resp.getResultsList()).as("query returned no result set: %s", query).isNotEmpty();
    assertThat(resp.getResultsList().getFirst().getRecordsList()).as("query returned no records: %s", query).isNotEmpty();
    return resp.getResultsList().getFirst().getRecordsList().getFirst();
  }

  private String firstString(final String query, final String prop) {
    final GrpcRecord rec = firstRecord(query);
    assertThat(rec.getPropertiesMap()).as("missing property '%s' in: %s", prop, query).containsKey(prop);
    return rec.getPropertiesMap().get(prop).getStringValue();
  }

  private String firstRid(final String query) {
    return firstRecord(query).getRid();
  }

  private long firstLong(final String query, final String prop) {
    final GrpcRecord rec = firstRecord(query);
    assertThat(rec.getPropertiesMap()).as("missing property '%s' in: %s", prop, query).containsKey(prop);
    return rec.getPropertiesMap().get(prop).getInt64Value();
  }

  /** Runs a single-chunk {@code InsertStream} in PER_STREAM mode and returns the summary. */
  private InsertSummary runStream(final String typeName, final InsertOptions.ConflictMode mode,
      final List<String> keyColumns, final List<String> updateColumns, final GrpcRecord... rows) throws Exception {
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
        .setConflictMode(mode)
        .addAllKeyColumns(keyColumns)
        .setTransactionMode(InsertOptions.TransactionMode.PER_STREAM);
    opts.addAllUpdateColumnsOnConflict(updateColumns);

    final InsertChunk.Builder chunk = InsertChunk.newBuilder()
        .setSessionId("issue-4656").setChunkSeq(0).setLast(true)
        .setOptions(opts.build());
    for (final GrpcRecord row : rows)
      chunk.addRows(row);
    req.onNext(chunk.build());
    req.onCompleted();

    assertThat(done.await(30, TimeUnit.SECONDS)).isTrue();
    assertThat(errorRef.get()).isNull();
    assertThat(summaryRef.get()).isNotNull();
    return summaryRef.get();
  }

  /** Sends a single-row {@code CONFLICT_UPDATE} {@code InsertStream} keyed on "k" and returns the summary. */
  private InsertSummary upsert(final String typeName, final GrpcRecord row, final List<String> updateColumns)
      throws Exception {
    return runStream(typeName, InsertOptions.ConflictMode.CONFLICT_UPDATE, List.of("k"), updateColumns, row);
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
      // The row must actually be merged, not silently left unchanged.
      assertThat(firstString("SELECT v FROM " + typeName + " WHERE k = 'a'", "v")).isEqualTo("changed");
      assertThat(firstLong("SELECT count(*) AS cnt FROM " + typeName, "cnt")).isEqualTo(1);
    } finally {
      cmd("DROP TYPE " + typeName + " IF EXISTS UNSAFE");
    }
  }

  @Test
  void conflictUpdateWithSameKeyTwiceInOneStreamDoesNotLoseTheRow() throws Exception {
    // The same (new) key appearing twice in a single CONFLICT_UPDATE stream must resolve to one
    // stored row (insert then update/merge), never a lost-row CONFLICT error.
    //
    // Coverage note: within one transaction the second row's upsert SELECT sees the first row's
    // pending index entry (read-your-own-writes), so this resolves via the normal upsert path. The
    // DuplicatedKeyException retry (fix 1) targets two *concurrent* streams racing the same new key;
    // that cross-stream race surfaces the conflict at commit time and is not deterministically
    // reproducible in a single-threaded test, so it is not directly exercised here.
    final String typeName = "Issue4656DupInStream_" + System.currentTimeMillis();
    cmd("CREATE DOCUMENT TYPE " + typeName);
    cmd("CREATE PROPERTY " + typeName + ".k STRING");
    cmd("CREATE PROPERTY " + typeName + ".v STRING");
    cmd("CREATE INDEX ON " + typeName + "(k) UNIQUE");
    try {
      final GrpcRecord first = GrpcRecord.newBuilder().setType(typeName)
          .putProperties("k", stringValue("a")).putProperties("v", stringValue("first")).build();
      final GrpcRecord second = GrpcRecord.newBuilder().setType(typeName)
          .putProperties("k", stringValue("a")).putProperties("v", stringValue("second")).build();

      final InsertSummary summary = runStream(typeName, InsertOptions.ConflictMode.CONFLICT_UPDATE, List.of("k"), List.of(), first, second);

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
  void conflictUpdateWithNoKeyColumnsCannotMatchAndReportsConflict() throws Exception {
    // Documents current behavior: CONFLICT_UPDATE without key_columns cannot resolve a match
    // (tryUpsertByRecord/keyExistsByRecord short-circuit on empty keys), so a unique-index duplicate
    // is reported as a failed row rather than merged. Guards against a silent behavior change.
    final String typeName = "Issue4656NoKeys_" + System.currentTimeMillis();
    cmd("CREATE DOCUMENT TYPE " + typeName);
    cmd("CREATE PROPERTY " + typeName + ".v STRING");
    cmd("CREATE INDEX ON " + typeName + "(v) UNIQUE");
    cmd("INSERT INTO " + typeName + " SET v = 'dup'");
    try {
      final GrpcRecord row = GrpcRecord.newBuilder().setType(typeName).putProperties("v", stringValue("dup")).build();

      // Empty key_columns on purpose.
      final InsertSummary summary = runStream(typeName, InsertOptions.ConflictMode.CONFLICT_UPDATE, List.of(), List.of(), row);

      assertThat(summary.getUpdated()).isEqualTo(0);
      assertThat(summary.getFailed()).isEqualTo(1);
      assertThat(summary.getErrorsList()).anyMatch(e -> "CONFLICT".equals(e.getCode()));
      // The duplicate was not stored: still exactly the pre-existing row.
      assertThat(firstLong("SELECT count(*) AS cnt FROM " + typeName, "cnt")).isEqualTo(1);
    } finally {
      cmd("DROP TYPE " + typeName + " IF EXISTS UNSAFE");
    }
  }

  @Test
  void conflictUpdateWithBlankKeyColumnReportsInvalidKeyColumn() throws Exception {
    final String typeName = "Issue4656BlankKey_" + System.currentTimeMillis();
    cmd("CREATE DOCUMENT TYPE " + typeName);
    cmd("CREATE PROPERTY " + typeName + ".v STRING");
    try {
      final GrpcRecord row = GrpcRecord.newBuilder().setType(typeName).putProperties("v", stringValue("x")).build();

      // A blank key column must yield a clean INVALID_KEY_COLUMN error, not a generic DB_ERROR.
      final InsertSummary summary = runStream(typeName, InsertOptions.ConflictMode.CONFLICT_UPDATE, List.of(""), List.of(), row);

      assertThat(summary.getErrorsList()).anyMatch(e -> "INVALID_KEY_COLUMN".equals(e.getCode()));
      assertThat(summary.getInserted()).isEqualTo(0);
      assertThat(summary.getUpdated()).isEqualTo(0);
    } finally {
      cmd("DROP TYPE " + typeName + " IF EXISTS UNSAFE");
    }
  }

  @Test
  void conflictUpdateWithQuotedKeyColumnName() throws Exception {
    // A key column whose name needs SQL quoting (here a space) must round-trip through the
    // backtick-quoted upsert query - proves quoteName() neutralizes special identifier characters.
    final String typeName = "Issue4656QuotedKey_" + System.currentTimeMillis();
    cmd("CREATE DOCUMENT TYPE " + typeName);
    cmd("CREATE PROPERTY " + typeName + ".`my key` STRING");
    cmd("CREATE PROPERTY " + typeName + ".v STRING");
    cmd("CREATE INDEX ON " + typeName + "(`my key`) UNIQUE");
    cmd("INSERT INTO " + typeName + " SET `my key` = 'a', v = 'orig'");
    try {
      final GrpcRecord row = GrpcRecord.newBuilder().setType(typeName)
          .putProperties("my key", stringValue("a")).putProperties("v", stringValue("changed")).build();

      final InsertSummary summary = runStream(typeName, InsertOptions.ConflictMode.CONFLICT_UPDATE, List.of("my key"), List.of(), row);

      assertThat(summary.getUpdated()).isEqualTo(1);
      assertThat(summary.getFailed()).isEqualTo(0);
      assertThat(firstString("SELECT v FROM " + typeName + " WHERE `my key` = 'a'", "v")).isEqualTo("changed");
      assertThat(firstLong("SELECT count(*) AS cnt FROM " + typeName, "cnt")).isEqualTo(1);
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

  @Test
  void conflictIgnoreSkipsExistingVertex() throws Exception {
    final String typeName = "Issue4656VertexIgn_" + System.currentTimeMillis();
    cmd("CREATE VERTEX TYPE " + typeName);
    cmd("CREATE PROPERTY " + typeName + ".k STRING");
    cmd("CREATE PROPERTY " + typeName + ".v STRING");
    cmd("CREATE INDEX ON " + typeName + "(k) UNIQUE");
    cmd("CREATE VERTEX " + typeName + " SET k = 'a', v = 'orig'");
    try {
      final GrpcRecord row = GrpcRecord.newBuilder().setType(typeName)
          .putProperties("k", stringValue("a")).putProperties("v", stringValue("changed")).build();

      final InsertSummary summary = runStream(typeName, InsertOptions.ConflictMode.CONFLICT_IGNORE, List.of("k"), List.of(), row);

      assertThat(summary.getIgnored()).isEqualTo(1);
      assertThat(summary.getInserted()).isEqualTo(0);
      assertThat(summary.getFailed()).isEqualTo(0);
      // The pre-existing vertex must be left untouched.
      assertThat(firstString("SELECT v FROM " + typeName + " WHERE k = 'a'", "v")).isEqualTo("orig");
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

      // The edge must be updated, not inserted, and not failed.
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
  void edgeConflictUpdateWithEmptyUpdateColumnsMergesWithoutTouchingEndpoints() throws Exception {
    // Merge-all path on an edge: every non-key property is merged, but out/in must NOT be written
    // (neither as topology nor as shadowing plain properties), even though the incoming record
    // carries a mismatched endpoint.
    final long suffix = System.currentTimeMillis();
    final String vType = "Issue4656NodeMrg_" + suffix;
    final String eType = "Issue4656EdgeMrg_" + suffix;
    cmd("CREATE VERTEX TYPE " + vType);
    cmd("CREATE PROPERTY " + vType + ".name STRING");
    cmd("CREATE VERTEX " + vType + " SET name = 'A'");
    cmd("CREATE VERTEX " + vType + " SET name = 'B'");
    cmd("CREATE VERTEX " + vType + " SET name = 'C'");
    cmd("CREATE EDGE TYPE " + eType);
    cmd("CREATE PROPERTY " + eType + ".k STRING");
    cmd("CREATE PROPERTY " + eType + ".v STRING");
    cmd("CREATE INDEX ON " + eType + "(k) UNIQUE");

    final String ridA = firstRid("SELECT FROM " + vType + " WHERE name = 'A'");
    final String ridB = firstRid("SELECT FROM " + vType + " WHERE name = 'B'");
    final String ridC = firstRid("SELECT FROM " + vType + " WHERE name = 'C'");
    cmd("CREATE EDGE " + eType + " FROM " + ridA + " TO " + ridB + " SET k = 'e1', v = 'orig'");
    try {
      // Empty update columns, and a deliberately mismatched 'in' endpoint (C instead of B).
      final GrpcRecord row = GrpcRecord.newBuilder().setType(eType)
          .putProperties("out", stringValue(ridA)).putProperties("in", stringValue(ridC))
          .putProperties("k", stringValue("e1")).putProperties("v", stringValue("changed"))
          .putProperties("w", stringValue("extra")).build();

      final InsertSummary summary = runStream(eType, InsertOptions.ConflictMode.CONFLICT_UPDATE, List.of("k"), List.of(), row);

      assertThat(summary.getUpdated()).isEqualTo(1);
      assertThat(summary.getFailed()).isEqualTo(0);

      // Non-key, non-endpoint properties were merged.
      final GrpcRecord edge = firstRecord("SELECT FROM " + eType + " WHERE k = 'e1'");
      assertThat(edge.getPropertiesMap().get("v").getStringValue()).isEqualTo("changed");
      assertThat(edge.getPropertiesMap().get("w").getStringValue()).isEqualTo("extra");
      // out/in were not written as plain properties.
      assertThat(edge.getPropertiesMap()).doesNotContainKey("out").doesNotContainKey("in");
      // Topology was not re-pointed: the edge still connects A -> B, not A -> C.
      assertThat(firstLong("SELECT count(*) AS cnt FROM (SELECT expand(out('" + eType + "')) FROM " + ridA + ") WHERE name = 'B'", "cnt"))
          .isEqualTo(1);
      assertThat(firstLong("SELECT count(*) AS cnt FROM (SELECT expand(out('" + eType + "')) FROM " + ridA + ") WHERE name = 'C'", "cnt"))
          .isEqualTo(0);
      assertThat(firstLong("SELECT count(*) AS cnt FROM " + eType, "cnt")).isEqualTo(1);
    } finally {
      cmd("DROP TYPE " + eType + " IF EXISTS UNSAFE");
      cmd("DROP TYPE " + vType + " IF EXISTS UNSAFE");
    }
  }

  @Test
  void edgeConflictUpdateIgnoresEndpointColumnsListedExplicitly() throws Exception {
    // Even when a caller explicitly lists 'in' in update_columns_on_conflict, the endpoint must not
    // be re-pointed or written as a plain property; only the other listed columns are merged.
    final long suffix = System.currentTimeMillis();
    final String vType = "Issue4656NodeExpl_" + suffix;
    final String eType = "Issue4656EdgeExpl_" + suffix;
    cmd("CREATE VERTEX TYPE " + vType);
    cmd("CREATE PROPERTY " + vType + ".name STRING");
    cmd("CREATE VERTEX " + vType + " SET name = 'A'");
    cmd("CREATE VERTEX " + vType + " SET name = 'B'");
    cmd("CREATE VERTEX " + vType + " SET name = 'C'");
    cmd("CREATE EDGE TYPE " + eType);
    cmd("CREATE PROPERTY " + eType + ".k STRING");
    cmd("CREATE PROPERTY " + eType + ".v STRING");
    cmd("CREATE INDEX ON " + eType + "(k) UNIQUE");

    final String ridA = firstRid("SELECT FROM " + vType + " WHERE name = 'A'");
    final String ridB = firstRid("SELECT FROM " + vType + " WHERE name = 'B'");
    final String ridC = firstRid("SELECT FROM " + vType + " WHERE name = 'C'");
    cmd("CREATE EDGE " + eType + " FROM " + ridA + " TO " + ridB + " SET k = 'e1', v = 'orig'");
    try {
      final GrpcRecord row = GrpcRecord.newBuilder().setType(eType)
          .putProperties("out", stringValue(ridA)).putProperties("in", stringValue(ridC))
          .putProperties("k", stringValue("e1")).putProperties("v", stringValue("changed")).build();

      // 'in' is listed explicitly but must still be ignored.
      final InsertSummary summary = runStream(eType, InsertOptions.ConflictMode.CONFLICT_UPDATE, List.of("k"), List.of("v", "in"), row);

      assertThat(summary.getUpdated()).isEqualTo(1);
      assertThat(summary.getFailed()).isEqualTo(0);

      final GrpcRecord edge = firstRecord("SELECT FROM " + eType + " WHERE k = 'e1'");
      assertThat(edge.getPropertiesMap().get("v").getStringValue()).isEqualTo("changed");
      assertThat(edge.getPropertiesMap()).doesNotContainKey("in");
      // Endpoint untouched: still A -> B, not A -> C.
      assertThat(firstLong("SELECT count(*) AS cnt FROM (SELECT expand(out('" + eType + "')) FROM " + ridA + ") WHERE name = 'B'", "cnt"))
          .isEqualTo(1);
      assertThat(firstLong("SELECT count(*) AS cnt FROM (SELECT expand(out('" + eType + "')) FROM " + ridA + ") WHERE name = 'C'", "cnt"))
          .isEqualTo(0);
    } finally {
      cmd("DROP TYPE " + eType + " IF EXISTS UNSAFE");
      cmd("DROP TYPE " + vType + " IF EXISTS UNSAFE");
    }
  }

  @Test
  void edgeConflictUpdateSameKeyTwiceInOneStreamCreatesNoGhostEdge() throws Exception {
    // The same new edge key appearing twice in one CONFLICT_UPDATE stream must yield exactly one
    // edge wired into the vertex edge-lists - no orphan/ghost edge record and no dangling endpoint.
    final long suffix = System.currentTimeMillis();
    final String vType = "Issue4656NodeGhost_" + suffix;
    final String eType = "Issue4656EdgeGhost_" + suffix;
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
    try {
      final GrpcRecord first = GrpcRecord.newBuilder().setType(eType)
          .putProperties("out", stringValue(ridA)).putProperties("in", stringValue(ridB))
          .putProperties("k", stringValue("e1")).putProperties("v", stringValue("first")).build();
      final GrpcRecord second = GrpcRecord.newBuilder().setType(eType)
          .putProperties("out", stringValue(ridA)).putProperties("in", stringValue(ridB))
          .putProperties("k", stringValue("e1")).putProperties("v", stringValue("second")).build();

      final InsertSummary summary = runStream(eType, InsertOptions.ConflictMode.CONFLICT_UPDATE, List.of("k"), List.of(), first, second);

      assertThat(summary.getFailed()).isEqualTo(0);
      assertThat(summary.getInserted() + summary.getUpdated()).isEqualTo(2);
      // Exactly one edge record (no orphan), merged to the last value.
      assertThat(firstLong("SELECT count(*) AS cnt FROM " + eType, "cnt")).isEqualTo(1);
      assertThat(firstString("SELECT v FROM " + eType + " WHERE k = 'e1'", "v")).isEqualTo("second");
      // A is wired to exactly one neighbour (the edge-list has no dangling/duplicate entry).
      assertThat(firstLong("SELECT count(*) AS cnt FROM (SELECT expand(out('" + eType + "')) FROM " + ridA + ")", "cnt"))
          .isEqualTo(1);
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
      final GrpcRecord row = GrpcRecord.newBuilder().setType(eType)
          .putProperties("out", stringValue(ridA)).putProperties("in", stringValue(ridB))
          .putProperties("k", stringValue("e1")).putProperties("v", stringValue("changed")).build();

      final InsertSummary summary = runStream(eType, InsertOptions.ConflictMode.CONFLICT_IGNORE, List.of("k"), List.of(), row);

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

  @Test
  void edgeConflictErrorReportsDuplicateKeyAsFailed() throws Exception {
    // CONFLICT_ERROR (the default) on an edge: a duplicate key is reported as a failed row and the
    // pre-existing edge is left intact (no second edge, no ghost).
    final long suffix = System.currentTimeMillis();
    final String vType = "Issue4656NodeErr_" + suffix;
    final String eType = "Issue4656EdgeErr_" + suffix;
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

      final InsertSummary summary = runStream(eType, InsertOptions.ConflictMode.CONFLICT_ERROR, List.of("k"), List.of(), row);

      assertThat(summary.getUpdated()).isEqualTo(0);
      assertThat(summary.getFailed()).isGreaterThanOrEqualTo(1);
      assertThat(summary.getErrorsList()).anyMatch(e -> "CONFLICT".equals(e.getCode()));
      assertThat(firstString("SELECT v FROM " + eType + " WHERE k = 'e1'", "v")).isEqualTo("orig");
      assertThat(firstLong("SELECT count(*) AS cnt FROM " + eType, "cnt")).isEqualTo(1);
    } finally {
      cmd("DROP TYPE " + eType + " IF EXISTS UNSAFE");
      cmd("DROP TYPE " + vType + " IF EXISTS UNSAFE");
    }
  }

  @Test
  void edgeConflictAbortReportsDuplicateKeyAsFailed() throws Exception {
    // CONFLICT_ABORT on an edge behaves like CONFLICT_ERROR: the duplicate is a failed row and the
    // pre-existing edge is left intact.
    final long suffix = System.currentTimeMillis();
    final String vType = "Issue4656NodeAbrt_" + suffix;
    final String eType = "Issue4656EdgeAbrt_" + suffix;
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

      final InsertSummary summary = runStream(eType, InsertOptions.ConflictMode.CONFLICT_ABORT, List.of("k"), List.of(), row);

      assertThat(summary.getUpdated()).isEqualTo(0);
      assertThat(summary.getFailed()).isGreaterThanOrEqualTo(1);
      assertThat(summary.getErrorsList()).anyMatch(e -> "CONFLICT".equals(e.getCode()));
      assertThat(firstString("SELECT v FROM " + eType + " WHERE k = 'e1'", "v")).isEqualTo("orig");
      assertThat(firstLong("SELECT count(*) AS cnt FROM " + eType, "cnt")).isEqualTo(1);
    } finally {
      cmd("DROP TYPE " + eType + " IF EXISTS UNSAFE");
      cmd("DROP TYPE " + vType + " IF EXISTS UNSAFE");
    }
  }
}
