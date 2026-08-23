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
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #6597: {@code arcadedb-server.proto} documents
 * {@code InsertChunk.database} (field 1) as REQUIRED on the first chunk of the
 * {@code insertStream} client-streaming RPC, but {@code ArcadeDbGrpcService#insertStream}'s
 * {@code onNext} handler never read it - the extraction line was commented out - and sourced the
 * database name exclusively from {@code InsertOptions.database} instead. A client that followed
 * the proto and set only {@code InsertChunk.database} on the first chunk failed with
 * {@code INVALID_ARGUMENT: Invalid database name: name is required}, even though it supplied
 * exactly what the contract requires. {@code graphBatchLoad}'s {@code onNext(GraphBatchChunk)}
 * reads {@code chunk.getDatabase()} directly and does not have this bug.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue6597InsertStreamChunkDatabaseIT extends BaseGraphServerTest {

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  private DatabaseCredentials credentials() {
    return DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build();
  }

  private GrpcValue stringValue(final String s) {
    return GrpcValue.newBuilder().setStringValue(s).build();
  }

  @Test
  void insertStreamMustHonorChunkLevelDatabaseWhenOptionsDatabaseIsUnset() throws Exception {
    final String typeName = "Issue6597ChunkDbType_" + System.currentTimeMillis();

    getServer(0).getDatabase(getDatabaseName()).command("sql", "CREATE DOCUMENT TYPE " + typeName);

    final ArcadeDbGrpcService service = new ArcadeDbGrpcService(getDatabaseName(), getServer(0));
    try {
      final AtomicReference<InsertSummary> summaryRef = new AtomicReference<>();
      final AtomicReference<Throwable> errorRef = new AtomicReference<>();
      final RecordingResponseObserver resp = new RecordingResponseObserver(summaryRef, errorRef);

      final StreamObserver<InsertChunk> req = service.insertStream(resp);

      // Per the proto contract, InsertChunk.database is REQUIRED on the first chunk.
      // InsertOptions deliberately carries NO database, so the fix must fall back to it.
      final InsertChunk chunk = InsertChunk.newBuilder()
          .setSessionId("issue-6597-chunk-database")
          .setChunkSeq(0)
          .setLast(true)
          .setDatabase(getDatabaseName())
          .setCredentials(credentials())
          .setOptions(InsertOptions.newBuilder()
              .setCredentials(credentials())
              .setTargetClass(typeName)
              .setConflictMode(InsertOptions.ConflictMode.CONFLICT_ERROR)
              .setTransactionMode(InsertOptions.TransactionMode.PER_STREAM)
              .build())
          .addRows(GrpcRecord.newBuilder().setType(typeName).putProperties("name", stringValue("chunk-db-row")).build())
          .build();

      req.onNext(chunk);
      assertThat(errorRef.get()).as("onNext should not error when InsertChunk.database is set").isNull();

      req.onCompleted();
      assertThat(errorRef.get()).as("onCompleted should not error").isNull();

      final InsertSummary summary = summaryRef.get();
      assertThat(summary).isNotNull();
      assertThat(summary.getReceived()).isEqualTo(1);
      assertThat(summary.getInserted()).isEqualTo(1);
      assertThat(summary.getFailed()).isEqualTo(0);
      assertThat(summary.getErrorsList()).isEmpty();

      final long total = getServer(0).getDatabase(getDatabaseName())
          .query("sql", "SELECT count(*) AS total FROM " + typeName).next().<Long>getProperty("total");
      assertThat(total).isEqualTo(1L);
    } finally {
      service.close();
      getServer(0).getDatabase(getDatabaseName()).command("sql", "DROP TYPE " + typeName + " IF EXISTS UNSAFE");
    }
  }

  @Test
  void insertStreamMustStillHonorOptionsDatabaseWhenChunkDatabaseIsUnset() throws Exception {
    final String typeName = "Issue6597OptionsDbType_" + System.currentTimeMillis();

    getServer(0).getDatabase(getDatabaseName()).command("sql", "CREATE DOCUMENT TYPE " + typeName);

    final ArcadeDbGrpcService service = new ArcadeDbGrpcService(getDatabaseName(), getServer(0));
    try {
      final AtomicReference<InsertSummary> summaryRef = new AtomicReference<>();
      final AtomicReference<Throwable> errorRef = new AtomicReference<>();
      final RecordingResponseObserver resp = new RecordingResponseObserver(summaryRef, errorRef);

      final StreamObserver<InsertChunk> req = service.insertStream(resp);

      // Pre-existing contract: InsertChunk.database left empty, InsertOptions.database set -
      // must keep working exactly as before the fix.
      final InsertChunk chunk = InsertChunk.newBuilder()
          .setSessionId("issue-6597-options-database-fallback")
          .setChunkSeq(0)
          .setLast(true)
          .setOptions(InsertOptions.newBuilder()
              .setDatabase(getDatabaseName())
              .setCredentials(credentials())
              .setTargetClass(typeName)
              .setConflictMode(InsertOptions.ConflictMode.CONFLICT_ERROR)
              .setTransactionMode(InsertOptions.TransactionMode.PER_STREAM)
              .build())
          .addRows(GrpcRecord.newBuilder().setType(typeName).putProperties("name", stringValue("options-db-row")).build())
          .build();

      req.onNext(chunk);
      assertThat(errorRef.get()).as("onNext should not error").isNull();

      req.onCompleted();
      assertThat(errorRef.get()).as("onCompleted should not error").isNull();

      final InsertSummary summary = summaryRef.get();
      assertThat(summary).isNotNull();
      assertThat(summary.getReceived()).isEqualTo(1);
      assertThat(summary.getInserted()).isEqualTo(1);
      assertThat(summary.getFailed()).isEqualTo(0);
    } finally {
      service.close();
      getServer(0).getDatabase(getDatabaseName()).command("sql", "DROP TYPE " + typeName + " IF EXISTS UNSAFE");
    }
  }

  @Test
  void insertStreamMustPreferChunkDatabaseOverOptionsDatabaseWhenBothAreSet() throws Exception {
    final String typeName = "Issue6597PrecedenceType_" + System.currentTimeMillis();

    getServer(0).getDatabase(getDatabaseName()).command("sql", "CREATE DOCUMENT TYPE " + typeName);

    final ArcadeDbGrpcService service = new ArcadeDbGrpcService(getDatabaseName(), getServer(0));
    try {
      final AtomicReference<InsertSummary> summaryRef = new AtomicReference<>();
      final AtomicReference<Throwable> errorRef = new AtomicReference<>();
      final RecordingResponseObserver resp = new RecordingResponseObserver(summaryRef, errorRef);

      final StreamObserver<InsertChunk> req = service.insertStream(resp);

      // Both InsertChunk.database and InsertOptions.database are set, to DIFFERENT values.
      // InsertOptions.database is deliberately a name rejected by validateDatabaseName (it
      // contains ".."), so if it were ever consulted instead of InsertChunk.database, the insert
      // would fail loudly rather than silently succeeding against the wrong database. This locks
      // in the precedence rule the fix establishes: InsertChunk.database wins.
      final InsertChunk chunk = InsertChunk.newBuilder()
          .setSessionId("issue-6597-precedence")
          .setChunkSeq(0)
          .setLast(true)
          .setDatabase(getDatabaseName())
          .setCredentials(credentials())
          .setOptions(InsertOptions.newBuilder()
              .setDatabase("../should-be-ignored")
              .setCredentials(credentials())
              .setTargetClass(typeName)
              .setConflictMode(InsertOptions.ConflictMode.CONFLICT_ERROR)
              .setTransactionMode(InsertOptions.TransactionMode.PER_STREAM)
              .build())
          .addRows(GrpcRecord.newBuilder().setType(typeName).putProperties("name", stringValue("precedence-row")).build())
          .build();

      req.onNext(chunk);
      assertThat(errorRef.get()).as("onNext should not error").isNull();

      req.onCompleted();
      assertThat(errorRef.get()).as("onCompleted should not error").isNull();

      final InsertSummary summary = summaryRef.get();
      assertThat(summary).isNotNull();
      assertThat(summary.getReceived()).isEqualTo(1);
      assertThat(summary.getInserted()).as("InsertChunk.database must take precedence over InsertOptions.database").isEqualTo(1);
      assertThat(summary.getFailed()).isEqualTo(0);
      assertThat(summary.getErrorsList()).isEmpty();

      final long total = getServer(0).getDatabase(getDatabaseName())
          .query("sql", "SELECT count(*) AS total FROM " + typeName).next().<Long>getProperty("total");
      assertThat(total).isEqualTo(1L);
    } finally {
      service.close();
      getServer(0).getDatabase(getDatabaseName()).command("sql", "DROP TYPE " + typeName + " IF EXISTS UNSAFE");
    }
  }

  /**
   * Minimal {@link ServerCallStreamObserver} test double that captures the single
   * {@link InsertSummary} emitted by the handler (or the terminal error), and provides no-op
   * implementations of the flow-control surface the handler touches ({@code request},
   * {@code setOnCancelHandler}, {@code disableAutoInboundFlowControl}).
   */
  private static final class RecordingResponseObserver extends ServerCallStreamObserver<InsertSummary> {
    private final AtomicReference<InsertSummary> summaryRef;
    private final AtomicReference<Throwable>     errorRef;

    private RecordingResponseObserver(final AtomicReference<InsertSummary> summaryRef, final AtomicReference<Throwable> errorRef) {
      this.summaryRef = summaryRef;
      this.errorRef = errorRef;
    }

    @Override public void onNext(final InsertSummary value) { summaryRef.set(value); }
    @Override public void onError(final Throwable t) { errorRef.set(t); }
    @Override public void onCompleted() { }

    @Override public boolean isCancelled() { return false; }
    @Override public void setOnCancelHandler(final Runnable onCancelHandler) { }
    @Override public void setCompression(final String compression) { }
    @Override public boolean isReady() { return true; }
    @Override public void setOnReadyHandler(final Runnable onReadyHandler) { }
    @Override public void request(final int count) { }
    @Override public void setMessageCompression(final boolean enable) { }
    @Override public void disableAutoInboundFlowControl() { }
  }
}
