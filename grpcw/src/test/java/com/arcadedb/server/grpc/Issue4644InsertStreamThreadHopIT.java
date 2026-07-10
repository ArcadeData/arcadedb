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
 * Regression test for issue #4644: gRPC {@code InsertStream} begins the transaction in
 * {@code onNext} but commits it in {@code onCompleted}, which the gRPC serializing executor may
 * run on a different pool thread. Because ArcadeDB transactions are bound to the calling thread
 * through a {@code ThreadLocal} ({@code DatabaseContext}), the deferred {@code PER_STREAM} commit
 * found no transaction bound to the committing thread and failed with
 * {@code TransactionException: Transaction not begun}, reclassifying every inserted row as failed
 * ({@code inserted=0, failed=N}, one {@code row_index=-1 COMMIT_FAILED} error).
 *
 * <p>The real-world failure is intermittent because it depends on whether gRPC's executor happens
 * to run the two callbacks on the same thread. To make the {@code onNext} -> {@code onCompleted}
 * thread hop deterministic, this test drives the {@code insertStream} handler directly: the data
 * chunk is delivered on one thread and the half-close on a different thread. Without the fix the
 * commit runs on a thread with no active transaction and fails; with the fix the handler re-binds
 * the transaction to the committing thread and the rows commit regardless of callback thread.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue4644InsertStreamThreadHopIT extends BaseGraphServerTest {

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
  void insertStreamPerStreamCommitMustNotDependOnCallbackThread() throws Exception {
    final String typeName = "Issue4644ThreadHopType_" + System.currentTimeMillis();

    getServer(0).getDatabase(getDatabaseName()).command("sql", "CREATE DOCUMENT TYPE " + typeName);

    final ArcadeDbGrpcService service = new ArcadeDbGrpcService(getDatabaseName(), getServer(0));
    try {
      final AtomicReference<InsertSummary> summaryRef = new AtomicReference<>();
      final RecordingResponseObserver resp = new RecordingResponseObserver(summaryRef);

      final StreamObserver<InsertChunk> req = service.insertStream(resp);

      // Build a single chunk carrying every row, marked last=true, in PER_STREAM mode so the commit
      // is deferred to onCompleted().
      final InsertChunk.Builder chunk = InsertChunk.newBuilder()
          .setSessionId("issue-4644-thread-hop")
          .setChunkSeq(0)
          .setLast(true)
          .setOptions(InsertOptions.newBuilder()
              .setDatabase(getDatabaseName())
              .setCredentials(credentials())
              .setTargetClass(typeName)
              .setConflictMode(InsertOptions.ConflictMode.CONFLICT_ERROR)
              .setTransactionMode(InsertOptions.TransactionMode.PER_STREAM)
              .build());
      for (int i = 0; i < 150; i++)
        chunk.addRows(GrpcRecord.newBuilder().setType(typeName).putProperties("name", stringValue("ag" + i)).build());

      // Deliver the data chunk (which calls db.begin() and inserts the rows) on thread A.
      final AtomicReference<Throwable> onNextError = new AtomicReference<>();
      final Thread threadA = new Thread(() -> {
        try {
          req.onNext(chunk.build());
        } catch (final Throwable t) {
          onNextError.set(t);
        }
      }, "issue4644-onNext");
      threadA.start();
      threadA.join(30_000);
      assertThat(onNextError.get()).as("onNext should not throw").isNull();

      // Deliver the half-close (deferred commit) on a DIFFERENT thread B, deterministically
      // reproducing the onNext -> onCompleted thread hop.
      final AtomicReference<Throwable> onCompletedError = new AtomicReference<>();
      final Thread threadB = new Thread(() -> {
        try {
          req.onCompleted();
        } catch (final Throwable t) {
          onCompletedError.set(t);
        }
      }, "issue4644-onCompleted");
      threadB.start();
      threadB.join(30_000);
      assertThat(onCompletedError.get()).as("onCompleted should not throw").isNull();

      final InsertSummary summary = summaryRef.get();
      assertThat(summary).isNotNull();
      assertThat(summary.getReceived()).isEqualTo(150);
      assertThat(summary.getInserted()).isEqualTo(150);
      assertThat(summary.getFailed()).isEqualTo(0);
      assertThat(summary.getErrorsList()).isEmpty();

      // The rows must actually be persisted and visible after commit.
      final long total = getServer(0).getDatabase(getDatabaseName())
          .query("sql", "SELECT count(*) AS total FROM " + typeName).next().<Long>getProperty("total");
      assertThat(total).isEqualTo(150L);
    } finally {
      service.close();
      getServer(0).getDatabase(getDatabaseName()).command("sql", "DROP TYPE " + typeName + " IF EXISTS UNSAFE");
    }
  }

  /**
   * Minimal {@link ServerCallStreamObserver} test double that captures the single {@link InsertSummary}
   * emitted by the handler and provides no-op implementations of the flow-control surface the handler
   * touches ({@code request}, {@code setOnCancelHandler}, {@code disableAutoInboundFlowControl}).
   */
  private static final class RecordingResponseObserver extends ServerCallStreamObserver<InsertSummary> {
    private final AtomicReference<InsertSummary> summaryRef;

    private RecordingResponseObserver(final AtomicReference<InsertSummary> summaryRef) {
      this.summaryRef = summaryRef;
    }

    @Override public void onNext(final InsertSummary value) { summaryRef.set(value); }
    @Override public void onError(final Throwable t) { }
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
