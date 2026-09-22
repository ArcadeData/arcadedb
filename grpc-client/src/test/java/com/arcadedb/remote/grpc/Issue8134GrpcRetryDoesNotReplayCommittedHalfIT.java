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
package com.arcadedb.remote.grpc;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.grpc.TransactionProtocol;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.ForwardingClientCallListener;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression for issue #8134, the follow-up to #8062: {@code RemoteGrpcDatabase} inherits
 * {@code RemoteDatabase.transaction(txBlock, join, attempts, ...)} - the third retry loop of #7916's shape - but
 * none of its work travels over HTTP, so the {@code arcadedb-session-partial-commit} RESPONSE HEADER that #8062
 * added never reached it. Over gRPC the loop therefore behaved exactly as it did before #8062: a block that
 * crossed a {@code BATCH n} boundary and then hit a retryable conflict was replayed, applied its already durable
 * half a second time, and could still return success.
 * <p>
 * The verdict now travels as a call TRAILER instead, set by the server on every RPC that ran work inside the
 * caller's transaction and latched by the client on the same field {@code RemoteDatabase} latches the header
 * into. {@code RemoteGrpcDatabase.begin(...)} does not call {@code super.begin(...)}, so it has to clear that
 * latch itself; the last two tests here are what fails if it does not.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue8134GrpcRetryDoesNotReplayCommittedHalfIT extends BaseGraphServerTest {
  private static final String ROWS      = "Issue8134Row";
  private static final int    SIZE      = 25;
  private static final int    ATTEMPTS  = 3;
  private static final int    GRPC_PORT = 50051;

  private RemoteGrpcServer   server;
  private RemoteGrpcDatabase database;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GRPC:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @BeforeEach
  @Override
  public void beginTest() {
    super.beginTest();
    server = new RemoteGrpcServer("localhost", GRPC_PORT, "root", DEFAULT_PASSWORD_FOR_TESTS, true, List.of());
    database = new RemoteGrpcDatabase(server, "localhost", GRPC_PORT, getServer(0).getHttpServer().getPort(),
        getDatabaseName(), "root", DEFAULT_PASSWORD_FOR_TESTS);
    populate();
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    if (database != null) {
      try {
        if (database.isTransactionActive())
          database.rollback();
      } catch (final Throwable ignore) {
        // the test already failed; do not mask it
      }
      database.close();
    }
    if (server != null)
      server.close();
    super.endTest();
  }

  /**
   * The defect itself, over gRPC: with a BATCH boundary crossed before the conflict, the retry used to
   * double-apply the rows already published. Now the block is not re-run at all, so every row is incremented
   * exactly once, and the conflict reaches the caller instead of being swallowed into a clean return.
   */
  @Test
  void aGrpcBlockThatCommittedPartOfItsWorkIsNotReplayed() {
    final AtomicInteger attempts = new AtomicInteger();
    final AtomicInteger okCalls = new AtomicInteger();

    assertThatThrownBy(() -> database.transaction(() -> {
      attempts.incrementAndGet();
      // Commits every 10 of the 25 rows: 20 are durable by the time the statement ends.
      database.command("sql", "UPDATE " + ROWS + " SET seq = seq + 1 BATCH 10").close();
      // ... and then the block fails, the way a genuine MVCC conflict on a later statement would.
      throw new ConcurrentModificationException("simulated conflict after the batch boundary");
    }, true, ATTEMPTS, okCalls::incrementAndGet, null)).isInstanceOf(NeedRetryException.class);

    assertThat(attempts.get()).as("the block must be run ONCE: its durable half cannot be replayed").isEqualTo(1);
    assertThat(okCalls.get()).as("and a block that failed must not report success").isZero();

    assertThat(countRowsWithSeq(1)).as("the 20 rows the batch published are incremented exactly once").isEqualTo(20);
    assertThat(countRowsWithSeq(0)).as("the remaining 5 were rolled back").isEqualTo(5);
    assertThat(countRowsWithSeq(2)).as("no row may be incremented twice").isZero();
  }

  /**
   * {@code DELETE ... BATCH n} goes through the same {@code BatchStep}, so it earns the same refusal. Driven
   * through its own entry point rather than assumed from the UPDATE above.
   */
  @Test
  void aGrpcBatchedDeleteThatCommittedPartOfItsWorkIsNotReplayed() {
    final AtomicInteger attempts = new AtomicInteger();

    assertThatThrownBy(() -> database.transaction(() -> {
      attempts.incrementAndGet();
      database.command("sql", "DELETE FROM " + ROWS + " BATCH 10").close();
      throw new ConcurrentModificationException("simulated conflict after the batch boundary");
    }, true, ATTEMPTS, null, null)).isInstanceOf(NeedRetryException.class);

    assertThat(attempts.get()).isEqualTo(1);
    assertThat(countRowsWithSeq(0)).as("20 rows are durably gone, the last 5 deletes were rolled back").isEqualTo(5);
  }

  /**
   * The read RPC carries the verdict too. {@code ExecuteQuery} runs inside the caller's transaction on exactly
   * the same server-side dispatch as {@code ExecuteCommand}, so a block whose LAST RPC before the conflict is a
   * query must still refuse to replay: the latch has to survive a call that published nothing of its own.
   */
  @Test
  void aQueryIssuedAfterTheBatchBoundaryDoesNotClearTheVerdict() {
    final AtomicInteger attempts = new AtomicInteger();

    assertThatThrownBy(() -> database.transaction(() -> {
      attempts.incrementAndGet();
      database.command("sql", "UPDATE " + ROWS + " SET seq = seq + 1 BATCH 10").close();
      // A plain read on the same transaction: its own RPC publishes nothing and must not un-say the verdict.
      try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM " + ROWS)) {
        assertThat(((Number) rs.next().getProperty("c")).longValue()).isEqualTo(SIZE);
      }
      throw new ConcurrentModificationException("simulated conflict after a read");
    }, true, ATTEMPTS, null, null)).isInstanceOf(NeedRetryException.class);

    assertThat(attempts.get()).as("the read must not clear a verdict an earlier RPC in the same transaction set")
        .isEqualTo(1);
    assertThat(countRowsWithSeq(2)).as("no row may be incremented twice").isZero();
  }

  /**
   * The guard must not disturb the ordinary case it sits next to: a block that publishes NOTHING before failing
   * is still retried the full number of attempts, because the rollback really did take everything back. This is
   * the test that fails if the trailer is latched on the wrong call.
   */
  @Test
  void aGrpcBlockThatCommittedNothingIsStillRetried() {
    final AtomicInteger attempts = new AtomicInteger();

    assertThatThrownBy(() -> database.transaction(() -> {
      attempts.incrementAndGet();
      database.command("sql", "UPDATE " + ROWS + " SET seq = seq + 1").close();
      throw new ConcurrentModificationException("simulated conflict with nothing published");
    }, true, ATTEMPTS, null, null)).isInstanceOf(NeedRetryException.class);

    assertThat(attempts.get()).as("nothing was durable, so all the attempts are still available").isEqualTo(ATTEMPTS);
    assertThat(countRowsWithSeq(0)).as("and every attempt's rollback took its work back").isEqualTo(SIZE);
  }

  /**
   * And the verdict does not survive the transaction it was reached in: a block that crosses a batch boundary
   * and SUCCEEDS leaves the next transaction on the same client free to retry as it always could.
   * {@code RemoteGrpcDatabase.begin(...)} never calls {@code super.begin(...)}, so without a reset of its own the
   * latch set here would silently disable retries for the rest of the connection's life.
   */
  @Test
  void aLaterTransactionOnTheSameGrpcClientStillRetries() {
    database.transaction(() -> database.command("sql", "UPDATE " + ROWS + " SET seq = seq + 1 BATCH 10").close());
    assertThat(countRowsWithSeq(1)).isEqualTo(SIZE);

    final AtomicInteger attempts = new AtomicInteger();
    assertThatThrownBy(() -> database.transaction(() -> {
      attempts.incrementAndGet();
      throw new ConcurrentModificationException("simulated conflict in a brand new transaction");
    }, true, ATTEMPTS, null, null)).isInstanceOf(NeedRetryException.class);

    assertThat(attempts.get()).as("the new transaction published nothing, so its attempts are intact")
        .isEqualTo(ATTEMPTS);
  }

  /**
   * The server half of the fix, read off the wire rather than inferred from the client's behaviour, because the
   * three assertions below are about which CALLS carry the trailer and the driver exposes none of that.
   * <p>
   * {@code ExecuteCommand} run inside the caller's transaction carries it when the statement crossed a BATCH
   * boundary; {@code CommitTransaction} does NOT, although committing is exactly what moves the counter the
   * verdict is derived from - the exemption {@code DatabaseAbstractHandler.reportsSessionPartialCommit()}
   * spells out for {@code /commit} on HTTP; and the same statement issued with no transaction at all carries
   * nothing, because there is no caller transaction to report on.
   */
  @Test
  void onlyTheCallsThatPublishedUnderTheCallersTransactionCarryTheTrailer() {
    final TrailerRecorder recorder = new TrailerRecorder();
    try (final RemoteGrpcServer recorded = new RemoteGrpcServer("localhost", GRPC_PORT, "root",
        DEFAULT_PASSWORD_FOR_TESTS, true, List.of(recorder));
        final RemoteGrpcDatabase db = new RemoteGrpcDatabase(recorded, "localhost", GRPC_PORT,
            getServer(0).getHttpServer().getPort(), getDatabaseName(), "root", DEFAULT_PASSWORD_FOR_TESTS)) {

      db.command("sql", "UPDATE " + ROWS + " SET seq = seq + 1 BATCH 10").close();
      assertThat(recorder.sawTrailerOn("ExecuteCommand"))
          .as("no caller transaction, so nothing to report on - exactly as a session-less HTTP command")
          .isFalse();

      recorder.reset();
      db.begin();
      db.command("sql", "UPDATE " + ROWS + " SET seq = seq + 1 BATCH 10").close();
      assertThat(recorder.sawTrailerOn("ExecuteCommand"))
          .as("the batch boundary published 20 of the 25 rows under the caller's transaction").isTrue();

      recorder.reset();
      db.commit();
      assertThat(recorder.sawTrailerOn("CommitTransaction"))
          .as("committing in full is not a PARTIAL commit, however much it moves the counter").isFalse();
    }
  }

  /**
   * Records which gRPC methods answered with the partial-commit trailer. Installed on the CHANNEL, so it sees
   * every call the database makes without depending on the database's own interceptor.
   */
  private static final class TrailerRecorder implements ClientInterceptor {
    private final Set<String> methodsWithTrailer = ConcurrentHashMap.newKeySet();

    void reset() {
      methodsWithTrailer.clear();
    }

    /** @param simpleMethodName the RPC name without its service prefix, e.g. {@code "ExecuteCommand"} */
    boolean sawTrailerOn(final String simpleMethodName) {
      return methodsWithTrailer.contains(simpleMethodName);
    }

    @Override
    public <Q, A> ClientCall<Q, A> interceptCall(final MethodDescriptor<Q, A> method, final CallOptions callOptions,
        final Channel next) {
      final String name = MethodDescriptor.extractBareMethodName(method.getFullMethodName());
      return new ForwardingClientCall.SimpleForwardingClientCall<Q, A>(next.newCall(method, callOptions)) {
        @Override
        public void start(final Listener<A> responseListener, final Metadata headers) {
          super.start(new ForwardingClientCallListener.SimpleForwardingClientCallListener<A>(responseListener) {
            @Override
            public void onClose(final Status status, final Metadata trailers) {
              if (name != null && trailers != null
                  && trailers.containsKey(TransactionProtocol.SESSION_PARTIAL_COMMIT_TRAILER))
                methodsWithTrailer.add(name);
              super.onClose(status, trailers);
            }
          }, headers);
        }
      };
    }
  }

  private void populate() {
    database.command("sql", "CREATE DOCUMENT TYPE " + ROWS + " IF NOT EXISTS").close();
    database.command("sql", "CREATE PROPERTY " + ROWS + ".seq IF NOT EXISTS INTEGER").close();
    database.transaction(() -> {
      for (int i = 0; i < SIZE; i++)
        database.command("sql", "INSERT INTO " + ROWS + " SET seq = 0").close();
    });
  }

  private long countRowsWithSeq(final int seq) {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM " + ROWS + " WHERE seq = ?", seq)) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }
}
