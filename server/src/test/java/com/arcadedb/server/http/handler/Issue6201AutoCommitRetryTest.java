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
package com.arcadedb.server.http.handler;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.security.ServerSecurityUser;

import io.undertow.server.HttpServerExchange;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for the first half of issue #6201: the {@code retries} the auto-commit wrapper passes to the
 * engine had no effect on anything raised while the command ran.
 * <p>
 * {@code DatabaseAbstractHandler} used to convert <b>every</b> exception the handler raised into a
 * {@code TransactionException} from inside the transaction block. {@code LocalDatabase.transaction(...)} decides
 * what to retry by type - {@code catch (NeedRetryException | DuplicatedKeyException)} - so a conflict raised
 * during {@code execute()} arrived there already wrapped, matched neither arm, fell into the generic
 * {@code catch (Throwable)} and propagated on the first attempt. Only a conflict detected by the wrapper's own
 * {@code commit()} was ever retried.
 * <p>
 * The wrapper now rethrows a {@link RuntimeException} unchanged and wraps only the checked exceptions
 * {@code TransactionScope} cannot declare, which is what these tests pin.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6201AutoCommitRetryTest {
  private static final String DATABASE_PATH = "./target/databases/Issue6201AutoCommitRetryTest";

  private DatabaseInternal database;

  @BeforeEach
  void createDatabase() {
    final DatabaseFactory factory = new DatabaseFactory(DATABASE_PATH);
    if (factory.exists())
      factory.open().drop();
    database = (DatabaseInternal) factory.create();
  }

  @AfterEach
  void dropDatabase() {
    if (database != null && database.isOpen())
      database.drop();
  }

  @Test
  void aConflictRaisedWhileTheCommandRunsIsRetried() {
    final AtomicInteger attempts = new AtomicInteger();
    final CountingHandler handler = new CountingHandler(attempts,
        attempt -> attempt < 3
            ? new ConcurrentModificationException("Record #1:1 modified by another transaction")
            : null);

    final AtomicReference<ExecutionResponse> response = new AtomicReference<>();
    handler.executeInTransaction(null, null, database, null, response, 5);

    assertThat(attempts.get())
        .as("the engine retries a NeedRetryException raised inside the transaction block, so the command runs "
            + "again until it succeeds")
        .isEqualTo(3);
    assertThat(response.get()).isNotNull();
    assertThat(response.get().getCode()).isEqualTo(200);
  }

  /**
   * A duplicated key is deterministic and fails identically on every attempt, so the engine allows it exactly one
   * retry (#4959) - the retry only exists to disambiguate a concurrency-induced duplicate. What matters here is
   * that it is classified at all: before the fix it was wrapped out of the classification and never retried once.
   */
  @Test
  void aDuplicatedKeyRaisedWhileTheCommandRunsIsRetriedOnce() {
    final AtomicInteger attempts = new AtomicInteger();
    final CountingHandler handler = new CountingHandler(attempts,
        attempt -> new DuplicatedKeyException("Idx", "[1]", new RID(1, 1)));

    assertThatThrownBy(
        () -> handler.executeInTransaction(null, null, database, null, new AtomicReference<>(), 5))
        .isInstanceOf(DuplicatedKeyException.class);

    assertThat(attempts.get()).isEqualTo(2);
  }

  /**
   * The other half of #6201: the exception reaching the HTTP error mapping must still be the typed one, so the
   * mapping answers 503 rather than the opaque 500 the wrapper produced. Exhausting the retries must not put the
   * wrapper back on.
   */
  @Test
  void anExhaustedConflictPropagatesAsItselfAndNotWrapped() {
    final AtomicInteger attempts = new AtomicInteger();
    final CountingHandler handler = new CountingHandler(attempts,
        attempt -> new ConcurrentModificationException("Record #1:1 modified by another transaction"));

    assertThatThrownBy(
        () -> handler.executeInTransaction(null, null, database, null, new AtomicReference<>(), 3))
        .isInstanceOf(ConcurrentModificationException.class);

    assertThat(attempts.get()).isEqualTo(3);
  }

  /**
   * A CHECKED exception is the one case that still needs the wrapper: {@code TransactionScope.execute()} declares
   * none, so it cannot travel out of the block as itself.
   */
  @Test
  void aCheckedExceptionIsStillWrapped() {
    final CountingHandler handler = new CountingHandler(new AtomicInteger(), attempt -> null) {
      @Override
      protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
          final Database database, final JSONObject payload) throws IOException {
        throw new IOException("simulated I/O failure");
      }
    };

    assertThatThrownBy(
        () -> handler.executeInTransaction(null, null, database, null, new AtomicReference<>(), 1))
        .isInstanceOf(TransactionException.class)
        .hasMessage("Error on executing command")
        .hasCauseInstanceOf(IOException.class);
  }

  /** Decides what the n-th (1-based) attempt throws; {@code null} means "succeed". */
  @FunctionalInterface
  private interface AttemptOutcome {
    RuntimeException failureFor(int attempt);
  }

  /**
   * A handler whose {@code execute()} counts its invocations and fails according to {@code outcome}, standing in
   * for a command losing an MVCC race on a record it saves.
   */
  private static class CountingHandler extends DatabaseAbstractHandler {
    private final AtomicInteger  attempts;
    private final AttemptOutcome outcome;

    private CountingHandler(final AtomicInteger attempts, final AttemptOutcome outcome) {
      super(null);
      this.attempts = attempts;
      this.outcome = outcome;
    }

    @Override
    protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
        final Database database, final JSONObject payload) throws Exception {
      final RuntimeException failure = outcome.failureFor(attempts.incrementAndGet());
      if (failure != null)
        throw failure;
      return new ExecutionResponse(200, "{}");
    }
  }
}
