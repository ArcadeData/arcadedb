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
package com.arcadedb.query.opencypher;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.TransactionRetryAndRollbackTest;
import com.arcadedb.event.AfterRecordCreateListener;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #13223: the HTTP, gRPC and MCP auto-commit wrappers run a request in {@code Database.transaction(block, false, retries)}
 * and replay the whole block on a conflict. A {@code CALL { ... } IN TRANSACTIONS} commits its batches (and, since #13172, the
 * statement's writes before the CALL) while the block runs, so a conflict after that point replayed work already durable: the
 * batches were applied twice. A block that committed part of its work is not retried any more: the conflict reaches the caller,
 * as a failure no layer retries (issue #13251).
 * <p>
 * Every test here runs the statement the way those wrappers do: inside {@code transaction(..., false, 3)}, in the implicit
 * transaction they open around one request.
 */
class CypherCallInTransactionsRetryIssue13223Test extends TestHelper {

  private static final int ATTEMPTS = 3;

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE Pre13223");
    database.command("sql", "CREATE VERTEX TYPE Batch13223");
    database.command("sql", "CREATE VERTEX TYPE Counter13223");
  }

  /** Batches only, nothing written before the CALL: a conflict after them must not run the batches again. */
  @Test
  void committedBatchesAreNotReplayedAfterALateConflict() {
    final AtomicInteger attempts = new AtomicInteger();
    assertThatThrownBy(() -> runAsTheServerWrapperDoes(attempts,
        "UNWIND range(1, 3) AS i CALL (i) { CREATE (:Batch13223 {i: i}) } IN TRANSACTIONS OF 1 ROW", true))
        .satisfies(TransactionRetryAndRollbackTest::assertNotReplayable);

    assertThat(attempts.get()).isEqualTo(1);
    assertThat(count("Batch13223")).isEqualTo(3L);
    assertThat(database.isTransactionActive()).isFalse();
  }

  /** The writes before the CALL, committed before the first batch since #13172, are not replayed either. */
  @Test
  void writesBeforeTheCallAndBatchesAreNotReplayedAfterALateConflict() {
    final AtomicInteger attempts = new AtomicInteger();
    assertThatThrownBy(() -> runAsTheServerWrapperDoes(attempts,
        "CREATE (:Pre13223) WITH 1 AS one UNWIND range(1, 3) AS i CALL (i) { CREATE (:Batch13223 {i: i}) } IN TRANSACTIONS OF 1 ROW",
        true))
        .satisfies(TransactionRetryAndRollbackTest::assertNotReplayable);

    assertThat(attempts.get()).isEqualTo(1);
    assertThat(count("Pre13223")).isEqualTo(1L);
    assertThat(count("Batch13223")).isEqualTo(3L);
  }

  /** A batch that failed under ON ERROR CONTINUE committed nothing, but the batches around it did: no replay. */
  @Test
  void onErrorContinueKeepsTheBatchesThatCommittedAndIsNotReplayed() {
    final AtomicInteger attempts = new AtomicInteger();
    assertThatThrownBy(() -> runAsTheServerWrapperDoes(attempts,
        "UNWIND [1, 0, 2] AS i CALL (i) { CREATE (:Batch13223 {i: 1 / i}) } IN TRANSACTIONS OF 1 ROW ON ERROR CONTINUE", true))
        .satisfies(TransactionRetryAndRollbackTest::assertNotReplayable);

    assertThat(attempts.get()).isEqualTo(1);
    assertThat(count("Batch13223")).isEqualTo(2L);
  }

  /**
   * Nothing committed when the conflict hit - the CALL had no row to run a batch for, so the writes before it were not committed
   * either: the block is still retried, and the retry is what makes the request succeed.
   */
  @Test
  void aBlockThatCommittedNothingIsStillRetried() {
    final AtomicInteger attempts = new AtomicInteger();
    runAsTheServerWrapperDoes(attempts,
        "CREATE (:Pre13223) WITH 1 AS one UNWIND [] AS i CALL (i) { CREATE (:Batch13223 {i: i}) } IN TRANSACTIONS OF 1 ROW", true);

    assertThat(attempts.get()).isEqualTo(2);
    assertThat(count("Pre13223")).isEqualTo(1L);
    assertThat(count("Batch13223")).isZero();
  }

  /**
   * Batches ran and committed, but none of them wrote anything (a MATCH ... SET that matched no node): nothing of the block is
   * durable, so a replay is safe and the block is still retried.
   */
  @Test
  void batchesThatWroteNothingDoNotStopTheRetry() {
    final AtomicInteger attempts = new AtomicInteger();
    runAsTheServerWrapperDoes(attempts,
        "UNWIND range(1, 3) AS i CALL (i) { MATCH (n:Batch13223 {i: -1}) SET n.touched = i } IN TRANSACTIONS OF 1 ROW", true);

    assertThat(attempts.get()).isEqualTo(2);
    assertThat(count("Batch13223")).isZero();
  }

  /**
   * A real failure of the final commit instead of a thrown one: while the batches run, another transaction commits the key the
   * statement creates after the CALL under a UNIQUE index, so the commit of the request's transaction fails with a retryable
   * duplicated key, as a concurrent writer makes it fail over HTTP.
   */
  @Test
  void aRealConflictOnTheFinalCommitDoesNotReplayTheBatches() {
    database.command("sql", "CREATE PROPERTY Counter13223.k INTEGER");
    database.command("sql", "CREATE INDEX ON Counter13223 (k) UNIQUE");

    final AtomicReference<Throwable> concurrentFailure = new AtomicReference<>();
    final AtomicInteger created = new AtomicInteger();
    final AfterRecordCreateListener concurrentInsert = record -> {
      if (record instanceof Vertex vertex && vertex.getTypeName().equals("Batch13223") && created.incrementAndGet() == 1) {
        // Another client commits the key while the first batch runs, on a thread of its own
        final Thread other = new Thread(() -> {
          try {
            database.transaction(() -> database.newVertex("Counter13223").set("k", 1).save());
          } catch (final Throwable e) {
            concurrentFailure.set(e);
          }
        });
        other.start();
        try {
          other.join(30_000);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    };

    database.getEvents().registerListener(concurrentInsert);
    final AtomicInteger attempts = new AtomicInteger();
    try {
      assertThatThrownBy(() -> runAsTheServerWrapperDoes(attempts, "UNWIND range(1, 3) AS i "//
          + "CALL (i) { CREATE (:Batch13223 {i: i}) } IN TRANSACTIONS OF 1 ROW "//
          + "WITH count(*) AS n CREATE (:Counter13223 {k: 1})", false))
          .isInstanceOf(DuplicatedKeyException.class);
    } finally {
      database.getEvents().unregisterListener(concurrentInsert);
    }

    assertThat(concurrentFailure.get()).isNull();
    assertThat(created.get()).isEqualTo(3);
    assertThat(attempts.get()).isEqualTo(1);
    assertThat(count("Batch13223")).isEqualTo(3L);
    assertThat(count("Counter13223")).isEqualTo(1L);
  }

  /**
   * The asynchronous executor's {@code transaction(block, retries)} replays its block on a conflict too, through its own loop: it
   * applies the same rule.
   */
  @Test
  void asyncTransactionDoesNotReplayCommittedBatches() {
    final AtomicInteger attempts = new AtomicInteger();
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    database.async().transaction(() -> {
      ((DatabaseInternal) database).getTransaction().setImplicit(true);
      drain("UNWIND range(1, 3) AS i CALL (i) { CREATE (:Batch13223 {i: i}) } IN TRANSACTIONS OF 1 ROW");
      if (attempts.incrementAndGet() == 1)
        throw new ConcurrentModificationException("late conflict after the batches committed (issue #13223)");
    }, ATTEMPTS, null, failure::set);
    database.async().waitCompletion();

    assertThat(attempts.get()).isEqualTo(1);
    TransactionRetryAndRollbackTest.assertNotReplayable(failure.get());
    assertThat(count("Batch13223")).isEqualTo(3L);
  }

  /**
   * What the HTTP handler's {@code executeInTransaction}, gRPC's {@code executeCommandWithRetries} and MCP's
   * {@code collectInTransaction} do around one request: an implicit transaction, replayed on a conflict.
   */
  private void runAsTheServerWrapperDoes(final AtomicInteger attempts, final String cypher, final boolean conflictOnFirstAttempt) {
    database.transaction(() -> {
      ((DatabaseInternal) database).getTransaction().setImplicit(true);
      drain(cypher);
      if (attempts.incrementAndGet() == 1 && conflictOnFirstAttempt)
        throw new ConcurrentModificationException("late conflict after the statement ran (issue #13223)");
    }, false, ATTEMPTS);
  }

  private void drain(final String cypher) {
    try (final ResultSet resultSet = database.command("opencypher", cypher)) {
      while (resultSet.hasNext())
        resultSet.next();
    }
  }

  private long count(final String type) {
    try (final ResultSet resultSet = database.query("opencypher", "MATCH (n:" + type + ") RETURN count(n) AS c")) {
      return resultSet.next().<Number>getProperty("c").longValue();
    }
  }
}
