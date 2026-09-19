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
package com.arcadedb.bolt;

import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.ErrorCategory;
import com.arcadedb.exception.LockTimeoutException;
import com.arcadedb.exception.TimeoutException;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #7915: the three Bolt transaction handlers must agree on the status code they send
 * for the same failure.
 * <p>
 * Issue #7624 replaced {@code classifyExecutionError}'s five-arm ladder with an exhaustive switch over
 * {@link ErrorCategory} and routed RUN, PULL and COMMIT through it. {@code handleBegin} and
 * {@code handleRollback} were left hand-coding {@code Neo.ClientError.Transaction.TransactionNotFound} for every
 * failure, three lines from a {@code handleCommit} that passes that same constant to the classifier as its
 * {@code defaultCode} - which is what the parameter was added for. So a BEGIN refused on an MVCC conflict told a
 * managed-transaction driver's retry predicate that a retryable conflict was a permanent client error, and a
 * BEGIN refused on a deadline or a permission denial reported a missing transaction.
 * <p>
 * The assertions run the real handlers rather than the classifier (see {@link BoltHandlerProbe}): the bug was
 * not in the classification, it was in two handlers not asking for it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7915BoltTransactionHandlerClassificationTest {

  /** BEGIN, COMMIT and ROLLBACK, with the state each has to be in to run. */
  private static final List<String[]> TRANSACTION_HANDLERS = List.of(
      new String[] { "handleBegin", BoltHandlerProbe.READY },
      new String[] { "handleCommit", BoltHandlerProbe.TX_READY },
      new String[] { "handleRollback", BoltHandlerProbe.TX_READY });

  /**
   * The failure that cost the most: a driver auto-retries a managed transaction on a TransientError and does not
   * retry a ClientError, so reporting a conflict as TransactionNotFound turned a conflict that would have
   * succeeded on the second attempt into a hard application error.
   */
  @Test
  void anMvccConflictIsTransientOnEveryTransactionHandler() throws Exception {
    for (final String[] handler : TRANSACTION_HANDLERS)
      assertThat(BoltHandlerProbe.failureCodeOf(handler[0], handler[1],
          new ConcurrentModificationException("the record was modified by another transaction")))
          .as("%s must report an MVCC conflict as retryable", handler[0])
          .isEqualTo(BoltErrorCodes.TRANSIENT_CONFLICT_ERROR);
  }

  @Test
  void aLockTimeoutIsTransientOnEveryTransactionHandler() throws Exception {
    for (final String[] handler : TRANSACTION_HANDLERS)
      assertThat(BoltHandlerProbe.failureCodeOf(handler[0], handler[1], new LockTimeoutException("lock timeout")))
          .as("%s must report lock contention as retryable", handler[0])
          .isEqualTo(BoltErrorCodes.TRANSIENT_CONFLICT_ERROR);
  }

  @Test
  void aSecurityRefusalIsForbiddenOnEveryTransactionHandler() throws Exception {
    for (final String[] handler : TRANSACTION_HANDLERS)
      assertThat(BoltHandlerProbe.failureCodeOf(handler[0], handler[1], new SecurityException("not allowed")))
          .as("%s must report a permission denial as Forbidden, not as a missing transaction", handler[0])
          .isEqualTo(BoltErrorCodes.FORBIDDEN_ERROR);
  }

  @Test
  void aDeadlineIsTransactionTimedOutOnEveryTransactionHandler() throws Exception {
    for (final String[] handler : TRANSACTION_HANDLERS)
      assertThat(BoltHandlerProbe.failureCodeOf(handler[0], handler[1], new TimeoutException("deadline exceeded")))
          .as("%s must report a deadline as TransactionTimedOut", handler[0])
          .isEqualTo(BoltErrorCodes.TRANSACTION_TIMED_OUT_ERROR);
  }

  /**
   * The half of the change that must NOT move: {@link ErrorCategory#SERVER} keeps the caller's default, so an
   * unclassified failure still answers exactly what it answered before - the whole reason {@code defaultCode}
   * exists and the reason routing these two handlers through the classifier is safe.
   */
  @Test
  void anUnclassifiedFailureStillAnswersTransactionNotFound() throws Exception {
    for (final String[] handler : TRANSACTION_HANDLERS)
      assertThat(BoltHandlerProbe.failureCodeOf(handler[0], handler[1], new IllegalStateException("something else")))
          .as("%s must keep TransactionNotFound as the unclassified fallback", handler[0])
          .isEqualTo(BoltErrorCodes.TRANSACTION_ERROR);
  }

  /**
   * Written as a sweep rather than as four separate expectations so a fourth transaction handler - or a handler
   * that stops consulting the classifier again - fails here instead of quietly disagreeing with its neighbours.
   */
  @Test
  void theThreeHandlersAnswerIdenticallyForEveryFailure() throws Exception {
    for (final RuntimeException failure : List.of(
        new ConcurrentModificationException("conflict"),
        new LockTimeoutException("lock timeout"),
        new SecurityException("not allowed"),
        new TimeoutException("deadline exceeded"),
        new IllegalStateException("unclassified"))) {

      final String begin = BoltHandlerProbe.failureCodeOf("handleBegin", BoltHandlerProbe.READY, failure);
      final String commit = BoltHandlerProbe.failureCodeOf("handleCommit", BoltHandlerProbe.TX_READY, failure);
      final String rollback = BoltHandlerProbe.failureCodeOf("handleRollback", BoltHandlerProbe.TX_READY, failure);

      assertThat(begin).as("BEGIN and COMMIT must agree on %s", failure.getClass().getSimpleName()).isEqualTo(commit);
      assertThat(rollback).as("ROLLBACK and COMMIT must agree on %s", failure.getClass().getSimpleName())
          .isEqualTo(commit);
    }
  }

  /**
   * The failure message is not the status code: a handler that classified correctly but dropped the engine's
   * explanation would leave the operator with a title and nothing to read.
   */
  @Test
  void theFailureStillCarriesTheEngineMessage() throws Exception {
    assertThat(BoltHandlerProbe.failureMetadataOf("handleBegin", BoltHandlerProbe.READY,
        new ConcurrentModificationException("the record was modified by another transaction")).get("message"))
        .asString().contains("modified by another transaction");
  }
}
