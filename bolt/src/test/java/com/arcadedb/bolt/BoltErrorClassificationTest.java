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

import com.arcadedb.database.RID;
import com.arcadedb.exception.ArithmeticErrorException;
import com.arcadedb.exception.CommandParameterMissingException;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.InvalidPropertyTypeException;
import com.arcadedb.exception.LockTimeoutException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.TimeoutException;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #4908: ArcadeDB's optimistic-concurrency conflicts ({@link NeedRetryException})
 * must be classified to a Neo4j <em>transient</em> Bolt status code so managed-transaction drivers auto-retry,
 * while all other errors keep their default (non-retryable) code.
 */
class BoltErrorClassificationTest {

  @Test
  void concurrentModificationClassifiesAsTransient() {
    final Throwable e = new ConcurrentModificationException("Concurrent modification on page ... Please retry the operation");
    assertThat(BoltNetworkExecutor.classifyExecutionError(e, BoltErrorCodes.DATABASE_ERROR))
        .isEqualTo(BoltErrorCodes.TRANSIENT_CONFLICT_ERROR);
  }

  @Test
  void lockTimeoutClassifiesAsTransient() {
    final Throwable e = new LockTimeoutException("Timeout on acquiring lock");
    assertThat(BoltNetworkExecutor.classifyExecutionError(e, BoltErrorCodes.TRANSACTION_ERROR))
        .isEqualTo(BoltErrorCodes.TRANSIENT_CONFLICT_ERROR);
  }

  @Test
  void needRetryWrappedAsCauseClassifiesAsTransient() {
    // The conflict is often wrapped by the query/command layer before reaching the Bolt handler.
    final Throwable wrapped = new RuntimeException("command failed", new ConcurrentModificationException("retry"));
    assertThat(BoltNetworkExecutor.classifyExecutionError(wrapped, BoltErrorCodes.DATABASE_ERROR))
        .isEqualTo(BoltErrorCodes.TRANSIENT_CONFLICT_ERROR);
  }

  /**
   * Issue #7729. openCypher refusing a map-valued property is a client type error, the same one Neo4j reports as
   * {@code Neo.ClientError.Statement.TypeError}. It used to be an {@link IllegalArgumentException} nothing here
   * recognised, so it reached the driver as DatabaseError - an unexplained server fault - which is what the
   * reporter of issue #7629 saw.
   */
  @Test
  void invalidPropertyTypeClassifiesAsAClientTypeError() {
    final Throwable e = new InvalidPropertyTypeException(
        "TypeError: InvalidPropertyType - Property values can only be of primitive types or arrays thereof.");
    assertThat(BoltNetworkExecutor.classifyExecutionError(e, BoltErrorCodes.DATABASE_ERROR))
        .isEqualTo(BoltErrorCodes.TYPE_ERROR);
  }

  @Test
  void invalidPropertyTypeWrappedAsCauseStillClassifiesAsAClientTypeError() {
    // The auto-commit wrapper and the CALL path both wrap it before it reaches the Bolt handler.
    final Throwable wrapped = new RuntimeException("command failed",
        new InvalidPropertyTypeException("TypeError: InvalidPropertyType - ..."));
    assertThat(BoltNetworkExecutor.classifyExecutionError(wrapped, BoltErrorCodes.DATABASE_ERROR))
        .isEqualTo(BoltErrorCodes.TYPE_ERROR);
  }

  @Test
  void aConflictOutranksATypeErrorSoTheDriverStillRetries() {
    // Same precedence rule the arithmetic arm follows: a chain carrying both must keep the transient verdict, which
    // is the one a managed-transaction driver has to act on.
    final Throwable e = new ConcurrentModificationException("retry",
        new InvalidPropertyTypeException("TypeError: InvalidPropertyType - ..."));
    assertThat(BoltNetworkExecutor.classifyExecutionError(e, BoltErrorCodes.DATABASE_ERROR))
        .isEqualTo(BoltErrorCodes.TRANSIENT_CONFLICT_ERROR);
  }

  @Test
  void genericErrorKeepsDefaultCode() {
    final Throwable e = new IllegalStateException("something else");
    assertThat(BoltNetworkExecutor.classifyExecutionError(e, BoltErrorCodes.DATABASE_ERROR))
        .isEqualTo(BoltErrorCodes.DATABASE_ERROR);
    assertThat(BoltNetworkExecutor.classifyExecutionError(e, BoltErrorCodes.TRANSACTION_ERROR))
        .isEqualTo(BoltErrorCodes.TRANSACTION_ERROR);
  }

  @Test
  void cyclicCauseChainTerminatesAndKeepsDefault() {
    // A self-referential cause chain must not spin forever; the bounded walk returns the default code.
    final RuntimeException a = new RuntimeException("a");
    final RuntimeException b = new RuntimeException("b", a);
    a.initCause(b); // a -> b -> a -> ...
    assertThat(BoltNetworkExecutor.classifyExecutionError(a, BoltErrorCodes.DATABASE_ERROR))
        .isEqualTo(BoltErrorCodes.DATABASE_ERROR);
  }

  @Test
  void isRetryableConflictDetectsNeedRetryForLogLevelDemotion() {
    // Drives both the transient classification and the FINE-vs-WARNING log level in the RUN/PULL handlers.
    assertThat(BoltNetworkExecutor.isRetryableConflict(new ConcurrentModificationException("retry"))).isTrue();
    assertThat(BoltNetworkExecutor.isRetryableConflict(new RuntimeException("x", new LockTimeoutException("t")))).isTrue();
    assertThat(BoltNetworkExecutor.isRetryableConflict(new IllegalStateException("other"))).isFalse();
    assertThat(BoltNetworkExecutor.isRetryableConflict(null)).isFalse();
  }

  @Test
  void transientCodeIsARetryableClassificationDriversHonor() {
    // Neo4j drivers retry on the TransientError classification, except the two excluded titles.
    assertThat(BoltErrorCodes.TRANSIENT_CONFLICT_ERROR).startsWith("Neo.TransientError.");
    assertThat(BoltErrorCodes.TRANSIENT_CONFLICT_ERROR).isNotEqualTo("Neo.TransientError.Transaction.Terminated");
    assertThat(BoltErrorCodes.TRANSIENT_CONFLICT_ERROR).isNotEqualTo("Neo.TransientError.Transaction.LockClientStopped");
  }

  @Test
  void semanticExceptionClassifiesAsSemanticError() {
    assertThat(BoltNetworkExecutor.classifyParsingError(new CommandSemanticException("UndefinedVariable")))
        .isEqualTo(BoltErrorCodes.SEMANTIC_ERROR);
  }

  @Test
  void plainParsingExceptionClassifiesAsSyntaxError() {
    assertThat(BoltNetworkExecutor.classifyParsingError(new CommandParsingException("unexpected token")))
        .isEqualTo(BoltErrorCodes.SYNTAX_ERROR);
  }

  /**
   * Issue #5561: an unbound {@code $parameter} is not a syntax error and not a generic semantic error - the
   * query text is valid and the client only failed to send a value. Neo4j gives it its own title, and drivers
   * key off it, so it must not collapse into either neighbour even though the exception extends
   * {@link CommandSemanticException}.
   */
  @Test
  void missingParameterClassifiesAsParameterMissing() {
    assertThat(BoltNetworkExecutor.classifyParsingError(new CommandParameterMissingException("threshold")))
        .isEqualTo(BoltErrorCodes.PARAMETER_MISSING_ERROR);
    assertThat(BoltErrorCodes.PARAMETER_MISSING_ERROR).isEqualTo("Neo.ClientError.Statement.ParameterMissing");
  }

  /**
   * Issue #5602: a 64-bit overflow or a division by zero is decided by the values the caller supplied, so a driver
   * must be told it is the client's error and not the generic DatabaseError it reports as "the server broke". Neo4j
   * calls it ArithmeticError.
   */
  @Test
  void arithmeticErrorClassifiesAsArithmeticError() {
    assertThat(BoltNetworkExecutor.classifyExecutionError(new ArithmeticErrorException("long overflow"),
        BoltErrorCodes.DATABASE_ERROR)).isEqualTo(BoltErrorCodes.ARITHMETIC_ERROR);
    assertThat(BoltErrorCodes.ARITHMETIC_ERROR).isEqualTo("Neo.ClientError.Statement.ArithmeticError");
  }

  @Test
  void arithmeticErrorWrappedAsCauseAlsoClassifiesAsArithmeticError() {
    // It reaches the handler wrapped by the auto-commit transaction wrapper, and carries the JDK ArithmeticException
    // it came from as its own cause, so the whole chain has to be searched.
    final Throwable wrapped = new RuntimeException("command failed",
        new ArithmeticErrorException("long overflow", new ArithmeticException("long overflow")));
    assertThat(BoltNetworkExecutor.classifyExecutionError(wrapped, BoltErrorCodes.DATABASE_ERROR))
        .isEqualTo(BoltErrorCodes.ARITHMETIC_ERROR);
  }

  @Test
  void aRetryableConflictStillWinsOverAnArithmeticError() {
    // Ordering matters: a conflict is retryable and an arithmetic error is not, so a chain carrying both must keep
    // the transient classification the driver acts on.
    final Throwable wrapped = new ArithmeticErrorException("long overflow", new ConcurrentModificationException("retry"));
    assertThat(BoltNetworkExecutor.classifyExecutionError(wrapped, BoltErrorCodes.DATABASE_ERROR))
        .isEqualTo(BoltErrorCodes.TRANSIENT_CONFLICT_ERROR);
  }

  /**
   * Issue #7123: a unique-index violation is a permanent failure - retrying the identical write can never
   * succeed - so it must not collapse into the generic DatabaseError, which a Neo4j driver's retry policy
   * reads as "server broke, safe to retry".
   */
  @Test
  void duplicatedKeyClassifiesAsConstraintViolation() {
    final Throwable e = new DuplicatedKeyException("Person.email", "[bob@x.com]", new RID(1, 0));
    assertThat(BoltNetworkExecutor.classifyExecutionError(e, BoltErrorCodes.DATABASE_ERROR))
        .isEqualTo(BoltErrorCodes.CONSTRAINT_VIOLATION_ERROR);
    assertThat(BoltErrorCodes.CONSTRAINT_VIOLATION_ERROR).isEqualTo("Neo.ClientError.Schema.ConstraintValidationFailed");
  }

  @Test
  void duplicatedKeyWrappedAsCauseAlsoClassifiesAsConstraintViolation() {
    final Throwable wrapped = new RuntimeException("command failed",
        new DuplicatedKeyException("Person.email", "[bob@x.com]", new RID(1, 0)));
    assertThat(BoltNetworkExecutor.classifyExecutionError(wrapped, BoltErrorCodes.DATABASE_ERROR))
        .isEqualTo(BoltErrorCodes.CONSTRAINT_VIOLATION_ERROR);
  }

  /**
   * Issue #7123: a permission denial is a permanent client error (retrying the same request as the same
   * user can never succeed), so it must be reported distinctly from DatabaseError rather than reading as a
   * server fault to the driver.
   */
  @Test
  void securityExceptionClassifiesAsForbidden() {
    final Throwable e = new SecurityException("User 'bob' is not allowed to execute this operation");
    assertThat(BoltNetworkExecutor.classifyExecutionError(e, BoltErrorCodes.DATABASE_ERROR))
        .isEqualTo(BoltErrorCodes.FORBIDDEN_ERROR);
    assertThat(BoltErrorCodes.FORBIDDEN_ERROR).isEqualTo("Neo.ClientError.Security.Forbidden");
  }

  @Test
  void securityExceptionWrappedAsCauseAlsoClassifiesAsForbidden() {
    final Throwable wrapped = new RuntimeException("command failed", new SecurityException("not allowed"));
    assertThat(BoltNetworkExecutor.classifyExecutionError(wrapped, BoltErrorCodes.DATABASE_ERROR))
        .isEqualTo(BoltErrorCodes.FORBIDDEN_ERROR);
  }

  /**
   * Issue #7123: a query/statement deadline (as opposed to the retryable {@code LockTimeoutException}
   * contention already covered by {@link #lockTimeoutClassifiesAsTransient}) must not read as a generic
   * DatabaseError either. NOT Transaction.Terminated (code review): that title means "explicitly killed
   * by the user" and both the Neo4j driver and Spring Data Neo4j explicitly exclude it from their retry
   * predicates, so mapping a deadline to it would make Neo4j drivers treat a deadline as non-retryable
   * for the wrong reason. TransactionTimedOut is Neo4j's own code for this case.
   */
  @Test
  void deadlineTimeoutClassifiesAsTransactionTimedOut() {
    final Throwable e = new TimeoutException("Query exceeded the configured timeout");
    assertThat(BoltNetworkExecutor.classifyExecutionError(e, BoltErrorCodes.DATABASE_ERROR))
        .isEqualTo(BoltErrorCodes.TRANSACTION_TIMED_OUT_ERROR);
    assertThat(BoltErrorCodes.TRANSACTION_TIMED_OUT_ERROR).isEqualTo("Neo.ClientError.Transaction.TransactionTimedOut");
  }

  @Test
  void lockTimeoutIsNotMisclassifiedAsTransactionTimedOut() {
    // LockTimeoutException extends NeedRetryException (contention, not a deadline) and must keep going
    // through the DeadlockDetected transient path rather than the deadline-timeout path.
    final Throwable e = new LockTimeoutException("Timeout on acquiring lock");
    assertThat(BoltNetworkExecutor.classifyExecutionError(e, BoltErrorCodes.DATABASE_ERROR))
        .isEqualTo(BoltErrorCodes.TRANSIENT_CONFLICT_ERROR);
  }
}
