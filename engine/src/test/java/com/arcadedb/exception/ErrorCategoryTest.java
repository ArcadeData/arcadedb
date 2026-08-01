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
package com.arcadedb.exception;

import com.arcadedb.database.RID;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Classification contract shared by every wire protocol (issue #5628). The wire modules translate a category into
 * their own vocabulary - a SQLSTATE, a RESP prefix, a MongoDB error code - so the categories themselves have to be
 * stable and have to survive the wrapping a failure picks up on its way out of the engine.
 */
class ErrorCategoryTest {

  @Test
  void nullAndUnknownFailuresAreServerFaults() {
    assertThat(ErrorCategory.of(null)).isEqualTo(ErrorCategory.SERVER);
    assertThat(ErrorCategory.of(new IOException("disk gone"))).isEqualTo(ErrorCategory.SERVER);
    assertThat(ErrorCategory.of(new CommandExecutionException("something broke"))).isEqualTo(ErrorCategory.SERVER);
  }

  @Test
  void arithmeticFailuresAreClientErrors() {
    assertThat(ErrorCategory.of(new ArithmeticErrorException("/ by zero"))).isEqualTo(ErrorCategory.ARITHMETIC);
    assertThat(ErrorCategory.of(new ArithmeticErrorException("long overflow"))).isEqualTo(ErrorCategory.ARITHMETIC);
  }

  @Test
  void aWrappedArithmeticErrorIsStillArithmetic() {
    // The three shapes the engine actually produces: the direct throw, the auto-commit TransactionException
    // wrapper, and the doubly-wrapped CALL path that made #5602 report a client error as a server fault.
    final ArithmeticErrorException arithmetic = new ArithmeticErrorException("long overflow");

    assertThat(ErrorCategory.of(new TransactionException("commit failed", arithmetic))).isEqualTo(ErrorCategory.ARITHMETIC);
    assertThat(ErrorCategory.of(new CommandExecutionException("outer", new TransactionException("inner", arithmetic))))
        .isEqualTo(ErrorCategory.ARITHMETIC);
  }

  @Test
  void aRetryableConflictWinsOverAnArithmeticError() {
    // Same precedence BoltNetworkExecutor.classifyExecutionError documents: a chain carrying both must keep the
    // transient classification, because that is the one a driver acts on.
    final Throwable both = new LockTimeoutException("lock timeout", new ArithmeticErrorException("long overflow"));

    assertThat(ErrorCategory.of(both)).isEqualTo(ErrorCategory.RETRY);
  }

  @Test
  void retryableConflictsAreTheirOwnCategory() {
    assertThat(ErrorCategory.of(new ConcurrentModificationException("conflict"))).isEqualTo(ErrorCategory.RETRY);
    assertThat(ErrorCategory.of(new LockTimeoutException("lock"))).isEqualTo(ErrorCategory.RETRY);
    assertThat(ErrorCategory.of(new TransactionException("wrapped", new ConcurrentModificationException("conflict"))))
        .isEqualTo(ErrorCategory.RETRY);
  }

  @Test
  void theRemainingClientErrorCategoriesAreRecognised() {
    assertThat(ErrorCategory.of(new DuplicatedKeyException("idx", "k", new RID(1, 1))))
        .isEqualTo(ErrorCategory.DUPLICATED_KEY);
    assertThat(ErrorCategory.of(new RecordNotFoundException("gone", new RID(1, 1)))).isEqualTo(ErrorCategory.NOT_FOUND);
    assertThat(ErrorCategory.of(new SecurityException("denied"))).isEqualTo(ErrorCategory.SECURITY);
    assertThat(ErrorCategory.of(new ValidationException("mandatory property"))).isEqualTo(ErrorCategory.VALIDATION);
    assertThat(ErrorCategory.of(new IllegalArgumentException("bad parameter"))).isEqualTo(ErrorCategory.VALIDATION);
    assertThat(ErrorCategory.of(new QueryNotIdempotentException("writes on a query"))).isEqualTo(ErrorCategory.VALIDATION);
    assertThat(ErrorCategory.of(new CommandParsingException("bad syntax"))).isEqualTo(ErrorCategory.PARSING);
    assertThat(ErrorCategory.of(new CommandSQLParsingException("bad syntax"))).isEqualTo(ErrorCategory.PARSING);
    assertThat(ErrorCategory.of(new TimeoutException("too slow"))).isEqualTo(ErrorCategory.TIMEOUT);
  }

  @Test
  void anArithmeticErrorIsNotDowngradedToItsCommandExecutionSupertype() {
    // ArithmeticErrorException extends CommandExecutionException on purpose, so the ladder has to test the
    // subtype first or every arithmetic failure silently classifies as a server fault.
    assertThat(ErrorCategory.of(new ArithmeticErrorException("% by zero"))).isNotEqualTo(ErrorCategory.SERVER);
  }

  @Test
  void aGraphqlStyleParsingWrapperDoesNotHideAnArithmeticError() {
    // graphql used to rewrap execution failures as CommandParsingException. Even with that wrapper the
    // arithmetic error underneath has to win, otherwise the caller is told their syntax was invalid.
    final Throwable wrapped = new CommandParsingException("Error on executing GraphQL query",
        new ArithmeticErrorException("/ by zero"));

    assertThat(ErrorCategory.of(wrapped)).isEqualTo(ErrorCategory.ARITHMETIC);
  }

  @Test
  void aCyclicCauseChainTerminates() {
    final CommandExecutionException a = new CommandExecutionException("a");
    final CommandExecutionException b = new CommandExecutionException("b", a);
    a.initCause(b);

    assertThat(ErrorCategory.of(a)).isEqualTo(ErrorCategory.SERVER);
  }

  @Test
  void onlyServerAndRetryAndTimeoutAreNotClientErrors() {
    assertThat(ErrorCategory.SERVER.isClientError()).isFalse();
    assertThat(ErrorCategory.RETRY.isClientError()).isFalse();
    assertThat(ErrorCategory.TIMEOUT.isClientError()).isFalse();

    assertThat(ErrorCategory.ARITHMETIC.isClientError()).isTrue();
    assertThat(ErrorCategory.DUPLICATED_KEY.isClientError()).isTrue();
    assertThat(ErrorCategory.NOT_FOUND.isClientError()).isTrue();
    assertThat(ErrorCategory.SECURITY.isClientError()).isTrue();
    assertThat(ErrorCategory.VALIDATION.isClientError()).isTrue();
    assertThat(ErrorCategory.PARSING.isClientError()).isTrue();
  }
}
