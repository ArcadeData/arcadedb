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
package com.arcadedb.postgres;

import com.arcadedb.database.RID;
import com.arcadedb.exception.ArithmeticErrorException;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.LockTimeoutException;
import com.arcadedb.exception.QueryNotIdempotentException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.exception.ValidationException;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * SQLSTATE mapping for the Postgres wire protocol (issue #5628). Before this, every failure that was not a parsing
 * error reported as {@code XX000} internal_error, so a caller's own mistake - a division by zero, a duplicate key -
 * looked to their driver like the server had broken, and a retryable conflict carried a code no driver retries.
 */
class PostgresErrorClassificationTest {

  @Test
  void aDivisionByZeroIsADataExceptionNotAServerFault() {
    assertThat(PostgresNetworkExecutor.sqlStateFor(new ArithmeticErrorException("/ by zero"))).isEqualTo("22012");
    assertThat(PostgresNetworkExecutor.sqlStateFor(new ArithmeticErrorException("% by zero"))).isEqualTo("22012");
    assertThat(PostgresNetworkExecutor.sqlStateFor(new ArithmeticErrorException("Cannot divide duration by zero")))
        .isEqualTo("22012");
  }

  @Test
  void anOverflowIsOutOfRange() {
    assertThat(PostgresNetworkExecutor.sqlStateFor(new ArithmeticErrorException("long overflow"))).isEqualTo("22003");
    assertThat(PostgresNetworkExecutor.sqlStateFor(new ArithmeticErrorException("duration overflow"))).isEqualTo("22003");
  }

  @Test
  void anUnrecognisedArithmeticMessageStaysInTheDataExceptionClass() {
    // The split between 22012 and 22003 is a nicety; what must not drift is the class-22 verdict that tells the
    // driver this was the caller's data, not a server fault.
    assertThat(PostgresNetworkExecutor.sqlStateFor(new ArithmeticErrorException(null))).startsWith("22");
    assertThat(PostgresNetworkExecutor.sqlStateFor(new ArithmeticErrorException("some future arithmetic failure")))
        .startsWith("22");
  }

  @Test
  void aWrappedArithmeticErrorIsStillADataException() {
    // A statement reaching Postgres through the auto-commit path arrives wrapped; inspecting only the outermost
    // throwable is what reported these as XX000.
    final Throwable wrapped = new TransactionException("Error on transaction commit",
        new ArithmeticErrorException("/ by zero"));

    assertThat(PostgresNetworkExecutor.sqlStateFor(wrapped)).isEqualTo("22012");
  }

  @Test
  void aConflictGetsTheCodeDriversRetryOn() {
    assertThat(PostgresNetworkExecutor.sqlStateFor(new ConcurrentModificationException("page version changed")))
        .isEqualTo("40001");
    assertThat(PostgresNetworkExecutor.sqlStateFor(new LockTimeoutException("lock timeout"))).isEqualTo("40001");
  }

  @Test
  void aConflictWinsOverAnArithmeticErrorInTheSameChain() {
    final Throwable both = new LockTimeoutException("lock timeout", new ArithmeticErrorException("long overflow"));

    assertThat(PostgresNetworkExecutor.sqlStateFor(both)).isEqualTo("40001");
  }

  @Test
  void theRemainingCategoriesGetTheirPostgresCodes() {
    assertThat(PostgresNetworkExecutor.sqlStateFor(new DuplicatedKeyException("idx", "k", new RID(1, 1))))
        .isEqualTo("23505");
    assertThat(PostgresNetworkExecutor.sqlStateFor(new RecordNotFoundException("gone", new RID(1, 1)))).isEqualTo("P0002");
    assertThat(PostgresNetworkExecutor.sqlStateFor(new SchemaException("Type with name 'Nope' was not found"))).isEqualTo("42P01");
    assertThat(PostgresNetworkExecutor.sqlStateFor(new SecurityException("denied"))).isEqualTo("42501");
    assertThat(PostgresNetworkExecutor.sqlStateFor(new ValidationException("mandatory property"))).isEqualTo("22023");
    assertThat(PostgresNetworkExecutor.sqlStateFor(new QueryNotIdempotentException("writes on a query"))).isEqualTo("22023");
    assertThat(PostgresNetworkExecutor.sqlStateFor(new CommandParsingException("bad syntax"))).isEqualTo("42601");
    assertThat(PostgresNetworkExecutor.sqlStateFor(new TimeoutException("too slow"))).isEqualTo("57014");
  }

  @Test
  void aParsingWrapperDoesNotHideTheRealFailure() {
    // Both execution arms used to hardcode 42601 for CommandParsingException, which made the
    // ARITHMETIC-before-PARSING ordering in ErrorCategory dead on this path: a query engine that wraps an
    // execution failure as a parsing exception - as GraphQL did until this change - would have reported a
    // division by zero as a syntax error. Routing the arm through sqlStateFor keeps genuine parse errors at
    // 42601 while letting the real cause win.
    assertThat(PostgresNetworkExecutor.sqlStateFor(
        new CommandParsingException("Error on executing query", new ArithmeticErrorException("/ by zero"))))
        .isEqualTo("22012");
    assertThat(PostgresNetworkExecutor.sqlStateFor(
        new CommandParsingException("Error on executing query", new ConcurrentModificationException("conflict"))))
        .isEqualTo("40001");
    assertThat(PostgresNetworkExecutor.sqlStateFor(
        new CommandParsingException("Error on executing query", new SchemaException("Type 'Nope' was not found"))))
        .isEqualTo("42P01");

    // A parse error that is genuinely just a parse error keeps the code it always had.
    assertThat(PostgresNetworkExecutor.sqlStateFor(new CommandParsingException("unexpected token"))).isEqualTo("42601");
  }

  @Test
  void anUnknownTypeIsNotAServerFault() {
    // SELECT FROM NonExistentType used to report XX000 internal_error, telling the driver the server had broken.
    assertThat(PostgresNetworkExecutor.sqlStateFor(new SchemaException("Type with name 'DoesNotExist' was not found")))
        .isEqualTo("42P01")
        .isNotEqualTo("XX000");
  }

  @Test
  void agenuineServerFaultIsStillInternalError() {
    assertThat(PostgresNetworkExecutor.sqlStateFor(new IOException("disk gone"))).isEqualTo("XX000");
    assertThat(PostgresNetworkExecutor.sqlStateFor(new CommandExecutionException("something broke"))).isEqualTo("XX000");
    assertThat(PostgresNetworkExecutor.sqlStateFor(null)).isEqualTo("XX000");
  }
}
