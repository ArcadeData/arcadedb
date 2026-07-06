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

import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.LockTimeoutException;
import com.arcadedb.exception.NeedRetryException;

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
}
