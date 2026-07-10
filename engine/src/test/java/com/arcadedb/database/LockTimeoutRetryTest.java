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
package com.arcadedb.database;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.LockTimeoutException;
import com.arcadedb.exception.TimeoutException;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for the lock-timeout vs deadline-timeout split. A {@link LockTimeoutException}
 * (lock contention, e.g. file locks during commit) is a {@code NeedRetryException} and must be
 * retried by {@code Database.transaction(..., attempts, ...)}. A plain {@link TimeoutException}
 * (query/iteration deadline) must NOT be retried - it propagates after a single attempt.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class LockTimeoutRetryTest extends TestHelper {

  @Test
  void lockTimeoutIsRetriedByTransactionLoop() {
    final int attempts = 3;
    final AtomicInteger calls = new AtomicInteger();
    // The retry loop must invoke the block once per attempt and, after exhausting them, rethrow.
    assertThatThrownBy(() -> database.transaction(() -> {
      calls.incrementAndGet();
      throw new LockTimeoutException("Timeout on locking files during commit");
    }, false, attempts)).isInstanceOf(LockTimeoutException.class);
    assertThat(calls.get()).isEqualTo(attempts);
  }

  @Test
  void deadlineTimeoutIsNotRetried() {
    final AtomicInteger calls = new AtomicInteger();
    // A deadline TimeoutException is not a NeedRetryException, so it must escape after one attempt
    // even when several retries are configured.
    assertThatThrownBy(() -> database.transaction(() -> {
      calls.incrementAndGet();
      throw new TimeoutException("Timeout expired");
    }, false, 3)).isInstanceOf(TimeoutException.class);
    assertThat(calls.get()).isEqualTo(1);
  }
}
