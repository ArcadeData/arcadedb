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
package com.arcadedb.database.async;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.ConcurrentModificationException;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for #4369: after retries are exhausted in {@link DatabaseAsyncTransaction},
 * the per-task {@code onErrorCallback} must be invoked and the transaction rolled back.
 * Before the fix the per-task callback was silently skipped and no exception was propagated
 * to the async worker, leaving a stale open transaction.
 */
class DatabaseAsyncTransactionRetryExhaustedTest extends TestHelper {

  @Test
  void perTaskErrorCallbackInvokedWhenRetriesExhausted() throws Exception {
    final AtomicBoolean okCalled = new AtomicBoolean(false);
    final AtomicReference<Throwable> perTaskError = new AtomicReference<>();
    final AtomicReference<Throwable> globalError = new AtomicReference<>();
    final CountDownLatch latch = new CountDownLatch(1);

    database.async().onError(e -> globalError.set(e));

    // Submit a transaction that always throws ConcurrentModificationException.
    // retries=2 means 3 total attempts before the error path is reached.
    database.async().transaction(
        () -> {
          throw new ConcurrentModificationException("simulated conflict");
        },
        2,
        () -> okCalled.set(true),
        e -> {
          perTaskError.set(e);
          latch.countDown();
        }
    );

    assertThat(latch.await(5, TimeUnit.SECONDS)).as("per-task error callback must fire within 5 s").isTrue();
    database.async().waitCompletion();

    assertThat(okCalled.get()).as("ok callback must not be invoked on failure").isFalse();
    assertThat(perTaskError.get()).as("per-task error callback must receive the ConcurrentModificationException")
        .isInstanceOf(ConcurrentModificationException.class);
    assertThat(globalError.get()).as("global onError must also be notified")
        .isInstanceOf(ConcurrentModificationException.class);
    assertThat(database.isTransactionActive()).as("stale transaction must be rolled back after exhaustion").isFalse();
  }
}
