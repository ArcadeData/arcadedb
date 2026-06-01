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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression for the missing rollback in {@link DatabaseAsyncTransaction} when
 * {@code tx.execute()} throws {@link ConcurrentModificationException}.
 *
 * <p>Without the fix, the failed attempt's transaction is still active when the retry begins.
 * {@code LocalDatabase.begin()} then creates a nested sub-transaction for the retry, leaving the
 * partial writes from attempt 0 in the outer (uncommitted) transaction. That outer transaction is
 * committed later (e.g. by the {@code waitCompletion} semaphore task), producing a duplicate
 * record in the database.
 *
 * <p>With the fix, the CME catch rolls back the active transaction before continuing to the next
 * retry, so each attempt starts with a clean slate.
 */
class DatabaseAsyncTransactionRetryTest extends TestHelper {

  private static final String TYPE = "TestAsyncDoc";

  /**
   * A CME on attempt 0 must not cause the partial writes from that attempt to appear in the
   * database. Only the successful retry's writes should be committed.
   */
  @Test
  void partialWriteFromFailedAttemptIsNotCommitted() throws Exception {
    database.getSchema().createDocumentType(TYPE);

    final AtomicInteger attempts = new AtomicInteger(0);
    final AtomicBoolean okCalled = new AtomicBoolean(false);
    final AtomicReference<Throwable> errorRef = new AtomicReference<>();

    database.async().transaction(
        () -> {
          database.newDocument(TYPE).set("attempt", attempts.get()).save();
          if (attempts.getAndIncrement() == 0)
            throw new ConcurrentModificationException("simulated conflict");
          // attempt 1 succeeds with the document already saved above
        },
        3,
        () -> okCalled.set(true),
        e -> errorRef.set(e));

    // waitCompletion issues a DatabaseAsyncCompletion task per thread, which commits any
    // active transaction on that thread.  Without the fix, it would commit the orphan
    // transaction that still holds the document from the failed attempt 0.
    database.async().waitCompletion(5_000);

    assertThat(errorRef.get()).isNull();
    assertThat(okCalled.get()).isTrue();

    // exactly one document must exist - only from the committed retry
    final long count = ((Number)
        database.query("sql", "SELECT count(*) as cnt FROM " + TYPE).next().getProperty("cnt")).longValue();
    assertThat(count).isEqualTo(1L);
  }
}
