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

import org.junit.jupiter.api.Test;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #4959: pins the DOCUMENTED isolation contract. ArcadeDB implements isolation with page-level MVCC
 * (optimistic write-write conflict detection on page versions) and performs no read-set validation. As a
 * consequence WRITE SKEW is possible under both READ_COMMITTED and REPEATABLE_READ: two transactions can each
 * read state the other is about to invalidate and both commit, because their write sets touch disjoint pages.
 * <p>
 * This is a legitimate, documented property of the engine (see {@link Database.TRANSACTION_ISOLATION_LEVEL}),
 * not a bug: this test exists so that any future change to the contract is a conscious one.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class TransactionIsolationContractTest extends TestHelper {

  private static final String TYPE_X = "IsolationAccountX";
  private static final String TYPE_Y = "IsolationAccountY";

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      // Two separate types = two separate files/pages, so the two writers never conflict at the page level.
      database.getSchema().createDocumentType(TYPE_X);
      database.getSchema().createDocumentType(TYPE_Y);
      database.newDocument(TYPE_X).set("balance", 100).save();
      database.newDocument(TYPE_Y).set("balance", 100).save();
    });
  }

  @Test
  void writeSkewIsPossibleUnderReadCommitted() throws Exception {
    runWriteSkew(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
  }

  @Test
  void writeSkewIsPossibleUnderRepeatableRead() throws Exception {
    runWriteSkew(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
  }

  /**
   * Classic write skew: the invariant "balance(X) + balance(Y) >= 100" is checked by both transactions, each
   * withdraws 100 from a DIFFERENT account, and both commit. Any serial execution preserves the invariant; the
   * concurrent execution violates it because neither transaction's read set is validated at commit.
   */
  private void runWriteSkew(final Database.TRANSACTION_ISOLATION_LEVEL isolationLevel) throws Exception {
    final CyclicBarrier bothHaveRead = new CyclicBarrier(2);
    final AtomicReference<Throwable> failure = new AtomicReference<>();

    final Thread t1 = new Thread(() -> withdrawIfCovered(isolationLevel, TYPE_X, bothHaveRead, failure));
    final Thread t2 = new Thread(() -> withdrawIfCovered(isolationLevel, TYPE_Y, bothHaveRead, failure));
    t1.start();
    t2.start();
    t1.join();
    t2.join();

    assertThat(failure.get()).as("both transactions must commit without conflicts").isNull();

    database.begin();
    final int sum = getBalance(TYPE_X) + getBalance(TYPE_Y);
    database.commit();

    // 200 - 100 - 100: the invariant sum >= 100 is violated, pinning that write skew IS possible.
    assertThat(sum).as("write skew: both withdrawals committed despite the shared invariant").isEqualTo(0);
  }

  private void withdrawIfCovered(final Database.TRANSACTION_ISOLATION_LEVEL isolationLevel, final String accountType,
      final CyclicBarrier bothHaveRead, final AtomicReference<Throwable> failure) {
    try {
      database.begin(isolationLevel);

      // Read BOTH balances (the read set spans both accounts), then wait for the other transaction to have
      // read too, so neither can see the other's write.
      final int sum = getBalance(TYPE_X) + getBalance(TYPE_Y);
      bothHaveRead.await();

      if (sum - 100 >= 100) {
        final MutableDocument account = database.iterateType(accountType, false).next().asDocument().modify();
        account.set("balance", account.getInteger("balance") - 100);
        account.save();
      }
      database.commit();
    } catch (final Throwable e) {
      failure.compareAndSet(null, e);
      if (database.isTransactionActive())
        database.rollback();
    }
  }

  private int getBalance(final String type) {
    return database.iterateType(type, false).next().asDocument().getInteger("balance");
  }
}
