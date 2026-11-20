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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The work a transaction owes at the start of its commit, while it is still active (issue #11937): it runs once, on the committing
 * thread, sees the transaction's own writes, is dropped by a rollback, and an exception it throws fails the commit.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class TransactionBeforeCommitCallbackTest extends TestHelper {
  @Override
  protected void beginTest() {
    database.transaction(() -> database.getSchema().createDocumentType("Doc"));
  }

  @Test
  void runsOnceWhenTheCommitStartsAndSeesTheTransactionsWrites() {
    final AtomicInteger runs = new AtomicInteger();
    final AtomicInteger seen = new AtomicInteger(-1);
    final AtomicBoolean active = new AtomicBoolean();

    database.begin();
    database.newDocument("Doc").set("n", 1).save();
    final TransactionContext tx = ((DatabaseInternal) database).getTransaction();
    assertThat(tx.addBeforeCommitCallbackIfAbsent("k", () -> {
      runs.incrementAndGet();
      active.set(database.isTransactionActive());
      seen.set((int) database.countType("Doc", false));
    })).isTrue();
    assertThat(tx.addBeforeCommitCallbackIfAbsent("k", runs::incrementAndGet)).as("one callback per key").isFalse();
    assertThat(runs.get()).as("not before the commit").isZero();

    database.commit();

    assertThat(runs.get()).isEqualTo(1);
    assertThat(active.get()).as("the transaction is still active when it runs").isTrue();
    assertThat(seen.get()).as("it reads what the commit writes").isEqualTo(1);

    database.transaction(() -> database.newDocument("Doc").set("n", 2).save());
    assertThat(runs.get()).as("the next transaction does not inherit it").isEqualTo(1);
  }

  @Test
  void aRollbackDropsIt() {
    final AtomicInteger runs = new AtomicInteger();
    database.begin();
    database.newDocument("Doc").set("n", 1).save();
    ((DatabaseInternal) database).getTransaction().addBeforeCommitCallbackIfAbsent("k", runs::incrementAndGet);
    database.rollback();

    database.transaction(() -> database.newDocument("Doc").set("n", 2).save());
    assertThat(runs.get()).isZero();
  }

  @Test
  void anExceptionItThrowsFailsTheCommit() {
    database.begin();
    database.newDocument("Doc").set("n", 1).save();
    ((DatabaseInternal) database).getTransaction().addBeforeCommitCallbackIfAbsent("k", () -> {
      throw new IllegalStateException("refused");
    });

    assertThatThrownBy(() -> database.commit()).isInstanceOf(IllegalStateException.class).hasMessage("refused");
    if (database.isTransactionActive())
      database.rollback();

    assertThat(database.countType("Doc", false)).as("nothing was committed").isZero();
  }
}
