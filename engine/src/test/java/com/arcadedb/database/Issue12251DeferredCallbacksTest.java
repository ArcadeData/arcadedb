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

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12251: {@link TransactionContext#deferCallbacks()} makes the end of a transaction hand back the callbacks it owes instead of
 * running them where it concludes, so the replication layer can run them once its commit has released the locks that a callback, a
 * transaction and a commit of its own, must not run under.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class Issue12251DeferredCallbacksTest extends TestHelper {

  @Test
  void aCommitConcludedWhileDeferringIsCountedAndOwesBothKindsOfCallbackInOrder() {
    final List<String> events = new ArrayList<>();

    database.begin();
    final TransactionContext tx = ((DatabaseInternal) database).getTransaction();
    tx.addAfterCommitCallback(() -> events.add("after-commit"));
    tx.addAfterCompletionCallbackIfAbsent("issue12251", () -> events.add("after-completion"));
    final long commitCountBefore = tx.getCommitCount();

    tx.deferCallbacks();
    assertThat(tx.isDeferringCallbacks()).isTrue();
    assertThat(tx.commit1stPhase(true)).as("nothing to write").isNull();
    tx.concludeCommitWithoutPublishing();

    assertThat(events).as("nothing runs where the transaction concludes").isEmpty();
    assertThat(tx.getCommitCount()).isEqualTo(commitCountBefore + 1);
    assertThat(database.isTransactionActive()).isFalse();

    final Runnable owed = tx.takeDeferredCallbacks();

    assertThat(owed).isNotNull();
    assertThat(tx.isDeferringCallbacks()).as("taking what is owed ends the deferral").isFalse();
    assertThat(events).as("and nothing runs by taking them").isEmpty();

    owed.run();

    assertThat(events).containsExactly("after-commit", "after-completion");
    assertThat(tx.takeDeferredCallbacks()).as("what was taken is not owed a second time").isNull();
  }

  @Test
  void aTransactionWithNoCallbacksOwesNothing() {
    database.begin();
    final TransactionContext tx = ((DatabaseInternal) database).getTransaction();
    final long commitCountBefore = tx.getCommitCount();

    tx.deferCallbacks();
    assertThat(tx.commit1stPhase(true)).as("nothing to write").isNull();
    tx.concludeCommitWithoutPublishing();

    assertThat(tx.takeDeferredCallbacks()).isNull();
    assertThat(tx.getCommitCount()).isEqualTo(commitCountBefore + 1);
    assertThat(database.isTransactionActive()).isFalse();
  }

  /** Without the deferral the callbacks run where the transaction concludes, as they always did. */
  @Test
  void aTransactionConcludedWithoutDeferringRunsItsCallbacksAtOnce() {
    final List<String> events = new ArrayList<>();

    database.begin();
    final TransactionContext tx = ((DatabaseInternal) database).getTransaction();
    tx.addAfterCommitCallback(() -> events.add("after-commit"));
    tx.addAfterCompletionCallbackIfAbsent("issue12251", () -> events.add("after-completion"));

    assertThat(tx.isDeferringCallbacks()).isFalse();
    assertThat(tx.commit1stPhase(true)).as("nothing to write").isNull();
    tx.concludeCommitWithoutPublishing();

    assertThat(events).containsExactly("after-commit", "after-completion");
    assertThat(tx.takeDeferredCallbacks()).isNull();
  }

  /** A rollback owes the after-completion callbacks (#10687) and not the after-commit ones, and the deferral moves only their run. */
  @Test
  void aRollbackWhileDeferringOwesTheAfterCompletionCallbacksOnly() {
    final List<String> events = new ArrayList<>();

    database.begin();
    final TransactionContext tx = ((DatabaseInternal) database).getTransaction();
    tx.addAfterCommitCallback(() -> events.add("after-commit"));
    tx.addAfterCompletionCallbackIfAbsent("issue12251", () -> events.add("after-completion"));

    tx.deferCallbacks();
    tx.rollback();

    assertThat(events).isEmpty();
    final Runnable owed = tx.takeDeferredCallbacks();
    assertThat(owed).isNotNull();
    owed.run();
    assertThat(events).containsExactly("after-completion");
  }

  /** A driver that rolls back what it failed to commit ends one context twice: both are owed, in the order they ended. */
  @Test
  void aTransactionEndedTwiceWhileDeferringOwesBothInOrder() {
    final List<String> events = new ArrayList<>();

    database.begin();
    final TransactionContext tx = ((DatabaseInternal) database).getTransaction();
    tx.addAfterCompletionCallbackIfAbsent("first", () -> events.add("first"));
    tx.deferCallbacks();
    assertThat(tx.commit1stPhase(true)).as("nothing to write").isNull();
    tx.concludeCommitWithoutPublishing();

    database.begin();
    assertThat(((DatabaseInternal) database).getTransaction()).as("the context is reused").isSameAs(tx);
    tx.addAfterCompletionCallbackIfAbsent("second", () -> events.add("second"));
    tx.rollback();

    assertThat(events).isEmpty();
    final Runnable owed = tx.takeDeferredCallbacks();
    assertThat(owed).isNotNull();
    owed.run();
    assertThat(events).containsExactly("first", "second");
  }
}
